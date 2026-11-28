import { describe, expect, it, vi } from "vite-plus/test";
import { AsyncResult } from "effect/reactivity";
import * as Cause from "effect/Cause";
import {
  EnvironmentId,
  MessageId,
  ThreadId,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import {
  AccountAutoRoutingSessionError,
  type PreparedThreadTurn,
  type StartThreadTurnInput,
} from "@t3tools/client-runtime/operations";
import { createPendingSendStore } from "./store";
import { sendPendingTurn } from "./send";
import { startFanoutThread } from "../../components/chat/chatViewSend";
const environmentId = EnvironmentId.make("env");
const input: StartThreadTurnInput = {
  threadId: ThreadId.make("thread"),
  runtimeMode: "full-access",
  interactionMode: "default",
  message: { messageId: MessageId.make("message"), role: "user", text: "hello", attachments: [] },
};
const prepared = Object.freeze({
  method: "orchestration.dispatchCommand",
  payload: Object.freeze({
    threadId: input.threadId,
    commandId: "frozen-command",
    messageId: input.message.messageId,
  }),
}) as unknown as PreparedThreadTurn;
const failure = () => AsyncResult.failure<never, Error>(Cause.fail(new Error("lost reply")));
const deferred = <A>() => {
  let resolve!: (value: A) => void;
  const promise = new Promise<A>((done) => {
    resolve = done;
  });
  return { promise, resolve };
};

describe("prepared pending turn", () => {
  it("admits before asynchronous preparation and never dispatches a preparation failure", async () => {
    const store = createPendingSendStore();
    const preparation = deferred<ReturnType<typeof failure>>();
    const dispatch = vi.fn(async () => AsyncResult.success({ sequence: 1 }));
    const result = sendPendingTurn({
      environmentId,
      input,
      store,
      prepare: () => preparation.promise,
      dispatch,
    });
    expect(store.getSnapshot()).toMatchObject([
      { delivery: "sending", working: true, issued: false },
    ]);
    preparation.resolve(failure());
    await result;
    expect(store.getSnapshot()).toMatchObject([{ delivery: "failed", working: false }]);
    expect(dispatch).not.toHaveBeenCalled();
  });
  it("waits for the recovery write and never issues when that write fails", async () => {
    const store = createPendingSendStore();
    const saving = deferred<boolean>();
    const beforeDispatch = vi.fn(async () => {
      if (!(await saving.promise)) throw new Error("Storage unavailable");
    });
    const dispatch = vi.fn(async () => AsyncResult.success({ sequence: 1 }));
    const result = sendPendingTurn({
      environmentId,
      input,
      store,
      prepare: async () => AsyncResult.success(prepared),
      beforeDispatch,
      dispatch,
    });
    await Promise.resolve();
    expect(beforeDispatch).toHaveBeenCalledWith(prepared);
    expect(dispatch).not.toHaveBeenCalled();
    expect(store.getSnapshot()[0]?.issued).toBe(false);

    saving.resolve(false);
    expect((await result)._tag).toBe("Failure");
    expect(store.getSnapshot()[0]).toMatchObject({ issued: false, delivery: "failed" });
    expect(dispatch).not.toHaveBeenCalled();
  });
  it("restores an unsent draft on capability loss and preserves uncertain earlier attempts", async () => {
    const unsupported = () =>
      AsyncResult.failure<never, Error>(
        Cause.fail(
          new AccountAutoRoutingSessionError({ message: "Automatic accounts unavailable" }),
        ),
      );
    const prepare = async () => AsyncResult.success(prepared);
    const first = createPendingSendStore();
    await sendPendingTurn({
      environmentId,
      input,
      store: first,
      prepare,
      dispatch: async () => unsupported(),
    });
    expect(first.getSnapshot()[0]?.delivery).toBe("failed");

    const issued = createPendingSendStore();
    await sendPendingTurn({
      environmentId,
      input,
      store: issued,
      prepare,
      dispatch: vi.fn().mockResolvedValueOnce(failure()).mockResolvedValueOnce(unsupported()),
    });
    expect(issued.getSnapshot()[0]?.delivery).toBe("uncertain");
    await issued.getSnapshot()[0]!.retry!();
    expect(issued.getSnapshot()[0]?.delivery).toBe("uncertain");
  });
  it("manual Retry is single-flight and dispatches the same prepared object without preparation or reconnect dispatch", async () => {
    const store = createPendingSendStore();
    const retryReply = deferred<ReturnType<typeof failure>>();
    const prepare = vi.fn(async () => AsyncResult.success(prepared));
    const dispatch = vi
      .fn()
      .mockResolvedValueOnce(failure())
      .mockImplementationOnce(() => retryReply.promise);
    await sendPendingTurn({ environmentId, input, store, prepare, dispatch });
    expect(store.getSnapshot()[0]?.delivery).toBe("uncertain");
    const retry = store.getSnapshot()[0]!.retry!;
    const pending = retry();
    await retry();
    expect(dispatch).toHaveBeenCalledTimes(2);
    expect(dispatch.mock.calls.every(([value]) => value === prepared)).toBe(true);
    expect(prepare).toHaveBeenCalledTimes(1);
    retryReply.resolve(failure());
    await pending;
    expect(store.getSnapshot()[0]?.delivery).toBe("uncertain");
  });
  it("late failure cannot undo committed evidence or offer another dispatch", async () => {
    const store = createPendingSendStore();
    const reply = deferred<ReturnType<typeof failure>>();
    const dispatch = vi.fn(() => reply.promise);
    const result = sendPendingTurn({
      environmentId,
      input,
      store,
      prepare: async () => AsyncResult.success(prepared),
      dispatch,
    });
    await Promise.resolve();
    const retry = store.getSnapshot()[0]!.retry!;
    store.reconcile(environmentId, input.threadId, {
      runs: [],
      messages: [{ id: input.message.messageId }],
      visibleTurnItems: [],
    } as unknown as OrchestrationV2ThreadProjection);
    reply.resolve(failure());
    await result;
    await retry();
    expect(store.getSnapshot()[0]?.delivery).toBe("accepted");
    expect(dispatch).toHaveBeenCalledTimes(1);
  });
});

describe("fanout acceptance guard", () => {
  it.each(["ack", "echo"] as const)(
    "clears only its target guard after exact-request Retry acceptance by %s",
    async (acceptance) => {
      const store = createPendingSendStore();
      const guards = new Map<string, ThreadId>();
      const retryKey = "model-target";
      const otherThread = ThreadId.make("other-target");
      guards.set("other-model", otherThread);
      const retryReply = deferred<ReturnType<typeof AsyncResult.success<{ sequence: number }>>>();
      const dispatch = vi
        .fn()
        .mockResolvedValueOnce(failure())
        .mockImplementationOnce(() => retryReply.promise);
      const key = store.admit(environmentId, input.threadId, {
        id: input.message.messageId,
        role: "user",
        text: "hello",
        runId: null,
        createdAt: "2026-10-26T20:00:00Z",
        updatedAt: "2026-10-26T20:00:00Z",
        streaming: false,
      });
      const request = {
        retryKey,
        model: "model",
        targetThreadId: input.threadId,
        uncertainSubmissions: guards,
        whenAccepted: (callback: () => void) =>
          store.whenAccepted(environmentId, input.threadId, input.message.messageId, callback),
        start: () =>
          sendPendingTurn({
            environmentId,
            input,
            store,
            prepare: async () => AsyncResult.success(prepared),
            dispatch,
          }),
        openThread: vi.fn(),
      };
      expect(await startFanoutThread(request)).toBe(false);
      expect(guards.get(retryKey)).toBe(input.threadId);
      const retry = store.getSnapshot()[0]!.retry!();
      if (acceptance === "echo") {
        store.reconcile(environmentId, input.threadId, {
          runs: [],
          messages: [{ id: input.message.messageId }],
          visibleTurnItems: [],
        } as unknown as OrchestrationV2ThreadProjection);
      }
      retryReply.resolve(AsyncResult.success({ sequence: 1 }));
      await retry;
      expect(guards.has(retryKey)).toBe(false);
      expect(guards.get("other-model")).toBe(otherThread);
      expect(dispatch.mock.calls[1]![0]).toBe(dispatch.mock.calls[0]![0]);
      store.dismiss(key);
      const nextStart = vi.fn(async () => AsyncResult.success({ sequence: 2 }));
      expect(
        await startFanoutThread({ ...request, targetThreadId: otherThread, start: nextStart }),
      ).toBe(true);
      expect(nextStart).toHaveBeenCalledWith(otherThread);
    },
  );

  it.each([false, true])(
    "does not restore a guard after echo-before-ACK, preserving a newer guard: %s",
    async (newerGuard) => {
      const store = createPendingSendStore();
      const guards = new Map<string, ThreadId>();
      const reply = deferred<ReturnType<typeof failure>>();
      const newerThread = ThreadId.make("newer-target");
      store.admit(environmentId, input.threadId, {
        id: input.message.messageId,
        role: "user",
        text: "hello",
        runId: null,
        createdAt: "2026-10-26T20:00:00Z",
        updatedAt: "2026-10-26T20:00:00Z",
        streaming: false,
      });
      const sending = startFanoutThread({
        retryKey: "model-target",
        model: "model",
        targetThreadId: input.threadId,
        uncertainSubmissions: guards,
        whenAccepted: (callback: () => void) =>
          store.whenAccepted(environmentId, input.threadId, input.message.messageId, callback),
        start: () =>
          sendPendingTurn({
            environmentId,
            input,
            store,
            prepare: async () => AsyncResult.success(prepared),
            dispatch: () => reply.promise,
          }),
        openThread: vi.fn(),
      });
      await Promise.resolve();
      if (newerGuard) guards.set("model-target", newerThread);
      store.reconcile(environmentId, input.threadId, {
        runs: [],
        messages: [],
        visibleTurnItems: [
          {
            sourceThreadId: input.threadId,
            item: { type: "user_message", messageId: input.message.messageId },
          },
        ],
      } as unknown as OrchestrationV2ThreadProjection);
      reply.resolve(failure());
      expect(await sending).toBe(true);
      expect(guards.get("model-target")).toBe(newerGuard ? newerThread : undefined);
    },
  );
});

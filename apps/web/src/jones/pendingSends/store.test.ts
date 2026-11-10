import { describe, expect, it, vi } from "vite-plus/test";
import {
  EnvironmentId,
  MessageId,
  ThreadId,
  CommandId,
  ProjectId,
  RunId,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import type { ChatMessage } from "../../types";
import { createPendingSendStore } from "./store";
import { initialMessageCommandId } from "@t3tools/contracts/jones/deliveryEvidence";
import { canSendAfterInitialMessageRejection } from "./newThread";

const env = EnvironmentId.make("env");
const thread = ThreadId.make("thread");
const message: ChatMessage = {
  id: MessageId.make("message"),
  role: "user",
  text: "hello",
  runId: null,
  createdAt: "2026-10-26T20:00:00Z",
  updatedAt: "2026-10-26T20:00:00Z",
  streaming: false,
};
function projection(
  input: {
    message?: boolean;
    run?: boolean;
    visible?: boolean;
    threadId?: ThreadId;
    status?: string;
  } = {},
) {
  return {
    messages: input.message ? [{ id: message.id }] : [],
    runs: input.run
      ? [{ id: RunId.make("run"), userMessageId: message.id, status: input.status ?? "completed" }]
      : [],
    visibleTurnItems: input.visible
      ? [
          {
            sourceThreadId: input.threadId ?? thread,
            item: { type: "user_message", messageId: message.id },
          },
        ]
      : [],
  } as unknown as OrchestrationV2ThreadProjection;
}

describe("pending sends", () => {
  it.each(["preparing", "starting", "running", "waiting"])(
    "keeps sidebar activity through %s acknowledgement until the shell catches up",
    (status) => {
      const store = createPendingSendStore();
      store.admit(env, thread, message);
      const stale = { latestRunId: RunId.make("previous"), activeRunId: null };
      store.reconcile(env, thread, projection({ message: true, run: true, status }), stale);
      expect(store.getSnapshot()).toMatchObject([{ delivery: "accepted", working: true }]);
      const visible = projection({ message: true, run: true, visible: true, status });
      store.reconcile(env, thread, visible, stale);
      expect(store.getSnapshot()).toMatchObject([{ visible: true, working: true }]);
      const stable = store.getSnapshot();
      store.reconcile(env, thread, visible, stale);
      expect(store.getSnapshot()).toBe(stable);
      store.reconcile(env, thread, visible, { latestRunId: RunId.make("run"), activeRunId: null });
      expect(store.getSnapshot()).toEqual([]);
    },
  );
  it.each(["completed", "interrupted", "failed", "cancelled", "rolled_back"])(
    "releases sidebar activity on authoritative %s even while the shell lags",
    (status) => {
      const store = createPendingSendStore();
      store.admit(env, thread, message);
      store.reconcile(env, thread, projection({ run: true, visible: true, status: "starting" }));
      expect(store.getSnapshot()[0]?.working).toBe(true);
      store.reconcile(env, thread, projection({ run: true, visible: true, status }));
      expect(store.getSnapshot()).toEqual([]);
    },
  );
  it("hands activity to the shell's active run even when a newer queued run is latest", () => {
    const store = createPendingSendStore();
    store.admit(env, thread, message);
    store.reconcile(env, thread, projection({ run: true, visible: true, status: "running" }), {
      latestRunId: RunId.make("queued"),
      activeRunId: RunId.make("run"),
    });
    expect(store.getSnapshot()).toEqual([]);
  });
  it("retains accepted creation independently through later uncertain replies and rejected first-message recovery", () => {
    const store = createPendingSendStore();
    const key = store.admit(env, thread, message);
    const commandId = CommandId.make("launch");
    const initialMessage = {
      status: "unknown" as const,
      commandId: initialMessageCommandId(commandId),
      messageId: message.id,
    };
    store.issuing(key);
    store.finish(key, {
      kind: "launch",
      commandId,
      projectId: ProjectId.make("project"),
      creation: { status: "accepted", threadId: thread },
      initialMessage,
    });
    store.finish(key, {
      kind: "launch",
      commandId,
      projectId: ProjectId.make("project"),
      creation: { status: "unknown" },
      initialMessage,
    });
    expect(store.getSnapshot()[0]).toMatchObject({
      delivery: "uncertain",
      working: true,
      deliveryEvidence: { creation: { status: "accepted", threadId: thread } },
    });
    expect(canSendAfterInitialMessageRejection(store.getSnapshot(), env, thread)).toBe(false);
    store.finish(key, {
      kind: "launch",
      commandId,
      projectId: ProjectId.make("project"),
      creation: { status: "unknown" },
      initialMessage: { ...initialMessage, status: "rejected" },
    });
    expect(store.getSnapshot()[0]).toMatchObject({
      delivery: "failed",
      working: false,
      deliveryEvidence: { creation: { status: "accepted", threadId: thread } },
    });
    expect(canSendAfterInitialMessageRejection(store.getSnapshot(), env, thread)).toBe(true);
    store.finish(key, {
      kind: "launch",
      commandId,
      projectId: ProjectId.make("project"),
      creation: { status: "unknown" },
      initialMessage,
    });
    expect(store.getSnapshot()[0]).toMatchObject({
      delivery: "failed",
      working: false,
      retry: undefined,
    });
    expect(canSendAfterInitialMessageRejection(store.getSnapshot(), env, thread)).toBe(true);
    expect(
      canSendAfterInitialMessageRejection(store.getSnapshot(), EnvironmentId.make("other"), thread),
    ).toBe(false);
    expect(
      canSendAfterInitialMessageRejection(store.getSnapshot(), env, ThreadId.make("other")),
    ).toBe(false);
    const correctedKey = store.admit(env, thread, { ...message, id: MessageId.make("corrected") });
    expect(canSendAfterInitialMessageRejection(store.getSnapshot(), env, thread)).toBe(false);
    store.issuing(correctedKey);
    store.finish(correctedKey, "unknown");
    expect(canSendAfterInitialMessageRejection(store.getSnapshot(), env, thread)).toBe(false);
    store.dismiss(correctedKey);
    expect(canSendAfterInitialMessageRejection(store.getSnapshot(), env, thread)).toBe(true);
    store.reconcile(env, thread, projection({ message: true }));
    expect(store.getSnapshot()[0]?.delivery).toBe("accepted");
    expect(canSendAfterInitialMessageRejection(store.getSnapshot(), env, thread)).toBe(false);
  });
  it("keeps an optimistic row on run completion or projection acknowledgement until its visible replacement", () => {
    const store = createPendingSendStore();
    const key = store.admit(env, thread, message);
    store.issuing(key);
    store.finish(key, false);
    store.reconcile(env, thread, projection({ message: true, run: true }));
    expect(store.getSnapshot()).toMatchObject([{ delivery: "accepted", working: false, message }]);
    store.finish(key, false);
    expect(store.getSnapshot()[0]?.delivery).toBe("accepted");
    store.reconcile(env, thread, projection({ visible: true }));
    expect(store.getSnapshot()).toEqual([]);
  });
  it("never treats another environment or inherited thread's message as acceptance", () => {
    const store = createPendingSendStore();
    store.admit(env, thread, message);
    store.reconcile(EnvironmentId.make("other"), thread, projection({ visible: true }));
    store.reconcile(env, thread, projection({ visible: true, threadId: ThreadId.make("parent") }));
    expect(store.getSnapshot()[0]?.delivery).toBe("sending");
  });
  it("retains failed rows across newer sends and accepts late authoritative evidence", () => {
    const store = createPendingSendStore();
    store.admit(env, thread, message);
    store.failBeforeDispatch(env, thread, message.id);
    expect(canSendAfterInitialMessageRejection(store.getSnapshot(), env, thread)).toBe(false);
    store.admit(env, thread, { ...message, id: MessageId.make("new") });
    expect(store.getSnapshot().map((entry) => entry.delivery)).toEqual(["failed", "sending"]);
    store.reconcile(env, thread, projection({ message: true }));
    expect(store.getSnapshot()[0]?.delivery).toBe("accepted");
  });
  it("finalizes once when committed evidence wins the race with a lost acknowledgement", () => {
    const store = createPendingSendStore();
    const key = store.admit(env, thread, message);
    const accepted = vi.fn();
    store.whenAccepted(env, thread, message.id, accepted);
    store.issuing(key);
    store.reconcile(env, thread, projection({ visible: true }));
    store.finish(key, false);
    store.finish(key, true);
    expect(accepted).toHaveBeenCalledTimes(1);
    expect(store.getSnapshot()).toEqual([]);
  });
  it("keeps previews through navigation and visible handoff until server images load; releases exactly once", () => {
    const revoke = vi.fn();
    const store = createPendingSendStore(revoke);
    const key = store.admit(env, thread, {
      ...message,
      attachments: [
        {
          type: "image",
          id: "image",
          name: "image.png",
          mimeType: "image/png",
          sizeBytes: 1,
          previewUrl: "blob:owned",
        },
      ],
    });
    const unsubscribe = store.subscribe(() => {});
    unsubscribe();
    store.reconcile(env, thread, projection({ visible: true }));
    expect(store.getSnapshot()[0]?.visible).toBe(true);
    expect(revoke).not.toHaveBeenCalled();
    store.dismiss(key);
    store.dismiss(key);
    expect(revoke).toHaveBeenCalledExactlyOnceWith("blob:owned");
  });
  it("hands queued messages to the queue's representation only when both run and message are known", () => {
    const store = createPendingSendStore();
    store.admit(env, thread, { ...message, inputIntent: "queued_turn" });
    store.reconcile(env, thread, projection({ message: true }));
    expect(store.getSnapshot()).toHaveLength(1);
    store.reconcile(env, thread, projection({ run: true, status: "cancelled" }));
    expect(store.getSnapshot()).toEqual([]);
  });
  it.each([undefined, "steer", "queued_turn"] as const)(
    "hands a server-queued %s send to the queue without retaining a duplicate bubble",
    (inputIntent) => {
      const store = createPendingSendStore();
      store.admit(env, thread, { ...message, ...(inputIntent && { inputIntent }) });
      const accepted = vi.fn();
      store.whenAccepted(env, thread, message.id, accepted);
      store.reconcile(env, thread, projection({ run: true, status: "queued" }));
      expect(store.getSnapshot()).toMatchObject([{ delivery: "accepted" }]);
      store.reconcile(env, thread, projection({ run: true, status: "queued", message: true }));
      expect(store.getSnapshot()).toEqual([]);
      expect(accepted).toHaveBeenCalledTimes(1);
    },
  );
  it.each([undefined, "steer", "queued_turn"] as const)(
    "removes a %s placeholder cancelled after server acceptance without a timeline item",
    (inputIntent) => {
      const store = createPendingSendStore();
      store.admit(env, thread, { ...message, ...(inputIntent && { inputIntent }) });
      store.reconcile(env, thread, projection({ run: true, status: "cancelled" }));
      expect(store.getSnapshot()).toEqual([]);
    },
  );
});

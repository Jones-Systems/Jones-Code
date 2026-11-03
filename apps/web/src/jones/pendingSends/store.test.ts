import { describe, expect, it, vi } from "vite-plus/test";
import {
  EnvironmentId,
  MessageId,
  ThreadId,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import type { ChatMessage } from "../../types";
import { createPendingSendStore } from "./store";

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
    runs: input.run ? [{ userMessageId: message.id, status: input.status ?? "completed" }] : [],
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
});

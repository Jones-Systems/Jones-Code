import type {
  EnvironmentId,
  MessageId,
  OrchestrationV2ThreadProjection,
  ThreadId,
} from "@t3tools/contracts";
import type { PreparedThreadTurn } from "@t3tools/client-runtime/operations";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";
import type { ChatMessage } from "../../types";

export type Delivery = "sending" | "uncertain" | "failed" | "accepted";
export interface PendingSend {
  readonly key: string;
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly message: ChatMessage;
  readonly delivery: Delivery;
  readonly issued: boolean;
  readonly working: boolean;
  readonly prepared?: PreparedThreadTurn;
  readonly retry?: () => Promise<unknown>;
  readonly inFlight: boolean;
  readonly visible: boolean;
  readonly sidebarThread?: EnvironmentThreadShell;
  readonly onAccepted?: (() => void) | undefined;
}

export function createPendingSendStore(revoke = (url: string) => URL.revokeObjectURL(url)) {
  let entries: readonly PendingSend[] = [];
  const listeners = new Set<() => void>();
  const publish = (next: readonly PendingSend[]) => {
    if (next === entries) return;
    entries = next;
    for (const listener of listeners) listener();
  };
  const key = (environmentId: EnvironmentId, threadId: ThreadId, messageId: MessageId) =>
    JSON.stringify([environmentId, threadId, messageId]);
  const get = (environmentId: EnvironmentId, threadId: ThreadId, messageId: MessageId) =>
    entries.find((entry) => entry.key === key(environmentId, threadId, messageId));
  const update = (entryKey: string, change: (entry: PendingSend) => PendingSend) => {
    let changed = false;
    const next = entries.map((entry) => {
      if (entry.key !== entryKey) return entry;
      const value = change(entry);
      changed ||= value !== entry;
      return value;
    });
    if (changed) publish(next);
  };
  const release = (entry: PendingSend) => {
    for (const attachment of entry.message.attachments ?? []) {
      if ("previewUrl" in attachment && attachment.previewUrl?.startsWith("blob:"))
        revoke(attachment.previewUrl);
    }
  };
  const dismiss = (entryKey: string) => {
    const entry = entries.find((entry) => entry.key === entryKey);
    if (!entry) return;
    release(entry);
    publish(entries.filter((entry) => entry.key !== entryKey));
  };
  const accept = (entryKey: string) => {
    const entry = entries.find((entry) => entry.key === entryKey);
    if (!entry || (entry.delivery === "accepted" && !entry.onAccepted)) return;
    const callback = entry.onAccepted;
    update(entryKey, (entry) => ({ ...entry, delivery: "accepted", onAccepted: undefined }));
    callback?.();
  };
  return {
    subscribe: (listener: () => void) => {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    getSnapshot: () => entries,
    get,
    attachThread(
      environmentId: EnvironmentId,
      threadId: ThreadId,
      sidebarThread: EnvironmentThreadShell,
    ) {
      for (const entry of entries)
        if (
          entry.environmentId === environmentId &&
          entry.threadId === threadId &&
          !entry.sidebarThread
        )
          update(entry.key, (entry) => ({ ...entry, sidebarThread }));
    },
    admit(environmentId: EnvironmentId, threadId: ThreadId, message: ChatMessage) {
      const existing = get(environmentId, threadId, message.id);
      if (existing) return existing.key;
      const entryKey = key(environmentId, threadId, message.id);
      publish([
        ...entries,
        {
          key: entryKey,
          environmentId,
          threadId,
          message,
          delivery: "sending",
          issued: false,
          working: true,
          inFlight: false,
          visible: false,
        },
      ]);
      return entryKey;
    },
    prepared(entryKey: string, prepared: PreparedThreadTurn, retry: () => Promise<unknown>) {
      update(entryKey, (entry) => ({ ...entry, prepared, retry }));
    },
    issuing(entryKey: string) {
      update(entryKey, (entry) => ({
        ...entry,
        issued: true,
        inFlight: true,
        delivery: entry.delivery === "accepted" ? "accepted" : "sending",
      }));
    },
    finish(entryKey: string, success: boolean) {
      if (success) accept(entryKey);
      update(entryKey, (entry) => ({
        ...entry,
        inFlight: false,
        delivery:
          entry.delivery === "accepted" || success
            ? "accepted"
            : entry.issued
              ? "uncertain"
              : "failed",
        working: entry.working && (success || entry.issued),
      }));
    },
    failBeforeDispatch(environmentId: EnvironmentId, threadId: ThreadId, messageId: MessageId) {
      const entry = get(environmentId, threadId, messageId);
      if (entry && !entry.issued && entry.delivery !== "accepted")
        update(entry.key, (entry) => ({ ...entry, delivery: "failed", working: false }));
    },
    whenAccepted(
      environmentId: EnvironmentId,
      threadId: ThreadId,
      messageId: MessageId,
      callback: () => void,
    ) {
      const entry = get(environmentId, threadId, messageId);
      if (!entry) return;
      if (entry.delivery === "accepted") callback();
      else update(entry.key, (entry) => ({ ...entry, onAccepted: callback }));
    },
    dismiss,
    reconcile(
      environmentId: EnvironmentId,
      threadId: ThreadId,
      projection: OrchestrationV2ThreadProjection,
    ) {
      for (const entry of entries) {
        if (entry.environmentId !== environmentId || entry.threadId !== threadId) continue;
        const run = projection.runs.find((run) => run.userMessageId === entry.message.id);
        const visible = projection.visibleTurnItems.some(
          (row) =>
            row.sourceThreadId === threadId &&
            row.item.type === "user_message" &&
            row.item.messageId === entry.message.id,
        );
        const message = projection.messages.some((message) => message.id === entry.message.id);
        // Queue delivery has its own representation, including cancelled runs without a turn item.
        const queueHandoff =
          entry.message.inputIntent === "queued_turn" &&
          run !== undefined &&
          (message || run.status === "cancelled");
        if (visible || queueHandoff) {
          accept(entry.key);
          const hasPreview = entry.message.attachments?.some(
            (attachment) =>
              "previewUrl" in attachment && attachment.previewUrl?.startsWith("blob:"),
          );
          if (visible && hasPreview) {
            if (!entry.visible || entry.working)
              update(entry.key, (entry) => ({ ...entry, visible: true, working: false }));
          } else dismiss(entry.key);
          continue;
        }
        if ((run || message) && (entry.delivery !== "accepted" || (run && entry.working))) {
          accept(entry.key);
          update(entry.key, (entry) => ({ ...entry, working: run ? false : entry.working }));
        }
      }
    },
  };
}
export const pendingSends = createPendingSendStore();

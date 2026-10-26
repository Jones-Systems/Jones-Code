import {
  OrchestrationV2ThreadShell,
  type OrchestrationV2ShellSnapshot,
  type OrchestrationV2ShellStreamItem,
  type ThreadId,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";

const sameThread = Schema.toEquivalence(OrchestrationV2ThreadShell);

type ShellDelta = Exclude<
  OrchestrationV2ShellStreamItem,
  { readonly kind: "snapshot" } | { readonly kind: "synchronized" }
>;
type ThreadRefresh = Extract<ShellDelta, { readonly kind: "thread.refreshed" | "thread.omitted" }>;
const refreshThreadId = (item: ThreadRefresh) => item.kind === "thread.omitted" ? item.threadId : item.thread.id;

function acceptsThreadRefresh(
  snapshot: OrchestrationV2ShellSnapshot,
  item: ThreadRefresh,
  removedAt: ReadonlyMap<ThreadId, number> | undefined,
  refreshedAt: ReadonlyMap<ThreadId, number> | undefined,
): boolean {
  return (
    item.sequence >= snapshot.snapshotSequence &&
    item.sequence >= (refreshedAt?.get(refreshThreadId(item)) ?? -1) &&
    item.sequence > (removedAt?.get(refreshThreadId(item)) ?? -1)
  );
}

export function isThreadReplayBehindRefresh(
  item: ShellDelta,
  refreshedAt?: ReadonlyMap<ThreadId, number>,
): boolean {
  if (item.kind !== "thread.updated" && item.kind !== "thread.removed") return false;
  const threadId = item.kind === "thread.removed" ? item.threadId : item.thread.id;
  const sequence = refreshedAt?.get(threadId);
  // Derived refresh facts win a same-sequence row replay; removal still wins ties.
  return (
    sequence !== undefined &&
    (item.sequence < sequence ||
      (item.sequence === sequence && item.kind === "thread.updated" && item.location === "active"))
  );
}

// Only refreshes ahead of replay need a separate fence. Once replay catches up,
// its cursor rejects older items without keeping one entry per historical thread.
export function advanceThreadRefreshWatermarks(
  refreshedAt: ReadonlyMap<ThreadId, number>,
  snapshot: OrchestrationV2ShellSnapshot,
  item: ShellDelta,
  removedAt: ReadonlyMap<ThreadId, number>,
): ReadonlyMap<ThreadId, number> {
  if (item.kind === "thread.refreshed" || item.kind === "thread.omitted") {
    if (
      !acceptsThreadRefresh(snapshot, item, removedAt, refreshedAt) ||
      item.sequence <= snapshot.snapshotSequence ||
      item.sequence === refreshedAt.get(refreshThreadId(item))
    ) {
      return refreshedAt;
    }
    return new Map(refreshedAt).set(refreshThreadId(item), item.sequence);
  }
  if (item.sequence <= snapshot.snapshotSequence || refreshedAt.size === 0) return refreshedAt;
  let next: Map<ThreadId, number> | undefined;
  for (const [threadId, sequence] of refreshedAt) {
    if (sequence > item.sequence) continue;
    next ??= new Map(refreshedAt);
    next.delete(threadId);
  }
  return next ?? refreshedAt;
}

export function applyThreadRefresh(
  snapshot: OrchestrationV2ShellSnapshot,
  item: ThreadRefresh,
  removedAt?: ReadonlyMap<ThreadId, number>,
  refreshedAt?: ReadonlyMap<ThreadId, number>,
): OrchestrationV2ShellSnapshot {
  if (!acceptsThreadRefresh(snapshot, item, removedAt, refreshedAt)) {
    return snapshot;
  }

  if (item.kind === "thread.omitted") {
    if (!snapshot.threads.some((thread) => thread.id === item.threadId)) return snapshot;
    // A filtered row may reappear at the same application sequence. Do not record
    // a deletion tombstone: on-demand family data still has its own authority.
    return { ...snapshot, threads: snapshot.threads.filter((thread) => thread.id !== item.threadId) };
  }

  const existing = snapshot.threads.find((thread) => thread.id === item.thread.id);
  if (existing !== undefined && sameThread(existing, item.thread)) return snapshot;
  // A refresh carries derived facts, not a replay cursor. Advancing here could skip
  // application events that have not reached the client yet.
  return {
    ...snapshot,
    threads:
      existing === undefined
        ? [...snapshot.threads, item.thread]
        : snapshot.threads.map((thread) => (thread.id === item.thread.id ? item.thread : thread)),
    archivedThreads: snapshot.archivedThreads.filter((thread) => thread.id !== item.thread.id),
  };
}

export function shellRefreshSubscription(config: { readonly shellThreadRefresh?: boolean | undefined }) {
  return config.shellThreadRefresh === true ? { acceptThreadRefresh: true as const } : {};
}

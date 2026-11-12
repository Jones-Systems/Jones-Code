import type {
  OrchestrationV2ShellSnapshot,
  OrchestrationV2ShellStreamItem,
  ThreadId,
} from "@t3tools/contracts";

/** Current active rows clear stale family copies even when subscriber filtering omits them. */
export function refreshItems(
  snapshot: OrchestrationV2ShellSnapshot,
  threadIds: ReadonlyArray<ThreadId>,
  currentThreads: OrchestrationV2ShellSnapshot["threads"] = snapshot.threads,
): ReadonlyArray<
  Extract<OrchestrationV2ShellStreamItem, { kind: "thread.refreshed" | "thread.omitted" }>
> {
  const visible = new Map(
    currentThreads
      .filter((thread) => thread.archivedAt === null && thread.deletedAt === null)
      .map((thread) => [thread.id, thread]),
  );
  return [...new Set(threadIds)].map((threadId) => {
    const thread = visible.get(threadId);
    // Absence is a subscriber projection fact, not a persisted deletion or replay cursor.
    return thread === undefined
      ? {
          kind: "thread.omitted",
          sequence: snapshot.snapshotSequence,
          location: "active",
          threadId,
        }
      : {
          kind: "thread.refreshed",
          sequence: snapshot.snapshotSequence,
          location: "active",
          thread,
        };
  });
}

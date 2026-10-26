import type { OrchestrationV2ShellSnapshot, OrchestrationV2ShellStreamItem, ThreadId } from "@t3tools/contracts";

/** Snapshot is already filtered using the subscriber's family and older-settled negotiation. */
export function refreshItems(snapshot: OrchestrationV2ShellSnapshot, threadIds: ReadonlyArray<ThreadId>): ReadonlyArray<Extract<OrchestrationV2ShellStreamItem, { kind: "thread.refreshed" | "thread.omitted" }>> {
  const visible = new Map(snapshot.threads.filter((thread) => thread.archivedAt === null && thread.deletedAt === null).map((thread) => [thread.id, thread]));
  return [...new Set(threadIds)].map((threadId) => {
    const thread = visible.get(threadId);
    // Absence is a subscriber projection fact, not a persisted deletion or replay cursor.
    return thread === undefined
      ? { kind: "thread.omitted", sequence: snapshot.snapshotSequence, location: "active", threadId }
      : { kind: "thread.refreshed", sequence: snapshot.snapshotSequence, location: "active", thread };
  });
}

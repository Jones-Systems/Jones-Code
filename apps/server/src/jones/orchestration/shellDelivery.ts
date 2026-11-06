import type { OrchestrationV2ShellStreamItem, ThreadId } from "@t3tools/contracts";

/** Keep row freshness separate from the application replay cursor. */
export function createShellDeliveryGuard() {
  const materializedAt = new WeakMap<OrchestrationV2ShellStreamItem, number>();
  const deliveredAt = new Map<ThreadId, number>();
  return {
    record<A extends OrchestrationV2ShellStreamItem>(item: A, version: number): A {
      materializedAt.set(item, version);
      return item;
    },
    accepts(item: OrchestrationV2ShellStreamItem): boolean {
      if (item.kind === "snapshot") {
        for (const thread of [...item.snapshot.threads, ...item.snapshot.archivedThreads]) {
          deliveredAt.set(
            thread.id,
            Math.max(deliveredAt.get(thread.id) ?? -1, item.snapshot.snapshotSequence),
          );
        }
        return true;
      }
      if (
        item.kind !== "thread.updated" &&
        item.kind !== "thread.refreshed" &&
        item.kind !== "thread.removed" &&
        item.kind !== "thread.omitted"
      ) {
        return true;
      }
      const threadId =
        item.kind === "thread.removed" || item.kind === "thread.omitted"
          ? item.threadId
          : item.thread.id;
      const version = materializedAt.get(item) ?? item.sequence;
      if (version < (deliveredAt.get(threadId) ?? -1)) return false;
      // Equal application sequences can carry new derived dependency facts.
      deliveredAt.set(threadId, version);
      return true;
    },
  };
}

import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";
import { usePendingSends } from "./hooks";
import type { PendingSend } from "./store";

export function overlayPendingThread(
  thread: EnvironmentThreadShell,
  entries: readonly PendingSend[],
): EnvironmentThreadShell {
  const entry = entries.findLast(
    (entry) =>
      entry.environmentId === thread.environmentId && entry.threadId === thread.id && entry.working,
  );
  if (!entry) return thread;
  return {
    ...thread,
    latestUserMessageAt: entry.message.createdAt,
    blockedOnThreadWait: false,
    runtime: {
      ...(thread.runtime ?? {
        activeRunId: null,
        providerInstanceId: thread.modelSelection.instanceId,
        providerName: null,
        lastError: null,
        updatedAt: entry.message.createdAt,
      }),
      status: "running",
      activityStartedAt: entry.message.createdAt,
    },
  };
}
export function mergePendingSidebarThreads(
  threads: readonly EnvironmentThreadShell[],
  entries: readonly PendingSend[],
) {
  const byKey = new Map(
    threads.map((thread) => [JSON.stringify([thread.environmentId, thread.id]), thread]),
  );
  for (const entry of entries) {
    const key = JSON.stringify([entry.environmentId, entry.threadId]);
    if (entry.sidebarThread && !byKey.has(key)) byKey.set(key, entry.sidebarThread);
  }
  return [...byKey.values()].map((thread) => overlayPendingThread(thread, entries));
}
export function usePendingSidebarThreads(threads: readonly EnvironmentThreadShell[]) {
  return mergePendingSidebarThreads(threads, usePendingSends());
}
export function usePendingSidebarThread(thread: EnvironmentThreadShell) {
  return overlayPendingThread(thread, usePendingSends());
}

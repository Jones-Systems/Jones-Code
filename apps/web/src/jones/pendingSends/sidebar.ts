import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";
import { useSyncExternalStore } from "react";
import { pendingSends, type PendingSend } from "./store";

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
interface PendingThreadSnapshot {
  readonly base: EnvironmentThreadShell;
  readonly createdAt: string | undefined;
  readonly value: EnvironmentThreadShell;
}

function projectPendingThread(
  base: EnvironmentThreadShell,
  entry: PendingSend | undefined,
  previous: PendingThreadSnapshot | undefined,
): PendingThreadSnapshot {
  const createdAt = entry?.message.createdAt;
  if (previous?.base === base && previous.createdAt === createdAt) return previous;
  return {
    base,
    createdAt,
    value: overlayPendingThread(base, entry ? [entry] : []),
  };
}

function createThreadSnapshot(thread: EnvironmentThreadShell) {
  let entries: readonly PendingSend[] | undefined;
  let snapshot = projectPendingThread(thread, undefined, undefined);
  return () => {
    const next = pendingSends.getSnapshot();
    if (entries !== next) {
      const entry = next.findLast(
        (entry) =>
          entry.environmentId === thread.environmentId &&
          entry.threadId === thread.id &&
          entry.working,
      );
      snapshot = projectPendingThread(thread, entry, snapshot);
      entries = next;
    }
    return snapshot.value;
  };
}

function createSidebarSnapshot(threads: readonly EnvironmentThreadShell[]) {
  let entries: readonly PendingSend[] | undefined;
  let rows = new Map<string, PendingThreadSnapshot>();
  let snapshot: EnvironmentThreadShell[] = [];
  return () => {
    const next = pendingSends.getSnapshot();
    if (entries === next) return snapshot;
    const byKey = new Map(
      threads.map((thread) => [JSON.stringify([thread.environmentId, thread.id]), thread]),
    );
    const working = new Map<string, PendingSend>();
    for (const entry of next) {
      const key = JSON.stringify([entry.environmentId, entry.threadId]);
      if (entry.sidebarThread && !byKey.has(key)) byKey.set(key, entry.sidebarThread);
      // Overwrite in store order: the last working entry wins, even at equal timestamps.
      if (entry.working) working.set(key, entry);
    }
    const nextRows = new Map<string, PendingThreadSnapshot>();
    const values: EnvironmentThreadShell[] = [];
    for (const [key, thread] of byKey) {
      const row = projectPendingThread(thread, working.get(key), rows.get(key));
      nextRows.set(key, row);
      values.push(row.value);
    }
    if (
      values.length !== snapshot.length ||
      values.some((thread, index) => thread !== snapshot[index])
    ) {
      snapshot = values;
    }
    rows = nextRows;
    entries = next;
    return snapshot;
  };
}

export function usePendingSidebarThreads(threads: readonly EnvironmentThreadShell[]) {
  const getSnapshot = createSidebarSnapshot(threads);
  return useSyncExternalStore(pendingSends.subscribe, getSnapshot);
}
export function usePendingSidebarThread(thread: EnvironmentThreadShell) {
  const getSnapshot = createThreadSnapshot(thread);
  return useSyncExternalStore(pendingSends.subscribe, getSnapshot);
}

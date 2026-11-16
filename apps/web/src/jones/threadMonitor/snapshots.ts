import {
  DESKTOP_THREAD_MONITOR_PROTOCOL,
  THREAD_MONITOR_LIMITS,
  threadMonitorFrameByteLength,
  type ThreadMonitorViewChunk,
  type ThreadMonitorEnvironment,
  type ThreadMonitorRow,
  type ThreadMonitorCounts,
} from "@t3tools/contracts/jones/desktopThreadMonitor";

export function monitorSnapshot(input: {
  publisherGeneration: string;
  revision: number;
  environments: ReadonlyArray<ThreadMonitorEnvironment>;
  rows: ReadonlyArray<ThreadMonitorRow>;
  counts: ThreadMonitorCounts;
}): ReadonlyArray<ThreadMonitorViewChunk> {
  const snapshotId = `${input.publisherGeneration}:${input.revision}`;
  const base = {
    type: "view" as const,
    publisherGeneration: input.publisherGeneration,
    revision: input.revision,
    snapshotId,
    replace: true,
    counts: input.counts,
  };
  const chunks: Array<{
    environments: ThreadMonitorEnvironment[];
    upserts: ThreadMonitorRow[];
    removed: [];
  }> = [];
  let part: (typeof chunks)[number] = { environments: [], upserts: [], removed: [] };
  const fits = (candidate: typeof part) =>
    candidate.environments.length <= 64 &&
    candidate.upserts.length <= THREAD_MONITOR_LIMITS.rowUpserts &&
    threadMonitorFrameByteLength({
      schemaVersion: DESKTOP_THREAD_MONITOR_PROTOCOL,
      payload: { ...base, ...candidate, chunkIndex: 999999, chunkCount: 999999, complete: false },
    }) <= THREAD_MONITOR_LIMITS.frameBytes;
  for (const environment of input.environments) {
    const next = { ...part, environments: [...part.environments, environment] };
    if (!fits(next)) {
      chunks.push(part);
      part = { environments: [environment], upserts: [], removed: [] };
    } else part = next;
  }
  for (const row of input.rows) {
    const next = { ...part, upserts: [...part.upserts, row] };
    if (!fits(next)) {
      chunks.push(part);
      part = { environments: [], upserts: [row], removed: [] };
    } else part = next;
  }
  chunks.push(part);
  return chunks.map((chunk, chunkIndex) => ({
    ...base,
    ...chunk,
    chunkIndex,
    chunkCount: chunks.length,
    complete: chunkIndex === chunks.length - 1,
  }));
}

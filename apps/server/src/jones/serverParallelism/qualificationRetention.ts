import { LIMITS } from "../serverCompute/protocol.ts";

export function qualificationWorkload(input: {
  readonly count: 2 | 3;
  readonly windows?: number;
  readonly acquisitionsPerWindow?: number;
  readonly filesPerKey?: number;
}) {
  const count = input.count;
  const windows = input.windows ?? 3;
  const acquisitions = input.acquisitionsPerWindow ?? 500;
  const files = input.filesPerKey ?? 512;
  if (
    ![windows, acquisitions, files].every(Number.isSafeInteger) ||
    ![2, 3].includes(count) ||
    windows < 3 ||
    windows > 5 ||
    acquisitions < 500 ||
    acquisitions > 2_000 ||
    files < 128 ||
    files > 2_048
  )
    throw new Error("Qualification workload outside bounded representative envelope");
  const workloadMs = 600_000;
  // This bounds the wire grace, not journal I/O. A longer run is incomplete.
  const shutdownMs = LIMITS.destroyMs + count * (100 + LIMITS.reapMs);
  const retentionMs = workloadMs + shutdownMs;
  const churnSets = [4 * count + 1, 8 * count] as const;
  const churnVisits = 3 * (churnSets[0] + churnSets[1]);
  const hotVisits = 6 * count;
  const opens = 4 * count + churnVisits + hotVisits;
  const dataJobs = 2 * count * acquisitions * (windows + 1) + 3 * churnVisits + 2 * hotVisits;
  const basisGreps = opens;
  const closes = 4 * count;
  const registrations = 8 * count;
  const maintenancePumps = Math.ceil(retentionMs / 250) + 1;
  const maintenanceStatus = count * maintenancePumps;
  // A fresh generation can emit on either side of a cadence boundary.
  const guardianMessages = count * (Math.ceil(retentionMs / 1_000) + 1) + opens;
  const generationTransitions = 2 * opens;
  // One first-grep checkpoint per generation, plus stable phase boundaries.
  const basisCheckpoints = opens + count * (2 * windows + 10);
  const terms = {
    dataJobs,
    basisGreps,
    opens,
    closes,
    registrations,
    maintenancePumps,
    maintenanceStatus,
    guardianMessages,
    generationTransitions,
    basisCheckpoints,
  };
  // Emitter audit: supervisor admission/queue/job + request's round-trip/four
  // timing rows + count/pump/finalizer rows fit 32 per job. Implicit opens,
  // registration, shutdown closes, startup/retirement, maintenance pump/status
  // and ProcessSlot guardian messages have separate allowances below. This is
  // a successful-workload envelope, not permission to forgive faults/extra work.
  const observationUnits =
    dataJobs +
    basisGreps +
    opens +
    closes +
    registrations +
    maintenancePumps +
    maintenanceStatus +
    guardianMessages +
    generationTransitions;
  return {
    count,
    windows,
    acquisitions,
    files,
    workloadMs,
    shutdownMs,
    retentionMs,
    churnSets,
    terms,
    capacities: {
      observations: 32 * observationUnits,
      executions: dataJobs + basisGreps,
      // Cadence plus initial/in-flight/final rows and explicit checkpoint reads.
      resources: Math.ceil(retentionMs / 10) + 4 + 2 * basisCheckpoints,
    },
  };
}

type QualificationWorkload = ReturnType<typeof qualificationWorkload>;

export function qualificationRetentionComplete(input: {
  readonly elapsedMs: number;
  readonly workload: Pick<QualificationWorkload, "retentionMs">;
  readonly observations: { readonly overflow: boolean };
  readonly executions: { readonly overflow: boolean };
  readonly resources: { readonly overflow: boolean };
}) {
  return (
    Number.isFinite(input.elapsedMs) &&
    input.elapsedMs >= 0 &&
    input.elapsedMs <= input.workload.retentionMs &&
    !input.observations.overflow &&
    !input.executions.overflow &&
    !input.resources.overflow
  );
}

/** Ordered, on-demand retention. Once lost, evidence never becomes complete again. */
export function qualificationStream<T>(capacity: number) {
  if (!Number.isSafeInteger(capacity) || capacity < 1) throw new Error("Invalid stream capacity");
  const rows: T[] = [];
  let attempted = 0;
  let overflow = false;
  return {
    rows,
    append(row: T) {
      attempted++;
      if (rows.length < capacity) rows.push(row);
      else overflow = true;
    },
    report: () => ({
      capacity,
      attempted,
      retained: rows.length,
      dropped: attempted - rows.length,
      overflow,
    }),
  };
}

import type { Bootstrap, Request } from "../serverCompute/protocol.ts";
import type { ComputeRequestTiming } from "./observations.ts";
import type { SqliteReaderObservation } from "@t3tools/shared/jones/sqliteReaderContracts";

export interface DiagnosticIdentity {
  readonly pid?: number | undefined;
  readonly bootstrap?: Bootstrap | undefined;
  readonly requestId?: number | undefined;
  readonly operation?: Request["operation"] | undefined;
  readonly token?: Request["token"] | undefined;
  readonly role?: "path" | "positive-grep" | "absent-grep" | "other";
}
export interface DiagnosticCpu {
  readonly process: { readonly user: number; readonly system: number };
  readonly thread: { readonly user: number; readonly system: number };
}
export type ResourceStage =
  | "batch"
  | "clock-ticks"
  | "pid"
  | "stat"
  | "status"
  | "task"
  | "fd"
  | "fdinfo";
export type DiagnosticEvent = DiagnosticIdentity & {
  readonly atMs: number;
  readonly phase: string;
} & (
    | {
        readonly kind: "resource";
        readonly readId: number;
        readonly stage: ResourceStage;
        readonly edge: "begin" | "end" | "error";
        readonly attempted?: number | undefined;
        readonly completed?: number | undefined;
        readonly errors?: number | undefined;
        readonly errno?: string | undefined;
        readonly raw?: string | undefined;
        readonly startTimeTicks?: string | undefined;
        readonly clockTicks?: number | undefined;
        readonly cpu?: DiagnosticCpu | undefined;
      }
    | {
        readonly kind: "lifecycle";
        readonly event:
          | "spawn"
          | "ready"
          | "register-complete"
          | "go"
          | "nativeReady"
          | "loaded"
          | "terminate-request"
          | "captured-exit"
          | "captured-close"
          | "parent-send"
          | "terminal-receive"
          | "disarm"
          | "settle";
        readonly code?: number | null;
        readonly signal?: string | null;
        readonly timing?: ComputeRequestTiming | undefined;
      }
    | {
        readonly kind: "qualification";
        readonly event:
          | "phase-begin"
          | "phase-end"
          | "checkpoint-begin"
          | "checkpoint-end"
          | "reserve-begin"
          | "reserve-end"
          | "reserve-error"
          | "register-begin"
          | "register-end"
          | "register-error"
          | "retired-begin"
          | "retired-end"
          | "retired-error"
          | "retire-begin"
          | "retire-resolved"
          | "retire-error"
          | "request-begin"
          | "request-end"
          | "request-error"
          | "request-timing"
          | "loop-window-begin"
          | "loop-window-end";
        readonly timing?: ComputeRequestTiming | undefined;
        readonly cpu?: DiagnosticCpu | undefined;
        readonly loop?: { readonly p99Ms: number; readonly maxMs: number };
        readonly raw?: string | undefined;
      }
    | {
        readonly kind: "combined";
        readonly event:
          | "window-begin"
          | "window-end"
          | "database-begin"
          | "database-ready"
          | "sql-load-begin"
          | "sql-load-end"
          | "compute-begin"
          | "compute-end"
          | "reader-begin"
          | "reader-end"
          | "reader-observation"
          | "writer-begin"
          | "writer-end"
          | "control-begin"
          | "control-end";
        readonly index?: number | undefined;
        readonly observation?: SqliteReaderObservation | undefined;
        readonly cpu?: DiagnosticCpu | undefined;
      }
  );
export type DiagnosticSink = (event: DiagnosticEvent) => void;

/** Retention loss is sticky; a full sink never changes the measured operation. */
export function diagnosticTrace(capacity: number) {
  if (!Number.isSafeInteger(capacity) || capacity < 1 || capacity > 1_000_000)
    throw new Error("Invalid diagnostic trace capacity");
  const rows: DiagnosticEvent[] = [];
  let attempted = 0,
    overflow = false;
  return {
    get rows(): readonly DiagnosticEvent[] {
      return rows;
    },
    append(event: DiagnosticEvent): void {
      attempted++;
      if (rows.length < capacity) rows.push(event);
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

export function diagnosticError(cause: unknown) {
  try {
    const errno =
      typeof cause === "object" && cause !== null && "code" in cause
        ? String(cause.code).slice(0, 64)
        : "unknown";
    return { errno, raw: String(cause).slice(0, 2_048) };
  } catch {
    return { errno: "unknown", raw: "unprintable-diagnostic-error" };
  }
}

/** These are recorded timeline relations, never kernel liveness or charge-release proof. */
export function classifyResourceInterval(input: {
  readonly startMs: number;
  readonly endMs: number;
  readonly available: boolean;
  readonly startTimeTicks?: string | undefined;
  readonly expectedStartTimeTicks?: string | undefined;
  readonly retirementStartedMs?: number | undefined;
  readonly capturedExitMs?: number | undefined;
  readonly capturedCloseMs?: number | undefined;
  readonly retireResolvedMs?: number | undefined;
  readonly journalRetiredMs?: number | undefined;
}) {
  let relation:
    | "invalid-time"
    | "pid-identity-mismatch"
    | "crosses-captured-exit"
    | "after-captured-exit"
    | "retirement-adjacent"
    | "before-recorded-retirement"
    | "no-retirement-recorded";
  if (![input.startMs, input.endMs].every(Number.isFinite) || input.endMs < input.startMs)
    relation = "invalid-time";
  else if (
    input.startTimeTicks !== undefined &&
    input.expectedStartTimeTicks !== undefined &&
    input.startTimeTicks !== input.expectedStartTimeTicks
  )
    relation = "pid-identity-mismatch";
  else if (input.capturedExitMs !== undefined && input.startMs >= input.capturedExitMs)
    relation = "after-captured-exit";
  else if (input.capturedExitMs !== undefined && input.endMs >= input.capturedExitMs)
    relation = "crosses-captured-exit";
  else if (input.retirementStartedMs !== undefined && input.endMs >= input.retirementStartedMs)
    relation = "retirement-adjacent";
  else
    relation =
      input.retirementStartedMs === undefined
        ? "no-retirement-recorded"
        : "before-recorded-retirement";
  return {
    relation,
    available: input.available,
    kernelLiveness: "unknown" as const,
    releaseCharge: false as const,
  };
}

export interface DiagnosticCpuPoint {
  readonly pid: number;
  readonly generation: number;
  readonly nonce: string;
  readonly startTimeTicks: string;
  readonly atMs: number;
  readonly finishedAtMs: number;
  readonly cpuMs: number;
}
export interface DiagnosticExecution {
  readonly pid: number;
  readonly generation: number;
  readonly nonce: string;
  readonly requestId: number;
  readonly startMs: number;
  readonly endMs: number;
}
/** One whole sample envelope must fit one request; CPU rises cannot join disjoint jobs. */
export function classifyCpuEnvelope(
  before: DiagnosticCpuPoint,
  after: DiagnosticCpuPoint,
  executions: readonly DiagnosticExecution[],
) {
  if (
    before.pid !== after.pid ||
    before.generation !== after.generation ||
    before.nonce !== after.nonce ||
    before.startTimeTicks !== after.startTimeTicks
  )
    return { relation: "identity-mismatch" as const, requestId: null };
  if (
    ![
      before.atMs,
      before.finishedAtMs,
      after.atMs,
      after.finishedAtMs,
      before.cpuMs,
      after.cpuMs,
    ].every(Number.isFinite) ||
    before.atMs > before.finishedAtMs ||
    before.finishedAtMs > after.atMs ||
    after.atMs > after.finishedAtMs
  )
    return { relation: "invalid-time" as const, requestId: null };
  if (after.cpuMs <= before.cpuMs) return { relation: "no-positive-cpu" as const, requestId: null };
  const request = executions.find(
    (item) =>
      item.pid === after.pid &&
      item.generation === after.generation &&
      item.nonce === after.nonce &&
      Number.isFinite(item.startMs) &&
      Number.isFinite(item.endMs) &&
      item.startMs < item.endMs &&
      item.startMs <= before.atMs &&
      item.endMs >= after.finishedAtMs,
  );
  return {
    relation: request ? ("contained" as const) : ("uncontained" as const),
    requestId: request?.requestId ?? null,
  };
}

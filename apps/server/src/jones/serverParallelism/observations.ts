import type { SqliteReaderObservation } from "@t3tools/shared/jones/sqliteReaderContracts";
import type { WorkspaceJobs } from "./computeContract.ts";
import type { ReplyTiming, estimateReplyDelivery } from "../serverCompute/protocol.ts";

type ObservationOutcome = SqliteReaderObservation["outcome"];
type ObservationTiming = {
  readonly slotId: number;
  readonly generation: number;
  /** null is unavailable, never a zero-duration measurement. */
  readonly durationMs: number | null;
  readonly queueDepth: number;
  readonly outcome: ObservationOutcome;
  readonly estimated?: boolean;
  readonly uncertaintyMs?: number;
  readonly lowerMs?: number;
  readonly upperMs?: number;
  readonly offsetLowerMs?: number;
  readonly offsetUpperMs?: number;
  readonly workerStartedMs?: number;
  readonly workerCompletedMs?: number;
};

export interface ComputeRequestTiming {
  readonly worker?: ReplyTiming;
  readonly delivery?: ReturnType<typeof estimateReplyDelivery>;
  readonly parentContinuationMs?: number;
}
export type ComputeObservation = (ObservationTiming &
  (
    | {
        readonly phase: "admission" | "queue" | "startup" | "execute" | "reply" | "reply_delivery" | "parent_continuation" | "request_round_trip" | "job" | "lifecycle";
        readonly operation: keyof WorkspaceJobs | "open" | "close" | "status";
      }
    | {
        readonly phase: "lifecycle";
        readonly operation: "pool";
        readonly counts: {
          readonly logicalHandles: number;
          readonly physicalEntries: number;
          /** Foreground open/refresh calls only; public readiness does not observe native completion. */
          readonly activeForegroundOpenRefresh: number;
          readonly chargedAllocations: number;
          /** Retiring or unresolved slots, including those awaiting durable retirement after exit. */
          readonly retiringGenerations: number;
          /** Incomplete readiness samples; zero never proves native quiescence. */
          readonly observedNativeScanning: number;
          readonly observedNativeWarmup: number;
        };
      }
  )) | {
    readonly phase: "resources";
    readonly operation: "pool";
    readonly slotId: number;
    readonly generation: number;
    readonly outcome: ObservationOutcome;
    readonly rssBytes: number;
  };

/** Optional child telemetry can explain a request, but cannot determine its outcome or release ownership. */
export function observeRequestTiming(observe: ((event: ComputeObservation) => void) | undefined,
  base: { slotId: number; generation: number; queueDepth: number; operation: keyof WorkspaceJobs | "open" | "close" | "status";
    outcome: ObservationOutcome }, timing?: ComputeRequestTiming): void {
  const emit = observeSafely(observe), worker = timing?.worker;
  emit({ ...base, phase: "execute", durationMs: worker ? worker.executionCompletedMs - worker.executionStartedMs : null,
    ...(worker ? { outcome: worker.executionOutcome, workerStartedMs: worker.executionStartedMs,
      workerCompletedMs: worker.executionCompletedMs } : {}) });
  emit({ ...base, phase: "reply", durationMs: worker ? worker.handoffMs - worker.replyStartedMs : null });
  emit({ ...base, phase: "reply_delivery", durationMs: timing?.delivery?.durationMs ?? null, estimated: true,
    ...(timing?.delivery ?? {}) });
  emit({ ...base, phase: "parent_continuation", durationMs: timing?.parentContinuationMs ?? null });
}

/** Instrumentation cannot interrupt a lease release or worker retirement. */
export const observeSafely =
  <A>(sink: ((event: A) => void) | undefined) =>
  (event: A): void => {
    try {
      sink?.(event);
    } catch {
      // Metrics delivery is best effort; resource ownership is not.
    }
  };

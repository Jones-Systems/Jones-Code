import type { SqliteReaderObservation } from "@t3tools/shared/jones/sqliteReaderContracts";
import type { WorkspaceJobs } from "./computeContract.ts";

type ObservationOutcome = SqliteReaderObservation["outcome"];
type ObservationTiming = {
  readonly slotId: number;
  readonly generation: number;
  readonly durationMs: number;
  readonly queueDepth: number;
  readonly outcome: ObservationOutcome;
  readonly estimated?: boolean;
  readonly uncertaintyMs?: number;
};

export type ComputeObservation = ObservationTiming &
  (
    | {
        readonly phase: "queue" | "startup" | "execute" | "reply" | "job" | "lifecycle";
        readonly operation: keyof WorkspaceJobs | "open" | "close";
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
  );

/** Instrumentation cannot interrupt a lease release or worker retirement. */
export const observeSafely = <A>(sink: ((event: A) => void) | undefined) =>
  (event: A): void => {
    try {
      sink?.(event);
    } catch {
      // Metrics delivery is best effort; resource ownership is not.
    }
  };

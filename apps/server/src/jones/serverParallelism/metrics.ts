import type { SqliteReaderObservation } from "@t3tools/shared/jones/sqliteReaderContracts";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Metric from "effect/Metric";
import { metricAttributes } from "../../observability/Metrics.ts";
import type { ComputeObservation } from "./observations.ts";
import { observeSafely } from "./observations.ts";
import type { ServerParallelismProfile } from "./config.ts";

const phaseDuration = Metric.timer("jones_server_pool_phase_duration", {
  description: "Pool phase duration. Estimated reply delivery carries clock uncertainty; not exact transfer time.",
});
const queueDepth = Metric.gauge("jones_server_pool_queue_depth");
const generation = Metric.gauge("jones_server_pool_generation");
const replyUncertainty = Metric.gauge("jones_server_pool_reply_uncertainty_ms");
const residency = Metric.gauge("jones_server_compute_residency");
const rss = Metric.gauge("jones_server_compute_resident_bytes", { attributes: { unit: "By" } });
const timingAvailable = Metric.gauge("jones_server_pool_timing_available");
const replyBound = Metric.gauge("jones_server_pool_reply_bound_ms");
const countNames = ["logicalHandles", "physicalEntries", "activeForegroundOpenRefresh",
  "chargedAllocations", "retiringGenerations", "observedNativeScanning", "observedNativeWarmup"] as const;

/** Synchronous callback adapter using the application's existing metric context.
 * No queue/fiber per observation, retained events, paths, queries or unbounded labels.
 */
export const makePoolObservers = Effect.fn("ServerParallelism.makePoolObservers")(function* (
  profile: ServerParallelismProfile,
) {
  const context = yield* Effect.context<never>();
  const record = (pool: "reader" | "compute", event: SqliteReaderObservation | ComputeObservation) => {
    const slotLabels = metricAttributes({ profile, pool, slot: event.slotId });
    Metric.withAttributes(generation, slotLabels).updateUnsafe(event.generation, context);
    if (event.phase === "resources") {
      Metric.withAttributes(rss, slotLabels).updateUnsafe(event.rssBytes, context);
      return;
    }
    const labels = metricAttributes({ profile, pool, phase: event.phase, slot: event.slotId,
      outcome: event.outcome,
      priority: "priority" in event ? event.priority : undefined,
      operation: "operation" in event ? event.operation : undefined,
      estimated: event.estimated ?? false });
    Metric.withAttributes(timingAvailable, labels).updateUnsafe(event.durationMs === null ? 0 : 1, context);
    if (event.durationMs !== null) {
      Metric.withAttributes(phaseDuration, labels).updateUnsafe(Duration.millis(event.durationMs), context);
    }
    Metric.withAttributes(queueDepth, labels).updateUnsafe(event.queueDepth, context);
    if (event.durationMs !== null && event.uncertaintyMs !== undefined) {
      Metric.withAttributes(replyUncertainty, labels).updateUnsafe(event.uncertaintyMs, context);
    }
    if ("lowerMs" in event && event.lowerMs !== undefined && event.upperMs !== undefined) {
      Metric.withAttributes(replyBound, [...labels, ["bound", "lower"]]).updateUnsafe(event.lowerMs, context);
      Metric.withAttributes(replyBound, [...labels, ["bound", "upper"]]).updateUnsafe(event.upperMs, context);
    }
    if ("counts" in event) {
      // Readiness gauges are incomplete samples, never full native-tail counts.
      for (const name of countNames) {
        Metric.withAttributes(residency, metricAttributes({ profile, name }))
          .updateUnsafe(event.counts[name], context);
      }
    }
  };
  return {
    reader: observeSafely((event: SqliteReaderObservation) => record("reader", event)),
    compute: observeSafely((event: ComputeObservation) => record("compute", event)),
  };
});

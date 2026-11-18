// @effect-diagnostics nodeBuiltinImport:off -- Synthetic snapshot codec qualification at an explicit runtime boundary.
import * as NodeZlib from "node:zlib";
import * as NodePerfHooks from "node:perf_hooks";
import * as NodeTimers from "node:timers";
import * as NodeTimersPromises from "node:timers/promises";
import { ThreadId, OrchestrationV2ShellSnapshotJson } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Fixture from "../threadResultDependencies/testkit.ts";

/** Snapshot offload is not enabled here. This measures the real capture/decode
 * and existing wire schema before root decides whether to activate that slice.
 */
const compiledEncodeEffect1 = Schema.encodeEffect(OrchestrationV2ShellSnapshotJson);
const loopResolutionMs = 10;
const maxPhaseLoopTicks = 4_096;
const waitForLoopSamples = Effect.promise(() =>
  NodeTimersPromises.setTimeout(3 * loopResolutionMs),
);

function startPhaseLoopTrace() {
  // Observe Node's timer turns directly, independently of the measured Effect scheduler.
  const trace = { ticksMs: [] as number[], overflow: false };
  // @effect-diagnostics-next-line globalTimers:off -- Measure the native event loop independently of Effect scheduling.
  const timer = NodeTimers.setInterval(() => {
    if (trace.ticksMs.length < maxPhaseLoopTicks) trace.ticksMs.push(performance.now());
    else trace.overflow = true;
  }, loopResolutionMs);
  return { trace, stop: () => NodeTimers.clearInterval(timer) };
}

export async function measureSnapshotPipeline(threadCount = 2_000) {
  if (!Number.isSafeInteger(threadCount) || threadCount < 100 || threadCount > 10_000)
    throw new Error("Bounded snapshot fixture requires 100..10000 threads");
  return Effect.runPromise(
    Effect.gen(function* () {
      const fixture = yield* Fixture.seed;
      for (let first = 0; first < threadCount; first += 50) {
        yield* fixture.sink.write({
          events: Array.from({ length: Math.min(50, threadCount - first) }, (_, offset) => {
            const id = ThreadId.make(`snapshot-fixture-${first + offset}`);
            return fixture.event({
              type: "thread.created",
              threadId: id,
              payload: {
                ...fixture.waiter,
                id,
                title: `Synthetic thread ${first + offset}`,
                lineage: { ...fixture.waiter.lineage, rootThreadId: id },
              },
            });
          }),
        });
      }
      const samples = [];
      for (let window = 0; window < 3; window++) {
        const loop = NodePerfHooks.monitorEventLoopDelay({ resolution: loopResolutionMs });
        loop.enable();
        const phaseLoop = startPhaseLoopTrace();
        try {
          // Arm the timer before the work. A new monitor can miss an entire synchronous block.
          yield* waitForLoopSamples;
          // Resetting the histogram also loses the timer's preceding timestamp on Node.
          const loopWarmupSampleCount = loop.count;
          const capturedAt = performance.now();
          const decode = yield* fixture.sql.withTransaction(fixture.projection.readShellSnapshot());
          const captureFinishedAt = performance.now();
          const decodedAt = performance.now(),
            shell = yield* decode;
          const decodeFinishedAt = performance.now();
          const projectedAt = performance.now();
          const projected = yield* compiledEncodeEffect1({
            ...shell,
            projects: [],
          });
          const projectionFinishedAt = performance.now();
          const encodedAt = performance.now();
          // @effect-diagnostics-next-line preferSchemaOverJson:off -- Measure the actual synchronous wire stringify separately from schema projection.
          const bytes = JSON.stringify(projected);
          const encodeMs = performance.now() - encodedAt;
          const compressedAt = performance.now(),
            compressed = NodeZlib.gzipSync(bytes);
          const compressionMs = performance.now() - compressedAt;
          // A later tick must close the interval that includes the final synchronous phase.
          const loopWorkEndSampleCount = loop.count;
          yield* waitForLoopSamples;
          const loopFinalSampleCount = loop.count;
          const loopSampleCount = loopFinalSampleCount - loopWarmupSampleCount;
          samples.push({
            captureMs: captureFinishedAt - capturedAt,
            decodeMs: decodeFinishedAt - decodedAt,
            schemaProjectionMs: projectionFinishedAt - projectedAt,
            phaseLoopTrace: {
              ...phaseLoop.trace,
              capture: { startMs: capturedAt, endMs: captureFinishedAt },
              decode: { startMs: decodedAt, endMs: decodeFinishedAt },
              schemaProjection: { startMs: projectedAt, endMs: projectionFinishedAt },
            },
            encodeMs,
            compressionMs,
            bytes: Buffer.byteLength(bytes),
            compressedBytes: compressed.byteLength,
            loopWarmupSampleCount,
            loopWorkEndSampleCount,
            loopFinalSampleCount,
            loopExceededCount: loop.exceeds,
            loopSampleCount,
            loopP99Ms: loopSampleCount > 0 ? loop.percentile(99) / 1e6 : null,
            loopMaxMs: loopSampleCount > 0 ? loop.max / 1e6 : null,
            watermark: shell.snapshotSequence,
          });
        } finally {
          phaseLoop.stop();
          loop.disable();
        }
      }
      return {
        samples,
        snapshotOffloadActivated: false,
        // JSON.stringify is synchronous, so this is a demonstrated contiguous block.
        contiguousEncodeTrigger: samples.some((sample) => sample.encodeMs >= 50),
        decodeProjectionElapsedTrigger: samples.some(
          (sample) => sample.decodeMs >= 50 || sample.schemaProjectionMs >= 50,
        ),
        compressionReportedSeparately: true,
        scope:
          "Synthetic shell pipeline; representative host/workload and combined-load attribution remain root qualification",
      };
    }).pipe(Effect.provide(Layer.fresh(Fixture.layer))),
  );
}

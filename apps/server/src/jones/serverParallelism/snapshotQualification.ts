// @effect-diagnostics nodeBuiltinImport:off globalTimers:off -- Synthetic snapshot codec qualification at an explicit runtime boundary.
import { gzipSync } from "node:zlib";
import { monitorEventLoopDelay } from "node:perf_hooks";
import { ThreadId, OrchestrationV2ShellSnapshotJson } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Fixture from "../threadResultDependencies/testkit.ts";

/** Snapshot offload is not enabled here. This measures the real capture/decode
 * and existing wire schema before root decides whether to activate that slice.
 */
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
        const loop = monitorEventLoopDelay({ resolution: 10 });
        loop.enable();
        try {
          const capturedAt = performance.now();
          const decode = yield* fixture.sql.withTransaction(fixture.projection.readShellSnapshot());
          const captureMs = performance.now() - capturedAt;
          const decodedAt = performance.now(),
            shell = yield* decode;
          const decodeMs = performance.now() - decodedAt;
          const projectedAt = performance.now();
          const projected = yield* Schema.encodeEffect(OrchestrationV2ShellSnapshotJson)({
            ...shell,
            projects: [],
          });
          const schemaProjectionMs = performance.now() - projectedAt;
          const encodedAt = performance.now(),
            bytes = JSON.stringify(projected);
          const encodeMs = performance.now() - encodedAt;
          const compressedAt = performance.now(),
            compressed = gzipSync(bytes);
          const compressionMs = performance.now() - compressedAt;
          // Let the loop monitor observe this pipeline's blocking interval.
          yield* Effect.promise(() => new Promise<void>((resolve) => setImmediate(resolve)));
          samples.push({
            captureMs,
            decodeMs,
            schemaProjectionMs,
            encodeMs,
            compressionMs,
            bytes: Buffer.byteLength(bytes),
            compressedBytes: compressed.byteLength,
            loopP99Ms: loop.percentile(99) / 1e6,
            loopMaxMs: loop.max / 1e6,
            watermark: shell.snapshotSequence,
          });
        } finally {
          loop.disable();
        }
      }
      return {
        samples,
        snapshotOffloadActivated: false,
        // JSON.stringify is synchronous, so this is a demonstrated contiguous block.
        contiguousEncodeTrigger: samples.some((sample) => sample.encodeMs >= 50),
        decodeProjectionAttributionRequired: samples.some(
          (sample) => sample.decodeMs >= 50 || sample.schemaProjectionMs >= 50,
        ),
        compressionReportedSeparately: true,
        scope:
          "Synthetic shell pipeline; representative host/workload and combined-load attribution remain root qualification",
      };
    }).pipe(Effect.provide(Layer.fresh(Fixture.layer))),
  );
}

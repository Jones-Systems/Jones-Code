import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { SqlitePersistenceMemory } from "../src/persistence/Layers/Sqlite.ts";
import * as EventSink from "../src/orchestration-v2/EventSink.ts";
import * as EventStore from "../src/orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../src/orchestration-v2/ProjectionStore.ts";
import { runV2Workload, validateV2Workload } from "./jones-sqlite-v2-workload.ts";

const database = SqlitePersistenceMemory;
const stores = Layer.mergeAll(database,
  EventStore.layer.pipe(Layer.provideMerge(database)),
  ProjectionStore.layer.pipe(Layer.provideMerge(database)));
const services = Layer.mergeAll(stores, EventSink.layer.pipe(Layer.provide(stores)));

it("refuses unbounded V2 workloads before any database work", () => {
  for (const commands of [0, 65, 1.5]) {
    assert.throws(() => validateV2Workload({ commands, payloadBytes: 16, intervalMs: 0 }));
  }
  assert.throws(() => validateV2Workload({ commands: 1, payloadBytes: 4097, intervalMs: 0 }));
  assert.throws(() => validateV2Workload({ commands: 1, payloadBytes: 16, intervalMs: -1 }));
});

for (const intervalMs of [0, 2]) {
  it.effect(`V2 acceptance preserves replay and rollback while separating timing populations (${intervalMs} ms arrivals)`, () =>
    Effect.gen(function* () {
      const result = yield* runV2Workload({ commands: 3, payloadBytes: 32, intervalMs });
      assert.strictEqual(result.traffic.eventSinkCompletion.samples, 3);
      assert.strictEqual(result.traffic.harnessQueueWait.samples, 3);
      assert.strictEqual(result.traffic.timerLag.samples, 3);
      assert.strictEqual(result.traffic.arrivalToCompletion.samples, 3);
      assert.strictEqual(result.protocol.eventSinkCompletion.samples, 5);
      assert.deepEqual(result.protocol, {
        eventSinkCompletion: result.protocol.eventSinkCompletion,
        acceptedReplay: true, rejectedReplay: true, rollbackUnchanged: true, retryCommitted: true,
      });
      assert.strictEqual(result.consistency.finalSequence, 5);
      assert.strictEqual(result.consistency.integrity, "ok");
      assert.strictEqual(result.consistency.foreignKeyViolations, 0);
      assert.isAtLeast(result.traffic.eventSinkCompletion.minimumMs!, 0);
      assert.isAtLeast(result.traffic.harnessQueueWait.minimumMs!, 0);
      assert.include(result.unavailable, "pure lock wait");
    }).pipe(Effect.provide(Layer.fresh(services))));
}

import { assert, it } from "@effect/vitest";
import { CommandId, RunId, EventId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";

class Published extends Context.Service<
  Published,
  Ref.Ref<
    ReadonlyArray<{
      events: number;
      choices: number;
      outcomes: number;
      receipts: number;
      reservations: number;
      effects: number;
    }>
  >
>()("t3/jones/importedHistory/ImportedHistoryEventSink.test/Published") {}
const stores = Layer.merge(EventStore.layer, ProjectionStore.layer).pipe(
  Layer.provideMerge(SqlitePersistence.layerMemory),
);
const log = Layer.effect(
  Published,
  Ref.make<
    ReadonlyArray<{
      events: number;
      choices: number;
      outcomes: number;
      receipts: number;
      reservations: number;
      effects: number;
    }>
  >([]),
);
const observedStore = Layer.effect(
  EventStore.EventStoreV2,
  Effect.gen(function* () {
    const store = yield* EventStore.EventStoreV2;
    const published = yield* Published;
    const sql = yield* SqlClient.SqlClient;
    return {
      ...store,
      publishCommitted: (events: Parameters<typeof store.publishCommitted>[0]) =>
        Effect.gen(function* () {
          if (events.length > 0) {
            const choices = yield* sql`SELECT * FROM jones_imported_history_choices`;
            const outcomes = yield* sql`SELECT * FROM jones_imported_history_outcomes`;
            const receipts = yield* sql`SELECT * FROM orchestration_command_receipts`;
            const reservations =
              yield* sql`SELECT * FROM jones_imported_history_start_reservations`;
            const effects = yield* sql`SELECT * FROM orchestration_v2_effect_outbox`;
            yield* Ref.update(published, (prior) => [
              ...prior,
              {
                events: events.length,
                choices: choices.length,
                outcomes: outcomes.length,
                receipts: receipts.length,
                reservations: reservations.length,
                effects: effects.length,
              },
            ]);
          }
          yield* store.publishCommitted(events);
        }).pipe(Effect.orDie),
    };
  }),
).pipe(Layer.provide(Layer.mergeAll(stores, log)));
const dependencies = Layer.mergeAll(stores, log, observedStore);
const testLayer = Layer.mergeAll(dependencies, EventSink.layer.pipe(Layer.provide(dependencies)));
import {
  principal,
  threadId,
  runId,
  now,
  capabilities,
  delivery,
  run,
  setup as seedFixture,
} from "./ImportedHistoryFixture.testkit.ts";
const setup = seedFixture.pipe(
  Effect.andThen(
    Effect.gen(function* () {
      yield* Ref.set(yield* Published, []);
    }),
  ),
);
const input = (reviewedBasis: string) => {
  const command = {
    type: "thread.imported-history.start" as const,
    commandId: CommandId.make("command:atomic-import"),
    threadId,
    delivery,
    reviewedBasis,
  };
  return {
    commandId: command.commandId,
    threadId,
    commandType: command.type,
    acceptedAt: now,
    events: [
      {
        id: EventId.make("event:import:starting"),
        type: "run.updated" as const,
        threadId,
        runId,
        occurredAt: now,
        payload: { ...run, status: "starting" as const, queueHeld: false },
      },
    ],
    effects: [
      {
        id: "effect:atomic-import",
        commandId: command.commandId,
        threadId,
        request: { type: "provider-turn.start" as const, runId },
      },
    ],
    importedHistory: { command, principal, targetCapabilities: capabilities },
  };
};
it.effect(
  "receiving EventSink commits queued choice, receipt, reservation and outbox before publication; duplicates publish nothing",
  () =>
    Effect.gen(function* () {
      yield* setup;
      const sink = yield* EventSink.EventSinkV2;
      assert.isDefined(sink.reviewImportedHistory);
      const review = yield* sink.reviewImportedHistory!({
        threadId,
        delivery,
        principal,
        targetCapabilities: capabilities,
      });
      assert.strictEqual(review.status, "available");
      const accepted = yield* sink.commitCommand(input(review.reviewedBasis ?? "unavailable"));
      assert.strictEqual(accepted.receipt.status, "accepted");
      const published = yield* Ref.get(yield* Published);
      assert.deepEqual(published, [
        { events: 1, choices: 1, outcomes: 1, receipts: 1, reservations: 1, effects: 1 },
      ]);
      const duplicate = yield* sink.commitCommand(input(review.reviewedBasis ?? "unavailable"));
      assert.isFalse(duplicate.committed);
      assert.deepEqual(yield* Ref.get(yield* Published), published);
    }).pipe(Effect.provide(testLayer)),
);
it.effect(
  "receiving EventSink rolls back staged events, choice, receipt and outbox on invalid start binding and publishes nothing",
  () =>
    Effect.gen(function* () {
      yield* setup;
      const sink = yield* EventSink.EventSinkV2;
      const sql = yield* SqlClient.SqlClient;
      const review = yield* sink.reviewImportedHistory!({
        threadId,
        delivery,
        principal,
        targetCapabilities: capabilities,
      });
      assert.strictEqual(review.status, "available");
      const prepared = input(review.reviewedBasis ?? "unavailable");
      const before = yield* sink.latestSequence();
      const failed = yield* sink
        .commitCommand({
          ...prepared,
          effects: [
            {
              ...prepared.effects[0]!,
              request: { type: "provider-turn.start", runId: RunId.make("run:foreign") },
            },
          ],
        })
        .pipe(Effect.result);
      assert.strictEqual(failed._tag, "Failure");
      assert.strictEqual(yield* sink.latestSequence(), before);
      assert.deepEqual(yield* sql`SELECT * FROM jones_imported_history_choices`, []);
      assert.deepEqual(yield* sql`SELECT * FROM jones_imported_history_outcomes`, []);
      assert.deepEqual(yield* sql`SELECT * FROM jones_imported_history_start_reservations`, []);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_command_receipts`, []);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_effect_outbox`, []);
      assert.deepEqual(yield* Ref.get(yield* Published), []);
      assert.strictEqual(
        (yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadRecords(threadId, ["runs"]))
          .runs[0]?.status,
        "queued",
      );
    }).pipe(Effect.provide(testLayer)),
);

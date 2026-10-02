import { assert, it } from "@effect/vitest";
import { ThreadId } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Console from "effect/Console";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as Statement from "effect/unstable/sql/Statement";

import { makeWorktreeOwnershipLeaseStore } from "../../orchestration/WorktreeOwnershipLease.ts";
import { migrationManifest, runMigrations } from "../Migrations.ts";
import migrate from "./005_JonesThreadCreationLookupIndex.ts";

const memory = NodeSqliteClient.layer({ filename: ":memory:" });
const indexName = "idx_orch_events_thread_creation_lookup";

const withQueryPlan = Effect.fnUntraced(function* <A, E, R>(query: Effect.Effect<A, E, R>) {
  const sql = yield* SqlClient.SqlClient;
  let compiled: ReturnType<Statement.Statement<unknown>["compile"]> | undefined;
  const result = yield* query.pipe(
    Effect.provideService(Statement.CurrentTransformer, (statement) =>
      Effect.sync(() => {
        compiled = statement.compile();
        return statement;
      }),
    ),
  );
  if (compiled === undefined) return yield* Effect.die(new Error("store query was not captured"));
  const plan = yield* sql.unsafe<{ detail: string }>(
    `EXPLAIN QUERY PLAN ${compiled[0]}`,
    compiled[1],
  );
  return { result, plan };
});

it.effect(
  "adds the covering partial creation index without changing event or migration history",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 54 });
      yield* sql`INSERT INTO orchestration_events
      (event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, actor_kind, payload_json, metadata_json)
      VALUES ('index-created', 'thread', 'index-thread', 1, 'thread.created', '2026-01-01T00:00:00.000Z', 'user', '{}', '{}')`;
      const eventsBefore = yield* sql`SELECT * FROM orchestration_events`;
      yield* migrate;
      yield* migrate;
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_events`, eventsBefore);

      const indexes = yield* sql<{ name: string; partial: number; unique: number }>`
      PRAGMA index_list(orchestration_events)
    `;
      assert.isTrue(
        indexes.some(
          (index) => index.name === indexName && index.partial === 1 && index.unique === 0,
        ),
      );
      const columns = yield* sql<{ name: string; desc: number; key: number }>`
      PRAGMA index_xinfo(idx_orch_events_thread_creation_lookup)
    `;
      assert.deepEqual(
        columns.filter((column) => column.key === 1).map(({ name, desc }) => ({ name, desc })),
        [
          { name: "stream_id", desc: 0 },
          { name: "sequence", desc: 1 },
          { name: "event_id", desc: 0 },
        ],
      );
      const indexRows = yield* sql<{ sql: string }>`
      SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ${indexName}
    `;
      assert.equal(
        indexRows[0]?.sql
          .split(/\bWHERE\b/i)[1]
          ?.replace(/\s+/g, " ")
          .trim(),
        "aggregate_kind = 'thread' AND event_type = 'thread.created'",
      );
      yield* runMigrations();
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_events`, eventsBefore);
      assert.deepEqual(
        yield* sql`SELECT migration_id, name FROM effect_sql_migrations ORDER BY migration_id`,
        migrationManifest.map(([migration_id, name]) => ({ migration_id, name })),
      );
      assert.deepEqual(yield* sql`SELECT name FROM jones_sql_migrations WHERE migration_id = 5`, [
        { name: "ThreadCreationLookupIndex" },
      ]);
      assert.deepEqual(yield* runMigrations(), []);
    }).pipe(Effect.provide(memory)),
);

it.live("uses a covering creation lookup as unrelated thread history grows", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations();
    const store = yield* makeWorktreeOwnershipLeaseStore();
    const threadId = ThreadId.make("index-history-thread");
    yield* sql`INSERT INTO orchestration_events
      (event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, actor_kind, payload_json, metadata_json)
      VALUES ('index-history-created', 'thread', ${threadId}, 1, 'thread.created', '2026-01-01T00:00:00.000Z', 'user', '{}', '{}')`;

    const sampleLookup = Effect.gen(function* () {
      for (let warmup = 0; warmup < 5; warmup += 1) yield* store.getThreadIncarnation(threadId);
      const samples: number[] = [];
      for (let sample = 0; sample < 31; sample += 1) {
        const startedAt = performance.now();
        assert.deepEqual(
          yield* store.getThreadIncarnation(threadId),
          Option.some("index-history-created"),
        );
        samples.push(performance.now() - startedAt);
      }
      samples.sort((left, right) => left - right);
      const meanMs = samples.reduce((sum, value) => sum + value, 0) / samples.length;
      const standardDeviationMs = Math.sqrt(
        samples.reduce((sum, value) => sum + (value - meanMs) ** 2, 0) / samples.length,
      );
      return {
        sampleCount: samples.length,
        minMs: samples[0]!,
        medianMs: samples[15]!,
        p95Ms: samples[29]!,
        maxMs: samples[30]!,
        meanMs,
        standardDeviationMs,
      };
    });
    let previousHistoryEvents = 0;
    for (const historyEvents of [10, 1_000, 10_000]) {
      yield* sql`WITH RECURSIVE history(version) AS (
        SELECT ${previousHistoryEvents + 2}
        UNION ALL SELECT version + 1 FROM history WHERE version < ${historyEvents + 1}
      )
      INSERT INTO orchestration_events
        (event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, actor_kind, payload_json, metadata_json)
      SELECT 'index-history-' || version, 'thread', ${threadId}, version, 'thread.activity-appended',
        '2026-01-01T00:00:00.000Z', 'user', '{}', '{}' FROM history`;
      previousHistoryEvents = historyEvents;
      // This disposable database compares the same store query with and without the new index.
      yield* sql`DROP INDEX idx_orch_events_thread_creation_lookup`;
      const baseline = yield* sampleLookup;
      yield* migrate;
      const indexed = yield* sampleLookup;
      const lookup = yield* withQueryPlan(store.getThreadIncarnation(threadId));
      assert.deepEqual(lookup.result, Option.some("index-history-created"));
      assert.isTrue(
        lookup.plan.some((row) => row.detail.includes(indexName) && /covering/i.test(row.detail)),
      );
      const acquisition = yield* withQueryPlan(
        store.acquire({
          resourcePath: "/fixture/index-history",
          leaseId: `index-history-lease-${historyEvents}`,
          ownerThreadId: threadId,
          branch: null,
          nowMs: historyEvents,
          expiresAtMs: historyEvents + 1_000,
        }),
      );
      assert.equal(Option.getOrThrow(acquisition.result).ownerIncarnation, "index-history-created");
      assert.isTrue(
        acquisition.plan.some(
          (row) => row.detail.includes(indexName) && /covering/i.test(row.detail),
        ),
      );
      yield* Console.info(
        JSON.stringify({
          schema: "jones-thread-creation-lookup-samples/v1",
          historyEvents,
          baseline,
          indexed,
          lookupPlan: lookup.plan.map((row) => row.detail),
          acquisitionPlan: acquisition.plan.map((row) => row.detail),
        }),
      );
    }
  }).pipe(Effect.provide(memory)),
);

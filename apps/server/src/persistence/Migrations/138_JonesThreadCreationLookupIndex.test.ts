import { assert, it } from "@effect/vitest";
import { ThreadId } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Console from "effect/Console";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Migrator from "effect/unstable/sql/Migrator";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as Statement from "effect/unstable/sql/Statement";

import { makeWorktreeOwnershipLeaseStore } from "../../orchestration-v2/WorktreeOwnershipLease.ts";
import { migrationManifest, runMigrations } from "../Migrations.ts";
import mainMigration0001 from "./001_JonesWorktreeOwnershipLeases.ts";
import mainMigration0002 from "./002_JonesProjectionThreadRuntimeIdentity.ts";
import mainMigration0003 from "./003_JonesNativeCreationIntents.ts";
import mainMigration0004 from "./004_JonesNativeCreationCommandIdentities.ts";
import mainMigration0005 from "./005_JonesWorkstreamsNativeAttempts.ts";
import mainMigration0006 from "./006_JonesWorkstreamsProviderEnrollments.ts";
import migrate from "./138_JonesThreadCreationLookupIndex.ts";

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

const memory = NodeSqliteClient.layer({ filename: ":memory:" });
const indexName = "idx_orch_v2_events_thread_creation_lookup";
// The native migrator creates the baseline schema and ledger from migration effects 1–6.
const mainForkMigrationLoader = Migrator.fromRecord({
  "1_WorktreeOwnershipLeases": mainMigration0001,
  "2_ProjectionThreadRuntimeIdentity": mainMigration0002,
  "3_NativeCreationIntents": mainMigration0003,
  "4_NativeCreationCommandIdentities": mainMigration0004,
  "5_WorkstreamsNativeAttempts": mainMigration0005,
  "6_WorkstreamsProviderEnrollments": mainMigration0006,
});
const runMainForkMigrations = Migrator.make({});

const LookupSampleStatistics = Schema.Struct({
  sampleCount: Schema.Number,
  minMs: Schema.Number,
  medianMs: Schema.Number,
  p95Ms: Schema.Number,
  maxMs: Schema.Number,
  meanMs: Schema.Number,
  standardDeviationMs: Schema.Number,
});
const encodeLookupSamples = Schema.encodeSync(
  Schema.fromJsonString(
    Schema.Struct({
      schema: Schema.Literal("jones-thread-creation-lookup-samples/v1"),
      historyEvents: Schema.Number,
      baseline: LookupSampleStatistics,
      indexed: LookupSampleStatistics,
      lookupPlan: Schema.Array(Schema.String),
      acquisitionPlan: Schema.Array(Schema.String),
    }),
  ),
);

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
  "adds migration 138 covering partial creation index without changing event or migration history",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 56 });
      yield* sql`INSERT INTO orchestration_events
      (event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, actor_kind, payload_json, metadata_json, application_event_version)
      VALUES ('index-created', 'thread', 'index-thread', 1, 'thread.created', '2026-01-01T00:00:00.000Z', 'user', '{}', '{}', 2)`;
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
      PRAGMA index_xinfo(idx_orch_v2_events_thread_creation_lookup)
    `;
      assert.deepEqual(
        columns.filter((column) => column.key === 1).map(({ name, desc }) => ({ name, desc })),
        [
          { name: "stream_id", desc: 0 },
          { name: "sequence", desc: 1 },
          { name: "event_id", desc: 0 },
          { name: "application_event_version", desc: 0 },
          { name: "aggregate_kind", desc: 0 },
          { name: "event_type", desc: 0 },
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
        "aggregate_kind = 'thread' AND application_event_version = 2 AND event_type = 'thread.created'",
      );
      yield* runMigrations();
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_events`, eventsBefore);
      assert.deepEqual(
        yield* sql`SELECT migration_id, name FROM effect_sql_migrations ORDER BY migration_id`,
        migrationManifest.map(([migration_id, name]) => ({ migration_id, name })),
      );
      assert.deepEqual(yield* sql`SELECT name FROM jones_sql_migrations WHERE migration_id = 138`, [
        { name: "ThreadCreationLookupIndex" },
      ]);
      assert.deepEqual(yield* runMigrations(), []);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "appends migration 138 to a populated fork-6 database without changing existing state",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      assert.deepEqual(yield* runMigrations({ toMigrationInclusive: 56 }), migrationManifest);
      assert.deepEqual(
        yield* runMainForkMigrations({
          loader: mainForkMigrationLoader,
          table: "jones_sql_migrations",
        }),
        [
          [1, "WorktreeOwnershipLeases"],
          [2, "ProjectionThreadRuntimeIdentity"],
          [3, "NativeCreationIntents"],
          [4, "NativeCreationCommandIdentities"],
          [5, "WorkstreamsNativeAttempts"],
          [6, "WorkstreamsProviderEnrollments"],
        ],
      );
      assert.deepEqual(
        yield* sql`SELECT name FROM sqlite_master WHERE type = 'index' AND name = ${indexName}`,
        [],
      );
      yield* sql`INSERT INTO orchestration_events
      (event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, actor_kind, payload_json, metadata_json, application_event_version)
      VALUES ('upgrade-created', 'thread', 'upgrade-thread', 1, 'thread.created', '2026-01-01T00:00:00.000Z', 'user', '{}', '{}', 2)`;
      const store = yield* makeWorktreeOwnershipLeaseStore();
      const lease = yield* store.acquire({
        resourcePath: "/fixture/index-upgrade",
        leaseId: "upgrade-lease",
        ownerThreadId: ThreadId.make("upgrade-thread"),
        branch: "upgrade-branch",
        nowMs: 10,
        expiresAtMs: 1_000,
      });
      assert.isTrue(Option.isSome(lease));
      yield* sql`INSERT INTO auth_sessions
      (session_id, subject, scopes, method, issued_at, expires_at)
      VALUES ('upgrade-session', 'upgrade-owner', '[]', 'pairing', '2026-01-01T00:00:00.000Z', '2026-01-02T00:00:00.000Z')`;
      const digest = "a".repeat(64);
      yield* sql`INSERT INTO workstreams_native_attempts
      (owner_id, principal_id, command_id, request_json, request_bytes_sha256, enrollment_sha256, enrollment_json, native_command_id, created_at)
      VALUES ('upgrade-owner', 'upgrade-principal', 'upgrade-command', '{}', ${digest}, ${digest}, '{}', 'upgrade-native-command', '2026-01-01T00:00:00.000Z')`;
      yield* sql`INSERT INTO workstreams_native_enrollments
      (enrollment_id, session_id, request_sha256, request_json, binding_json)
      VALUES ('upgrade-enrollment', 'upgrade-session', ${digest}, '{}', '{}')`;

      const upstreamBefore = yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`;
      const forkBefore = yield* sql`SELECT * FROM jones_sql_migrations ORDER BY migration_id`;
      const eventsBefore = yield* sql`SELECT * FROM orchestration_events ORDER BY sequence`;
      const leaseBefore = yield* sql`SELECT * FROM worktree_ownership_leases`;
      const attemptsBefore = yield* sql`SELECT * FROM workstreams_native_attempts`;
      const enrollmentsBefore = yield* sql`SELECT * FROM workstreams_native_enrollments`;
      const schemaBefore = yield* sql`SELECT name, type, sql FROM sqlite_master
      WHERE tbl_name IN ('worktree_ownership_leases', 'workstreams_native_attempts', 'workstreams_native_enrollments') ORDER BY name`;

      assert.deepEqual(yield* runMigrations(), []);
      const forkAfter = yield* sql`SELECT * FROM jones_sql_migrations ORDER BY migration_id`;
      assert.equal(forkAfter.length, 10);
      assert.deepEqual(forkAfter.slice(0, 6), forkBefore);
      assert.deepEqual(
        forkAfter.slice(6).map(({ migration_id, name }) => ({ migration_id, name })),
        [
          { migration_id: 138, name: "ThreadCreationLookupIndex" },
          { migration_id: 139, name: "DeletionWorktreeAdmission" },
          { migration_id: 140, name: "OrdinaryCheckoutOwnership" },
          { migration_id: 141, name: "OrdinaryCheckoutExecutionLifetime" },
        ],
      );
      assert.deepEqual(
        yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`,
        upstreamBefore,
      );
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_events ORDER BY sequence`,
        eventsBefore,
      );
      assert.deepEqual(yield* sql`SELECT * FROM worktree_ownership_leases`, leaseBefore);
      assert.deepEqual(yield* sql`SELECT * FROM workstreams_native_attempts`, attemptsBefore);
      assert.deepEqual(yield* sql`SELECT * FROM workstreams_native_enrollments`, enrollmentsBefore);
      assert.deepEqual(
        yield* sql`SELECT name, type, sql FROM sqlite_master
        WHERE tbl_name IN ('worktree_ownership_leases', 'workstreams_native_attempts', 'workstreams_native_enrollments') ORDER BY name`,
        schemaBefore,
      );
      assert.deepEqual(
        yield* sql`SELECT name FROM sqlite_master WHERE type = 'index' AND name = ${indexName}`,
        [{ name: indexName }],
      );
      assert.deepEqual(yield* sql`PRAGMA integrity_check`, [{ integrity_check: "ok" }]);
      assert.deepEqual(yield* sql`PRAGMA foreign_key_check`, []);
      assert.deepEqual(yield* runMigrations(), []);
      assert.deepEqual(
        yield* sql`SELECT * FROM jones_sql_migrations ORDER BY migration_id`,
        forkAfter,
      );
    }).pipe(Effect.provide(memory)),
);

it.live("uses a covering creation lookup as unrelated thread history grows", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations();
    const store = yield* makeWorktreeOwnershipLeaseStore();
    const threadId = ThreadId.make("index-history-thread");
    yield* sql`INSERT INTO orchestration_events
      (event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, actor_kind, payload_json, metadata_json, application_event_version)
      VALUES ('index-history-created', 'thread', ${threadId}, 1, 'thread.created', '2026-01-01T00:00:00.000Z', 'user', '{}', '{}', 2)`;

    const sampleLookup = Effect.gen(function* () {
      for (let warmup = 0; warmup < 5; warmup += 1) yield* store.getThreadIncarnation(threadId);
      const samples: number[] = [];
      for (let sample = 0; sample < 31; sample += 1) {
        const startedAt = performance.now();
        assert.deepEqual(
          yield* store.getThreadIncarnation(threadId),
          Option.some(
            encodeJson(["t3.orchestration-v2.thread-birth/v1", "index-history-created", 1]),
          ),
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
        (event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, actor_kind, payload_json, metadata_json, application_event_version)
      SELECT 'index-history-' || version, 'thread', ${threadId}, version, 'thread.activity-appended',
        '2026-01-01T00:00:00.000Z', 'user', '{}', '{}', 2 FROM history`;
      previousHistoryEvents = historyEvents;
      // This disposable database compares the same store query with and without the new index.
      yield* sql`DROP INDEX idx_orch_v2_events_thread_creation_lookup`;
      const baseline = yield* sampleLookup;
      yield* migrate;
      const indexed = yield* sampleLookup;
      const lookup = yield* withQueryPlan(store.getThreadIncarnation(threadId));
      assert.deepEqual(
        lookup.result,
        Option.some(
          encodeJson(["t3.orchestration-v2.thread-birth/v1", "index-history-created", 1]),
        ),
      );
      assert.isTrue(
        lookup.plan.some((row) => row.detail.includes(indexName) && /covering/i.test(row.detail)),
        encodeJson(lookup.plan),
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
      assert.equal(
        Option.getOrThrow(acquisition.result).ownerIncarnation,
        encodeJson(["t3.orchestration-v2.thread-birth/v1", "index-history-created", 1]),
      );
      assert.isTrue(
        acquisition.plan.some(
          (row) => row.detail.includes(indexName) && /covering/i.test(row.detail),
        ),
      );
      yield* Console.info(
        encodeLookupSamples({
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

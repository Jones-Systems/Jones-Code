import { assert, it } from "@effect/vitest";
import { ThreadId } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Cause from "effect/Cause";
import * as Exit from "effect/Exit";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { readApplicationBirthRecord } from "./ApplicationBirth.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import { receivingCreationLookupProgram } from "../../../../../scripts/jones/performance/migration-restore-worker.mjs";

const memory = NodeSqliteClient.layer({ filename: ":memory:" });
const threadId = ThreadId.make("thread:birth-custody");
const payload = {
  id: threadId,
  projectId: "project:birth-custody",
  createdAt: "2026-10-07T00:00:00.000Z",
  deletedAt: null,
};
const encodeThreadPayload = Schema.encodeEffect(
  Schema.fromJsonString(
    Schema.Struct({
      id: Schema.String,
      projectId: Schema.String,
      createdAt: Schema.String,
      deletedAt: Schema.NullOr(Schema.String),
    }),
  ),
);
const encodeFixturePayload = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const setup = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE orchestration_events (
    event_id TEXT, sequence INTEGER, payload_json TEXT, application_event_version INTEGER,
    aggregate_kind TEXT, stream_id TEXT, event_type TEXT)`;
  yield* sql`CREATE TABLE orchestration_v2_projection_threads (thread_id TEXT, payload_json TEXT)`;
  yield* sql`INSERT INTO orchestration_v2_projection_threads VALUES (${threadId}, ${yield* encodeThreadPayload(payload)})`;
});
const addBirth = (id: string, sequence: number, value: unknown = payload) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO orchestration_events VALUES (
      ${id}, ${sequence}, ${yield* encodeFixturePayload(value)}, 2, 'thread', ${threadId}, 'thread.created')`;
  });

it.effect("binds the latest application birth to the live persisted projection", () =>
  Effect.gen(function* () {
    yield* setup;
    yield* addBirth("event:old-birth", 1);
    yield* addBirth("event:replacement-birth", 3);
    assert.deepEqual(yield* readApplicationBirthRecord(threadId), {
      kind: "application_v2_thread_birth",
      threadId,
      eventId: "event:replacement-birth",
      sequence: 3,
    });
  }).pipe(Effect.provide(memory)),
);

it.effect.each([
  ["project changed", { ...payload, projectId: "project:replacement" }],
  ["same-time foreign thread", { ...payload, id: "thread:foreign" }],
  ["creation changed", { ...payload, createdAt: "2026-10-08T00:00:00.000Z" }],
  ["malformed birth", {}],
] as const)("does not qualify %s", ([_name, birth]) =>
  Effect.gen(function* () {
    yield* setup;
    yield* addBirth("event:mismatched-birth", 2, birth);
    assert.isNull(yield* readApplicationBirthRecord(threadId));
  }).pipe(Effect.provide(memory)),
);

it.effect("missing, deleted and ambiguous projections do not manufacture a birth", () =>
  Effect.gen(function* () {
    yield* setup;
    assert.isNull(yield* readApplicationBirthRecord(threadId));
    yield* addBirth("event:birth", 1);
    const sql = yield* SqlClient.SqlClient;
    yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json = ${yield* encodeThreadPayload({ ...payload, deletedAt: payload.createdAt })}`;
    assert.isNull(yield* readApplicationBirthRecord(threadId));
    yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json = ${yield* encodeThreadPayload(payload)}`;
    yield* sql`INSERT INTO orchestration_v2_projection_threads VALUES (${threadId}, ${yield* encodeThreadPayload(payload)})`;
    assert.isNull(yield* readApplicationBirthRecord(threadId));
  }).pipe(Effect.provide(memory)),
);

it.effect("legacy births and invalid application event sequences are not evidence", () =>
  Effect.gen(function* () {
    yield* setup;
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO orchestration_events VALUES (
      'event:legacy-birth', 1, ${yield* encodeThreadPayload(payload)}, 1, 'thread', ${threadId}, 'thread.created')`;
    assert.isNull(yield* readApplicationBirthRecord(threadId));
    yield* addBirth("event:invalid-sequence", 0);
    assert.isNull(yield* readApplicationBirthRecord(threadId));
  }).pipe(Effect.provide(memory)),
);

it.effect(
  "qualification lookup probe executes the receiving owner and rolls back synthetic rows and index changes",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const queryEffect = (text: string, values: ReadonlyArray<string | number | null> = []) =>
        sql.unsafe<Readonly<Record<string, unknown>>>(text, values);
      yield* queryEffect(
        "INSERT INTO jones_sql_migrations(migration_id,name) VALUES(7,'ThreadCreationLookupIndex')",
      );
      const snapshot = Effect.gen(function* () {
        return {
          ledger: yield* queryEffect("SELECT * FROM jones_sql_migrations ORDER BY migration_id"),
          events: yield* queryEffect("SELECT * FROM orchestration_events ORDER BY sequence"),
          threads: yield* queryEffect(
            "SELECT * FROM orchestration_v2_projection_threads ORDER BY thread_id",
          ),
          index: yield* queryEffect(
            "SELECT sql FROM sqlite_schema WHERE name='orchestration_events_v2_created_threads_idx'",
          ),
        };
      });
      const program = receivingCreationLookupProgram({
        modules: { Effect, Contracts: { ThreadId }, Birth: { readApplicationBirthRecord } },
        queryEffect,
      });
      const before = yield* snapshot;
      const evidence = yield* program;
      assert.strictEqual(evidence.owner, "055_OrchestrationV2/RecoveryIndexes");
      assert.strictEqual(evidence.index, "orchestration_events_v2_created_threads_idx");
      assert.isTrue(evidence.actualLookupExecuted);
      assert.isTrue(evidence.foreignBirthExcluded);
      assert.isTrue(evidence.changedProjectionRejected);
      assert.isTrue(evidence.missingIndexRejected);
      assert.deepStrictEqual(yield* snapshot, before);
      yield* queryEffect("DROP INDEX orchestration_events_v2_created_threads_idx");
      const withoutIndex = yield* snapshot;
      const rejected = yield* Effect.exit(program);
      assert.isTrue(Exit.isFailure(rejected));
      if (Exit.isFailure(rejected))
        assert.match(Cause.pretty(rejected.cause), /receiving migration055 lookup index missing/);
      assert.deepStrictEqual(yield* snapshot, withoutIndex);
    }).pipe(Effect.provide(SqlitePersistence.layerMemory.pipe(Layer.provide(NodeServices.layer)))),
);

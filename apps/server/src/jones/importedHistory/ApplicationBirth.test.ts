import { assert, it } from "@effect/vitest";
import { ThreadId } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { readApplicationBirthRecord } from "./ApplicationBirth.ts";

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

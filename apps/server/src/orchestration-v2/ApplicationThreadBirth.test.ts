import { assert, it } from "@effect/vitest";
import { EventId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import { readApplicationThreadBirth } from "./ApplicationThreadBirth.ts";

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

const threadId = ThreadId.make("thread:birth-authority");
const eventId = EventId.make("event:birth-authority");
const identity = {
  id: threadId,
  projectId: "project:birth-authority",
  createdAt: "2026-10-05T00:00:00.000Z",
  deletedAt: null,
};

const seed = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{ readonly sequence: number }>`INSERT INTO orchestration_events
    (event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, actor_kind,
      payload_json, metadata_json, application_event_version)
    VALUES (${eventId}, 'thread', ${threadId}, 1, 'thread.created', ${identity.createdAt}, 'user',
      ${encodeJson(identity)}, '{}', 2) RETURNING sequence`;
  yield* sql`INSERT INTO orchestration_v2_projection_threads ${sql.insert({
    thread_id: threadId,
    project_id: identity.projectId,
    title: "Synthetic birth authority",
    default_provider: "codex",
    runtime_mode: "full-access",
    interaction_mode: "default",
    created_at: identity.createdAt,
    updated_at: identity.createdAt,
    payload_json: encodeJson(identity),
  })}`;
  return rows[0]!.sequence;
});

it.effect("binds authority to the exact canonical birth event and sequence", () =>
  Effect.gen(function* () {
    const sequence = yield* seed;
    assert.deepEqual(yield* readApplicationThreadBirth(threadId), {
      kind: "application_v2_thread_birth",
      threadId,
      eventId,
      sequence,
    });
    assert.isNull(yield* readApplicationThreadBirth(ThreadId.make("thread:absent")));
  }).pipe(Effect.provide(SqlitePersistenceMemory)),
);

it.effect.each([
  ["id", "thread:foreign"],
  ["projectId", "project:foreign"],
  ["createdAt", "2026-10-05T01:00:00.000Z"],
  ["deletedAt", "2026-10-05T01:00:00.000Z"],
] as const)("refuses a projection with changed %s", ([field, value]) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* seed;
    yield* sql`UPDATE orchestration_v2_projection_threads
      SET payload_json = ${encodeJson({ ...identity, [field]: value })}
      WHERE thread_id = ${threadId}`;
    assert.isNull(yield* readApplicationThreadBirth(threadId));
  }).pipe(Effect.provide(SqlitePersistenceMemory)),
);

it.effect("refuses legacy births and a recreated id until its projection agrees", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* seed;
    yield* sql`UPDATE orchestration_events SET application_event_version = 1
      WHERE event_id = ${eventId}`;
    assert.isNull(yield* readApplicationThreadBirth(threadId));
    const recreated = { ...identity, createdAt: "2026-10-05T02:00:00.000Z" };
    const rows = yield* sql<{ readonly sequence: number }>`INSERT INTO orchestration_events
      (event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, actor_kind,
        payload_json, metadata_json, application_event_version)
      VALUES ('event:recreated', 'thread', ${threadId}, 2, 'thread.created', ${recreated.createdAt},
        'user', ${encodeJson(recreated)}, '{}', 2) RETURNING sequence`;
    assert.isNull(yield* readApplicationThreadBirth(threadId));
    yield* sql`UPDATE orchestration_v2_projection_threads
      SET payload_json = ${encodeJson(recreated)} WHERE thread_id = ${threadId}`;
    assert.deepEqual(yield* readApplicationThreadBirth(threadId), {
      kind: "application_v2_thread_birth",
      threadId,
      eventId: EventId.make("event:recreated"),
      sequence: rows[0]!.sequence,
    });
  }).pipe(Effect.provide(SqlitePersistenceMemory)),
);

it.effect("refuses a birth whose current projection is absent", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* seed;
    yield* sql`DELETE FROM orchestration_v2_projection_threads WHERE thread_id = ${threadId}`;
    assert.isNull(yield* readApplicationThreadBirth(threadId));
  }).pipe(Effect.provide(SqlitePersistenceMemory)),
);

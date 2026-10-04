import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  nativeCreationCanonicalJson,
  nativeCreationSha256,
} from "../../orchestration-v2/NativeCreationPreparation.ts";
import { migrationManifest, runMigrations } from "../Migrations.ts";

class SyntheticObservationFailure extends Schema.TaggedError<SyntheticObservationFailure>()(
  "SyntheticObservationFailure",
  { cause: Schema.Defect() },
) {}

const memory = NodeSqliteClient.layer({ filename: ":memory:" });
const timestamp = "2026-10-03T00:00:00.000Z";
const effectId = "effect:attachment-fixture";
// Synthetic SQL carriers exercise storage constraints without asserting qualified cleanup evidence.
const taskJson = nativeCreationCanonicalJson({
  effectId,
  fixture: "attachment observation storage task",
});
const observationRow = {
  effect_id: effectId,
  ordinal: 0,
  binding_sha256: nativeCreationSha256(taskJson),
  canonical_task_json: taskJson,
  observation_json: nativeCreationCanonicalJson({ fixture: "append-only observation", step: 0 }),
  correlation_json: nativeCreationCanonicalJson({ fixture: "bounded storage correlation" }),
  recorded_at: timestamp,
};
const insertParent = (id = effectId) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO orchestration_v2_effect_outbox
    (effect_id, command_id, thread_id, effect_type, payload_json, status, attempt_count, available_at, created_at, updated_at)
    VALUES (${id}, 'command:attachment-fixture', 'thread:attachment-fixture', 'attachment.cleanup',
      '{"type":"attachment.cleanup","attachmentIds":[]}', 'pending', 0, ${timestamp}, ${timestamp}, ${timestamp})`;
  });

it.effect(
  "Jones10 registers one empty observation table after upstream56 while explicit upstream replay excludes it",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 56 });
      const upstream = yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`;
      assert.deepEqual(
        upstream.map((row): readonly [unknown, unknown] => [row.migration_id, row.name]),
        migrationManifest,
      );
      assert.deepEqual(
        yield* sql`SELECT name FROM sqlite_master WHERE name = 'orchestration_v2_attachment_cleanup_observations'`,
        [],
      );
      yield* runMigrations();
      assert.deepEqual(yield* runMigrations(), []);
      assert.deepEqual(
        yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`,
        upstream,
      );
      assert.deepEqual(
        yield* sql`SELECT migration_id, name FROM jones_sql_migrations WHERE migration_id = 10`,
        [{ migration_id: 10, name: "AttachmentCleanup" }],
      );
      assert.deepEqual(
        yield* sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'orchestration_v2_attachment_cleanup_%'`,
        [{ name: "orchestration_v2_attachment_cleanup_observations" }],
      );
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_attachment_cleanup_observations`,
        [],
      );
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "observations require actual outbox parent, nonnegative ordinal, lowercase digest and complete object carriers",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      assert.equal(
        (yield* Effect.result(
          sql`INSERT INTO orchestration_v2_attachment_cleanup_observations ${sql.insert(observationRow)}`,
        ))._tag,
        "Failure",
      );
      yield* insertParent();
      for (const invalid of [
        { effect_id: null },
        { effect_id: "missing-effect" },
        { ordinal: null },
        { ordinal: -1 },
        { binding_sha256: null },
        { binding_sha256: "A".repeat(64) },
        { binding_sha256: "g".repeat(64) },
        { binding_sha256: "a".repeat(63) },
        { binding_sha256: "a".repeat(65) },
        { canonical_task_json: null },
        { canonical_task_json: "invalid" },
        { canonical_task_json: "[]" },
        { canonical_task_json: "null" },
        { observation_json: null },
        { observation_json: "invalid" },
        { observation_json: "[]" },
        { observation_json: '"text"' },
        { correlation_json: null },
        { correlation_json: "invalid" },
        { correlation_json: "[]" },
        { correlation_json: "null" },
        { recorded_at: null },
        { recorded_at: "" },
      ])
        assert.equal(
          (yield* Effect.result(
            sql`INSERT INTO orchestration_v2_attachment_cleanup_observations ${sql.insert({ ...observationRow, ...invalid })}`,
          ))._tag,
          "Failure",
        );
      yield* sql`INSERT INTO orchestration_v2_attachment_cleanup_observations ${sql.insert(observationRow)}`;
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_attachment_cleanup_observations`, [
        observationRow,
      ]);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_lease_cleanup_task_bindings`, []);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_command_receipts`, []);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "later ordinal appends retain earlier canonical task, observation and correlation independently for each effect",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* insertParent();
      yield* insertParent("effect:attachment-independent");
      const later = {
        ...observationRow,
        ordinal: 1,
        observation_json: nativeCreationCanonicalJson({
          fixture: "append-only observation",
          step: 1,
        }),
        correlation_json: nativeCreationCanonicalJson({ fixture: "later bounded correlation" }),
      };
      const independentTask = nativeCreationCanonicalJson({
        effectId: "effect:attachment-independent",
        fixture: "attachment observation storage task",
      });
      const independent = {
        ...observationRow,
        effect_id: "effect:attachment-independent",
        canonical_task_json: independentTask,
        binding_sha256: nativeCreationSha256(independentTask),
      };
      yield* sql`INSERT INTO orchestration_v2_attachment_cleanup_observations ${sql.insert(observationRow)}`;
      yield* sql`INSERT INTO orchestration_v2_attachment_cleanup_observations ${sql.insert(later)}`;
      yield* sql`INSERT INTO orchestration_v2_attachment_cleanup_observations ${sql.insert(independent)}`;
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_attachment_cleanup_observations WHERE effect_id = ${effectId} ORDER BY ordinal`,
        [observationRow, later],
      );
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_attachment_cleanup_observations WHERE effect_id = 'effect:attachment-independent'`,
        [independent],
      );
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "observations cannot be updated, deleted or replaced at the same effect and ordinal with recursive triggers off",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* sql`PRAGMA recursive_triggers = OFF`;
      yield* insertParent();
      yield* sql`INSERT INTO orchestration_v2_attachment_cleanup_observations ${sql.insert(observationRow)}`;
      for (const changed of [
        { effect_id: "changed" },
        { ordinal: 1 },
        { binding_sha256: "b".repeat(64) },
        { canonical_task_json: "{}" },
        { observation_json: "{}" },
        { correlation_json: "{}" },
        { recorded_at: "changed" },
      ])
        assert.equal(
          (yield* Effect.result(
            sql`UPDATE orchestration_v2_attachment_cleanup_observations ${sql.update(changed)}`,
          ))._tag,
          "Failure",
        );
      for (const mutation of [
        sql`DELETE FROM orchestration_v2_attachment_cleanup_observations`,
        sql`DELETE FROM orchestration_v2_effect_outbox WHERE effect_id = ${effectId}`,
        sql`INSERT INTO orchestration_v2_attachment_cleanup_observations ${sql.insert(observationRow)}`,
        sql`INSERT OR REPLACE INTO orchestration_v2_attachment_cleanup_observations ${sql.insert({
          ...observationRow,
          observation_json: '{"fixture":"changed observation"}',
          correlation_json: '{"fixture":"changed correlation"}',
        })}`,
      ])
        assert.equal((yield* Effect.result(mutation))._tag, "Failure");
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_attachment_cleanup_observations`, [
        observationRow,
      ]);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "failed outer write rolls back new outbox parent and observation without retaining partial evidence",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      const injectedCause = new Error("synthetic attachment observation transaction failure");
      const result = yield* Effect.result(
        sql.withTransaction(
          Effect.gen(function* () {
            yield* insertParent();
            yield* sql`INSERT INTO orchestration_v2_attachment_cleanup_observations ${sql.insert(observationRow)}`;
            return yield* Effect.fail(new SyntheticObservationFailure({ cause: injectedCause }));
          }),
        ),
      );
      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") {
        assert.equal(result.failure._tag, "SyntheticObservationFailure");
        if (result.failure._tag === "SyntheticObservationFailure") {
          assert.strictEqual(result.failure.cause, injectedCause);
        }
      }
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_effect_outbox`, []);
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_attachment_cleanup_observations`,
        [],
      );
    }).pipe(Effect.provide(memory)),
);

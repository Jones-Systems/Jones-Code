import { assert, it } from "@effect/vitest";
import { OrchestrationV2Command } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SchemaGetter from "effect/SchemaGetter";
import * as SchemaIssue from "effect/SchemaIssue";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { migrationManifest, runMigrations } from "../Migrations.ts";
import {
  nativeCreationCanonicalJson,
  nativeCreationSha256,
} from "../../orchestration-v2/NativeCreationPreparation.ts";

// Keep native JSON exceptions as defects and retain undefined serialization results.
const FixtureJsonText = Schema.Unknown.pipe(
  Schema.decodeTo(Schema.UndefinedOr(Schema.String), {
    decode: SchemaGetter.onSome<string | undefined, unknown>((input, options) => {
      try {
        return Effect.succeed(Option.some(JSON.stringify(input)));
      } catch (cause) {
        return Effect.fail(
          new SchemaIssue.InvalidValue({ nativeJsonCause: cause }, input, options),
        );
      }
    }),
    encode: SchemaGetter.forbiddenEncoding,
  }),
);
const encodeFixtureJson = Schema.decodeEffect(FixtureJsonText);

function dieNativeJsonCause(error: Schema.SchemaError) {
  let issue = error.issue;
  while (issue._tag === "Encoding") issue = issue.issue;
  if (
    issue._tag === "InvalidValue" &&
    issue.annotations !== undefined &&
    Object.hasOwn(issue.annotations, "nativeJsonCause")
  ) {
    return Effect.die(issue.annotations["nativeJsonCause"]);
  }
  return Effect.die(error);
}

const CleanupStartOrdinalJson = Schema.String.pipe(
  Schema.decodeTo(Schema.Unknown, {
    decode: SchemaGetter.onSome<unknown, string>((input, options) => {
      try {
        const ordinal: unknown = JSON.parse(input).cleanupStartOrdinal;
        return Effect.succeed(Option.some(ordinal));
      } catch (cause) {
        return Effect.fail(
          new SchemaIssue.InvalidValue({ nativeJsonCause: cause }, input, options),
        );
      }
    }),
    encode: SchemaGetter.forbiddenEncoding,
  }),
);
const decodeCleanupStartOrdinal = Schema.decodeEffect(CleanupStartOrdinalJson);

const memory = NodeSqliteClient.layer({ filename: ":memory:" });
const digest = "a".repeat(64);
const nativeIdentity = {
  command_id: "command",
  kind: "guarded_message_dispatch",
  version: 2,
  command_type: "thread.message.send",
  aggregate_kind: "thread",
  aggregate_id: "thread",
  normalized_command_digest: digest,
  binding_digest: digest,
};

it.effect("records Jones007 after upstream56 without changing released migration identities", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: 56 });
    const history = yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`;
    assert.deepEqual(yield* runMigrations(), []);
    assert.deepEqual(yield* runMigrations(), []);
    assert.deepEqual(
      yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`,
      history,
    );
    assert.deepEqual(
      history.map((row): readonly [unknown, unknown] => [row.migration_id, row.name]),
      migrationManifest,
    );
    assert.deepEqual(
      yield* sql`SELECT migration_id, name FROM jones_sql_migrations WHERE migration_id = 7`,
      [{ migration_id: 7, name: "V2NativeAcceptance" }],
    );
  }).pipe(Effect.provide(memory)),
);

it.effect(
  "native V2 identity validates its exact carrier and cannot be changed, deleted or replaced",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      for (const [index, kind] of [
        "guarded_message_dispatch",
        "native_creation_stage",
        "workstream_settlement",
      ].entries()) {
        const commandId = `command-${index}`;
        yield* sql`INSERT INTO orchestration_command_receipts
        (command_id, command_type, aggregate_kind, aggregate_id, accepted_at, result_sequence, status)
        VALUES (${commandId}, 'thread.message.send', 'thread', 'thread', '2026-10-02T00:00:00Z', 1, 'accepted')`;
        yield* sql`INSERT INTO orchestration_v2_native_command_identities ${sql.insert({ ...nativeIdentity, command_id: commandId, kind })}`;
      }
      const original =
        yield* sql`SELECT * FROM orchestration_v2_native_command_identities ORDER BY command_id`;
      for (const mutation of [
        sql`UPDATE orchestration_v2_native_command_identities SET binding_digest = ${"b".repeat(64)}`,
        sql`DELETE FROM orchestration_v2_native_command_identities`,
        sql`INSERT OR REPLACE INTO orchestration_v2_native_command_identities ${sql.insert({ ...nativeIdentity, command_id: "command-0" })}`,
        sql`DELETE FROM orchestration_command_receipts WHERE command_id = 'command-0'`,
        sql`INSERT INTO orchestration_v2_native_command_identities ${sql.insert(nativeIdentity)}`,
      ]) {
        assert.equal((yield* Effect.result(mutation))._tag, "Failure");
      }
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_native_command_identities ORDER BY command_id`,
        original,
      );
      for (const [index, invalid] of [
        { kind: "legacy" },
        { version: 1 },
        { aggregate_kind: "project" },
        { normalized_command_digest: "A".repeat(64) },
        { normalized_command_digest: "g".repeat(64) },
        { normalized_command_digest: "a".repeat(63) },
        { binding_digest: "A".repeat(64) },
        { binding_digest: "g".repeat(64) },
        { binding_digest: "a".repeat(65) },
      ].entries()) {
        const commandId = `invalid-${index}`;
        yield* sql`INSERT INTO orchestration_command_receipts
        (command_id, aggregate_kind, aggregate_id, accepted_at, result_sequence, status)
        VALUES (${commandId}, 'thread', 'thread', '2026-10-02T00:00:00Z', 0, 'rejected')`;
        assert.equal(
          (yield* Effect.result(
            sql`INSERT INTO orchestration_v2_native_command_identities ${sql.insert({ ...nativeIdentity, ...invalid, command_id: commandId })}`,
          ))._tag,
          "Failure",
        );
      }
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_native_command_identities ORDER BY command_id`,
        original,
      );
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "unknown effect holds retain evidence and require an existing effect and positive claim attempt",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      const hold = {
        effect_id: "effect",
        worker_id: "worker",
        operation_id: "operation",
        evidence_json: '{"disposition":"unknown"}',
        expected_attempt: 1,
        held_at: "2026-10-02T00:00:00Z",
      };
      const insertEffect = (effectId: string) => sql`INSERT INTO orchestration_v2_effect_outbox
      (effect_id, command_id, thread_id, effect_type, payload_json, status, attempt_count, available_at, lease_owner, created_at, updated_at)
      VALUES (${effectId}, 'command', 'thread', 'provider.turn.start', '{}', 'running', 1, '2026-10-02', 'worker', '2026-10-02', '2026-10-02')`;
      yield* insertEffect("effect");
      yield* sql`INSERT INTO orchestration_v2_unknown_effect_holds ${sql.insert(hold)}`;
      for (const mutation of [
        sql`UPDATE orchestration_v2_unknown_effect_holds SET operation_id = 'other'`,
        sql`DELETE FROM orchestration_v2_unknown_effect_holds`,
        sql`INSERT OR REPLACE INTO orchestration_v2_unknown_effect_holds ${sql.insert({ ...hold, worker_id: "other" })}`,
        sql`DELETE FROM orchestration_v2_effect_outbox WHERE effect_id = 'effect'`,
        sql`INSERT INTO orchestration_v2_unknown_effect_holds ${sql.insert({ ...hold, effect_id: "missing" })}`,
      ]) {
        assert.equal((yield* Effect.result(mutation))._tag, "Failure");
      }
      for (const [index, invalid] of [
        { expected_attempt: 0 },
        { expected_attempt: -1 },
        { evidence_json: "invalid" },
      ].entries()) {
        const effectId = `invalid-${index}`;
        yield* insertEffect(effectId);
        assert.equal(
          (yield* Effect.result(
            sql`INSERT INTO orchestration_v2_unknown_effect_holds ${sql.insert({ ...hold, ...invalid, effect_id: effectId })}`,
          ))._tag,
          "Failure",
        );
      }
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_unknown_effect_holds`, [hold]);
    }).pipe(Effect.provide(memory)),
);

it.effect("failed acceptance transaction leaves neither receipt nor companion identity", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations();
    const result = yield* Effect.result(
      sql.withTransaction(
        Effect.gen(function* () {
          yield* sql`INSERT INTO orchestration_command_receipts
        (command_id, aggregate_kind, aggregate_id, accepted_at, result_sequence, status)
        VALUES ('command', 'thread', 'thread', '2026-10-02', 1, 'accepted')`;
          yield* sql`INSERT INTO orchestration_v2_native_command_identities ${sql.insert(nativeIdentity)}`;
          return yield* Effect.fail("injected after identity insertion");
        }),
      ),
    );
    assert.equal(result._tag, "Failure");
    assert.deepEqual(yield* sql`SELECT * FROM orchestration_command_receipts`, []);
    assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_native_command_identities`, []);
  }).pipe(Effect.provide(memory)),
);

it.effect(
  "runtime evidence starts empty and rejects missing identities, nonpositive revisions and malformed observations",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_provider_runtime_evidence`, []);
      const registration = {
        thread_id: "thread",
        provider_thread_id: "provider-thread",
        provider_session_id: "provider-session",
        provider_instance_id: "instance",
        driver: "codex",
        native_thread_id: null,
        runtime_generation: "actual-process-generation",
        evidence_revision: 1,
        observation_json: null,
        registered_at: "2026-10-02T00:00:00Z",
      };
      yield* sql`INSERT INTO orchestration_v2_provider_runtime_evidence ${sql.insert(registration)}`;
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_provider_runtime_evidence`, [
        registration,
      ]);
      for (const [index, invalid] of [
        { provider_thread_id: null },
        { provider_session_id: null },
        { provider_instance_id: null },
        { driver: null },
        { runtime_generation: null },
        { evidence_revision: 0 },
        { evidence_revision: -1 },
        { observation_json: "invalid" },
      ].entries()) {
        assert.equal(
          (yield* Effect.result(
            sql`INSERT INTO orchestration_v2_provider_runtime_evidence ${sql.insert({ ...registration, ...invalid, thread_id: `invalid-${index}` })}`,
          ))._tag,
          "Failure",
        );
      }
      yield* sql`UPDATE orchestration_v2_provider_runtime_evidence
      SET runtime_generation = 'new-actual-generation', evidence_revision = 2, observation_json = NULL
      WHERE thread_id = 'thread' AND runtime_generation = 'actual-process-generation' AND evidence_revision = 1`;
      assert.deepEqual(
        yield* sql`SELECT runtime_generation, evidence_revision, observation_json FROM orchestration_v2_provider_runtime_evidence`,
        [
          {
            runtime_generation: "new-actual-generation",
            evidence_revision: 2,
            observation_json: null,
          },
        ],
      );
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "legacy dispositions preserve unknown, unsupported and qualified evidence without promotion or deletion",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      const dispositions = [
        {
          thread_id: "unknown",
          provenance: "legacy_row",
          qualification_json: '{"type":"unknown","reason":"stopped_proof_missing"}',
          evidence_json: null,
          imported_at: "2026-10-02",
        },
        {
          thread_id: "unsupported",
          provenance: "native_import",
          qualification_json: '{"type":"unsupported","reason":"store_incompatible"}',
          evidence_json: "{}",
          imported_at: "2026-10-02",
        },
        {
          thread_id: "qualified",
          provenance: "legacy_row",
          qualification_json:
            '{"type":"qualified","nativeThreadId":"historical-native-thread","continuationKey":"historical-key"}',
          evidence_json: '{"sourceHome":"/fixture/source-home"}',
          imported_at: "2026-10-02",
        },
      ];
      for (const disposition of dispositions) {
        yield* sql`INSERT INTO orchestration_v2_legacy_continuation_dispositions ${sql.insert(disposition)}`;
      }
      const original =
        yield* sql`SELECT * FROM orchestration_v2_legacy_continuation_dispositions ORDER BY thread_id`;
      for (const mutation of [
        sql`UPDATE orchestration_v2_legacy_continuation_dispositions SET qualification_json = '{"type":"qualified","nativeThreadId":"historical-native-thread","continuationKey":"historical-key"}' WHERE thread_id = 'unknown'`,
        sql`DELETE FROM orchestration_v2_legacy_continuation_dispositions`,
        sql`INSERT OR REPLACE INTO orchestration_v2_legacy_continuation_dispositions ${sql.insert({ ...dispositions[0]!, qualification_json: '{"type":"qualified","nativeThreadId":"historical-native-thread","continuationKey":"historical-key"}' })}`,
      ]) {
        assert.equal((yield* Effect.result(mutation))._tag, "Failure");
      }
      for (const [index, invalid] of [
        { provenance: "synthetic-session" },
        { qualification_json: "invalid" },
        { evidence_json: "invalid" },
      ].entries()) {
        assert.equal(
          (yield* Effect.result(
            sql`INSERT INTO orchestration_v2_legacy_continuation_dispositions ${sql.insert({ ...dispositions[0]!, ...invalid, thread_id: `invalid-${index}` })}`,
          ))._tag,
          "Failure",
        );
      }
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_legacy_continuation_dispositions ORDER BY thread_id`,
        original,
      );
    }).pipe(Effect.provide(memory)),
);

const restartMarker = {
  marker_id: "restart-marker",
  thread_id: "thread",
  project_id: "project",
  source_run_id: "source-run",
  source_run_attempt_id: "source-attempt",
  binding_json: '{"runtimeGeneration":"source-generation","providerSessionId":"source-session"}',
  evidence_revision: 1,
  status: "dormant",
  created_at: "2026-10-02T00:00:00Z",
  updated_at: "2026-10-02T00:00:00Z",
  effect_id: null,
};

it.effect(
  "restart markers begin empty and require the complete source tuple, binding and status-effect correspondence",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_restart_continuation_markers`, []);
      for (const [index, invalid] of [
        { marker_id: null },
        { thread_id: null },
        { project_id: null },
        { source_run_id: null },
        { source_run_attempt_id: null },
        { binding_json: null },
        { binding_json: "invalid" },
        { evidence_revision: null },
        { evidence_revision: 0 },
        { evidence_revision: -1 },
        { status: null },
        { status: "pending" },
        { created_at: null },
        { updated_at: null },
        { status: "released", effect_id: null },
        { status: "released", effect_id: "missing-effect" },
        { status: "dormant", effect_id: "effect" },
        { status: "cleared", effect_id: "effect" },
      ].entries()) {
        assert.equal(
          (yield* Effect.result(
            sql`INSERT INTO orchestration_v2_restart_continuation_markers ${sql.insert({ ...restartMarker, marker_id: `invalid-${index}`, ...invalid })}`,
          ))._tag,
          "Failure",
        );
      }
      yield* sql`INSERT INTO orchestration_v2_restart_continuation_markers ${sql.insert(restartMarker)}`;
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_restart_continuation_markers`, [
        restartMarker,
      ]);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_effect_outbox`, []);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "restart source identity is immutable and competing dormant markers cannot replace it",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA recursive_triggers = OFF`;
      yield* sql`INSERT INTO orchestration_v2_restart_continuation_markers ${sql.insert(restartMarker)}`;
      for (const mutation of [
        { marker_id: "other-marker" },
        { thread_id: "other-thread" },
        { project_id: "other-project" },
        { source_run_id: "other-run" },
        { source_run_attempt_id: "other-attempt" },
        { binding_json: '{"runtimeGeneration":"replacement-generation"}' },
        { evidence_revision: 2 },
        { created_at: "2026-10-03T00:00:00Z" },
      ]) {
        assert.equal(
          (yield* Effect.result(
            sql`UPDATE orchestration_v2_restart_continuation_markers ${sql.update({ ...mutation, status: "cleared", updated_at: "2026-10-03T00:00:00Z" })} WHERE marker_id = 'restart-marker'`,
          ))._tag,
          "Failure",
        );
      }
      for (const mutation of [
        sql`DELETE FROM orchestration_v2_restart_continuation_markers`,
        sql`INSERT OR REPLACE INTO orchestration_v2_restart_continuation_markers ${sql.insert({ ...restartMarker, source_run_attempt_id: "other-attempt" })}`,
        sql`INSERT OR REPLACE INTO orchestration_v2_restart_continuation_markers ${sql.insert({ ...restartMarker, marker_id: "competing-marker" })}`,
        sql`INSERT INTO orchestration_v2_restart_continuation_markers ${sql.insert({ ...restartMarker, marker_id: "competing-marker" })}`,
        sql`UPDATE orchestration_v2_restart_continuation_markers SET updated_at = 'later' WHERE marker_id = 'restart-marker'`,
      ]) {
        assert.equal((yield* Effect.result(mutation))._tag, "Failure");
      }
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_restart_continuation_markers`, [
        restartMarker,
      ]);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "restart release and its continuation effect commit or roll back together and terminal markers cannot change",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* sql`INSERT INTO orchestration_v2_restart_continuation_markers ${sql.insert(restartMarker)}`;
      const release = Effect.gen(function* () {
        yield* sql`INSERT INTO orchestration_v2_effect_outbox
        (effect_id, command_id, thread_id, effect_type, payload_json, status, available_at, created_at, updated_at)
        VALUES ('continuation-effect', 'restart-command', 'thread', 'provider-runtime.continue', '{}', 'pending', '2026-10-03', '2026-10-03', '2026-10-03')`;
        yield* sql`UPDATE orchestration_v2_restart_continuation_markers
        SET status = 'released', updated_at = '2026-10-03', effect_id = 'continuation-effect'
        WHERE marker_id = ${restartMarker.marker_id} AND thread_id = ${restartMarker.thread_id}
          AND project_id = ${restartMarker.project_id} AND source_run_id = ${restartMarker.source_run_id}
          AND source_run_attempt_id = ${restartMarker.source_run_attempt_id}
          AND binding_json = ${restartMarker.binding_json} AND evidence_revision = ${restartMarker.evidence_revision}
          AND created_at = ${restartMarker.created_at} AND status = 'dormant'`;
      });
      assert.equal(
        (yield* Effect.result(
          sql.withTransaction(release.pipe(Effect.andThen(Effect.fail("injected after release")))),
        ))._tag,
        "Failure",
      );
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_restart_continuation_markers`, [
        restartMarker,
      ]);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_effect_outbox`, []);
      yield* sql.withTransaction(release);
      const released = {
        ...restartMarker,
        status: "released",
        updated_at: "2026-10-03",
        effect_id: "continuation-effect",
      };
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_restart_continuation_markers`, [
        released,
      ]);
      assert.equal((yield* sql`SELECT * FROM orchestration_v2_effect_outbox`).length, 1);
      for (const mutation of [
        sql`UPDATE orchestration_v2_restart_continuation_markers SET status = 'dormant', effect_id = NULL`,
        sql`UPDATE orchestration_v2_restart_continuation_markers SET status = 'cleared', effect_id = NULL`,
        sql`UPDATE orchestration_v2_restart_continuation_markers SET updated_at = 'later'`,
        sql`DELETE FROM orchestration_v2_restart_continuation_markers`,
        sql`INSERT OR REPLACE INTO orchestration_v2_restart_continuation_markers ${sql.insert(restartMarker)}`,
        sql`DELETE FROM orchestration_v2_effect_outbox WHERE effect_id = 'continuation-effect'`,
      ]) {
        assert.equal((yield* Effect.result(mutation))._tag, "Failure");
      }
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_restart_continuation_markers`, [
        released,
      ]);
    }).pipe(Effect.provide(memory)),
);

it.effect("a captured old restart reference cannot clear a later marker for the same thread", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations();
    yield* sql`INSERT INTO orchestration_v2_restart_continuation_markers ${sql.insert(restartMarker)}`;
    yield* sql`UPDATE orchestration_v2_restart_continuation_markers
      SET status = 'cleared', updated_at = '2026-10-03'
      WHERE marker_id = ${restartMarker.marker_id} AND thread_id = ${restartMarker.thread_id}
        AND project_id = ${restartMarker.project_id} AND source_run_id = ${restartMarker.source_run_id}
        AND source_run_attempt_id = ${restartMarker.source_run_attempt_id}
        AND binding_json = ${restartMarker.binding_json} AND evidence_revision = ${restartMarker.evidence_revision}
        AND created_at = ${restartMarker.created_at} AND status = 'dormant'`;
    const newerMarker = {
      ...restartMarker,
      marker_id: "new-marker",
      source_run_id: "new-run",
      source_run_attempt_id: "new-attempt",
      binding_json: '{"runtimeGeneration":"new-generation","providerSessionId":"new-session"}',
      evidence_revision: 2,
      created_at: "2026-10-03",
      updated_at: "2026-10-03",
    };
    yield* sql`INSERT INTO orchestration_v2_restart_continuation_markers ${sql.insert(newerMarker)}`;
    yield* sql`UPDATE orchestration_v2_restart_continuation_markers
      SET status = 'cleared', updated_at = '2026-10-04'
      WHERE marker_id = ${restartMarker.marker_id} AND thread_id = ${restartMarker.thread_id}
        AND project_id = ${restartMarker.project_id} AND source_run_id = ${restartMarker.source_run_id}
        AND source_run_attempt_id = ${restartMarker.source_run_attempt_id}
        AND binding_json = ${restartMarker.binding_json} AND evidence_revision = ${restartMarker.evidence_revision}
        AND created_at = ${restartMarker.created_at} AND status = 'dormant'`;
    assert.deepEqual(yield* sql`SELECT changes() AS count`, [{ count: 0 }]);
    assert.deepEqual(
      yield* sql`SELECT * FROM orchestration_v2_restart_continuation_markers WHERE marker_id = 'new-marker'`,
      [newerMarker],
    );
    assert.equal(
      (yield* Effect.result(
        sql`INSERT OR REPLACE INTO orchestration_v2_restart_continuation_markers ${sql.insert({ ...restartMarker, thread_id: "other-thread" })}`,
      ))._tag,
      "Failure",
    );
    assert.equal(
      (yield* Effect.result(
        sql`UPDATE orchestration_v2_restart_continuation_markers SET status = 'released', effect_id = 'effect' WHERE marker_id = ${restartMarker.marker_id}`,
      ))._tag,
      "Failure",
    );
    assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_effect_outbox`, []);
  }).pipe(Effect.provide(memory)),
);

const providerContinuationSource = {
  source_id: "b".repeat(64),
  thread_id: "thread",
  provider_thread_id: "provider-thread",
  provider_session_id: "provider-session",
  provider_instance_id: "instance",
  driver: "codex",
  native_thread_id: "native-thread",
  runtime_generation: "actual-process-generation",
  continuation_key: "historical-source-key",
  registered_at: "2026-10-02T00:00:00Z",
};

it.effect(
  "continuation source storage stays empty without a native-bound source and round trips a complete source tuple",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_provider_continuation_sources`,
        [],
      );
      yield* sql`INSERT INTO orchestration_v2_provider_runtime_evidence
      (thread_id, provider_thread_id, provider_session_id, provider_instance_id, driver, runtime_generation, evidence_revision, registered_at)
      VALUES ('thread', 'provider-thread', 'provider-session', 'instance', 'codex', 'actual-process-generation', 1, '2026-10-02')`;
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_provider_continuation_sources`,
        [],
      );
      yield* sql`INSERT INTO orchestration_v2_provider_continuation_sources ${sql.insert(providerContinuationSource)}`;
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_provider_continuation_sources`, [
        providerContinuationSource,
      ]);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "continuation source constraints reject missing native identity, malformed source digests and empty generation or key",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      for (const [index, invalid] of [
        { source_id: null },
        { source_id: "B".repeat(64) },
        { source_id: "g".repeat(64) },
        { source_id: "b".repeat(63) },
        { source_id: "b".repeat(65) },
        { thread_id: null },
        { thread_id: "" },
        { provider_thread_id: null },
        { provider_thread_id: "" },
        { provider_session_id: null },
        { provider_session_id: "" },
        { provider_instance_id: null },
        { provider_instance_id: "" },
        { driver: null },
        { driver: "" },
        { native_thread_id: null },
        { native_thread_id: "" },
        { runtime_generation: null },
        { runtime_generation: "" },
        { runtime_generation: "   " },
        { continuation_key: null },
        { continuation_key: "" },
        { continuation_key: "   " },
        { registered_at: null },
      ].entries()) {
        assert.equal(
          (yield* Effect.result(
            sql`INSERT INTO orchestration_v2_provider_continuation_sources ${sql.insert({
              ...providerContinuationSource,
              source_id: (index + 100).toString(16).padStart(64, "0"),
              ...invalid,
            })}`,
          ))._tag,
          "Failure",
        );
      }
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_provider_continuation_sources`,
        [],
      );
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "continuation source identity and historical key cannot update, delete or replace by source ID or complete tuple",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA recursive_triggers = OFF`;
      yield* sql`INSERT INTO orchestration_v2_provider_continuation_sources ${sql.insert(providerContinuationSource)}`;
      for (const mutation of [
        { source_id: "c".repeat(64) },
        { thread_id: "other-thread" },
        { provider_thread_id: "other-provider-thread" },
        { provider_session_id: "other-provider-session" },
        { provider_instance_id: "other-instance" },
        { driver: "claudeAgent" },
        { native_thread_id: "other-native-thread" },
        { runtime_generation: "other-process-generation" },
        { continuation_key: "replacement-settings-key" },
        { registered_at: "2026-10-03T00:00:00Z" },
      ]) {
        assert.equal(
          (yield* Effect.result(
            sql`UPDATE orchestration_v2_provider_continuation_sources ${sql.update(mutation)} WHERE source_id = ${providerContinuationSource.source_id}`,
          ))._tag,
          "Failure",
        );
      }
      for (const mutation of [
        sql`DELETE FROM orchestration_v2_provider_continuation_sources`,
        sql`INSERT OR REPLACE INTO orchestration_v2_provider_continuation_sources ${sql.insert({ ...providerContinuationSource, native_thread_id: "other-native-thread" })}`,
        sql`INSERT OR REPLACE INTO orchestration_v2_provider_continuation_sources ${sql.insert({ ...providerContinuationSource, source_id: "c".repeat(64), continuation_key: "replacement-settings-key" })}`,
        sql`INSERT INTO orchestration_v2_provider_continuation_sources ${sql.insert({ ...providerContinuationSource, source_id: "c".repeat(64) })}`,
      ]) {
        assert.equal((yield* Effect.result(mutation))._tag, "Failure");
      }
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_provider_continuation_sources`, [
        providerContinuationSource,
      ]);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "historical continuation sources survive current process replacement and detach while tuple mismatches return no row",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* sql`INSERT INTO orchestration_v2_provider_runtime_evidence
      (thread_id, provider_thread_id, provider_session_id, provider_instance_id, driver, native_thread_id, runtime_generation, evidence_revision, registered_at)
      VALUES ('thread', 'provider-thread', 'provider-session', 'instance', 'codex', 'native-thread', 'actual-process-generation', 1, '2026-10-02')`;
      yield* sql`INSERT INTO orchestration_v2_provider_continuation_sources ${sql.insert(providerContinuationSource)}`;
      yield* sql`UPDATE orchestration_v2_provider_runtime_evidence
      SET provider_session_id = 'new-session', native_thread_id = 'new-native-thread',
        runtime_generation = 'new-process-generation', evidence_revision = 2, observation_json = NULL
      WHERE thread_id = 'thread' AND evidence_revision = 1`;
      const newerSource = {
        ...providerContinuationSource,
        source_id: "c".repeat(64),
        provider_session_id: "new-session",
        native_thread_id: "new-native-thread",
        runtime_generation: "new-process-generation",
        continuation_key: "new-observed-source-key",
        registered_at: "2026-10-03T00:00:00Z",
      };
      yield* sql`INSERT INTO orchestration_v2_provider_continuation_sources ${sql.insert(newerSource)}`;
      yield* sql`DELETE FROM orchestration_v2_provider_runtime_evidence WHERE thread_id = 'thread'`;
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_provider_continuation_sources ORDER BY source_id`,
        [providerContinuationSource, newerSource],
      );
      const readSource = (binding: typeof providerContinuationSource) => sql`
      SELECT * FROM orchestration_v2_provider_continuation_sources
      WHERE thread_id = ${binding.thread_id} AND provider_thread_id = ${binding.provider_thread_id}
        AND provider_session_id = ${binding.provider_session_id} AND provider_instance_id = ${binding.provider_instance_id}
        AND driver = ${binding.driver} AND native_thread_id = ${binding.native_thread_id}
        AND runtime_generation = ${binding.runtime_generation}
    `;
      assert.deepEqual(yield* readSource(providerContinuationSource), [providerContinuationSource]);
      assert.deepEqual(yield* readSource(newerSource), [newerSource]);
      for (const mismatch of [
        { thread_id: "other-thread" },
        { provider_thread_id: "other-provider-thread" },
        { provider_session_id: "new-session" },
        { provider_instance_id: "other-instance" },
        { driver: "claudeAgent" },
        { native_thread_id: "new-native-thread" },
        { runtime_generation: "new-process-generation" },
      ]) {
        assert.deepEqual(yield* readSource({ ...providerContinuationSource, ...mismatch }), []);
      }
    }).pipe(Effect.provide(memory)),
);

const nativeImportSeal = {
  schema_version: 1,
  thread_id: "sealed-import",
  project_id: "project",
  provenance: "native_import",
  source_identity_json: JSON.stringify({
    provider: "codex",
    providerInstanceId: "instance",
    providerSessionId: "native-source-session",
    filePath: "/fixture/native-session.jsonl",
    size: 128,
    mtimeMs: 0,
    device: 1,
    inode: 2,
    birthtimeMs: 0,
  }),
  parser_policy: "agent_session_visible_messages_v1",
  message_count: 1,
  events_sha256: "d".repeat(64),
  event_basis_json: JSON.stringify([
    { eventId: "imported-message", sequence: 2 },
    { eventId: "imported-turn-item", sequence: 3 },
  ]),
  birth_event_id: "imported-birth",
  birth_sequence: 1,
  imported_at: "2026-10-02T00:00:00Z",
};

const insertNativeImportSealParents = (input: {
  readonly threadId: string;
  readonly birthEventId: string;
  readonly birthSequence: number;
}) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO orchestration_v2_legacy_continuation_dispositions
    (thread_id, provenance, qualification_json, imported_at)
    VALUES (${input.threadId}, 'native_import', '{"type":"unknown","reason":"stopped_proof_missing"}', '2026-10-02')`;
    yield* sql`INSERT INTO orchestration_events
    (sequence, event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, actor_kind, payload_json, metadata_json, application_event_version)
    VALUES (${input.birthSequence}, ${input.birthEventId}, 'thread', ${input.threadId}, 1, 'thread.created', '2026-10-02', 'server', '{}', '{}', 2)`;
  });

const insertDefaultNativeImportSealParents = insertNativeImportSealParents({
  threadId: nativeImportSeal.thread_id,
  birthEventId: nativeImportSeal.birth_event_id,
  birthSequence: nativeImportSeal.birth_sequence,
});

it.effect(
  "native import seal storage preserves absent historical seals and accepts the visible parser bounds",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* sql`INSERT INTO orchestration_v2_legacy_continuation_dispositions
      (thread_id, provenance, qualification_json, imported_at)
      VALUES ('older-import', 'native_import', '{"type":"unknown","reason":"stopped_proof_missing"}', '2026-10-01')`;
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_native_import_transcript_seals`,
        [],
      );
      for (const messageCount of [1, 200]) {
        const seal = {
          ...nativeImportSeal,
          thread_id: `sealed-import-${messageCount}`,
          message_count: messageCount,
          birth_event_id: `imported-birth-${messageCount}`,
          birth_sequence: messageCount,
          event_basis_json: yield* encodeFixtureJson(
            Array.from({ length: messageCount * 2 }, (_, index) => ({
              eventId: `import-${messageCount}-event-${index}`,
              sequence: messageCount + index + 1,
            })),
          ).pipe(Effect.catch(dieNativeJsonCause)),
        };
        yield* insertNativeImportSealParents({
          threadId: seal.thread_id,
          birthEventId: seal.birth_event_id,
          birthSequence: seal.birth_sequence,
        });
        yield* sql`INSERT INTO orchestration_v2_native_import_transcript_seals ${sql.insert(seal)}`;
        assert.deepEqual(
          yield* sql`SELECT * FROM orchestration_v2_native_import_transcript_seals WHERE thread_id = ${seal.thread_id}`,
          [seal],
        );
      }
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_native_import_transcript_seals WHERE thread_id = 'older-import'`,
        [],
      );
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "native import seal constraints reject missing birth or disposition evidence, wrong policy and message-basis count mismatch",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* insertDefaultNativeImportSealParents;
      for (const invalid of [
        { schema_version: null },
        { schema_version: 2 },
        { thread_id: null },
        { thread_id: "missing-disposition" },
        { project_id: "" },
        { provenance: "legacy_row" },
        { source_identity_json: "invalid" },
        { parser_policy: "raw_full_transcript" },
        { message_count: 0 },
        { message_count: 201 },
        { events_sha256: "D".repeat(64) },
        { events_sha256: "g".repeat(64) },
        { events_sha256: "d".repeat(63) },
        { events_sha256: "d".repeat(65) },
        { event_basis_json: null },
        { event_basis_json: "invalid" },
        { event_basis_json: "{}" },
        { event_basis_json: "[]" },
        { event_basis_json: '[{"eventId":"only-one","sequence":2}]' },
        {
          event_basis_json:
            '[{"eventId":"one","sequence":2},{"eventId":"two","sequence":3},{"eventId":"three","sequence":4}]',
        },
        { birth_event_id: "missing-birth" },
        { birth_sequence: 0 },
        { birth_sequence: -1 },
        { birth_sequence: 99 },
        { imported_at: null },
        { imported_at: "" },
      ]) {
        assert.equal(
          (yield* Effect.result(
            sql`INSERT INTO orchestration_v2_native_import_transcript_seals ${sql.insert({ ...nativeImportSeal, ...invalid })}`,
          ))._tag,
          "Failure",
        );
      }
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_native_import_transcript_seals`,
        [],
      );
      yield* sql`INSERT INTO orchestration_v2_native_import_transcript_seals ${sql.insert(nativeImportSeal)}`;
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_native_import_transcript_seals`, [
        nativeImportSeal,
      ]);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "native import seals and their source, parser and committed basis fields cannot change, disappear or be replaced",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* sql`PRAGMA recursive_triggers = OFF`;
      yield* insertDefaultNativeImportSealParents;
      yield* sql`INSERT INTO orchestration_v2_native_import_transcript_seals ${sql.insert(nativeImportSeal)}`;
      for (const mutation of [
        { schema_version: 1 },
        { thread_id: "other-thread" },
        { project_id: "other-project" },
        { provenance: "native_import" },
        { source_identity_json: "{}" },
        { parser_policy: "agent_session_visible_messages_v1" },
        { message_count: 2 },
        { events_sha256: "e".repeat(64) },
        {
          event_basis_json:
            '[{"eventId":"other-one","sequence":2},{"eventId":"other-two","sequence":3}]',
        },
        { birth_event_id: "other-birth" },
        { birth_sequence: 2 },
        { imported_at: "2026-10-03" },
      ]) {
        assert.equal(
          (yield* Effect.result(
            sql`UPDATE orchestration_v2_native_import_transcript_seals ${sql.update(mutation)} WHERE thread_id = ${nativeImportSeal.thread_id}`,
          ))._tag,
          "Failure",
        );
      }
      for (const mutation of [
        sql`DELETE FROM orchestration_v2_native_import_transcript_seals`,
        sql`INSERT OR REPLACE INTO orchestration_v2_native_import_transcript_seals ${sql.insert({ ...nativeImportSeal, events_sha256: "e".repeat(64) })}`,
        sql`DELETE FROM orchestration_events WHERE event_id = ${nativeImportSeal.birth_event_id}`,
      ]) {
        assert.equal((yield* Effect.result(mutation))._tag, "Failure");
      }
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_native_import_transcript_seals`, [
        nativeImportSeal,
      ]);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "native import parent evidence and seal roll back together and a retry writes one permanent seal",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      const insertSeal = Effect.gen(function* () {
        yield* insertDefaultNativeImportSealParents;
        yield* sql`INSERT INTO orchestration_v2_native_import_transcript_seals ${sql.insert(nativeImportSeal)}`;
      });
      assert.equal(
        (yield* Effect.result(
          sql.withTransaction(
            insertSeal.pipe(Effect.andThen(Effect.fail("injected after seal insertion"))),
          ),
        ))._tag,
        "Failure",
      );
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_native_import_transcript_seals`,
        [],
      );
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_legacy_continuation_dispositions`,
        [],
      );
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_events`, []);
      yield* sql.withTransaction(insertSeal);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_native_import_transcript_seals`, [
        nativeImportSeal,
      ]);
      assert.equal(
        (yield* Effect.result(
          sql`INSERT OR REPLACE INTO orchestration_v2_native_import_transcript_seals ${sql.insert(nativeImportSeal)}`,
        ))._tag,
        "Failure",
      );
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_native_import_transcript_seals`, [
        nativeImportSeal,
      ]);
    }).pipe(Effect.provide(memory)),
);

const nativeEffectConfirmation = {
  effect_id: "confirmed-effect",
  command_id: "confirmation-command",
  thread_id: "confirmation-thread",
  worker_id: "worker",
  operation_id: "native-operation",
  run_id: "run",
  run_attempt_id: "run-attempt",
  expected_attempt: 1,
  binding_json: JSON.stringify({
    threadId: "confirmation-thread",
    providerThreadId: "provider-thread",
    providerSessionId: "provider-session",
    instanceId: "instance",
    driver: "codex",
    nativeThreadId: "native-thread",
    runtimeGeneration: "actual-generation",
  }),
  evidence_json: JSON.stringify({
    operationId: "native-operation",
    operation: "start_turn",
    instanceId: "instance",
    threadId: "confirmation-thread",
    providerSessionId: "provider-session",
    providerThreadId: "provider-thread",
    runtimeGeneration: "actual-generation",
    attemptId: "run-attempt",
    outcome: "confirmed_success",
  }),
  evidence_revision: 1,
  native_execution_reference_json: null,
  command_event_id: "command-event",
  command_event_sequence: 1,
  confirmed_at: "2026-10-02T00:00:00Z",
};

const insertNativeConfirmationParents = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO orchestration_v2_effect_outbox
    (effect_id, command_id, thread_id, effect_type, payload_json, status, attempt_count, available_at, lease_owner, lease_expires_at, created_at, updated_at)
    VALUES ('confirmed-effect', 'confirmation-command', 'confirmation-thread', 'provider-turn.start', '{}', 'running', 1, '2026-10-02', 'worker', '2099-01-01', '2026-10-02', '2026-10-02')`;
  yield* sql`INSERT INTO orchestration_events
    (sequence, event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, command_id, actor_kind, payload_json, metadata_json, application_event_version)
    VALUES (1, 'command-event', 'thread', 'confirmation-thread', 1, 'run.created', '2026-10-02', 'confirmation-command', 'server', '{}', '{}', 2)`;
});

it.effect(
  "native effect confirmation stores ACK evidence separately from command-event attribution and rejects missing claim fields",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* insertNativeConfirmationParents;
      for (const invalid of [
        { effect_id: null },
        { effect_id: "missing-effect" },
        { command_id: "" },
        { thread_id: "" },
        { worker_id: "" },
        { operation_id: "" },
        { run_id: "" },
        { run_attempt_id: "" },
        { expected_attempt: 0 },
        { expected_attempt: -1 },
        { binding_json: "invalid" },
        { evidence_json: "invalid" },
        { evidence_revision: 0 },
        { evidence_revision: -1 },
        { native_execution_reference_json: "invalid" },
        { command_event_id: "missing-event" },
        { command_event_sequence: 0 },
        { command_event_sequence: 99 },
        { confirmed_at: "" },
      ]) {
        assert.equal(
          (yield* Effect.result(
            sql`INSERT INTO orchestration_v2_native_effect_confirmations ${sql.insert({ ...nativeEffectConfirmation, ...invalid })}`,
          ))._tag,
          "Failure",
        );
      }
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_native_effect_confirmations`, []);
      yield* sql`INSERT INTO orchestration_v2_native_effect_confirmations ${sql.insert(nativeEffectConfirmation)}`;
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_native_effect_confirmations`, [
        nativeEffectConfirmation,
      ]);
      assert.deepEqual(yield* sql`SELECT status FROM orchestration_v2_effect_outbox`, [
        { status: "running" },
      ]);
      assert.equal((yield* sql`SELECT * FROM orchestration_events`).length, 1);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "native effect confirmations preserve every ACK and binding field and cannot replace or lose their referenced effect or event",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* sql`PRAGMA recursive_triggers = OFF`;
      yield* insertNativeConfirmationParents;
      yield* sql`INSERT INTO orchestration_v2_native_effect_confirmations ${sql.insert(nativeEffectConfirmation)}`;
      for (const mutation of [
        { effect_id: "other-effect" },
        { command_id: "other-command" },
        { thread_id: "other-thread" },
        { worker_id: "other-worker" },
        { operation_id: "other-operation" },
        { run_id: "other-run" },
        { run_attempt_id: "other-attempt" },
        { expected_attempt: 2 },
        { binding_json: "{}" },
        { evidence_json: "{}" },
        { evidence_revision: 2 },
        { native_execution_reference_json: "{}" },
        { command_event_id: "other-event" },
        { command_event_sequence: 2 },
        { confirmed_at: "2026-10-03" },
      ]) {
        assert.equal(
          (yield* Effect.result(
            sql`UPDATE orchestration_v2_native_effect_confirmations ${sql.update(mutation)}`,
          ))._tag,
          "Failure",
        );
      }
      for (const mutation of [
        sql`DELETE FROM orchestration_v2_native_effect_confirmations`,
        sql`INSERT OR REPLACE INTO orchestration_v2_native_effect_confirmations ${sql.insert({ ...nativeEffectConfirmation, worker_id: "other-worker" })}`,
        sql`DELETE FROM orchestration_v2_effect_outbox WHERE effect_id = 'confirmed-effect'`,
        sql`DELETE FROM orchestration_events WHERE event_id = 'command-event'`,
      ]) {
        assert.equal((yield* Effect.result(mutation))._tag, "Failure");
      }
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_native_effect_confirmations`, [
        nativeEffectConfirmation,
      ]);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "native confirmation and outbox terminal storage roll back together before a coherent commit",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* insertNativeConfirmationParents;
      const confirm = Effect.gen(function* () {
        yield* sql`INSERT INTO orchestration_v2_native_effect_confirmations ${sql.insert(nativeEffectConfirmation)}`;
        yield* sql`UPDATE orchestration_v2_effect_outbox SET status = 'succeeded', lease_owner = NULL, lease_expires_at = NULL, completed_at = '2026-10-02'
        WHERE effect_id = 'confirmed-effect' AND status = 'running' AND lease_owner = 'worker' AND attempt_count = 1`;
      });
      assert.equal(
        (yield* Effect.result(
          sql.withTransaction(
            confirm.pipe(Effect.andThen(Effect.fail("injected after native confirmation"))),
          ),
        ))._tag,
        "Failure",
      );
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_native_effect_confirmations`, []);
      assert.deepEqual(yield* sql`SELECT status, lease_owner FROM orchestration_v2_effect_outbox`, [
        { status: "running", lease_owner: "worker" },
      ]);
      yield* sql.withTransaction(confirm);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_native_effect_confirmations`, [
        nativeEffectConfirmation,
      ]);
      assert.deepEqual(yield* sql`SELECT status, lease_owner FROM orchestration_v2_effect_outbox`, [
        { status: "succeeded", lease_owner: null },
      ]);
      assert.equal((yield* sql`SELECT * FROM orchestration_events`).length, 1);
    }).pipe(Effect.provide(memory)),
);

const importedHistoryChoice = {
  command_id: "imported-start",
  command_type: "thread.imported-history.start",
  thread_id: "imported-thread",
  actor_session_id: "actor-session",
  reviewed_basis: "reviewed-snapshot",
  reserved_at: "2026-10-02T00:00:00Z",
  command_digest: "e".repeat(64),
  canonical_command_json: '{"type":"thread.imported-history.start","threadId":"imported-thread"}',
  basis_json: '{"threadId":"imported-thread","source":"imported_snapshot"}',
};

const acceptedImportedHistoryOutcome = {
  command_id: importedHistoryChoice.command_id,
  intent_status: "accepted",
  run_id: "actual-run",
  message_id: "actual-message",
  effect_id: null,
  outcome_json: '{"intentStatus":"accepted","runId":"actual-run","messageId":"actual-message"}',
  recorded_at: "2026-10-02T00:00:00Z",
};

const insertImportedHistoryReceipt = (commandId: string, status: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO orchestration_command_receipts
    (command_id, command_type, aggregate_kind, aggregate_id, accepted_at, result_sequence, status)
    VALUES (${commandId}, 'thread.imported-history.start', 'thread', 'imported-thread', '2026-10-02', 0, ${status})`;
  });

it.effect(
  "imported history choices reserve a canonical command before receipt or run allocation and reject malformed identities",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      for (const invalid of [
        { command_id: null },
        { command_type: "thread.message.send" },
        { thread_id: "" },
        { actor_session_id: "" },
        { reviewed_basis: "" },
        { reserved_at: "" },
        { command_digest: "E".repeat(64) },
        { command_digest: "g".repeat(64) },
        { command_digest: "e".repeat(63) },
        { command_digest: "e".repeat(65) },
        { canonical_command_json: "invalid" },
        { basis_json: "invalid" },
      ]) {
        assert.equal(
          (yield* Effect.result(
            sql`INSERT INTO orchestration_v2_imported_history_start_choices ${sql.insert({ ...importedHistoryChoice, ...invalid })}`,
          ))._tag,
          "Failure",
        );
      }
      yield* sql`INSERT INTO orchestration_v2_imported_history_start_choices ${sql.insert(importedHistoryChoice)}`;
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_imported_history_start_choices`, [
        importedHistoryChoice,
      ]);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_command_receipts`, []);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_events`, []);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_effect_outbox`, []);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_projection_runs`, []);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_projection_messages`, []);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "imported history canonical reservations cannot change digest, actor or reviewed basis and duplicate IDs cannot replace them",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA recursive_triggers = OFF`;
      yield* sql`INSERT INTO orchestration_v2_imported_history_start_choices ${sql.insert(importedHistoryChoice)}`;
      for (const mutation of [
        { command_id: "other-command" },
        { command_type: "thread.imported-history.start" },
        { thread_id: "other-thread" },
        { actor_session_id: "other-actor" },
        { reviewed_basis: "different-snapshot" },
        { reserved_at: "2026-10-03" },
        { command_digest: "f".repeat(64) },
        { canonical_command_json: "{}" },
        { basis_json: "{}" },
      ]) {
        assert.equal(
          (yield* Effect.result(
            sql`UPDATE orchestration_v2_imported_history_start_choices ${sql.update(mutation)}`,
          ))._tag,
          "Failure",
        );
      }
      for (const mutation of [
        sql`DELETE FROM orchestration_v2_imported_history_start_choices`,
        sql`INSERT OR REPLACE INTO orchestration_v2_imported_history_start_choices ${sql.insert({ ...importedHistoryChoice, command_digest: "f".repeat(64) })}`,
      ]) {
        assert.equal((yield* Effect.result(mutation))._tag, "Failure");
      }
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_imported_history_start_choices`, [
        importedHistoryChoice,
      ]);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "imported history outcomes require both reservation and receipt and preserve accepted or rejected result shapes",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      assert.equal(
        (yield* Effect.result(
          sql`INSERT INTO orchestration_v2_imported_history_start_outcomes ${sql.insert(acceptedImportedHistoryOutcome)}`,
        ))._tag,
        "Failure",
      );
      yield* sql`INSERT INTO orchestration_v2_imported_history_start_choices ${sql.insert(importedHistoryChoice)}`;
      assert.equal(
        (yield* Effect.result(
          sql`INSERT INTO orchestration_v2_imported_history_start_outcomes ${sql.insert(acceptedImportedHistoryOutcome)}`,
        ))._tag,
        "Failure",
      );
      yield* insertImportedHistoryReceipt(importedHistoryChoice.command_id, "accepted");
      yield* insertImportedHistoryReceipt("unreserved", "accepted");
      assert.equal(
        (yield* Effect.result(
          sql`INSERT INTO orchestration_v2_imported_history_start_outcomes ${sql.insert({ ...acceptedImportedHistoryOutcome, command_id: "unreserved" })}`,
        ))._tag,
        "Failure",
      );
      for (const invalid of [
        { intent_status: "pending" },
        { run_id: null },
        { run_id: "" },
        { message_id: null },
        { message_id: "" },
        { effect_id: "missing-effect" },
        { intent_status: "rejected" },
        { outcome_json: "invalid" },
        { recorded_at: "" },
      ]) {
        assert.equal(
          (yield* Effect.result(
            sql`INSERT INTO orchestration_v2_imported_history_start_outcomes ${sql.insert({ ...acceptedImportedHistoryOutcome, ...invalid })}`,
          ))._tag,
          "Failure",
        );
      }
      yield* sql`INSERT INTO orchestration_v2_imported_history_start_outcomes ${sql.insert(acceptedImportedHistoryOutcome)}`;
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_imported_history_start_outcomes`, [
        acceptedImportedHistoryOutcome,
      ]);
      yield* sql`INSERT INTO orchestration_v2_imported_history_start_choices ${sql.insert({ ...importedHistoryChoice, command_id: "accepted-with-effect" })}`;
      yield* insertImportedHistoryReceipt("accepted-with-effect", "accepted");
      yield* sql`INSERT INTO orchestration_v2_effect_outbox
      (effect_id, command_id, thread_id, effect_type, payload_json, status, attempt_count, available_at, created_at, updated_at)
      VALUES ('imported-effect', 'accepted-with-effect', 'imported-thread', 'provider-turn.start', '{}', 'pending', 0, '2026-10-02', '2026-10-02', '2026-10-02')`;
      const acceptedWithEffect = {
        ...acceptedImportedHistoryOutcome,
        command_id: "accepted-with-effect",
        effect_id: "imported-effect",
      };
      yield* sql`INSERT INTO orchestration_v2_imported_history_start_outcomes ${sql.insert(acceptedWithEffect)}`;
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_imported_history_start_outcomes WHERE command_id = 'accepted-with-effect'`,
        [acceptedWithEffect],
      );
      yield* sql`INSERT INTO orchestration_v2_imported_history_start_choices ${sql.insert({ ...importedHistoryChoice, command_id: "rejected-start" })}`;
      yield* insertImportedHistoryReceipt("rejected-start", "rejected");
      const rejected = {
        ...acceptedImportedHistoryOutcome,
        command_id: "rejected-start",
        intent_status: "rejected",
        run_id: null,
        message_id: null,
        effect_id: null,
        outcome_json: '{"intentStatus":"rejected"}',
      };
      for (const invalid of [
        { run_id: "run" },
        { message_id: "message" },
        { effect_id: "imported-effect" },
      ]) {
        assert.equal(
          (yield* Effect.result(
            sql`INSERT INTO orchestration_v2_imported_history_start_outcomes ${sql.insert({ ...rejected, ...invalid })}`,
          ))._tag,
          "Failure",
        );
      }
      yield* sql`INSERT INTO orchestration_v2_imported_history_start_outcomes ${sql.insert(rejected)}`;
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_imported_history_start_outcomes WHERE command_id = 'rejected-start'`,
        [rejected],
      );
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "imported history outcomes are permanent and reservation, receipt and outcome roll back as one boundary",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* sql`PRAGMA recursive_triggers = OFF`;
      const accept = Effect.gen(function* () {
        yield* sql`INSERT INTO orchestration_v2_imported_history_start_choices ${sql.insert(importedHistoryChoice)}`;
        yield* insertImportedHistoryReceipt(importedHistoryChoice.command_id, "accepted");
        yield* sql`INSERT INTO orchestration_v2_imported_history_start_outcomes ${sql.insert(acceptedImportedHistoryOutcome)}`;
      });
      assert.equal(
        (yield* Effect.result(
          sql.withTransaction(
            accept.pipe(Effect.andThen(Effect.fail("injected after imported start outcome"))),
          ),
        ))._tag,
        "Failure",
      );
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_imported_history_start_choices`,
        [],
      );
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_command_receipts`, []);
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_imported_history_start_outcomes`,
        [],
      );
      yield* sql.withTransaction(accept);
      for (const mutation of [
        sql`UPDATE orchestration_v2_imported_history_start_outcomes SET intent_status = 'rejected', run_id = NULL, message_id = NULL`,
        sql`UPDATE orchestration_v2_imported_history_start_outcomes SET command_id = 'other-command'`,
        sql`UPDATE orchestration_v2_imported_history_start_outcomes SET run_id = 'other-run'`,
        sql`UPDATE orchestration_v2_imported_history_start_outcomes SET message_id = 'other-message'`,
        sql`UPDATE orchestration_v2_imported_history_start_outcomes SET effect_id = NULL`,
        sql`UPDATE orchestration_v2_imported_history_start_outcomes SET outcome_json = '{}'`,
        sql`UPDATE orchestration_v2_imported_history_start_outcomes SET recorded_at = '2026-10-03'`,
        sql`DELETE FROM orchestration_v2_imported_history_start_outcomes`,
        sql`INSERT OR REPLACE INTO orchestration_v2_imported_history_start_outcomes ${sql.insert({ ...acceptedImportedHistoryOutcome, run_id: "other-run" })}`,
        sql`DELETE FROM orchestration_command_receipts WHERE command_id = ${importedHistoryChoice.command_id}`,
      ]) {
        assert.equal((yield* Effect.result(mutation))._tag, "Failure");
      }
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_imported_history_start_choices`, [
        importedHistoryChoice,
      ]);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_imported_history_start_outcomes`, [
        acceptedImportedHistoryOutcome,
      ]);
    }).pipe(Effect.provide(memory)),
);

const currentRuntimeStopIntent = {
  command_id: "stop-command",
  thread_id: "stop-thread",
  application_birth_json: '{"applicationEventVersion":2,"eventId":"birth-event","sequence":1}',
  canonical_request_digest: "a".repeat(64),
  actor_binding_digest: "b".repeat(64),
  target_binding_json:
    '{"threadId":"stop-thread","providerThreadId":"provider-thread","providerSessionId":"provider-session","instanceId":"instance","driver":"codex","nativeThreadId":"native-thread","runtimeGeneration":"generation"}',
  target_evidence_revision: 1,
  stop_event_id: "stop-event",
  stop_event_sequence: 2,
  affected_run_ids_json: '["queued-run"]',
  queued_bases_json:
    '[{"runId":"queued-run","executionIntent":{"eventId":"queued-event","sequence":3}}]',
  accepted_at: "2026-10-02T00:00:00Z",
};

const queuedRuntimeStopFence = {
  stop_command_id: currentRuntimeStopIntent.command_id,
  run_id: "queued-run",
  thread_id: currentRuntimeStopIntent.thread_id,
  application_birth_json: currentRuntimeStopIntent.application_birth_json,
  queued_provider_thread_id: "queued-provider-thread",
  source_binding_json: currentRuntimeStopIntent.target_binding_json,
  source_evidence_revision: 1,
  switch_plan_json: null,
  source_mode: "queued_thread",
  execution_intent_json: '{"eventId":"queued-event","sequence":3}',
  basis_digest: "c".repeat(64),
};

const queuedStartReservation = {
  effect_id: "queued-effect",
  command_id: "queued-command",
  thread_id: currentRuntimeStopIntent.thread_id,
  run_id: queuedRuntimeStopFence.run_id,
  run_attempt_id: "queued-run-attempt",
  application_birth_json: currentRuntimeStopIntent.application_birth_json,
  execution_intent_json: queuedRuntimeStopFence.execution_intent_json,
  basis_json:
    '{"runId":"queued-run","providerThreadId":"queued-provider-thread","evidenceRevision":1}',
  basis_digest: queuedRuntimeStopFence.basis_digest,
  reserved_at: "2026-10-02T00:00:00Z",
};

const insertCurrentRuntimeStopParents = (
  commandId = "stop-command",
  eventId = "stop-event",
  sequence = 2,
) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO orchestration_command_receipts
    (command_id, command_type, aggregate_kind, aggregate_id, accepted_at, result_sequence, status)
    VALUES (${commandId}, 'thread.current-runtime.stop', 'thread', 'stop-thread', '2026-10-02', ${sequence}, 'accepted')`;
    yield* sql`INSERT INTO orchestration_events
    (sequence, event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, command_id, actor_kind, payload_json, metadata_json, application_event_version)
    VALUES (${sequence}, ${eventId}, 'thread', 'stop-thread', ${sequence}, 'thread.current-runtime.stopped', '2026-10-02', ${commandId}, 'server', '{}', '{}', 2)`;
  });

const insertQueuedStartEffect = (effectId = "queued-effect") =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO orchestration_v2_effect_outbox
    (effect_id, command_id, thread_id, effect_type, payload_json, status, attempt_count, available_at, lease_owner, lease_expires_at, created_at, updated_at)
    VALUES (${effectId}, 'queued-command', 'stop-thread', 'provider-turn.start', '{}', 'running', 1, '2026-10-02', 'queued-worker', '2099-01-01', '2026-10-02', '2026-10-02')`;
  });

it.effect(
  "current runtime stop intent requires receipt, attributed event and complete immutable JSON carriers",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      assert.equal(
        (yield* Effect.result(
          sql`INSERT INTO orchestration_v2_current_runtime_stop_intents ${sql.insert(currentRuntimeStopIntent)}`,
        ))._tag,
        "Failure",
      );
      yield* insertCurrentRuntimeStopParents();
      for (const invalid of [
        { command_id: null },
        { command_id: "missing-receipt" },
        { thread_id: "" },
        { application_birth_json: "invalid" },
        { application_birth_json: "[]" },
        { application_birth_json: null },
        { canonical_request_digest: "A".repeat(64) },
        { canonical_request_digest: "a".repeat(63) },
        { actor_binding_digest: "g".repeat(64) },
        { actor_binding_digest: "b".repeat(65) },
        { target_binding_json: "invalid" },
        { target_binding_json: "[]" },
        { target_evidence_revision: 0 },
        { target_evidence_revision: -1 },
        { stop_event_id: "missing-event" },
        { stop_event_sequence: 0 },
        { stop_event_sequence: 99 },
        { affected_run_ids_json: "{}" },
        { affected_run_ids_json: "invalid" },
        { queued_bases_json: "{}" },
        { queued_bases_json: "invalid" },
        { accepted_at: "" },
      ]) {
        assert.equal(
          (yield* Effect.result(
            sql`INSERT INTO orchestration_v2_current_runtime_stop_intents ${sql.insert({ ...currentRuntimeStopIntent, ...invalid })}`,
          ))._tag,
          "Failure",
        );
      }
      yield* sql`INSERT INTO orchestration_v2_current_runtime_stop_intents ${sql.insert(currentRuntimeStopIntent)}`;
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_current_runtime_stop_intents`, [
        currentRuntimeStopIntent,
      ]);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "current runtime stop intent cannot change actor, target, birth, events or queued bases or be replaced",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* sql`PRAGMA recursive_triggers = OFF`;
      yield* insertCurrentRuntimeStopParents();
      yield* sql`INSERT INTO orchestration_v2_current_runtime_stop_intents ${sql.insert(currentRuntimeStopIntent)}`;
      for (const mutation of [
        { command_id: "other-command" },
        { thread_id: "other-thread" },
        { application_birth_json: "{}" },
        { canonical_request_digest: "d".repeat(64) },
        { actor_binding_digest: "d".repeat(64) },
        { target_binding_json: "{}" },
        { target_evidence_revision: 2 },
        { stop_event_id: "other-event" },
        { stop_event_sequence: 3 },
        { affected_run_ids_json: "[]" },
        { queued_bases_json: "[]" },
        { accepted_at: "2026-10-03" },
      ]) {
        assert.equal(
          (yield* Effect.result(
            sql`UPDATE orchestration_v2_current_runtime_stop_intents ${sql.update(mutation)}`,
          ))._tag,
          "Failure",
        );
      }
      for (const mutation of [
        sql`DELETE FROM orchestration_v2_current_runtime_stop_intents`,
        sql`INSERT OR REPLACE INTO orchestration_v2_current_runtime_stop_intents ${sql.insert({ ...currentRuntimeStopIntent, queued_bases_json: "[]" })}`,
        sql`DELETE FROM orchestration_command_receipts WHERE command_id = 'stop-command'`,
        sql`DELETE FROM orchestration_events WHERE event_id = 'stop-event'`,
      ])
        assert.equal((yield* Effect.result(mutation))._tag, "Failure");
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_current_runtime_stop_intents`, [
        currentRuntimeStopIntent,
      ]);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "queued runtime stop fences require exact stop association and constrained pre-stop source and intent carriers",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      assert.equal(
        (yield* Effect.result(
          sql`INSERT INTO orchestration_v2_queued_runtime_stop_fences ${sql.insert(queuedRuntimeStopFence)}`,
        ))._tag,
        "Failure",
      );
      yield* insertCurrentRuntimeStopParents();
      yield* sql`INSERT INTO orchestration_v2_current_runtime_stop_intents ${sql.insert(currentRuntimeStopIntent)}`;
      for (const invalid of [
        { stop_command_id: null },
        { stop_command_id: "missing-stop" },
        { run_id: null },
        { run_id: "" },
        { thread_id: "" },
        { application_birth_json: "[]" },
        { application_birth_json: "invalid" },
        { queued_provider_thread_id: "" },
        { source_binding_json: "[]" },
        { source_binding_json: "invalid" },
        { source_evidence_revision: 0 },
        { switch_plan_json: "invalid" },
        { source_mode: "current_thread" },
        { execution_intent_json: "[]" },
        { execution_intent_json: "invalid" },
        { basis_digest: "C".repeat(64) },
        { basis_digest: "g".repeat(64) },
        { basis_digest: "c".repeat(63) },
      ])
        assert.equal(
          (yield* Effect.result(
            sql`INSERT INTO orchestration_v2_queued_runtime_stop_fences ${sql.insert({ ...queuedRuntimeStopFence, ...invalid })}`,
          ))._tag,
          "Failure",
        );
      yield* sql`INSERT INTO orchestration_v2_queued_runtime_stop_fences ${sql.insert(queuedRuntimeStopFence)}`;
      const copiedSource = {
        ...queuedRuntimeStopFence,
        run_id: "copied-run",
        source_mode: "active_native_copy",
        switch_plan_json: '{"targetProviderThreadId":"queued-provider-thread"}',
      };
      yield* sql`INSERT INTO orchestration_v2_queued_runtime_stop_fences ${sql.insert(copiedSource)}`;
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_queued_runtime_stop_fences ORDER BY run_id`,
        [copiedSource, queuedRuntimeStopFence],
      );
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "queued runtime stop fences preserve old intent while allowing distinct stop commands for the same run",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* sql`PRAGMA recursive_triggers = OFF`;
      yield* insertCurrentRuntimeStopParents();
      yield* sql`INSERT INTO orchestration_v2_current_runtime_stop_intents ${sql.insert(currentRuntimeStopIntent)}`;
      yield* sql`INSERT INTO orchestration_v2_queued_runtime_stop_fences ${sql.insert(queuedRuntimeStopFence)}`;
      for (const mutation of [
        { stop_command_id: "other-stop" },
        { run_id: "other-run" },
        { thread_id: "other-thread" },
        { application_birth_json: "{}" },
        { queued_provider_thread_id: "other-provider-thread" },
        { source_binding_json: "{}" },
        { source_evidence_revision: 2 },
        { switch_plan_json: "{}" },
        { source_mode: "active_native_copy" },
        { execution_intent_json: "{}" },
        { basis_digest: "d".repeat(64) },
      ])
        assert.equal(
          (yield* Effect.result(
            sql`UPDATE orchestration_v2_queued_runtime_stop_fences ${sql.update(mutation)}`,
          ))._tag,
          "Failure",
        );
      for (const mutation of [
        sql`DELETE FROM orchestration_v2_queued_runtime_stop_fences`,
        sql`INSERT OR REPLACE INTO orchestration_v2_queued_runtime_stop_fences ${sql.insert({ ...queuedRuntimeStopFence, execution_intent_json: "{}" })}`,
      ])
        assert.equal((yield* Effect.result(mutation))._tag, "Failure");
      yield* insertCurrentRuntimeStopParents("later-stop", "later-stop-event", 4);
      yield* sql`INSERT INTO orchestration_v2_current_runtime_stop_intents ${sql.insert({ ...currentRuntimeStopIntent, command_id: "later-stop", stop_event_id: "later-stop-event", stop_event_sequence: 4 })}`;
      const laterFence = {
        ...queuedRuntimeStopFence,
        stop_command_id: "later-stop",
        execution_intent_json: '{"eventId":"later-intent","sequence":5}',
        basis_digest: "d".repeat(64),
      };
      yield* sql`INSERT INTO orchestration_v2_queued_runtime_stop_fences ${sql.insert(laterFence)}`;
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_queued_runtime_stop_fences ORDER BY stop_command_id`,
        [laterFence, queuedRuntimeStopFence],
      );
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "queued start reservations require existing stable effects and preserve full attempt and basis before provider work",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* sql`PRAGMA recursive_triggers = OFF`;
      assert.equal(
        (yield* Effect.result(
          sql`INSERT INTO orchestration_v2_queued_start_reservations ${sql.insert(queuedStartReservation)}`,
        ))._tag,
        "Failure",
      );
      yield* insertQueuedStartEffect();
      for (const invalid of [
        { effect_id: null },
        { effect_id: "missing-effect" },
        { command_id: "" },
        { thread_id: "" },
        { run_id: "" },
        { run_attempt_id: "" },
        { application_birth_json: "[]" },
        { application_birth_json: "invalid" },
        { execution_intent_json: "[]" },
        { execution_intent_json: "invalid" },
        { basis_json: "[]" },
        { basis_json: "invalid" },
        { basis_digest: "C".repeat(64) },
        { basis_digest: "g".repeat(64) },
        { basis_digest: "c".repeat(65) },
        { reserved_at: "" },
      ])
        assert.equal(
          (yield* Effect.result(
            sql`INSERT INTO orchestration_v2_queued_start_reservations ${sql.insert({ ...queuedStartReservation, ...invalid })}`,
          ))._tag,
          "Failure",
        );
      yield* sql`INSERT INTO orchestration_v2_queued_start_reservations ${sql.insert(queuedStartReservation)}`;
      for (const mutation of [
        { effect_id: "other-effect" },
        { command_id: "other-command" },
        { thread_id: "other-thread" },
        { run_id: "other-run" },
        { run_attempt_id: "other-attempt" },
        { application_birth_json: "{}" },
        { execution_intent_json: "{}" },
        { basis_json: "{}" },
        { basis_digest: "d".repeat(64) },
        { reserved_at: "2026-10-03" },
      ])
        assert.equal(
          (yield* Effect.result(
            sql`UPDATE orchestration_v2_queued_start_reservations ${sql.update(mutation)}`,
          ))._tag,
          "Failure",
        );
      for (const mutation of [
        sql`DELETE FROM orchestration_v2_queued_start_reservations`,
        sql`INSERT OR REPLACE INTO orchestration_v2_queued_start_reservations ${sql.insert({ ...queuedStartReservation, run_attempt_id: "other-attempt" })}`,
        sql`DELETE FROM orchestration_v2_effect_outbox WHERE effect_id = 'queued-effect'`,
      ])
        assert.equal((yield* Effect.result(mutation))._tag, "Failure");
      yield* insertQueuedStartEffect("fresh-effect");
      const freshIntent = {
        ...queuedStartReservation,
        effect_id: "fresh-effect",
        execution_intent_json: '{"eventId":"fresh-intent","sequence":5}',
        basis_digest: "d".repeat(64),
      };
      yield* sql`INSERT INTO orchestration_v2_queued_start_reservations ${sql.insert(freshIntent)}`;
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_queued_start_reservations ORDER BY effect_id`,
        [freshIntent, queuedStartReservation],
      );
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_command_receipts`, []);
      assert.deepEqual(
        yield* sql`SELECT lease_owner, status FROM orchestration_v2_effect_outbox ORDER BY effect_id`,
        [
          { lease_owner: "queued-worker", status: "running" },
          { lease_owner: "queued-worker", status: "running" },
        ],
      );
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "stop receipt, event, fences and queued reservation share rollback without clearing a running claim",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* insertQueuedStartEffect();
      const recordStop = Effect.gen(function* () {
        yield* insertCurrentRuntimeStopParents();
        yield* sql`INSERT INTO orchestration_v2_current_runtime_stop_intents ${sql.insert(currentRuntimeStopIntent)}`;
        yield* sql`INSERT INTO orchestration_v2_queued_runtime_stop_fences ${sql.insert(queuedRuntimeStopFence)}`;
        yield* sql`INSERT INTO orchestration_v2_queued_start_reservations ${sql.insert(queuedStartReservation)}`;
      });
      assert.equal(
        (yield* Effect.result(
          sql.withTransaction(
            recordStop.pipe(Effect.andThen(Effect.fail("injected after stop companions"))),
          ),
        ))._tag,
        "Failure",
      );
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_command_receipts`, []);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_events`, []);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_current_runtime_stop_intents`, []);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_queued_runtime_stop_fences`, []);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_queued_start_reservations`, []);
      yield* sql.withTransaction(recordStop);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_current_runtime_stop_intents`, [
        currentRuntimeStopIntent,
      ]);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_queued_runtime_stop_fences`, [
        queuedRuntimeStopFence,
      ]);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_queued_start_reservations`, [
        queuedStartReservation,
      ]);
      assert.deepEqual(
        yield* sql`SELECT status, attempt_count, lease_owner, lease_expires_at FROM orchestration_v2_effect_outbox`,
        [
          {
            status: "running",
            attempt_count: 1,
            lease_owner: "queued-worker",
            lease_expires_at: "2099-01-01",
          },
        ],
      );
    }).pipe(Effect.provide(memory)),
);

const workstreamSettlementWitnessValue = {
  version: 2,
  command: { type: "thread.settle", commandId: "witness-command", threadId: "witness-thread" },
  attemptKey: { owner_id: "owner", principal_id: "principal", command_id: "witness-command" },
  dispatchStartedAt: "2026-10-02T00:00:00Z",
  actorSessionId: "actor-session",
  enrollmentSha256: "d".repeat(64),
  requestBytesSha256: "e".repeat(64),
  authority: { environmentId: "environment", authorityNamespace: "authority", storeGeneration: 1 },
  incarnation: { eventId: "witness-birth", sequence: 1 },
  targetEventSequence: 1,
  provider: {
    binding: {
      threadId: "witness-thread",
      providerThreadId: "provider-thread",
      providerSessionId: "provider-session",
      instanceId: "instance",
      driver: "codex",
      nativeThreadId: "native-thread",
      runtimeGeneration: "generation",
    },
    evidenceRevision: 1,
  },
};

const workstreamSettlementWitness = {
  command_id: "witness-command",
  thread_id: "witness-thread",
  witness_json: JSON.stringify(workstreamSettlementWitnessValue),
  recorded_at: "2026-10-02T00:00:00Z",
};

const insertWorkstreamWitnessReceipt = (
  commandId = "witness-command",
  commandType = "thread.settle",
) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO orchestration_command_receipts
    (command_id, command_type, aggregate_kind, aggregate_id, accepted_at, result_sequence, status)
    VALUES (${commandId}, ${commandType}, 'thread', 'witness-thread', '2026-10-02', 1, 'accepted')`;
  });

it.effect(
  "workstream witness storage requires a receipt and immutable object carrier and preserves explicit provider absence",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      assert.equal(
        (yield* Effect.result(
          sql`INSERT INTO orchestration_v2_workstream_settlement_witnesses ${sql.insert(workstreamSettlementWitness)}`,
        ))._tag,
        "Failure",
      );
      yield* insertWorkstreamWitnessReceipt();
      for (const invalid of [
        { command_id: null },
        { command_id: "missing-command" },
        { thread_id: "" },
        { witness_json: null },
        { witness_json: "invalid" },
        { witness_json: "[]" },
        { witness_json: "null" },
        { recorded_at: "" },
      ])
        assert.equal(
          (yield* Effect.result(
            sql`INSERT INTO orchestration_v2_workstream_settlement_witnesses ${sql.insert({ ...workstreamSettlementWitness, ...invalid })}`,
          ))._tag,
          "Failure",
        );
      yield* sql`INSERT INTO orchestration_v2_workstream_settlement_witnesses ${sql.insert(workstreamSettlementWitness)}`;
      yield* insertWorkstreamWitnessReceipt("absence-witness", "thread.unsettle");
      const absence = {
        ...workstreamSettlementWitness,
        command_id: "absence-witness",
        witness_json: yield* encodeFixtureJson({
          ...workstreamSettlementWitnessValue,
          command: {
            type: "thread.unsettle",
            commandId: "absence-witness",
            threadId: "witness-thread",
            reason: "user",
          },
          attemptKey: {
            ...workstreamSettlementWitnessValue.attemptKey,
            command_id: "absence-witness",
          },
          provider: null,
        }).pipe(Effect.catch(dieNativeJsonCause)),
      };
      yield* sql`INSERT INTO orchestration_v2_workstream_settlement_witnesses ${sql.insert(absence)}`;
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_workstream_settlement_witnesses ORDER BY command_id`,
        [absence, workstreamSettlementWitness],
      );
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "workstream witness retains original provider and attempt bytes through provider advancement and cannot be updated or replaced",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* sql`PRAGMA recursive_triggers = OFF`;
      yield* insertWorkstreamWitnessReceipt();
      yield* sql`INSERT INTO orchestration_v2_workstream_settlement_witnesses ${sql.insert(workstreamSettlementWitness)}`;
      yield* sql`INSERT INTO orchestration_v2_provider_runtime_evidence
      (thread_id, provider_thread_id, provider_session_id, provider_instance_id, driver, native_thread_id, runtime_generation, evidence_revision, observation_json, registered_at)
      VALUES ('witness-thread', 'provider-thread', 'provider-session', 'instance', 'codex', 'native-thread', 'generation', 1, NULL, '2026-10-02')`;
      yield* sql`UPDATE orchestration_v2_provider_runtime_evidence
      SET provider_thread_id = 'advanced-provider-thread', provider_session_id = 'advanced-provider-session',
      runtime_generation = 'advanced-generation', evidence_revision = 2 WHERE thread_id = 'witness-thread'`;
      for (const mutation of [
        { command_id: "other-command" },
        { thread_id: "other-thread" },
        {
          witness_json: yield* encodeFixtureJson({
            ...workstreamSettlementWitnessValue,
            provider: null,
          }).pipe(Effect.catch(dieNativeJsonCause)),
        },
        { recorded_at: "2026-10-03" },
      ])
        assert.equal(
          (yield* Effect.result(
            sql`UPDATE orchestration_v2_workstream_settlement_witnesses ${sql.update(mutation)}`,
          ))._tag,
          "Failure",
        );
      for (const mutation of [
        sql`DELETE FROM orchestration_v2_workstream_settlement_witnesses`,
        sql`INSERT OR REPLACE INTO orchestration_v2_workstream_settlement_witnesses ${sql.insert({ ...workstreamSettlementWitness, witness_json: "{}" })}`,
        sql`DELETE FROM orchestration_command_receipts WHERE command_id = 'witness-command'`,
      ])
        assert.equal((yield* Effect.result(mutation))._tag, "Failure");
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_workstream_settlement_witnesses`, [
        workstreamSettlementWitness,
      ]);
      assert.deepEqual(
        yield* sql`SELECT provider_thread_id, evidence_revision FROM orchestration_v2_provider_runtime_evidence`,
        [{ provider_thread_id: "advanced-provider-thread", evidence_revision: 2 }],
      );
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "workstream witness, native identity, receipt and attributed event roll back and commit together",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      const recordWitness = Effect.gen(function* () {
        yield* insertWorkstreamWitnessReceipt();
        yield* sql`INSERT INTO orchestration_v2_native_command_identities ${sql.insert({
          ...nativeIdentity,
          command_id: "witness-command",
          kind: "workstream_settlement",
          command_type: "thread.settle",
          aggregate_id: "witness-thread",
        })}`;
        yield* sql`INSERT INTO orchestration_events
        (sequence, event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, command_id, actor_kind, payload_json, metadata_json, application_event_version)
        VALUES (2, 'witness-event', 'thread', 'witness-thread', 2, 'thread.settled', '2026-10-02', 'witness-command', 'server', '{}', '{}', 2)`;
        yield* sql`INSERT INTO orchestration_v2_workstream_settlement_witnesses ${sql.insert(workstreamSettlementWitness)}`;
      });
      assert.equal(
        (yield* Effect.result(
          sql.withTransaction(
            recordWitness.pipe(Effect.andThen(Effect.fail("injected after workstream witness"))),
          ),
        ))._tag,
        "Failure",
      );
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_workstream_settlement_witnesses`,
        [],
      );
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_native_command_identities`, []);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_command_receipts`, []);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_events`, []);
      yield* sql.withTransaction(recordWitness);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_workstream_settlement_witnesses`, [
        workstreamSettlementWitness,
      ]);
      assert.equal(
        (yield* sql`SELECT * FROM orchestration_v2_native_command_identities`).length,
        1,
      );
      assert.equal((yield* sql`SELECT * FROM orchestration_command_receipts`).length, 1);
      assert.equal((yield* sql`SELECT * FROM orchestration_events`).length, 1);
    }).pipe(Effect.provide(memory)),
);

const capturedRestartMarker = {
  markerId: "captured-marker",
  threadId: "captured-thread",
  projectId: "captured-project",
  sourceRunId: "captured-source-run",
  sourceRunAttemptId: "captured-source-attempt",
  binding: {
    threadId: "captured-thread",
    providerThreadId: "captured-provider-thread",
    providerSessionId: "captured-provider-session",
    instanceId: "captured-instance",
    driver: "codex",
    nativeThreadId: "captured-native-thread",
    runtimeGeneration: "captured-generation",
  },
  evidenceRevision: 1,
  createdAt: "2026-10-02T00:00:00Z",
};

const capturedRestartCanonicalCommand = nativeCreationCanonicalJson(
  Schema.encodeSync(OrchestrationV2Command)(
    Schema.decodeUnknownSync(OrchestrationV2Command, { onExcessProperty: "error" })({
      type: "message.dispatch",
      commandId: "captured-command",
      threadId: capturedRestartMarker.threadId,
      messageId: "message:restart-continuation:captured-source-run",
      text: "Continue where you left off.",
      attachments: [],
      modelSelection: {
        instanceId: capturedRestartMarker.binding.instanceId,
        model: "fixture-model",
      },
      dispatchMode: { type: "start_immediately" },
      createdBy: "agent",
      creationSource: "server",
      restartContinuationOfRunId: capturedRestartMarker.sourceRunId,
    }),
  ),
);

const capturedRestartCommandOrigin = {
  command_id: "captured-command",
  thread_id: "captured-thread",
  effect_id: "captured-effect",
  marker_json: JSON.stringify(capturedRestartMarker),
  canonical_command_json: capturedRestartCanonicalCommand,
  command_digest: nativeCreationSha256(capturedRestartCanonicalCommand),
  original_claim_json:
    '{"workerId":"captured-worker","expectedAttempt":1,"leaseExpiresAt":"2099-01-01T00:00:00Z"}',
  recorded_at: "2026-10-02T00:00:00Z",
};

const insertCapturedRestartEffect = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO orchestration_v2_effect_outbox
    (effect_id, command_id, thread_id, effect_type, payload_json, status, attempt_count, available_at, lease_owner, lease_expires_at, created_at, updated_at)
    VALUES ('captured-effect', 'effect-source-command', 'captured-thread', 'provider-turn.start', '{}', 'running', 1, '2026-10-02', 'captured-worker', '2099-01-01T00:00:00Z', '2026-10-02', '2026-10-02')`;
});

const insertCapturedRestartReceipt = (commandId = "captured-command", status = "accepted") =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO orchestration_command_receipts
    (command_id, command_type, aggregate_kind, aggregate_id, accepted_at, result_sequence, status)
    VALUES (${commandId}, 'message.dispatch', 'thread', 'captured-thread', '2026-10-02', 0, ${status})`;
  });

it.effect(
  "captured restart origins require a receipt and actual effect plus constrained JSON object carriers and digest",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      assert.equal(
        (yield* Effect.result(
          sql`INSERT INTO orchestration_v2_captured_restart_command_origins ${sql.insert(capturedRestartCommandOrigin)}`,
        ))._tag,
        "Failure",
      );
      yield* insertCapturedRestartReceipt();
      assert.equal(
        (yield* Effect.result(
          sql`INSERT INTO orchestration_v2_captured_restart_command_origins ${sql.insert(capturedRestartCommandOrigin)}`,
        ))._tag,
        "Failure",
      );
      yield* insertCapturedRestartEffect;
      for (const invalid of [
        { command_id: null },
        { command_id: "missing-receipt" },
        { thread_id: "" },
        { effect_id: null },
        { effect_id: "missing-effect" },
        { marker_json: null },
        { marker_json: "invalid" },
        { marker_json: "[]" },
        { canonical_command_json: null },
        { canonical_command_json: "invalid" },
        { canonical_command_json: "[]" },
        { command_digest: "F".repeat(64) },
        { command_digest: "g".repeat(64) },
        { command_digest: "f".repeat(63) },
        { command_digest: "f".repeat(65) },
        { original_claim_json: null },
        { original_claim_json: "invalid" },
        { original_claim_json: "[]" },
        { recorded_at: "" },
      ])
        assert.equal(
          (yield* Effect.result(
            sql`INSERT INTO orchestration_v2_captured_restart_command_origins ${sql.insert({ ...capturedRestartCommandOrigin, ...invalid })}`,
          ))._tag,
          "Failure",
        );
      yield* sql`INSERT INTO orchestration_v2_captured_restart_command_origins ${sql.insert(capturedRestartCommandOrigin)}`;
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_captured_restart_command_origins`,
        [capturedRestartCommandOrigin],
      );
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_restart_continuation_markers`, []);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "captured restart origins permanently retain every marker, canonical command and original claim field",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* sql`PRAGMA recursive_triggers = OFF`;
      yield* insertCapturedRestartReceipt();
      yield* insertCapturedRestartEffect;
      yield* sql`INSERT INTO orchestration_v2_captured_restart_command_origins ${sql.insert(capturedRestartCommandOrigin)}`;
      for (const mutation of [
        { command_id: "other-command" },
        { thread_id: "other-thread" },
        { effect_id: "other-effect" },
        {
          marker_json: yield* encodeFixtureJson({
            ...capturedRestartMarker,
            sourceRunAttemptId: "other-attempt",
          }).pipe(Effect.catch(dieNativeJsonCause)),
        },
        { canonical_command_json: '{"changed":true}' },
        { command_digest: "a".repeat(64) },
        {
          original_claim_json:
            '{"workerId":"other-worker","expectedAttempt":2,"leaseExpiresAt":"2099-01-02T00:00:00Z"}',
        },
        { recorded_at: "2026-10-03" },
      ])
        assert.equal(
          (yield* Effect.result(
            sql`UPDATE orchestration_v2_captured_restart_command_origins ${sql.update(mutation)}`,
          ))._tag,
          "Failure",
        );
      for (const mutation of [
        sql`DELETE FROM orchestration_v2_captured_restart_command_origins`,
        sql`INSERT OR REPLACE INTO orchestration_v2_captured_restart_command_origins ${sql.insert({ ...capturedRestartCommandOrigin, marker_json: "{}" })}`,
        sql`DELETE FROM orchestration_command_receipts WHERE command_id = 'captured-command'`,
        sql`DELETE FROM orchestration_v2_effect_outbox WHERE effect_id = 'captured-effect'`,
      ])
        assert.equal((yield* Effect.result(mutation))._tag, "Failure");
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_captured_restart_command_origins`,
        [capturedRestartCommandOrigin],
      );
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "captured restart origin reads remain stable across later claims and conflicting same-command inserts cannot rewrite history",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* insertCapturedRestartReceipt();
      yield* insertCapturedRestartEffect;
      yield* sql`INSERT INTO orchestration_v2_captured_restart_command_origins ${sql.insert(capturedRestartCommandOrigin)}`;
      yield* sql`UPDATE orchestration_v2_effect_outbox SET attempt_count = 2, lease_owner = 'later-worker',
      lease_expires_at = '2099-01-02T00:00:00Z' WHERE effect_id = 'captured-effect'`;
      for (let observation = 0; observation < 2; observation++) {
        assert.deepEqual(
          yield* sql`SELECT * FROM orchestration_v2_captured_restart_command_origins WHERE command_id = 'captured-command'`,
          [capturedRestartCommandOrigin],
        );
      }
      for (const conflict of [
        capturedRestartCommandOrigin,
        {
          ...capturedRestartCommandOrigin,
          marker_json: yield* encodeFixtureJson({
            ...capturedRestartMarker,
            evidenceRevision: 2,
          }).pipe(Effect.catch(dieNativeJsonCause)),
        },
        {
          ...capturedRestartCommandOrigin,
          canonical_command_json: '{"changed":true}',
          command_digest: "a".repeat(64),
        },
        {
          ...capturedRestartCommandOrigin,
          original_claim_json:
            '{"workerId":"later-worker","expectedAttempt":2,"leaseExpiresAt":"2099-01-02T00:00:00Z"}',
        },
      ])
        assert.equal(
          (yield* Effect.result(
            sql`INSERT INTO orchestration_v2_captured_restart_command_origins ${sql.insert(conflict)}`,
          ))._tag,
          "Failure",
        );
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_captured_restart_command_origins`,
        [capturedRestartCommandOrigin],
      );
      assert.deepEqual(
        yield* sql`SELECT attempt_count, lease_owner FROM orchestration_v2_effect_outbox`,
        [{ attempt_count: 2, lease_owner: "later-worker" }],
      );
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "fresh accepted and rejected restart receipts store their origins atomically and failed insertion rolls back the receipt",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* insertCapturedRestartEffect;
      for (const [index, status] of ["accepted", "rejected"].entries()) {
        const commandId = `captured-${status}`;
        const origin = {
          ...capturedRestartCommandOrigin,
          command_id: commandId,
          command_digest: String(index).repeat(64),
        };
        const record = Effect.gen(function* () {
          yield* insertCapturedRestartReceipt(commandId, status);
          yield* sql`INSERT INTO orchestration_v2_captured_restart_command_origins ${sql.insert(origin)}`;
        });
        assert.equal(
          (yield* Effect.result(
            sql.withTransaction(
              record.pipe(Effect.andThen(Effect.fail("injected after captured restart origin"))),
            ),
          ))._tag,
          "Failure",
        );
        assert.deepEqual(
          yield* sql`SELECT * FROM orchestration_command_receipts WHERE command_id = ${commandId}`,
          [],
        );
        assert.deepEqual(
          yield* sql`SELECT * FROM orchestration_v2_captured_restart_command_origins WHERE command_id = ${commandId}`,
          [],
        );
        yield* sql.withTransaction(record);
        assert.deepEqual(
          yield* sql`SELECT * FROM orchestration_v2_captured_restart_command_origins WHERE command_id = ${commandId}`,
          [origin],
        );
        assert.deepEqual(
          yield* sql`SELECT status FROM orchestration_command_receipts WHERE command_id = ${commandId}`,
          [{ status }],
        );
      }
      assert.equal(
        (yield* Effect.result(
          sql.withTransaction(
            Effect.gen(function* () {
              yield* insertCapturedRestartReceipt("invalid-origin");
              yield* sql`INSERT INTO orchestration_v2_captured_restart_command_origins ${sql.insert({ ...capturedRestartCommandOrigin, command_id: "invalid-origin", effect_id: "missing-effect" })}`;
            }),
          ),
        ))._tag,
        "Failure",
      );
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_command_receipts WHERE command_id = 'invalid-origin'`,
        [],
      );
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_captured_restart_command_origins WHERE command_id = 'invalid-origin'`,
        [],
      );
      assert.deepEqual(
        yield* sql`SELECT status, attempt_count, lease_owner FROM orchestration_v2_effect_outbox`,
        [{ status: "running", attempt_count: 1, lease_owner: "captured-worker" }],
      );
    }).pipe(Effect.provide(memory)),
);

const nativeThreadRecoveryCommand = Schema.decodeUnknownSync(OrchestrationV2Command, {
  onExcessProperty: "error",
})({
  type: "thread.delete",
  commandId: "recovery-create:bootstrap-thread-delete",
  threadId: "recovery-thread",
});
const nativeThreadRecoveryCommandDigest = nativeCreationSha256(
  nativeCreationCanonicalJson(
    Schema.encodeSync(OrchestrationV2Command)(nativeThreadRecoveryCommand),
  ),
);
const nativeThreadRecoveryCarrier = {
  version: 2,
  claimId: "recovery-claim",
  commandId: nativeThreadRecoveryCommand.commandId,
  threadId: "recovery-thread",
  commandType: "thread.delete",
  canonicalCommand: Schema.encodeSync(OrchestrationV2Command)(nativeThreadRecoveryCommand),
  commandDigest: nativeThreadRecoveryCommandDigest,
  commandStartEffectId: "original-command-start",
  cleanupStartEffectId: "original-cleanup-start",
  cleanupStartOrdinal: 0,
  recoveryScopeId: "recovery-scope",
  resource: {
    kind: "thread",
    threadId: "recovery-thread",
    incarnation: { eventId: "recovery-birth", sequence: 1 },
  },
};
const nativeThreadRecoveryReservation = {
  command_id: nativeThreadRecoveryCarrier.commandId,
  claim_id: nativeThreadRecoveryCarrier.claimId,
  thread_id: nativeThreadRecoveryCarrier.threadId,
  command_digest: nativeThreadRecoveryCarrier.commandDigest,
  recovery_json: JSON.stringify(nativeThreadRecoveryCarrier),
  reserved_at: "2026-10-02T00:00:00Z",
};

const insertNativeRecoveryClaim = (claimId = "recovery-claim", suffix = "") =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO native_creation_intents
    (claim_id, operation_id, preparation_id, command_id, thread_id, message_id, project_cwd, branch, worktree_path, canonical_preparation, intent_json)
    VALUES (${claimId}, ${`recovery-operation${suffix}`}, ${`recovery-preparation${suffix}`}, ${`recovery-create${suffix}`}, ${`recovery-thread${suffix}`},
      ${`recovery-message${suffix}`}, '/fixture/recovery-project', ${`recovery-branch${suffix}`}, ${`/fixture/recovery-worktree${suffix}`}, '{}', '{}')`;
  });

it.effect(
  "native thread recovery reservation requires an immutable claim and constrained carrier before any deletion receipt",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      assert.equal(
        (yield* Effect.result(
          sql`INSERT INTO native_creation_thread_recovery_commands ${sql.insert(nativeThreadRecoveryReservation)}`,
        ))._tag,
        "Failure",
      );
      yield* insertNativeRecoveryClaim();
      for (const invalid of [
        { command_id: null },
        { command_id: "" },
        { claim_id: null },
        { claim_id: "missing-claim" },
        { thread_id: "" },
        { command_digest: "A".repeat(64) },
        { command_digest: "g".repeat(64) },
        { command_digest: "a".repeat(63) },
        { command_digest: "a".repeat(65) },
        { recovery_json: null },
        { recovery_json: "invalid" },
        { recovery_json: "[]" },
        { recovery_json: "null" },
        { reserved_at: "" },
      ])
        assert.equal(
          (yield* Effect.result(
            sql`INSERT INTO native_creation_thread_recovery_commands ${sql.insert({ ...nativeThreadRecoveryReservation, ...invalid })}`,
          ))._tag,
          "Failure",
        );
      yield* sql`INSERT INTO native_creation_thread_recovery_commands ${sql.insert(nativeThreadRecoveryReservation)}`;
      assert.deepEqual(yield* sql`SELECT * FROM native_creation_thread_recovery_commands`, [
        nativeThreadRecoveryReservation,
      ]);
      assert.equal(
        yield* decodeCleanupStartOrdinal(nativeThreadRecoveryReservation.recovery_json).pipe(
          Effect.catch(dieNativeJsonCause),
        ),
        0,
      );
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_command_receipts`, []);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_events`, []);
      assert.deepEqual(yield* sql`SELECT * FROM native_creation_reserved_commands`, []);
      assert.deepEqual(yield* sql`SELECT * FROM native_creation_reserved_command_identities`, []);
      assert.deepEqual(yield* sql`SELECT * FROM native_creation_effect_facts`, []);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "native recovery reservation permanently retains claim, exact command, cleanup references and thread incarnation",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* sql`PRAGMA recursive_triggers = OFF`;
      yield* insertNativeRecoveryClaim();
      yield* sql`INSERT INTO native_creation_thread_recovery_commands ${sql.insert(nativeThreadRecoveryReservation)}`;
      for (const mutation of [
        { command_id: "other-command" },
        { claim_id: "other-claim" },
        { thread_id: "other-thread" },
        { command_digest: "a".repeat(64) },
        { recovery_json: "{}" },
        { reserved_at: "2026-10-03" },
      ])
        assert.equal(
          (yield* Effect.result(
            sql`UPDATE native_creation_thread_recovery_commands ${sql.update(mutation)}`,
          ))._tag,
          "Failure",
        );
      for (const changedCarrier of [
        { ...nativeThreadRecoveryCarrier, version: 3 },
        {
          ...nativeThreadRecoveryCarrier,
          canonicalCommand: {
            ...nativeThreadRecoveryCarrier.canonicalCommand,
            threadId: "other-thread",
          },
        },
        { ...nativeThreadRecoveryCarrier, commandStartEffectId: "other-command-start" },
        { ...nativeThreadRecoveryCarrier, cleanupStartEffectId: "other-cleanup-start" },
        { ...nativeThreadRecoveryCarrier, cleanupStartOrdinal: 1 },
        { ...nativeThreadRecoveryCarrier, recoveryScopeId: "other-scope" },
        {
          ...nativeThreadRecoveryCarrier,
          resource: {
            ...nativeThreadRecoveryCarrier.resource,
            incarnation: { eventId: "replacement-birth", sequence: 2 },
          },
        },
      ])
        assert.equal(
          (yield* Effect.result(
            sql`UPDATE native_creation_thread_recovery_commands SET recovery_json = ${yield* encodeFixtureJson(changedCarrier).pipe(Effect.catch(dieNativeJsonCause))}`,
          ))._tag,
          "Failure",
        );
      for (const mutation of [
        sql`DELETE FROM native_creation_thread_recovery_commands`,
        sql`INSERT OR REPLACE INTO native_creation_thread_recovery_commands ${sql.insert({ ...nativeThreadRecoveryReservation, recovery_json: "{}" })}`,
        sql`DELETE FROM native_creation_intents WHERE claim_id = 'recovery-claim'`,
      ])
        assert.equal((yield* Effect.result(mutation))._tag, "Failure");
      assert.deepEqual(yield* sql`SELECT * FROM native_creation_thread_recovery_commands`, [
        nativeThreadRecoveryReservation,
      ]);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "native recovery command and claim collisions cannot replace the original association while unrelated claims remain independent",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* sql`PRAGMA recursive_triggers = OFF`;
      yield* insertNativeRecoveryClaim();
      yield* insertNativeRecoveryClaim("other-recovery-claim", "-other");
      yield* sql`INSERT INTO native_creation_thread_recovery_commands ${sql.insert(nativeThreadRecoveryReservation)}`;
      for (const collision of [
        {
          ...nativeThreadRecoveryReservation,
          claim_id: "other-recovery-claim",
          thread_id: "recovery-thread-other",
        },
        { ...nativeThreadRecoveryReservation, command_id: "other-command" },
      ]) {
        assert.equal(
          (yield* Effect.result(
            sql`INSERT INTO native_creation_thread_recovery_commands ${sql.insert(collision)}`,
          ))._tag,
          "Failure",
        );
        assert.equal(
          (yield* Effect.result(
            sql`INSERT OR REPLACE INTO native_creation_thread_recovery_commands ${sql.insert(collision)}`,
          ))._tag,
          "Failure",
        );
      }
      const otherCommand = yield* Schema.decodeUnknownEffect(OrchestrationV2Command, {
        onExcessProperty: "error",
      })({
        type: "thread.delete",
        commandId: "recovery-create-other:bootstrap-thread-delete",
        threadId: "recovery-thread-other",
      }).pipe(Effect.orDie);
      const otherDigest = nativeCreationSha256(
        nativeCreationCanonicalJson(
          yield* Schema.encodeEffect(OrchestrationV2Command)(otherCommand).pipe(Effect.orDie),
        ),
      );
      const otherCarrier = {
        ...nativeThreadRecoveryCarrier,
        claimId: "other-recovery-claim",
        commandId: otherCommand.commandId,
        threadId: "recovery-thread-other",
        canonicalCommand: yield* Schema.encodeEffect(OrchestrationV2Command)(otherCommand).pipe(
          Effect.orDie,
        ),
        commandDigest: otherDigest,
        commandStartEffectId: "other-command-start",
        cleanupStartEffectId: "other-cleanup-start",
        recoveryScopeId: "other-scope",
        resource: {
          kind: "thread",
          threadId: "recovery-thread-other",
          incarnation: { eventId: "other-birth", sequence: 1 },
        },
      };
      const other = {
        ...nativeThreadRecoveryReservation,
        command_id: otherCommand.commandId,
        claim_id: otherCarrier.claimId,
        thread_id: otherCarrier.threadId,
        command_digest: otherDigest,
        recovery_json: yield* encodeFixtureJson(otherCarrier).pipe(
          Effect.catch(dieNativeJsonCause),
        ),
      };
      yield* sql`INSERT INTO native_creation_thread_recovery_commands ${sql.insert(other)}`;
      assert.deepEqual(
        yield* sql`SELECT * FROM native_creation_thread_recovery_commands ORDER BY claim_id`,
        [other, nativeThreadRecoveryReservation],
      );
      assert.deepEqual(yield* sql`SELECT * FROM native_creation_reserved_commands`, []);
      assert.deepEqual(yield* sql`SELECT * FROM native_creation_reserved_command_identities`, []);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "native recovery reservation, deletion receipt and attributed V2 event roll back together without changing creation inventory",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* insertNativeRecoveryClaim();
      const record = Effect.gen(function* () {
        yield* sql`INSERT INTO native_creation_thread_recovery_commands ${sql.insert(nativeThreadRecoveryReservation)}`;
        yield* sql`INSERT INTO orchestration_command_receipts
        (command_id, command_type, aggregate_kind, aggregate_id, accepted_at, result_sequence, status)
        VALUES (${nativeThreadRecoveryReservation.command_id}, 'thread.delete', 'thread', 'recovery-thread', '2026-10-02', 2, 'accepted')`;
        yield* sql`INSERT INTO orchestration_events
        (sequence, event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, command_id, actor_kind, payload_json, metadata_json, application_event_version)
        VALUES (2, 'recovery-deleted', 'thread', 'recovery-thread', 2, 'thread.deleted', '2026-10-02', ${nativeThreadRecoveryReservation.command_id}, 'server', '{}', '{}', 2)`;
      });
      assert.equal(
        (yield* Effect.result(
          sql.withTransaction(
            record.pipe(Effect.andThen(Effect.fail("injected after recovery deletion event"))),
          ),
        ))._tag,
        "Failure",
      );
      assert.deepEqual(yield* sql`SELECT * FROM native_creation_thread_recovery_commands`, []);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_command_receipts`, []);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_events`, []);
      yield* sql.withTransaction(record);
      assert.deepEqual(yield* sql`SELECT * FROM native_creation_thread_recovery_commands`, [
        nativeThreadRecoveryReservation,
      ]);
      assert.deepEqual(
        yield* sql`SELECT command_id, command_type, status FROM orchestration_command_receipts`,
        [
          {
            command_id: nativeThreadRecoveryReservation.command_id,
            command_type: "thread.delete",
            status: "accepted",
          },
        ],
      );
      assert.deepEqual(
        yield* sql`SELECT event_id, sequence, command_id, application_event_version FROM orchestration_events`,
        [
          {
            event_id: "recovery-deleted",
            sequence: 2,
            command_id: nativeThreadRecoveryReservation.command_id,
            application_event_version: 2,
          },
        ],
      );
      assert.deepEqual(yield* sql`SELECT * FROM native_creation_reserved_commands`, []);
      assert.deepEqual(yield* sql`SELECT * FROM native_creation_reserved_command_identities`, []);
      assert.deepEqual(yield* sql`SELECT * FROM native_creation_effect_facts`, []);
      assert.deepEqual(
        yield* sql`SELECT canonical_preparation, intent_json FROM native_creation_intents`,
        [{ canonical_preparation: "{}", intent_json: "{}" }],
      );
    }).pipe(Effect.provide(memory)),
);

const leaseCleanupLease = {
  resourcePath: "/fixture/cleanup-worktree",
  leaseId: "cleanup-lease",
  ownerThreadId: "cleanup-owner",
  ownerIncarnation: "opaque-original-incarnation",
  branch: null,
  acquiredAtMs: 0,
  renewedAtMs: 0,
  expiresAtMs: 100,
};
const leaseCleanupOwnerBirth = {
  kind: "application_v2_thread_birth",
  threadId: "cleanup-owner",
  eventId: "cleanup-birth",
  sequence: 1,
};
const leaseCleanupDeletion = {
  commandId: "cleanup-delete",
  eventId: "cleanup-deleted",
  sequence: 2,
};
const leaseCleanupProviderTask = {
  kind: "provider",
  evidenceRevision: 1,
  expectedBinding: {
    threadId: "cleanup-owner",
    providerThreadId: "cleanup-provider-thread",
    providerSessionId: "cleanup-session",
    instanceId: "cleanup-instance",
    driver: "codex",
    nativeThreadId: "cleanup-native-thread",
    runtimeGeneration: "cleanup-generation",
  },
};
const leaseCleanupBindingInput = {
  version: 2,
  effectId: "cleanup-effect",
  threadId: "cleanup-owner",
  lease: leaseCleanupLease,
  ownerBirth: leaseCleanupOwnerBirth,
  deletion: leaseCleanupDeletion,
  task: leaseCleanupProviderTask,
};
const leaseCleanupBindingDigest = nativeCreationSha256(
  nativeCreationCanonicalJson(leaseCleanupBindingInput),
);
const leaseCleanupTaskBinding = {
  effect_id: leaseCleanupBindingInput.effectId,
  thread_id: leaseCleanupBindingInput.threadId,
  lease_json: JSON.stringify(leaseCleanupLease),
  owner_birth_json: JSON.stringify(leaseCleanupOwnerBirth),
  deletion_json: JSON.stringify(leaseCleanupDeletion),
  task_json: JSON.stringify(leaseCleanupProviderTask),
  binding_sha256: leaseCleanupBindingDigest,
  recorded_at: "2026-10-02T00:00:00Z",
};
const leaseCleanupTaskOutcome = {
  effect_id: leaseCleanupTaskBinding.effect_id,
  ordinal: 0,
  outcome_json: '{"taskId":"cleanup-effect","result":"failed","effect":"no_effect"}',
  correlation_json: JSON.stringify({
    workerId: "cleanup-worker",
    expectedAttempt: 1,
    bindingSha256: leaseCleanupBindingDigest,
    evidence: { operationId: "cleanup-operation", outcome: "known_no_effect" },
  }),
  recorded_at: "2026-10-02T00:00:00Z",
};
const insertLeaseCleanupEffect = (effectId = "cleanup-effect") =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO orchestration_v2_effect_outbox
    (effect_id, command_id, thread_id, effect_type, payload_json, status, attempt_count, available_at, lease_owner, lease_expires_at, created_at, updated_at)
    VALUES (${effectId}, 'cleanup-delete', 'cleanup-owner', 'provider-session.stop', '{}', 'running', 1, '2026-10-02', 'cleanup-worker', '2099-01-01', '2026-10-02', '2026-10-02')`;
  });

it.effect(
  "lease cleanup bindings require an existing effect and preserve full provider, terminal and attachment target carriers",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      assert.equal(
        (yield* Effect.result(
          sql`INSERT INTO orchestration_v2_lease_cleanup_task_bindings ${sql.insert(leaseCleanupTaskBinding)}`,
        ))._tag,
        "Failure",
      );
      yield* insertLeaseCleanupEffect();
      for (const invalid of [
        { effect_id: null },
        { effect_id: "missing-effect" },
        { thread_id: "" },
        { lease_json: null },
        { lease_json: "invalid" },
        { lease_json: "[]" },
        { owner_birth_json: null },
        { owner_birth_json: "invalid" },
        { owner_birth_json: "[]" },
        { deletion_json: null },
        { deletion_json: "invalid" },
        { deletion_json: "[]" },
        { task_json: null },
        { task_json: "invalid" },
        { task_json: "[]" },
        { binding_sha256: "A".repeat(64) },
        { binding_sha256: "g".repeat(64) },
        { binding_sha256: "a".repeat(63) },
        { binding_sha256: "a".repeat(65) },
        { recorded_at: "" },
      ])
        assert.equal(
          (yield* Effect.result(
            sql`INSERT INTO orchestration_v2_lease_cleanup_task_bindings ${sql.insert({ ...leaseCleanupTaskBinding, ...invalid })}`,
          ))._tag,
          "Failure",
        );
      yield* sql`INSERT INTO orchestration_v2_lease_cleanup_task_bindings ${sql.insert(leaseCleanupTaskBinding)}`;
      const tasks = [
        {
          effectId: "cleanup-terminal",
          task: {
            kind: "terminal",
            capture: {
              managerId: "synthetic-manager",
              threadId: "cleanup-owner",
              ownerBirth: leaseCleanupOwnerBirth,
              status: "captured",
              managedTargetsOnly: true,
              targets: [
                {
                  threadId: "cleanup-owner",
                  terminalId: "synthetic-terminal",
                  handleId: "synthetic-handle",
                  ownerBirth: leaseCleanupOwnerBirth,
                },
              ],
            },
          },
        },
        {
          effectId: "cleanup-attachment",
          task: { kind: "attachment", attachmentIds: ["attachment-one", "attachment-two"] },
        },
      ];
      for (const { effectId, task } of tasks) {
        yield* insertLeaseCleanupEffect(effectId);
        const binding = {
          ...leaseCleanupTaskBinding,
          effect_id: effectId,
          task_json: yield* encodeFixtureJson(task).pipe(Effect.catch(dieNativeJsonCause)),
          binding_sha256: nativeCreationSha256(
            nativeCreationCanonicalJson({ ...leaseCleanupBindingInput, effectId, task }),
          ),
        };
        yield* sql`INSERT INTO orchestration_v2_lease_cleanup_task_bindings ${sql.insert(binding)}`;
        assert.deepEqual(
          yield* sql`SELECT * FROM orchestration_v2_lease_cleanup_task_bindings WHERE effect_id = ${effectId}`,
          [binding],
        );
      }
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_lease_cleanup_task_bindings WHERE effect_id = 'cleanup-effect'`,
        [leaseCleanupTaskBinding],
      );
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_command_receipts`, []);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "lease cleanup binding bytes remain immutable when provider ownership advances and cannot be replaced or detached from their effect",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* sql`PRAGMA recursive_triggers = OFF`;
      yield* insertLeaseCleanupEffect();
      yield* sql`INSERT INTO orchestration_v2_lease_cleanup_task_bindings ${sql.insert(leaseCleanupTaskBinding)}`;
      yield* sql`INSERT INTO orchestration_v2_provider_runtime_evidence
      (thread_id, provider_thread_id, provider_session_id, provider_instance_id, driver, native_thread_id, runtime_generation, evidence_revision, observation_json, registered_at)
      VALUES ('cleanup-owner', 'cleanup-provider-thread', 'cleanup-session', 'cleanup-instance', 'codex', 'cleanup-native-thread', 'cleanup-generation', 1, NULL, '2026-10-02')`;
      yield* sql`UPDATE orchestration_v2_provider_runtime_evidence SET provider_thread_id = 'replacement-provider',
      provider_session_id = 'replacement-session', native_thread_id = 'replacement-native', runtime_generation = 'replacement-generation',
      evidence_revision = 2 WHERE thread_id = 'cleanup-owner'`;
      for (const mutation of [
        { effect_id: "other-effect" },
        { thread_id: "other-owner" },
        { lease_json: "{}" },
        { owner_birth_json: "{}" },
        { deletion_json: "{}" },
        { task_json: "{}" },
        { binding_sha256: "a".repeat(64) },
        { recorded_at: "2026-10-03" },
      ])
        assert.equal(
          (yield* Effect.result(
            sql`UPDATE orchestration_v2_lease_cleanup_task_bindings ${sql.update(mutation)}`,
          ))._tag,
          "Failure",
        );
      for (const mutation of [
        sql`DELETE FROM orchestration_v2_lease_cleanup_task_bindings`,
        sql`INSERT OR REPLACE INTO orchestration_v2_lease_cleanup_task_bindings ${sql.insert({ ...leaseCleanupTaskBinding, task_json: "{}" })}`,
        sql`DELETE FROM orchestration_v2_effect_outbox WHERE effect_id = 'cleanup-effect'`,
      ])
        assert.equal((yield* Effect.result(mutation))._tag, "Failure");
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_lease_cleanup_task_bindings`, [
        leaseCleanupTaskBinding,
      ]);
      assert.deepEqual(
        yield* sql`SELECT provider_thread_id, evidence_revision FROM orchestration_v2_provider_runtime_evidence`,
        [{ provider_thread_id: "replacement-provider", evidence_revision: 2 }],
      );
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "lease cleanup outcomes require their binding and nonnegative ordinal and append failed and unknown evidence without overwriting it",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      assert.equal(
        (yield* Effect.result(
          sql`INSERT INTO orchestration_v2_lease_cleanup_task_outcomes ${sql.insert(leaseCleanupTaskOutcome)}`,
        ))._tag,
        "Failure",
      );
      yield* insertLeaseCleanupEffect();
      yield* sql`INSERT INTO orchestration_v2_lease_cleanup_task_bindings ${sql.insert(leaseCleanupTaskBinding)}`;
      for (const invalid of [
        { effect_id: null },
        { effect_id: "missing-binding" },
        { ordinal: null },
        { ordinal: -1 },
        { outcome_json: null },
        { outcome_json: "invalid" },
        { outcome_json: "[]" },
        { correlation_json: null },
        { correlation_json: "invalid" },
        { correlation_json: "[]" },
        { recorded_at: "" },
      ])
        assert.equal(
          (yield* Effect.result(
            sql`INSERT INTO orchestration_v2_lease_cleanup_task_outcomes ${sql.insert({ ...leaseCleanupTaskOutcome, ...invalid })}`,
          ))._tag,
          "Failure",
        );
      yield* sql`INSERT INTO orchestration_v2_lease_cleanup_task_outcomes ${sql.insert(leaseCleanupTaskOutcome)}`;
      const unknown = {
        ...leaseCleanupTaskOutcome,
        ordinal: 1,
        outcome_json: '{"taskId":"cleanup-effect","result":null,"effect":"unknown"}',
        correlation_json: yield* encodeFixtureJson({
          workerId: "cleanup-worker",
          expectedAttempt: 1,
          bindingSha256: leaseCleanupBindingDigest,
          evidence: { operationId: "cleanup-operation", outcome: "unknown" },
        }).pipe(Effect.catch(dieNativeJsonCause)),
      };
      yield* sql`INSERT INTO orchestration_v2_lease_cleanup_task_outcomes ${sql.insert(unknown)}`;
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_lease_cleanup_task_outcomes ORDER BY ordinal`,
        [leaseCleanupTaskOutcome, unknown],
      );
      assert.deepEqual(yield* sql`SELECT status, lease_owner FROM orchestration_v2_effect_outbox`, [
        { status: "running", lease_owner: "cleanup-worker" },
      ]);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "lease cleanup outcome ordinals and all correlated evidence remain permanent with recursive triggers disabled",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* sql`PRAGMA recursive_triggers = OFF`;
      yield* insertLeaseCleanupEffect();
      yield* sql`INSERT INTO orchestration_v2_lease_cleanup_task_bindings ${sql.insert(leaseCleanupTaskBinding)}`;
      yield* sql`INSERT INTO orchestration_v2_lease_cleanup_task_outcomes ${sql.insert(leaseCleanupTaskOutcome)}`;
      for (const mutation of [
        { effect_id: "other-effect" },
        { ordinal: 1 },
        { outcome_json: "{}" },
        { correlation_json: "{}" },
        { recorded_at: "2026-10-03" },
      ])
        assert.equal(
          (yield* Effect.result(
            sql`UPDATE orchestration_v2_lease_cleanup_task_outcomes ${sql.update(mutation)}`,
          ))._tag,
          "Failure",
        );
      for (const mutation of [
        sql`DELETE FROM orchestration_v2_lease_cleanup_task_outcomes`,
        sql`INSERT OR REPLACE INTO orchestration_v2_lease_cleanup_task_outcomes ${sql.insert({ ...leaseCleanupTaskOutcome, correlation_json: "{}" })}`,
        sql`INSERT INTO orchestration_v2_lease_cleanup_task_outcomes ${sql.insert(leaseCleanupTaskOutcome)}`,
      ])
        assert.equal((yield* Effect.result(mutation))._tag, "Failure");
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_lease_cleanup_task_outcomes`, [
        leaseCleanupTaskOutcome,
      ]);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "cleanup binding and unknown outcome roll back together while preserving the exact retained lease and running claim",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* insertLeaseCleanupEffect();
      yield* sql`INSERT INTO worktree_ownership_leases
      (resource_path, lease_id, owner_thread_id, owner_incarnation, branch, acquired_at_ms, renewed_at_ms, expires_at_ms)
      VALUES ('/fixture/cleanup-worktree', 'cleanup-lease', 'cleanup-owner', 'opaque-original-incarnation', NULL, 0, 0, 100)`;
      const leaseBefore = yield* sql`SELECT * FROM worktree_ownership_leases`;
      const unknown = {
        ...leaseCleanupTaskOutcome,
        outcome_json: '{"taskId":"cleanup-effect","result":null,"effect":"unknown"}',
        correlation_json: yield* encodeFixtureJson({
          workerId: "cleanup-worker",
          expectedAttempt: 1,
          bindingSha256: leaseCleanupBindingDigest,
          evidence: { operationId: "cleanup-operation", outcome: "unknown" },
        }).pipe(Effect.catch(dieNativeJsonCause)),
      };
      const record = Effect.gen(function* () {
        yield* sql`INSERT INTO orchestration_v2_lease_cleanup_task_bindings ${sql.insert(leaseCleanupTaskBinding)}`;
        yield* sql`INSERT INTO orchestration_v2_lease_cleanup_task_outcomes ${sql.insert(unknown)}`;
      });
      assert.equal(
        (yield* Effect.result(
          sql.withTransaction(
            record.pipe(Effect.andThen(Effect.fail("injected after cleanup outcome"))),
          ),
        ))._tag,
        "Failure",
      );
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_lease_cleanup_task_bindings`, []);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_lease_cleanup_task_outcomes`, []);
      assert.deepEqual(yield* sql`SELECT * FROM worktree_ownership_leases`, leaseBefore);
      yield* sql.withTransaction(record);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_lease_cleanup_task_bindings`, [
        leaseCleanupTaskBinding,
      ]);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_lease_cleanup_task_outcomes`, [
        unknown,
      ]);
      assert.deepEqual(yield* sql`SELECT * FROM worktree_ownership_leases`, leaseBefore);
      assert.deepEqual(
        yield* sql`SELECT status, attempt_count, lease_owner, lease_expires_at FROM orchestration_v2_effect_outbox`,
        [
          {
            status: "running",
            attempt_count: 1,
            lease_owner: "cleanup-worker",
            lease_expires_at: "2099-01-01",
          },
        ],
      );
    }).pipe(Effect.provide(memory)),
);

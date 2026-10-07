import { assert, it } from "@effect/vitest";
import { OrchestrationV2Command } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { canonicalJson, sha256 } from "../../orchestration-v2/CanonicalJson.ts";
import { migrationManifest, runMigrations } from "../Migrations.ts";

const memory = NodeSqliteClient.layer({ filename: ":memory:" });
const timestamp = "2026-10-03T00:00:00Z";
const startedAt = "2026-10-03T00:00:01Z";
const updatedAt = "2026-10-03T00:00:02Z";
// Same bytes as JSON.stringify; a failure stays a defect as the native throw was.
const encodeJson = (value: unknown) =>
  Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown))(value).pipe(Effect.orDie);
const consent = {
  projectId: "project:deletion-fixture",
  path: "/fixture/worktrees/original",
  branch: "fixture-branch",
  force: true,
};
const canonicalDeletion = (commandId = "command:deletion-fixture", withConsent = true) =>
  canonicalJson(
    Schema.encodeSync(OrchestrationV2Command)(
      Schema.decodeUnknownSync(OrchestrationV2Command, { onExcessProperty: "error" })({
        type: "thread.delete",
        commandId,
        threadId: "thread:deletion-fixture",
        ...(withConsent ? { worktreeRemoval: consent } : {}),
      }),
    ),
  );
const inventory = {
  worktree: { projectId: consent.projectId, path: consent.path, branch: consent.branch },
  projectRoot: "/fixture/repo",
  leaseInventory: { status: "absent" },
  prerequisiteEffectIds: ["effect:deletion-fixture:terminal"],
  captureStatus: "captured",
  reason: null,
};
const deletionCommand = {
  command_id: "command:deletion-fixture",
  thread_id: "thread:deletion-fixture",
  canonical_command_json: canonicalDeletion(),
  command_digest: sha256(canonicalDeletion()),
  owner_birth_json:
    '{"birth":{"kind":"application_v2_thread_birth","threadId":"thread:deletion-fixture","eventId":"event:deletion-fixture:birth","sequence":1}}',
  worktree_inventory_json: JSON.stringify(inventory),
  deletion_event_id: "event:deletion-fixture:deleted",
  deletion_event_sequence: 2,
  recorded_at: timestamp,
};
const removalAdmission = {
  operation_id: "operation:removal-fixture",
  canonical_path: consent.path,
  kind: "worktree_removal",
  subject_json: JSON.stringify({
    version: 1,
    effectId: "effect:command:deletion-fixture:worktree.cleanup:thread:deletion-fixture",
    threadId: deletionCommand.thread_id,
    commandId: deletionCommand.command_id,
    bindingSha256: "a".repeat(64),
  }),
  state: "reserved",
  started_at: null,
  outcome_json: null,
  recorded_at: timestamp,
  updated_at: timestamp,
};
const syntheticOutcome = '{"fixture":"bounded storage outcome"}';

const insertDeletionParents = (
  commandId = deletionCommand.command_id,
  eventId = deletionCommand.deletion_event_id,
  sequence = 2,
) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO orchestration_command_receipts
    (command_id, command_type, aggregate_kind, aggregate_id, accepted_at, result_sequence, status)
    VALUES (${commandId}, 'thread.delete', 'thread', ${deletionCommand.thread_id}, ${timestamp}, ${sequence}, 'accepted')`;
    yield* sql`INSERT INTO orchestration_events
    (sequence, event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, command_id, actor_kind, payload_json, metadata_json, application_event_version)
    VALUES (${sequence}, ${eventId}, 'thread', ${deletionCommand.thread_id}, ${sequence}, 'thread.deleted', ${timestamp}, ${commandId}, 'server', '{}', '{}', 2)`;
  });

it.effect(
  "records Jones139 after released upstream56 and bounded upstream replay excludes the new private tables",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 56 });
      const history = yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`;
      assert.deepEqual(
        history.map((row): readonly [unknown, unknown] => [row.migration_id, row.name]),
        migrationManifest,
      );
      assert.deepEqual(
        yield* sql`SELECT name FROM sqlite_master WHERE name IN ('orchestration_v2_thread_deletion_commands', 'orchestration_v2_worktree_path_admissions')`,
        [],
      );
      assert.deepEqual(yield* runMigrations(), []);
      assert.deepEqual(yield* runMigrations(), []);
      assert.deepEqual(
        yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`,
        history,
      );
      assert.deepEqual(
        yield* sql`SELECT migration_id, name FROM jones_sql_migrations WHERE migration_id = 139`,
        [{ migration_id: 139, name: "DeletionWorktreeAdmission" }],
      );
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_thread_deletion_commands`, []);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_worktree_path_admissions`, []);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "original deletion capture requires receipt and event parents plus constrained object and digest carriers",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      assert.equal(
        (yield* Effect.result(
          sql`INSERT INTO orchestration_v2_thread_deletion_commands ${sql.insert(deletionCommand)}`,
        ))._tag,
        "Failure",
      );
      yield* insertDeletionParents();
      for (const invalid of [
        { command_id: null },
        { command_id: "missing-receipt" },
        { thread_id: "" },
        { canonical_command_json: null },
        { canonical_command_json: "invalid" },
        { canonical_command_json: "[]" },
        { command_digest: "A".repeat(64) },
        { command_digest: "g".repeat(64) },
        { command_digest: "a".repeat(63) },
        { owner_birth_json: null },
        { owner_birth_json: "invalid" },
        { owner_birth_json: "[]" },
        { worktree_inventory_json: null },
        { worktree_inventory_json: "invalid" },
        { worktree_inventory_json: "[]" },
        { deletion_event_id: "missing-event" },
        { deletion_event_sequence: 0 },
        { deletion_event_sequence: 99 },
        { recorded_at: "" },
      ])
        assert.equal(
          (yield* Effect.result(
            sql`INSERT INTO orchestration_v2_thread_deletion_commands ${sql.insert({ ...deletionCommand, ...invalid })}`,
          ))._tag,
          "Failure",
        );
      yield* sql`INSERT INTO orchestration_v2_thread_deletion_commands ${sql.insert(deletionCommand)}`;
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_thread_deletion_commands`, [
        deletionCommand,
      ]);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "original consent and no-consent captures remain permanent and historical receipts cannot manufacture missing captures",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* sql`PRAGMA recursive_triggers = OFF`;
      yield* insertDeletionParents();
      yield* sql`INSERT INTO orchestration_v2_thread_deletion_commands ${sql.insert(deletionCommand)}`;
      yield* insertDeletionParents("command:no-consent", "event:no-consent", 4);
      const canonical = canonicalDeletion("command:no-consent", false);
      const noConsentInventoryJson = yield* encodeJson({
        ...inventory,
        leaseInventory: { status: "unavailable" },
        captureStatus: "retained",
        reason: "not_requested",
      });
      const noConsent = {
        ...deletionCommand,
        command_id: "command:no-consent",
        canonical_command_json: canonical,
        command_digest: sha256(canonical),
        owner_birth_json: '{"birth":null}',
        worktree_inventory_json: noConsentInventoryJson,
        deletion_event_id: "event:no-consent",
        deletion_event_sequence: 4,
      };
      yield* sql`INSERT INTO orchestration_v2_thread_deletion_commands ${sql.insert(noConsent)}`;
      for (const mutation of [
        { command_id: noConsent.command_id },
        { thread_id: "other-thread" },
        { canonical_command_json: canonicalDeletion(undefined, false) },
        { command_digest: "b".repeat(64) },
        { owner_birth_json: '{"birth":null}' },
        { worktree_inventory_json: "{}" },
        { deletion_event_id: noConsent.deletion_event_id },
        { deletion_event_sequence: noConsent.deletion_event_sequence },
        { recorded_at: updatedAt },
      ])
        assert.equal(
          (yield* Effect.result(
            sql`UPDATE orchestration_v2_thread_deletion_commands ${sql.update(mutation)} WHERE command_id = ${deletionCommand.command_id}`,
          ))._tag,
          "Failure",
        );
      for (const mutation of [
        sql`DELETE FROM orchestration_v2_thread_deletion_commands`,
        sql`INSERT OR REPLACE INTO orchestration_v2_thread_deletion_commands ${sql.insert({
          ...deletionCommand,
          canonical_command_json: canonicalDeletion(undefined, false),
          command_digest: sha256(canonicalDeletion(undefined, false)),
        })}`,
        sql`DELETE FROM orchestration_command_receipts WHERE command_id = ${deletionCommand.command_id}`,
        sql`DELETE FROM orchestration_events WHERE event_id = ${deletionCommand.deletion_event_id}`,
      ])
        assert.equal((yield* Effect.result(mutation))._tag, "Failure");
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_thread_deletion_commands ORDER BY command_id`,
        [deletionCommand, noConsent],
      );
      yield* insertDeletionParents("command:historical", "event:historical", 6);
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_thread_deletion_commands WHERE command_id = 'command:historical'`,
        [],
      );
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "path admission constrains original identity and state payloads without requiring unrelated receipt authority",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      for (const invalid of [
        { operation_id: null },
        { operation_id: "" },
        { canonical_path: "" },
        { kind: "global_lock" },
        { subject_json: null },
        { subject_json: "invalid" },
        { subject_json: "[]" },
        { state: "pending" },
        { recorded_at: "" },
        { updated_at: "" },
        { started_at: startedAt },
        { outcome_json: syntheticOutcome },
        { state: "started" },
        { state: "completed", started_at: startedAt },
        { state: "completed", outcome_json: syntheticOutcome },
        { state: "unknown" },
        { state: "no_effect" },
        { state: "released" },
        { state: "unknown", outcome_json: "invalid" },
        { state: "unknown", outcome_json: "[]" },
      ])
        assert.equal(
          (yield* Effect.result(
            sql`INSERT INTO orchestration_v2_worktree_path_admissions ${sql.insert({ ...removalAdmission, ...invalid })}`,
          ))._tag,
          "Failure",
        );
      yield* sql`INSERT INTO orchestration_v2_worktree_path_admissions ${sql.insert(removalAdmission)}`;
      yield* sql`INSERT INTO orchestration_v2_worktree_path_admissions ${sql.insert({
        ...removalAdmission,
        operation_id: "operation:native-independent",
        canonical_path: "/fixture/worktrees/independent",
        kind: "native_operation",
        subject_json: '{"fixture":"native-operation identity"}',
      })}`;
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_command_receipts`, []);
      assert.equal((yield* sql`SELECT * FROM orchestration_v2_worktree_path_admissions`).length, 2);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "reserved, started, unknown and completed admissions exclude competing native and removal identities until released",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA recursive_triggers = OFF`;
      yield* sql`INSERT INTO orchestration_v2_worktree_path_admissions ${sql.insert(removalAdmission)}`;
      for (const stage of ["reserved", "started", "unknown", "completed"]) {
        if (stage === "started")
          yield* sql`UPDATE orchestration_v2_worktree_path_admissions SET state = 'started', started_at = ${startedAt}, updated_at = ${updatedAt}`;
        if (stage === "unknown")
          yield* sql`UPDATE orchestration_v2_worktree_path_admissions SET state = 'unknown', outcome_json = ${syntheticOutcome}, updated_at = ${updatedAt}`;
        if (stage === "completed")
          yield* sql`UPDATE orchestration_v2_worktree_path_admissions SET state = 'completed', outcome_json = ${syntheticOutcome}, updated_at = ${updatedAt}`;
        for (const kind of ["native_operation", "worktree_removal"]) {
          const competing = {
            ...removalAdmission,
            operation_id: `operation:competing:${stage}:${kind}`,
            kind,
          };
          assert.equal(
            (yield* Effect.result(
              sql`INSERT INTO orchestration_v2_worktree_path_admissions ${sql.insert(competing)}`,
            ))._tag,
            "Failure",
          );
          assert.equal(
            (yield* Effect.result(
              sql`INSERT OR REPLACE INTO orchestration_v2_worktree_path_admissions ${sql.insert(competing)}`,
            ))._tag,
            "Failure",
          );
        }
        assert.deepEqual(
          yield* sql`SELECT state, started_at FROM orchestration_v2_worktree_path_admissions`,
          [{ state: stage, started_at: stage === "reserved" ? null : startedAt }],
        );
      }
      yield* sql`UPDATE orchestration_v2_worktree_path_admissions SET state = 'released', outcome_json = ${syntheticOutcome}, updated_at = ${updatedAt}`;
      const replacement = {
        ...removalAdmission,
        operation_id: "operation:fresh-native",
        kind: "native_operation",
        subject_json: '{"fixture":"fresh identity"}',
      };
      yield* sql`INSERT INTO orchestration_v2_worktree_path_admissions ${sql.insert(replacement)}`;
      assert.deepEqual(
        yield* sql`SELECT operation_id, state FROM orchestration_v2_worktree_path_admissions ORDER BY operation_id`,
        [
          { operation_id: replacement.operation_id, state: "reserved" },
          { operation_id: removalAdmission.operation_id, state: "released" },
        ],
      );
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "admission identities and their first persisted start cannot change during otherwise allowed transitions",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA recursive_triggers = OFF`;
      yield* sql`INSERT INTO orchestration_v2_worktree_path_admissions ${sql.insert(removalAdmission)}`;
      for (const mutation of [
        { operation_id: "other-operation" },
        { canonical_path: "/fixture/worktrees/other" },
        { kind: "native_operation" },
        { subject_json: '{"fixture":"changed identity"}' },
        { recorded_at: updatedAt },
      ])
        assert.equal(
          (yield* Effect.result(
            sql`UPDATE orchestration_v2_worktree_path_admissions ${sql.update({
              ...mutation,
              state: "started",
              started_at: startedAt,
              updated_at: updatedAt,
            })}`,
          ))._tag,
          "Failure",
        );
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_worktree_path_admissions`, [
        removalAdmission,
      ]);
      yield* sql`UPDATE orchestration_v2_worktree_path_admissions SET state = 'started', started_at = ${startedAt}, updated_at = ${updatedAt}`;
      for (const replacement of [null, updatedAt])
        assert.equal(
          (yield* Effect.result(
            sql`UPDATE orchestration_v2_worktree_path_admissions SET state = 'unknown', started_at = ${replacement}, outcome_json = ${syntheticOutcome}, updated_at = ${updatedAt}`,
          ))._tag,
          "Failure",
        );
      assert.equal(
        (yield* Effect.result(
          sql`INSERT OR REPLACE INTO orchestration_v2_worktree_path_admissions ${sql.insert(removalAdmission)}`,
        ))._tag,
        "Failure",
      );
      assert.equal(
        (yield* Effect.result(sql`DELETE FROM orchestration_v2_worktree_path_admissions`))._tag,
        "Failure",
      );
      assert.deepEqual(
        yield* sql`SELECT state, started_at FROM orchestration_v2_worktree_path_admissions`,
        [{ state: "started", started_at: startedAt }],
      );
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "unknown admission may retain no start until reconciliation while forbidden shortcuts and terminal rewrites are rejected",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA recursive_triggers = OFF`;
      yield* sql`INSERT INTO orchestration_v2_worktree_path_admissions ${sql.insert(removalAdmission)}`;
      for (const shortcut of [
        { state: "completed", started_at: startedAt, outcome_json: syntheticOutcome },
        { state: "released", outcome_json: syntheticOutcome },
        { updated_at: updatedAt },
      ])
        assert.equal(
          (yield* Effect.result(
            sql`UPDATE orchestration_v2_worktree_path_admissions ${sql.update({ ...shortcut, updated_at: updatedAt })}`,
          ))._tag,
          "Failure",
        );
      yield* sql`UPDATE orchestration_v2_worktree_path_admissions SET state = 'unknown', outcome_json = ${syntheticOutcome}, updated_at = ${updatedAt}`;
      assert.deepEqual(
        yield* sql`SELECT state, started_at FROM orchestration_v2_worktree_path_admissions`,
        [{ state: "unknown", started_at: null }],
      );
      for (const shortcut of [
        { state: "reserved", outcome_json: null },
        { state: "started", started_at: startedAt, outcome_json: null },
        { state: "released", outcome_json: syntheticOutcome },
      ])
        assert.equal(
          (yield* Effect.result(
            sql`UPDATE orchestration_v2_worktree_path_admissions ${sql.update({ ...shortcut, updated_at: updatedAt })}`,
          ))._tag,
          "Failure",
        );
      yield* sql`UPDATE orchestration_v2_worktree_path_admissions SET state = 'no_effect', outcome_json = ${syntheticOutcome}, updated_at = ${updatedAt}`;
      for (const terminalMutation of [
        sql`UPDATE orchestration_v2_worktree_path_admissions SET state = 'unknown', updated_at = ${updatedAt}`,
        sql`UPDATE orchestration_v2_worktree_path_admissions SET outcome_json = '{}'`,
        sql`DELETE FROM orchestration_v2_worktree_path_admissions`,
        sql`INSERT OR REPLACE INTO orchestration_v2_worktree_path_admissions ${sql.insert({ ...removalAdmission, canonical_path: "/fixture/worktrees/different" })}`,
      ])
        assert.equal((yield* Effect.result(terminalMutation))._tag, "Failure");
      yield* sql`INSERT INTO orchestration_v2_worktree_path_admissions ${sql.insert({ ...removalAdmission, operation_id: "operation:after-no-effect", kind: "native_operation" })}`;
      assert.deepEqual(
        yield* sql`SELECT state FROM orchestration_v2_worktree_path_admissions WHERE operation_id = ${removalAdmission.operation_id}`,
        [{ state: "no_effect" }],
      );
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "deletion receipt, attributed event, original capture and reserved path share one rollback boundary",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      const record = Effect.gen(function* () {
        yield* insertDeletionParents();
        yield* sql`INSERT INTO orchestration_v2_thread_deletion_commands ${sql.insert(deletionCommand)}`;
        yield* sql`INSERT INTO orchestration_v2_worktree_path_admissions ${sql.insert(removalAdmission)}`;
      });
      assert.equal(
        (yield* Effect.result(
          sql.withTransaction(
            record.pipe(Effect.andThen(Effect.fail("injected after path reservation"))),
          ),
        ))._tag,
        "Failure",
      );
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_command_receipts`, []);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_events`, []);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_thread_deletion_commands`, []);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_worktree_path_admissions`, []);
      yield* sql.withTransaction(record);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_thread_deletion_commands`, [
        deletionCommand,
      ]);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_worktree_path_admissions`, [
        removalAdmission,
      ]);
      assert.deepEqual(yield* sql`SELECT command_id, status FROM orchestration_command_receipts`, [
        { command_id: deletionCommand.command_id, status: "accepted" },
      ]);
      assert.deepEqual(
        yield* sql`SELECT event_id, sequence, command_id FROM orchestration_events`,
        [
          {
            event_id: deletionCommand.deletion_event_id,
            sequence: deletionCommand.deletion_event_sequence,
            command_id: deletionCommand.command_id,
          },
        ],
      );
    }).pipe(Effect.provide(memory)),
);

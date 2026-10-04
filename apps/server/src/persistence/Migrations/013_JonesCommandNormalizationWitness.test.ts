import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SchemaGetter from "effect/SchemaGetter";
import * as SchemaIssue from "effect/SchemaIssue";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as Migrator from "effect/unstable/sql/Migrator";

import * as Witness from "../../orchestration-v2/NormalizationWitness.ts";
import { migrationManifest, runMigrations } from "../Migrations.ts";
import JonesMigration0001 from "./001_JonesWorktreeOwnershipLeases.ts";
import JonesMigration0002 from "./002_JonesProjectionThreadRuntimeIdentity.ts";
import JonesMigration0003 from "./003_JonesNativeCreationIntents.ts";
import JonesMigration0004 from "./004_JonesNativeCreationCommandIdentities.ts";
import JonesMigration0005 from "./005_JonesWorkstreamsNativeAttempts.ts";
import JonesMigration0006 from "./006_JonesWorkstreamsProviderEnrollments.ts";
import JonesMigration0007 from "./007_JonesV2NativeAcceptance.ts";
import JonesMigration0008 from "./008_JonesDeletionWorktreeAdmission.ts";
import JonesMigration0009 from "./009_JonesOrdinaryCheckoutOwnership.ts";
import JonesMigration0010 from "./010_JonesAttachmentCleanup.ts";
import JonesMigration0011 from "./011_JonesOrdinaryCheckoutExecutionLifetime.ts";
import JonesMigration0012 from "./012_JonesImportedApplicationAttachments.ts";

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

const memory = NodeSqliteClient.layer({ filename: ":memory:" });
const runForkMigrations = Migrator.make({});
const priorForkLoader = Migrator.fromRecord({
  "1_WorktreeOwnershipLeases": JonesMigration0001,
  "2_ProjectionThreadRuntimeIdentity": JonesMigration0002,
  "3_NativeCreationIntents": JonesMigration0003,
  "4_NativeCreationCommandIdentities": JonesMigration0004,
  "5_WorkstreamsNativeAttempts": JonesMigration0005,
  "6_WorkstreamsProviderEnrollments": JonesMigration0006,
  "7_V2NativeAcceptance": JonesMigration0007,
  "8_DeletionWorktreeAdmission": JonesMigration0008,
  "9_OrdinaryCheckoutOwnership": JonesMigration0009,
  "10_AttachmentCleanup": JonesMigration0010,
  "11_OrdinaryCheckoutExecutionLifetime": JonesMigration0011,
  "12_ImportedApplicationAttachments": JonesMigration0012,
});

const timestamp = "2026-10-04T00:00:00.000Z";
const commandId = "command:normalization-migration";
const threadId = "thread:normalization-migration";
const acceptedCommand = {
  type: "runtime-request.respond",
  commandId,
  threadId,
  requestId: "request:normalization-migration",
  answers: {
    question: ["Chosen answer", 'Attached file "note.txt": "/attachments/accepted-first.txt"'],
  },
};
const witness = Schema.decodeUnknownSync(Witness.NormalizationWitnessV1)({
  commandId,
  commandType: acceptedCommand.type,
  witnessVersion: 1,
  requestDigest: Witness.requestDigest({
    commandId,
    threadId,
    pendingIds: ["pending-first", "pending-second"],
  }),
  attachments: [
    {
      pendingId: "pending-first",
      contentSha256: "a".repeat(64),
      sizeBytes: 3,
      finalId: "accepted-first",
    },
    {
      pendingId: "pending-second",
      contentSha256: "b".repeat(64),
      sizeBytes: 8,
      finalId: "accepted-second",
    },
  ],
  contextRemaps: [
    { sourceId: "pending-second", finalId: "accepted-second" },
    { sourceId: "pending-first", finalId: "accepted-first" },
  ],
  acceptedCommand,
  acceptedCommandDigest: Witness.requestDigest(acceptedCommand),
  receiptSequence: 11,
  threadId,
  projectId: "project:normalization-migration",
  applicationBirth: {
    kind: "application_v2_thread_birth",
    threadId,
    eventId: "birth:normalization-migration",
    sequence: 7,
  },
  createdAt: timestamp,
});
const row = Witness.encodeNormalizationWitnessRow(witness);
// This storage fixture binds real SQL parent rows; live birth and digest qualification belongs to EventSink.
const insertParents = Effect.fnUntraced(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO orchestration_events
    (sequence, event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, command_id,
      actor_kind, payload_json, metadata_json, application_event_version)
    VALUES (${witness.applicationBirth.sequence}, ${witness.applicationBirth.eventId}, 'thread', ${threadId},
      1, 'thread.created', ${timestamp}, 'command:normalization-birth', 'server', '{}', '{}', 2)`;
  yield* sql`INSERT INTO orchestration_command_receipts
    (command_id, command_type, aggregate_kind, aggregate_id, accepted_at, result_sequence, status)
    VALUES (${commandId}, ${witness.commandType}, 'thread', ${threadId}, ${timestamp}, ${witness.receiptSequence}, 'accepted')`;
});

it.effect("Jones13 installs a fresh empty witness ledger without altering upstream history", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    assert.deepEqual(yield* runMigrations(), migrationManifest);
    assert.deepEqual(yield* runMigrations(), []);
    assert.deepEqual(
      yield* sql`SELECT migration_id, name FROM jones_sql_migrations WHERE migration_id = 13`,
      [{ migration_id: 13, name: "CommandNormalizationWitness" }],
    );
    assert.deepEqual(
      yield* sql`SELECT migration_id, name FROM effect_sql_migrations ORDER BY migration_id`,
      migrationManifest.map(([migration_id, name]) => ({ migration_id, name })),
    );
    assert.deepEqual(yield* sql`SELECT * FROM command_normalization_witnesses`, []);
  }).pipe(Effect.provide(memory)),
);

it.effect(
  "Jones13 upgrades Jones12 without backfilling witnesses or rewriting old receipt and admission identity",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 56 });
      yield* runForkMigrations({ loader: priorForkLoader, table: "jones_sql_migrations" });
      const upstream = yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`;
      const priorFork = yield* sql`SELECT * FROM jones_sql_migrations ORDER BY migration_id`;
      assert.equal(priorFork.at(-1)?.migration_id, 12);
      assert.deepEqual(
        yield* sql`SELECT name FROM sqlite_master WHERE name = 'command_normalization_witnesses'`,
        [],
      );
      yield* sql`INSERT INTO orchestration_command_receipts
      (command_id, command_type, aggregate_kind, aggregate_id, accepted_at, result_sequence, status)
      VALUES ('command:before-witness', 'message.dispatch', 'thread', 'thread:before-witness',
        '2026-10-03T00:00:00.000Z', 1, 'accepted')`;
      yield* sql`INSERT INTO orchestration_v2_ordinary_checkout_admissions
      (admission_id, command_id, thread_id, admission_sha256, admission_json, recorded_at)
      VALUES (${"a".repeat(64)}, 'command:before-witness', 'thread:before-witness', ${"b".repeat(64)},
        '{"version":1,"fixture":"existing immutable admission"}', '2026-10-03T00:00:00.000Z')`;
      const oldReceipts = yield* sql`SELECT * FROM orchestration_command_receipts`;
      const oldAdmissions = yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_admissions`;
      yield* runMigrations();
      assert.deepEqual(yield* runMigrations(), []);
      assert.deepEqual(
        yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`,
        upstream,
      );
      assert.deepEqual(
        yield* sql`SELECT * FROM jones_sql_migrations WHERE migration_id <= 12 ORDER BY migration_id`,
        priorFork,
      );
      assert.deepEqual(
        yield* sql`SELECT migration_id, name FROM jones_sql_migrations WHERE migration_id = 13`,
        [{ migration_id: 13, name: "CommandNormalizationWitness" }],
      );
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_command_receipts`, oldReceipts);
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_admissions`,
        oldAdmissions,
      );
      assert.deepEqual(yield* sql`SELECT * FROM command_normalization_witnesses`, []);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "normalization witnesses require a receipt and constrained identity then round trip ordered copies, remaps and full birth",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      assert.equal(
        (yield* Effect.result(sql`INSERT INTO command_normalization_witnesses ${sql.insert(row)}`))
          ._tag,
        "Failure",
      );
      yield* insertParents();
      for (const column of Object.keys(row)) {
        assert.equal(
          (yield* Effect.result(
            sql`INSERT INTO command_normalization_witnesses ${sql.insert({ ...row, [column]: null })}`,
          ))._tag,
          "Failure",
        );
      }
      for (const invalid of [
        { command_type: "legacy" },
        { witness_version: 2 },
        { witness_version: 1.5 },
        { request_digest: "A".repeat(64) },
        { request_digest: "a".repeat(63) },
        { request_digest: "g".repeat(64) },
        { attachments_json: "invalid" },
        { attachments_json: "{}" },
        { context_remaps_json: "invalid" },
        { context_remaps_json: "{}" },
        { accepted_command_json: "invalid" },
        { accepted_command_json: "[]" },
        { accepted_command_json: "{}" },
        {
          accepted_command_json: yield* encodeFixtureJson({
            ...acceptedCommand,
            commandId: "other",
          }).pipe(Effect.catch(dieNativeJsonCause)),
        },
        {
          accepted_command_json: yield* encodeFixtureJson({
            ...acceptedCommand,
            type: "message.dispatch",
          }).pipe(Effect.catch(dieNativeJsonCause)),
        },
        {
          accepted_command_json: yield* encodeFixtureJson({
            ...acceptedCommand,
            threadId: "other",
          }).pipe(Effect.catch(dieNativeJsonCause)),
        },
        { accepted_command_digest: "A".repeat(64) },
        { accepted_command_digest: "a".repeat(63) },
        { receipt_sequence: -1 },
        { receipt_sequence: 1.5 },
        { thread_id: "" },
        { project_id: "" },
        { application_birth_json: "invalid" },
        { application_birth_json: "[]" },
        { application_birth_json: "{}" },
        {
          application_birth_json: yield* encodeFixtureJson({
            ...witness.applicationBirth,
            kind: "legacy_birth",
          }).pipe(Effect.catch(dieNativeJsonCause)),
        },
        {
          application_birth_json: yield* encodeFixtureJson({
            ...witness.applicationBirth,
            threadId: "other",
          }).pipe(Effect.catch(dieNativeJsonCause)),
        },
        {
          application_birth_json: yield* encodeFixtureJson({
            ...witness.applicationBirth,
            eventId: "",
          }).pipe(Effect.catch(dieNativeJsonCause)),
        },
        {
          application_birth_json: yield* encodeFixtureJson({
            ...witness.applicationBirth,
            sequence: 0,
          }).pipe(Effect.catch(dieNativeJsonCause)),
        },
        {
          application_birth_json: yield* encodeFixtureJson({
            ...witness.applicationBirth,
            sequence: "7",
          }).pipe(Effect.catch(dieNativeJsonCause)),
        },
        { created_at: "" },
      ])
        assert.equal(
          (yield* Effect.result(
            sql`INSERT INTO command_normalization_witnesses ${sql.insert({ ...row, ...invalid })}`,
          ))._tag,
          "Failure",
        );
      yield* sql`INSERT INTO command_normalization_witnesses ${sql.insert(row)}`;
      const stored =
        yield* sql`SELECT * FROM command_normalization_witnesses WHERE command_id = ${commandId}`;
      assert.deepEqual(stored, [row]);
      assert.deepEqual(yield* Witness.decodeNormalizationWitnessRow(stored[0]), witness);
      assert.deepEqual(
        (yield* sql<{
          name: string;
          notnull: number;
        }>`PRAGMA table_info(command_normalization_witnesses)`).map((column) => [
          column.name,
          column.notnull,
        ]),
        Object.keys(row).map((column) => [column, 1]),
      );
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "every witness column is immutable and deletion or replacement cannot erase accepted identity",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* sql`PRAGMA recursive_triggers = OFF`;
      yield* insertParents();
      yield* sql`INSERT INTO command_normalization_witnesses ${sql.insert(row)}`;
      for (const [column, value] of Object.entries(row)) {
        const failure = yield* Effect.flip(
          sql`UPDATE command_normalization_witnesses SET ${sql.update({
            [column]: typeof value === "number" ? value + 1 : `${value}:changed`,
          })} WHERE command_id = ${commandId}`,
        );
        assert.include(
          String(failure.reason.cause),
          "command normalization witnesses are immutable",
        );
      }
      const deletion = yield* Effect.flip(
        sql`DELETE FROM command_normalization_witnesses WHERE command_id = ${commandId}`,
      );
      assert.include(
        String(deletion.reason.cause),
        "command normalization witnesses are permanent",
      );
      for (const insert of [
        sql`INSERT INTO command_normalization_witnesses ${sql.insert(row)}`,
        sql`INSERT OR REPLACE INTO command_normalization_witnesses ${sql.insert({ ...row, request_digest: "c".repeat(64) })}`,
      ]) {
        const replacement = yield* Effect.flip(insert);
        assert.include(
          String(replacement.reason.cause),
          "command normalization witness identity is permanent",
        );
      }
      assert.equal(
        (yield* Effect.result(
          sql`DELETE FROM orchestration_command_receipts WHERE command_id = ${commandId}`,
        ))._tag,
        "Failure",
      );
      assert.deepEqual(yield* sql`SELECT * FROM command_normalization_witnesses`, [row]);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "a failed acceptance transaction rolls back witness and receipt together without deleting immutable rows",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      const failure = yield* Effect.result(
        sql.withTransaction(
          Effect.gen(function* () {
            yield* insertParents();
            yield* sql`INSERT INTO command_normalization_witnesses ${sql.insert(row)}`;
            return yield* Effect.fail("synthetic acceptance failure");
          }),
        ),
      );
      assert.equal(failure._tag, "Failure");
      assert.deepEqual(yield* sql`SELECT * FROM command_normalization_witnesses`, []);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_command_receipts`, []);
      assert.deepEqual(
        yield* sql`SELECT event_id FROM orchestration_events WHERE event_id = ${witness.applicationBirth.eventId}`,
        [],
      );
    }).pipe(Effect.provide(memory)),
);

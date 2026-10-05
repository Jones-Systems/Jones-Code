import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  ImportedApplicationAttachmentBirthV1,
  ImportedApplicationAttachmentCarrierV1,
  ImportedApplicationAttachmentInventoryV1,
  ImportedApplicationAttachmentSourceV1,
  type ImportedApplicationAttachmentSnapshotV1,
  canonicalImportedApplicationAttachmentCarriersV1,
  collectImportedApplicationAttachmentPathsV1,
  importedApplicationAttachmentCanonicalJsonV1,
  importedApplicationAttachmentCarrierIdV1,
  importedApplicationAttachmentSha256V1,
  makeImportedApplicationAttachmentInventoryIdV1,
  qualifyImportedApplicationAttachmentSnapshotV1,
} from "../../orchestration-v2/ImportedApplicationAttachmentInventory.ts";
import { migrationManifest, runMigrations } from "../Migrations.ts";

class SyntheticAdoptionFailure extends Schema.TaggedError<SyntheticAdoptionFailure>()(
  "SyntheticAdoptionFailure",
  { cause: Schema.Defect() },
) {}

const memory = NodeSqliteClient.layer({ filename: ":memory:" });
const timestamp = "2026-10-03T00:00:00.000Z";
const canonical = importedApplicationAttachmentCanonicalJsonV1;
const birth = Schema.decodeUnknownSync(ImportedApplicationAttachmentBirthV1)({
  kind: "application_v2_thread_birth",
  threadId: "thread:inventory-fixture",
  eventId: "event:inventory-fixture:birth",
  sequence: 5,
});
const message = Schema.decodeUnknownSync(ImportedApplicationAttachmentCarrierV1)({
  kind: "legacy_message",
  messageId: "carrier:shared-fixture",
  sourceThreadId: birth.threadId,
  turnId: null,
  role: "system",
  createdAt: timestamp,
  updatedAt: timestamp,
  attachmentsJson: null,
  attachments: [],
  sourceRowSha256: importedApplicationAttachmentSha256V1({
    id: "carrier:shared-fixture",
    role: "system",
    attachments_json: null,
  }),
});
const payload = {
  requestId: "request:inventory-fixture",
  questionTextById: { first: "Keep this question" },
  answers: { first: "yes" },
  attachmentsByQuestionId: {
    first: [
      {
        type: "file",
        id: "application-file",
        name: "report.TXT",
        mimeType: "text/plain",
        sizeBytes: 10,
      },
    ],
  },
};
const answer = Schema.decodeUnknownSync(ImportedApplicationAttachmentCarrierV1)({
  kind: "legacy_answer",
  activityId: "carrier:shared-fixture",
  sourceThreadId: birth.threadId,
  turnId: null,
  sequence: null,
  createdAt: timestamp,
  payloadJson: JSON.stringify(payload),
  answer: payload,
  sourceRowSha256: importedApplicationAttachmentSha256V1({ id: "carrier:shared-fixture", payload }),
});
function legacySnapshot(
  carriers: ReadonlyArray<typeof message>,
  cut = 2,
): ImportedApplicationAttachmentSnapshotV1 {
  const paths = collectImportedApplicationAttachmentPathsV1(carriers);
  if (paths.status !== "complete") throw new Error(paths.reason);
  const source = Schema.decodeUnknownSync(ImportedApplicationAttachmentSourceV1)({
    kind: "legacy_projection",
    legacyBirth: { eventId: "event:legacy-fixture:birth", sequence: 1 },
    projectId: "project:inventory-fixture",
    sourceCreatedAt: timestamp,
    sourceCut: {
      legacyEventSequence: cut,
      projectorPositions: { threads: cut, messages: cut, activities: cut, turns: cut },
      messageRowsSha256: importedApplicationAttachmentSha256V1(
        canonicalImportedApplicationAttachmentCarriersV1(
          carriers.filter((row) => row.kind === "legacy_message"),
        ),
      ),
      answerRowsSha256: importedApplicationAttachmentSha256V1(
        canonicalImportedApplicationAttachmentCarriersV1(
          carriers.filter((row) => row.kind === "legacy_answer"),
        ),
      ),
    },
  });
  if (source.kind !== "legacy_projection")
    throw new Error("Expected actual legacy inventory fixture source");
  const identity: Omit<ImportedApplicationAttachmentInventoryV1, "inventoryId" | "recordedAt"> = {
    version: 1,
    domain: "jones_materialized_attachment_references/v1",
    applicationBirth: birth,
    projectId: source.projectId,
    source,
    sourceHistoryCoverage: "legacy_materialized_projection",
    completeness: "complete_application_refs",
    messageCarrierCount: paths.messageCarrierCount,
    answerCarrierCount: paths.answerCarrierCount,
    attachmentReferenceCount: paths.attachmentReferenceCount,
    carrierSetSha256: paths.carrierSetSha256,
  };
  return {
    header: {
      ...identity,
      inventoryId: makeImportedApplicationAttachmentInventoryIdV1(identity),
      recordedAt: timestamp,
    },
    carriers: canonicalImportedApplicationAttachmentCarriersV1(carriers),
  };
}
const snapshot = legacySnapshot([message, answer]);
const headerRow = (value = snapshot, ordinal = 0) => ({
  inventory_id: value.header.inventoryId,
  thread_id: value.header.applicationBirth.threadId,
  project_id: value.header.projectId,
  application_birth_event_id: value.header.applicationBirth.eventId,
  application_birth_sequence: value.header.applicationBirth.sequence,
  adoption_ordinal: ordinal,
  source_kind: value.header.source.kind,
  canonical_header_json: canonical(
    Schema.encodeSync(ImportedApplicationAttachmentInventoryV1)(value.header),
  ),
  canonical_source_json: canonical(
    Schema.encodeSync(ImportedApplicationAttachmentSourceV1)(value.header.source),
  ),
  message_carrier_count: value.header.messageCarrierCount,
  answer_carrier_count: value.header.answerCarrierCount,
  attachment_reference_count: value.header.attachmentReferenceCount,
  carrier_set_sha256: value.header.carrierSetSha256,
  recorded_at: value.header.recordedAt,
});
const carrierRows = (value = snapshot) =>
  value.carriers.map((carrier) => ({
    inventory_id: value.header.inventoryId,
    carrier_kind: carrier.kind,
    carrier_id: importedApplicationAttachmentCarrierIdV1(carrier),
    canonical_carrier_json: canonical(
      Schema.encodeSync(ImportedApplicationAttachmentCarrierV1)(carrier),
    ),
    source_row_sha256: carrier.sourceRowSha256,
  }));
const insertBirth = () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO orchestration_events
    (sequence, event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, command_id, actor_kind, payload_json, metadata_json, application_event_version)
    VALUES (${birth.sequence}, ${birth.eventId}, 'thread', ${birth.threadId}, 1, 'thread.created', ${timestamp},
      'command:inventory-fixture', 'server', '{}', '{}', 2)`;
  });
// Synthetic source rows exercise immutable storage; real source-cut qualification belongs to the SQL producer.
const insertSnapshot = (value = snapshot, ordinal = 0) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO orchestration_v2_imported_application_attachment_inventories ${sql.insert(headerRow(value, ordinal))}`;
    for (const carrier of carrierRows(value))
      yield* sql`INSERT INTO orchestration_v2_imported_application_attachment_carriers ${sql.insert(carrier)}`;
  });

it.effect(
  "Jones144 adds two empty inventory relations after upstream56 and preserves explicit upstream migration replay",
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
        yield* sql`SELECT name FROM sqlite_master WHERE name IN ('orchestration_v2_imported_application_attachment_inventories', 'orchestration_v2_imported_application_attachment_carriers')`,
        [],
      );
      yield* runMigrations();
      assert.deepEqual(yield* runMigrations(), []);
      assert.deepEqual(
        yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`,
        upstream,
      );
      assert.deepEqual(
        yield* sql`SELECT migration_id, name FROM jones_sql_migrations WHERE migration_id = 144`,
        [{ migration_id: 144, name: "ImportedApplicationAttachments" }],
      );
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_imported_application_attachment_inventories`,
        [],
      );
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_imported_application_attachment_carriers`,
        [],
      );
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "inventory headers require original application birth and constrained adoption, count, JSON and digest shape",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      const original = headerRow();
      assert.equal(
        (yield* Effect.result(
          sql`INSERT INTO orchestration_v2_imported_application_attachment_inventories ${sql.insert(original)}`,
        ))._tag,
        "Failure",
      );
      yield* insertBirth();
      for (const invalid of [
        { inventory_id: null },
        { inventory_id: "A".repeat(64) },
        { inventory_id: "g".repeat(64) },
        { inventory_id: "a".repeat(63) },
        { thread_id: "" },
        { project_id: "" },
        { application_birth_event_id: "missing" },
        { application_birth_sequence: 0 },
        { adoption_ordinal: -1 },
        { adoption_ordinal: null },
        { source_kind: "native_raw_history" },
        { canonical_header_json: null },
        { canonical_header_json: "invalid" },
        { canonical_header_json: "[]" },
        { canonical_header_json: "{}" },
        { canonical_header_json: '{"version":2}' },
        { canonical_header_json: '{"version":"1"}' },
        { canonical_source_json: null },
        { canonical_source_json: "invalid" },
        { canonical_source_json: "[]" },
        { message_carrier_count: -1 },
        { answer_carrier_count: -1 },
        { attachment_reference_count: -1 },
        { carrier_set_sha256: "A".repeat(64) },
        { carrier_set_sha256: "g".repeat(64) },
        { carrier_set_sha256: "a".repeat(63) },
        { recorded_at: null },
        { recorded_at: "" },
      ])
        assert.equal(
          (yield* Effect.result(
            sql`INSERT INTO orchestration_v2_imported_application_attachment_inventories ${sql.insert({ ...original, ...invalid })}`,
          ))._tag,
          "Failure",
        );
      yield* sql.withTransaction(insertSnapshot());
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_imported_application_attachment_inventories`,
        [original],
      );
      assert.equal(qualifyImportedApplicationAttachmentSnapshotV1(snapshot).status, "complete");
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "birth adoption cannot be duplicated or replaced while an explicit new source cut appends immutable history",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* sql`PRAGMA recursive_triggers = OFF`;
      yield* insertBirth();
      yield* sql.withTransaction(insertSnapshot());
      const original = headerRow();
      const revision = legacySnapshot([message, answer], 3);
      assert.notEqual(revision.header.inventoryId, snapshot.header.inventoryId);
      for (const mutation of [
        sql`INSERT INTO orchestration_v2_imported_application_attachment_inventories ${sql.insert(original)}`,
        sql`INSERT OR REPLACE INTO orchestration_v2_imported_application_attachment_inventories ${sql.insert({ ...original, recorded_at: "changed" })}`,
        sql`INSERT OR REPLACE INTO orchestration_v2_imported_application_attachment_inventories ${sql.insert(headerRow(revision, 0))}`,
      ])
        assert.equal((yield* Effect.result(mutation))._tag, "Failure");
      yield* sql.withTransaction(insertSnapshot(revision, 1));
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_imported_application_attachment_inventories ORDER BY adoption_ordinal`,
        [original, headerRow(revision, 1)],
      );
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_imported_application_attachment_carriers WHERE inventory_id = ${snapshot.header.inventoryId} ORDER BY carrier_kind`,
        carrierRows(),
      );
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "carrier identity includes kind and original row ID, requires inventory parent and preserves canonical raw and decoded references",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* insertBirth();
      const rows = carrierRows();
      assert.equal(rows.length, 2);
      assert.equal(rows[0]!.carrier_id, rows[1]!.carrier_id);
      assert.notEqual(rows[0]!.carrier_kind, rows[1]!.carrier_kind);
      assert.equal(
        (yield* Effect.result(
          sql`INSERT INTO orchestration_v2_imported_application_attachment_carriers ${sql.insert(rows[0]!)}`,
        ))._tag,
        "Failure",
      );
      yield* sql`INSERT INTO orchestration_v2_imported_application_attachment_inventories ${sql.insert(headerRow())}`;
      for (const invalid of [
        { inventory_id: null },
        { inventory_id: "d".repeat(64) },
        { carrier_kind: "provider_raw_message" },
        { carrier_id: null },
        { carrier_id: "" },
        { canonical_carrier_json: null },
        { canonical_carrier_json: "invalid" },
        { canonical_carrier_json: "[]" },
        { source_row_sha256: null },
        { source_row_sha256: "A".repeat(64) },
        { source_row_sha256: "g".repeat(64) },
        { source_row_sha256: "a".repeat(63) },
      ])
        assert.equal(
          (yield* Effect.result(
            sql`INSERT INTO orchestration_v2_imported_application_attachment_carriers ${sql.insert({ ...rows[0]!, ...invalid })}`,
          ))._tag,
          "Failure",
        );
      for (const row of rows)
        yield* sql`INSERT INTO orchestration_v2_imported_application_attachment_carriers ${sql.insert(row)}`;
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_imported_application_attachment_carriers ORDER BY carrier_kind`,
        rows,
      );
      assert.deepEqual(
        yield* sql`SELECT carrier_kind, COUNT(*) AS count FROM orchestration_v2_imported_application_attachment_carriers GROUP BY carrier_kind ORDER BY carrier_kind`,
        [
          { carrier_kind: "legacy_answer", count: 1 },
          { carrier_kind: "legacy_message", count: 1 },
        ],
      );
      assert.equal(snapshot.header.attachmentReferenceCount, 1);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "every header and carrier field is permanent and composite replacement fails with recursive triggers off",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* sql`PRAGMA recursive_triggers = OFF`;
      yield* insertBirth();
      yield* sql.withTransaction(insertSnapshot());
      const original = headerRow();
      const rows = carrierRows();
      for (const changed of [
        { inventory_id: "d".repeat(64) },
        { thread_id: "changed" },
        { project_id: "changed" },
        { application_birth_event_id: "changed" },
        { application_birth_sequence: 6 },
        { adoption_ordinal: 1 },
        { source_kind: "native_import_batch" },
        { canonical_header_json: '{"version":1,"fixture":"changed"}' },
        { canonical_source_json: "{}" },
        { message_carrier_count: 0 },
        { answer_carrier_count: 0 },
        { attachment_reference_count: 0 },
        { carrier_set_sha256: "d".repeat(64) },
        { recorded_at: "changed" },
      ])
        assert.equal(
          (yield* Effect.result(
            sql`UPDATE orchestration_v2_imported_application_attachment_inventories ${sql.update(changed)}`,
          ))._tag,
          "Failure",
        );
      for (const changed of [
        { inventory_id: "d".repeat(64) },
        { carrier_kind: "legacy_message" },
        { carrier_id: "changed" },
        { canonical_carrier_json: "{}" },
        { source_row_sha256: "d".repeat(64) },
      ])
        assert.equal(
          (yield* Effect.result(
            sql`UPDATE orchestration_v2_imported_application_attachment_carriers ${sql.update(changed)} WHERE carrier_kind = 'legacy_answer'`,
          ))._tag,
          "Failure",
        );
      for (const mutation of [
        sql`DELETE FROM orchestration_v2_imported_application_attachment_carriers`,
        sql`DELETE FROM orchestration_v2_imported_application_attachment_inventories`,
        sql`DELETE FROM orchestration_events WHERE event_id = ${birth.eventId}`,
        sql`INSERT OR REPLACE INTO orchestration_v2_imported_application_attachment_carriers ${sql.insert({ ...rows[0]!, canonical_carrier_json: "{}" })}`,
      ])
        assert.equal((yield* Effect.result(mutation))._tag, "Failure");
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_imported_application_attachment_inventories`,
        [original],
      );
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_imported_application_attachment_carriers ORDER BY carrier_kind`,
        rows,
      );
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "native application baseline retains actual seal message count with zero carriers and forbids answer or reference counts",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* insertBirth();
      const empty = legacySnapshot([]);
      const nativeSource = yield* Schema.decodeUnknownEffect(ImportedApplicationAttachmentSourceV1)(
        {
          kind: "native_import_batch",
          parserPolicy: "agent_session_visible_messages_v1",
          birth,
          source: {
            provider: "codex",
            providerInstanceId: "fixture-codex",
            providerSessionId: "session:inventory-fixture",
            filePath: "/synthetic/imported-history.jsonl",
            size: 100,
            mtimeMs: null,
            device: 1,
            inode: null,
            birthtimeMs: null,
          },
          eventsSha256: importedApplicationAttachmentSha256V1("sealed fixture events"),
          messageCount: 2,
          eventBasis: Array.from({ length: 4 }, (_, index) => ({
            eventId: `event:native-fixture:${index}`,
            sequence: 6 + index,
          })),
        },
      ).pipe(Effect.orDie);
      const { inventoryId: _id, recordedAt, ...prior } = empty.header;
      const identity = {
        ...prior,
        source: nativeSource,
        sourceHistoryCoverage: "native_visible_message_subset" as const,
        messageCarrierCount: 2,
      };
      const native: ImportedApplicationAttachmentSnapshotV1 = {
        header: {
          ...identity,
          inventoryId: makeImportedApplicationAttachmentInventoryIdV1(identity),
          recordedAt,
        },
        carriers: [],
      };
      const original = headerRow(native);
      for (const invalid of [{ answer_carrier_count: 1 }, { attachment_reference_count: 1 }])
        assert.equal(
          (yield* Effect.result(
            sql`INSERT INTO orchestration_v2_imported_application_attachment_inventories ${sql.insert({ ...original, ...invalid })}`,
          ))._tag,
          "Failure",
        );
      yield* sql.withTransaction(insertSnapshot(native));
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_imported_application_attachment_inventories`,
        [original],
      );
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_imported_application_attachment_carriers`,
        [],
      );
      assert.equal(native.header.messageCarrierCount, 2);
      assert.equal(qualifyImportedApplicationAttachmentSnapshotV1(native).status, "complete");
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "failed complete adoption rolls back header and partial carrier set without leaving false complete evidence",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* insertBirth();
      assert.equal(
        qualifyImportedApplicationAttachmentSnapshotV1({ ...snapshot, carriers: [message] }).status,
        "unavailable",
      );
      const injectedCause = new Error("synthetic complete adoption failure");
      const result = yield* Effect.result(
        sql.withTransaction(
          Effect.gen(function* () {
            yield* sql`INSERT INTO orchestration_v2_imported_application_attachment_inventories ${sql.insert(headerRow())}`;
            yield* sql`INSERT INTO orchestration_v2_imported_application_attachment_carriers ${sql.insert(carrierRows()[0]!)}`;
            return yield* Effect.fail(new SyntheticAdoptionFailure({ cause: injectedCause }));
          }),
        ),
      );
      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") {
        assert.equal(result.failure._tag, "SyntheticAdoptionFailure");
        if (result.failure._tag === "SyntheticAdoptionFailure") {
          assert.strictEqual(result.failure.cause, injectedCause);
        }
      }
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_imported_application_attachment_inventories`,
        [],
      );
      assert.deepEqual(
        yield* sql`SELECT * FROM orchestration_v2_imported_application_attachment_carriers`,
        [],
      );
    }).pipe(Effect.provide(memory)),
);

import { assert, describe, it } from "@effect/vitest";
import { EventId, ProviderDriverKind, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Tracer from "effect/Tracer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import { listLinkedPullRequestThreads } from "../../pullRequest/linkedThreads.ts";
import * as EventSink from "../EventSink.ts";
import * as EventStore from "../EventStore.ts";
import * as LegacyV1ThreadImporter from "./LegacyV1ThreadImporter.ts";
import * as ProjectionMaintenance from "../ProjectionMaintenance.ts";
import * as ProjectionStore from "../ProjectionStore.ts";
import {
  collectImportedApplicationAttachmentPathsV1,
  type ImportedApplicationAttachmentBirthV1,
} from "../ImportedApplicationAttachmentInventory.ts";
import * as ProviderSessionRuntime from "../../persistence/ProviderSessionRuntime.ts";

const databaseLayer = SqlitePersistenceMemory;
const eventStoreProvided = EventStore.layer.pipe(Layer.provideMerge(databaseLayer));
const projectionStoreProvided = ProjectionStore.layer.pipe(Layer.provideMerge(databaseLayer));
const storesProvided = Layer.mergeAll(databaseLayer, eventStoreProvided, projectionStoreProvided);
const eventSinkProvided = EventSink.layer.pipe(Layer.provide(storesProvided));
const importerProvided = LegacyV1ThreadImporter.layer.pipe(
  Layer.provide(Layer.mergeAll(storesProvided, eventSinkProvided)),
);
const projectionMaintenanceProvided = ProjectionMaintenance.layer.pipe(
  Layer.provide(storesProvided),
);
const TestLayer = Layer.mergeAll(
  storesProvided,
  eventSinkProvided,
  importerProvided,
  projectionMaintenanceProvided,
);

// Same bytes as JSON.stringify; a failure stays a defect as the native throw was.
const encodeJson = (value: unknown) =>
  Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown))(value).pipe(Effect.orDie);
// Only the asserted fields are read; the assertions below stay authoritative.
const decodeQualification = Schema.decodeEffect(
  Schema.fromJsonString(Schema.Struct({ type: Schema.Unknown })),
);
const decodeContinuationEvidence = Schema.decodeEffect(
  Schema.fromJsonString(
    Schema.Struct({
      providerInstanceId: Schema.Unknown,
      stoppedProof: Schema.Struct({ providerInstanceId: Schema.Unknown }),
      historicalSourceIdentity: Schema.Struct({ sourceHomeIdentity: Schema.Unknown }),
      continuationKey: Schema.optional(Schema.Unknown),
    }),
  ),
);

const seedHistoricalShell = Effect.fnUntraced(function* (threadId: ThreadId) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    INSERT INTO projection_projects (
      project_id, title, workspace_root, scripts_json, created_at, updated_at
    ) VALUES ('project:legacy-evidence', 'Historical', '/fixture/historical-project', '[]',
      '2026-01-01T00:00:00.000Z', '2026-01-01T00:00:00.000Z')
    ON CONFLICT(project_id) DO NOTHING
  `;
  yield* sql`
    INSERT INTO projection_threads (
      thread_id, project_id, title, model_selection_json, runtime_mode,
      interaction_mode, created_at, updated_at
    ) VALUES (${threadId}, 'project:legacy-evidence', 'Historical shell',
      '{"instanceId":"codex","model":"gpt-5.4"}', 'full-access', 'default',
      '2026-01-01T00:00:00.000Z', '2026-01-02T00:00:00.000Z')
  `;
});

const seedSnapshotTranscript = Effect.fnUntraced(function* (threadId: ThreadId) {
  yield* seedHistoricalShell(threadId);
  const sql = yield* SqlClient.SqlClient;
  for (const ordinal of [1, 2, 3, 4]) {
    yield* sql`INSERT INTO projection_thread_messages
      (message_id, thread_id, role, text, attachments_json, context_json, is_streaming, created_at, updated_at)
      VALUES (${`${threadId}:message:${ordinal}`}, ${threadId}, ${ordinal % 2 === 1 ? "user" : "assistant"},
        ${`Snapshot message ${ordinal}`}, '[]', NULL, ${ordinal === 4 ? 1 : 0},
        ${`2026-01-01T0${ordinal}:00:00.000Z`}, ${`2026-01-01T0${ordinal}:00:00.000Z`})`;
  }
});

const seedApplicationAttachmentSource = Effect.fnUntraced(function* (
  threadId: ThreadId,
  mode: "all" | "answers" | "empty" = "all",
) {
  yield* seedHistoricalShell(threadId);
  const sql = yield* SqlClient.SqlClient;
  const createdAt = "2026-01-01T00:00:00.000Z";
  const payload = {
    threadId,
    projectId: "project:legacy-evidence",
    title: "Historical shell",
    modelSelection: { instanceId: "codex", model: "gpt-5.4" },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    createdAt,
    updatedAt: "2026-01-02T00:00:00.000Z",
  };
  const payloadJson = yield* encodeJson(payload);
  const births = yield* sql<{ readonly sequence: number }>`INSERT INTO orchestration_events
    (event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, actor_kind, payload_json, metadata_json, application_event_version)
    VALUES (${`${threadId}:original-birth`}, 'thread', ${threadId}, 1, 'thread.created', ${createdAt}, 'user', ${payloadJson}, '{}', 1)
    RETURNING sequence`;
  for (const projector of [
    "projection.threads",
    "projection.thread-messages",
    "projection.thread-activities",
    "projection.thread-turns",
  ]) {
    yield* sql`INSERT INTO projection_state (projector, last_applied_sequence, updated_at)
      VALUES (${projector}, ${births[0]!.sequence}, ${createdAt}) ON CONFLICT(projector) DO UPDATE
      SET last_applied_sequence = excluded.last_applied_sequence, updated_at = excluded.updated_at`;
  }
  const file = {
    type: "file",
    id: "retained-historical-file",
    name: "notes.TXT",
    mimeType: "text/plain",
    sizeBytes: 10,
  };
  if (mode === "all") {
    for (const role of ["user", "assistant", "system"]) {
      const attachmentsJson = role === "system" ? yield* encodeJson([file]) : null;
      yield* sql`INSERT INTO projection_thread_messages
        (message_id, thread_id, turn_id, role, text, attachments_json, is_streaming, created_at, updated_at)
        VALUES (${`${threadId}:message:${role}`}, ${threadId}, NULL, ${role}, ${`Historical ${role}`},
          ${attachmentsJson}, 0, ${createdAt}, ${createdAt})`;
    }
  }
  if (mode !== "empty") {
    for (const id of ["first", "second", "zero"]) {
      const answer = {
        requestId: "same-request",
        questionTextById: { first: "Preserved question" },
        answers: { first: "yes" },
        attachmentsByQuestionId: { first: id === "zero" ? [] : [file] },
      };
      const answerJson = yield* encodeJson(answer);
      yield* sql`INSERT INTO projection_thread_activities
        (activity_id, thread_id, turn_id, sequence, tone, kind, summary, payload_json, created_at)
        VALUES (${`${threadId}:answer:${id}`}, ${threadId}, NULL, NULL, 'info', 'user-input.answer-submitted',
          'Historical answer', ${answerJson}, ${createdAt})`;
    }
  }
});

const importedApplicationBirth = Effect.fnUntraced(function* (threadId: ThreadId) {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{
    readonly event_id: string;
    readonly sequence: number;
  }>`SELECT event_id, sequence
    FROM orchestration_events WHERE application_event_version = 2 AND aggregate_kind = 'thread'
      AND stream_id = ${threadId} AND event_type = 'thread.created' ORDER BY sequence DESC LIMIT 1`;
  assert.lengthOf(rows, 1);
  return {
    kind: "application_v2_thread_birth",
    threadId,
    eventId: EventId.make(rows[0]!.event_id),
    sequence: rows[0]!.sequence,
  } satisfies ImportedApplicationAttachmentBirthV1;
});

// Deliberate corrupt-row cases need separate stores: they must not poison a
// later full event-store rebuild or leave unrelated incomplete import markers.
const testEffect = <A, E>(
  name: string,
  body: () => Effect.Effect<A, E, Layer.Success<typeof TestLayer>>,
) => it.effect(name, () => body().pipe(Effect.provide(TestLayer)));

describe("LegacyV1ThreadImporter", () => {
  testEffect(
    "adopts complete application references across all roles and duplicate-request answer rows",
    () =>
      Effect.gen(function* () {
        const threadId = ThreadId.make("thread:application-inventory-all-roles");
        yield* seedApplicationAttachmentSource(threadId);
        const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
        yield* importer.reconcileShells;
        const expectedBirth = yield* importedApplicationBirth(threadId);
        const sink = yield* EventSink.EventSinkV2;
        const first = yield* sink.readImportedApplicationAttachmentInventory({
          threadId,
          expectedBirth,
        });
        assert.equal(first.status, "complete");
        if (first.status !== "complete") return;
        assert.equal(first.inventory.header.messageCarrierCount, 3);
        assert.equal(first.inventory.header.answerCarrierCount, 3);
        assert.equal(first.inventory.header.attachmentReferenceCount, 3);
        assert.deepEqual(
          first.inventory.carriers
            .filter((row) => row.kind === "legacy_message")
            .map((row) => row.role),
          ["assistant", "system", "user"],
        );
        assert.deepEqual(
          collectImportedApplicationAttachmentPathsV1(first.inventory.carriers).status,
          "complete",
        );
        const answers = first.inventory.carriers.filter((row) => row.kind === "legacy_answer");
        assert.equal(new Set(answers.map((row) => row.answer.requestId)).size, 1);
        assert.deepEqual(answers[0]?.answer.questionTextById, { first: "Preserved question" });
        yield* importer.ensureTranscript(threadId);
        assert.deepEqual(
          yield* importer.ensureApplicationAttachmentInventory({ threadId, expectedBirth }),
          first,
        );
        const sql = yield* SqlClient.SqlClient;
        assert.equal(
          (yield* sql<{
            readonly count: number;
          }>`SELECT COUNT(*) AS count FROM orchestration_v2_imported_application_attachment_inventories WHERE thread_id = ${threadId}`)[0]
            ?.count,
          1,
        );
        assert.equal(
          (yield* sql<{
            readonly count: number;
          }>`SELECT COUNT(*) AS count FROM orchestration_v2_projection_runs WHERE thread_id = ${threadId}`)[0]
            ?.count,
          0,
        );
      }),
  );
  for (const mode of ["answers", "empty"] as const) {
    testEffect(
      `backfills ${mode} application references independently of a committed transcript marker`,
      () =>
        Effect.gen(function* () {
          const threadId = ThreadId.make(`thread:application-inventory-${mode}`);
          yield* seedApplicationAttachmentSource(threadId, mode);
          const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
          yield* importer.reconcileShells;
          yield* importer.ensureTranscript(threadId);
          const expectedBirth = yield* importedApplicationBirth(threadId);
          const result = yield* importer.ensureApplicationAttachmentInventory({
            threadId,
            expectedBirth,
          });
          assert.equal(result.status, "complete");
          if (result.status !== "complete") return;
          assert.equal(result.inventory.header.messageCarrierCount, 0);
          assert.equal(result.inventory.header.answerCarrierCount, mode === "answers" ? 3 : 0);
          assert.equal(
            result.inventory.header.attachmentReferenceCount,
            mode === "answers" ? 2 : 0,
          );
          yield* importer.importPendingTranscripts;
          assert.deepEqual(
            yield* importer.ensureApplicationAttachmentInventory({ threadId, expectedBirth }),
            result,
          );
        }),
    );
  }
  testEffect(
    "rolls back application inventory adoption with its outer shell import and retries once",
    () =>
      Effect.gen(function* () {
        const threadId = ThreadId.make("thread:application-inventory-outer-rollback");
        yield* seedApplicationAttachmentSource(threadId, "answers");
        const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
        const sink = yield* EventSink.EventSinkV2;
        const sql = yield* SqlClient.SqlClient;
        const failed = yield* sink
          .withTransaction(
            importer.reconcileShells.pipe(
              Effect.andThen(Effect.fail("after adoption before outer commit")),
            ),
          )
          .pipe(Effect.result);
        assert.equal(failed._tag, "Failure");
        assert.deepEqual(
          yield* sql`SELECT inventory_id FROM orchestration_v2_imported_application_attachment_inventories WHERE thread_id = ${threadId}`,
          [],
        );
        assert.deepEqual(
          yield* sql`SELECT thread_id FROM orchestration_v2_legacy_imports WHERE thread_id = ${threadId}`,
          [],
        );
        yield* importer.reconcileShells;
        const expectedBirth = yield* importedApplicationBirth(threadId);
        const recovered = yield* importer.ensureApplicationAttachmentInventory({
          threadId,
          expectedBirth,
        });
        assert.equal(recovered.status, "complete");
        assert.equal(
          (yield* sql<{
            readonly count: number;
          }>`SELECT COUNT(*) AS count FROM orchestration_v2_imported_application_attachment_inventories WHERE thread_id = ${threadId}`)[0]
            ?.count,
          1,
        );
      }),
  );
  testEffect(
    "adopts changed answer carriers after transcript confirmation without reimporting history",
    () =>
      Effect.gen(function* () {
        const threadId = ThreadId.make("thread:application-inventory-cached-backfill");
        yield* seedApplicationAttachmentSource(threadId);
        const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
        yield* importer.reconcileShells;
        yield* importer.ensureTranscript(threadId);
        const expectedBirth = yield* importedApplicationBirth(threadId);
        const original = yield* importer.ensureApplicationAttachmentInventory({
          threadId,
          expectedBirth,
        });
        assert.equal(original.status, "complete");
        if (original.status !== "complete") return;
        const sql = yield* SqlClient.SqlClient;
        const file = {
          type: "file",
          id: "retained-new-answer",
          name: "new.txt",
          mimeType: "text/plain",
          sizeBytes: 10,
        };
        const answer = {
          requestId: "same-request",
          answers: {},
          attachmentsByQuestionId: { first: [file] },
        };
        const answerJson = yield* encodeJson(answer);
        yield* sql`INSERT INTO projection_thread_activities (activity_id, thread_id, turn_id, sequence, tone, kind, summary, payload_json, created_at)
        VALUES (${`${threadId}:answer:new`}, ${threadId}, NULL, NULL, 'info', 'user-input.answer-submitted', 'New answer',
          ${answerJson}, '2026-01-03T00:00:00.000Z')`;
        assert.deepEqual(yield* importer.ensureTranscript(threadId), {
          importedThreadCount: 0,
          importedMessageCount: 0,
        });
        yield* importer.importPendingTranscripts;
        const updated = yield* importer.ensureApplicationAttachmentInventory({
          threadId,
          expectedBirth,
        });
        assert.equal(updated.status, "complete");
        if (updated.status !== "complete") return;
        assert.notEqual(
          updated.inventory.header.inventoryId,
          original.inventory.header.inventoryId,
        );
        assert.equal(updated.inventory.header.answerCarrierCount, 4);
        assert.equal(updated.inventory.header.attachmentReferenceCount, 4);
        const sink = yield* EventSink.EventSinkV2;
        assert.deepEqual(
          yield* sink.readImportedApplicationAttachmentInventory({
            threadId,
            expectedBirth,
            inventoryId: original.inventory.header.inventoryId,
          }),
          original,
        );
        assert.equal(
          (yield* sql<{
            readonly count: number;
          }>`SELECT COUNT(*) AS count FROM orchestration_v2_imported_application_attachment_inventories WHERE thread_id = ${threadId}`)[0]
            ?.count,
          2,
        );
      }),
  );
  testEffect(
    "adopts only currently retained materialized turn rows and preserves null-turn carriers",
    () =>
      Effect.gen(function* () {
        const threadId = ThreadId.make("thread:application-inventory-retained-cut");
        yield* seedApplicationAttachmentSource(threadId);
        const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
        yield* importer.reconcileShells;
        const expectedBirth = yield* importedApplicationBirth(threadId);
        const original = yield* importer.ensureApplicationAttachmentInventory({
          threadId,
          expectedBirth,
        });
        assert.equal(original.status, "complete");
        if (original.status !== "complete") return;
        const sql = yield* SqlClient.SqlClient;
        yield* sql`UPDATE projection_thread_messages SET turn_id = 'turn:removed' WHERE message_id = ${`${threadId}:message:user`}`;
        yield* sql`UPDATE projection_thread_activities SET turn_id = 'turn:removed' WHERE activity_id = ${`${threadId}:answer:first`}`;
        // This is the committed materialized result of the parent retained-turn cut,
        // not a replay of historical activities or a mapping into application RunIds.
        yield* sql`DELETE FROM projection_thread_messages WHERE thread_id = ${threadId} AND turn_id = 'turn:removed'`;
        yield* sql`DELETE FROM projection_thread_activities WHERE thread_id = ${threadId} AND turn_id = 'turn:removed'`;
        const retained = yield* importer.ensureApplicationAttachmentInventory({
          threadId,
          expectedBirth,
        });
        assert.equal(retained.status, "complete");
        if (retained.status !== "complete") return;
        assert.notEqual(
          retained.inventory.header.inventoryId,
          original.inventory.header.inventoryId,
        );
        assert.equal(retained.inventory.header.messageCarrierCount, 2);
        assert.equal(retained.inventory.header.answerCarrierCount, 2);
        assert.equal(retained.inventory.header.attachmentReferenceCount, 2);
        assert.isTrue(retained.inventory.carriers.every((carrier) => carrier.turnId === null));
        assert.isFalse(
          retained.inventory.carriers.some(
            (carrier) =>
              carrier.kind === "legacy_message" && carrier.messageId === `${threadId}:message:user`,
          ),
        );
        assert.isFalse(
          retained.inventory.carriers.some(
            (carrier) =>
              carrier.kind === "legacy_answer" && carrier.activityId === `${threadId}:answer:first`,
          ),
        );
        const paths = collectImportedApplicationAttachmentPathsV1(retained.inventory.carriers);
        assert.equal(paths.status, "complete");
        if (paths.status === "complete")
          assert.deepEqual(paths.relativePaths, ["retained-historical-file.txt"]);
      }),
  );
  testEffect(
    "holds a partial legacy source cut without manufacturing a complete application zero",
    () =>
      Effect.gen(function* () {
        const threadId = ThreadId.make("thread:application-inventory-partial-cut");
        yield* seedApplicationAttachmentSource(threadId, "answers");
        const sql = yield* SqlClient.SqlClient;
        yield* sql`UPDATE projection_state SET last_applied_sequence = 0 WHERE projector = 'projection.thread-activities'`;
        const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
        yield* importer.reconcileShells;
        const expectedBirth = yield* importedApplicationBirth(threadId);
        assert.deepEqual(
          yield* importer.ensureApplicationAttachmentInventory({ threadId, expectedBirth }),
          {
            status: "unavailable",
            reason: "legacy_application_source_cut_incomplete",
          },
        );
        assert.deepEqual(
          yield* sql`SELECT inventory_id FROM orchestration_v2_imported_application_attachment_inventories WHERE thread_id = ${threadId}`,
          [],
        );
      }),
  );
  testEffect("holds malformed answer carriers and missing or copied application birth proof", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("thread:application-inventory-malformed");
      yield* seedApplicationAttachmentSource(threadId, "answers");
      const sql = yield* SqlClient.SqlClient;
      yield* sql`UPDATE projection_thread_activities SET payload_json = '{' WHERE activity_id = ${`${threadId}:answer:first`}`;
      const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
      yield* importer.reconcileShells;
      const expectedBirth = yield* importedApplicationBirth(threadId);
      assert.deepEqual(
        yield* importer.ensureApplicationAttachmentInventory({ threadId, expectedBirth }),
        { status: "unavailable", reason: "carrier_decode_unavailable" },
      );
      assert.deepEqual(
        yield* importer.ensureApplicationAttachmentInventory({
          threadId,
          expectedBirth: { ...expectedBirth, eventId: EventId.make("event:copied-birth") },
        }),
        { status: "unavailable", reason: "imported_application_birth_unavailable" },
      );
      assert.deepEqual(
        yield* sql`SELECT inventory_id FROM orchestration_v2_imported_application_attachment_inventories WHERE thread_id = ${threadId}`,
        [],
      );
    }),
  );

  testEffect(
    "reads complete legacy source and normalized event parity without hydrating or writing",
    () =>
      Effect.gen(function* () {
        const threadId = ThreadId.make("thread:legacy-snapshot-positive");
        yield* seedSnapshotTranscript(threadId);
        const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
        assert.isNull(yield* importer.readTranscriptSnapshotEvidence(threadId));
        yield* importer.reconcileShells;
        const sink = yield* EventSink.EventSinkV2;
        const shellSequence = yield* sink.latestSequence({ threadId });
        assert.isNull(yield* importer.readTranscriptSnapshotEvidence(threadId));
        assert.equal(yield* sink.latestSequence({ threadId }), shellSequence);
        yield* importer.ensureTranscript(threadId);
        const sequence = yield* sink.latestSequence({ threadId });
        const proof = yield* importer.readTranscriptSnapshotEvidence(threadId);
        assert.isNotNull(proof);
        assert.equal(proof?.policy, "legacy_user_assistant_rows_v1");
        assert.equal(proof?.sourceUpdatedAt, "2026-01-02T00:00:00.000Z");
        assert.equal(proof?.messageCount, 4);
        assert.lengthOf(proof!.eventBasis, 8);
        assert.match(proof!.sourceRowsSha256, /^[0-9a-f]{64}$/);
        assert.match(proof!.eventsSha256, /^[0-9a-f]{64}$/);
        assert.equal(proof?.eventBasis[0]?.eventId, `migration:v1:message:${threadId}:message:3`);
        assert.isTrue(
          proof!.eventBasis.every(
            (entry, index, basis) => index === 0 || entry.sequence > basis[index - 1]!.sequence,
          ),
        );
        assert.deepEqual(yield* importer.readTranscriptSnapshotEvidence(threadId), proof);
        assert.equal(yield* sink.latestSequence({ threadId }), sequence);
        const sql = yield* SqlClient.SqlClient;
        const before =
          yield* sql`SELECT * FROM orchestration_v2_legacy_imports WHERE thread_id = ${threadId}`;
        yield* sql.withTransaction(importer.readTranscriptSnapshotEvidence(threadId));
        assert.deepEqual(
          yield* sql`SELECT * FROM orchestration_v2_legacy_imports WHERE thread_id = ${threadId}`,
          before,
        );
      }),
  );

  for (const mismatch of [
    "missing-marker",
    "partial",
    "last-error",
    "count",
    "source-timestamp",
    "source-text",
    "source-order",
    "missing-source-message",
    "event-text",
    "event-version",
    "event-type",
    "malformed-payload",
    "malformed-source",
    "position",
    "extra-position",
    "extra-event",
  ] as const) {
    testEffect(
      `returns unknown for legacy snapshot ${mismatch} without importing missing history`,
      () =>
        Effect.gen(function* () {
          const threadId = ThreadId.make(`thread:legacy-snapshot:${mismatch}`);
          yield* seedSnapshotTranscript(threadId);
          const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
          yield* importer.reconcileShells;
          yield* importer.ensureTranscript(threadId);
          assert.isNotNull(yield* importer.readTranscriptSnapshotEvidence(threadId));
          const sql = yield* SqlClient.SqlClient;
          const messageId = `${threadId}:message:1`;
          const eventId = `migration:v1:message:${messageId}`;
          if (mismatch === "missing-marker") {
            yield* sql`DELETE FROM orchestration_v2_legacy_imports WHERE thread_id = ${threadId}`;
          } else if (mismatch === "partial") {
            yield* sql`UPDATE orchestration_v2_legacy_imports SET transcript_imported_at = NULL WHERE thread_id = ${threadId}`;
          } else if (mismatch === "last-error") {
            yield* sql`UPDATE orchestration_v2_legacy_imports SET last_error = 'Incomplete import' WHERE thread_id = ${threadId}`;
          } else if (mismatch === "count") {
            yield* sql`UPDATE orchestration_v2_legacy_imports SET imported_message_count = 3 WHERE thread_id = ${threadId}`;
          } else if (mismatch === "source-timestamp") {
            yield* sql`UPDATE projection_threads SET updated_at = '2026-01-03T00:00:00.000Z' WHERE thread_id = ${threadId}`;
          } else if (mismatch === "source-text") {
            yield* sql`UPDATE projection_thread_messages SET text = 'Changed without changing the count' WHERE message_id = ${messageId}`;
          } else if (mismatch === "source-order") {
            yield* sql`UPDATE projection_thread_messages SET created_at = '2026-01-01T05:00:00.000Z' WHERE message_id = ${messageId}`;
          } else if (mismatch === "missing-source-message") {
            yield* sql`DELETE FROM projection_thread_messages WHERE message_id = ${messageId}`;
          } else if (mismatch === "event-text") {
            yield* sql`UPDATE orchestration_events SET payload_json = json_set(payload_json, '$.text', 'Changed pair')
            WHERE event_id IN (${eventId}, ${`migration:v1:turn-item:${messageId}`})`;
          } else if (mismatch === "event-version") {
            yield* sql`UPDATE orchestration_events SET application_event_version = 1 WHERE event_id = ${eventId}`;
          } else if (mismatch === "event-type") {
            yield* sql`UPDATE orchestration_events SET event_type = 'turn-item.updated' WHERE event_id = ${eventId}`;
          } else if (mismatch === "malformed-payload") {
            yield* sql`UPDATE orchestration_events SET payload_json = '{}' WHERE event_id = ${eventId}`;
          } else if (mismatch === "malformed-source") {
            yield* sql`UPDATE projection_thread_messages SET attachments_json = '{}' WHERE message_id = ${messageId}`;
          } else if (mismatch === "position") {
            yield* sql`UPDATE orchestration_v2_turn_item_positions SET ordinal = 10 WHERE turn_item_id = ${`migration:v1:turn-item:${messageId}`}`;
          } else if (mismatch === "extra-position") {
            yield* sql`INSERT INTO orchestration_v2_turn_item_positions (thread_id, turn_item_id, ordinal)
            VALUES (${threadId}, 'migration:v1:turn-item:unexpected', 10)`;
          } else {
            const projections = yield* ProjectionStore.ProjectionStoreV2;
            const message = (yield* projections.getThreadProjection(threadId)).messages[0]!;
            const sink = yield* EventSink.EventSinkV2;
            yield* sink.write({
              events: [
                {
                  id: EventId.make(`migration:v1:message:unexpected:${threadId}`),
                  type: "message.updated",
                  threadId,
                  occurredAt: message.updatedAt,
                  payload: message,
                },
              ],
            });
          }
          const sink = yield* EventSink.EventSinkV2;
          const sequence = yield* sink.latestSequence({ threadId });
          const before =
            yield* sql`SELECT * FROM orchestration_v2_legacy_imports WHERE thread_id = ${threadId}`;
          assert.isNull(yield* importer.readTranscriptSnapshotEvidence(threadId));
          assert.equal(yield* sink.latestSequence({ threadId }), sequence);
          assert.deepEqual(
            yield* sql`SELECT * FROM orchestration_v2_legacy_imports WHERE thread_id = ${threadId}`,
            before,
          );
        }),
    );
  }

  for (const fixture of [
    "historical_store",
    "native_read",
    "native_read_no_key",
    "no-evidence",
    "no-store",
    "no-home",
    "no-target-key",
    "wrong-instance",
    "running",
    "error",
    "malformed-cursor",
    "wrong-native-ref",
    "ambiguous-adapter",
    "synthetic-stopped",
    "unsupported-driver",
    "unsupported-store",
    "unsupported-resume",
  ] as const) {
    testEffect(`preserves historical shell and records ${fixture} continuation disposition`, () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const threadId = ThreadId.make(`thread:legacy-evidence:${fixture}`);
        const driver = ProviderDriverKind.make("codex");
        const instanceId = ProviderInstanceId.make("codex");
        const nativeThreadId = `native-history:${fixture}`;
        const key = "codex:home:/fixture/historical-home";
        yield* seedHistoricalShell(threadId);
        const sourceRow: ProviderSessionRuntime.ProviderSessionRuntime = {
          threadId,
          providerName: "codex",
          providerInstanceId:
            fixture === "wrong-instance"
              ? ProviderInstanceId.make("different-historical-instance")
              : null,
          adapterKey: fixture === "ambiguous-adapter" ? "claudeAgent" : "codex",
          runtimeMode: "full-access",
          status: fixture === "running" || fixture === "error" ? fixture : "stopped",
          lastSeenAt: "2026-01-02T00:00:00.000Z",
          resumeCursor:
            fixture === "malformed-cursor"
              ? { wrongKey: nativeThreadId }
              : { threadId: nativeThreadId },
          runtimePayload:
            fixture === "synthetic-stopped"
              ? { importOrigin: "native_import" }
              : { cwd: "/fixture/historical-project" },
        };
        yield* Effect.gen(function* () {
          const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
          yield* runtimes.upsert(sourceRow);
        }).pipe(Effect.provide(ProviderSessionRuntime.layer));
        const targetKey =
          fixture === "no-target-key"
            ? ""
            : fixture === "unsupported-store"
              ? "another-store"
              : key;
        const supplied: ProviderSessionRuntime.LegacyProviderContinuationInputV1 = {
          driver,
          nativeThreadId:
            fixture === "wrong-native-ref" ? "another-native-history" : nativeThreadId,
          continuationKey: fixture === "native_read_no_key" ? null : key,
          historicalSourceIdentity:
            fixture === "no-store"
              ? null
              : fixture === "no-home"
                ? { storeIdentity: "historical-source-store", sourceHomeIdentity: null }
                : {
                    storeIdentity: "historical-source-store",
                    sourceHomeIdentity: "/fixture/historical-home",
                  },
          accessibility: {
            providerInstanceId: instanceId,
            driver,
            nativeThreadId,
            continuationKey: key,
            source:
              fixture === "native_read" || fixture === "native_read_no_key"
                ? "native_read"
                : "historical_store",
          },
          target: {
            providerInstanceId: instanceId,
            driver:
              fixture === "unsupported-driver" ? ProviderDriverKind.make("claudeAgent") : driver,
            continuationKey: targetKey,
            supportsNativeResume: fixture !== "unsupported-resume",
          },
        };
        const inputs = new Map<ThreadId, ProviderSessionRuntime.LegacyProviderContinuationInputV1>(
          fixture === "no-evidence" ? [] : [[threadId, supplied]],
        );
        const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
        yield* importer.reconcileShells.pipe(
          Effect.provideService(ProviderSessionRuntime.LegacyProviderContinuationInputsV1, inputs),
        );
        const rows = yield* sql<{
          readonly qualification_json: string;
          readonly evidence_json: string | null;
        }>`
          SELECT qualification_json, evidence_json FROM orchestration_v2_legacy_continuation_dispositions WHERE thread_id = ${threadId}
        `;
        assert.lengthOf(rows, 1);
        const qualification = yield* decodeQualification(rows[0]!.qualification_json);
        const qualified =
          fixture === "historical_store" ||
          fixture === "native_read" ||
          fixture === "native_read_no_key";
        const unsupported = fixture.startsWith("unsupported-");
        assert.equal(
          qualification.type,
          qualified ? "qualified" : unsupported ? "unsupported" : "unknown",
        );
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const projection = yield* projections.getThreadProjection(threadId);
        assert.equal(projection.thread.runtimeMode, "full-access");
        assert.equal(projection.thread.interactionMode, "default");
        assert.isNull(projection.thread.settledOverride);
        assert.deepEqual(projection.runs, []);
        assert.deepEqual(projection.providerSessions, []);
        assert.deepEqual(projection.runtimeRequests, []);
        assert.lengthOf(projection.providerThreads, qualified ? 1 : 0);
        if (qualified) {
          assert.equal(projection.providerThreads[0]?.status, "not_loaded");
          assert.isNull(projection.providerThreads[0]?.providerSessionId);
          const evidence = yield* decodeContinuationEvidence(rows[0]!.evidence_json!);
          assert.isNull(evidence.providerInstanceId);
          assert.isNull(evidence.stoppedProof.providerInstanceId);
          assert.equal(
            evidence.historicalSourceIdentity.sourceHomeIdentity,
            "/fixture/historical-home",
          );
          if (fixture === "native_read_no_key") assert.isNull(evidence.continuationKey);
        } else {
          assert.isNull(projection.thread.activeProviderThreadId);
        }
      }),
    );
  }

  testEffect(
    "does not publish or confirm a shell or transcript import before its outer transaction commits",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const threadId = ThreadId.make("thread:legacy-outer-rollback");
        yield* seedHistoricalShell(threadId);
        for (const ordinal of [1, 2, 3, 4]) {
          yield* sql`
          INSERT INTO projection_thread_messages (
            message_id, thread_id, role, text, is_streaming, created_at, updated_at
          ) VALUES (${`legacy-outer-message:${ordinal}`}, ${threadId}, ${ordinal % 2 === 1 ? "user" : "assistant"},
            ${`Historical ${ordinal}`}, 0, ${`2026-01-0${ordinal}T00:00:00.000Z`}, ${`2026-01-0${ordinal}T00:00:00.000Z`})
        `;
        }
        const store = yield* EventStore.EventStoreV2;
        const published: unknown[] = [];
        const observedStore = EventStore.EventStoreV2.of({
          ...store,
          publishCommitted: (events) =>
            Effect.sync(() => void published.push(...events)).pipe(
              Effect.andThen(store.publishCommitted(events)),
            ),
        });
        // Local builds capture these observed services instead of reusing the
        // enclosing test layer's memoized sink and importer.
        const sink = yield* Effect.gen(function* () {
          return yield* EventSink.EventSinkV2;
        }).pipe(
          Effect.provide(EventSink.layer, { local: true }),
          Effect.provideService(EventStore.EventStoreV2, observedStore),
        );
        const importer = yield* Effect.gen(function* () {
          return yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
        }).pipe(
          Effect.provide(LegacyV1ThreadImporter.layer, { local: true }),
          Effect.provideService(EventSink.EventSinkV2, sink),
        );
        const failedShell = yield* sink
          .withTransaction(
            importer.reconcileShells.pipe(
              Effect.andThen(Effect.fail("after shell before outer commit")),
            ),
          )
          .pipe(Effect.result);
        assert.equal(failedShell._tag, "Failure");
        assert.deepEqual(published, []);
        assert.equal(yield* sink.latestSequence({ threadId }), 0);
        const markers =
          yield* sql`SELECT thread_id FROM orchestration_v2_legacy_imports WHERE thread_id = ${threadId}`;
        const dispositions =
          yield* sql`SELECT thread_id FROM orchestration_v2_legacy_continuation_dispositions WHERE thread_id = ${threadId}`;
        assert.deepEqual(markers, []);
        assert.deepEqual(dispositions, []);
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        assert.isNull(yield* projections.getThreadShell(threadId));
        yield* importer.reconcileShells;
        assert.isAbove(published.length, 0);
        published.length = 0;
        const shellSequence = yield* sink.latestSequence({ threadId });
        const failedTranscript = yield* sink
          .withTransaction(
            importer
              .ensureTranscript(threadId)
              .pipe(Effect.andThen(Effect.fail("after transcript before outer commit"))),
          )
          .pipe(Effect.result);
        assert.equal(failedTranscript._tag, "Failure");
        assert.deepEqual(published, []);
        assert.equal(yield* sink.latestSequence({ threadId }), shellSequence);
        assert.lengthOf((yield* projections.getThreadProjection(threadId)).messages, 2);
        const transcriptMarker = yield* sql<{ readonly transcript_imported_at: string | null }>`
        SELECT transcript_imported_at FROM orchestration_v2_legacy_imports WHERE thread_id = ${threadId}
      `;
        assert.isNull(transcriptMarker[0]?.transcript_imported_at);
        assert.deepEqual(yield* importer.ensureTranscript(threadId), {
          importedThreadCount: 1,
          importedMessageCount: 2,
        });
        assert.isAbove(published.length, 0);
        assert.lengthOf((yield* projections.getThreadProjection(threadId)).messages, 4);
        assert.deepEqual(yield* importer.ensureTranscript(threadId), {
          importedThreadCount: 0,
          importedMessageCount: 0,
        });
      }),
  );

  testEffect("uses the created-thread index for startup migration checks", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
      const statements: string[] = [];
      const tracer = Tracer.make({
        span(options) {
          const span = new Tracer.NativeSpan(options);
          const end = span.end.bind(span);
          span.end = (endTime, exit) => {
            end(endTime, exit);
            const query = span.attributes.get("db.query.text");
            if (
              typeof query === "string" &&
              query.includes("FROM projection_threads AS thread") &&
              query.includes("FROM orchestration_events AS event")
            ) {
              statements.push(query);
            }
          };
          return span;
        },
      });

      assert.equal(yield* importer.pendingThreadCount.pipe(Effect.withTracer(tracer)), 0);
      assert.deepStrictEqual(yield* importer.reconcileShells.pipe(Effect.withTracer(tracer)), {
        importedThreadCount: 0,
        importedMessageCount: 0,
      });
      assert.lengthOf(statements, 2);
      for (const statement of statements) {
        const plan = yield* sql.unsafe<{ readonly detail: string }>(
          `EXPLAIN QUERY PLAN ${statement}`,
        );
        assert.match(
          plan.map((row) => row.detail).join("\n"),
          /SEARCH event USING INDEX orchestration_events_v2_created_threads_idx \(stream_id=\?\)/,
        );
      }
    }),
  );

  testEffect("imports lightweight shells, hydrates transcripts, and remains idempotent", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
      const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const eventSink = yield* EventSink.EventSinkV2;
      const threadId = ThreadId.make("thread:legacy-import");

      yield* sql`
        INSERT INTO projection_projects (
          project_id,
          title,
          workspace_root,
          default_model_selection_json,
          scripts_json,
          created_at,
          updated_at,
          deleted_at
        ) VALUES (
          'project:legacy-import',
          'Legacy project',
          '/tmp/legacy-project',
          '{"instanceId":"codex","model":"gpt-5.4"}',
          '[]',
          '2026-01-01T00:00:00.000Z',
          '2026-01-04T00:00:00.000Z',
          NULL
        )
      `;
      yield* sql`
        INSERT INTO projection_threads (
          thread_id,
          project_id,
          title,
          model_selection_json,
          runtime_mode,
          interaction_mode,
          branch,
          worktree_path,
          latest_turn_id,
          created_at,
          updated_at,
          archived_at,
          settled_override,
          settled_at,
          unsettled_at,
          snoozed_until,
          snoozed_at,
          pinned_at,
          pin_order_key,
          linked_pull_request_json,
          deleted_at
        ) VALUES (
          ${threadId},
          'project:legacy-import',
          'Migrated conversation',
          '{"instanceId":"codex","model":"gpt-5.4"}',
          'full-access',
          'default',
          ' main ',
          ' /tmp/legacy-project ',
          NULL,
          '2026-01-01T00:00:00.000Z',
          '2026-01-04T00:00:00.000Z',
          NULL,
          NULL,
          NULL,
          '2026-01-03T12:00:00.000Z',
          '2026-02-01T00:00:00.000Z',
          '2026-01-04T00:00:00.000Z',
          '2026-01-02T00:00:00.000Z',
          'm',
          '{"projectId":"project:legacy-import","repository":"pingdotgg/t3code","number":9000,"url":"https://github.com/pingdotgg/t3code/pull/9000"}',
          NULL
        )
      `;
      yield* sql`
        INSERT INTO projection_thread_messages (
          message_id,
          thread_id,
          turn_id,
          role,
          text,
          attachments_json,
          is_streaming,
          created_at,
          updated_at
        ) VALUES
          (
            'message:legacy:1',
            ${threadId},
            NULL,
            'user',
            'First question',
            '[]',
            0,
            '2026-01-01T01:00:00.000Z',
            '2026-01-01T01:00:00.000Z'
          ),
          (
            'message:legacy:2',
            ${threadId},
            NULL,
            'assistant',
            'First answer',
            '[]',
            0,
            '2026-01-02T01:00:00.000Z',
            '2026-01-02T01:00:00.000Z'
          ),
          (
            'message:legacy:3',
            ${threadId},
            NULL,
            'user',
            'Follow-up question',
            '[]',
            0,
            '2026-01-03T01:00:00.000Z',
            '2026-01-03T01:00:00.000Z'
          ),
          (
            'message:legacy:4',
            ${threadId},
            NULL,
            'assistant',
            'Partial answer',
            '[]',
            1,
            '2026-01-04T01:00:00.000Z',
            '2026-01-04T01:00:00.000Z'
        )
      `;

      yield* sql`
        INSERT INTO projection_thread_pull_requests (
          thread_id, host, repository, number, url, source, linked_at, snapshot_json
        ) VALUES
          (${threadId}, 'github.com', 'pingdotgg/t3code', 9002,
            'https://github.com/pingdotgg/t3code/pull/9002', 'created',
            '2026-01-03T00:00:00.000Z',
            '{"state":"open","title":"Second PR","headBranch":"feature-two","baseBranch":"main","isDraft":false,"updatedAt":"2026-01-03T00:00:00.000Z","syncedAt":"2026-01-03T00:00:00.000Z"}'),
          (${threadId}, 'github.com', 'pingdotgg/t3code', 9003,
            'https://github.com/pingdotgg/t3code/pull/9003', 'manual',
            '2026-01-04T00:00:00.000Z', NULL)
      `;

      assert.equal(yield* importer.pendingThreadCount, 1);
      const shellImport = yield* importer.reconcileShells;
      assert.equal(yield* importer.pendingThreadCount, 1);
      assert.deepStrictEqual(shellImport, {
        importedThreadCount: 1,
        importedMessageCount: 2,
      });
      const shellEventCount = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count
        FROM orchestration_events
        WHERE application_event_version = 2
          AND aggregate_kind = 'thread'
          AND stream_id = ${threadId}
      `;
      assert.equal(shellEventCount[0]?.count, 6);

      assert.isTrue((yield* maintenance.verify).valid);
      const shellProjection = yield* projections.getThreadProjection(threadId);
      assert.equal(shellProjection.thread.historyOrigin, "v1_import");
      assert.equal(shellProjection.thread.branch, "main");
      assert.equal(shellProjection.thread.worktreePath, "/tmp/legacy-project");
      assert.deepEqual(
        shellProjection.thread.pinnedAt,
        DateTime.makeUnsafe("2026-01-02T00:00:00.000Z"),
      );
      assert.equal(shellProjection.thread.pinOrderKey, "m");
      assert.deepEqual(
        shellProjection.thread.snoozedUntil,
        DateTime.makeUnsafe("2026-02-01T00:00:00.000Z"),
      );
      assert.deepEqual(
        shellProjection.thread.unsettledAt,
        DateTime.makeUnsafe("2026-01-03T12:00:00.000Z"),
      );
      assert.equal(shellProjection.thread.linkedPullRequest?.number, 9000);
      assert.deepStrictEqual(
        (shellProjection.thread.pullRequests ?? []).map((link) => link.number),
        [9002, 9003, 9000],
      );
      assert.equal(shellProjection.thread.pullRequests?.[0]?.snapshot?.title, "Second PR");
      assert.deepStrictEqual(
        (yield* listLinkedPullRequestThreads({
          host: "github.com",
          repository: "pingdotgg/t3code",
          number: 9002,
        })).threads.map((thread) => thread.id),
        [threadId],
      );
      const shellSnapshot = yield* projections.getShellSnapshot();
      assert.equal(
        shellSnapshot.threads.find((thread) => thread.id === threadId)?.historyOrigin,
        "v1_import",
      );
      assert.deepStrictEqual(
        shellProjection.messages.map((message) => message.id),
        ["message:legacy:3", "message:legacy:4"],
      );

      const renamedAt = DateTime.makeUnsafe("2026-01-05T00:00:00.000Z");
      yield* eventSink.write({
        events: [
          {
            id: EventId.make("event:legacy-import:metadata-after-shell"),
            type: "thread.metadata-updated",
            threadId,
            providerInstanceId: shellProjection.thread.providerInstanceId,
            occurredAt: renamedAt,
            payload: {
              ...shellProjection.thread,
              title: "Renamed after shell import",
              runtimeMode: "approval-required",
              interactionMode: "plan",
              archivedAt: renamedAt,
              settledOverride: "settled",
              settledAt: renamedAt,
              updatedAt: renamedAt,
            },
          },
        ],
      });

      const transcriptImport = yield* importer.ensureTranscript(threadId);
      assert.equal(yield* importer.pendingThreadCount, 0);
      assert.deepStrictEqual(transcriptImport, {
        importedThreadCount: 1,
        importedMessageCount: 2,
      });
      const projection = yield* projections.getThreadProjection(threadId);
      assert.deepEqual(projection.runs, []);
      assert.deepEqual(projection.attempts, []);
      assert.deepEqual(projection.providerSessions, []);
      assert.deepEqual(projection.runtimeRequests, []);
      assert.deepEqual(projection.providerTurns, []);
      assert.isNull(projection.thread.activeProviderThreadId);
      assert.isTrue(projection.messages.every((message) => message.runId === null));
      assert.isTrue(
        projection.turnItems.every((item) => item.runId === null && item.providerTurnId === null),
      );
      assert.equal(projection.thread.title, "Renamed after shell import");
      assert.equal(projection.thread.runtimeMode, "approval-required");
      assert.equal(projection.thread.interactionMode, "plan");
      assert.deepEqual(projection.thread.archivedAt, renamedAt);
      assert.equal(projection.thread.settledOverride, "settled");
      assert.deepEqual(projection.thread.settledAt, renamedAt);
      assert.deepStrictEqual(
        projection.messages.map((message) => message.id),
        ["message:legacy:1", "message:legacy:2", "message:legacy:3", "message:legacy:4"],
      );
      assert.deepStrictEqual(
        projection.turnItems
          .filter(
            (
              item,
            ): item is Extract<
              (typeof projection.turnItems)[number],
              { readonly type: "user_message" | "assistant_message" }
            > => item.type === "user_message" || item.type === "assistant_message",
          )
          .map((item) => [item.messageId, item.ordinal, item.status]),
        [
          ["message:legacy:1", 1, "completed"],
          ["message:legacy:2", 2, "completed"],
          ["message:legacy:3", 3, "completed"],
          ["message:legacy:4", 4, "interrupted"],
        ],
      );

      yield* eventSink.write({
        events: [
          {
            id: EventId.make("event:legacy-import:explicit-unpin"),
            type: "thread.unpinned",
            threadId,
            providerInstanceId: projection.thread.providerInstanceId,
            occurredAt: renamedAt,
            payload: { ...projection.thread, pinnedAt: null, pinOrderKey: null },
          },
        ],
      });
      yield* sql`
        UPDATE orchestration_v2_projection_threads
        SET payload_json = json_remove(
          payload_json,
          '$.snoozedUntil',
          '$.snoozedAt',
          '$.unsettledAt',
          '$.linkedPullRequest',
          '$.pullRequests'
        )
        WHERE thread_id = ${threadId}
      `;
      assert.deepStrictEqual(yield* importer.reconcileShells, {
        importedThreadCount: 1,
        importedMessageCount: 0,
      });
      const repaired = yield* projections.getThreadProjection(threadId);
      assert.isNull(repaired.thread.pinnedAt);
      assert.isNull(repaired.thread.pinOrderKey);
      assert.deepEqual(
        repaired.thread.snoozedUntil,
        DateTime.makeUnsafe("2026-02-01T00:00:00.000Z"),
      );
      assert.deepEqual(
        repaired.thread.unsettledAt,
        DateTime.makeUnsafe("2026-01-03T12:00:00.000Z"),
      );
      assert.equal(repaired.thread.linkedPullRequest?.number, 9000);
      assert.deepStrictEqual(
        (repaired.thread.pullRequests ?? []).map((link) => link.number),
        [9002, 9003, 9000],
      );
      assert.equal(repaired.thread.pullRequests?.[0]?.snapshot?.title, "Second PR");
      const eventCountBeforeRetry = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count
        FROM orchestration_events
        WHERE application_event_version = 2
          AND aggregate_kind = 'thread'
          AND stream_id = ${threadId}
      `;
      assert.deepStrictEqual(yield* importer.reconcileShells, {
        importedThreadCount: 0,
        importedMessageCount: 0,
      });
      assert.deepStrictEqual(yield* importer.ensureTranscript(threadId), {
        importedThreadCount: 0,
        importedMessageCount: 0,
      });
      const eventCountAfterRetry = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count
        FROM orchestration_events
        WHERE application_event_version = 2
          AND aggregate_kind = 'thread'
          AND stream_id = ${threadId}
      `;
      assert.equal(eventCountAfterRetry[0]?.count, eventCountBeforeRetry[0]?.count);
    }),
  );

  testEffect("repairs newly added metadata after an earlier metadata repair", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
      const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const eventSink = yield* EventSink.EventSinkV2;
      const threadId = ThreadId.make("thread:legacy-metadata-upgrade");
      const previousRepairId = EventId.make(`migration:v1:thread:${threadId}:metadata-repair`);

      yield* sql`
        INSERT INTO projection_projects (
          project_id, title, workspace_root, scripts_json, created_at, updated_at
        ) VALUES (
          'project:legacy-metadata-upgrade',
          'Legacy project',
          '/tmp/legacy-metadata-upgrade',
          '[]',
          '2026-01-01T00:00:00.000Z',
          '2026-01-01T00:00:00.000Z'
        )
      `;
      yield* sql`
        INSERT INTO projection_threads (
          thread_id, project_id, title, model_selection_json, runtime_mode,
          interaction_mode, created_at, updated_at, pinned_at, pin_order_key,
          linked_pull_request_json, branch_pull_request_json, active_order_key
        ) VALUES (
          ${threadId},
          'project:legacy-metadata-upgrade',
          'Original v1 title',
          '{"instanceId":"codex","model":"gpt-5.4"}',
          'full-access',
          'default',
          '2026-01-01T00:00:00.000Z',
          '2026-01-01T00:00:00.000Z',
          '2026-01-02T00:00:00.000Z',
          'm',
          '{"projectId":"project:legacy-metadata-upgrade","repository":"pingdotgg/t3code","number":9000,"url":"https://github.com/pingdotgg/t3code/pull/9000"}',
          '{"projectId":"project:legacy-metadata-upgrade","repository":"pingdotgg/t3code","number":9001,"url":"https://github.com/pingdotgg/t3code/pull/9001"}',
          'az'
        )
      `;
      yield* importer.reconcileShells;
      yield* maintenance.rebuild;
      const shellProjection = yield* projections.getThreadProjection(threadId);
      assert.deepStrictEqual(
        (shellProjection.thread.pullRequests ?? []).map((link) => link.number),
        [9000],
      );
      const previousRepairThread = {
        ...shellProjection.thread,
        title: "Renamed in v2",
        pinnedAt: null,
        pinOrderKey: null,
        linkedPullRequest: null,
        pullRequests: [],
      };
      delete previousRepairThread.branchPullRequest;
      delete previousRepairThread.activeOrderKey;
      yield* eventSink.write({
        events: [
          {
            id: previousRepairId,
            type: "thread.metadata-updated",
            threadId,
            providerInstanceId: previousRepairThread.providerInstanceId,
            occurredAt: DateTime.makeUnsafe("2026-01-03T00:00:00.000Z"),
            payload: previousRepairThread,
          },
        ],
      });

      assert.deepStrictEqual(yield* importer.reconcileShells, {
        importedThreadCount: 1,
        importedMessageCount: 0,
      });
      const repaired = yield* projections.getThreadProjection(threadId);
      assert.equal(repaired.thread.title, "Renamed in v2");
      assert.isNull(repaired.thread.pinnedAt);
      assert.isNull(repaired.thread.pinOrderKey);
      assert.isNull(repaired.thread.linkedPullRequest);
      assert.deepStrictEqual(repaired.thread.pullRequests, []);
      assert.equal(repaired.thread.branchPullRequest?.number, 9001);
      assert.equal(repaired.thread.activeOrderKey, "az");

      const eventsBeforeRetry = yield* sql<{ readonly event_id: string }>`
        SELECT event_id
        FROM orchestration_events
        WHERE application_event_version = 2 AND stream_id = ${threadId}
        ORDER BY sequence
      `;
      assert.equal(eventsBeforeRetry.length, 4);
      assert.isTrue(eventsBeforeRetry.some((event) => event.event_id === previousRepairId));
      assert.deepStrictEqual(yield* importer.reconcileShells, {
        importedThreadCount: 0,
        importedMessageCount: 0,
      });

      assert.isTrue((yield* maintenance.rebuild).valid);
      const replayed = yield* projections.getThreadProjection(threadId);
      assert.deepStrictEqual(replayed.thread, repaired.thread);
      assert.deepStrictEqual(
        yield* Effect.gen(function* () {
          const restartedImporter = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
          return yield* restartedImporter.reconcileShells;
        }).pipe(Effect.provide(LegacyV1ThreadImporter.layer)),
        { importedThreadCount: 0, importedMessageCount: 0 },
      );
      const eventsAfterRestart = yield* sql<{ readonly event_id: string }>`
        SELECT event_id
        FROM orchestration_events
        WHERE application_event_version = 2 AND stream_id = ${threadId}
        ORDER BY sequence
      `;
      assert.deepStrictEqual(eventsAfterRestart, eventsBeforeRetry);
    }),
  );
});

import { assert, it } from "@effect/vitest";
import {
  AuthSessionId,
  AuthOrchestrationOperateScope,
  CommandId,
  ThreadId,
  RunId,
  MessageId,
} from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { importedApplicationAttachmentSha256V1 } from "./ImportedApplicationAttachmentInventory.ts";
import migration from "../persistence/Migrations/102_JonesImportedHistoryChoices.ts";
import {
  commandIdentity,
  reserveImportedHistoryChoice,
  reviewImportedHistory,
  claimImportedHistoryStart,
} from "./ImportedHistoryChoice.ts";

const memory = NodeSqliteClient.layer({ filename: ":memory:" });
const principal = {
  sessionId: AuthSessionId.make("session:reviewed"),
  subject: "fixture",
  method: "browser-session-cookie" as const,
  scopes: new Set([AuthOrchestrationOperateScope]),
};
const threadId = ThreadId.make("thread:reviewed");
const delivery = {
  type: "queued_run" as const,
  runId: RunId.make("run:queued"),
  messageId: MessageId.make("message:queued"),
};
const capabilities = {
  context: { canConsumeHandoffSummaries: true, supportsFullThreadHandoff: true },
};
const encode = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const setup = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE orchestration_command_receipts(command_id TEXT PRIMARY KEY)`;
  yield* sql`CREATE TABLE orchestration_v2_effect_outbox(effect_id TEXT PRIMARY KEY, thread_id TEXT, status TEXT)`;
  yield* sql`CREATE TABLE auth_sessions(session_id TEXT,subject TEXT,method TEXT,scopes TEXT,expires_at TEXT,revoked_at TEXT)`;
  yield* sql`CREATE TABLE native_creation_automation_enrollments(session_id TEXT)`;
  yield* sql`CREATE TABLE workstreams_native_enrollments(session_id TEXT)`;
  yield* sql`CREATE TABLE jones_native_workspace_admissions(claim_id TEXT)`;
  yield* sql`CREATE TABLE native_creation_intents(claim_id TEXT,thread_id TEXT)`;
  yield* sql`CREATE TABLE orchestration_events(event_id TEXT,sequence INTEGER,payload_json TEXT,application_event_version INTEGER,aggregate_kind TEXT,stream_id TEXT,event_type TEXT)`;
  yield* sql`CREATE TABLE orchestration_v2_projection_threads(thread_id TEXT,payload_json TEXT)`;
  yield* sql`CREATE TABLE orchestration_v2_projection_provider_threads(provider_thread_id TEXT,thread_id TEXT,payload_json TEXT)`;
  yield* sql`CREATE TABLE orchestration_v2_projection_turn_items(turn_item_id TEXT,thread_id TEXT,run_id TEXT,ordinal INTEGER,type TEXT,payload_json TEXT)`;
  yield* sql`CREATE TABLE orchestration_v2_projection_runs(run_id TEXT,thread_id TEXT,status TEXT,ordinal INTEGER,payload_json TEXT)`;
  yield* sql`CREATE TABLE orchestration_v2_projection_messages(message_id TEXT,thread_id TEXT,payload_json TEXT)`;
  yield* sql`CREATE TABLE orchestration_v2_projection_nodes(node_id TEXT,thread_id TEXT,payload_json TEXT)`;
  yield* sql`CREATE TABLE orchestration_v2_projection_run_attempts(attempt_id TEXT,thread_id TEXT,payload_json TEXT)`;
  yield* sql`CREATE TABLE orchestration_v2_projection_checkpoint_scopes(scope_id TEXT,thread_id TEXT,payload_json TEXT)`;
  yield* sql`CREATE TABLE orchestration_v2_projection_provider_sessions(provider_session_id TEXT,thread_id TEXT,payload_json TEXT)`;
  yield* sql`CREATE TABLE projection_projects(project_id TEXT,deleted_at TEXT)`;
  yield* migration;
  yield* sql`INSERT INTO auth_sessions VALUES (${principal.sessionId}, 'fixture', 'browser-session-cookie', ${yield* encode([...principal.scopes])}, '2099-01-01T00:00:00Z', NULL)`;
  const thread = {
    id: threadId,
    projectId: "project:reviewed",
    createdAt: "2026-10-07T00:00:00Z",
    deletedAt: null,
    archivedAt: null,
    historyOrigin: "v1_import",
  };
  const encoded = yield* encode(thread);
  yield* sql`INSERT INTO orchestration_events VALUES (${`migration:v1:thread:${threadId}:created`},1,${encoded},2,'thread',${threadId},'thread.created')`;
  yield* sql`INSERT INTO orchestration_v2_projection_threads VALUES (${threadId},${encoded})`;
  const queued = {
    id: delivery.runId,
    threadId,
    ordinal: 1,
    providerInstanceId: "codex",
    modelSelection: { instanceId: "codex", model: "fixture" },
    providerThreadId: "provider:queued",
    userMessageId: delivery.messageId,
    rootNodeId: "node:queued",
    activeAttemptId: "attempt:queued",
    status: "queued",
    queueHeld: true,
    requestedAt: "2026-10-07T00:00:00Z",
    startedAt: null,
    completedAt: null,
    checkpointId: null,
    contextHandoffId: null,
  };
  yield* sql`INSERT INTO orchestration_v2_projection_runs VALUES (${delivery.runId},${threadId},'queued',1,${yield* encode(queued)})`;
  yield* sql`INSERT INTO orchestration_v2_projection_messages VALUES (${delivery.messageId},${threadId},${yield* encode(
    {
      id: delivery.messageId,
      threadId,
      runId: delivery.runId,
      nodeId: "node:queued",
      role: "user",
      text: "Continue after review",
      attachments: [],
      streaming: false,
      createdBy: "user",
      creationSource: "web",
      createdAt: "2026-10-07T00:00:00Z",
      updatedAt: "2026-10-07T00:00:00Z",
    },
  )})`;
  yield* sql`INSERT INTO projection_projects VALUES ('project:reviewed',NULL)`;
  yield* sql`INSERT INTO orchestration_v2_projection_turn_items VALUES ('item:legacy',${threadId},NULL,1,'user_message',${yield* encode({ threadId, runId: null, text: "Original visible history" })})`;
});
const context = (reviewedBasis: string) => ({
  command: {
    type: "thread.imported-history.start" as const,
    commandId: CommandId.make("command:reviewed"),
    threadId,
    delivery,
    reviewedBasis,
  },
  principal,
  targetCapabilities: capabilities,
});
const review = () =>
  reviewImportedHistory({ threadId, delivery, principal, targetCapabilities: capabilities });

it.effect(
  "immutable full command and actor identity observe duplicate choice but deny reassociation",
  () =>
    Effect.gen(function* () {
      yield* setup;
      const facts = yield* review();
      assert.strictEqual(facts.review.status, "available");
      assert.isNotNull(facts.review.reviewedBasis);
      const ctx = context(facts.review.reviewedBasis ?? "unavailable");
      assert.isFalse((yield* reserveImportedHistoryChoice(ctx)).duplicate);
      assert.isTrue((yield* reserveImportedHistoryChoice(ctx)).duplicate);
      const changed = {
        ...ctx,
        command: {
          ...ctx.command,
          delivery: { ...delivery, messageId: MessageId.make("message:changed") },
        },
      };
      assert.strictEqual(
        (yield* reserveImportedHistoryChoice(changed).pipe(Effect.result))._tag,
        "Failure",
      );
      assert.notDeepEqual(yield* commandIdentity(ctx), yield* commandIdentity(changed));
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "changed source returns a durable rejection decision rather than accepting a stale review",
  () =>
    Effect.gen(function* () {
      yield* setup;
      const original = yield* review();
      const sql = yield* SqlClient.SqlClient;
      yield* sql`UPDATE orchestration_v2_projection_turn_items SET payload_json=${yield* encode({ threadId, runId: null, text: "Changed history" })}`;
      const reserved = yield* reserveImportedHistoryChoice(
        context(original.review.reviewedBasis ?? "unavailable"),
      );
      assert.strictEqual(reserved.rejectionReason, "review_changed");
    }).pipe(Effect.provide(memory)),
);

it.effect.each(["native enrollment", "revoked actor"] as const)(
  "refuses %s before choice insertion",
  (kind) =>
    Effect.gen(function* () {
      yield* setup;
      const sql = yield* SqlClient.SqlClient;
      if (kind === "native enrollment")
        yield* sql`INSERT INTO native_creation_automation_enrollments VALUES (${principal.sessionId})`;
      else yield* sql`UPDATE auth_sessions SET revoked_at='2026-10-07T00:00:00Z'`;
      assert.strictEqual(
        (yield* reserveImportedHistoryChoice(context("a".repeat(64))).pipe(Effect.result))._tag,
        "Failure",
      );
      assert.deepEqual(yield* sql`SELECT * FROM jones_imported_history_choices`, []);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "transaction rollback leaves no choice, and immutable rows reject update/delete/replace",
  () =>
    Effect.gen(function* () {
      yield* setup;
      const sql = yield* SqlClient.SqlClient;
      const facts = yield* review();
      const ctx = context(facts.review.reviewedBasis ?? "unavailable");
      yield* sql
        .withTransaction(
          reserveImportedHistoryChoice(ctx).pipe(Effect.andThen(Effect.fail("fixture rollback"))),
        )
        .pipe(Effect.result);
      assert.deepEqual(yield* sql`SELECT * FROM jones_imported_history_choices`, []);
      yield* reserveImportedHistoryChoice(ctx);
      for (const mutation of [
        sql`UPDATE jones_imported_history_choices SET thread_id='foreign'`,
        sql`DELETE FROM jones_imported_history_choices`,
        sql`INSERT OR REPLACE INTO jones_imported_history_choices SELECT * FROM jones_imported_history_choices`,
      ])
        assert.strictEqual((yield* mutation.pipe(Effect.result))._tag, "Failure");
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "unknown captured execution cannot be invoked again and stale transcripts do not start",
  () =>
    Effect.gen(function* () {
      yield* setup;
      const sql = yield* SqlClient.SqlClient;
      const facts = yield* review();
      const ctx = context(facts.review.reviewedBasis ?? "unavailable");
      yield* reserveImportedHistoryChoice(ctx);
      yield* sql`INSERT INTO orchestration_v2_effect_outbox VALUES ('effect:reviewed',${threadId},'running')`;
      const source =
        yield* sql`SELECT * FROM orchestration_v2_projection_turn_items WHERE thread_id=${threadId} AND run_id IS NULL ORDER BY ordinal,turn_item_id`;
      yield* sql`INSERT INTO jones_imported_history_start_reservations VALUES ('effect:reviewed',${ctx.command.commandId},${threadId},${delivery.runId},'attempt:reviewed','provider:reviewed',${importedApplicationAttachmentSha256V1(source)})`;
      yield* sql`UPDATE orchestration_v2_projection_runs SET status='starting', payload_json=${yield* encode({ activeAttemptId: "attempt:reviewed", providerThreadId: "provider:reviewed" })} WHERE run_id=${delivery.runId}`;
      const input = { threadId, runId: delivery.runId, providerThreadId: "provider:reviewed" };
      yield* sql.withTransaction(claimImportedHistoryStart(input));
      assert.strictEqual(
        (yield* sql.withTransaction(claimImportedHistoryStart(input)).pipe(Effect.result))._tag,
        "Failure",
      );
      assert.strictEqual(
        (yield* claimImportedHistoryStart({ ...input, providerThreadId: "foreign" }).pipe(
          Effect.result,
        ))._tag,
        "Failure",
      );
    }).pipe(Effect.provide(memory)),
);

it.effect("review pins held queued delivery and refuses a changed message or queue identity", () =>
  Effect.gen(function* () {
    yield* setup;
    const sql = yield* SqlClient.SqlClient;
    const original = yield* review();
    assert.strictEqual(original.review.status, "available");
    const wrong = yield* reviewImportedHistory({
      threadId,
      principal,
      targetCapabilities: capabilities,
      delivery: { ...delivery, messageId: MessageId.make("message:foreign") },
    });
    assert.strictEqual(wrong.review.reason, "queued_delivery_changed");
    yield* sql`UPDATE orchestration_v2_projection_messages SET payload_json=json_set(payload_json,'$.text','Changed queued prompt')`;
    const choice = yield* reserveImportedHistoryChoice(
      context(original.review.reviewedBasis ?? "unavailable"),
    );
    assert.strictEqual(choice.rejectionReason, "review_changed");
  }).pipe(Effect.provide(memory)),
);

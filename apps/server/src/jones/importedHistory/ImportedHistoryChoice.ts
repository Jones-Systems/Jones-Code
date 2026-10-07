import {
  AuthOrchestrationOperateScope,
  OrchestrationV2ConversationMessageJson,
  ImportedHistoryStart,
  ImportedHistoryOutcome,
  type ImportedHistoryDelivery,
  type ImportedHistoryReview,
  type EnvironmentSessionPrincipalShape,
  type CommandId,
  type ThreadId,
  type RunId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { RecordedRunJson } from "../../orchestration-v2/RecordedTypes.ts";
import { queuedRunsInDeliveryOrder } from "../../orchestration-v2/QueuedRunOrder.ts";
import { readApplicationBirthRecord } from "./ApplicationBirth.ts";
import {
  nativeCreationCanonicalJson as canonical,
  nativeCreationSha256 as sha256,
} from "../nativeCreation/NativeCreationPreparation.ts";

export class ImportedHistoryChoiceError extends Schema.TaggedError<ImportedHistoryChoiceError>()(
  "ImportedHistoryChoiceError",
  { code: Schema.String },
) {
  override get message(): string {
    return `Imported history admission failed: ${this.code}.`;
  }
}
const deny = (code: string) => Effect.fail(new ImportedHistoryChoiceError({ code }));
export interface ImportedHistoryContext {
  readonly command: ImportedHistoryStart;
  readonly principal: EnvironmentSessionPrincipalShape;
  readonly targetCapabilities: unknown;
}
export const commandIdentity = Effect.fnUntraced(function* (context: ImportedHistoryContext) {
  const command = yield* Schema.decodeUnknownEffect(ImportedHistoryStart)(context.command, {
    onExcessProperty: "error",
  });
  if (
    command.delivery.type === "message" &&
    (command.delivery.command.type !== "message.dispatch" ||
      command.delivery.command.commandId !== command.commandId ||
      command.delivery.command.threadId !== command.threadId)
  )
    return yield* deny("delivery_identity_conflict");
  const encoded = yield* Schema.encodeEffect(ImportedHistoryStart)(command);
  return {
    command,
    canonicalCommand: canonical(encoded),
    commandDigest: sha256(canonical(encoded)),
    deliveryDigest: sha256(canonical(encoded.delivery)),
    actorDigest: sha256(
      canonical({
        sessionId: context.principal.sessionId,
        subject: context.principal.subject,
        method: context.principal.method,
        proofKeyThumbprint: context.principal.proofKeyThumbprint ?? null,
        scopes: [...context.principal.scopes].sort(),
      }),
    ),
  };
});
const currentActor = Effect.fnUntraced(function* (principal: EnvironmentSessionPrincipalShape) {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{
    subject: string;
    method: string;
    scopes: string;
    expires_at: string;
    revoked_at: string | null;
  }>`
    SELECT subject, method, scopes, expires_at, revoked_at FROM auth_sessions WHERE session_id = ${principal.sessionId}`;
  const row = rows[0];
  if (
    rows.length !== 1 ||
    row === undefined ||
    row.revoked_at !== null ||
    row.subject !== principal.subject ||
    row.method !== principal.method ||
    !Number.isFinite(Date.parse(row.expires_at)) ||
    Date.parse(row.expires_at) <= DateTime.toEpochMillis(yield* DateTime.now)
  )
    return yield* deny("actor_changed");
  const scopes = Schema.decodeUnknownOption(Schema.fromJsonString(Schema.Array(Schema.String)))(
    row.scopes,
  );
  if (
    Option.isNone(scopes) ||
    !scopes.value.includes(AuthOrchestrationOperateScope) ||
    !principal.scopes.has(AuthOrchestrationOperateScope)
  )
    return yield* deny("actor_scope_changed");
  const native =
    yield* sql`SELECT session_id FROM native_creation_automation_enrollments WHERE session_id = ${principal.sessionId}
    UNION ALL SELECT session_id FROM workstreams_native_enrollments WHERE session_id = ${principal.sessionId}`;
  if (native.length !== 0) return yield* deny("qualified_native_imported_authority_unavailable");
});
export const reviewImportedHistory = Effect.fnUntraced(function* (input: {
  readonly threadId: ThreadId;
  readonly principal: EnvironmentSessionPrincipalShape;
  readonly delivery: ImportedHistoryDelivery;
  readonly targetCapabilities: unknown;
}) {
  const sql = yield* SqlClient.SqlClient;
  yield* currentActor(input.principal);
  const unavailable = (reason: string) => ({
    review: { status: "unavailable", reviewedBasis: null, reason } satisfies ImportedHistoryReview,
    basis: null,
  });
  const birth = yield* readApplicationBirthRecord(input.threadId);
  if (birth === null || birth.eventId !== `migration:v1:thread:${input.threadId}:created`)
    return unavailable("imported_birth_unavailable");
  const threads = yield* sql<{
    payload_json: string;
  }>`SELECT payload_json FROM orchestration_v2_projection_threads WHERE thread_id = ${input.threadId}`;
  const thread = Schema.decodeUnknownOption(
    Schema.fromJsonString(
      Schema.Struct({
        historyOrigin: Schema.Literal("v1_import"),
        deletedAt: Schema.Null,
        archivedAt: Schema.Null,
      }),
    ),
  )(threads[0]?.payload_json);
  if (threads.length !== 1 || Option.isNone(thread))
    return unavailable("imported_thread_unavailable");
  const workspace =
    yield* sql`SELECT admission.claim_id FROM jones_native_workspace_admissions admission
    JOIN native_creation_intents intent ON intent.claim_id = admission.claim_id WHERE intent.thread_id = ${input.threadId}`;
  if (workspace.length !== 0) return unavailable("native_workspace_owned");
  const providers = yield* sql<{
    payload_json: string;
  }>`SELECT payload_json FROM orchestration_v2_projection_provider_threads WHERE thread_id = ${input.threadId} ORDER BY provider_thread_id`;
  const fresh = Schema.fromJsonString(
    Schema.Struct({
      providerSessionId: Schema.Null,
      nativeThreadRef: Schema.Null,
      nativeConversationHeadRef: Schema.Null,
    }),
  );
  if (providers.some((row) => Option.isNone(Schema.decodeUnknownOption(fresh)(row.payload_json))))
    return unavailable("existing_native_context_requires_review");
  const source = yield* sql<{
    payload_json: string;
    type: string;
  }>`SELECT * FROM orchestration_v2_projection_turn_items WHERE thread_id = ${input.threadId} AND run_id IS NULL ORDER BY ordinal, turn_item_id`;
  if (
    source.length === 0 ||
    !source.some((row) => row.type === "user_message" || row.type === "assistant_message")
  )
    return unavailable("imported_transcript_unavailable");
  const transcriptItem = Schema.fromJsonString(
    Schema.Struct({ threadId: Schema.String, runId: Schema.Null, text: Schema.String }),
  );
  if (
    source.some((row) => {
      const decoded = Schema.decodeUnknownOption(transcriptItem)(row.payload_json);
      return Option.isNone(decoded) || decoded.value.threadId !== input.threadId;
    })
  )
    return unavailable("imported_transcript_decode_unavailable");
  const runs =
    yield* sql`SELECT run_id, payload_json FROM orchestration_v2_projection_runs WHERE thread_id = ${input.threadId} AND status IN ('preparing', 'starting', 'running', 'waiting') ORDER BY ordinal`;
  if (runs.length !== 0) return unavailable("other_work_pending");
  const queuedRuns = yield* sql<{
    payload_json: string;
  }>`SELECT payload_json FROM orchestration_v2_projection_runs WHERE thread_id = ${input.threadId} AND status = 'queued' ORDER BY ordinal, run_id`;
  const queuedMessages = yield* sql<{
    payload_json: string;
  }>`SELECT payload_json FROM orchestration_v2_projection_messages WHERE thread_id = ${input.threadId} ORDER BY message_id`;
  const queuedNodes =
    yield* sql`SELECT * FROM orchestration_v2_projection_nodes WHERE thread_id = ${input.threadId} ORDER BY node_id`;
  const queuedAttempts =
    yield* sql`SELECT * FROM orchestration_v2_projection_run_attempts WHERE thread_id = ${input.threadId} ORDER BY attempt_id`;
  const queuedScopes =
    yield* sql`SELECT * FROM orchestration_v2_projection_checkpoint_scopes WHERE thread_id = ${input.threadId} ORDER BY scope_id`;
  const queuedSessions =
    yield* sql`SELECT * FROM orchestration_v2_projection_provider_sessions WHERE thread_id = ${input.threadId} ORDER BY provider_session_id`;
  if (input.delivery.type === "queued_run") {
    const decodedRuns = yield* Effect.forEach(queuedRuns, (row) =>
      Schema.decodeUnknownEffect(Schema.fromJsonString(RecordedRunJson))(row.payload_json),
    );
    const decodedMessages = yield* Effect.forEach(queuedMessages, (row) =>
      Schema.decodeUnknownEffect(Schema.fromJsonString(OrchestrationV2ConversationMessageJson))(
        row.payload_json,
      ),
    );
    const first = queuedRunsInDeliveryOrder({ runs: decodedRuns, messages: decodedMessages })[0];
    const delivery = input.delivery;
    const message = decodedMessages.find((message) => message.id === delivery.messageId);
    if (
      first === undefined ||
      first.id !== input.delivery.runId ||
      first.userMessageId !== input.delivery.messageId ||
      message?.runId !== first.id ||
      message.role !== "user"
    )
      return unavailable("queued_delivery_changed");
    if (
      first.rootNodeId === null ||
      first.activeAttemptId === null ||
      first.providerThreadId === null
    )
      return unavailable("queued_execution_identity_unavailable");
  } else if (queuedRuns.length !== 0) return unavailable("queued_delivery_requires_review");
  const projects =
    yield* sql`SELECT project.* FROM projection_projects project JOIN orchestration_v2_projection_threads thread ON json_extract(thread.payload_json, '$.projectId') = project.project_id WHERE thread.thread_id = ${input.threadId}`;
  if (projects.length !== 1) return unavailable("project_unavailable");
  const effects =
    yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE thread_id = ${input.threadId} AND status IN ('pending', 'running')`;
  if (effects.length !== 0) return unavailable("unresolved_effects");
  const capabilities = Schema.decodeUnknownOption(
    Schema.Struct({
      context: Schema.Struct({
        canConsumeHandoffSummaries: Schema.Literal(true),
        supportsFullThreadHandoff: Schema.Literal(true),
      }),
    }),
  )(input.targetCapabilities);
  if (Option.isNone(capabilities)) return unavailable("target_handoff_unavailable");
  const encodedDelivery = yield* Schema.encodeEffect(ImportedHistoryStart.fields.delivery)(
    input.delivery,
  );
  const basis = {
    domain: "jones.imported-history.review/v1",
    birth,
    threads,
    providers,
    source,
    projects,
    queuedRuns,
    queuedMessages,
    queuedNodes,
    queuedAttempts,
    queuedScopes,
    queuedSessions,
    actorSessionId: input.principal.sessionId,
    delivery: encodedDelivery,
    targetCapabilities: input.targetCapabilities,
  };
  return {
    review: {
      status: "available",
      reviewedBasis: sha256(canonical(basis)),
      reason: null,
    } satisfies ImportedHistoryReview,
    basis,
  };
});
export const reserveImportedHistoryChoice = Effect.fnUntraced(function* (
  context: ImportedHistoryContext,
) {
  const sql = yield* SqlClient.SqlClient;
  const identity = yield* commandIdentity(context);
  yield* currentActor(context.principal);
  const existing = yield* sql<{
    actor_digest: string;
    command_digest: string;
    command_json: string;
    delivery_digest: string;
  }>`
    SELECT actor_digest, command_digest, command_json, delivery_digest FROM jones_imported_history_choices WHERE command_id = ${identity.command.commandId}`;
  if (existing.length > 0) {
    const row = existing[0]!;
    if (
      existing.length !== 1 ||
      row.actor_digest !== identity.actorDigest ||
      row.command_digest !== identity.commandDigest ||
      row.command_json !== identity.canonicalCommand ||
      row.delivery_digest !== identity.deliveryDigest
    )
      return yield* deny("identity_conflict");
    return { duplicate: true, identity, rejectionReason: null };
  }
  const facts = yield* reviewImportedHistory({
    ...context,
    threadId: identity.command.threadId,
    delivery: identity.command.delivery,
  });
  const rejectionReason =
    facts.basis === null || facts.review.reviewedBasis !== identity.command.reviewedBasis
      ? (facts.review.reason ?? "review_changed")
      : null;
  yield* sql`INSERT INTO jones_imported_history_choices
    (command_id, thread_id, actor_session_id, actor_digest, command_digest, delivery_digest, reviewed_basis, command_json, basis_json)
    VALUES (${identity.command.commandId}, ${identity.command.threadId}, ${context.principal.sessionId}, ${identity.actorDigest}, ${identity.commandDigest},
      ${identity.deliveryDigest}, ${identity.command.reviewedBasis}, ${identity.canonicalCommand}, ${canonical(facts.basis ?? facts.review)})`;
  return { duplicate: false, identity, rejectionReason };
});
export const readImportedHistoryChoice = Effect.fnUntraced(function* (input: {
  threadId: ThreadId;
  commandId: CommandId;
  principal: EnvironmentSessionPrincipalShape;
}) {
  const sql = yield* SqlClient.SqlClient;
  yield* currentActor(input.principal);
  const rows = yield* sql<{
    outcome_json: string;
    actor_session_id: string;
  }>`SELECT outcome.outcome_json, choice.actor_session_id
    FROM jones_imported_history_outcomes outcome JOIN jones_imported_history_choices choice ON choice.command_id = outcome.command_id
    WHERE choice.command_id = ${input.commandId} AND choice.thread_id = ${input.threadId}`;
  if (rows.length === 0) return null;
  if (rows.length !== 1 || rows[0]!.actor_session_id !== input.principal.sessionId)
    return yield* deny("actor_conflict");
  const outcome = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(ImportedHistoryOutcome))(
    rows[0]!.outcome_json,
  );
  if (
    outcome.commandId !== input.commandId ||
    outcome.threadId !== input.threadId ||
    outcome.actorSessionId !== input.principal.sessionId
  )
    return yield* deny("outcome_identity_conflict");
  if (outcome.status === "accepted" && outcome.effectId !== null) {
    const started =
      yield* sql`SELECT effect_id FROM jones_imported_history_execution_starts WHERE effect_id = ${outcome.effectId}`;
    if (started.length !== 0)
      return {
        ...outcome,
        status: "unknown" as const,
        reason: "execution_requires_reconciliation",
      };
  }
  return outcome;
});
export const recordImportedHistoryOutcome = Effect.fnUntraced(function* (
  outcome: ImportedHistoryOutcome,
) {
  const sql = yield* SqlClient.SqlClient;
  const encoded = yield* Schema.encodeEffect(ImportedHistoryOutcome)(outcome);
  yield* sql`INSERT INTO jones_imported_history_outcomes (command_id, receipt_command_id, outcome_json)
    VALUES (${outcome.commandId}, ${outcome.commandId}, ${canonical(encoded)})`;
});
export const claimImportedHistoryStart = Effect.fnUntraced(function* (input: {
  threadId: ThreadId;
  runId: RunId;
  providerThreadId: string;
}) {
  const sql = yield* SqlClient.SqlClient;
  const installed =
    yield* sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'jones_imported_history_start_reservations'`;
  if (installed.length === 0) return;
  const rows = yield* sql<{
    effect_id: string;
    command_id: string;
    run_attempt_id: string;
    provider_thread_id: string;
    source_digest: string;
  }>`
    SELECT * FROM jones_imported_history_start_reservations WHERE thread_id = ${input.threadId} AND run_id = ${input.runId}`;
  if (rows.length === 0) {
    const owned = yield* sql`SELECT outcome.command_id FROM jones_imported_history_outcomes outcome
      JOIN jones_imported_history_choices choice ON choice.command_id = outcome.command_id
      WHERE choice.thread_id = ${input.threadId} AND json_extract(outcome.outcome_json, '$.runId') = ${input.runId}`;
    if (owned.length !== 0) return yield* deny("start_reservation_unavailable");
    return;
  }
  const row = rows[0]!;
  if (rows.length !== 1 || row.provider_thread_id !== input.providerThreadId)
    return yield* deny("run_binding_changed");
  const source =
    yield* sql`SELECT * FROM orchestration_v2_projection_turn_items WHERE thread_id = ${input.threadId} AND run_id IS NULL ORDER BY ordinal, turn_item_id`;
  if (sha256(canonical(source)) !== row.source_digest)
    return yield* deny("transcript_source_changed");
  const choice = yield* sql<{
    actor_session_id: string;
    command_json: string;
  }>`SELECT actor_session_id, command_json FROM jones_imported_history_choices WHERE command_id = ${row.command_id}`;
  if (choice.length !== 1) return yield* deny("choice_unavailable");
  const session = yield* sql<{
    revoked_at: string | null;
    expires_at: string;
    scopes: string;
  }>`SELECT revoked_at, expires_at, scopes FROM auth_sessions WHERE session_id = ${choice[0]!.actor_session_id}`;
  const scopes = Schema.decodeUnknownOption(Schema.fromJsonString(Schema.Array(Schema.String)))(
    session[0]?.scopes,
  );
  if (
    session.length !== 1 ||
    session[0]!.revoked_at !== null ||
    !Number.isFinite(Date.parse(session[0]!.expires_at)) ||
    Date.parse(session[0]!.expires_at) <= DateTime.toEpochMillis(yield* DateTime.now) ||
    Option.isNone(scopes) ||
    !scopes.value.includes(AuthOrchestrationOperateScope)
  )
    return yield* deny("actor_changed");
  const started =
    yield* sql`SELECT effect_id FROM jones_imported_history_execution_starts WHERE effect_id = ${row.effect_id}`;
  if (started.length !== 0) return yield* deny("unknown_prior_execution");
  const runs = yield* sql<{
    payload_json: string;
  }>`SELECT payload_json FROM orchestration_v2_projection_runs WHERE thread_id = ${input.threadId} AND run_id = ${input.runId}`;
  const run = Schema.decodeUnknownOption(
    Schema.fromJsonString(
      Schema.Struct({ activeAttemptId: Schema.String, providerThreadId: Schema.String }),
    ),
  )(runs[0]?.payload_json);
  if (
    runs.length !== 1 ||
    Option.isNone(run) ||
    run.value.activeAttemptId !== row.run_attempt_id ||
    run.value.providerThreadId !== row.provider_thread_id
  )
    return yield* deny("run_binding_changed");
  yield* sql`INSERT INTO jones_imported_history_execution_starts (effect_id, captured_json) VALUES (${row.effect_id}, ${canonical({ ...input, ...row })})`;
});

import {
  threadPullRequestKeysEqual,
  threadPullRequestsOf,
} from "@t3tools/shared/threadPullRequests";
import {
  ChatAttachment,
  CommandId,
  OrchestrationMessageContext,
  DEFAULT_MODEL,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  DEFAULT_RUNTIME_MODE,
  EventId,
  IsoDateTime,
  MessageId,
  ModelSelection,
  type OrchestrationV2AppThread,
  OrchestrationV2AppThreadJson,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2DomainEvent,
  OrchestrationV2DomainEventJson,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2TurnItem,
  ProjectId,
  ProviderInstanceId,
  ProviderDriverKind,
  ProviderInteractionMode,
  RuntimeMode,
  RuntimeIdentityAttestation,
  ThreadId,
  ThreadLinkedPullRequest,
  ThreadPullRequestLink,
  TurnItemId,
  TurnId,
  TrimmedNonEmptyString,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import * as ProviderSessionRuntime from "../../persistence/ProviderSessionRuntime.ts";
import * as EventSink from "../EventSink.ts";
import * as IdAllocator from "../IdAllocator.ts";
import {
  qualifyProviderContinuation,
  type ProviderContinuationQualification,
} from "../ProviderContinuationQualification.ts";
import { makeKeyedSerialExecutor } from "../KeyedSerialExecutor.ts";
import { randomUuidV4 } from "../RandomUuid.ts";
import { nativeCreationCanonicalJson, nativeCreationSha256 } from "../NativeCreationPreparation.ts";
import {
  ImportedApplicationAttachmentBirthV1,
  type ImportedApplicationAttachmentQualificationV1,
} from "../ImportedApplicationAttachmentInventory.ts";

const IMPORT_EVENT_PREFIX = "migration:v1";
const TRANSCRIPT_EVENT_BATCH_SIZE = 100;

// These readers preserve stored V1 history only. Their output is never a V2
// command, run, provider process, approval, or actionable plan.
const historicalSourcePlan = Schema.Struct({
  threadId: ThreadId,
  planId: TrimmedNonEmptyString,
});
const historicalRuntimeMode = RuntimeMode.pipe(
  Schema.withDecodingDefault(Effect.succeed(DEFAULT_RUNTIME_MODE)),
);
const historicalInteractionMode = ProviderInteractionMode.pipe(
  Schema.withDecodingDefault(Effect.succeed(DEFAULT_PROVIDER_INTERACTION_MODE)),
);
const historicalMessageFields = {
  role: Schema.Literals(["user", "assistant"]),
  text: Schema.String,
  attachments: Schema.optional(Schema.Array(ChatAttachment)),
  context: Schema.optional(OrchestrationMessageContext),
  turnId: Schema.NullOr(TurnId).pipe(Schema.withDecodingDefault(Effect.succeed(null))),
  streaming: Schema.Boolean,
  createdAt: IsoDateTime,
  updatedAt: IsoDateTime,
};
const historicalMessage = Schema.Struct({ id: MessageId, ...historicalMessageFields });
const historicalProposedPlan = Schema.Struct({
  id: TrimmedNonEmptyString,
  turnId: Schema.NullOr(TurnId),
  planMarkdown: TrimmedNonEmptyString,
  implementedAt: Schema.NullOr(IsoDateTime).pipe(Schema.withDecodingDefault(Effect.succeed(null))),
  implementationThreadId: Schema.NullOr(ThreadId).pipe(
    Schema.withDecodingDefault(Effect.succeed(null)),
  ),
  createdAt: IsoDateTime,
  updatedAt: IsoDateTime,
});
const historicalLatestTurn = Schema.Struct({
  turnId: TurnId,
  state: Schema.Literals(["running", "completed", "interrupted", "error"]),
  requestedAt: IsoDateTime,
  startedAt: Schema.NullOr(IsoDateTime),
  completedAt: Schema.NullOr(IsoDateTime),
  assistantMessageId: Schema.NullOr(MessageId),
  sourceProposedPlan: Schema.optional(historicalSourcePlan),
});
export const HistoricalV1 = {
  threadCreated: Schema.Struct({
    threadId: ThreadId,
    projectId: ProjectId,
    title: TrimmedNonEmptyString,
    modelSelection: ModelSelection,
    runtimeMode: historicalRuntimeMode,
    interactionMode: historicalInteractionMode,
    branch: Schema.NullOr(TrimmedNonEmptyString),
    worktreePath: Schema.NullOr(TrimmedNonEmptyString),
    createdAt: IsoDateTime,
    updatedAt: IsoDateTime,
  }),
  messageSent: Schema.Struct({
    threadId: ThreadId,
    messageId: MessageId,
    ...historicalMessageFields,
  }),
  proposedPlan: historicalProposedPlan,
  turnStartRequested: Schema.Struct({
    threadId: ThreadId,
    messageId: MessageId,
    modelSelection: Schema.optional(ModelSelection),
    titleSeed: Schema.optional(TrimmedNonEmptyString),
    runtimeMode: historicalRuntimeMode,
    interactionMode: historicalInteractionMode,
    sourceProposedPlan: Schema.optional(historicalSourcePlan),
    createdAt: IsoDateTime,
  }),
  latestTurn: historicalLatestTurn,
  session: Schema.Struct({
    threadId: ThreadId,
    status: Schema.Literals([
      "idle",
      "starting",
      "ready",
      "running",
      "interrupted",
      "error",
      "stopped",
    ]),
    providerName: Schema.NullOr(TrimmedNonEmptyString),
    providerInstanceId: Schema.optional(ProviderInstanceId),
    runtimeMode: historicalRuntimeMode,
    activeTurnId: Schema.NullOr(TurnId),
    lastError: Schema.NullOr(TrimmedNonEmptyString),
    runtimeIdentity: Schema.optional(RuntimeIdentityAttestation),
    updatedAt: IsoDateTime,
  }),
  thread: Schema.Struct({
    id: ThreadId,
    projectId: ProjectId,
    title: TrimmedNonEmptyString,
    modelSelection: ModelSelection,
    runtimeMode: historicalRuntimeMode,
    interactionMode: historicalInteractionMode,
    branch: Schema.NullOr(TrimmedNonEmptyString),
    worktreePath: Schema.NullOr(TrimmedNonEmptyString),
    linkedPullRequest: Schema.optional(Schema.NullOr(ThreadLinkedPullRequest)),
    pullRequests: Schema.Array(ThreadPullRequestLink).pipe(
      Schema.withDecodingDefault(Effect.succeed([])),
    ),
    latestTurn: Schema.NullOr(historicalLatestTurn),
    createdAt: IsoDateTime,
    updatedAt: IsoDateTime,
    archivedAt: Schema.NullOr(IsoDateTime).pipe(Schema.withDecodingDefault(Effect.succeed(null))),
    settledOverride: Schema.NullOr(Schema.Literals(["settled", "active"])).pipe(
      Schema.withDecodingDefault(Effect.succeed(null)),
    ),
    settledAt: Schema.NullOr(IsoDateTime).pipe(Schema.withDecodingDefault(Effect.succeed(null))),
    messages: Schema.Array(historicalMessage),
    proposedPlans: Schema.Array(historicalProposedPlan).pipe(
      Schema.withDecodingDefault(Effect.succeed([])),
    ),
  }),
  historyImport: Schema.Struct({
    type: Schema.Literal("thread.history.import"),
    commandId: CommandId,
    threadId: ThreadId,
    messages: Schema.Array(
      Schema.Struct({
        messageId: MessageId,
        role: Schema.Literals(["user", "assistant"]),
        text: Schema.String,
        createdAt: IsoDateTime,
      }),
    ).check(Schema.isNonEmpty()),
  }),
} as const;

/** Historical source qualification is shared by the two importers, never a live-process proof. */
export function qualifyLegacyV1ImportContinuation(input: {
  readonly threadId: ThreadId;
  readonly provenance: "legacy_row" | "native_import";
  readonly sourceRow: ProviderSessionRuntime.ProviderSessionRuntime | null;
  readonly source: "persisted_runtime_row" | "synthetic_import";
  readonly supplied?: ProviderSessionRuntime.LegacyProviderContinuationInputV1;
}): {
  readonly qualification: ProviderContinuationQualification;
  readonly evidence: ProviderSessionRuntime.LegacyProviderContinuationEvidenceV1 | null;
} {
  const unknown = (
    reason: string,
    evidence: ProviderSessionRuntime.LegacyProviderContinuationEvidenceV1 | null = null,
  ) => ({ qualification: { type: "unknown", reason } as const, evidence });
  const row = input.sourceRow;
  if (row === null) return unknown("source_runtime_missing");
  if (row.threadId !== input.threadId) return unknown("source_thread_mismatch");
  const decodedDriver = Schema.decodeUnknownOption(ProviderDriverKind)(row.adapterKey);
  const driver = row.providerName === row.adapterKey ? Option.getOrNull(decodedDriver) : null;
  const nativeThreadId =
    driver === "codex"
      ? Option.getOrNull(
          Option.map(
            Schema.decodeUnknownOption(Schema.Struct({ threadId: TrimmedNonEmptyString }))(
              row.resumeCursor,
            ),
            (cursor) => cursor.threadId,
          ),
        )
      : Option.getOrNull(
          Option.map(
            Schema.decodeUnknownOption(Schema.Struct({ resume: TrimmedNonEmptyString }))(
              row.resumeCursor,
            ),
            (cursor) => cursor.resume,
          ),
        );
  const importedPayload = Schema.decodeUnknownOption(
    Schema.Struct({
      importOrigin: Schema.optional(Schema.String),
      importedTranscripts: Schema.optional(Schema.Array(Schema.Unknown)),
    }),
  )(row.runtimePayload);
  const source =
    Option.isSome(importedPayload) &&
    (importedPayload.value.importOrigin === "native_import" ||
      importedPayload.value.importedTranscripts !== undefined)
      ? "synthetic_import"
      : input.source;
  const stoppedProof = ProviderSessionRuntime.makeLegacyStoppedRuntimeProofV1({
    sourceRow: row,
    source,
    driver,
    nativeThreadId,
  });
  const supplied = input.supplied;
  let evidence: ProviderSessionRuntime.LegacyProviderContinuationEvidenceV1;
  try {
    evidence = ProviderSessionRuntime.makeLegacyProviderContinuationEvidenceV1({
      sourceRow: row,
      provenance: input.provenance,
      driver,
      nativeThreadId,
      continuationKey: supplied?.continuationKey ?? null,
      historicalSourceIdentity: supplied?.historicalSourceIdentity ?? null,
      stoppedProof,
      accessibility: supplied?.accessibility ?? null,
    });
  } catch {
    return unknown("historical_evidence_mismatch");
  }
  if (driver === null) return unknown("source_driver_unproved", evidence);
  if (nativeThreadId === null) return unknown("native_reference_missing", evidence);
  if (
    supplied !== undefined &&
    (supplied.driver !== driver || supplied.nativeThreadId !== nativeThreadId)
  )
    return unknown("historical_native_reference_mismatch", evidence);
  if (supplied === undefined) return unknown("target_identity_missing", evidence);
  if (
    supplied.historicalSourceIdentity === null ||
    supplied.historicalSourceIdentity.sourceHomeIdentity === null
  )
    return unknown("historical_source_identity_unproved", evidence);
  return {
    evidence,
    qualification: qualifyProviderContinuation({
      source: {
        provenance: input.provenance,
        threadId: row.threadId,
        providerInstanceId: row.providerInstanceId,
        driver,
        nativeThreadId,
        continuationKey: evidence.continuationKey,
        status: row.status,
      },
      target: supplied.target,
      ...(evidence.accessibility === null ? {} : { accessibility: evidence.accessibility }),
      ...(stoppedProof === null ? {} : { stoppedProof }),
    }),
  };
}

interface LegacyThreadRow {
  readonly thread_id: string;
  readonly project_id: string;
  readonly title: string;
  readonly model_selection_json: string | null;
  readonly runtime_mode: string;
  readonly interaction_mode: string;
  readonly branch: string | null;
  readonly worktree_path: string | null;
  readonly created_at: string;
  readonly updated_at: string;
  readonly archived_at: string | null;
  readonly settled_override: string | null;
  readonly settled_at: string | null;
  readonly unsettled_at: string | null;
  readonly snoozed_until: string | null;
  readonly snoozed_at: string | null;
  readonly pinned_at: string | null;
  readonly auto_settle_disabled_at: string | null;
  readonly pin_order_key: string | null;
  readonly pull_requests_json: string;
  readonly linked_pull_request_json: string | null;
  readonly branch_pull_request_json: string | null;
  readonly active_order_key: string | null;
  readonly deleted_at: string | null;
}

interface LegacyRepairRow extends LegacyThreadRow {
  readonly payload_json: string;
}

interface LegacyMessageRow {
  readonly message_id: string;
  readonly thread_id: string;
  readonly role: "user" | "assistant";
  readonly text: string;
  readonly attachments_json: string | null;
  readonly context_json?: string | null;
  readonly is_streaming: number;
  readonly created_at: string;
  readonly updated_at: string;
  readonly ordinal: number;
}

interface LegacyImportRow {
  readonly thread_id: string;
  readonly transcript_imported_at: string | null;
}

export interface LegacyV1ImportSummary {
  readonly importedThreadCount: number;
  readonly importedMessageCount: number;
}

export class LegacyV1ThreadImportError extends Schema.TaggedError<LegacyV1ThreadImportError>()(
  "LegacyV1ThreadImportError",
  {
    operation: Schema.String,
    threadId: Schema.optional(ThreadId),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return this.threadId === undefined
      ? `Failed to ${this.operation} legacy v1 threads.`
      : `Failed to ${this.operation} legacy v1 thread ${this.threadId}.`;
  }
}

export interface LegacyV1ThreadImporterShape {
  readonly ensureApplicationAttachmentInventory: (input: {
    readonly threadId: ThreadId;
    readonly expectedBirth: ImportedApplicationAttachmentBirthV1;
  }) => Effect.Effect<ImportedApplicationAttachmentQualificationV1, LegacyV1ThreadImportError>;
  readonly readTranscriptSnapshotEvidence: (
    threadId: ThreadId,
  ) => Effect.Effect<EventSink.LegacyImportTranscriptSnapshotV1 | null, LegacyV1ThreadImportError>;
  readonly pendingThreadCount: Effect.Effect<number, LegacyV1ThreadImportError>;
  readonly reconcileShells: Effect.Effect<LegacyV1ImportSummary, LegacyV1ThreadImportError>;
  readonly ensureTranscript: (
    threadId: ThreadId,
  ) => Effect.Effect<LegacyV1ImportSummary, LegacyV1ThreadImportError>;
  readonly importPendingTranscripts: Effect.Effect<LegacyV1ImportSummary, never>;
}

export class LegacyV1ThreadImporter extends Context.Service<
  LegacyV1ThreadImporter,
  LegacyV1ThreadImporterShape
>()("t3/orchestration-v2/legacy/LegacyV1ThreadImporter") {}

const decodeModelSelection = Schema.decodeUnknownOption(ModelSelection);
const decodeAttachments = Schema.decodeUnknownOption(Schema.Array(ChatAttachment));
const decodePullRequests = Schema.decodeUnknownOption(Schema.Array(ThreadPullRequestLink));
const decodeLinkedPullRequest = Schema.decodeUnknownOption(ThreadLinkedPullRequest);
const decodeStoredThread = Schema.decodeUnknownOption(
  Schema.fromJsonString(OrchestrationV2AppThreadJson),
);

function parseJson(json: string): unknown {
  try {
    return JSON.parse(json);
  } catch {
    return undefined;
  }
}

function modelSelectionFor(row: LegacyThreadRow) {
  const decoded =
    row.model_selection_json === null
      ? Option.none()
      : decodeModelSelection(parseJson(row.model_selection_json));
  return Option.getOrElse(decoded, () => ({
    instanceId: ProviderInstanceId.make("codex"),
    model: DEFAULT_MODEL,
  }));
}

function attachmentsFor(row: LegacyMessageRow) {
  if (row.attachments_json === null) return [];
  return Option.getOrElse(decodeAttachments(parseJson(row.attachments_json)), () => []);
}

function linkedPullRequestFor(row: LegacyThreadRow) {
  if (row.linked_pull_request_json === null) return null;
  return Option.getOrNull(decodeLinkedPullRequest(parseJson(row.linked_pull_request_json)));
}

function branchPullRequestFor(row: LegacyThreadRow) {
  if (row.branch_pull_request_json === null) return null;
  return Option.getOrNull(decodeLinkedPullRequest(parseJson(row.branch_pull_request_json)));
}

function runtimeModeFor(value: string): OrchestrationV2AppThread["runtimeMode"] {
  return value === "approval-required" ||
    value === "auto-accept-edits" ||
    value === "auto" ||
    value === "full-access"
    ? value
    : "full-access";
}

function interactionModeFor(value: string): OrchestrationV2AppThread["interactionMode"] {
  return value === "plan" ? "plan" : "default";
}

function settledOverrideFor(value: string | null): OrchestrationV2AppThread["settledOverride"] {
  return value === "settled" || value === "active" ? value : null;
}

function dateTime(value: string): DateTime.Utc {
  return DateTime.makeUnsafe(value);
}

function nullableDateTime(value: string | null): DateTime.Utc | null {
  return value === null ? null : dateTime(value);
}

function importedThread(row: LegacyThreadRow): OrchestrationV2AppThread {
  const threadId = ThreadId.make(row.thread_id);
  const modelSelection = modelSelectionFor(row);
  const branch = row.branch?.trim() || null;
  const worktreePath = row.worktree_path?.trim() || null;
  const pullRequests = Option.getOrElse(
    decodePullRequests(parseJson(row.pull_requests_json)),
    () => [],
  );
  const linkedPullRequest = linkedPullRequestFor(row);
  const legacyLink = threadPullRequestsOf({ linkedPullRequest })[0];
  const importedPullRequests =
    legacyLink !== undefined &&
    !pullRequests.some((link) => threadPullRequestKeysEqual(link, legacyLink))
      ? [...pullRequests, legacyLink]
      : pullRequests;
  return {
    createdBy: "system",
    creationSource: "server",
    id: threadId,
    projectId: ProjectId.make(row.project_id),
    title: row.title.trim() === "" ? "Untitled thread" : row.title,
    providerInstanceId: modelSelection.instanceId,
    modelSelection,
    runtimeMode: runtimeModeFor(row.runtime_mode),
    interactionMode: interactionModeFor(row.interaction_mode),
    branch,
    worktreePath,
    linkedPullRequest,
    pullRequests: importedPullRequests,
    branchPullRequest: branchPullRequestFor(row),
    activeOrderKey: row.active_order_key?.trim() || null,
    activeProviderThreadId: null,
    historyOrigin: "v1_import",
    lineage: {
      parentThreadId: null,
      relationshipToParent: null,
      rootThreadId: threadId,
    },
    forkedFrom: null,
    createdAt: dateTime(row.created_at),
    updatedAt: dateTime(row.updated_at),
    archivedAt: nullableDateTime(row.archived_at),
    settledOverride: settledOverrideFor(row.settled_override),
    settledAt: nullableDateTime(row.settled_at),
    unsettledAt: nullableDateTime(row.unsettled_at),
    snoozedUntil: nullableDateTime(row.snoozed_until),
    snoozedAt: nullableDateTime(row.snoozed_at),
    pinnedAt: nullableDateTime(row.pinned_at),
    autoSettleDisabledAt: nullableDateTime(row.auto_settle_disabled_at),
    pinOrderKey: row.pin_order_key?.trim() || null,
    lastVisitedAt: null,
    deletedAt: nullableDateTime(row.deleted_at),
  };
}

function messageEvents(row: LegacyMessageRow): ReadonlyArray<OrchestrationV2DomainEvent> {
  const threadId = ThreadId.make(row.thread_id);
  const messageId = MessageId.make(row.message_id);
  const createdAt = dateTime(row.created_at);
  const updatedAt = dateTime(row.updated_at);
  const attachments = attachmentsFor(row);
  const message: OrchestrationV2ConversationMessage = {
    createdBy: row.role === "user" ? "user" : "agent",
    creationSource: "server",
    id: messageId,
    threadId,
    runId: null,
    nodeId: null,
    role: row.role,
    text: row.text,
    ...(row.context_json
      ? {
          context: Schema.decodeUnknownSync(OrchestrationMessageContext)(
            parseJson(row.context_json),
          ),
        }
      : {}),
    attachments,
    streaming: false,
    createdAt,
    updatedAt,
  };
  const baseTurnItem = {
    id: TurnItemId.make(`${IMPORT_EVENT_PREFIX}:turn-item:${row.message_id}`),
    threadId,
    runId: null,
    nodeId: null,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal: row.ordinal,
    status: row.is_streaming === 1 ? ("interrupted" as const) : ("completed" as const),
    title: null,
    startedAt: createdAt,
    completedAt: updatedAt,
    updatedAt,
  };
  const turnItem: OrchestrationV2TurnItem =
    row.role === "user"
      ? {
          ...baseTurnItem,
          createdBy: "user",
          creationSource: "server",
          type: "user_message",
          messageId,
          inputIntent: "turn_start",
          text: row.text,
          ...(row.context_json
            ? {
                context: Schema.decodeUnknownSync(OrchestrationMessageContext)(
                  parseJson(row.context_json),
                ),
              }
            : {}),
          attachments,
        }
      : {
          ...baseTurnItem,
          type: "assistant_message",
          messageId,
          text: row.text,
          ...(row.context_json
            ? {
                context: Schema.decodeUnknownSync(OrchestrationMessageContext)(
                  parseJson(row.context_json),
                ),
              }
            : {}),
          streaming: false,
        };
  return [
    {
      id: EventId.make(`${IMPORT_EVENT_PREFIX}:message:${row.message_id}`),
      type: "message.updated",
      threadId,
      occurredAt: updatedAt,
      payload: message,
    },
    {
      id: EventId.make(`${IMPORT_EVENT_PREFIX}:turn-item:${row.message_id}`),
      type: "turn-item.updated",
      threadId,
      occurredAt: updatedAt,
      payload: turnItem,
    },
  ];
}

function chunks<A>(items: ReadonlyArray<A>, size: number): Array<ReadonlyArray<A>> {
  const result: Array<ReadonlyArray<A>> = [];
  for (let index = 0; index < items.length; index += size) {
    result.push(items.slice(index, index + size));
  }
  return result;
}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const eventSink = yield* EventSink.EventSinkV2;
  const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const transcriptImports = yield* makeKeyedSerialExecutor<ThreadId>();

  const ensureApplicationAttachmentInventory: LegacyV1ThreadImporterShape["ensureApplicationAttachmentInventory"] =
    (input) =>
      eventSink
        .withTransaction(eventSink.prepareImportedApplicationAttachmentInventory(input))
        .pipe(
          Effect.mapError(
            (cause) =>
              new LegacyV1ThreadImportError({
                operation: "prepare application attachment inventory for",
                threadId: input.threadId,
                cause,
              }),
          ),
        );

  const prepareApplicationAttachmentInventoryForThread = (threadId: ThreadId) =>
    Effect.gen(function* () {
      const rows = yield* sql<{
        readonly event_id: string;
        readonly sequence: number;
        readonly payload_json: string;
      }>`
      SELECT event_id, sequence, payload_json FROM orchestration_events
      WHERE application_event_version = 2 AND aggregate_kind = 'thread' AND stream_id = ${threadId}
        AND event_type = 'thread.created'
      ORDER BY sequence DESC LIMIT 1
    `;
      const row = rows[0];
      if (row === undefined) return;
      const thread = decodeStoredThread(row.payload_json);
      if (
        Option.isNone(thread) ||
        thread.value.id !== threadId ||
        thread.value.historyOrigin !== "v1_import"
      )
        return;
      const birth = Schema.decodeUnknownOption(ImportedApplicationAttachmentBirthV1)({
        kind: "application_v2_thread_birth",
        threadId,
        eventId: row.event_id,
        sequence: row.sequence,
      });
      if (Option.isNone(birth)) return;
      yield* ensureApplicationAttachmentInventory({ threadId, expectedBirth: birth.value });
    });

  const prepareApplicationAttachmentInventories = Effect.gen(function* () {
    const rows = yield* sql<{ readonly thread_id: string }>`
      SELECT projection.thread_id FROM orchestration_v2_projection_threads AS projection
      WHERE json_extract(projection.payload_json, '$.historyOrigin') = 'v1_import'
      ORDER BY projection.thread_id
    `;
    for (const row of rows)
      yield* prepareApplicationAttachmentInventoryForThread(ThreadId.make(row.thread_id));
  });

  const listMessages = (threadId: ThreadId) =>
    sql<LegacyMessageRow>`
      SELECT
        message_id,
        thread_id,
        role,
        text,
        attachments_json,
        context_json,
        is_streaming,
        created_at,
        updated_at,
        ROW_NUMBER() OVER (
          PARTITION BY thread_id
          ORDER BY created_at ASC, message_id ASC
        ) AS ordinal
      FROM projection_thread_messages
      WHERE thread_id = ${threadId}
        AND role IN ('user', 'assistant')
      ORDER BY created_at ASC, message_id ASC
    `;

  const readTranscriptSnapshotEvidence: LegacyV1ThreadImporterShape["readTranscriptSnapshotEvidence"] =
    (threadId) =>
      sql
        .withTransaction(
          Effect.gen(function* () {
            const markers = yield* sql<{
              readonly source_updated_at: string;
              readonly transcript_imported_at: string | null;
              readonly imported_message_count: number;
              readonly last_error: string | null;
              readonly updated_at: string;
              readonly project_id: string;
              readonly payload_json: string;
            }>`
        SELECT marker.source_updated_at, marker.transcript_imported_at, marker.imported_message_count,
          marker.last_error, source.updated_at, source.project_id, projection.payload_json
        FROM orchestration_v2_legacy_imports AS marker
        INNER JOIN projection_threads AS source ON source.thread_id = marker.thread_id
        INNER JOIN orchestration_v2_projection_threads AS projection ON projection.thread_id = marker.thread_id
        WHERE marker.thread_id = ${threadId}
      `;
            const marker = markers[0];
            if (
              marker === undefined ||
              marker.transcript_imported_at === null ||
              marker.last_error !== null ||
              marker.source_updated_at !== marker.updated_at ||
              Option.isNone(Schema.decodeUnknownOption(IsoDateTime)(marker.source_updated_at)) ||
              Option.isNone(Schema.decodeUnknownOption(IsoDateTime)(marker.transcript_imported_at))
            )
              return null;
            const projected = decodeStoredThread(marker.payload_json);
            if (
              Option.isNone(projected) ||
              projected.value.id !== threadId ||
              projected.value.projectId !== marker.project_id ||
              projected.value.historyOrigin !== "v1_import"
            )
              return null;
            const messages = yield* listMessages(threadId);
            if (messages.length === 0 || messages.length !== marker.imported_message_count)
              return null;
            const rows = yield* sql<{
              readonly event_id: string;
              readonly sequence: number;
              readonly application_event_version: number;
              readonly aggregate_kind: string;
              readonly event_type: string;
              readonly command_id: string | null;
              readonly occurred_at: string;
              readonly payload_json: string;
            }>`
        SELECT event_id, sequence, application_event_version, aggregate_kind, event_type, command_id, occurred_at, payload_json
        FROM orchestration_events
        WHERE stream_id = ${threadId}
          AND (event_id LIKE ${`${IMPORT_EVENT_PREFIX}:message:%`} OR event_id LIKE ${`${IMPORT_EVENT_PREFIX}:turn-item:%`})
        ORDER BY sequence ASC
      `;
            const positions = yield* sql<{
              readonly turn_item_id: string;
              readonly ordinal: number;
            }>`
        SELECT turn_item_id, ordinal FROM orchestration_v2_turn_item_positions
        WHERE thread_id = ${threadId} AND turn_item_id LIKE ${`${IMPORT_EVENT_PREFIX}:turn-item:%`}
        ORDER BY ordinal ASC
      `;
            if (rows.length !== messages.length * 2 || positions.length !== messages.length)
              return null;
            const validated = yield* Effect.gen(function* () {
              for (const message of messages) {
                if (message.is_streaming !== 0 && message.is_streaming !== 1) return null;
                if (
                  message.attachments_json !== null &&
                  Option.isNone(decodeAttachments(parseJson(message.attachments_json)))
                )
                  return null;
              }
              const expected = yield* Effect.try(() => messages.flatMap(messageEvents));
              const encoded = yield* Effect.forEach(expected, (event) =>
                Schema.encodeEffect(OrchestrationV2DomainEventJson)(event),
              );
              const expectedById = new Map(encoded.map((event) => [event.id, event]));
              if (expectedById.size !== rows.length) return null;
              const actual: unknown[] = [];
              for (const row of rows) {
                const event = expectedById.get(row.event_id);
                if (
                  event === undefined ||
                  row.application_event_version !== 2 ||
                  row.aggregate_kind !== "thread" ||
                  row.event_type !== event.type ||
                  row.command_id !== null ||
                  !Number.isSafeInteger(row.sequence) ||
                  row.sequence < 1 ||
                  row.occurred_at !== event.occurredAt
                )
                  return null;
                const payload = yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown))(
                  row.payload_json,
                );
                if (
                  nativeCreationCanonicalJson(payload) !==
                  nativeCreationCanonicalJson(event.payload)
                )
                  return null;
                actual.push({
                  id: event.id,
                  type: event.type,
                  threadId,
                  occurredAt: event.occurredAt,
                  payload: event.payload,
                });
              }
              for (const [index, message] of messages.entries()) {
                if (
                  positions[index]?.turn_item_id !==
                    `${IMPORT_EVENT_PREFIX}:turn-item:${message.message_id}` ||
                  positions[index]?.ordinal !== message.ordinal
                )
                  return null;
              }
              // Shell previews precede hydration in the event log. Source order is proved by item ordinals.
              return {
                version: 1,
                threadId,
                policy: "legacy_user_assistant_rows_v1",
                sourceUpdatedAt: marker.source_updated_at,
                messageCount: messages.length,
                sourceRowsSha256: nativeCreationSha256(
                  nativeCreationCanonicalJson({
                    threadId,
                    projectId: marker.project_id,
                    sourceUpdatedAt: marker.source_updated_at,
                    messages,
                  }),
                ),
                eventsSha256: nativeCreationSha256(nativeCreationCanonicalJson(actual)),
                eventBasis: rows.map((row) => ({
                  eventId: EventId.make(row.event_id),
                  sequence: row.sequence,
                })),
              } satisfies EventSink.LegacyImportTranscriptSnapshotV1;
            }).pipe(Effect.option);
            return Option.getOrNull(validated) ?? null;
          }),
        )
        .pipe(
          Effect.mapError(
            (cause) =>
              new LegacyV1ThreadImportError({
                operation: "inspect transcript snapshot for",
                threadId,
                cause,
              }),
          ),
        );

  const listShellMessages = (threadId: ThreadId) =>
    Effect.gen(function* () {
      const latest = yield* sql<LegacyMessageRow>`
        SELECT
          message.message_id,
          message.thread_id,
          message.role,
          message.text,
          message.attachments_json,
          message.context_json,
          message.is_streaming,
          message.created_at,
          message.updated_at,
          (
            SELECT COUNT(*)
            FROM projection_thread_messages AS earlier
            WHERE earlier.thread_id = message.thread_id
              AND earlier.role IN ('user', 'assistant')
              AND (
                earlier.created_at < message.created_at
                OR (
                  earlier.created_at = message.created_at
                  AND earlier.message_id <= message.message_id
                )
              )
          ) AS ordinal
        FROM projection_thread_messages AS message
        WHERE message.thread_id = ${threadId}
          AND message.role IN ('user', 'assistant')
        ORDER BY message.created_at DESC, message.message_id DESC
        LIMIT 1
      `;
      const latestUser = yield* sql<LegacyMessageRow>`
        SELECT
          message.message_id,
          message.thread_id,
          message.role,
          message.text,
          message.attachments_json,
          message.context_json,
          message.is_streaming,
          message.created_at,
          message.updated_at,
          (
            SELECT COUNT(*)
            FROM projection_thread_messages AS earlier
            WHERE earlier.thread_id = message.thread_id
              AND earlier.role IN ('user', 'assistant')
              AND (
                earlier.created_at < message.created_at
                OR (
                  earlier.created_at = message.created_at
                  AND earlier.message_id <= message.message_id
                )
              )
          ) AS ordinal
        FROM projection_thread_messages AS message
        WHERE message.thread_id = ${threadId}
          AND message.role = 'user'
        ORDER BY message.created_at DESC, message.message_id DESC
        LIMIT 1
      `;
      return [latestUser[0], latest[0]].filter(
        (message, index, selected): message is LegacyMessageRow =>
          message !== undefined &&
          selected.findIndex((candidate) => candidate?.message_id === message.message_id) === index,
      );
    });

  const reconcileShellsBase = Effect.gen(function* () {
    const now = DateTime.formatIso(yield* DateTime.now);
    const repairRows = yield* sql<LegacyRepairRow>`
      SELECT
        thread.thread_id,
        thread.project_id,
        thread.title,
        thread.model_selection_json,
        thread.runtime_mode,
        thread.interaction_mode,
        thread.branch,
        thread.worktree_path,
        thread.created_at,
        thread.updated_at,
        thread.archived_at,
        thread.settled_override,
        thread.settled_at,
        thread.unsettled_at,
        thread.snoozed_until,
        thread.snoozed_at,
        thread.pinned_at,
        thread.auto_settle_disabled_at,
        thread.pin_order_key,
        (SELECT json_group_array(json_object('host', pr.host, 'repository', pr.repository, 'number', pr.number, 'url', pr.url, 'source', pr.source, 'linkedAt', pr.linked_at, 'snapshot', json(pr.snapshot_json), 'stack', json(pr.stack_json))) FROM projection_thread_pull_requests pr WHERE pr.thread_id = thread.thread_id) AS pull_requests_json,
        thread.linked_pull_request_json,
        thread.branch_pull_request_json,
        thread.active_order_key,
        thread.deleted_at,
        projection.payload_json
      FROM orchestration_v2_legacy_imports AS legacy_import
      INNER JOIN projection_threads AS thread
        ON thread.thread_id = legacy_import.thread_id
      INNER JOIN orchestration_v2_projection_threads AS projection
        ON projection.thread_id = legacy_import.thread_id
      WHERE json_type(projection.payload_json, '$.pinnedAt') IS NULL
         OR json_type(projection.payload_json, '$.pinOrderKey') IS NULL
         OR json_type(projection.payload_json, '$.snoozedUntil') IS NULL
         OR json_type(projection.payload_json, '$.snoozedAt') IS NULL
         OR json_type(projection.payload_json, '$.unsettledAt') IS NULL
         OR json_type(projection.payload_json, '$.linkedPullRequest') IS NULL
         OR json_type(projection.payload_json, '$.pullRequests') IS NULL
         OR json_type(projection.payload_json, '$.branchPullRequest') IS NULL
         OR json_type(projection.payload_json, '$.activeOrderKey') IS NULL
      ORDER BY thread.created_at ASC, thread.thread_id ASC
    `;
    let repairedThreadCount = 0;
    for (const row of repairRows) {
      const decoded = decodeStoredThread(row.payload_json);
      if (Option.isNone(decoded)) continue;
      const current = decoded.value;
      const legacy = importedThread(row);
      const legacyPullRequests = legacy.pullRequests ?? [];
      const repaired: OrchestrationV2AppThread = {
        ...current,
        pinnedAt: current.pinnedAt === undefined ? legacy.pinnedAt : current.pinnedAt,
        autoSettleDisabledAt:
          current.autoSettleDisabledAt === undefined
            ? legacy.autoSettleDisabledAt
            : current.autoSettleDisabledAt,
        pinOrderKey: current.pinOrderKey === undefined ? legacy.pinOrderKey : current.pinOrderKey,
        snoozedUntil:
          current.snoozedUntil === undefined ? legacy.snoozedUntil : current.snoozedUntil,
        snoozedAt: current.snoozedAt === undefined ? legacy.snoozedAt : current.snoozedAt,
        unsettledAt: current.unsettledAt === undefined ? legacy.unsettledAt : current.unsettledAt,
        linkedPullRequest:
          current.linkedPullRequest === undefined
            ? legacy.linkedPullRequest
            : current.linkedPullRequest,
        pullRequests:
          current.pullRequests === undefined
            ? current.linkedPullRequest === null
              ? []
              : legacyPullRequests.length > 0
                ? legacyPullRequests
                : threadPullRequestsOf({
                    linkedPullRequest:
                      current.linkedPullRequest === undefined
                        ? legacy.linkedPullRequest
                        : current.linkedPullRequest,
                  })
            : current.pullRequests,
        branchPullRequest:
          current.branchPullRequest === undefined
            ? legacy.branchPullRequest
            : current.branchPullRequest,
        activeOrderKey:
          current.activeOrderKey === undefined ? legacy.activeOrderKey : current.activeOrderKey,
      };
      // Later schema additions can require another repair for the same thread.
      const repairId = yield* randomUuidV4;
      yield* eventSink.write({
        events: [
          {
            id: EventId.make(
              `${IMPORT_EVENT_PREFIX}:thread:${row.thread_id}:metadata-repair:${repairId}`,
            ),
            type: "thread.metadata-updated",
            threadId: repaired.id,
            providerInstanceId: repaired.providerInstanceId,
            occurredAt: dateTime(now),
            payload: repaired,
          },
        ],
      });
      repairedThreadCount += 1;
    }
    const rows = yield* sql<LegacyThreadRow>`
      SELECT
        thread.thread_id,
        thread.project_id,
        thread.title,
        thread.model_selection_json,
        thread.runtime_mode,
        thread.interaction_mode,
        thread.branch,
        thread.worktree_path,
        thread.created_at,
        thread.updated_at,
        thread.archived_at,
        thread.settled_override,
        thread.settled_at,
        thread.unsettled_at,
        thread.snoozed_until,
        thread.snoozed_at,
        thread.pinned_at,
        thread.auto_settle_disabled_at,
        thread.pin_order_key,
        (SELECT json_group_array(json_object('host', pr.host, 'repository', pr.repository, 'number', pr.number, 'url', pr.url, 'source', pr.source, 'linkedAt', pr.linked_at, 'snapshot', json(pr.snapshot_json), 'stack', json(pr.stack_json))) FROM projection_thread_pull_requests pr WHERE pr.thread_id = thread.thread_id) AS pull_requests_json,
        thread.linked_pull_request_json,
        thread.branch_pull_request_json,
        thread.active_order_key,
        thread.deleted_at
      FROM projection_threads AS thread
      WHERE NOT EXISTS (
        SELECT 1
        FROM orchestration_events AS event INDEXED BY orchestration_events_v2_created_threads_idx
        WHERE event.application_event_version = 2
          AND event.aggregate_kind = 'thread'
          AND event.stream_id = thread.thread_id
          AND event.event_type = 'thread.created'
      )
      ORDER BY thread.created_at ASC, thread.thread_id ASC
    `;
    let importedThreadCount = repairedThreadCount;
    let importedMessageCount = 0;
    for (const row of rows) {
      const thread = importedThread(row);
      const previews = yield* listShellMessages(thread.id);
      const events: Array<OrchestrationV2DomainEvent> = [
        {
          id: EventId.make(`${IMPORT_EVENT_PREFIX}:thread:${row.thread_id}:created`),
          type: "thread.created",
          threadId: thread.id,
          providerInstanceId: thread.providerInstanceId,
          occurredAt: thread.createdAt,
          payload: thread,
        },
        ...previews.flatMap(messageEvents),
        {
          id: EventId.make(`${IMPORT_EVENT_PREFIX}:thread:${row.thread_id}:shell`),
          type: "thread.metadata-updated",
          threadId: thread.id,
          providerInstanceId: thread.providerInstanceId,
          occurredAt: thread.updatedAt,
          payload: thread,
        },
      ];
      yield* eventSink.withTransaction(
        Effect.gen(function* () {
          const sourceRow = Option.getOrNull(
            yield* runtimes.getByThreadId({ threadId: thread.id }),
          );
          const supplied = (yield* ProviderSessionRuntime.LegacyProviderContinuationInputsV1).get(
            thread.id,
          );
          let continuation = qualifyLegacyV1ImportContinuation({
            threadId: thread.id,
            provenance: "legacy_row",
            sourceRow,
            source: "persisted_runtime_row",
            ...(supplied === undefined ? {} : { supplied }),
          });
          if (continuation.qualification.type === "qualified" && supplied !== undefined) {
            if (thread.providerInstanceId !== supplied.target.providerInstanceId) {
              continuation = {
                ...continuation,
                qualification: { type: "unknown", reason: "shell_target_instance_mismatch" },
              };
            } else {
              const providerThreadId = idAllocator.derive.providerThread({
                driver: supplied.target.driver,
                nativeThreadId: continuation.qualification.nativeThreadId,
                providerInstanceId: supplied.target.providerInstanceId,
              });
              const owners = yield* sql<{ readonly thread_id: string | null }>`
                SELECT thread_id FROM orchestration_v2_projection_provider_threads
                WHERE provider_thread_id = ${providerThreadId}
              `;
              if (owners.some((owner) => owner.thread_id !== thread.id)) {
                continuation = {
                  ...continuation,
                  qualification: { type: "unknown", reason: "native_owner_conflict" },
                };
              } else {
                const providerThread: OrchestrationV2ProviderThread = {
                  id: providerThreadId,
                  driver: supplied.target.driver,
                  providerInstanceId: supplied.target.providerInstanceId,
                  providerSessionId: null,
                  appThreadId: thread.id,
                  ownerNodeId: null,
                  nativeThreadRef: {
                    driver: supplied.target.driver,
                    nativeId: continuation.qualification.nativeThreadId,
                    strength: "strong",
                  },
                  nativeConversationHeadRef: null,
                  status: "not_loaded",
                  firstRunOrdinal: null,
                  lastRunOrdinal: null,
                  handoffIds: [],
                  forkedFrom: null,
                  pendingBackgroundTasks: [],
                  createdAt: thread.createdAt,
                  updatedAt: thread.updatedAt,
                };
                const qualifiedThread = { ...thread, activeProviderThreadId: providerThreadId };
                for (const [index, event] of events.entries()) {
                  if (event.type === "thread.created" || event.type === "thread.metadata-updated") {
                    events[index] = { ...event, payload: qualifiedThread };
                  }
                }
                events.push({
                  id: EventId.make(`${IMPORT_EVENT_PREFIX}:provider-thread:${thread.id}`),
                  type: "provider-thread.updated",
                  threadId: thread.id,
                  driver: providerThread.driver,
                  providerInstanceId: providerThread.providerInstanceId,
                  occurredAt: thread.updatedAt,
                  payload: providerThread,
                });
              }
            }
          }
          yield* eventSink.recordLegacyContinuationDisposition({
            threadId: thread.id,
            provenance: "legacy_row",
            ...continuation,
            importedAt: now,
          });
          yield* Effect.forEach(
            previews,
            (message) =>
              sql`
                INSERT INTO orchestration_v2_turn_item_positions (
                  thread_id,
                  turn_item_id,
                  ordinal
                )
                VALUES (
                  ${thread.id},
                  ${TurnItemId.make(`${IMPORT_EVENT_PREFIX}:turn-item:${message.message_id}`)},
                  ${message.ordinal}
                )
                ON CONFLICT(thread_id, turn_item_id) DO NOTHING
              `,
            { discard: true },
          );
          yield* eventSink.write({ events });
          yield* sql`
            INSERT INTO orchestration_v2_legacy_imports (
              thread_id,
              source_updated_at,
              shell_imported_at,
              transcript_imported_at,
              imported_message_count,
              last_error
            )
            VALUES (
              ${thread.id},
              ${row.updated_at},
              ${now},
              NULL,
              ${previews.length},
              NULL
            )
            ON CONFLICT(thread_id) DO NOTHING
          `;
          yield* prepareApplicationAttachmentInventoryForThread(thread.id);
        }),
      );
      importedThreadCount += 1;
      importedMessageCount += previews.length;
    }
    // Inventory adoption is independent of transcript markers: answer-only,
    // empty and already hydrated imports still need their materialized baseline.
    yield* prepareApplicationAttachmentInventories;
    return { importedThreadCount, importedMessageCount };
  });

  const reconcileShells = reconcileShellsBase.pipe(
    Effect.mapError((cause) => new LegacyV1ThreadImportError({ operation: "import", cause })),
  );

  const pendingThreadCount = sql<{ readonly count: number }>`
    SELECT COUNT(*) AS count
    FROM (
      SELECT thread.thread_id
      FROM projection_threads AS thread
      WHERE NOT EXISTS (
        SELECT 1
        FROM orchestration_events AS event INDEXED BY orchestration_events_v2_created_threads_idx
        WHERE event.application_event_version = 2
          AND event.aggregate_kind = 'thread'
          AND event.stream_id = thread.thread_id
          AND event.event_type = 'thread.created'
      )
      UNION
      SELECT legacy_import.thread_id
      FROM orchestration_v2_legacy_imports AS legacy_import
      WHERE legacy_import.transcript_imported_at IS NULL
    )
  `.pipe(
    Effect.map((rows) => rows[0]?.count ?? 0),
    Effect.mapError(
      (cause) => new LegacyV1ThreadImportError({ operation: "inspect pending", cause }),
    ),
  );

  // Threads whose transcript import this process has already confirmed.
  // Only a committed `transcript_imported_at` confirms the cache. A caller's
  // outer import transaction can still roll back after hydration returns.
  // The marker is never reset to NULL, so confirmed answers stay valid;
  // ensureTranscript runs on most
  // thread reads and command dispatches, so skipping the lock + lookup here
  // keeps that path off the database entirely after first confirmation.
  const confirmedTranscriptThreadIds = new Set<ThreadId>();

  const ensureTranscriptBase = (threadId: ThreadId) =>
    transcriptImports.withLock(
      threadId,
      eventSink.withTransaction(
        Effect.gen(function* () {
          const imports = yield* sql<LegacyImportRow>`
          SELECT thread_id, transcript_imported_at
          FROM orchestration_v2_legacy_imports
          WHERE thread_id = ${threadId}
          LIMIT 1
        `;
          const imported = imports[0];
          if (imported === undefined || imported.transcript_imported_at !== null) {
            if (imported !== undefined) {
              yield* prepareApplicationAttachmentInventoryForThread(threadId);
              yield* eventSink.onCommit(
                Effect.sync(() => void confirmedTranscriptThreadIds.add(threadId)),
              );
            }
            return { importedThreadCount: 0, importedMessageCount: 0 };
          }
          const messages = yield* listMessages(threadId);
          const existingRows = yield* sql<{ readonly event_id: string }>`
          SELECT event_id
          FROM orchestration_events
          WHERE application_event_version = 2
            AND aggregate_kind = 'thread'
            AND stream_id = ${threadId}
            AND event_id LIKE ${`${IMPORT_EVENT_PREFIX}:message:%`}
        `;
          const existing = new Set(existingRows.map((row) => row.event_id));
          const missing = messages.filter(
            (message) => !existing.has(`${IMPORT_EVENT_PREFIX}:message:${message.message_id}`),
          );
          for (const batch of chunks(missing, TRANSCRIPT_EVENT_BATCH_SIZE / 2)) {
            yield* Effect.forEach(
              batch,
              (message) =>
                sql`
                INSERT INTO orchestration_v2_turn_item_positions (
                  thread_id,
                  turn_item_id,
                  ordinal
                )
                VALUES (
                  ${threadId},
                  ${TurnItemId.make(`${IMPORT_EVENT_PREFIX}:turn-item:${message.message_id}`)},
                  ${message.ordinal}
                )
                ON CONFLICT(thread_id, turn_item_id) DO NOTHING
              `,
              { discard: true },
            );
            yield* eventSink.write({ events: batch.flatMap(messageEvents) });
            yield* Effect.yieldNow;
          }
          const now = DateTime.formatIso(yield* DateTime.now);
          yield* sql`
          UPDATE orchestration_v2_legacy_imports
          SET
            transcript_imported_at = ${now},
            imported_message_count = ${messages.length},
            last_error = NULL
          WHERE thread_id = ${threadId}
        `;
          yield* prepareApplicationAttachmentInventoryForThread(threadId);
          yield* eventSink.onCommit(
            Effect.sync(() => void confirmedTranscriptThreadIds.add(threadId)),
          );
          return {
            importedThreadCount: 1,
            importedMessageCount: missing.length,
          };
        }),
      ),
    );

  const ensureTranscript = (threadId: ThreadId) =>
    confirmedTranscriptThreadIds.has(threadId)
      ? Effect.succeed({ importedThreadCount: 0, importedMessageCount: 0 })
      : ensureTranscriptBase(threadId).pipe(
          Effect.mapError(
            (cause) =>
              new LegacyV1ThreadImportError({
                operation: "hydrate transcript for",
                threadId,
                cause,
              }),
          ),
        );

  const importPendingTranscripts = Effect.gen(function* () {
    yield* prepareApplicationAttachmentInventories;
    const rows = yield* sql<LegacyImportRow>`
      SELECT thread_id, transcript_imported_at
      FROM orchestration_v2_legacy_imports
      WHERE transcript_imported_at IS NULL
      ORDER BY shell_imported_at ASC, thread_id ASC
    `;
    let importedThreadCount = 0;
    let importedMessageCount = 0;
    for (const row of rows) {
      const result = yield* ensureTranscript(ThreadId.make(row.thread_id)).pipe(
        Effect.tapError((error) =>
          Effect.logWarning("Failed to hydrate migrated v1 thread transcript", {
            threadId: row.thread_id,
            cause: error,
          }),
        ),
        Effect.catch(() =>
          sql`
            UPDATE orchestration_v2_legacy_imports
            SET last_error = 'Transcript hydration failed; retry on next open.'
            WHERE thread_id = ${row.thread_id}
          `.pipe(
            Effect.as({ importedThreadCount: 0, importedMessageCount: 0 }),
            Effect.orElseSucceed(() => ({
              importedThreadCount: 0,
              importedMessageCount: 0,
            })),
          ),
        ),
      );
      importedThreadCount += result.importedThreadCount;
      importedMessageCount += result.importedMessageCount;
      yield* Effect.yieldNow;
    }
    return { importedThreadCount, importedMessageCount };
  }).pipe(
    Effect.catchCause((cause) =>
      Effect.logWarning("Legacy v1 transcript background import stopped", { cause }).pipe(
        Effect.as({ importedThreadCount: 0, importedMessageCount: 0 }),
      ),
    ),
  );

  return LegacyV1ThreadImporter.of({
    ensureApplicationAttachmentInventory,
    readTranscriptSnapshotEvidence,
    pendingThreadCount,
    reconcileShells,
    ensureTranscript,
    importPendingTranscripts,
  });
});

export const layer: Layer.Layer<
  LegacyV1ThreadImporter,
  never,
  EventSink.EventSinkV2 | SqlClient.SqlClient
> = Layer.effect(LegacyV1ThreadImporter, make).pipe(
  Layer.provide(Layer.merge(ProviderSessionRuntime.layer, IdAllocator.layer)),
);

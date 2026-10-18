import {
  type EnvironmentAuthenticatedPrincipal,
  type CommandId,
  type ThreadId,
  EventId,
  NodeId,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2Run,
  OrchestrationV2ProviderThreadJson,
} from "@t3tools/contracts";
import {
  ProviderConversationReselectionInput,
  ReselectProviderConversationInput,
  ProviderConversationReselectionRun,
  ProviderConversationReselectionError,
  type ProviderConversationReselectionReason,
  type PreviewProviderConversationReselectionResult,
  type ReselectProviderConversationResult,
} from "@t3tools/contracts";
import { runRanAfter } from "@t3tools/shared/orchestrationV2ThreadError";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as AuthSessions from "../../persistence/AuthSessions.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as ProviderSessions from "../../orchestration-v2/ProviderSessionManager.ts";
import * as ThreadCommands from "../../orchestration-v2/ThreadCommandExecutor.ts";
import * as CommandReceipts from "../../orchestration-v2/CommandReceiptStore.ts";
import type {
  RecordedLifecycleEvent,
  RecordedStoredLifecycleEvent,
} from "../../orchestration-v2/RecordedTypes.ts";
import { neverStarted } from "../../orchestration-v2/RestartBackgroundNote.ts";
import {
  nativeCreationCanonicalJson,
  nativeCreationSha256,
} from "../nativeCreation/NativeCreationPreparation.ts";

type Principal = EnvironmentAuthenticatedPrincipal["Service"];
export interface ProviderConversationReselectionCommitContext {
  readonly revalidate: (
    envelope: {
      readonly commandId: CommandId;
      readonly threadId: ThreadId;
      readonly commandType: string;
      readonly events: ReadonlyArray<RecordedLifecycleEvent>;
      readonly effects: ReadonlyArray<unknown>;
      readonly cancelUnsettledEffects?: unknown;
    },
    replay?: {
      readonly receipt: CommandReceipts.CommandReceiptV2;
      readonly storedEvents: ReadonlyArray<RecordedStoredLifecycleEvent>;
    },
  ) => Effect.Effect<void, ProviderConversationReselectionError>;
}
export class ProviderConversationReselection extends Context.Service<
  ProviderConversationReselection,
  {
    readonly preview: (
      input: ProviderConversationReselectionInput,
      principal: Principal,
    ) => Effect.Effect<
      PreviewProviderConversationReselectionResult,
      ProviderConversationReselectionError
    >;
    readonly reselect: (
      input: ReselectProviderConversationInput,
      principal: Principal,
    ) => Effect.Effect<ReselectProviderConversationResult, ProviderConversationReselectionError>;
  }
>()("t3/jones/runtime/ProviderConversationReselection") {}

const commandType = "jones.provider-conversation.reselect";
// This versioned derivation is a durable replay contract; changes make earlier requests fail closed.
export const providerConversationReselectionEventId = (
  input: ReselectProviderConversationInput,
): EventId =>
  EventId.make(
    `event:${input.commandId}:provider-conversation-reselect:${nativeCreationSha256(
      nativeCreationCanonicalJson({
        version: 1,
        commandType,
        commandId: input.commandId,
        threadId: input.threadId,
        targetProviderThreadId: input.targetProviderThreadId,
        expectedTargetNativeThreadId: input.expectedTargetNativeThreadId,
        expectedActiveProviderThreadId: input.expectedActiveProviderThreadId,
        expectedPreimageDigest: input.expectedPreimageDigest,
      }),
    )}`,
  );

const decodeRoutingTaskIds = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.Array(NodeId)),
);
const decodeRunMetadata = Schema.decodeUnknownEffect(ProviderConversationReselectionRun);
const encodeProviderRow = Schema.encodeSync(OrchestrationV2ProviderThreadJson);
const isReselectionError = Schema.is(ProviderConversationReselectionError);
const decodePreviewInput = Schema.decodeUnknownEffect(ProviderConversationReselectionInput);
const decodeCommitInput = Schema.decodeUnknownEffect(ReselectProviderConversationInput);

const latestStartedRun = (runs: ReadonlyArray<OrchestrationV2Run>) =>
  runs.reduce<OrchestrationV2Run | undefined>(
    (latest, run) =>
      run.status !== "queued" &&
      run.status !== "rolled_back" &&
      !neverStarted(run) &&
      (latest === undefined || runRanAfter(run, latest))
        ? run
        : latest,
    undefined,
  );
const summarizeRow = (row: OrchestrationV2ProviderThread | undefined) =>
  row === undefined
    ? null
    : {
        id: row.id,
        instance: row.providerInstanceId,
        session: row.providerSessionId,
        status: row.status,
        nativeId: row.nativeThreadRef?.nativeId ?? null,
        firstRunOrdinal: row.firstRunOrdinal,
        lastRunOrdinal: row.lastRunOrdinal,
        hasGeneration: row.runtimeIdentity?.runtimeGeneration !== undefined,
        evidenceRevision: row.runtimeIdentity?.evidenceRevision ?? null,
        requestedInstance: row.runtimeIdentity?.requested.providerInstanceId ?? null,
        pendingBackgroundTaskCount: row.pendingBackgroundTasks?.length ?? 0,
      };

const make = Effect.gen(function* () {
  const sink = yield* EventSink.EventSinkV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const manager = yield* ProviderSessions.ProviderSessionManagerV2;
  const sessions = yield* AuthSessions.AuthSessionRepository;
  const locks = yield* ThreadCommands.ThreadCommandExecutor;
  const sql = yield* SqlClient.SqlClient;
  const receipts = yield* CommandReceipts.CommandReceiptStoreV2;
  const authorize = (principal: Principal) =>
    Effect.gen(function* () {
      const session = yield* sessions.getById({ sessionId: principal.sessionId });
      const now = yield* DateTime.now;
      if (
        Option.isNone(session) ||
        session.value.revokedAt !== null ||
        session.value.subject !== principal.subject ||
        session.value.method !== principal.method ||
        DateTime.toEpochMillis(session.value.expiresAt) <= DateTime.toEpochMillis(now) ||
        (principal.expiresAt !== undefined &&
          DateTime.toEpochMillis(principal.expiresAt) <= DateTime.toEpochMillis(now)) ||
        !["orchestration:operate", "environment:maintain"].every(
          (scope) =>
            principal.scopes.has(scope as "orchestration:operate" | "environment:maintain") &&
            session.value.scopes.includes(
              scope as "orchestration:operate" | "environment:maintain",
            ),
        )
      )
        return yield* new ProviderConversationReselectionError({ reason: "actor_unavailable" });
    }).pipe(
      Effect.mapError(
        () => new ProviderConversationReselectionError({ reason: "actor_unavailable" }),
      ),
    );
  const snapshot = (input: ProviderConversationReselectionInput) =>
    Effect.gen(function* () {
      const shell = yield* projections.getThreadShell(input.threadId);
      const attachment =
        manager.readCurrentThreadRuntimeAttachment === undefined
          ? null
          : yield* manager.readCurrentThreadRuntimeAttachment(input.threadId);
      const refusalCodes: Array<ProviderConversationReselectionReason> = [];
      const thread = shell === null ? null : yield* projections.getThread(input.threadId);
      if (thread === null)
        return {
          target: undefined,
          preview: {
            status: "ineligible" as const,
            refusalCodes: ["thread_unavailable" as const],
            preimageDigest: nativeCreationSha256(
              nativeCreationCanonicalJson({ threadId: input.threadId, thread: null }),
            ),
            thread: null,
            current: null,
            target: null,
            runsSinceTarget: [],
            runtimeAttachment: attachment?.status ?? ("unavailable" as const),
          },
        };
      const records = yield* projections.getThreadRecords(input.threadId, [
        "runs",
        "providerThreads",
        "providerTurns",
        "attempts",
        "runtimeRequests",
        "contextTransfers",
        "contextHandoffs",
      ]);
      const current = records.providerThreads.find(
        (row) => row.id === thread?.activeProviderThreadId,
      );
      const target = records.providerThreads.find((row) => row.id === input.targetProviderThreadId);
      if (thread === null || thread.archivedAt !== null || thread.deletedAt !== null)
        refusalCodes.push("thread_unavailable");
      if (thread?.activeProviderThreadId !== input.expectedActiveProviderThreadId)
        refusalCodes.push("active_pointer_changed");
      if (thread?.activeProviderThreadId === input.targetProviderThreadId)
        refusalCodes.push("target_already_selected");
      if (current === undefined) refusalCodes.push("current_unavailable");
      if (target === undefined) refusalCodes.push("target_unavailable");
      for (const row of [current, target]) {
        if (row === undefined) continue;
        if (row.appThreadId !== input.threadId || row.ownerNodeId !== null)
          refusalCodes.push("row_ownership_conflict");
        if (row.status === "active") refusalCodes.push("row_active");
        if ((row.pendingBackgroundTasks?.length ?? 0) !== 0)
          refusalCodes.push("background_work_pending");
      }
      if (
        target !== undefined &&
        (target.driver !== "codex" ||
          target.nativeThreadRef?.driver !== "codex" ||
          target.nativeThreadRef.nativeId !== input.expectedTargetNativeThreadId ||
          (target.runtimeIdentity !== undefined &&
            (target.runtimeIdentity.requested.providerInstanceId !== target.providerInstanceId ||
              target.runtimeIdentity.requested.providerDriver !== target.driver)))
      )
        refusalCodes.push("target_identity_conflict");
      if (
        target !== undefined &&
        (thread?.providerInstanceId !== target.providerInstanceId ||
          thread?.modelSelection.instanceId !== target.providerInstanceId)
      )
        refusalCodes.push("model_selection_conflict");
      const started = latestStartedRun(records.runs);
      if (started === undefined || started.providerThreadId !== target?.id)
        refusalCodes.push("started_history_conflict");
      const targetStarted = latestStartedRun(
        records.runs.filter((run) => run.providerThreadId === target?.id),
      );
      const since = records.runs
        .filter((run) => targetStarted === undefined || runRanAfter(run, targetStarted))
        .sort((a, b) => (runRanAfter(a, b) ? 1 : -1));
      const pendingStatuses = new Set(["queued", "preparing", "starting", "running", "waiting"]);
      if (records.runs.some((run) => pendingStatuses.has(run.status)))
        refusalCodes.push("run_pending");
      for (const run of since) {
        const attempts = records.attempts.filter((attempt) => attempt.runId === run.id);
        if (
          !neverStarted(run) ||
          records.providerTurns.some((turn) =>
            attempts.some((attempt) => turn.runAttemptId === attempt.id),
          )
        )
          refusalCodes.push("later_run_started");
      }
      if (
        records.runtimeRequests.some(
          (request) => request.status === "pending" && request.responseCapability.type === "live",
        )
      )
        refusalCodes.push("runtime_input_pending");
      if (
        records.contextTransfers.some(
          (transfer) =>
            (transfer.type === "fork" || transfer.type === "merge_back") &&
            transfer.status === "pending",
        )
      )
        refusalCodes.push("context_transfer_pending");
      if (
        records.contextHandoffs.some(
          (handoff) =>
            handoff.toProviderThreadId === input.targetProviderThreadId &&
            (handoff.status === "pending" || handoff.delivery?.status === "pending"),
        )
      )
        refusalCodes.push("handoff_pending");
      const runtimeAttachment = attachment?.status ?? "unavailable";
      if (runtimeAttachment !== "stopped")
        refusalCodes.push(
          runtimeAttachment === "attached"
            ? "runtime_attached"
            : runtimeAttachment === "unknown"
              ? "runtime_unknown"
              : "runtime_unavailable",
        );
      if (attachment?.status === "stopped" && !(yield* attachment.isCurrent))
        refusalCodes.push("runtime_unknown");
      const runsSinceTarget: Array<typeof ProviderConversationReselectionRun.Type> = [];
      for (const run of since) {
        const messages = yield* sql<{
          creation_source: string | null;
          scheduled_task_id: string | null;
          sender_thread_id: string | null;
          task_ids: string | null;
        }>`
        SELECT json_extract(payload_json, '$.creationSource') AS creation_source,
          json_extract(payload_json, '$.scheduledTaskId') AS scheduled_task_id,
          json_extract(payload_json, '$.senderThreadId') AS sender_thread_id,
          json_extract(payload_json, '$.delegatedCompletion.taskIds') AS task_ids
        FROM orchestration_v2_projection_messages WHERE message_id = ${run.userMessageId} AND thread_id = ${input.threadId}`;
        const message = messages[0];
        runsSinceTarget.push(
          yield* decodeRunMetadata({
            runId: run.id,
            ordinal: run.ordinal,
            status: run.status,
            messageId: run.userMessageId,
            creationSource: message?.creation_source ?? null,
            ...(message?.scheduled_task_id == null
              ? {}
              : { scheduledTaskId: message.scheduled_task_id }),
            ...(message?.sender_thread_id == null
              ? {}
              : { senderThreadId: message.sender_thread_id }),
            ...(message?.task_ids == null
              ? {}
              : {
                  delegatedTaskIds: yield* decodeRoutingTaskIds(message.task_ids),
                }),
          }),
        );
      }
      const encodeRow = (row: OrchestrationV2ProviderThread | undefined) =>
        row === undefined ? null : encodeProviderRow(row);
      const maxRunOrdinal = Math.max(0, ...records.runs.map((run) => run.ordinal));
      const preimageDigest = nativeCreationSha256(
        nativeCreationCanonicalJson({
          threadId: input.threadId,
          activeProviderThreadId: thread?.activeProviderThreadId ?? null,
          modelSelection: thread?.modelSelection ?? null,
          providerInstanceId: thread?.providerInstanceId ?? null,
          current: encodeRow(current),
          target: encodeRow(target),
          maxRunOrdinal,
          runsSinceTarget,
        }),
      );
      const preview: PreviewProviderConversationReselectionResult = {
        status: refusalCodes.length === 0 ? "eligible" : "ineligible",
        refusalCodes: [...new Set(refusalCodes)],
        preimageDigest,
        thread:
          thread === null
            ? null
            : {
                activeProviderThreadId: thread.activeProviderThreadId,
                providerInstanceId: thread.providerInstanceId,
                modelSelection: thread.modelSelection,
                maxRunOrdinal,
              },
        current: summarizeRow(current),
        target: summarizeRow(target),
        runsSinceTarget,
        runtimeAttachment,
      };
      return { preview, target };
    }).pipe(
      Effect.mapError((cause) =>
        isReselectionError(cause)
          ? cause
          : new ProviderConversationReselectionError({ reason: "state_unavailable" }),
      ),
    );
  const preview = (candidate: ProviderConversationReselectionInput, principal: Principal) =>
    Effect.gen(function* () {
      const input = yield* decodePreviewInput(candidate, { onExcessProperty: "error" }).pipe(
        Effect.mapError(
          () => new ProviderConversationReselectionError({ reason: "invalid_request" }),
        ),
      );
      yield* authorize(principal);
      return yield* locks.withLock(
        input.threadId,
        sql
          .withTransaction(authorize(principal).pipe(Effect.andThen(snapshot(input))))
          .pipe(Effect.map((state) => state.preview)),
      );
    }).pipe(
      Effect.mapError((cause) =>
        isReselectionError(cause)
          ? cause
          : new ProviderConversationReselectionError({ reason: "state_unavailable" }),
      ),
    );
  const reselect = (candidate: ReselectProviderConversationInput, principal: Principal) =>
    Effect.gen(function* () {
      const input = yield* decodeCommitInput(candidate, { onExcessProperty: "error" }).pipe(
        Effect.mapError(
          () => new ProviderConversationReselectionError({ reason: "invalid_request" }),
        ),
      );
      yield* authorize(principal);
      return yield* locks.withLock(
        input.threadId,
        Effect.gen(function* () {
          if (
            Option.isSome(
              yield* receipts
                .getProjectByCommandId(input.commandId)
                .pipe(
                  Effect.mapError(
                    () => new ProviderConversationReselectionError({ reason: "state_unavailable" }),
                  ),
                ),
            )
          )
            return yield* new ProviderConversationReselectionError({
              reason: "command_identity_conflict",
            });
          const initial = yield* snapshot(input);
          const now = yield* DateTime.now;
          const eventId = providerConversationReselectionEventId(input);
          const revalidate: ProviderConversationReselectionCommitContext["revalidate"] = (
            envelope,
            replay,
          ) =>
            Effect.gen(function* () {
              yield* authorize(principal);
              if (
                envelope.commandId !== input.commandId ||
                envelope.threadId !== input.threadId ||
                envelope.commandType !== commandType ||
                envelope.effects.length !== 0 ||
                envelope.cancelUnsettledEffects !== undefined
              )
                return yield* new ProviderConversationReselectionError({
                  reason: "command_identity_conflict",
                });
              if (replay !== undefined) {
                const event = replay.storedEvents[0]?.event;
                if (
                  replay.receipt.threadId !== input.threadId ||
                  replay.receipt.commandType !== commandType ||
                  replay.receipt.status !== "accepted" ||
                  replay.storedEvents.length !== 1 ||
                  event?.id !== eventId ||
                  event.type !== "provider-thread.updated" ||
                  event.payload.id !== input.targetProviderThreadId ||
                  event.payload.nativeThreadRef?.nativeId !== input.expectedTargetNativeThreadId
                )
                  return yield* new ProviderConversationReselectionError({
                    reason: "command_identity_conflict",
                  });
                return;
              }
              const event = envelope.events[0];
              if (
                envelope.events.length !== 1 ||
                event?.id !== eventId ||
                event.type !== "provider-thread.updated" ||
                event.threadId !== input.threadId ||
                event.payload.id !== input.targetProviderThreadId ||
                nativeCreationCanonicalJson(event.payload) !==
                  nativeCreationCanonicalJson({ ...initial.target, updatedAt: now })
              )
                return yield* new ProviderConversationReselectionError({
                  reason: "command_identity_conflict",
                });
              const current = yield* snapshot(input);
              if (
                current.preview.preimageDigest !== input.expectedPreimageDigest ||
                initial.preview.preimageDigest !== input.expectedPreimageDigest
              )
                return yield* new ProviderConversationReselectionError({
                  reason: "preimage_changed",
                });
              const refusal = current.preview.refusalCodes[0];
              if (refusal !== undefined)
                return yield* new ProviderConversationReselectionError({ reason: refusal });
            });
          let rejection: ProviderConversationReselectionError | undefined;
          const context: ProviderConversationReselectionCommitContext = {
            revalidate: (envelope, replay) =>
              revalidate(envelope, replay).pipe(
                Effect.tapError((error) =>
                  Effect.sync(() => {
                    rejection = error;
                  }),
                ),
              ),
          };
          // A missing target can only reach replay verification; a new reservation is rejected in revalidate.
          const events =
            initial.target === undefined
              ? []
              : [
                  {
                    id: eventId,
                    type: "provider-thread.updated" as const,
                    threadId: input.threadId,
                    driver: initial.target.driver,
                    providerInstanceId: initial.target.providerInstanceId,
                    occurredAt: now,
                    payload: { ...initial.target, updatedAt: now },
                  },
                ];
          const result = yield* sink
            .commitCommand({
              commandId: input.commandId,
              threadId: input.threadId,
              commandType,
              acceptedAt: now,
              events,
              effects: [],
              providerConversationReselection: context,
            })
            .pipe(
              Effect.mapError(
                () =>
                  rejection ??
                  new ProviderConversationReselectionError({ reason: "commit_unconfirmed" }),
              ),
            );
          const thread = yield* projections
            .getThread(input.threadId)
            .pipe(
              Effect.mapError(
                () => new ProviderConversationReselectionError({ reason: "commit_unconfirmed" }),
              ),
            );
          return {
            commandId: input.commandId,
            threadId: input.threadId,
            status: result.committed ? ("committed" as const) : ("already_committed" as const),
            resultSequence: result.receipt.resultSequence,
            previousActiveProviderThreadId: input.expectedActiveProviderThreadId,
            activeProviderThreadId: thread.activeProviderThreadId,
          };
        }),
      );
    });
  return ProviderConversationReselection.of({ preview, reselect });
});
export const layer = Layer.effect(ProviderConversationReselection, make);

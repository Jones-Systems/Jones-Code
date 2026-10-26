import {
  CommandId,
  MessageId,
  ModelSelection,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import { makeJonesTablePresence } from "../persistence/JonesTablePresence.ts";

const Decision = Schema.Literals([
  "switched",
  "exhausted",
  "stale",
  "not_failed",
  "local_failure",
  "not_latest",
  "not_codex",
  "selection_moved",
  "provider_thread_changed",
  "no_native_ref",
  "stopped",
  "pending_request",
  "inactive_thread",
  "snoozed",
  "worktree_missing",
  "admission_error",
]);
export type CodexFailoverDecision = typeof Decision.Type;
const StepStatus = Schema.Literals(["switched", "continued", "abandoned"]);
export type CodexFailoverStepStatus = typeof StepStatus.Type;
const Failure = Schema.Literals(["usage_limit", "unavailable"]);
const SelectionJson = Schema.fromJsonString(ModelSelection);
const EvidenceRecord = Schema.Struct({
  providerTurnId: ProviderTurnId,
  threadId: ThreadId,
  runId: RunId,
  attemptId: RunAttemptId,
  providerThreadId: ProviderThreadId,
  providerInstanceId: ProviderInstanceId,
  disposition: Failure,
  occurredAt: Schema.String,
  state: Schema.Literals(["recorded", "deferred", "decided"]),
  decision: Schema.NullOr(Decision),
  updatedAt: Schema.NullOr(Schema.String),
});
export type CodexFailoverEvidenceRecord = typeof EvidenceRecord.Type;
const Step = Schema.Struct({
  switchCommandId: CommandId,
  threadId: ThreadId,
  chainRootRunId: RunId,
  originRunId: RunId,
  originProviderTurnId: ProviderTurnId,
  sourceInstanceId: ProviderInstanceId,
  sourceFailure: Failure,
  sourceResetAt: Schema.NullOr(Schema.String),
  targetInstanceId: ProviderInstanceId,
  savedSelection: SelectionJson,
  continuationSelection: SelectionJson,
  continueCommandId: CommandId,
  continueMessageId: MessageId,
  status: StepStatus,
  createdAt: Schema.String,
  updatedAt: Schema.String,
});
export type CodexFailoverStep = typeof Step.Type;
const QueuedRun = Schema.Struct({
  runId: RunId,
  threadId: ThreadId,
  inheritedAt: Schema.String,
  overrideSelection: Schema.NullOr(SelectionJson),
  overrideSwitchCommandId: Schema.NullOr(CommandId),
  updatedAt: Schema.String,
});
export type CodexFailoverQueuedRun = typeof QueuedRun.Type;
export type CodexFailoverEvidenceMark =
  | { readonly state: "deferred"; readonly updatedAt: string }
  | {
      readonly state: "decided";
      readonly decision: CodexFailoverDecision;
      readonly updatedAt: string;
    };
export interface CodexFailoverQueuedOverride {
  readonly runId: RunId;
  readonly threadId: ThreadId;
  readonly selection: ModelSelection;
  readonly switchCommandId: CommandId;
  readonly updatedAt: string;
}
export class CodexFailoverStoreError extends Schema.TaggedError<CodexFailoverStoreError>()(
  "CodexFailoverStoreError",
  { cause: Schema.Defect() },
) {
  override get message(): string {
    return "Codex failover storage is unavailable.";
  }
}
type Result<A> = Effect.Effect<A, CodexFailoverStoreError>;
export class CodexFailoverStore extends Context.Service<
  CodexFailoverStore,
  {
    readonly getEvidence: (id: ProviderTurnId) => Result<CodexFailoverEvidenceRecord | null>;
    readonly markEvidence: (id: ProviderTurnId, mark: CodexFailoverEvidenceMark) => Result<boolean>;
    readonly listOpenEvidence: () => Result<ReadonlyArray<CodexFailoverEvidenceRecord>>;
    readonly listOpenEvidenceForThread: (
      id: ThreadId,
    ) => Result<ReadonlyArray<CodexFailoverEvidenceRecord>>;
    readonly pruneDecidedEvidence: (before: string) => Result<void>;
    readonly insertStep: (step: CodexFailoverStep) => Result<boolean>;
    readonly getStepByOrigin: (id: RunId) => Result<CodexFailoverStep | null>;
    readonly getStepByContinueCommand: (id: CommandId) => Result<CodexFailoverStep | null>;
    readonly getStepByContinueMessage: (id: MessageId) => Result<CodexFailoverStep | null>;
    readonly listChainSteps: (id: RunId) => Result<ReadonlyArray<CodexFailoverStep>>;
    readonly listUnexpiredUsageResets: (
      id: ThreadId,
      now: string,
    ) => Result<ReadonlyArray<CodexFailoverStep>>;
    readonly setStepStatus: (
      id: CommandId,
      status: CodexFailoverStepStatus,
      updatedAt: string,
    ) => Result<boolean>;
    readonly listSwitchedSteps: () => Result<ReadonlyArray<CodexFailoverStep>>;
    readonly insertInheritedQueuedRun: (input: {
      readonly runId: RunId;
      readonly threadId: ThreadId;
      readonly inheritedAt: string;
    }) => Result<boolean>;
    readonly listInheritedQueuedRuns: (
      id?: ThreadId,
    ) => Result<ReadonlyArray<CodexFailoverQueuedRun>>;
    readonly setQueuedOverride: (input: CodexFailoverQueuedOverride) => Result<boolean>;
    readonly getQueuedOverride: (id: RunId) => Result<ModelSelection | null>;
    readonly deleteQueuedRuns: (ids: ReadonlyArray<RunId>) => Result<void>;
    readonly selectionRevision: (id: ThreadId) => Result<number>;
  }
>()("t3/jones/codexFailover/CodexFailoverStore") {}

const decodeEvidence = Schema.decodeUnknownEffect(Schema.Array(EvidenceRecord));
const decodeSteps = Schema.decodeUnknownEffect(Schema.Array(Step));
const decodeQueued = Schema.decodeUnknownEffect(Schema.Array(QueuedRun));

const evidenceSelect = `SELECT e.provider_turn_id AS providerTurnId, e.thread_id AS threadId,
  e.run_id AS runId, e.attempt_id AS attemptId, e.provider_thread_id AS providerThreadId,
  e.provider_instance_id AS providerInstanceId, e.disposition, e.occurred_at AS occurredAt,
  COALESCE(d.state, 'recorded') AS state, d.decision, d.updated_at AS updatedAt
  FROM jones_codex_failover_evidence e LEFT JOIN jones_codex_failover_decisions d
  ON d.provider_turn_id = e.provider_turn_id AND d.thread_id = e.thread_id`;
const stepSelect = `SELECT switch_command_id AS switchCommandId, thread_id AS threadId,
  chain_root_run_id AS chainRootRunId, origin_run_id AS originRunId,
  origin_provider_turn_id AS originProviderTurnId, source_instance_id AS sourceInstanceId,
  source_failure AS sourceFailure, source_reset_at AS sourceResetAt, target_instance_id AS targetInstanceId,
  saved_selection_json AS savedSelection, continuation_selection_json AS continuationSelection,
  continue_command_id AS continueCommandId, continue_message_id AS continueMessageId,
  status, created_at AS createdAt, updated_at AS updatedAt FROM jones_codex_failover_steps`;
const queuedSelect = `SELECT run_id AS runId, thread_id AS threadId, inherited_at AS inheritedAt,
  override_selection_json AS overrideSelection, override_switch_command_id AS overrideSwitchCommandId,
  updated_at AS updatedAt FROM jones_codex_failover_queued_runs`;
const storageError = (cause: unknown) => new CodexFailoverStoreError({ cause });
const encodeSelection = Schema.encodeSync(SelectionJson);

/** Uses the caller's SQL context, including an enclosing EventSink transaction. */
export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const present = makeJonesTablePresence(sql);
  const evidencePresent = Effect.gen(function* () {
    return (
      (yield* present("jones_codex_failover_evidence")) &&
      (yield* present("jones_codex_failover_decisions"))
    );
  });
  const readEvidence = Effect.fn("CodexFailoverStore.readEvidence")(function* (
    where: string,
    params: ReadonlyArray<string>,
  ) {
    if (!(yield* evidencePresent)) return [];
    return yield* decodeEvidence(
      yield* sql.unsafe(
        `${evidenceSelect} ${where} ORDER BY e.occurred_at, e.provider_turn_id`,
        params,
      ),
    );
  });
  const getEvidence = Effect.fn("CodexFailoverStore.getEvidence")(function* (id: ProviderTurnId) {
    return (yield* readEvidence("WHERE e.provider_turn_id = ?", [id]))[0] ?? null;
  }, Effect.mapError(storageError));
  const listOpenEvidence = Effect.fn("CodexFailoverStore.listOpenEvidence")(function* () {
    return yield* readEvidence("WHERE d.state IS NULL OR d.state <> 'decided'", []);
  }, Effect.mapError(storageError));
  const listOpenEvidenceForThread = Effect.fn("CodexFailoverStore.listOpenEvidenceForThread")(
    function* (id: ThreadId) {
      return yield* readEvidence(
        "WHERE e.thread_id = ? AND (d.state IS NULL OR d.state <> 'decided')",
        [id],
      );
    },
    Effect.mapError(storageError),
  );
  const markEvidence = Effect.fn("CodexFailoverStore.markEvidence")(function* (
    id: ProviderTurnId,
    mark: CodexFailoverEvidenceMark,
  ) {
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        const evidence = yield* getEvidence(id);
        if (!evidence) return false;
        const owners = yield* sql`SELECT 1 FROM orchestration_v2_projection_runs
        WHERE run_id = ${evidence.runId} AND thread_id = ${evidence.threadId}`;
        if (!owners.length) return false;
        const rows = yield* sql`INSERT INTO jones_codex_failover_decisions
        (provider_turn_id, thread_id, state, decision, updated_at)
        VALUES (${id}, ${evidence.threadId}, ${mark.state}, ${mark.state === "decided" ? mark.decision : null}, ${mark.updatedAt})
        ON CONFLICT(provider_turn_id) DO UPDATE SET state = excluded.state,
          decision = excluded.decision, updated_at = excluded.updated_at
        WHERE thread_id = excluded.thread_id RETURNING provider_turn_id`;
        return rows.length > 0;
      }),
    );
  }, Effect.mapError(storageError));
  const pruneDecidedEvidence = Effect.fn("CodexFailoverStore.pruneDecidedEvidence")(function* (
    before: string,
  ) {
    yield* sql.withTransaction(
      Effect.gen(function* () {
        if (!(yield* evidencePresent)) return;
        const rows = yield* sql<{ provider_turn_id: string }>`SELECT e.provider_turn_id
        FROM jones_codex_failover_evidence e JOIN jones_codex_failover_decisions d
        ON d.provider_turn_id = e.provider_turn_id AND d.thread_id = e.thread_id
        WHERE e.occurred_at < ${before} AND d.state = 'decided'`;
        for (const row of rows) {
          yield* sql`DELETE FROM jones_codex_failover_decisions WHERE provider_turn_id = ${row.provider_turn_id}`;
          yield* sql`DELETE FROM jones_codex_failover_evidence WHERE provider_turn_id = ${row.provider_turn_id}`;
        }
      }),
    );
  }, Effect.mapError(storageError));
  const readSteps = Effect.fn("CodexFailoverStore.readSteps")(function* (
    where: string,
    params: ReadonlyArray<string>,
  ) {
    if (!(yield* present("jones_codex_failover_steps"))) return [];
    return yield* decodeSteps(
      yield* sql.unsafe(`${stepSelect} ${where} ORDER BY created_at, switch_command_id`, params),
    );
  });
  const insertStep = Effect.fn("CodexFailoverStore.insertStep")(function* (
    step: CodexFailoverStep,
  ) {
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        if (!(yield* present("jones_codex_failover_steps"))) return false;
        const evidence = yield* getEvidence(step.originProviderTurnId);
        if (
          !evidence ||
          evidence.threadId !== step.threadId ||
          evidence.runId !== step.originRunId ||
          evidence.providerInstanceId !== step.sourceInstanceId ||
          evidence.disposition !== step.sourceFailure
        )
          return false;
        for (const runId of [step.originRunId, step.chainRootRunId]) {
          const owners = yield* sql`SELECT 1 FROM orchestration_v2_projection_runs
          WHERE run_id = ${runId} AND thread_id = ${step.threadId}`;
          if (!owners.length) return false;
        }
        const rows = yield* sql`INSERT INTO jones_codex_failover_steps
        (switch_command_id, thread_id, chain_root_run_id, origin_run_id, origin_provider_turn_id,
         source_instance_id, source_failure, source_reset_at, target_instance_id, saved_selection_json,
         continuation_selection_json, continue_command_id, continue_message_id, status, created_at, updated_at)
        VALUES (${step.switchCommandId}, ${step.threadId}, ${step.chainRootRunId}, ${step.originRunId},
         ${step.originProviderTurnId}, ${step.sourceInstanceId}, ${step.sourceFailure}, ${step.sourceResetAt},
         ${step.targetInstanceId}, ${encodeSelection(step.savedSelection)}, ${encodeSelection(step.continuationSelection)},
         ${step.continueCommandId}, ${step.continueMessageId}, ${step.status}, ${step.createdAt}, ${step.updatedAt})
        ON CONFLICT DO NOTHING RETURNING switch_command_id`;
        return rows.length > 0;
      }),
    );
  }, Effect.mapError(storageError));
  const getStepByOrigin = Effect.fn("CodexFailoverStore.getStepByOrigin")(function* (id: RunId) {
    return (yield* readSteps("WHERE origin_run_id = ?", [id]))[0] ?? null;
  }, Effect.mapError(storageError));
  const getStepByContinueCommand = Effect.fn("CodexFailoverStore.getStepByContinueCommand")(
    function* (id: CommandId) {
      return (yield* readSteps("WHERE continue_command_id = ?", [id]))[0] ?? null;
    },
    Effect.mapError(storageError),
  );
  const getStepByContinueMessage = Effect.fn("CodexFailoverStore.getStepByContinueMessage")(
    function* (id: MessageId) {
      return (yield* readSteps("WHERE continue_message_id = ?", [id]))[0] ?? null;
    },
    Effect.mapError(storageError),
  );
  const listChainSteps = Effect.fn("CodexFailoverStore.listChainSteps")(function* (id: RunId) {
    return yield* readSteps("WHERE chain_root_run_id = ?", [id]);
  }, Effect.mapError(storageError));
  const listUnexpiredUsageResets = Effect.fn("CodexFailoverStore.listUnexpiredUsageResets")(
    function* (id: ThreadId, now: string) {
      return yield* readSteps(
        "WHERE thread_id = ? AND source_failure = 'usage_limit' AND source_reset_at IS NOT NULL AND source_reset_at > ?",
        [id, now],
      );
    },
    Effect.mapError(storageError),
  );
  const listSwitchedSteps = Effect.fn("CodexFailoverStore.listSwitchedSteps")(function* () {
    return yield* readSteps("WHERE status = 'switched'", []);
  }, Effect.mapError(storageError));
  const setStepStatus = Effect.fn("CodexFailoverStore.setStepStatus")(function* (
    id: CommandId,
    status: CodexFailoverStepStatus,
    updatedAt: string,
  ) {
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        if (!(yield* present("jones_codex_failover_steps"))) return false;
        const steps = yield* readSteps("WHERE switch_command_id = ?", [id]);
        if (!steps[0]) return false;
        const owners = yield* sql`SELECT 1 FROM orchestration_v2_projection_runs
        WHERE run_id = ${steps[0].originRunId} AND thread_id = ${steps[0].threadId}`;
        if (!owners.length) return false;
        const rows =
          yield* sql`UPDATE jones_codex_failover_steps SET status = ${status}, updated_at = ${updatedAt}
        WHERE switch_command_id = ${id} RETURNING switch_command_id`;
        return rows.length > 0;
      }),
    );
  }, Effect.mapError(storageError));
  const readQueued = Effect.fn("CodexFailoverStore.readQueued")(function* (
    where: string,
    params: ReadonlyArray<string>,
  ) {
    if (!(yield* present("jones_codex_failover_queued_runs"))) return [];
    return yield* decodeQueued(
      yield* sql.unsafe(`${queuedSelect} ${where} ORDER BY inherited_at, run_id`, params),
    );
  });
  const insertInheritedQueuedRun = Effect.fn("CodexFailoverStore.insertInheritedQueuedRun")(
    function* (input: {
      readonly runId: RunId;
      readonly threadId: ThreadId;
      readonly inheritedAt: string;
    }) {
      return yield* sql.withTransaction(
        Effect.gen(function* () {
          if (!(yield* present("jones_codex_failover_queued_runs"))) return false;
          const owners = yield* sql`SELECT 1 FROM orchestration_v2_projection_runs
        WHERE run_id = ${input.runId} AND thread_id = ${input.threadId}`;
          if (!owners.length) return false;
          const rows = yield* sql`INSERT INTO jones_codex_failover_queued_runs
        (run_id, thread_id, inherited_at, updated_at)
        VALUES (${input.runId}, ${input.threadId}, ${input.inheritedAt}, ${input.inheritedAt})
        ON CONFLICT(run_id) DO NOTHING RETURNING run_id`;
          return rows.length > 0;
        }),
      );
    },
    Effect.mapError(storageError),
  );
  const listInheritedQueuedRuns = Effect.fn("CodexFailoverStore.listInheritedQueuedRuns")(
    function* (id?: ThreadId) {
      return yield* readQueued(
        id === undefined ? "" : "WHERE thread_id = ?",
        id === undefined ? [] : [id],
      );
    },
    Effect.mapError(storageError),
  );
  const setQueuedOverride = Effect.fn("CodexFailoverStore.setQueuedOverride")(function* (
    input: CodexFailoverQueuedOverride,
  ) {
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        if (
          !(yield* present("jones_codex_failover_queued_runs")) ||
          !(yield* present("jones_codex_failover_steps"))
        )
          return false;
        const owners = yield* sql`SELECT 1 FROM orchestration_v2_projection_runs
        WHERE run_id = ${input.runId} AND thread_id = ${input.threadId}`;
        const steps = yield* sql`SELECT 1 FROM jones_codex_failover_steps
        WHERE switch_command_id = ${input.switchCommandId} AND thread_id = ${input.threadId}`;
        if (!owners.length || !steps.length) return false;
        const rows = yield* sql`UPDATE jones_codex_failover_queued_runs
        SET override_selection_json = ${encodeSelection(input.selection)},
          override_switch_command_id = ${input.switchCommandId}, updated_at = ${input.updatedAt}
        WHERE run_id = ${input.runId} AND thread_id = ${input.threadId} RETURNING run_id`;
        return rows.length > 0;
      }),
    );
  }, Effect.mapError(storageError));
  const getQueuedOverride = Effect.fn("CodexFailoverStore.getQueuedOverride")(function* (
    id: RunId,
  ) {
    return (yield* readQueued("WHERE run_id = ?", [id]))[0]?.overrideSelection ?? null;
  }, Effect.mapError(storageError));
  const deleteQueuedRuns = Effect.fn("CodexFailoverStore.deleteQueuedRuns")(function* (
    ids: ReadonlyArray<RunId>,
  ) {
    yield* sql.withTransaction(
      Effect.gen(function* () {
        if (!(yield* present("jones_codex_failover_queued_runs"))) return;
        for (const id of ids)
          yield* sql`DELETE FROM jones_codex_failover_queued_runs WHERE run_id = ${id}`;
      }),
    );
  }, Effect.mapError(storageError));
  const selectionRevision = Effect.fn("CodexFailoverStore.selectionRevision")(function* (
    threadId: ThreadId,
  ) {
    // Keep parity with ThreadSelfConfiguration's selection revision query.
    const rows = yield* sql<{ revision: number }>`
      SELECT COALESCE(MAX(sequence), 0) AS revision FROM orchestration_events
      WHERE application_event_version = 2 AND aggregate_kind = 'thread' AND stream_id = ${threadId}
        AND event_type IN ('thread.created', 'thread.model-selection-updated', 'thread.provider-switched')
    `;
    return rows[0]?.revision ?? 0;
  }, Effect.mapError(storageError));
  return CodexFailoverStore.of({
    getEvidence,
    markEvidence,
    listOpenEvidence,
    listOpenEvidenceForThread,
    pruneDecidedEvidence,
    insertStep,
    getStepByOrigin,
    getStepByContinueCommand,
    getStepByContinueMessage,
    listChainSteps,
    listUnexpiredUsageResets,
    setStepStatus,
    listSwitchedSteps,
    insertInheritedQueuedRun,
    listInheritedQueuedRuns,
    setQueuedOverride,
    getQueuedOverride,
    deleteQueuedRuns,
    selectionRevision,
  });
});
export const layer = Layer.effect(CodexFailoverStore, make);

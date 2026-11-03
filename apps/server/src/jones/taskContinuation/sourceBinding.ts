import {
  OrchestratorMcpFailure,
  type ModelSelection,
  type NodeId,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2Run,
  type OrchestrationV2RunAttempt,
  type OrchestrationV2Subagent,
  type OrchestrationV2ThreadProjection,
  type ProviderInstanceId,
  type ProviderThreadId,
  type RunId,
} from "@t3tools/contracts";
import { modelSelectionsEqual } from "@t3tools/shared/model";
import { derivePendingBackgroundWork } from "@t3tools/shared/orchestrationV2PendingBackgroundWork";
import * as Schema from "effect/Schema";

export class TaskContinuationError extends Schema.TaggedError<TaskContinuationError>()(
  "TaskContinuationError",
  {
    reason: Schema.Literals([
      "task_not_found",
      "source_not_completed",
      "driver_unsupported",
      "child_busy",
      "lineage_changed",
      "native_source_unavailable",
      "source_link_unavailable",
      "native_source_uncaptured",
      "native_source_conflict",
      "native_session_mismatch",
      "native_reference_moved",
      "selection_changed",
      "request_conflict",
      "acceptance_unknown",
      "refused_key",
      "guard_missing",
      "ancestry_invalid",
      "witness_conflict",
      "storage_unavailable",
      "parent_not_active",
      "thread_not_sendable",
    ]),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `[${this.reason}] ${continuationHints[this.reason]}`;
  }
}

const continuationHints: Record<TaskContinuationError["reason"], string> = {
  task_not_found:
    "The source must be a local app-owned task of this parent. Check the source taskId with task_status.",
  source_not_completed:
    "The source task has no published completed result. Wait for task_status to report completed.",
  driver_unsupported: "Native task continuation currently supports Claude tasks only.",
  child_busy:
    "The child has active, queued, nested or pending work. Retry when it is idle, or cancel that work first.",
  lineage_changed:
    "The child no longer has its recorded parent/task lineage. Reconcile the source task before continuing.",
  source_link_unavailable:
    "The source run lacks an unambiguous recorded task/spawn link. Inspect the source child's recorded spawn history; no turn was started.",
  native_source_unavailable:
    "The source has no completed provider-accepted attempt. Inspect task_status and reconcile its provider outcome before retrying.",
  native_source_uncaptured:
    "An accepted source attempt has no captured Claude session ID. Today's provider row cannot recover that identity; reconcile the original source evidence before continuing.",
  native_source_conflict:
    "The recorded source lineage or accepted session identities conflict. Reconcile the source evidence; no replacement conversation was started.",
  native_session_mismatch:
    "expectedNativeSessionId differs from the captured source ID. Inspect task_status.nativeSession and correct the expected ID before retrying.",
  native_reference_moved:
    "The child's active provider row or native session no longer matches the captured source. Inspect task_status.nativeSession and reconcile the changed binding.",
  selection_changed:
    "The child's provider instance, model or options changed. Restore the source selection before retrying.",
  request_conflict:
    "This clientRequestId already identifies a different request. Retry its original body, or use a new key for a different request.",
  acceptance_unknown:
    "Continuation acceptance is not yet reconciled. Retry the identical request and clientRequestId; do not start a replacement task.",
  refused_key:
    "This clientRequestId was already refused. Reconcile the refusal before submitting a new request key.",
  guard_missing:
    "The durable continuation guard is unavailable. Restore the source server's continuation records before retrying.",
  ancestry_invalid:
    "The recorded restart ancestry is missing, cyclic or conflicts with its source. Reconcile that history before continuing.",
  witness_conflict:
    "Native session confirmation conflicts with the accepted continuation. Inspect the continued task; do not start a replacement conversation.",
  storage_unavailable:
    "Continuation storage is unavailable. Restore server storage, then retry the identical request and clientRequestId.",
  parent_not_active:
    "The parent is no longer an active caller. Resume the owning parent before continuing its task.",
  thread_not_sendable:
    "The child is archived, deleted or blocked from messages. Restore its sendable state before retrying.",
};

export const toMcpFailure = (error: TaskContinuationError): OrchestratorMcpFailure =>
  new OrchestratorMcpFailure({
    code:
      error.reason === "task_not_found"
        ? "task_not_found"
        : error.reason === "request_conflict" || error.reason === "refused_key"
          ? "request_conflict"
          : error.reason === "parent_not_active"
            ? "parent_not_active"
            : error.reason === "storage_unavailable" || error.reason === "acceptance_unknown"
              ? "orchestration_error"
              : "thread_not_sendable",
    message: error.message,
  });

export interface SourceBinding {
  readonly sourceTaskId: NodeId;
  readonly sourceRunId: RunId;
  readonly sourceAttemptId: OrchestrationV2RunAttempt["id"];
  readonly sourceEvidence: "attempt";
  readonly providerInstanceId: ProviderInstanceId;
  readonly providerThreadId: ProviderThreadId;
  readonly nativeSessionId: string;
  readonly modelSelection: ModelSelection;
}

export const terminalRun = (run: Pick<OrchestrationV2Run, "status">): boolean =>
  ["completed", "failed", "cancelled", "interrupted", "rolled_back"].includes(run.status);
const terminalTask = (task: OrchestrationV2Subagent): boolean =>
  ["completed", "failed", "cancelled", "interrupted"].includes(task.status);

export function continuationAncestry(
  runs: ReadonlyArray<OrchestrationV2Run>,
  run: OrchestrationV2Run,
): ReadonlyArray<OrchestrationV2Run> | TaskContinuationError {
  const seen = new Set<RunId>();
  const ancestry: Array<OrchestrationV2Run> = [];
  let current = run;
  for (let depth = 0; depth < 64; depth++) {
    if (seen.has(current.id) || current.threadId !== run.threadId)
      return new TaskContinuationError({ reason: "ancestry_invalid" });
    seen.add(current.id);
    ancestry.push(current);
    if (current.restartContinuationOfRunId === undefined) return ancestry;
    const parent = runs.find((candidate) => candidate.id === current.restartContinuationOfRunId);
    if (parent === undefined) return new TaskContinuationError({ reason: "ancestry_invalid" });
    current = parent;
  }
  return new TaskContinuationError({ reason: "ancestry_invalid" });
}

export function continuationRoot(
  runs: ReadonlyArray<OrchestrationV2Run>,
  run: OrchestrationV2Run,
): OrchestrationV2Run | TaskContinuationError {
  const ancestry = continuationAncestry(runs, run);
  if (Schema.is(TaskContinuationError)(ancestry)) return ancestry;
  for (const current of ancestry) {
    if (
      current.delegatedTaskId !== run.delegatedTaskId ||
      current.providerInstanceId !== run.providerInstanceId ||
      current.providerThreadId !== run.providerThreadId ||
      !modelSelectionsEqual(current.modelSelection, run.modelSelection)
    )
      return new TaskContinuationError({ reason: "ancestry_invalid" });
  }
  return ancestry[ancestry.length - 1]!;
}

type AdmissionRecords = Pick<
  OrchestrationV2ThreadProjection,
  | "thread"
  | "runs"
  | "attempts"
  | "subagents"
  | "providerThreads"
  | "providerTurns"
  | "runtimeRequests"
  | "turnItems"
>;

type SourceRecords = Pick<
  OrchestrationV2ThreadProjection,
  "thread" | "runs" | "attempts" | "providerTurns" | "contextTransfers"
>;

function attemptAcceptance(
  attempt: OrchestrationV2RunAttempt,
  turns: ReadonlyArray<OrchestrationV2ProviderTurn>,
): "accepted" | "unaccepted" | "conflict" {
  const settlement = attempt.providerSettlement;
  const namedTurnIds = new Set(
    [attempt.providerTurnId, settlement?.providerTurnId].filter((id) => id != null),
  );
  const related = turns.filter(
    (turn) => turn.runAttemptId === attempt.id || namedTurnIds.has(turn.id),
  );
  // A permissive legacy link must never hide contradictory attribution.
  if (
    related.some(
      (turn) =>
        turn.providerThreadId !== attempt.providerThreadId ||
        (turn.runAttemptId !== null && turn.runAttemptId !== attempt.id),
    ) ||
    (settlement != null &&
      (settlement.runAttemptId !== attempt.id ||
        (attempt.providerTurnId !== null && settlement.providerTurnId !== attempt.providerTurnId)))
  )
    return "conflict";
  return related.length > 0 ? "accepted" : "unaccepted";
}

function sourceRuns(
  task: OrchestrationV2Subagent,
  child: SourceRecords,
): ReadonlyArray<OrchestrationV2Run> | TaskContinuationError {
  const tagged = child.runs.filter((run) => run.delegatedTaskId === task.id);
  const original =
    child.thread.forkedFrom?.type === "node" && child.thread.forkedFrom.nodeId === task.id;
  let root: OrchestrationV2Run | undefined;
  if (original) {
    if (tagged.length > 0) return new TaskContinuationError({ reason: "native_source_conflict" });
    const spawns = child.contextTransfers.filter(
      (transfer) =>
        transfer.type === "subagent_spawn" &&
        transfer.sourceThreadId === task.threadId &&
        transfer.targetThreadId === child.thread.id,
    );
    const spawn = spawns[0];
    if (spawns.length !== 1 || spawn?.status !== "consumed" || spawn.targetRunId === null)
      return new TaskContinuationError({ reason: "source_link_unavailable" });
    root = child.runs.find((run) => run.id === spawn.targetRunId);
    if (
      root === undefined ||
      root.threadId !== child.thread.id ||
      root.delegatedTaskId !== undefined ||
      root.restartContinuationOfRunId !== undefined ||
      (spawn.targetProviderInstanceId !== null &&
        spawn.targetProviderInstanceId !== root.providerInstanceId)
    )
      return new TaskContinuationError({ reason: "source_link_unavailable" });
  } else {
    if (tagged.length === 0)
      return new TaskContinuationError({ reason: "source_link_unavailable" });
    const candidate = continuationRoot(child.runs, tagged[0]!);
    if (Schema.is(TaskContinuationError)(candidate)) return candidate;
    root = candidate;
    if (
      tagged.some((run) => {
        const ancestor = continuationRoot(child.runs, run);
        return Schema.is(TaskContinuationError)(ancestor) || ancestor.id !== candidate.id;
      })
    )
      return new TaskContinuationError({ reason: "source_link_unavailable" });
  }
  // Initial launches are untagged. The consumed spawn identifies their root;
  // unrelated wakes must not substitute their newer native conversation.
  const ids = new Set([root.id]);
  const chain = [root];
  for (let depth = 0; depth < 64; depth++) {
    const descendants = child.runs.filter(
      (run) =>
        !ids.has(run.id) &&
        run.restartContinuationOfRunId !== undefined &&
        ids.has(run.restartContinuationOfRunId),
    );
    if (descendants.length === 0) return chain.toSorted((a, b) => a.ordinal - b.ordinal);
    for (const run of descendants) {
      const ancestor = continuationRoot(child.runs, run);
      if (Schema.is(TaskContinuationError)(ancestor) || ancestor.id !== root.id)
        return new TaskContinuationError({ reason: "native_source_conflict" });
      ids.add(run.id);
      chain.push(run);
    }
  }
  return new TaskContinuationError({ reason: "ancestry_invalid" });
}

export function sourceBinding(
  task: OrchestrationV2Subagent,
  child: SourceRecords,
): SourceBinding | TaskContinuationError {
  if (
    task.origin !== "app_owned" ||
    task.childThreadId !== child.thread.id ||
    task.remoteChild !== undefined
  )
    return new TaskContinuationError({ reason: "task_not_found" });
  if (task.driver !== "claudeAgent")
    return new TaskContinuationError({ reason: "driver_unsupported" });
  if (task.status !== "completed" || task.result === null || task.completedAt === null)
    return new TaskContinuationError({ reason: "source_not_completed" });
  const runs = sourceRuns(task, child);
  if (Schema.is(TaskContinuationError)(runs)) return runs;
  const last = runs.at(-1);
  if (last === undefined || last.status !== "completed")
    return new TaskContinuationError({ reason: "native_source_unavailable" });
  const acceptedAttempts: Array<OrchestrationV2RunAttempt> = [];
  for (const attempt of child.attempts.filter((attempt) =>
    runs.some((run) => run.id === attempt.runId),
  )) {
    const acceptance = attemptAcceptance(attempt, child.providerTurns);
    if (acceptance === "conflict")
      return new TaskContinuationError({ reason: "native_source_conflict" });
    if (acceptance === "accepted") acceptedAttempts.push(attempt);
  }
  const accepted = acceptedAttempts
    .filter((attempt) => attempt.runId === last.id && attempt.status === "completed")
    .toSorted((a, b) => b.attemptOrdinal - a.attemptOrdinal)[0];
  if (
    accepted === undefined ||
    (accepted.providerSettlement != null && accepted.providerSettlement.status !== "completed")
  )
    return new TaskContinuationError({ reason: "native_source_unavailable" });
  if (
    accepted.providerInstanceId !== last.providerInstanceId ||
    accepted.providerThreadId !== last.providerThreadId ||
    accepted.providerInstanceId !== task.providerInstanceId
  )
    return new TaskContinuationError({ reason: "native_source_conflict" });
  if (acceptedAttempts.some((attempt) => !attempt.nativeThreadId?.trim()))
    return new TaskContinuationError({ reason: "native_source_uncaptured" });
  if (
    acceptedAttempts.some(
      (attempt) =>
        attempt.nativeThreadId !== accepted.nativeThreadId ||
        attempt.providerInstanceId !== last.providerInstanceId ||
        attempt.providerThreadId !== last.providerThreadId,
    )
  )
    return new TaskContinuationError({ reason: "native_source_conflict" });
  return {
    sourceTaskId: task.id,
    sourceRunId: last.id,
    sourceAttemptId: accepted.id,
    sourceEvidence: "attempt",
    providerInstanceId: last.providerInstanceId,
    providerThreadId: accepted.providerThreadId,
    nativeSessionId: accepted.nativeThreadId!,
    modelSelection: last.modelSelection,
  };
}

export function assertCurrentBinding(
  binding: SourceBinding,
  child: Pick<AdmissionRecords, "thread" | "providerThreads">,
): TaskContinuationError | undefined {
  const thread = child.thread;
  if (
    thread.deletedAt !== null ||
    thread.archivedAt !== null ||
    thread.threadMessagesBlocked === true
  )
    return new TaskContinuationError({ reason: "thread_not_sendable" });
  if (
    !modelSelectionsEqual(thread.modelSelection, binding.modelSelection) ||
    thread.providerInstanceId !== binding.providerInstanceId
  )
    return new TaskContinuationError({ reason: "selection_changed" });
  const provider = child.providerThreads.find((row) => row.id === binding.providerThreadId);
  if (
    thread.activeProviderThreadId !== binding.providerThreadId ||
    !providerMatchesBinding(binding, provider, thread.id)
  )
    return new TaskContinuationError({ reason: "native_reference_moved" });
}

export function providerMatchesBinding(
  binding: SourceBinding,
  row: OrchestrationV2ProviderThread | undefined,
  childThreadId: string,
): boolean {
  return (
    row !== undefined &&
    row.id === binding.providerThreadId &&
    row.appThreadId === childThreadId &&
    row.providerInstanceId === binding.providerInstanceId &&
    row.driver === "claudeAgent" &&
    row.nativeThreadRef?.driver === "claudeAgent" &&
    row.nativeThreadRef.nativeId === binding.nativeSessionId
  );
}

export function idleEligibility(
  parent: Pick<OrchestrationV2ThreadProjection, "thread" | "subagents">,
  child: AdmissionRecords,
): TaskContinuationError | undefined {
  const originalTaskId =
    child.thread.forkedFrom?.type === "node" ? child.thread.forkedFrom.nodeId : undefined;
  if (
    child.thread.lineage.parentThreadId !== parent.thread.id ||
    child.thread.lineage.relationshipToParent !== "subagent" ||
    child.thread.forkedFrom?.type !== "node" ||
    !parent.subagents.some(
      (task) =>
        task.id === originalTaskId &&
        task.origin === "app_owned" &&
        task.childThreadId === child.thread.id &&
        task.result !== null,
    )
  )
    return new TaskContinuationError({ reason: "lineage_changed" });
  if (
    parent.subagents.some(
      (task) => task.childThreadId === child.thread.id && !terminalTask(task),
    ) ||
    child.runs.some((run) => !terminalRun(run)) ||
    child.runtimeRequests.some((request) => request.status === "pending") ||
    child.turnItems.some(
      (item) =>
        item.type === "user_input_request" &&
        item.responseMode === "message" &&
        ["pending", "running", "waiting"].includes(item.status),
    ) ||
    child.turnItems.some(
      (item) => item.type === "secret_request" && item.secretStatus === "pending",
    ) ||
    child.subagents.some((task) => !terminalTask(task)) ||
    child.providerThreads.some((row) => (row.pendingBackgroundTasks?.length ?? 0) > 0) ||
    derivePendingBackgroundWork({
      latestRun: child.runs.toSorted((a, b) => b.ordinal - a.ordinal)[0],
      providerThreads: child.providerThreads,
      turnItems: child.turnItems,
      runs: child.runs,
      activeProviderThreadId: child.thread.activeProviderThreadId,
    }).length > 0
  )
    return new TaskContinuationError({ reason: "child_busy" });
}

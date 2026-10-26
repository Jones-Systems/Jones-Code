import {
  OrchestratorMcpFailure,
  type ModelSelection,
  type NodeId,
  type OrchestrationV2ProviderThread,
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
    return `[${this.reason}] Claude task continuation could not preserve its original conversation.`;
  }
}

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

export function continuationRoot(
  runs: ReadonlyArray<OrchestrationV2Run>,
  run: OrchestrationV2Run,
): OrchestrationV2Run | TaskContinuationError {
  const seen = new Set<RunId>();
  let current = run;
  for (let depth = 0; depth < 64; depth++) {
    if (
      seen.has(current.id) ||
      current.threadId !== run.threadId ||
      current.delegatedTaskId !== run.delegatedTaskId ||
      current.providerInstanceId !== run.providerInstanceId ||
      current.providerThreadId !== run.providerThreadId ||
      !modelSelectionsEqual(current.modelSelection, run.modelSelection)
    )
      return new TaskContinuationError({ reason: "ancestry_invalid" });
    seen.add(current.id);
    if (current.restartContinuationOfRunId === undefined) return current;
    const parent = runs.find((candidate) => candidate.id === current.restartContinuationOfRunId);
    if (parent === undefined) return new TaskContinuationError({ reason: "ancestry_invalid" });
    current = parent;
  }
  return new TaskContinuationError({ reason: "ancestry_invalid" });
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

export function sourceBinding(
  task: OrchestrationV2Subagent,
  child: AdmissionRecords,
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
  const runs = child.runs
    .filter((run) => run.delegatedTaskId === task.id)
    .toSorted((a, b) => a.ordinal - b.ordinal);
  const last = runs.at(-1);
  if (last === undefined || last.status !== "completed")
    return new TaskContinuationError({ reason: "native_source_unavailable" });
  const root = continuationRoot(child.runs, last);
  if (
    Schema.is(TaskContinuationError)(root) ||
    root.delegatedTaskId !== task.id ||
    runs.some((run) => {
      const candidate = continuationRoot(child.runs, run);
      return Schema.is(TaskContinuationError)(candidate) || candidate.id !== root.id;
    })
  )
    return new TaskContinuationError({ reason: "native_source_unavailable" });
  const accepted = child.attempts
    .filter(
      (attempt) =>
        attempt.runId === last.id &&
        child.providerTurns.some(
          (turn) =>
            turn.runAttemptId === attempt.id && turn.providerThreadId === attempt.providerThreadId,
        ),
    )
    .toSorted((a, b) => b.attemptOrdinal - a.attemptOrdinal)[0];
  if (
    accepted === undefined ||
    accepted.status !== "completed" ||
    accepted.nativeThreadId === undefined ||
    accepted.nativeThreadId.length === 0 ||
    accepted.providerInstanceId !== last.providerInstanceId ||
    accepted.providerThreadId !== last.providerThreadId
  )
    return new TaskContinuationError({ reason: "native_source_unavailable" });
  return {
    sourceTaskId: task.id,
    sourceRunId: last.id,
    sourceAttemptId: accepted.id,
    sourceEvidence: "attempt",
    providerInstanceId: last.providerInstanceId,
    providerThreadId: accepted.providerThreadId,
    nativeSessionId: accepted.nativeThreadId,
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

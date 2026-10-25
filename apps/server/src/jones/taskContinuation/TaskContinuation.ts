import {
  nativeCreationCanonicalJson,
  nativeCreationSha256,
} from "../nativeCreation/NativeCreationPreparation.ts";
import {
  CommandId,
  MessageId,
  OrchestratorMcpFailure,
  type TaskContinueInput,
  type TaskContinueResult,
  type ThreadId,
} from "@t3tools/contracts";
import { modelSelectionsEqual } from "@t3tools/shared/model";
import { derive } from "@t3tools/provider-core/server/IdAllocator";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Ref from "effect/Ref";
import { requireThreadScope, type McpInvocationScope } from "../../mcp/McpInvocationContext.ts";
import { isLiveCallerShell } from "../../mcp/threadAccess.ts";
import { resolveInteractionMode, resolveRuntimeMode } from "../../mcp/delegatedTaskModes.ts";
import * as OrchestratorMcp from "../../mcp/OrchestratorMcpService.ts";
import * as Threads from "../../orchestration-v2/ThreadManagementService.ts";
import * as Projections from "../../orchestration-v2/ProjectionStore.ts";
import * as Receipts from "../../orchestration-v2/CommandReceiptStore.ts";
import { DispatchModeLimit } from "../../orchestration-v2/DispatchModeLimit.ts";
import { TaskContinuationCommit, type TaskContinuationCommitContext } from "./commitContext.ts";
import * as Store from "./TaskContinuationStore.ts";
import {
  assertCurrentBinding,
  continuationRoot,
  idleEligibility,
  sourceBinding,
  TaskContinuationError,
  terminalRun,
  toMcpFailure,
} from "./sourceBinding.ts";

export function requestIdentity(parentThreadId: ThreadId, input: TaskContinueInput) {
  const hash = (value: unknown) => nativeCreationSha256(nativeCreationCanonicalJson(value));
  const key = hash(["task-continue", parentThreadId, input.clientRequestId]);
  return {
    commandId: CommandId.make(`command:jones:task-continue:${key}`),
    messageId: MessageId.make(`message:jones:task-continue:${key}`),
    requestDigest: hash([input.taskId, input.message, input.expectedNativeSessionId ?? null]),
  };
}

export class TaskContinuation extends Context.Service<
  TaskContinuation,
  {
    readonly continue: (
      scope: McpInvocationScope,
      input: TaskContinueInput,
    ) => Effect.Effect<TaskContinueResult, OrchestratorMcpFailure>;
  }
>()("t3/jones/taskContinuation/TaskContinuation") {}

const make = Effect.gen(function* () {
  const store = yield* Store.TaskContinuationStore;
  const projections = yield* Projections.ProjectionStoreV2;
  const threads = yield* Threads.ThreadManagementService;
  const orchestrator = yield* OrchestratorMcp.OrchestratorMcpService;
  const receipts = yield* Receipts.CommandReceiptStoreV2;
  const view = Effect.fn("TaskContinuation.view")(
    function* (scope: McpInvocationScope, row: Store.TaskContinuationRecord, replayed: boolean) {
      const task = yield* orchestrator.taskStatus(scope, row.taskId);
      const current = yield* projections.getThreadRecords(row.childThreadId, ["runs", "attempts"]);
      const chain = current.runs.filter((run) => {
        const root = continuationRoot(current.runs, run);
        return (
          !Schema.is(TaskContinuationError)(root) &&
          root.id === row.runId &&
          run.delegatedTaskId === row.taskId
        );
      });
      if (!chain.some((run) => run.id === row.runId && run.userMessageId === row.messageId))
        return yield* new TaskContinuationError({ reason: "acceptance_unknown" });
      const witnesses = yield* store.witnesses(row);
      const verified = witnesses
        .filter(
          (witness) =>
            chain.some((run) => run.id === witness.runId) &&
            current.attempts.some(
              (attempt) =>
                attempt.id === witness.attemptId &&
                attempt.runId === witness.runId &&
                attempt.nativeThreadId === row.nativeSessionId,
            ),
        )
        .toSorted(
          (a, b) =>
            (chain.find((r) => r.id === b.runId)?.ordinal ?? 0) -
            (chain.find((r) => r.id === a.runId)?.ordinal ?? 0),
        )[0];
      return {
        ...task,
        replayed,
        continuation: {
          sourceTaskId: row.sourceTaskId,
          sourceRunId: row.sourceRunId,
          sourceEvidence: row.sourceEvidence,
          providerInstanceId: row.providerInstanceId,
          providerThreadId: row.providerThreadId,
          nativeSessionId: row.nativeSessionId,
          modelSelection: row.modelSelection,
          verification:
            verified !== undefined
              ? ("verified" as const)
              : chain.length > 0 && chain.every(terminalRun)
                ? ("unverified" as const)
                : ("pending" as const),
          verifiedRunId: verified?.runId ?? null,
          account: "unavailable" as const,
        },
      } satisfies TaskContinueResult;
    },
    Effect.mapError((cause) =>
      Schema.is(OrchestratorMcpFailure)(cause)
        ? cause
        : toMcpFailure(
            Schema.is(TaskContinuationError)(cause)
              ? cause
              : new TaskContinuationError({ reason: "storage_unavailable", cause }),
          ),
    ),
  );

  return TaskContinuation.of({
    continue: Effect.fn("TaskContinuation.continue")(
      function* (callerScope, input) {
        const scope = yield* requireThreadScope(callerScope, "t3_task_continue");
        const identity = requestIdentity(scope.thread.threadId, input);
        const reconcile = Effect.fn("TaskContinuation.reconcile")(function* () {
          const row = yield* store.get(identity.messageId);
          if (
            row !== null &&
            (row.parentThreadId !== scope.thread.threadId ||
              row.requestDigest !== identity.requestDigest ||
              row.commandId !== identity.commandId)
          )
            return yield* new TaskContinuationError({ reason: "request_conflict" });
          return row;
        });
        const previous = yield* reconcile();
        if (previous !== null) return yield* view(scope, previous, true);
        const receipt = yield* receipts.getByCommandId(identity.commandId);
        if (Option.isSome(receipt)) {
          // The receipt read can race another call's atomic acceptance.
          const accepted = yield* reconcile();
          if (accepted !== null) return yield* view(scope, accepted, true);
          return yield* new TaskContinuationError({
            reason: receipt.value.status === "rejected" ? "refused_key" : "acceptance_unknown",
          });
        }

        // Reused at commit time: thread-scoped reads use the existing upstream indexes.
        const eligible = Effect.fn("TaskContinuation.eligible")(function* () {
          const parent = yield* projections.getThreadRecords(scope.thread.threadId, ["subagents"]);
          const shell = yield* projections.getThreadShell(scope.thread.threadId);
          if (
            parent.thread.deletedAt !== null ||
            !isLiveCallerShell(shell, scope.thread.providerInstanceId)
          )
            return yield* new TaskContinuationError({ reason: "parent_not_active" });
          const task = parent.subagents.find(
            (task) =>
              task.id === input.taskId &&
              task.threadId === scope.thread.threadId &&
              task.origin === "app_owned",
          );
          if (task === undefined || task.childThreadId === null || task.remoteChild !== undefined)
            return yield* new TaskContinuationError({ reason: "task_not_found" });
          const child = yield* projections.getThreadRecords(task.childThreadId, [
            "runs",
            "attempts",
            "providerTurns",
            "providerThreads",
            "subagents",
            "runtimeRequests",
            "turnItems",
          ]);
          const binding = sourceBinding(task, child);
          if (Schema.is(TaskContinuationError)(binding)) return yield* binding;
          const idle = idleEligibility(parent, child);
          if (idle !== undefined) return yield* idle;
          const current = assertCurrentBinding(binding, child);
          if (current !== undefined) return yield* current;
          if (
            input.expectedNativeSessionId !== undefined &&
            input.expectedNativeSessionId !== binding.nativeSessionId
          )
            return yield* new TaskContinuationError({ reason: "native_session_mismatch" });
          yield* resolveRuntimeMode(parent.thread.runtimeMode, child.thread.runtimeMode);
          yield* resolveInteractionMode(
            parent.thread.interactionMode,
            child.thread.interactionMode,
          );
          return { parent, child, binding };
        });
        const preflight = yield* eligible().pipe(
          Effect.catch((cause) =>
            reconcile().pipe(
              Effect.flatMap((row) => (row === null ? Effect.fail(cause) : Effect.succeed(row))),
            ),
          ),
        );
        if ("messageId" in preflight) return yield* view(scope, preflight, true);
        yield* threads.assertProviderInstanceEnabled({
          projectId: preflight.child.thread.projectId,
          commandId: identity.commandId,
          instanceId: preflight.binding.providerInstanceId,
        });
        const inserted = yield* Ref.make(false);
        const context: TaskContinuationCommitContext = {
          accept: (envelope) =>
            Effect.gen(function* () {
              const existing = yield* reconcile();
              if (existing !== null) return;
              const fresh = yield* eligible();
              if (
                envelope.commandId !== identity.commandId ||
                envelope.commandType !== "message.dispatch" ||
                envelope.threadId !== fresh.child.thread.id ||
                fresh.child.thread.id !== preflight.child.thread.id ||
                nativeCreationCanonicalJson(fresh.binding) !==
                  nativeCreationCanonicalJson(preflight.binding)
              )
                return yield* new TaskContinuationError({ reason: "native_reference_moved" });
              const taskId = derive.delegatedTaskNode({ commandId: identity.commandId });
              const run = envelope.events
                .flatMap((event) =>
                  (event.type === "run.created" || event.type === "run.updated") &&
                  event.threadId === fresh.child.thread.id &&
                  event.payload.userMessageId === identity.messageId
                    ? [event.payload]
                    : [],
                )
                .at(-1);
              const attempt = envelope.events
                .flatMap((event) =>
                  event.type === "run-attempt.created" && event.payload.runId === run?.id
                    ? [event.payload]
                    : [],
                )
                .at(-1);
              const task = envelope.events
                .flatMap((event) =>
                  event.type === "subagent.updated" &&
                  event.threadId === scope.thread.threadId &&
                  event.payload.id === taskId
                    ? [event.payload]
                    : [],
                )
                .at(-1);
              if (
                run === undefined ||
                run.status !== "starting" ||
                run.queueHeld === true ||
                run.delegatedTaskId !== taskId ||
                run.providerInstanceId !== fresh.binding.providerInstanceId ||
                run.providerThreadId !== fresh.binding.providerThreadId ||
                !modelSelectionsEqual(run.modelSelection, fresh.binding.modelSelection) ||
                attempt === undefined ||
                attempt.id !== run.activeAttemptId ||
                attempt.providerThreadId !== fresh.binding.providerThreadId ||
                attempt.providerInstanceId !== fresh.binding.providerInstanceId ||
                task?.origin !== "app_owned" ||
                task.childThreadId !== fresh.child.thread.id ||
                task.providerThreadId !== fresh.binding.providerThreadId ||
                task.result !== null ||
                task.completionWake !== "always"
              )
                return yield* new TaskContinuationError({ reason: "child_busy" });
              yield* store.insert({
                ...identity,
                ...fresh.binding,
                runId: run.id,
                parentThreadId: scope.thread.threadId,
                childThreadId: fresh.child.thread.id,
                taskId,
                acceptedAt: DateTime.formatIso(yield* DateTime.now),
              });
              yield* Ref.set(inserted, true);
            }).pipe(
              Effect.mapError((cause) =>
                Schema.is(TaskContinuationError)(cause)
                  ? cause
                  : new TaskContinuationError({ reason: "acceptance_unknown", cause }),
              ),
            ),
        };
        const dispatched = yield* Effect.result(
          threads
            .dispatch({
              type: "message.dispatch",
              commandId: identity.commandId,
              messageId: identity.messageId,
              threadId: preflight.child.thread.id,
              createdBy: "agent",
              creationSource: "mcp",
              senderThreadId: scope.thread.threadId,
              text: input.message,
              attachments: [],
              dispatchMode: { type: "start_immediately" },
            })
            .pipe(
              Effect.provideService(TaskContinuationCommit, context),
              Effect.provideService(DispatchModeLimit, {
                runtimeMode: preflight.parent.thread.runtimeMode,
                interactionMode: preflight.parent.thread.interactionMode,
              }),
            ),
        );
        // Mandatory reconciliation covers both early receipt returns and concurrent changed-body calls.
        const accepted = yield* reconcile();
        if (accepted !== null) return yield* view(scope, accepted, !(yield* Ref.get(inserted)));
        if (dispatched._tag === "Failure") {
          let cause: unknown = dispatched.failure;
          for (let depth = 0; depth < 12 && cause !== null && typeof cause === "object"; depth++) {
            if (Schema.is(TaskContinuationError)(cause)) return yield* cause;
            cause = "cause" in cause ? cause.cause : undefined;
          }
        }
        return yield* new TaskContinuationError({ reason: "acceptance_unknown" });
      },
      Effect.mapError((cause) =>
        Schema.is(OrchestratorMcpFailure)(cause)
          ? cause
          : toMcpFailure(
              Schema.is(TaskContinuationError)(cause)
                ? cause
                : new TaskContinuationError({ reason: "storage_unavailable", cause }),
            ),
      ),
    ),
  });
});
export const layer = Layer.effect(TaskContinuation, make).pipe(Layer.provide(Receipts.layer));

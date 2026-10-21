import {
  CommandId,
  MessageId,
  type NodeId,
  OrchestratorMcpFailure,
  type OrchestrationV2RemoteTaskChild,
  type OrchestrationV2Subagent,
  type ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schedule from "effect/Schedule";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import type { McpThreadInvocationScope } from "../../mcp/McpInvocationContext.ts";
import { ProjectToolkit } from "../../mcp/toolkits/project/tools.ts";
import * as ThreadManagement from "../../orchestration-v2/ThreadManagementService.ts";
import * as PeerForwarding from "../../peer/PeerForwarding.ts";
import * as PeerLinks from "../../peer/PeerLinks.ts";
import * as IntentStore from "./IntentStore.ts";
import { CancelRemoteAdmissionTool } from "../remoteAdmissionIntegration/cancelTool.ts";

const retry = Schedule.exponential(Duration.seconds(2)).pipe(
  Schedule.modifyDelay(({ duration }) =>
    Effect.succeed(Duration.min(duration, Duration.minutes(5))),
  ),
  Schedule.jittered,
);
const refusedBeforeLaunch = (error: OrchestratorMcpFailure) =>
  error.code === "runtime_mode_escalation_denied" ||
  error.code === "interaction_mode_escalation_denied" ||
  error.code === "capability_denied" ||
  error.code === "provider_unavailable" ||
  error.code === "model_unavailable";
const unavailable = () =>
  new OrchestratorMcpFailure({
    code: "orchestration_error",
    message: "Remote delegation is durably pending reconciliation.",
  });
const scopeOf = (intent: IntentStore.Intent): McpThreadInvocationScope => ({
  ...intent.envelope.scope,
  capabilities: new Set(["orchestration"]),
  client: undefined,
  issuedAt: 0,
});

export const make = Effect.fn("RemoteDelegationLifecycle.make")(function*(onBound: (parentThreadId: ThreadId, taskId: NodeId) => Effect.Effect<void>) {
    const store = yield* IntentStore.make;
    const forwarding = yield* PeerForwarding.PeerForwarding;
    const links = yield* PeerLinks.PeerLinks;
    const threads = yield* ThreadManagement.ThreadManagementService;
    const scope = yield* Scope.Scope;
    const locks = new Map<CommandId, Semaphore.Semaphore>();
    const recovering = new Set<CommandId>();
    const locked = <A>(id: CommandId, effect: Effect.Effect<A, OrchestratorMcpFailure>) =>
      Effect.suspend(() => {
        let lock = locks.get(id);
        if (lock === undefined) {
          lock = Semaphore.makeUnsafe(1);
          locks.set(id, lock);
        }
        return lock.withPermits(1)(effect);
      });
    const taskFor = (intent: IntentStore.Intent) =>
      threads.getThreadRecords(intent.envelope.request.parentThreadId, ["subagents"]).pipe(
        Effect.map(({ subagents }) => subagents.find((task) => task.id === intent.taskId)),
        Effect.mapError(unavailable),
      );
    const reserve = Effect.fn("RemoteDelegationLifecycle.reserve")(function*(intent: IntentStore.Intent) {
        if (intent.taskId !== null) return intent;
        const recorded = yield* threads
          .dispatch(intent.envelope.request)
          .pipe(Effect.mapError(unavailable));
        const event = recorded.storedEvents.find(
          (stored) => stored.event.type === "subagent.updated",
        );
        const taskId =
          event?.event.type === "subagent.updated" ? event.event.payload.id : undefined;
        // Command receipts replay their original events, including the task identity.
        if (taskId === undefined) return yield* unavailable();
        yield* store.reserved(intent.commandId, taskId);
        return { ...intent, taskId };
      });
    const finishDenied = Effect.fn("RemoteDelegationLifecycle.finishDenied")(function*(intent: IntentStore.Intent, error: OrchestratorMcpFailure) {
        if (intent.taskId === null) return yield* unavailable();
        yield* threads
          .dispatch({
            type: "delegated_task.remote.complete",
            commandId: CommandId.make(`${intent.commandId}:denied`),
            parentThreadId: intent.envelope.request.parentThreadId,
            taskId: intent.taskId,
            status: error.remoteAdmission?.status === "cancelled" ? "cancelled" : "failed",
            result:
              error.remoteAdmission?.status === "cancelled"
                ? "Remote admission withdrawn."
                : error.code === "remote_admission_denied"
                  ? "Remote admission denied."
                  : error.message,
          })
          .pipe(Effect.mapError(unavailable));
        yield* store.finish(intent.commandId, "denied");
      });
    const attempt = (id: CommandId): Effect.Effect<void, OrchestratorMcpFailure> =>
      locked(
        id,
        Effect.gen(function* () {
          const saved = yield* store.get(id);
          if (saved === undefined || saved.status !== "pending") return;
          const intent = yield* reserve(saved);
          const task = yield* taskFor(intent);
          if (task === undefined) return yield* unavailable();
          if (task.result !== null && !intent.launchStarted) {
            yield* store.finish(id, "stopped");
            return;
          }
          const { envelope } = intent;
          const link = yield* links.get(envelope.environmentId).pipe(Effect.mapError(unavailable));
          if (Option.isNone(link) || DateTime.formatIso(link.value.linkedAt) !== envelope.linkedAt)
            return yield* new OrchestratorMcpFailure({
              code: "invalid_request",
              message:
                "The original remote delegation link is unavailable or was replaced; execution remains unresolved.",
            });
          const caller = scopeOf(intent);
          if (task.result !== null && intent.admission != null && intent.launched === null) {
            const withdrawn = yield* forwarding.call(
              caller,
              CancelRemoteAdmissionTool,
              envelope.environmentId,
              {
                id: intent.admission.id,
                bindingDigest: intent.admission.bindingDigest,
              },
            );
            if (withdrawn.status === "cancelled" || withdrawn.status === "denied") {
              yield* store.finish(id, "stopped");
              return;
            }
          }
          let launched = intent.launched;
          if (launched === null || (launched.runId === null && task.result === null)) {
            yield* store.beginLaunch(id);
            const outcome = yield* forwarding
              .call(
                caller,
                ProjectToolkit.tools.t3_thread_launch,
                envelope.environmentId,
                envelope.launch,
              )
              .pipe(Effect.result);
            if (outcome._tag === "Failure") {
              const error = outcome.failure;
              if (error.remoteAdmission !== undefined)
                yield* store.admission(id, error.remoteAdmission);
              if (
                error.code === "remote_admission_pending" &&
                error.remoteAdmission?.status === "held"
              ) {
                const progress = `Awaiting owner approval in ${envelope.label}.`;
                yield* threads
                  .dispatch({
                    type: "delegated_task.remote.hold",
                    commandId: CommandId.make(`${id}:hold`),
                    parentThreadId: envelope.request.parentThreadId,
                    taskId: task.id,
                    progress,
                  })
                  .pipe(Effect.mapError(unavailable));
                const current = yield* taskFor(intent);
                if (current?.result === null && envelope.request.completionWake !== "settled_only")
                  yield* threads
                    .dispatch({
                      type: "message.dispatch",
                      commandId: CommandId.make(`${id}:hold-notice`),
                      threadId: envelope.request.parentThreadId,
                      messageId: MessageId.make(`${id}:hold-notice`),
                      text: `Remote task ${task.id}: ${progress} Ask the owner to review it in that environment. This task remains pending; do not launch a replacement.`,
                      attachments: [],
                      createdBy: "system",
                      creationSource: "mcp",
                      dispatchMode: { type: "queue_after_active" },
                    })
                    .pipe(Effect.mapError(unavailable));
              }
              if (
                error.code === "remote_admission_denied" ||
                (!intent.launchStarted && refusedBeforeLaunch(error))
              ) {
                yield* finishDenied(intent, error);
                return;
              }
              return yield* error;
            }
            launched = outcome.success;
            yield* store.launched(id, launched);
          }
          const remoteChild: OrchestrationV2RemoteTaskChild = {
            environmentId: envelope.environmentId,
            threadId: launched.threadId,
            label: envelope.label,
            ...(launched.runId === null ? {} : { runId: launched.runId }),
          };
          if (launched.runId === null && task.result === null) return yield* unavailable();
          yield* threads
            .dispatch({
              type: "delegated_task.remote.bind",
              commandId: CommandId.make(`${id}:bind`),
              parentThreadId: envelope.request.parentThreadId,
              taskId: task.id,
              remoteChild,
            })
            .pipe(Effect.mapError(unavailable));
          yield* store.recorded(id, task.id);
          yield* onBound(envelope.request.parentThreadId, task.id);
        }),
      );
    const recover = (id: CommandId) =>
      Effect.suspend(() => {
        if (recovering.has(id)) return Effect.void;
        recovering.add(id);
        return attempt(id).pipe(
          Effect.retry(retry),
          Effect.ignoreCause({ log: true }),
          Effect.ensuring(
            Effect.sync(() => {
              recovering.delete(id);
            }),
          ),
          Effect.forkIn(scope),
          Effect.asVoid,
        );
      });
    const submit = Effect.fn("RemoteDelegationLifecycle.submit")(function*(commandId: CommandId, request: unknown, envelope: IntentStore.IntentEnvelope) {
        const intent = yield* store.prepare(commandId, request, envelope);
        const reserved = yield* locked(commandId, reserve(intent));
        yield* attempt(commandId).pipe(Effect.catch(() => recover(commandId)));
        return { taskId: reserved.taskId! };
      });
    const replay = Effect.fn("RemoteDelegationLifecycle.replay")(function*(commandId: CommandId, request: unknown) {
        const intent = yield* store.find(commandId, request);
        if (intent === undefined) return undefined;
        const reserved = yield* locked(commandId, reserve(intent));
        if (intent.status === "pending") yield* recover(commandId);
        else if (intent.status === "recorded")
          yield* onBound(intent.envelope.request.parentThreadId, reserved.taskId!);
        return { taskId: reserved.taskId! };
      });
    const cancelPending = Effect.fn("RemoteDelegationLifecycle.cancelPending")(function*(task: OrchestrationV2Subagent, reason?: string) {
        const intent = yield* store.byTask(task.id);
        if (intent === undefined)
          return yield* new OrchestratorMcpFailure({
            code: "task_not_found",
            message: "Remote delegation intent was not found.",
          });
        yield* threads
          .dispatch({
            type: "delegated_task.remote.complete",
            commandId: CommandId.make(`${intent.commandId}:cancel-pending`),
            parentThreadId: task.threadId,
            taskId: task.id,
            status: "cancelled",
            result:
              reason === undefined
                ? "Cancelled. Remote launch reconciliation remains pending."
                : `Cancelled: ${reason}. Remote launch reconciliation remains pending.`,
          })
          .pipe(Effect.mapError(unavailable));
        yield* recover(intent.commandId);
        return { taskId: task.id, status: "cancel_requested" as const };
      });
    return {
      submit,
      replay,
      cancelPending,
      start: store.pending.pipe(
        Effect.flatMap((pending) =>
          Effect.forEach(pending, (intent) => recover(intent.commandId), { discard: true }),
        ),
      ),
    };
  });

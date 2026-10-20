import {
  OrchestratorMcpFailure,
  RuntimeIdentityAttestation,
  type OrchestrationV2ProviderThread,
  type ThreadGoalInput,
  type ThreadGoalResult,
} from "@t3tools/contracts";
import type { ProviderAdapterV2SessionRuntime } from "@t3tools/provider-core/server/ProviderAdapter";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import {
  DispatchModeLimit,
  type DispatchModeRefusal,
} from "../../orchestration-v2/DispatchModeLimit.ts";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as ThreadManagement from "../../orchestration-v2/ThreadManagementService.ts";
import * as Sessions from "../../orchestration-v2/ProviderSessionManager.ts";
import * as Invocation from "../../mcp/McpInvocationContext.ts";
import {
  assertLiveCaller,
  assertTargetWithinLimits,
  readThread,
  unavailable,
} from "../../mcp/threadAccess.ts";
import * as GoalStart from "./GoalStartDispatch.ts";

export class GoalControl extends Context.Service<
  GoalControl,
  {
    readonly execute: (
      scope: Invocation.McpInvocationScope,
      input: ThreadGoalInput,
    ) => Effect.Effect<ThreadGoalResult, OrchestratorMcpFailure>;
  }
>()("t3/jones/goals/GoalControl") {}

const sameIdentity = Schema.toEquivalence(
  Schema.Union([RuntimeIdentityAttestation, Schema.Undefined]),
);
const sameBinding = (
  left: OrchestrationV2ProviderThread,
  right: OrchestrationV2ProviderThread | undefined,
) =>
  right !== undefined &&
  left.id === right.id &&
  left.providerSessionId === right.providerSessionId &&
  left.providerInstanceId === right.providerInstanceId &&
  left.nativeThreadRef?.nativeId === right.nativeThreadRef?.nativeId &&
  left.nativeThreadRef?.driver === right.nativeThreadRef?.driver &&
  sameIdentity(left.runtimeIdentity, right.runtimeIdentity);
interface Receipt {
  readonly fingerprint: string;
  readonly objective?: string;
  readonly previousStatus?: string;
  readonly startRequestId?: string;
  result: ThreadGoalResult;
}
interface Binding {
  readonly token: string;
  readonly provider: OrchestrationV2ProviderThread;
  readonly runtime: ProviderAdapterV2SessionRuntime;
  readonly receipts: Map<string, Receipt>;
  uncertain: boolean;
}

const make = Effect.gen(function* () {
  const threads = yield* ThreadManagement.ThreadManagementService;
  const sessions = yield* Effect.serviceOption(Sessions.ProviderSessionManagerV2);
  const starts = yield* Effect.serviceOption(GoalStart.GoalStartDispatch);
  const crypto = yield* Crypto.Crypto;
  const lock = yield* Semaphore.make(1);
  const bindings = new Map<string, Binding>();
  const execute = (scope: Invocation.McpInvocationScope, input: ThreadGoalInput) =>
    lock
      .withPermits(1)(
        Effect.gen(function* () {
          const context = yield* readThread(input.threadId, ["providerThreads", "runs"]);
          const { thread } = context.projection;
          const now = DateTime.formatIso(yield* DateTime.now);
          const answer = (
            outcome: ThreadGoalResult["outcome"],
            reason: string,
            fields: Partial<ThreadGoalResult> = {},
          ): ThreadGoalResult => ({
            threadId: thread.id,
            action: input.action,
            outcome,
            reason,
            source: "none",
            goal: null,
            observedAt: now,
            ...fields,
          });
          const mutation = input.action !== "read";
          if (mutation) {
            if (scope.client?.access === "read-only")
              return yield* new OrchestratorMcpFailure({
                code: "capability_denied",
                message: "This MCP client has read-only access.",
              });
            yield* assertLiveCaller(context);
            yield* assertTargetWithinLimits(context.limits, thread);
            if (
              !input.clientRequestId ||
              !input.controlToken ||
              (input.action === "replace"
                ? !input.objective?.trim()
                : input.objective !== undefined)
            )
              return yield* new OrchestratorMcpFailure({
                code: "invalid_request",
                message:
                  "Goal mutations require the controlToken from read and a stable clientRequestId; only replace accepts and requires objective.",
              });
          } else if (input.objective !== undefined)
            return yield* new OrchestratorMcpFailure({
              code: "invalid_request",
              message: "Only replace accepts objective.",
            });
          const provider = context.projection.providerThreads.find(
            (p) => p.id === thread.activeProviderThreadId,
          );
          if (provider?.driver === "claudeAgent")
            return answer(
              mutation ? "unsupported" : "observed",
              "claude_sdk_controls_unavailable",
              { source: "claude_transcript", goal: provider.goal ?? null },
            );
          if (provider?.driver !== "codex")
            return answer("unsupported", "provider_goal_controls_unsupported");
          if (
            thread.archivedAt !== null ||
            provider.providerSessionId === null ||
            Option.isNone(sessions)
          )
            return answer("unavailable", "session_unavailable");
          if (provider.nativeThreadRef?.driver !== "codex" || !provider.nativeThreadRef.nativeId)
            return answer("unavailable", "native_binding_missing");
          const runtimeOption = yield* sessions.value
            .get(provider.providerSessionId)
            .pipe(Effect.mapError(unavailable));
          if (Option.isNone(runtimeOption)) return answer("unavailable", "session_stopped");
          const runtime = runtimeOption.value;
          if (
            runtime.instanceId !== thread.providerInstanceId ||
            runtime.instanceId !== provider.providerInstanceId
          )
            return answer("stale", "instance_changed");
          if (runtime.controlGoal === undefined)
            return answer("unsupported", "runtime_controls_unsupported");
          const bindingKey = thread.id;
          let binding = bindings.get(bindingKey);
          if (
            binding === undefined ||
            binding.runtime !== runtime ||
            !sameBinding(binding.provider, provider)
          ) {
            if (mutation) return answer("stale", "read_current_goal_before_mutation");
            binding = {
              token: yield* crypto.randomUUIDv4().pipe(Effect.orDie),
              provider,
              runtime,
              receipts: new Map(),
              uncertain: false,
            };
            bindings.set(bindingKey, binding);
          }
          if (mutation && input.controlToken !== binding.token)
            return answer("stale", "control_token_expired");
          const fingerprint = [input.action, input.objective ?? ""]
            .map(encodeURIComponent)
            .join(":");
          const key = [scope.requestNamespace, input.clientRequestId ?? ""]
            .map(encodeURIComponent)
            .join(":");
          const prior = mutation ? binding.receipts.get(key) : undefined;
          if (prior !== undefined) {
            if (prior.fingerprint !== fingerprint)
              return yield* new OrchestratorMcpFailure({
                code: "invalid_request",
                message: "clientRequestId was already used for a different goal operation.",
              });
            return { ...prior.result, replayed: true };
          }
          if (mutation && binding.uncertain)
            return answer("unknown", "prior_mutation_unresolved", { controlToken: binding.token });
          const revalidate = Effect.gen(function* () {
            const fresh = yield* readThread(thread.id, ["providerThreads"]);
            if (
              fresh.projection.thread.archivedAt !== null ||
              fresh.projection.thread.providerInstanceId !== thread.providerInstanceId ||
              !sameBinding(
                provider,
                fresh.projection.providerThreads.find(
                  (p) => p.id === fresh.projection.thread.activeProviderThreadId,
                ),
              )
            )
              return false;
            if (mutation) {
              yield* assertLiveCaller(fresh);
              if (scope.thread !== undefined) {
                const callerRecords = yield* threads.getThreadRecords(scope.thread.threadId, [
                  "providerThreads",
                ]);
                const callerProvider = callerRecords.providerThreads.find(
                  (p) => p.id === callerRecords.thread.activeProviderThreadId,
                );
                if (
                  callerProvider?.providerSessionId !== scope.thread.providerSessionId ||
                  callerProvider.providerInstanceId !== scope.thread.providerInstanceId
                )
                  return false;
              }
              yield* assertTargetWithinLimits(fresh.limits, fresh.projection.thread);
            }
            const current = yield* sessions.value.get(provider.providerSessionId!);
            return Option.isSome(current) && current.value === runtime;
          }).pipe(
            Effect.orElseSucceed(() => false),
            Effect.provideService(Invocation.McpInvocationContext, scope),
            Effect.provideService(ThreadManagement.ThreadManagementService, threads),
          );
          const observed = yield* runtime.controlGoal({
            providerThread: provider,
            action: "read",
            revalidate,
          });
          const base = {
            source: "native_rpc" as const,
            goal: observed.goal,
            observedAt: DateTime.formatIso(yield* DateTime.now),
            controlToken: binding.token,
          };
          if (observed.outcome !== "observed")
            return answer(observed.outcome, observed.reason, base);
          if (!mutation) {
            for (const receipt of binding.receipts.values()) {
              if (receipt.result.outcome !== "unknown") continue;
              if (receipt.result.action === "resume") {
                if (receipt.startRequestId !== undefined && Option.isSome(starts)) {
                  const reconciled = yield* starts.value.observe(thread.id, receipt.startRequestId);
                  receipt.result = {
                    ...receipt.result,
                    ...reconciled,
                    observedAt: base.observedAt,
                  };
                }
                continue;
              }
              const confirmed =
                receipt.result.action === "clear"
                  ? observed.goal === null
                  : receipt.result.action === "pause"
                    ? observed.goal?.status === "paused"
                    : receipt.result.action === "replace" &&
                      observed.goal?.objective === receipt.objective &&
                      observed.goal?.status === receipt.previousStatus;
              if (confirmed)
                receipt.result = {
                  ...receipt.result,
                  outcome: "applied",
                  reason: "reconciled_by_native_read",
                  goal: observed.goal,
                  observedAt: now,
                };
            }
            binding.uncertain = [...binding.receipts.values()].some(
              (receipt) => receipt.result.outcome === "unknown",
            );
            return answer(
              "observed",
              binding.uncertain ? "native_read_prior_mutation_unresolved" : observed.reason,
              base,
            );
          }
          if (!(yield* revalidate)) return answer("stale", "binding_or_access_changed", base);
          const active = context.projection.runs.some((run) =>
            ["preparing", "queued", "starting", "running", "waiting"].includes(run.status),
          );
          if (input.action === "resume" && active)
            return answer("busy", "run_already_active", base);
          if (input.action === "resume" && observed.goal === null)
            return answer("unavailable", "goal_missing", base);
          const receipt: Receipt = {
            fingerprint,
            ...(input.action === "resume"
              ? {
                  startRequestId: [thread.id, key].map(encodeURIComponent).join(":"),
                }
              : {}),
            ...(input.objective === undefined ? {} : { objective: input.objective }),
            ...(observed.goal === null ? {} : { previousStatus: observed.goal.status }),
            result: answer("unknown", "mutation_in_progress_or_acknowledgement_lost", base),
          };
          // Session-bound receipts survive caller cancellation; a restart invalidates every old token.
          binding.receipts.set(key, receipt);
          binding.uncertain = true;
          if (input.action === "resume" && !active) {
            if (Option.isNone(starts))
              receipt.result = answer("unavailable", "owned_start_unavailable", base);
            else {
              const captured = yield* starts.value.capture(thread.id).pipe(Effect.result);
              if (captured._tag === "Failure")
                receipt.result = answer("unavailable", "start_guard_unavailable", base);
              else if (captured.success === null || !(yield* revalidate))
                receipt.result = answer("busy", "target_changed_before_start", base);
              else {
                const refused = yield* Ref.make<DispatchModeRefusal | undefined>(undefined);
                const started = yield* starts.value
                  .start({
                    thread,
                    guard: captured.success,
                    requestId: receipt.startRequestId!,
                    ...(scope.thread === undefined
                      ? {}
                      : { senderThreadId: scope.thread.threadId }),
                    text: "/goal resume",
                  })
                  .pipe(Effect.provideService(DispatchModeLimit, { ...context.limits, refused }));
                receipt.result = answer(started.outcome, started.reason, {
                  ...base,
                  ...(started.runId === undefined ? {} : { runId: started.runId }),
                });
              }
            }
          } else {
            const controlled = yield* runtime.controlGoal({
              providerThread: provider,
              action: input.action as "pause" | "clear" | "replace",
              ...(input.objective === undefined ? {} : { objective: input.objective }),
              revalidate,
            });
            receipt.result = answer(controlled.outcome, controlled.reason, {
              ...base,
              observedAt: DateTime.formatIso(yield* DateTime.now),
              goal: controlled.goal,
            });
          }
          binding.uncertain = receipt.result.outcome === "unknown";
          return receipt.result;
        }),
      )
      .pipe(
        Effect.provideService(Invocation.McpInvocationContext, scope),
        Effect.provideService(ThreadManagement.ThreadManagementService, threads),
      );
  return GoalControl.of({ execute });
});
export const layer = Layer.effect(GoalControl, make);

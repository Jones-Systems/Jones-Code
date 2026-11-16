import {
  OrchestratorMcpFailure,
  RuntimeIdentityAttestation,
  type OrchestrationV2ProviderThread,
  type ThreadGoalInput,
  type ThreadGoalResult,
} from "@t3tools/contracts";
import type { ProviderAdapterV2SessionRuntime } from "@t3tools/provider-core/server/ProviderAdapter";
import * as Context from "effect/Context";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Deferred from "effect/Deferred";
import * as Stream from "effect/Stream";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
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
import * as SqlClient from "effect/sql/SqlClient";
import { readEventLogPrunedThrough } from "../persistence/EventLogPruneBoundary.ts";
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
  left.appThreadId === right.appThreadId &&
  left.driver === right.driver &&
  left.providerSessionId === right.providerSessionId &&
  left.providerInstanceId === right.providerInstanceId &&
  left.nativeThreadRef?.nativeId === right.nativeThreadRef?.nativeId &&
  left.nativeThreadRef?.driver === right.nativeThreadRef?.driver &&
  left.nativeThreadRef?.strength === right.nativeThreadRef?.strength &&
  sameIdentity(left.runtimeIdentity, right.runtimeIdentity);
interface Receipt {
  readonly fingerprint: string;
  readonly objective?: string;
  readonly previousStatus?: string;
  startRequestId?: string;
  result: ThreadGoalResult;
}
interface Binding {
  readonly token: string;
  readonly provider: OrchestrationV2ProviderThread;
  // Undefined once the session stopped: the runtime reference is released, but the
  // receipts stay so a retried key still resolves to a stale answer, never a new mutation.
  readonly runtime: ProviderAdapterV2SessionRuntime | undefined;
  readonly receipts: Map<string, Receipt>;
  uncertain: boolean;
  controlOrder: number;
  intent?: {
    readonly done: Deferred.Deferred<void>;
    readonly receipt: Receipt;
    readonly order: number;
  };
}

const sameConversation = (
  binding: Binding,
  provider: OrchestrationV2ProviderThread,
  runtime: ProviderAdapterV2SessionRuntime,
) =>
  binding.runtime === runtime &&
  binding.provider.nativeThreadRef?.strength === "strong" &&
  binding.provider.runtimeIdentity?.runtimeGeneration !== undefined &&
  sameBinding(
    {
      ...binding.provider,
      runtimeIdentity: {
        ...binding.provider.runtimeIdentity,
        evidenceRevision: provider.runtimeIdentity?.evidenceRevision,
      },
    },
    provider,
  );

const make = Effect.gen(function* () {
  const threads = yield* ThreadManagement.ThreadManagementService;
  const sessions = yield* Effect.serviceOption(Sessions.ProviderSessionManagerV2);
  const starts = yield* Effect.serviceOption(GoalStart.GoalStartDispatch);
  const crypto = yield* SecureRandom.SecureRandom;
  const events = yield* Effect.serviceOption(EventSink.EventSinkV2);
  const history = yield* EventStore.EventStoreV2;
  const sql = yield* SqlClient.SqlClient;
  // MCP registrations are built once by layerHttp, outside individual invocation scopes.
  const serviceScope = yield* Effect.scope;
  type Disposition =
    | "control_superseded"
    | "stop_requested"
    | "owner_run_superseded"
    | "caller_did_not_complete"
    | "binding_or_access_changed"
    | "goal_changed"
    | "event_subscription_failed"
    | "event_history_incomplete"
    | "dispatch_attempts_exhausted";
  const dispose = Effect.fn("GoalControl.disposeResume")(function* (
    binding: Binding,
    reason: Disposition,
  ) {
    const intent = binding.intent;
    if (intent === undefined) return;
    delete binding.intent;
    intent.receipt.result = {
      ...intent.receipt.result,
      outcome: intent.receipt.result.outcome === "unknown" ? "unknown" : "stale",
      reason: `resume_discarded_${reason}`,
    };
    yield* Deferred.succeed(intent.done, undefined);
  });
  const bindings = new Map<string, Binding>();
  // One lock per target thread, so a slow native call on one thread never queues another.
  // An entry exists only while a call holds or awaits it.
  const locks = new Map<string, { readonly semaphore: Semaphore.Semaphore; holders: number }>();
  // Release the runtime reference held for a stopped or replaced session. Receipts are kept
  // so every reused clientRequestId still resolves as stale after the next read rebinds.
  const retire = Effect.fn("GoalControl.retire")(function* (key: string) {
    const binding = bindings.get(key);
    if (binding === undefined) return;
    yield* dispose(binding, "binding_or_access_changed");
    if (binding.receipts.size === 0) bindings.delete(key);
    else bindings.set(key, { ...binding, runtime: undefined });
  });
  // ProviderSessionManagerV2 publishes no stop event, so release stopped sessions' runtime
  // references on the next call. A thread with a call in flight is left to that call.
  const retireStopped = Effect.fn("GoalControl.retireStopped")(function* (currentKey: string) {
    if (Option.isNone(sessions)) return;
    for (const [key, binding] of bindings) {
      const sessionId = binding.provider.providerSessionId;
      if (
        sessionId === null ||
        key === currentKey ||
        binding.runtime === undefined ||
        locks.has(key)
      )
        continue;
      const found = yield* sessions.value
        .get(sessionId, { touchActivity: false })
        .pipe(Effect.option);
      if (Option.isNone(found)) continue; // lookup failed: keep the binding
      // Re-check after the lookup: a call may have started or rebound this thread.
      if (
        Option.getOrUndefined(found.value) !== binding.runtime &&
        bindings.get(key) === binding &&
        !locks.has(key)
      )
        yield* retire(key);
    }
  });
  const perThread =
    (key: string) =>
    <A, E, R>(effect: Effect.Effect<A, E, R>) =>
      retireStopped(key).pipe(
        Effect.andThen(
          Effect.acquireUseRelease(
            Effect.sync(() => {
              let entry = locks.get(key);
              if (entry === undefined) {
                entry = { semaphore: Semaphore.makeUnsafe(1), holders: 0 };
                locks.set(key, entry);
              }
              entry.holders += 1;
              return entry;
            }),
            (entry) => entry.semaphore.withPermits(1)(effect),
            (entry) =>
              Effect.sync(() => {
                entry.holders -= 1;
                if (entry.holders === 0 && locks.get(key) === entry) locks.delete(key);
              }),
          ),
        ),
      );
  const execute = (scope: Invocation.McpInvocationScope, input: ThreadGoalInput) =>
    perThread(input.threadId ?? scope.thread?.threadId ?? "")(
      Effect.gen(function* () {
        const targetId = input.threadId ?? scope.thread?.threadId;
        const eventMark = Option.isSome(events)
          ? yield* events.value
              .latestSequence(targetId === undefined ? {} : { threadId: targetId })
              .pipe(Effect.mapError(unavailable))
          : undefined;
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
            (input.action === "replace" ? !input.objective?.trim() : input.objective !== undefined)
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
          return answer(mutation ? "unsupported" : "observed", "claude_sdk_controls_unavailable", {
            source: "claude_transcript",
            goal: provider.goal ?? null,
          });
        if (provider?.driver !== "codex")
          return answer("unsupported", "provider_goal_controls_unsupported");
        if (
          thread.archivedAt !== null ||
          provider.providerSessionId === null ||
          Option.isNone(sessions)
        ) {
          yield* retire(thread.id);
          return answer("unavailable", "session_unavailable");
        }
        if (provider.nativeThreadRef?.driver !== "codex" || !provider.nativeThreadRef.nativeId)
          return answer("unavailable", "native_binding_missing");
        const runtimeOption = yield* sessions.value
          .get(provider.providerSessionId)
          .pipe(Effect.mapError(unavailable));
        if (Option.isNone(runtimeOption)) {
          yield* retire(thread.id);
          return answer("unavailable", "session_stopped");
        }
        const runtime = runtimeOption.value;
        if (
          runtime.instanceId !== thread.providerInstanceId ||
          runtime.instanceId !== provider.providerInstanceId
        ) {
          yield* retire(thread.id);
          return answer("stale", "instance_changed");
        }
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
          const token = yield* crypto.uuidV4();
          const retain = binding !== undefined && sameConversation(binding, provider, runtime);
          // Preserve rejected keys across an ownership change so durable command dedupe
          // cannot attribute an old conversation's run to its replacement.
          const receipts = retain
            ? binding!.receipts
            : new Map(
                [...(binding?.receipts ?? [])].map(([key, receipt]) => [
                  key,
                  {
                    fingerprint: receipt.fingerprint,
                    result: answer("stale", "goal_binding_changed", {
                      action: receipt.result.action,
                      controlToken: token,
                    }),
                  },
                ]),
              );
          if (binding !== undefined) yield* dispose(binding, "binding_or_access_changed");
          binding = {
            controlOrder: 0,
            token,
            provider,
            runtime,
            receipts,
            uncertain: retain && binding!.uncertain,
          };
          bindings.set(bindingKey, binding);
        }
        if (mutation && input.controlToken !== binding.token)
          return answer("stale", "control_token_expired");
        const fingerprint = [input.action, input.objective ?? ""].map(encodeURIComponent).join(":");
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
          return { ...prior.result, controlToken: binding.token, replayed: true };
        }
        if (mutation) {
          binding.controlOrder += 1;
          yield* dispose(binding, "control_superseded");
        }
        if (mutation && binding.uncertain)
          return answer("unknown", "prior_mutation_unresolved", { controlToken: binding.token });
        const revalidation = Effect.fn("GoalControl.revalidate")(
          function* (deferred: boolean) {
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
              // Only the captured self-intent may outlive its run. All other gates remain.
              if (!deferred) yield* assertLiveCaller(fresh);
              yield* assertTargetWithinLimits(context.limits, fresh.projection.thread);
              if (scope.thread !== undefined) {
                const callerRecords = yield* threads.getThreadRecords(scope.thread.threadId, [
                  "providerThreads",
                ]);
                const callerProvider = callerRecords.providerThreads.find(
                  (p) => p.id === callerRecords.thread.activeProviderThreadId,
                );
                if (
                  callerProvider === undefined ||
                  callerProvider.providerSessionId === null ||
                  callerProvider.providerInstanceId !== scope.thread.providerInstanceId
                )
                  return false;
                if (
                  !(yield* sessions.value.isMcpCallerAttached({
                    threadId: scope.thread.threadId,
                    providerSessionId: callerProvider.providerSessionId,
                    providerInstanceId: scope.thread.providerInstanceId,
                    mcpCredentialId: scope.thread.providerSessionId,
                  }))
                )
                  return false;
              }
              yield* assertTargetWithinLimits(fresh.limits, fresh.projection.thread);
            }
            const current = yield* sessions.value.get(provider.providerSessionId!);
            return Option.isSome(current) && current.value === runtime;
          },
          (effect) =>
            effect.pipe(
              Effect.orElseSucceed(() => false),
              Effect.provideService(Invocation.McpInvocationContext, scope),
              Effect.provideService(ThreadManagement.ThreadManagementService, threads),
            ),
        );
        const revalidate = revalidation(false);
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
        if (observed.outcome !== "observed") return answer(observed.outcome, observed.reason, base);
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
        if (input.action === "resume" && active) {
          const owner = context.projection.runs.find(
            (run) => run.id === context.caller?.activeRunId,
          );
          if (
            scope.thread?.threadId !== thread.id ||
            owner === undefined ||
            owner.providerThreadId !== provider.id ||
            owner.providerInstanceId !== scope.thread.providerInstanceId ||
            !["preparing", "queued", "starting", "running", "waiting"].includes(owner.status) ||
            !["paused", "blocked"].includes(observed.goal?.status ?? "") ||
            Option.isNone(events) ||
            Option.isNone(starts) ||
            eventMark === undefined
          )
            return answer("busy", "run_already_active", base);
          const receipt: Receipt = {
            fingerprint,
            startRequestId: [thread.id, key].map(encodeURIComponent).join(":"),
            result: answer("busy", "resume_armed_session_local_until_idle", base),
          };
          binding.receipts.set(key, receipt);
          const capturedBinding = binding;
          const intent = {
            done: yield* Deferred.make<void>(),
            receipt,
            order: binding.controlOrder,
          };
          binding.intent = intent;
          const objective = observed.goal!.objective;
          const deferredValidation = revalidation(true);
          let afterSequence = eventMark;
          let attempts = 0;
          let observing = false;
          const originalRequestId = receipt.startRequestId!;
          const sink = events.value;
          const historyStore = history;
          const dispatch = starts.value;
          const complete = Effect.fn("GoalControl.completeResume")(function* (
            result: ThreadGoalResult,
          ) {
            receipt.result = result;
            capturedBinding.uncertain = result.outcome === "unknown";
            if (result.outcome !== "unknown") {
              delete capturedBinding.intent;
              yield* Deferred.succeed(intent.done, undefined);
            }
          });
          // The replay mark is captured before the request projection. Subscribe-before-replay
          // in EventSink closes the terminal-event/registration race while history is retained.
          // Prove the finite replay complete before evaluating projected state or moving its cursor.
          const sweep = Effect.fn("GoalControl.sweepResumeEvents")(function* () {
            const checked = afterSequence;
            const highWater = yield* sink.latestSequence({ threadId: thread.id });
            if (highWater < checked)
              return yield* dispose(capturedBinding, "event_history_incomplete");
            if (highWater === checked) return;
            if (checked < (yield* readEventLogPrunedThrough(sql)))
              return yield* dispose(capturedBinding, "event_history_incomplete");
            // The live stream omits private bootstrap events. Finite stored replay must
            // not wait for a public event to reach a private high-water mark.
            const stored = yield* historyStore
              .read({ threadId: thread.id, afterSequence: checked, throughSequence: highWater })
              .pipe(Stream.runCollect);
            // The pruner publishes its global boundary before deleting. Compare with the original
            // cursor again: retained later rows cannot prove an earlier Stop was absent.
            if (checked < (yield* readEventLogPrunedThrough(sql)))
              return yield* dispose(capturedBinding, "event_history_incomplete");
            for (const { event } of stored) {
              if (
                event.type === "turn-item.updated" &&
                event.payload.type === "run_interrupt_request"
              )
                yield* dispose(capturedBinding, "stop_requested");
              if (
                event.type === "run.updated" &&
                event.payload.id === owner.id &&
                !["preparing", "queued", "starting", "running", "waiting", "completed"].includes(
                  event.payload.status,
                )
              )
                yield* dispose(capturedBinding, "caller_did_not_complete");
              if (event.type === "provider-thread.updated" && event.payload.id === provider.id) {
                if (!sameBinding(provider, event.payload))
                  yield* dispose(capturedBinding, "binding_or_access_changed");
                else if (
                  event.payload.goal === null ||
                  (event.payload.goal !== undefined && event.payload.goal.objective !== objective)
                )
                  yield* dispose(capturedBinding, "goal_changed");
              }
              if (
                event.type === "thread.archived" ||
                (event.type === "provider-session.detach-requested" &&
                  event.payload.providerSessionId === provider.providerSessionId) ||
                (event.type === "provider-session.detached" &&
                  event.payload.providerSessionId === provider.providerSessionId)
              )
                yield* dispose(capturedBinding, "binding_or_access_changed");
            }
            afterSequence = highWater;
          });
          const evaluate = Effect.fn("GoalControl.consumeResume")(function* () {
            if (capturedBinding.intent !== intent) return;
            yield* sweep();
            if (capturedBinding.intent !== intent) return;
            if (
              bindings.get(thread.id) !== capturedBinding ||
              capturedBinding.controlOrder !== intent.order ||
              !(yield* deferredValidation)
            )
              return yield* dispose(capturedBinding, "binding_or_access_changed");
            const fresh = yield* threads.getThreadRecords(thread.id, ["runs", "messages"]);
            const currentOwner = fresh.runs.find((run) => run.id === owner.id);
            if (
              currentOwner === undefined ||
              !["preparing", "queued", "starting", "running", "waiting", "completed"].includes(
                currentOwner.status,
              )
            )
              return yield* dispose(capturedBinding, "caller_did_not_complete");
            for (const run of fresh.runs) {
              if (run.ordinal <= owner.ordinal) continue;
              const message = fresh.messages.find((message) => message.id === run.userMessageId);
              // Only persisted, server-authored result metadata proves a non-owner wake.
              const resultWake =
                message?.createdBy === "agent" &&
                message.creationSource === "server" &&
                (message.delegatedCompletion !== undefined ||
                  (message.notification !== undefined &&
                    message.notification.outcome !== "updated" &&
                    ["subagent", "background_task", "monitor"].includes(
                      message.notification.source.kind,
                    )));
              if (!resultWake && run.userMessageId !== `goal:${receipt.startRequestId}`)
                return yield* dispose(capturedBinding, "owner_run_superseded");
            }
            if (observing) {
              const result = yield* dispatch.observe(thread.id, receipt.startRequestId!);
              return yield* complete(answer(result.outcome, result.reason, { ...base, ...result }));
            }
            const guard = yield* dispatch.capture(thread.id);
            if (guard === null) return;
            const native = yield* runtime.controlGoal!({
              providerThread: provider,
              action: "read",
              revalidate: deferredValidation,
            });
            if (native.outcome !== "observed")
              return yield* dispose(capturedBinding, "binding_or_access_changed");
            if (native.goal === null || native.goal.objective !== objective)
              return yield* dispose(capturedBinding, "goal_changed");
            yield* sweep();
            if (capturedBinding.intent !== intent) return;
            if (!(yield* deferredValidation))
              return yield* dispose(capturedBinding, "binding_or_access_changed");
            if (native.goal.status === "active")
              return yield* complete(
                answer("observed", "resume_satisfied_by_native_read", {
                  ...base,
                  goal: native.goal,
                }),
              );
            if (!["paused", "blocked"].includes(native.goal.status))
              return yield* dispose(capturedBinding, "goal_changed");
            attempts += 1;
            // A proved rejected command is immutable. Subsequent no-effect attempts remain
            // correlated to the original key; an unknown attempt never advances this suffix.
            receipt.startRequestId =
              attempts === 1 ? originalRequestId : `${originalRequestId}:attempt:${attempts}`;
            const refused = yield* Ref.make<DispatchModeRefusal | undefined>(undefined);
            observing = true;
            receipt.result = answer("unknown", "dispatch_readback_unresolved", base);
            capturedBinding.uncertain = true;
            const result = yield* dispatch
              .start({
                thread: fresh.thread,
                guard,
                requestId: receipt.startRequestId,
                senderThreadId: thread.id,
                text: "/goal resume",
              })
              .pipe(
                Effect.provideService(DispatchModeLimit, { ...context.limits, refused }),
                Effect.orElseSucceed(() => ({
                  outcome: "unknown" as const,
                  reason: "dispatch_readback_unresolved",
                })),
              );
            if (result.outcome === "busy") {
              observing = false;
              capturedBinding.uncertain = false;
              receipt.result = answer("busy", "resume_armed_session_local_until_idle", base);
              if (attempts >= 3) yield* dispose(capturedBinding, "dispatch_attempts_exhausted");
              return;
            }
            observing = result.outcome === "unknown";
            yield* complete(answer(result.outcome, result.reason, { ...base, ...result }));
          });
          yield* sink.stream({ threadId: thread.id, afterSequence: eventMark }).pipe(
            Stream.runForEach(() => perThread(thread.id)(evaluate())),
            Effect.catch(() =>
              perThread(thread.id)(dispose(capturedBinding, "event_subscription_failed")),
            ),
            Effect.raceFirst(Deferred.await(intent.done)),
            Effect.provideService(Invocation.McpInvocationContext, scope),
            Effect.provideService(ThreadManagement.ThreadManagementService, threads),
            Effect.forkIn(serviceScope),
          );
          return receipt.result;
        }
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
        // Receipts survive caller cancellation; every rebind still expires old control tokens.
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
                  ...(scope.thread === undefined ? {} : { senderThreadId: scope.thread.threadId }),
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
    ).pipe(
      Effect.provideService(Invocation.McpInvocationContext, scope),
      Effect.provideService(ThreadManagement.ThreadManagementService, threads),
    );
  return GoalControl.of({ execute });
});
export const layerFromEventStore = Layer.effect(GoalControl, make);
export const layer = layerFromEventStore.pipe(Layer.provide(EventStore.layer));

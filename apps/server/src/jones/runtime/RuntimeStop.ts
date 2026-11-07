import * as RestartPreparation from "../orchestration/RestartPreparation.ts";
import {
  EnvironmentAuthenticatedPrincipal,
  StopCurrentThreadRuntimeInput,
  type StopCurrentThreadRuntimeResult,
  type ReadCurrentRuntimeStopTargetResult,
  EventId,
  type CommandId,
  type ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Schedule from "effect/Schedule";
import * as AuthSessions from "../../persistence/AuthSessions.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as ProviderSessions from "../../orchestration-v2/ProviderSessionManager.ts";
import type { CapturedRuntimeStop } from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as StopStore from "./RuntimeStopSqlite.ts";
import {
  nativeCreationCanonicalJson,
  nativeCreationSha256,
} from "../nativeCreation/NativeCreationPreparation.ts";

export const RUNTIME_STOP_WAIT_TIMEOUT_MS = 3_000;

export class CurrentRuntimeStop extends Context.Service<
  CurrentRuntimeStop,
  {
    readonly stop: (
      input: StopCurrentThreadRuntimeInput,
    ) => Effect.Effect<
      StopCurrentThreadRuntimeResult,
      StopStore.RuntimeStopError,
      EnvironmentAuthenticatedPrincipal
    >;
    readonly readTarget: (
      threadId: ThreadId,
    ) => Effect.Effect<
      ReadCurrentRuntimeStopTargetResult,
      StopStore.RuntimeStopError,
      EnvironmentAuthenticatedPrincipal
    >;
    readonly observe: (
      input: StopCurrentThreadRuntimeInput,
    ) => Effect.Effect<
      StopCurrentThreadRuntimeResult | null,
      StopStore.RuntimeStopError,
      EnvironmentAuthenticatedPrincipal
    >;
    readonly execute: (commandId: CommandId) => Effect.Effect<void, StopStore.RuntimeStopError>;
  }
>()("t3/jones/runtime/RuntimeStop/CurrentRuntimeStop") {}
const compiledIs1 = Schema.is(StopStore.RuntimeStopError);
const compiledDecodeEffect2 = Schema.decodeEffect(StopCurrentThreadRuntimeInput);

const make = Effect.gen(function* () {
  const eventSink = yield* EventSink.EventSinkV2;
  const restartPreparation = yield* RestartPreparation.RestartPreparation;
  const manager = yield* ProviderSessions.ProviderSessionManagerV2;
  const sessions = yield* AuthSessions.AuthSessionRepository;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const scope = yield* Effect.scope;
  const captures = new Map<CommandId, CapturedRuntimeStop>();
  const workers = new Map<CommandId, Deferred.Deferred<void, StopStore.RuntimeStopError>>();
  const error = (reason: string, cause?: unknown) =>
    new StopStore.RuntimeStopError({ reason, ...(cause === undefined ? {} : { cause }) });
  const canObserveOrInitiate = Effect.fn("CurrentRuntimeStop.canObserveOrInitiate")(function* (
    capture: CapturedRuntimeStop,
  ) {
    if (capture.retirement !== undefined && (yield* capture.retirement.started)) return true;
    return yield* capture.isCurrent;
  });
  const awaitWorker = Effect.fn("CurrentRuntimeStop.awaitWorker")(
    (worker: Deferred.Deferred<void, StopStore.RuntimeStopError>) => Deferred.await(worker),
    (effect) =>
      effect.pipe(
        Effect.timeoutOrElse({
          duration: RUNTIME_STOP_WAIT_TIMEOUT_MS,
          orElse: () => Effect.fail(error("runtime_stop_retirement_pending")),
        }),
      ),
  );
  const readTarget = Effect.fn("CurrentRuntimeStop.readTarget")(
    function* (threadId: ThreadId) {
      const principal = yield* EnvironmentAuthenticatedPrincipal;
      const session = yield* sessions.getById({ sessionId: principal.sessionId });
      const now = yield* DateTime.now;
      if (
        Option.isNone(session) ||
        session.value.revokedAt !== null ||
        session.value.subject !== principal.subject ||
        session.value.method !== principal.method ||
        !session.value.scopes.includes("orchestration:read") ||
        !principal.scopes.has("orchestration:read") ||
        DateTime.toEpochMillis(session.value.expiresAt) <= DateTime.toEpochMillis(now)
      )
        return yield* error("current_stop_actor_unavailable");
      if (!manager.captureCurrentThreadRuntimeStop || !eventSink.readRuntimeStop)
        return {
          status: "unavailable" as const,
          reason: "captured_runtime_stop_unavailable",
          backgroundCoverage: "partial" as const,
        };
      const held = yield* restartPreparation.captureHeldRuntimeStop(threadId);
      const captured = held ?? (yield* manager.captureCurrentThreadRuntimeStop(threadId));
      if (captured === null || !(yield* canObserveOrInitiate(captured)))
        return {
          status: "unavailable" as const,
          reason: "physical_runtime_stop_capture_unavailable",
          backgroundCoverage: "partial" as const,
        };
      return {
        status: "available" as const,
        target: { binding: captured.binding, evidenceRevision: captured.evidenceRevision },
        backgroundCoverage: "partial" as const,
      };
    },
    Effect.mapError((cause) =>
      compiledIs1(cause) ? cause : error("runtime_stop_target_unavailable"),
    ),
  );
  const observe = Effect.fn("CurrentRuntimeStop.observe")(
    function* (candidate: StopCurrentThreadRuntimeInput) {
      const principal = yield* EnvironmentAuthenticatedPrincipal;
      const input = yield* compiledDecodeEffect2(candidate, {
        onExcessProperty: "error",
      }).pipe(Effect.mapError(() => error("invalid_stop_observation")));
      if (input.target.binding.threadId !== input.threadId)
        return yield* error("stop_target_thread_conflict");
      const session = yield* sessions.getById({ sessionId: principal.sessionId });
      const now = yield* DateTime.now;
      if (
        Option.isNone(session) ||
        session.value.revokedAt !== null ||
        session.value.subject !== principal.subject ||
        session.value.method !== principal.method ||
        !session.value.scopes.includes("orchestration:read") ||
        !principal.scopes.has("orchestration:read") ||
        DateTime.toEpochMillis(session.value.expiresAt) <= DateTime.toEpochMillis(now)
      )
        return yield* error("current_stop_actor_unavailable");
      if (!eventSink.readRuntimeStop) return yield* error("runtime_stop_observation_unavailable");
      const prior = yield* eventSink.readRuntimeStop(input.commandId);
      if (prior === null) return null;
      const actorDigest = nativeCreationSha256(
        nativeCreationCanonicalJson({
          sessionId: principal.sessionId,
          subject: principal.subject,
          method: principal.method,
        }),
      );
      if (
        prior.identity.actorDigest !== actorDigest ||
        nativeCreationCanonicalJson(prior.identity.request) !== nativeCreationCanonicalJson(input)
      )
        return yield* error("stop_observation_identity_conflict");
      const worker = workers.get(input.commandId);
      if (worker !== undefined) yield* awaitWorker(worker).pipe(Effect.ignore);
      const current = yield* eventSink.readRuntimeStop(input.commandId);
      return {
        commandId: input.commandId,
        threadId: input.threadId,
        status: current?.status ?? prior.status,
        affectedRunIds: prior.identity.affectedRunIds,
        backgroundCoverage: "partial" as const,
      };
    },
    Effect.mapError((cause) =>
      compiledIs1(cause) ? cause : error("runtime_stop_observation_unavailable", cause),
    ),
  );
  const stop = Effect.fn("CurrentRuntimeStop.stop")(
    function* (candidate: StopCurrentThreadRuntimeInput) {
      const principal = yield* EnvironmentAuthenticatedPrincipal;
      const input = yield* compiledDecodeEffect2(candidate, {
        onExcessProperty: "error",
      }).pipe(Effect.mapError(() => error("invalid_stop_request")));
      if (input.target.binding.threadId !== input.threadId)
        return yield* error("stop_target_thread_conflict");
      if (
        !eventSink.readRuntimeStop ||
        !eventSink.startRuntimeStop ||
        !eventSink.completeRuntimeStop ||
        !manager.captureCurrentThreadRuntimeStop
      )
        return yield* error("captured_runtime_stop_unavailable");
      const authorize = Effect.gen(function* () {
        const session = yield* sessions.getById({ sessionId: principal.sessionId });
        const now = yield* DateTime.now;
        if (
          Option.isNone(session) ||
          session.value.revokedAt !== null ||
          session.value.subject !== principal.subject ||
          session.value.method !== principal.method ||
          !session.value.scopes.includes("orchestration:operate") ||
          !principal.scopes.has("orchestration:operate") ||
          DateTime.toEpochMillis(session.value.expiresAt) <= DateTime.toEpochMillis(now)
        )
          return yield* error("current_stop_actor_unavailable");
      }).pipe(Effect.mapError(() => error("current_stop_actor_unavailable")));
      yield* authorize;
      const actorDigest = nativeCreationSha256(
        nativeCreationCanonicalJson({
          sessionId: principal.sessionId,
          subject: principal.subject,
          method: principal.method,
        }),
      );
      const prior = yield* eventSink.readRuntimeStop(input.commandId);
      if (prior !== null) {
        if (
          prior.identity.actorDigest !== actorDigest ||
          nativeCreationCanonicalJson(prior.identity.request) !== nativeCreationCanonicalJson(input)
        )
          return yield* error("stop_command_identity_conflict");
        return {
          commandId: input.commandId,
          threadId: input.threadId,
          status: prior.status,
          affectedRunIds: prior.identity.affectedRunIds,
          backgroundCoverage: "partial" as const,
        };
      }
      const held = yield* restartPreparation.captureHeldRuntimeStop(input.threadId);
      const captured = held ?? (yield* manager.captureCurrentThreadRuntimeStop(input.threadId));
      if (
        captured === null ||
        nativeCreationCanonicalJson({
          binding: captured.binding,
          evidenceRevision: captured.evidenceRevision,
        }) !== nativeCreationCanonicalJson(input.target)
      )
        return yield* error("physical_runtime_stop_capture_unavailable_or_changed");
      const revalidate = Effect.gen(function* () {
        yield* authorize;
        if (!(yield* canObserveOrInitiate(captured)))
          return yield* error("captured_runtime_replaced");
      });
      yield* revalidate;
      const projection = yield* projections.getThreadRecords(input.threadId, [
        "runs",
        "providerThreads",
      ]);
      const affectedRunIds = projection.runs
        .filter((run) => run.status === "queued" || run.status === "starting")
        .map((run) => run.id);
      const identity = {
        request: input,
        actor: {
          sessionId: principal.sessionId,
          subject: principal.subject,
          method: principal.method,
        },
        actorDigest,
        affectedRunIds,
      };
      const now = yield* DateTime.now;
      captures.set(input.commandId, captured);
      yield* eventSink
        .commitCommand({
          commandId: input.commandId,
          threadId: input.threadId,
          commandType: "provider-session.detach",
          acceptedAt: now,
          runtimeStop: { identity, revalidate },
          events: [
            {
              id: EventId.make(`event:${input.commandId}:captured-runtime-stop`),
              type: "provider-session.detach-requested",
              threadId: input.threadId,
              driver: captured.binding.driver,
              providerInstanceId: captured.binding.providerInstanceId,
              occurredAt: now,
              payload: { providerSessionId: captured.binding.providerSessionId },
            },
          ],
          effects: [
            {
              id: `effect:${input.commandId}:captured-runtime-stop`,
              commandId: input.commandId,
              threadId: input.threadId,
              request: {
                type: "provider-session.detach",
                providerSessionId: captured.binding.providerSessionId,
                runtimeStopCommandId: input.commandId,
              },
            },
          ],
          cancelUnsettledEffects: {
            effectTypes: ["provider-turn.start"],
            reason: "Captured runtime stop fences previous queued execution lineage.",
          },
        })
        .pipe(
          Effect.onExit((exit) =>
            Effect.sync(() => {
              if (Exit.isFailure(exit) && captures.get(input.commandId) === captured)
                captures.delete(input.commandId);
            }),
          ),
        );
      return {
        commandId: input.commandId,
        threadId: input.threadId,
        status: "accepted" as const,
        affectedRunIds,
        backgroundCoverage: "partial" as const,
      };
    },
    Effect.mapError((cause) =>
      compiledIs1(cause) ? cause : error("runtime_stop_acceptance_unavailable", cause),
    ),
  );
  const execute = Effect.fn("CurrentRuntimeStop.execute")(
    function* (commandId: CommandId) {
      if (
        !eventSink.readRuntimeStop ||
        !eventSink.startRuntimeStop ||
        !eventSink.completeRuntimeStop
      )
        return yield* error("runtime_stop_owner_unavailable");
      const state = yield* eventSink.readRuntimeStop(commandId);
      if (state === null) return yield* error("runtime_stop_has_no_acceptance");
      if (state.status === "stopped") return;
      const worker = workers.get(commandId);
      if (worker !== undefined) return yield* awaitWorker(worker);
      if (state.status === "unknown") return yield* error("runtime_stop_prior_completion_unknown");
      const capture = captures.get(commandId);
      if (!capture) return yield* error("runtime_stop_physical_capture_lost");
      const revalidate = Effect.gen(function* () {
        const actor = state.identity.actor;
        const session = yield* sessions.getById({ sessionId: actor.sessionId });
        const now = yield* DateTime.now;
        if (
          Option.isNone(session) ||
          session.value.revokedAt !== null ||
          session.value.subject !== actor.subject ||
          session.value.method !== actor.method ||
          !session.value.scopes.includes("orchestration:operate") ||
          DateTime.toEpochMillis(session.value.expiresAt) <= DateTime.toEpochMillis(now)
        )
          return yield* error("current_stop_actor_unavailable");
        if (!(yield* canObserveOrInitiate(capture)))
          return yield* error("captured_runtime_replaced");
      }).pipe(Effect.mapError(() => error("current_stop_actor_or_runtime_unavailable")));
      const retirement = capture.retirement;
      if (retirement !== undefined) {
        if (
          nativeCreationCanonicalJson(retirement.binding) !==
          nativeCreationCanonicalJson(capture.binding)
        )
          return yield* error("runtime_stop_retirement_identity_conflict");
        const pending = yield* Effect.uninterruptibleMask(() =>
          Effect.gen(function* () {
            const existing = workers.get(commandId);
            if (existing !== undefined) return existing;
            const pending = Deferred.makeUnsafe<void, StopStore.RuntimeStopError>();
            workers.set(commandId, pending);
            let invocationStarted = false;
            const retryObservation = Effect.fn("CurrentRuntimeStop.retryObservation")(
              function* <A, E>(observation: Effect.Effect<A, E>) {
                const attempt = yield* observation.pipe(Effect.forkChild);
                const result = yield* Fiber.await(attempt);
                if (Exit.isFailure(result))
                  return yield* error("runtime_stop_observation_pending", result.cause);
                return result.value;
              },
              Effect.retry({ schedule: Schedule.spaced("1 second") }),
            );
            // The service scope owns this worker; a caller's deadline or cancellation only stops its wait.
            yield* Effect.gen(function* () {
              const started = yield* eventSink.startRuntimeStop!(
                commandId,
                state.identity.request.target,
                revalidate,
              );
              if (!started) return yield* error("runtime_stop_prior_invocation_unknown");
              invocationStarted = true;
              if (!(yield* retirement.started)) {
                yield* revalidate;
                yield* Effect.exit(capture.stop);
              }
              const receipt = yield* retryObservation(retirement.awaitRetired);
              if (
                nativeCreationCanonicalJson(receipt.binding) !==
                nativeCreationCanonicalJson(capture.binding)
              )
                return yield* error("runtime_stop_retirement_identity_conflict");
              yield* retryObservation(eventSink.completeRuntimeStop!(commandId, "stopped"));
              yield* restartPreparation.resolveAfterTeardown(capture.binding);
              captures.delete(commandId);
            }).pipe(
              Effect.mapError((cause) =>
                compiledIs1(cause) ? cause : error("runtime_stop_execution_unavailable", cause),
              ),
              Effect.onExit((exit) =>
                Effect.sync(() => {
                  if (
                    (!invocationStarted || Exit.isSuccess(exit)) &&
                    workers.get(commandId) === pending
                  )
                    workers.delete(commandId);
                }).pipe(Effect.andThen(Deferred.done(pending, exit))),
              ),
              Effect.ignoreCause,
              Effect.forkIn(scope, { uninterruptible: false }),
            );
            return pending;
          }),
        );
        return yield* awaitWorker(pending);
      }
      yield* Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const started = yield* eventSink.startRuntimeStop!(
            commandId,
            state.identity.request.target,
            revalidate,
          );
          if (!started) return yield* error("runtime_stop_prior_invocation_unknown");
          const completion = yield* Effect.exit(
            restore(revalidate.pipe(Effect.andThen(capture.stop))),
          );
          yield* eventSink.completeRuntimeStop!(
            commandId,
            Exit.isSuccess(completion) ? "stopped" : "unknown",
          );
          captures.delete(commandId);
          if (Exit.isFailure(completion)) return yield* error("runtime_stop_completion_unknown");
          yield* restartPreparation.resolveAfterTeardown(capture.binding);
        }),
      );
    },
    Effect.mapError((cause) =>
      compiledIs1(cause) ? cause : error("runtime_stop_execution_unavailable"),
    ),
  );
  return CurrentRuntimeStop.of({ stop, execute, readTarget, observe });
});
export const layer = Layer.effect(CurrentRuntimeStop, make);

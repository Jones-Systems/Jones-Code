import * as RestartPreparation from "../orchestration/RestartPreparation.ts";
import {
  EnvironmentAuthenticatedPrincipal,
  StopCurrentThreadRuntimeInput,
  type StopCurrentThreadRuntimeResult,
  type ReadCurrentRuntimeStopTargetResult,
  EventId,
  ProviderRuntimeBinding,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2Run,
  type OrchestrationV2RunAttempt,
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
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as AuthSessions from "../../persistence/AuthSessions.ts";
import * as CommandReceipts from "../../orchestration-v2/CommandReceiptStore.ts";
import * as Outbox from "../../orchestration-v2/EffectOutbox.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as ProviderSessions from "../../orchestration-v2/ProviderSessionManager.ts";
import {
  runtimeBinding,
  type CapturedRuntimeStop,
} from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as StopStore from "./RuntimeStopSqlite.ts";
import * as StartRefusals from "../startRefusal/Store.ts";
import {
  nativeCreationCanonicalJson,
  nativeCreationSha256,
} from "../nativeCreation/NativeCreationPreparation.ts";

type PendingRun = {
  readonly run: OrchestrationV2Run;
  readonly attempt: OrchestrationV2RunAttempt;
  readonly root: OrchestrationV2ExecutionNode;
};

export const RUNTIME_STOP_WAIT_TIMEOUT_MS = 3_000;

export const capturedStopQueueHoldEvent = (run: OrchestrationV2Run, occurredAt: DateTime.Utc) => ({
  type: "run.updated" as const,
  threadId: run.threadId,
  runId: run.id,
  providerInstanceId: run.providerInstanceId,
  occurredAt,
  payload: { ...run, queueHeld: true },
});

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
const sameBinding = Schema.toEquivalence(ProviderRuntimeBinding);

function matchesCapture(provider: OrchestrationV2ProviderThread, captured: ProviderRuntimeBinding) {
  const generation = provider.runtimeIdentity?.runtimeGeneration;
  const binding = generation === undefined ? undefined : runtimeBinding(provider, generation);
  return (
    binding !== undefined &&
    provider.nativeThreadRef?.driver === captured.driver &&
    sameBinding(binding, captured)
  );
}

function sameUnstartedRun(candidate: OrchestrationV2Run, run: OrchestrationV2Run) {
  return (
    candidate.id === run.id &&
    candidate.threadId === run.threadId &&
    candidate.activeAttemptId === run.activeAttemptId &&
    candidate.rootNodeId === run.rootNodeId &&
    candidate.userMessageId === run.userMessageId &&
    candidate.providerThreadId === run.providerThreadId &&
    candidate.providerInstanceId === run.providerInstanceId &&
    candidate.startedAt === null &&
    candidate.completedAt === null
  );
}

function isInitialPendingAttempt(attempt: OrchestrationV2RunAttempt) {
  return (
    attempt.reason === "initial" &&
    attempt.attemptOrdinal === 1 &&
    attempt.status === "pending" &&
    attempt.startedAt === null &&
    attempt.completedAt === null &&
    attempt.providerTurnId === null &&
    attempt.providerSettlement == null
  );
}

function samePendingAttempt(
  candidate: OrchestrationV2RunAttempt,
  attempt: OrchestrationV2RunAttempt,
) {
  return (
    candidate.id === attempt.id &&
    candidate.runId === attempt.runId &&
    candidate.rootNodeId === attempt.rootNodeId &&
    candidate.providerThreadId === attempt.providerThreadId &&
    candidate.providerInstanceId === attempt.providerInstanceId &&
    isInitialPendingAttempt(candidate)
  );
}

function isPendingRoot(root: OrchestrationV2ExecutionNode, run: OrchestrationV2Run) {
  return (
    root.id === run.rootNodeId &&
    root.rootNodeId === root.id &&
    root.parentNodeId === null &&
    root.runId === run.id &&
    root.kind === "root_turn" &&
    root.status === "pending" &&
    root.threadId === run.threadId &&
    root.providerThreadId === run.providerThreadId &&
    root.providerTurnId === null &&
    root.nativeItemRef === null &&
    root.runtimeRequestId === null &&
    root.startedAt === null &&
    root.completedAt === null
  );
}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const eventSink = yield* EventSink.EventSinkV2;
  const restartPreparation = yield* RestartPreparation.RestartPreparation;
  const manager = yield* ProviderSessions.ProviderSessionManagerV2;
  const sessions = yield* AuthSessions.AuthSessionRepository;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const outbox = yield* Outbox.EffectOutboxV2;
  const receipts = yield* CommandReceipts.CommandReceiptStoreV2;
  const refusals = yield* StartRefusals.StartRefusals;
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
  const readPendingRun = Effect.fn("CurrentRuntimeStop.readPendingRun")(function* (
    expected: OrchestrationV2Run,
    binding?: ProviderRuntimeBinding,
  ) {
    if (expected.rootNodeId === null) return;
    const records = yield* projections.getRunProviderRecords(
      expected.threadId,
      expected.id,
      expected.rootNodeId,
    );
    const run = records.runs[0];
    const attempt = records.attempts[0];
    const root = records.nodes[0];
    if (
      run === undefined ||
      run.status !== expected.status ||
      !sameUnstartedRun(run, expected) ||
      run.manualContinuationOfRunId !== undefined ||
      run.restartContinuationOfRunId !== undefined ||
      run.usageLimitContinuationOfRunId !== undefined ||
      (binding !== undefined &&
        (run.providerThreadId !== binding.providerThreadId ||
          run.providerInstanceId !== binding.providerInstanceId ||
          records.thread.activeProviderThreadId !== binding.providerThreadId ||
          !records.providerThreads.some((provider) => matchesCapture(provider, binding)))) ||
      attempt === undefined ||
      root === undefined ||
      attempt.id !== run.activeAttemptId ||
      attempt.runId !== run.id ||
      attempt.rootNodeId !== root.id ||
      attempt.providerThreadId !== run.providerThreadId ||
      attempt.providerInstanceId !== run.providerInstanceId ||
      !isInitialPendingAttempt(attempt) ||
      records.providerTurns.length !== 0 ||
      !isPendingRoot(root, run)
    )
      return;
    const prefix = yield* refusals.readPrefix({
      threadId: run.threadId,
      runId: run.id,
      attemptId: attempt.id,
    });
    if (prefix.state !== "unknown") return;
    return { run, attempt, root } satisfies PendingRun;
  });

  const readAcceptedEvents = Effect.fn("CurrentRuntimeStop.readAcceptedEvents")(function* (
    commandId: CommandId,
    threadId: ThreadId,
  ) {
    const receipt = yield* receipts.getByCommandId(commandId);
    if (
      Option.isNone(receipt) ||
      receipt.value.status !== "accepted" ||
      receipt.value.threadId !== threadId
    )
      return;
    const events = yield* eventSink.readByCommandId({ commandId }).pipe(Stream.runCollect);
    if (
      receipt.value.resultSequence !== events.at(-1)?.sequence ||
      events.some((stored) => stored.commandId !== commandId || stored.event.threadId !== threadId)
    )
      return;
    return { commandType: receipt.value.commandType, events };
  });

  const provesPreparedRelease = Effect.fn("CurrentRuntimeStop.provesPreparedRelease")(function* (
    pending: PendingRun,
    release: NonNullable<Effect.Success<ReturnType<typeof readAcceptedEvents>>>["events"],
    binding: ProviderRuntimeBinding,
  ) {
    const { run, attempt, root } = pending;
    const [scope, rootUpdate, ready, start] = release;
    if (
      release.length !== 4 ||
      scope?.event.type !== "checkpoint-scope.created" ||
      rootUpdate?.event.type !== "node.updated" ||
      ready?.event.type !== "turn-item.updated" ||
      start?.event.type !== "run.updated" ||
      !release.every(({ event }) => event.runId === run.id && event.nodeId === root.id) ||
      !sameUnstartedRun(start.event.payload, run) ||
      start.event.payload.status !== "starting" ||
      !isPendingRoot(rootUpdate.event.payload, run) ||
      rootUpdate.event.payload.checkpointScopeId !== root.checkpointScopeId ||
      scope.event.payload.id !== root.checkpointScopeId ||
      scope.event.payload.threadId !== run.threadId ||
      scope.event.payload.runId !== run.id ||
      scope.event.payload.nodeId !== root.id ||
      scope.event.payload.providerThreadId !== binding.providerThreadId ||
      scope.event.payload.kind !== "root_run" ||
      scope.event.payload.parentScopeId !== null ||
      scope.event.payload.ordinalWithinParent !== 0 ||
      !scope.event.payload.advancesAppRunCount ||
      ready.event.payload.type !== "command_execution" ||
      ready.event.payload.runId !== run.id ||
      ready.event.payload.nodeId !== root.id ||
      ready.event.payload.status !== "completed" ||
      ready.event.payload.exitCode !== 0 ||
      ready.event.payload.completedAt === null ||
      ready.event.payload.providerThreadId !== binding.providerThreadId
    )
      return false;
    const birth = yield* StopStore.readPreparedStopBirth(sql, run.threadId, run.id, scope.sequence);
    if (birth === undefined) return false;
    const original = yield* readAcceptedEvents(birth.commandId, run.threadId);
    if (
      original?.commandType !== "message.dispatch" ||
      original.events.some(
        ({ sequence }) => sequence <= birth.prunedThrough || sequence >= scope.sequence,
      )
    )
      return false;
    const born = original.events.find(({ sequence }) => sequence === birth.sequence)?.event;
    const producer = original.events.findLast(
      ({ event }) =>
        event.type === "provider-thread.updated" && event.payload.id === binding.providerThreadId,
    )?.event;
    const preparationItemId = ready.event.payload.id;
    const preparation = original.events.find(
      ({ event }) => event.type === "turn-item.updated" && event.payload.id === preparationItemId,
    )?.event;
    if (
      born?.type !== "run.created" ||
      born.runId !== run.id ||
      born.payload.status !== "preparing" ||
      !sameUnstartedRun(born.payload, run) ||
      producer?.type !== "provider-thread.updated" ||
      !matchesCapture(producer.payload, binding) ||
      preparation?.type !== "turn-item.updated" ||
      preparation.payload.type !== "command_execution" ||
      preparation.payload.status !== "running" ||
      preparation.payload.runId !== run.id ||
      preparation.payload.nodeId !== root.id ||
      preparation.payload.providerThreadId !== binding.providerThreadId ||
      preparation.payload.input !== ready.event.payload.input ||
      !original.events.some(
        ({ event }) =>
          event.type === "run-attempt.created" && samePendingAttempt(event.payload, attempt),
      ) ||
      !original.events.some(
        ({ event }) =>
          event.type === "node.updated" &&
          isPendingRoot(event.payload, run) &&
          event.payload.checkpointScopeId === null,
      )
    )
      return false;
    const current = yield* projections.getCheckpointCaptureContext(run.threadId, {
      runId: run.id,
      scopeId: scope.event.payload.id,
    });
    return (
      current.scope !== undefined &&
      nativeCreationCanonicalJson(current.scope) ===
        nativeCreationCanonicalJson(scope.event.payload)
    );
  });

  const readCancellableStart = Effect.fn("CurrentRuntimeStop.readCancellableStart")(
    function* (expected: OrchestrationV2Run, binding: ProviderRuntimeBinding) {
      const starts = yield* StopStore.readStopStartEffectIds(sql, expected.threadId, expected.id);
      if (starts.length !== 1) return;
      const effect = Option.getOrUndefined(yield* outbox.get(starts[0]!.effect_id));
      if (
        effect === undefined ||
        effect.threadId !== expected.threadId ||
        effect.request.type !== "provider-turn.start" ||
        effect.request.runId !== expected.id ||
        effect.id !== `effect:${effect.commandId}:provider-turn.start:${expected.id}` ||
        effect.completedAt !== null ||
        effect.nativeCreationExecutionReference !== undefined ||
        effect.payloadIdentity === undefined ||
        (!(
          effect.status === "pending" &&
          effect.attemptCount === 0 &&
          effect.leaseOwner === null
        ) &&
          !(
            effect.status === "running" &&
            effect.attemptCount === 1 &&
            effect.leaseOwner !== null &&
            outbox.startDispatch?.hasWitness(effect) === true
          ))
      )
        return;

      const pending = yield* readPendingRun(expected, binding);
      if (pending === undefined) return;
      const accepted = yield* readAcceptedEvents(effect.commandId, pending.run.threadId);
      if (accepted === undefined) return;
      const { commandType, events } = accepted;
      if (commandType === "prepared-run.release")
        return (yield* provesPreparedRelease(pending, events, binding))
          ? { pending, effect }
          : undefined;
      if (commandType !== "message.dispatch" && commandType !== "queue.start") return;
      const { run, attempt } = pending;
      const runEvent = events.findLast(
        ({ event }) =>
          event.type === (commandType === "queue.start" ? "run.updated" : "run.created") &&
          event.runId === run.id,
      )?.event;
      const providerEvent = events.findLast(
        ({ event }) =>
          event.type === "provider-thread.updated" && event.payload.id === binding.providerThreadId,
      )?.event;
      // A provider thread id can be reused. The original admission must name this generation.
      if (
        (runEvent?.type !== "run.created" && runEvent?.type !== "run.updated") ||
        !sameUnstartedRun(runEvent.payload, run) ||
        runEvent.payload.status !== "starting" ||
        providerEvent?.type !== "provider-thread.updated" ||
        !matchesCapture(providerEvent.payload, binding) ||
        (commandType === "message.dispatch" &&
          !events.some(
            ({ event }) =>
              event.type === "run-attempt.created" && samePendingAttempt(event.payload, attempt),
          ))
      )
        return;
      return { pending, effect };
    },
    // Unreadable start evidence still permits the existing physical Stop and fences.
    Effect.orElseSucceed(() => undefined),
  );
  const settlePendingRun = Effect.fn("CurrentRuntimeStop.settlePendingRun")(function* (
    { run, attempt, root }: PendingRun,
    commandId: CommandId,
    now: DateTime.Utc,
    expectedStatus: "starting" | "queued",
  ) {
    const eventScope = {
      threadId: run.threadId,
      runId: run.id,
      nodeId: root.id,
      providerInstanceId: run.providerInstanceId,
      occurredAt: now,
    };
    const terminal = { status: "cancelled", completedAt: now } as const;
    const eventId = (record: string) =>
      EventId.make(
        `event:${commandId}:cancel-${expectedStatus === "starting" ? "start" : "queued"}:${run.id}:${record}`,
      );
    const written = yield* eventSink.writeIfRunCurrent({
      threadId: run.threadId,
      runId: run.id,
      activeAttemptId: attempt.id,
      expectedStatus,
      events: [
        {
          ...eventScope,
          id: eventId("run"),
          type: "run.updated",
          payload: { ...run, ...terminal, queuePosition: null },
        },
        {
          ...eventScope,
          id: eventId("attempt"),
          type: "run-attempt.updated",
          payload: { ...attempt, ...terminal },
        },
        {
          ...eventScope,
          id: eventId("root"),
          type: "node.updated",
          payload: { ...root, ...terminal },
        },
      ],
    });
    if (!written.committed)
      return yield* error(
        expectedStatus === "starting"
          ? "runtime_stop_start_basis_changed"
          : "runtime_stop_queue_basis_changed",
      );
  });

  const settleCancelledStart = Effect.fn("CurrentRuntimeStop.settleCancelledStart")(function* (
    { effect, pending }: NonNullable<Effect.Success<ReturnType<typeof readCancellableStart>>>,
    commandId: CommandId,
    now: DateTime.Utc,
  ) {
    const cancelled = yield* outbox.get(effect.id);
    if (
      Option.isNone(cancelled) ||
      cancelled.value.threadId !== pending.run.threadId ||
      cancelled.value.commandId !== effect.commandId ||
      cancelled.value.payloadIdentity !== effect.payloadIdentity ||
      cancelled.value.status !== "cancelled" ||
      cancelled.value.attemptCount !== effect.attemptCount ||
      cancelled.value.leaseOwner !== null
    )
      return yield* error("runtime_stop_start_basis_changed");
    yield* settlePendingRun(pending, commandId, now, "starting");
  });
  const disposeFencedQueue = Effect.fn("CurrentRuntimeStop.disposeFencedQueue")(function* (
    threadId: ThreadId,
    queue: ReadonlyArray<OrchestrationV2Run>,
    commandId: CommandId,
    now: DateTime.Utc,
  ) {
    if (queue.length === 0) return;
    const { runs } = yield* projections.getThreadRecords(threadId, ["runs"], {
      runIds: queue.map((run) => run.id),
      runStatuses: ["queued"],
    });
    const held: OrchestrationV2Run[] = [];
    for (const run of runs) {
      if (run.queueHeld === true) continue;
      const pending = yield* Effect.gen(function* () {
        if ((yield* StopStore.readStopStartEffectIds(sql, threadId, run.id)).length !== 0) return;
        return yield* readPendingRun(run);
      }).pipe(Effect.orElseSucceed(() => undefined));
      if (pending !== undefined) {
        yield* settlePendingRun(pending, commandId, now, "queued");
      } else {
        held.push(run);
      }
    }
    if (held.length > 0)
      yield* eventSink.writeWithEffects({
        events: held.map((run) => ({
          ...capturedStopQueueHoldEvent(run, now),
          id: EventId.make(`event:${commandId}:hold-queued:${run.id}`),
        })),
        effects: [],
        queueHold: { reason: "stop", runIds: held.map((run) => run.id) },
      });
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
      const readRuntimeStop = eventSink.readRuntimeStop;
      const replay = Effect.gen(function* () {
        const prior = yield* readRuntimeStop(input.commandId);
        if (prior === null) return null;
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
      });
      const prior = yield* replay;
      if (prior !== null) return prior;
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
      let registeredCapture = false;
      return yield* eventSink
        .withProviderBatch(
          Effect.gen(function* () {
            const previous = yield* replay;
            if (previous !== null) return previous;
            const {
              thread,
              runs: visibleRuns,
              providerThreads,
            } = yield* projections.getThreadRecords(input.threadId, ["runs", "providerThreads"], {
              runStatuses: ["queued", "starting", "running", "waiting"],
            });
            const runs = visibleRuns.filter(
              (run) => run.status === "queued" || run.status === "starting",
            );
            const { binding } = captured;
            // A previously captured retirement can remain observable after its run is replaced.
            const attributedRun =
              thread.activeProviderThreadId === binding.providerThreadId &&
              providerThreads.some((provider) => matchesCapture(provider, binding)) &&
              (yield* captured.isCurrent)
                ? visibleRuns
                    .filter(
                      (run) =>
                        (run.status === "starting" ||
                          run.status === "running" ||
                          run.status === "waiting") &&
                        run.completedAt === null &&
                        run.activeAttemptId !== null &&
                        run.providerThreadId === binding.providerThreadId &&
                        run.providerInstanceId === binding.providerInstanceId,
                    )
                    .toSorted((left, right) => right.ordinal - left.ordinal)[0]
                : undefined;
            const affectedRunIds = runs.map((run) => run.id);
            const identity = {
              request: input,
              actor: {
                sessionId: principal.sessionId,
                subject: principal.subject,
                method: principal.method,
              },
              actorDigest,
              affectedRunIds,
              ...(attributedRun === undefined ? {} : { attributedRunId: attributedRun.id }),
            };
            const now = yield* DateTime.now;
            const candidates = runs.filter(
              (run) =>
                run.status === "starting" && run.providerThreadId === binding.providerThreadId,
            );
            const cancellable = yield* Effect.forEach(candidates, (run) =>
              readCancellableStart(run, binding),
            );
            const accepted = yield* eventSink.commitCommand({
              commandId: input.commandId,
              threadId: input.threadId,
              commandType: "provider-session.detach",
              acceptedAt: now,
              runtimeStop: {
                identity,
                revalidate: Effect.gen(function* () {
                  yield* revalidate;
                  if (attributedRun !== undefined && !(yield* captured.isCurrent))
                    return yield* error("captured_runtime_replaced");
                }),
              },
              events: [
                {
                  id: EventId.make(`event:${input.commandId}:captured-runtime-stop`),
                  type: "provider-session.detach-requested",
                  threadId: input.threadId,
                  ...(attributedRun === undefined ? {} : { runId: attributedRun.id }),
                  driver: binding.driver,
                  providerInstanceId: binding.providerInstanceId,
                  occurredAt: now,
                  payload: { providerSessionId: binding.providerSessionId },
                },
              ],
              effects: [
                {
                  id: `effect:${input.commandId}:captured-runtime-stop`,
                  commandId: input.commandId,
                  threadId: input.threadId,
                  request: {
                    type: "provider-session.detach",
                    providerSessionId: binding.providerSessionId,
                    runtimeStopCommandId: input.commandId,
                  },
                },
              ],
              cancelUnsettledEffects: {
                effectTypes: ["provider-turn.start"],
                reason: "Captured runtime stop fences previous queued execution lineage.",
              },
            });
            // The Stop cohort CAS needs to see the live runs. Settle only after its
            // cancellation, in the same writer transaction so no worker can cross the dispatch gate.
            if (!accepted.committed) return yield* error("stop_command_identity_conflict");
            yield* disposeFencedQueue(
              input.threadId,
              runs.filter((run) => run.status === "queued"),
              input.commandId,
              now,
            );
            for (const start of cancellable)
              if (start !== undefined) yield* settleCancelledStart(start, input.commandId, now);
            captures.set(input.commandId, captured);
            registeredCapture = true;
            return {
              commandId: input.commandId,
              threadId: input.threadId,
              status: "accepted" as const,
              affectedRunIds,
              backgroundCoverage: "partial" as const,
            };
          }),
        )
        .pipe(
          Effect.onExit((exit) =>
            Effect.sync(() => {
              if (
                Exit.isFailure(exit) &&
                registeredCapture &&
                captures.get(input.commandId) === captured
              )
                captures.delete(input.commandId);
            }),
          ),
        );
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
export const layer = Layer.effect(CurrentRuntimeStop, make).pipe(
  Layer.provide(Layer.mergeAll(Outbox.layer, CommandReceipts.layer, StartRefusals.layer)),
);

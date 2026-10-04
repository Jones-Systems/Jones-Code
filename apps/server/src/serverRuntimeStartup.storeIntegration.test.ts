import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Ref from "effect/Ref";
import * as DateTime from "effect/DateTime";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as NodePath from "@effect/platform-node/NodePath";
import { HostProcessEnvironment } from "@t3tools/shared/hostProcess";
import {
  CommandId, EventId, MessageId, NodeId, ProjectId, ProviderDriverKind, ProviderInstanceId, ProviderSessionId,
  ProviderThreadId, RunAttemptId, RunId, ThreadId,
  type OrchestrationV2AppThread, type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import type { RestartContinuationMarkerV2 } from "./orchestration-v2/EventSink.ts";
import { captureServerUpdateContinuations } from "./jonesUpdates/service.ts";
import * as ServerConfig from "./config.ts";
import * as ServiceLauncherClient from "./cloud/serviceLauncherClient.ts";
import { SERVICE_LAUNCHER_CONTEXT_ENV, SERVICE_LAUNCHER_PROTOCOL } from "./cloud/serviceProtocol.ts";

import { SqlitePersistenceMemory } from "./persistence/Layers/Sqlite.ts";
import * as Recovery from "./orchestration-v2/ProviderRuntimeRecoveryService.ts";
import * as ProviderSessions from "./orchestration-v2/ProviderSessionManager.ts";
import * as EventSink from "./orchestration-v2/EventSink.ts";
import * as EventStore from "./orchestration-v2/EventStore.ts";
import * as ProjectionStore from "./orchestration-v2/ProjectionStore.ts";
import * as EffectOutbox from "./orchestration-v2/EffectOutbox.ts";
import * as IdAllocator from "./orchestration-v2/IdAllocator.ts";
import { CodexProviderCapabilitiesV2 } from "./orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as ServerSettings from "./serverSettings.ts";

import * as ServerActivation from "./serverActivation.ts";
import * as Startup from "./serverRuntimeStartup.ts";

it.effect("keeps recovered workers and commands parked until the exact trial commits", () =>
  Effect.scoped(Effect.gen(function* () {
    const activation = yield* Deferred.make<void>();
    const trialEntered = yield* Deferred.make<void>();
    const commit = yield* Deferred.make<void>();
    const recoveryEntered = yield* Deferred.make<void>();
    const reconciled = yield* Deferred.make<void>();
    const workerRan = yield* Deferred.make<void>();
    const commandRan = yield* Deferred.make<void>();
    const gate = yield* Startup.makeCommandGate;
    const worker = yield* Ref.make<Fiber.Fiber<void, never> | null>(null);
    const phases: string[] = [];
    const record = (phase: string) => Effect.sync(() => { phases.push(phase); });
    const queued = yield* gate.enqueueCommand(Deferred.succeed(commandRan, undefined)).pipe(Effect.forkScoped);
    const starting = yield* Effect.gen(function* () {
      yield* Startup.runOrderedV2StartupPhases({
        importLegacyShells: record("shells"),
        recover: record("staged-recovery"),
        startEffectWorker: Startup.startEffectWorkerWithRelay({
          runWorker: Deferred.succeed(workerRan, undefined).pipe(Effect.andThen(Effect.never)),
          startRelay: record("relay-parked"),
          workerFiberRef: worker,
        }),
        autoBootstrap: record("bootstrap"),
      });
      return yield* Startup.runOrderedV2ActivationPhases({
        awaitHttpListening: record("listener"),
        awaitAuxiliaryParked: record("auxiliary-parked"),
        prepareTrial: record("prepared").pipe(Effect.as("trial-receipt")),
        commitJonesTrial: Deferred.succeed(trialEntered, undefined).pipe(Effect.andThen(Deferred.await(commit))),
        reconcileAfterTrial: record("posttrial-recovery").pipe(
          Effect.andThen(Deferred.succeed(recoveryEntered, undefined)),
          Effect.andThen(Deferred.await(reconciled)),
        ),
        publishWelcome: record("welcome"),
        activate: record("activate").pipe(Effect.andThen(Deferred.succeed(activation, undefined)), Effect.asVoid),
        signalCommandReady: gate.signalCommandReady,
      });
    }).pipe(
      Effect.provideService(ServerActivation.ServerActivation, Deferred.await(activation)),
      Effect.forkScoped,
    );
    yield* Deferred.await(trialEntered);
    assert.deepEqual(phases, ["shells", "staged-recovery", "relay-parked", "bootstrap", "listener", "auxiliary-parked", "prepared"]);
    assert.isFalse(yield* Deferred.isDone(workerRan));
    assert.isFalse(yield* Deferred.isDone(commandRan));
    yield* Deferred.succeed(commit, undefined);
    yield* Deferred.await(recoveryEntered);
    assert.isFalse(yield* Deferred.isDone(workerRan));
    assert.isFalse(yield* Deferred.isDone(commandRan));
    assert.isFalse(yield* Deferred.isDone(activation));
    assert.deepEqual(phases.slice(-1), ["posttrial-recovery"]);
    yield* Deferred.succeed(reconciled, undefined);
    assert.equal(yield* Fiber.join(starting), "trial-receipt");
    yield* Deferred.await(workerRan);
    yield* Fiber.join(queued);
    assert.isTrue(yield* Deferred.isDone(commandRan));
    assert.deepEqual(phases.slice(-2), ["welcome", "activate"]);
    assert.equal(phases.filter((phase) => phase === "prepared").length, 1);
  })),
);

it.effect.each(["recovery", "relay", "trial", "posttrial recovery"] as const)(
  "a failed %s phase cannot activate a recovered worker or accept commands",
  (failure) => Effect.scoped(Effect.gen(function* () {
    const activation = yield* Deferred.make<void>();
    const ran = yield* Deferred.make<void>();
    const worker = yield* Ref.make<Fiber.Fiber<void, never> | null>(null);
    const gate = yield* Startup.makeCommandGate;
    const commandRan = yield* Deferred.make<void>();
    const queued = yield* gate.enqueueCommand(Deferred.succeed(commandRan, undefined)).pipe(Effect.forkScoped);
    const fail = Effect.fail("stopped-before-activation");
    const result = yield* Effect.gen(function* () {
      yield* Startup.runOrderedV2StartupPhases({
        importLegacyShells: Effect.void,
        recover: failure === "recovery" ? fail : Effect.void,
        startEffectWorker: Startup.startEffectWorkerWithRelay({
          runWorker: Deferred.succeed(ran, undefined).pipe(Effect.andThen(Effect.never)),
          startRelay: failure === "relay" ? Effect.die("relay unavailable") : Effect.void,
          workerFiberRef: worker,
        }),
        autoBootstrap: Effect.void,
      });
      yield* Startup.runOrderedV2ActivationPhases({
        awaitHttpListening: Effect.void,
        awaitAuxiliaryParked: Effect.void,
        prepareTrial: Effect.void,
        commitJonesTrial: failure === "trial" ? fail : Effect.void,
        reconcileAfterTrial: failure === "posttrial recovery" ? fail : Effect.void,
        publishWelcome: Effect.void,
        activate: Deferred.succeed(activation, undefined).pipe(Effect.asVoid),
        signalCommandReady: gate.signalCommandReady,
      });
    }).pipe(
      Effect.provideService(ServerActivation.ServerActivation, Deferred.await(activation)),
      Effect.exit,
    );
    assert.isTrue(Exit.isFailure(result));
    assert.isFalse(yield* Deferred.isDone(activation));
    assert.isFalse(yield* Deferred.isDone(ran));
    assert.isFalse(yield* Deferred.isDone(commandRan));
    const startupError = new Startup.ServerRuntimeStartupError({
      mode: "web", host: "127.0.0.1", port: 3773,
      cause: Exit.isFailure(result) ? result.cause : "missing startup failure",
    });
    yield* gate.failCommandReady(startupError);
    const commandExit = yield* Fiber.join(queued).pipe(Effect.exit);
    assert.isTrue(Exit.isFailure(commandExit));
    assert.isFalse(yield* Deferred.isDone(commandRan));
    if (failure === "relay") assert.isNull(yield* Ref.get(worker));
  })),
);

it.effect("cancellation at the trial boundary cannot release parked roots", () =>
  Effect.scoped(Effect.gen(function* () {
    const entered = yield* Deferred.make<void>();
    const activated = yield* Deferred.make<void>();
    const ready = yield* Deferred.make<void>();
    const startup = yield* Startup.runOrderedV2ActivationPhases({
      awaitHttpListening: Effect.void,
      awaitAuxiliaryParked: Effect.void,
      prepareTrial: Effect.void,
      commitJonesTrial: Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
      reconcileAfterTrial: Effect.die("cancelled trial cannot reconcile or release markers"),
      publishWelcome: Effect.void,
      activate: Deferred.succeed(activated, undefined).pipe(Effect.asVoid),
      signalCommandReady: Deferred.succeed(ready, undefined).pipe(Effect.asVoid),
    }).pipe(Effect.forkScoped);
    yield* Deferred.await(entered);
    yield* Fiber.interrupt(startup);
    assert.isFalse(yield* Deferred.isDone(activated));
    assert.isFalse(yield* Deferred.isDone(ready));
  })),
);

it.effect.each(["state.sqlite", "statev2.sqlite"])(
  "prepares a service trial only for the actually selected %s binding",
  (databaseName) => Effect.gen(function* () {
    let prepared = 0;
    const config = { baseDir: "/fixture/startup", dbPath: `/fixture/startup/userdata/${databaseName}`, mode: "web", host: "127.0.0.1", port: 3773 } as ServerConfig.ServerConfig["Service"];
    const context = (dbPath: string) => ({
      protocol: SERVICE_LAUNCHER_PROTOCOL,
      childVersion: "1.1.0",
      update: { id: "trial", fromVersion: "1.0.0", targetVersion: "1.1.0", dbPath, status: "pending", phase: "trial-ready" },
    });
    const run = (dbPath: string) => Startup.prepareServiceLauncherTrial.pipe(
      Effect.provideService(ServerConfig.ServerConfig, config),
      Effect.provideService(ServiceLauncherClient.ServiceLauncherClient, {
        managed: true,
        prepareTrial: Effect.sync(() => { prepared += 1; return undefined; }),
        requestUpdate: () => Effect.die("unused update request"),
      }),
      Effect.provideService(HostProcessEnvironment, {
        T3CODE_HOME: config.baseDir,
        [SERVICE_LAUNCHER_CONTEXT_ENV]: JSON.stringify(context(dbPath)),
      }),
      Effect.provide(NodePath.layer),
    );
    assert.isTrue(Exit.isFailure(yield* Effect.exit(run("/fixture/startup/userdata/other.sqlite"))));
    assert.equal(prepared, 0);
    yield* run(config.dbPath);
    assert.equal(prepared, 1);
  }),
);

it.effect("clears only the immutable marker batch returned to an update helper", () =>
  Effect.gen(function* () {
    const threadId = ThreadId.make("captured-update-thread");
    const marker = (markerId: string, generation: string): RestartContinuationMarkerV2 => ({
      markerId,
      threadId,
      projectId: ProjectId.make("captured-update-project"),
      sourceRunId: RunId.make("captured-update-run"),
      sourceRunAttemptId: RunAttemptId.make("captured-update-attempt"),
      binding: {
        threadId,
        providerThreadId: ProviderThreadId.make("captured-update-provider-thread"),
        providerSessionId: ProviderSessionId.make("captured-update-session"),
        instanceId: ProviderInstanceId.make("codex"),
        driver: ProviderDriverKind.make("codex"),
        nativeThreadId: "captured-native-thread",
        runtimeGeneration: generation,
      },
      evidenceRevision: generation === "first" ? 1 : 2,
      createdAt: "2026-01-01T00:00:00.000Z",
    });
    const first = [marker("first-marker", "first")];
    const replacement = [marker("replacement-marker", "replacement")];
    let current: ReadonlyArray<RestartContinuationMarkerV2> = first;
    const cleared: Array<ReadonlyArray<RestartContinuationMarkerV2>> = [];
    const captured = captureServerUpdateContinuations({
      prepare: Effect.sync(() => current),
      clear: (markers) => Effect.sync(() => { cleared.push(markers); }),
    });
    const firstIds = yield* captured.prepare;
    current = replacement;
    const replacementIds = yield* captured.prepare;
    assert.deepEqual(firstIds, replacementIds);
    assert.isTrue(Exit.isFailure(yield* Effect.exit(captured.clear([...firstIds]))));
    assert.deepEqual(cleared, []);
    yield* captured.clear(firstIds);
    assert.strictEqual(cleared[0], first);
    assert.strictEqual(cleared[0]![0], first[0]);
    assert.isTrue(Exit.isFailure(yield* Effect.exit(captured.clear(firstIds))));
    yield* captured.clear(replacementIds);
    assert.strictEqual(cleared[1], replacement);
  }),
);

it.effect.each(["typed failure", "defect"] as const)(
  "startup target discovery %s cannot activate roots or accept commands",
  (failure) => Effect.scoped(Effect.gen(function* () {
    const activation = yield* Deferred.make<void>();
    const ran = yield* Deferred.make<void>();
    const worker = yield* Ref.make<Fiber.Fiber<void, never> | null>(null);
    const result = yield* Startup.runOrderedV2StartupPhases({
      importLegacyShells: Effect.void,
      recover: Effect.void,
      startEffectWorker: Startup.startEffectWorkerWithRelay({
        runWorker: Deferred.succeed(ran, undefined).pipe(Effect.andThen(Effect.never)),
        startRelay: Effect.void,
        workerFiberRef: worker,
      }),
      autoBootstrap: failure === "typed failure" ? Effect.fail("bootstrap unavailable") : Effect.die("bootstrap defect"),
    }).pipe(
      Effect.andThen(Deferred.succeed(activation, undefined)),
      Effect.provideService(ServerActivation.ServerActivation, Deferred.await(activation)),
      Effect.exit,
    );
    assert.isTrue(Exit.isFailure(result));
    assert.isFalse(yield* Deferred.isDone(activation));
    assert.isFalse(yield* Deferred.isDone(ran));
  })),
);

it.effect("an empty bootstrap target result still reaches trial before command readiness", () =>
  Effect.gen(function* () {
    const phases: string[] = [];
    const record = (phase: string) => Effect.sync(() => { phases.push(phase); });
    const { bootstrap } = yield* Startup.runOrderedV2StartupPhases({
      importLegacyShells: record("shells"),
      recover: record("staged"),
      startEffectWorker: record("parked"),
      autoBootstrap: Effect.succeed({}),
    });
    yield* Startup.runOrderedV2ActivationPhases({
      awaitHttpListening: record("listener"),
      awaitAuxiliaryParked: record("auxiliary"),
      prepareTrial: record("prepared"),
      commitJonesTrial: record("committed"),
      reconcileAfterTrial: record("reconciled"),
      publishWelcome: record("welcome"),
      activate: record("activated"),
      signalCommandReady: record("ready"),
    });
    assert.deepEqual(bootstrap, {});
    assert.deepEqual(phases, ["shells", "staged", "parked", "listener", "auxiliary", "prepared", "committed", "reconciled", "welcome", "activated", "ready"]);
  }),
);

const restartStores = Layer.mergeAll(
  EventStore.layer,
  ProjectionStore.layer,
  EffectOutbox.layer,
).pipe(Layer.provideMerge(SqlitePersistenceMemory));
const restartCore = Layer.mergeAll(
  restartStores,
  EventSink.layer.pipe(Layer.provide(restartStores)),
);

it.effect.each([false, true])(
  "both successful preparations with respectProjectPreference=%s retain one full marker and release it only after trial",
  (respectProjectPreference) => Effect.scoped(Effect.gen(function* () {
    const threadId = ThreadId.make(`startup-repeated-capture-${respectProjectPreference}`);
    const projectId = ProjectId.make("startup-capture-project");
    const instanceId = ProviderInstanceId.make("codex");
    const driver = ProviderDriverKind.make("codex");
    const providerThreadId = ProviderThreadId.make(`startup-capture-provider-${respectProjectPreference}`);
    const providerSessionId = ProviderSessionId.make(`startup-capture-session-${respectProjectPreference}`);
    const runId = RunId.make(`startup-capture-run-${respectProjectPreference}`);
    const attemptId = RunAttemptId.make(`startup-capture-attempt-${respectProjectPreference}`);
    const binding = {
      threadId, providerThreadId, providerSessionId, instanceId, driver,
      nativeThreadId: "startup-capture-native", runtimeGeneration: "startup-capture-generation",
    } satisfies EventSink.ProviderBindingExpectationV2;
    let observations = 0;
    const settingsLayer = ServerSettings.layerTest({ continueThreadsAfterServerUpdate: respectProjectPreference });
    const recoveryLayer = Recovery.layer.pipe(Layer.provide(Layer.mergeAll(
      restartCore,
      settingsLayer,
      IdAllocator.layer,
      Layer.mock(ProviderSessions.ProviderSessionManagerV2)({
        observeCurrentThreadRuntime: (id) => Effect.sync(() => {
          assert.equal(id, threadId);
          observations++;
          return { status: "monitoring" as const, binding, observedAt: "2026-10-03T00:00:00Z" };
        }),
      }),
    )));
    yield* Effect.gen(function* () {
      const sink = yield* EventSink.EventSinkV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const sql = yield* SqlClient.SqlClient;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const recovery = yield* Recovery.ProviderRuntimeRecoveryService;
      const settings = yield* ServerSettings.ServerSettingsService;
      const now = yield* DateTime.now;
      const thread: OrchestrationV2AppThread = {
        id: threadId, projectId, title: "Captured source", providerInstanceId: instanceId,
        modelSelection: { instanceId, model: "fixture-model" }, runtimeMode: "full-access", interactionMode: "default",
        branch: null, worktreePath: null, activeProviderThreadId: providerThreadId,
        lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId }, forkedFrom: null,
        createdBy: "user", creationSource: "web", createdAt: now, updatedAt: now,
        archivedAt: null, deletedAt: null, settledAt: null, settledOverride: null, lastVisitedAt: null,
      };
      const events: ReadonlyArray<OrchestrationV2DomainEvent> = [
        { id: EventId.make(`capture-birth-${respectProjectPreference}`), type: "thread.created", threadId,
          providerInstanceId: instanceId, occurredAt: now, payload: thread },
        { id: EventId.make(`capture-session-${respectProjectPreference}`), type: "provider-session.attached", threadId,
          providerInstanceId: instanceId, driver, occurredAt: now,
          payload: { id: providerSessionId, providerInstanceId: instanceId, driver, status: "ready", cwd: "/fixture",
            model: "fixture-model", capabilities: CodexProviderCapabilitiesV2, createdAt: now, updatedAt: now, lastError: null } },
        { id: EventId.make(`capture-provider-${respectProjectPreference}`), type: "provider-thread.updated", threadId,
          providerInstanceId: instanceId, driver, occurredAt: now,
          payload: { id: providerThreadId, appThreadId: threadId, ownerNodeId: null, providerInstanceId: instanceId, driver,
            providerSessionId, nativeThreadRef: { driver, nativeId: binding.nativeThreadId!, strength: "strong" },
            nativeConversationHeadRef: null, status: "idle", firstRunOrdinal: 1, lastRunOrdinal: 1,
            pendingBackgroundTasks: [{ kind: "monitor", taskId: "startup-capture-monitor", description: "Monitor captured source" }],
            handoffIds: [], forkedFrom: null, createdAt: now, updatedAt: now } },
        { id: EventId.make(`capture-run-${respectProjectPreference}`), type: "run.created", threadId, runId, occurredAt: now,
          payload: { id: runId, threadId, ordinal: 1, providerInstanceId: instanceId, modelSelection: thread.modelSelection,
            providerThreadId, userMessageId: MessageId.make("capture-source-message"), rootNodeId: null,
            activeAttemptId: attemptId, status: "completed", requestedAt: now, startedAt: now, completedAt: now,
            checkpointId: null, contextHandoffId: null } },
        { id: EventId.make(`capture-attempt-${respectProjectPreference}`), type: "run-attempt.created", threadId, runId, occurredAt: now,
          payload: { id: attemptId, runId, attemptOrdinal: 1, rootNodeId: NodeId.make("capture-source-node"),
            providerInstanceId: instanceId, providerThreadId, providerTurnId: null, reason: "initial", status: "completed",
            startedAt: now, completedAt: now } },
      ];
      yield* sink.write({ events });
      const registered = yield* sink.registerProviderRuntime({
        expectedBinding: { ...binding, runtimeGeneration: null }, expectedEvidenceRevision: 0,
        actualBinding: binding,
      });
      assert.isTrue(registered.committed);
      assert.deepEqual(yield* projections.getRecoveryThreadIds("runtime"), [threadId]);
      const captureProjection = yield* projections.getRuntimeRecoveryProjection(threadId);
      assert.equal(captureProjection.providerThreads[0]?.id, providerThreadId);
      assert.equal(captureProjection.providerThreads[0]?.providerSessionId, providerSessionId);
      assert.equal(captureProjection.runs[0]?.id, runId);
      assert.deepEqual((yield* sink.readProviderRuntimeEvidence(threadId))?.binding, binding);
      const prepare = respectProjectPreference
        ? Startup.markOptedInProviderSessionsForContinuation
        : Startup.markRunningProviderSessionsForContinuation;
      const first = yield* prepare;
      assert.lengthOf(first, 1);
      const firstRows = yield* sql`SELECT * FROM orchestration_v2_restart_continuation_markers ORDER BY marker_id`;
      const second = yield* prepare;
      assert.lengthOf(second, 1);
      assert.deepEqual(second, first);
      assert.equal(observations, 1);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_restart_continuation_markers ORDER BY marker_id`, firstRows);
      const marker = first[0]!;
      const commandId = CommandId.make(`command:restart-continuation:${marker.markerId}`);
      assert.deepEqual(yield* outbox.listByCommandId(commandId), []);
      yield* settings.updateSettings({ continueThreadsAfterServerUpdate: false });
      const stage = yield* recovery.stageStartupRecovery;
      assert.deepEqual(stage.continuationMarkers, first);
      const trialEntered = yield* Deferred.make<void>();
      const commit = yield* Deferred.make<void>();
      const activated = yield* Deferred.make<void>();
      const ready = yield* Deferred.make<void>();
      const starting = yield* Startup.runOrderedV2ActivationPhases({
        awaitHttpListening: Effect.void,
        awaitAuxiliaryParked: Effect.void,
        prepareTrial: Effect.void,
        commitJonesTrial: Deferred.succeed(trialEntered, undefined).pipe(Effect.andThen(Deferred.await(commit))),
        reconcileAfterTrial: recovery.reconcileAfterStartupTrial(stage).pipe(Effect.tap((result) => Effect.sync(() => {
          assert.deepEqual(result.releasedContinuationMarkerIds, [marker.markerId]);
          assert.deepEqual(result.heldContinuationMarkers, []);
          assert.deepEqual(result.failedThreadIds, []);
        })), Effect.asVoid),
        publishWelcome: Effect.void,
        activate: Deferred.succeed(activated, undefined).pipe(Effect.asVoid),
        signalCommandReady: Deferred.succeed(ready, undefined).pipe(Effect.asVoid),
      }).pipe(Effect.forkScoped);
      yield* Deferred.await(trialEntered);
      assert.isFalse(yield* Deferred.isDone(activated));
      assert.isFalse(yield* Deferred.isDone(ready));
      assert.deepEqual(yield* outbox.listByCommandId(commandId), []);
      assert.deepEqual(yield* sink.readDormantRestartContinuations, first);
      yield* Deferred.succeed(commit, undefined);
      yield* Fiber.join(starting);
      assert.isTrue(yield* Deferred.isDone(activated));
      assert.isTrue(yield* Deferred.isDone(ready));
      assert.deepEqual(yield* sink.readDormantRestartContinuations, []);
      const effects = yield* outbox.listByCommandId(commandId);
      assert.lengthOf(effects, 1);
      assert.equal(effects[0]!.id, `restart-continuation:${marker.markerId}`);
      assert.deepEqual(effects[0]!.request, { type: "provider-runtime.continue", sourceRunId: runId });
      assert.equal(effects[0]!.status, "pending");
      assert.deepEqual(yield* sink.readReleasedRestartContinuation({ effectId: effects[0]!.id, threadId, sourceRunId: runId }), marker);
    }).pipe(Effect.provide(Layer.fresh(Layer.mergeAll(restartCore, settingsLayer, recoveryLayer))));
  })),
);

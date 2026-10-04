import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  RunAttemptId,
  RunId,
  ThreadId,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ProviderSession,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Ref from "effect/Ref";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { makeSqlitePersistenceLive } from "../src/persistence/Layers/Sqlite.ts";
import * as ProviderSessionRuntime from "../src/persistence/ProviderSessionRuntime.ts";
import * as EventSink from "../src/orchestration-v2/EventSink.ts";
import * as EventStore from "../src/orchestration-v2/EventStore.ts";
import * as EffectOutbox from "../src/orchestration-v2/EffectOutbox.ts";
import * as IdAllocator from "../src/orchestration-v2/IdAllocator.ts";
import { CodexProviderCapabilitiesV2 } from "../src/orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as LegacyImporter from "../src/orchestration-v2/legacy/LegacyV1ThreadImporter.ts";
import * as ProjectionStore from "../src/orchestration-v2/ProjectionStore.ts";
import * as Recovery from "../src/orchestration-v2/ProviderRuntimeRecoveryService.ts";
import * as ProviderSessions from "../src/orchestration-v2/ProviderSessionManager.ts";
import * as ServerActivation from "../src/serverActivation.ts";
import * as Startup from "../src/serverRuntimeStartup.ts";
import * as ServerSettings from "../src/serverSettings.ts";

const instanceId = ProviderInstanceId.make("codex");
const driver = ProviderDriverKind.make("codex");
const projectId = ProjectId.make("project:startup-cold");

const ids = (name: string) => ({
  threadId: ThreadId.make(`thread:startup:${name}`),
  providerThreadId: ProviderThreadId.make(`provider-thread:startup:${name}`),
  providerSessionId: ProviderSessionId.make(`provider-session:startup:${name}`),
  runId: RunId.make(`run:startup:${name}`),
  attemptId: RunAttemptId.make(`attempt:startup:${name}`),
  messageId: MessageId.make(`message:startup:${name}`),
});

// Every open builds fresh services around the same file. A disposed runtime's
// memoized services and observation state cannot stand in for cold recovery.
const runtimeLayer = (
  dbPath: string,
  monitoring?: EventSink.ProviderBindingExpectationV2,
  optedIn = false,
) => {
  const database = makeSqlitePersistenceLive(dbPath).pipe(Layer.provide(NodeServices.layer));
  const stores = Layer.mergeAll(
    EventStore.layer,
    ProjectionStore.layer,
    EffectOutbox.layer,
    ProviderSessionRuntime.layer,
  ).pipe(Layer.provideMerge(database));
  const core = Layer.mergeAll(
    stores,
    IdAllocator.layer,
    EventSink.layer.pipe(Layer.provide(stores)),
    ServerSettings.layerTest({ continueThreadsAfterServerUpdate: optedIn }),
  );
  const importer = LegacyImporter.layer.pipe(Layer.provide(core));
  const recovery = Recovery.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        core,
        Layer.mock(ProviderSessions.ProviderSessionManagerV2)({
          observeCurrentThreadRuntime: (threadId) =>
            Effect.gen(function* () {
              if (monitoring === undefined)
                return yield* Effect.die(
                  "Cold storage recovery must not claim a resident native process",
                );
              assert.equal(threadId, monitoring.threadId);
              if (monitoring.runtimeGeneration === null || monitoring.nativeThreadId === null)
                return yield* Effect.die(
                  "The synthetic monitoring fixture requires a concrete native binding",
                );
              return {
                status: "monitoring" as const,
                binding: {
                  threadId,
                  providerThreadId: monitoring.providerThreadId,
                  providerSessionId: monitoring.providerSessionId,
                  instanceId: monitoring.instanceId,
                  runtimeGeneration: monitoring.runtimeGeneration,
                  nativeThreadId: monitoring.nativeThreadId,
                },
                observedAt: "2026-10-03T00:00:00.000Z",
              };
            }),
        }),
      ),
    ),
  );
  return Layer.fresh(Layer.mergeAll(core, importer, recovery));
};

const fixture = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  // A disk-backed root owns the database, WAL, and worktree placeholder together.
  const root = yield* fs.makeTempDirectoryScoped({
    directory: process.cwd(),
    prefix: ".startup-cold-",
  });
  const worktreePath = path.join(root, "stopped-binding-worktree");
  yield* fs.makeDirectory(worktreePath);
  yield* fs.writeFileString(
    path.join(worktreePath, "preserved.txt"),
    "Unrelated worktree contents",
  );
  return { root, dbPath: path.join(root, "state.sqlite"), worktreePath };
});

const seed = Effect.fnUntraced(function* (
  name: string,
  worktreePath: string,
  status: "starting" | "completed" | "queued",
  stopped = false,
) {
  const value = ids(name);
  const now = yield* DateTime.now;
  const thread: OrchestrationV2AppThread = {
    id: value.threadId,
    projectId,
    title: name,
    providerInstanceId: instanceId,
    modelSelection: { instanceId, model: "fixture-model" },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: "fixture-branch",
    worktreePath,
    activeProviderThreadId: value.providerThreadId,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: value.threadId },
    forkedFrom: null,
    createdBy: "user",
    creationSource: "web",
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    deletedAt: null,
    settledAt: null,
    settledOverride: null,
    lastVisitedAt: null,
  };
  const session: OrchestrationV2ProviderSession = {
    id: value.providerSessionId,
    providerInstanceId: instanceId,
    driver,
    status: "ready",
    cwd: worktreePath,
    model: "fixture-model",
    capabilities: CodexProviderCapabilitiesV2,
    createdAt: now,
    updatedAt: now,
    lastError: null,
  };
  const events: ReadonlyArray<OrchestrationV2DomainEvent> = [
    {
      id: EventId.make(`birth:${name}`),
      type: "thread.created",
      threadId: value.threadId,
      occurredAt: now,
      payload: thread,
    },
    {
      id: EventId.make(`session:${name}`),
      type: "provider-session.attached",
      threadId: value.threadId,
      occurredAt: now,
      payload: session,
    },
    {
      id: EventId.make(`provider:${name}`),
      type: "provider-thread.updated",
      threadId: value.threadId,
      occurredAt: now,
      payload: {
        id: value.providerThreadId,
        appThreadId: value.threadId,
        ownerNodeId: null,
        providerInstanceId: instanceId,
        driver,
        providerSessionId: value.providerSessionId,
        nativeThreadRef: { driver, nativeId: `native:${name}`, strength: "strong" },
        nativeConversationHeadRef: null,
        status: stopped ? "not_loaded" : "idle",
        firstRunOrdinal: 1,
        lastRunOrdinal: 1,
        ...(status === "completed"
          ? {
              pendingBackgroundTasks: [
                {
                  kind: "monitor" as const,
                  taskId: `monitor:${name}`,
                  description: "Captured monitoring",
                },
              ],
            }
          : {}),
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      },
    },
    {
      id: EventId.make(`run:${name}`),
      type: "run.created",
      threadId: value.threadId,
      occurredAt: now,
      payload: {
        id: value.runId,
        threadId: value.threadId,
        ordinal: 1,
        providerInstanceId: instanceId,
        modelSelection: thread.modelSelection,
        providerThreadId: value.providerThreadId,
        userMessageId: value.messageId,
        rootNodeId: NodeId.make(`node:${name}`),
        activeAttemptId: value.attemptId,
        status,
        ...(status === "queued" ? { queueHeld: true } : {}),
        requestedAt: now,
        startedAt: status === "completed" ? now : null,
        completedAt: status === "completed" ? now : null,
        checkpointId: null,
        contextHandoffId: null,
      },
    },
    {
      id: EventId.make(`attempt:${name}`),
      type: "run-attempt.created",
      threadId: value.threadId,
      occurredAt: now,
      payload: {
        id: value.attemptId,
        runId: value.runId,
        attemptOrdinal: 1,
        rootNodeId: NodeId.make(`node:${name}`),
        providerInstanceId: instanceId,
        providerThreadId: value.providerThreadId,
        providerTurnId: null,
        reason: "initial",
        status: status === "completed" ? "completed" : "pending",
        startedAt: status === "completed" ? now : null,
        completedAt: status === "completed" ? now : null,
      },
    },
  ];
  const sink = yield* EventSink.EventSinkV2;
  yield* sink.write({ events });
  const resumeCursor = { threadId: `native:${name}` };
  const runtimePayload = { cwd: worktreePath, unrelated: `preserve:${name}`, activeTurnId: null };
  const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
  yield* runtimes.upsert({
    threadId: value.threadId,
    providerName: "codex",
    providerInstanceId: instanceId,
    adapterKey: "codex",
    status: stopped ? "stopped" : "running",
    runtimeMode: "full-access",
    resumeCursor,
    runtimePayload,
    lastSeenAt: DateTime.formatIso(now),
  });
  const binding: EventSink.ProviderBindingExpectationV2 = {
    threadId: value.threadId,
    providerThreadId: value.providerThreadId,
    providerSessionId: value.providerSessionId,
    instanceId,
    driver,
    nativeThreadId: `native:${name}`,
    runtimeGeneration: `fixture-generation:${name}`,
  };
  assert.isTrue(
    (yield* sink.registerProviderRuntime({
      expectedBinding: { ...binding, runtimeGeneration: null },
      expectedEvidenceRevision: 0,
      actualBinding: {
        threadId: value.threadId,
        providerThreadId: value.providerThreadId,
        providerSessionId: value.providerSessionId,
        instanceId,
        nativeThreadId: `native:${name}`,
        runtimeGeneration: `fixture-generation:${name}`,
      },
    })).committed,
  );
  // A stopped binding retains an earlier registration. The production guard
  // rejects registering a new generation against an already stopped session.
  if (stopped)
    yield* sink.write({
      events: [
        {
          id: EventId.make(`session-stopped:${name}`),
          type: "provider-session.updated",
          threadId: value.threadId,
          occurredAt: now,
          payload: { ...session, status: "stopped" },
        },
      ],
    });
  return { ...value, thread, binding, resumeCursor, runtimePayload };
});

// This helper uses the production ordering, command queue and activation parking.
// The parked root observes queue readiness only; no provider effect is executed.
// Trial uses a deterministic commit barrier. Native Jones grant/listener
// qualification remains covered by the native trial helper's separate checks.
const coldStartup = Effect.fnUntraced(function* <E>(
  inspectBeforeTrial: (stage: Recovery.ProviderStartupRecoveryStage) => Effect.Effect<void, E>,
) {
  const recovery = yield* Recovery.ProviderRuntimeRecoveryService;
  const importer = yield* LegacyImporter.LegacyV1ThreadImporter;
  const activated = yield* Deferred.make<void>();
  const trialEntered = yield* Deferred.make<void>();
  const trialCommit = yield* Deferred.make<void>();
  const afterTrialEntered = yield* Deferred.make<void>();
  const permitActivation = yield* Deferred.make<void>();
  const workerRan = yield* Deferred.make<void>();
  const relayRan = yield* Deferred.make<void>();
  const commandRan = yield* Deferred.make<void>();
  const workerRef = yield* Ref.make<Fiber.Fiber<void, never> | null>(null);
  const gate = yield* Startup.makeCommandGate;
  const queuedCommand = yield* gate
    .enqueueCommand(Deferred.succeed(commandRan, undefined))
    .pipe(Effect.forkScoped);
  const staged = yield* Startup.runOrderedV2StartupPhases({
    importLegacyShells: importer.reconcileShells,
    recover: recovery.stageStartupRecovery,
    startEffectWorker: Startup.startEffectWorkerWithRelay({
      runWorker: Deferred.succeed(workerRan, undefined).pipe(Effect.andThen(Effect.never)),
      startRelay: ServerActivation.forkParked(
        Deferred.succeed(relayRan, undefined).pipe(Effect.andThen(Effect.never)),
      ),
      workerFiberRef: workerRef,
    }),
    autoBootstrap: Effect.succeed({}),
  }).pipe(Effect.provideService(ServerActivation.ServerActivation, Deferred.await(activated)));
  yield* inspectBeforeTrial(staged.recovery);
  let result: Recovery.ProviderStartupRecoveryResult | undefined;
  const activation = yield* Startup.runOrderedV2ActivationPhases({
    awaitHttpListening: Effect.void,
    awaitAuxiliaryParked: Effect.void,
    prepareTrial: Effect.succeed({ selectedDatabase: "state.sqlite" }),
    commitJonesTrial: Deferred.succeed(trialEntered, undefined).pipe(
      Effect.andThen(Deferred.await(trialCommit)),
    ),
    reconcileAfterTrial: recovery.reconcileAfterStartupTrial(staged.recovery).pipe(
      Effect.tap((value) =>
        Effect.sync(() => {
          result = value;
        }),
      ),
      Effect.andThen(Deferred.succeed(afterTrialEntered, undefined)),
      Effect.andThen(Deferred.await(permitActivation)),
    ),
    publishWelcome: Effect.void,
    activate: Deferred.succeed(activated, undefined).pipe(Effect.asVoid),
    signalCommandReady: gate.signalCommandReady,
  }).pipe(Effect.forkScoped);
  yield* Deferred.await(trialEntered);
  assert.isFalse(yield* Deferred.isDone(workerRan));
  assert.isFalse(yield* Deferred.isDone(relayRan));
  assert.isFalse(yield* Deferred.isDone(commandRan));
  yield* Deferred.succeed(trialCommit, undefined);
  yield* Deferred.await(afterTrialEntered);
  assert.isFalse(yield* Deferred.isDone(activated));
  assert.isFalse(yield* Deferred.isDone(commandRan));
  assert.isFalse(yield* Deferred.isDone(workerRan));
  assert.isFalse(yield* Deferred.isDone(relayRan));
  yield* Deferred.succeed(permitActivation, undefined);
  yield* Fiber.join(activation);
  yield* gate.awaitCommandReady;
  yield* Fiber.join(queuedCommand);
  yield* Deferred.await(workerRan);
  yield* Deferred.await(relayRan);
  assert.isTrue(yield* Deferred.isDone(commandRan));
  if (result === undefined) return yield* Effect.die("Posttrial recovery did not run");
  return result;
});

it.effect(
  "cold file recovery preserves stopped native/worktree identity and accepted held V2 queue behind trial readiness",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture;
        const prior = yield* Effect.scoped(
          Effect.gen(function* () {
            const starting = yield* seed("starting", f.worktreePath, "starting");
            const stopped = yield* seed("stopped", f.worktreePath, "starting", true);
            const queued = yield* seed("queued", f.worktreePath, "queued", true);
            const sql = yield* SqlClient.SqlClient;
            yield* sql`CREATE TABLE startup_unrelated (value TEXT NOT NULL)`;
            yield* sql`INSERT INTO startup_unrelated VALUES ('preserve-unrelated')`;
            return { starting, stopped, queued };
          }).pipe(Effect.provide(runtimeLayer(f.dbPath), { local: true })),
        );
        yield* Effect.scoped(
          Effect.gen(function* () {
            const projections = yield* ProjectionStore.ProjectionStoreV2;
            const before = yield* projections.getThreadProjection(prior.starting.threadId);
            assert.equal(before.runs[0]?.status, "starting");
            const result = yield* coldStartup((stage) =>
              Effect.gen(function* () {
                assert.deepEqual(stage.continuationMarkers, []);
                assert.deepEqual(
                  yield* projections.getThreadProjection(prior.starting.threadId),
                  before,
                );
              }),
            );
            assert.equal(result.terminalizedRuns, 2);
            const recovered = yield* projections.getThreadProjection(prior.starting.threadId);
            assert.equal(recovered.runs[0]?.status, "cancelled");
            assert.equal(recovered.providerSessions[0]?.status, "stopped");
            const stopped = yield* projections.getThreadProjection(prior.stopped.threadId);
            assert.equal(stopped.thread.worktreePath, f.worktreePath);
            assert.equal(
              stopped.providerThreads[0]?.nativeThreadRef?.nativeId,
              prior.stopped.binding.nativeThreadId,
            );
            const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
            const stoppedRow = Option.getOrThrow(
              yield* runtimes.getByThreadId({ threadId: prior.stopped.threadId }),
            );
            assert.equal(stoppedRow.status, "stopped");
            assert.deepEqual(stoppedRow.resumeCursor, prior.stopped.resumeCursor);
            assert.deepEqual(stoppedRow.runtimePayload, prior.stopped.runtimePayload);
            const queued = yield* projections.getThreadProjection(prior.queued.threadId);
            assert.equal(queued.runs[0]?.status, "queued");
            assert.isTrue(queued.runs[0]?.queueHeld);
            const sql = yield* SqlClient.SqlClient;
            assert.deepEqual(yield* sql`SELECT value FROM startup_unrelated`, [
              { value: "preserve-unrelated" },
            ]);
            const fs = yield* FileSystem.FileSystem;
            const path = yield* Path.Path;
            assert.equal(
              yield* fs.readFileString(path.join(f.worktreePath, "preserved.txt")),
              "Unrelated worktree contents",
            );
          }).pipe(Effect.provide(runtimeLayer(f.dbPath), { local: true })),
        );
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

for (const mode of [
  "opt-in desktop restart",
  "marked remote update",
  "changed source",
  "current STOP",
] as const) {
  it.effect(`cold ${mode} retains the complete marker and gates its continuation after trial`, () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture;
        const name = "captured-source";
        const sourceIds = ids(name);
        const binding: EventSink.ProviderBindingExpectationV2 = {
          threadId: sourceIds.threadId,
          providerThreadId: sourceIds.providerThreadId,
          providerSessionId: sourceIds.providerSessionId,
          instanceId,
          driver,
          nativeThreadId: `native:${name}`,
          runtimeGeneration: `fixture-generation:${name}`,
        };
        const captured = yield* Effect.scoped(
          Effect.gen(function* () {
            const source = yield* seed(name, f.worktreePath, "completed");
            const markers = yield* mode === "opt-in desktop restart"
              ? Startup.markOptedInProviderSessionsForContinuation
              : Startup.markRunningProviderSessionsForContinuation;
            assert.lengthOf(markers, 1);
            const sink = yield* EventSink.EventSinkV2;
            assert.deepEqual(yield* sink.readDormantRestartContinuations, markers);
            return { source, marker: markers[0]! };
          }).pipe(
            Effect.provide(runtimeLayer(f.dbPath, binding, mode === "opt-in desktop restart"), {
              local: true,
            }),
          ),
        );
        yield* Effect.scoped(
          Effect.gen(function* () {
            const sink = yield* EventSink.EventSinkV2;
            const projections = yield* ProjectionStore.ProjectionStoreV2;
            const outbox = yield* EffectOutbox.EffectOutboxV2;
            const marker = captured.marker;
            assert.deepEqual(marker, {
              markerId: marker.markerId,
              threadId: captured.source.threadId,
              projectId,
              sourceRunId: captured.source.runId,
              sourceRunAttemptId: captured.source.attemptId,
              binding: captured.source.binding,
              evidenceRevision: 1,
              createdAt: marker.createdAt,
            });
            const commandId = CommandId.make(`command:restart-continuation:${marker.markerId}`);
            const result = yield* coldStartup((stage) =>
              Effect.gen(function* () {
                assert.deepEqual(stage.continuationMarkers, [marker]);
                assert.deepEqual(yield* outbox.listByCommandId(commandId), []);
                if (mode === "changed source") {
                  const projection = yield* projections.getThreadProjection(marker.threadId);
                  yield* sink.write({
                    events: [
                      {
                        id: EventId.make("changed-source-selection"),
                        type: "thread.metadata-updated",
                        threadId: marker.threadId,
                        occurredAt: yield* DateTime.now,
                        payload: {
                          ...projection.thread,
                          modelSelection: {
                            ...projection.thread.modelSelection,
                            instanceId: ProviderInstanceId.make("changed-instance"),
                          },
                        },
                      },
                    ],
                  });
                }
              }),
            );
            if (mode === "changed source") {
              assert.deepEqual(result.releasedContinuationMarkerIds, []);
              assert.deepEqual(result.heldContinuationMarkers, [
                { marker, reason: "source_changed" },
              ]);
              assert.deepEqual(yield* sink.readDormantRestartContinuations, [marker]);
              assert.deepEqual(yield* outbox.listByCommandId(commandId), []);
              return;
            }
            assert.deepEqual(result.releasedContinuationMarkerIds, [marker.markerId]);
            assert.deepEqual(result.heldContinuationMarkers, []);
            const effects = yield* outbox.listByCommandId(commandId);
            assert.lengthOf(effects, 1);
            assert.equal(effects[0]!.status, "pending");
            assert.deepEqual(effects[0]!.request, {
              type: "provider-runtime.continue",
              sourceRunId: marker.sourceRunId,
            });
            assert.deepEqual(
              yield* sink.readReleasedRestartContinuation({
                effectId: effects[0]!.id,
                threadId: marker.threadId,
                sourceRunId: marker.sourceRunId,
              }),
              marker,
            );
            const claimed = Option.getOrThrow(
              yield* outbox.claimNext({ workerId: "worker:startup-cold", leaseDurationMs: 60_000 }),
            );
            const command = {
              type: "message.dispatch" as const,
              ...EventSink.capturedRestartContinuationIdsV1({ effectId: claimed.id, marker }),
              threadId: marker.threadId,
              text: "Continue where you left off.",
              attachments: [],
              modelSelection: captured.source.thread.modelSelection,
              dispatchMode: { type: "start_immediately" as const },
              createdBy: "agent" as const,
              creationSource: "server" as const,
              restartContinuationOfRunId: marker.sourceRunId,
            };
            const context = {
              effectId: claimed.id,
              marker,
              workerId: "worker:startup-cold",
              expectedAttempt: claimed.attemptCount,
            };
            // Storage qualification is synthetic and exact. This proves the STOP
            // persistence fence, not production caller authority or physical closure.
            if (mode === "current STOP") {
              const stopCommandId = CommandId.make("command:startup:current-stop");
              const runtimeGeneration = marker.binding.runtimeGeneration;
              if (runtimeGeneration === null)
                return yield* Effect.die(
                  "The current STOP fixture requires a registered runtime generation",
                );
              const facts = yield* sink.readNativeCommandFacts({
                threadId: marker.threadId,
                commandId: stopCommandId,
              });
              const now = yield* DateTime.now;
              yield* sink.commitCommand({
                commandId: stopCommandId,
                threadId: marker.threadId,
                commandType: "provider-session.detach",
                acceptedAt: now,
                events: [
                  {
                    id: EventId.make("event:startup:current-stop"),
                    type: "provider-session.detach-requested",
                    threadId: marker.threadId,
                    occurredAt: now,
                    payload: {
                      providerSessionId: marker.binding.providerSessionId,
                      reason: "explicit startup fixture STOP",
                    },
                  },
                ],
                effects: [],
                stopContext: {
                  snapshot: facts.commitSnapshot,
                  incarnation: (yield* sink.readApplicationThreadBirth(marker.threadId))!,
                  canonicalRequestDigest: "c".repeat(64),
                  actorBindingDigest: "d".repeat(64),
                  targetBinding: { ...marker.binding, runtimeGeneration },
                  targetEvidenceRevision: marker.evidenceRevision,
                  queuedBases: [],
                  affectedRunIds: [],
                  revalidateCurrentTarget: Effect.gen(function* () {
                    const current = yield* sink.readNativeCommandFacts({
                      threadId: marker.threadId,
                      commandId: stopCommandId,
                    });
                    assert.deepEqual(current.commitSnapshot, facts.commitSnapshot);
                    const registered = yield* sink.readProviderRuntimeEvidence(marker.threadId);
                    assert.deepEqual(registered?.binding, marker.binding);
                    assert.equal(registered?.evidenceRevision, marker.evidenceRevision);
                  }),
                },
              });
              const error = yield* sink
                .readCapturedRestartCommandOrigin({ command, context })
                .pipe(Effect.flip);
              assert.equal(error._tag, "NativeCommandPreconditionError");
              assert.isNull((yield* sink.readCommandReceiptIdentity(command.commandId)).receipt);
            } else {
              assert.isNull(yield* sink.readCapturedRestartCommandOrigin({ command, context }));
              const changedMarker = { ...marker, createdAt: "2026-10-03T01:00:00.000Z" };
              const error = yield* sink
                .readCapturedRestartCommandOrigin({
                  command,
                  context: { ...context, marker: changedMarker },
                })
                .pipe(Effect.flip);
              assert.equal(error._tag, "NativeCommandPreconditionError");
            }
            const projection = yield* projections.getThreadProjection(marker.threadId);
            assert.equal(projection.runs.length, 1);
            assert.equal(projection.thread.worktreePath, f.worktreePath);
            assert.equal(
              projection.providerThreads[0]?.nativeThreadRef?.nativeId,
              marker.binding.nativeThreadId,
            );
            assert.lengthOf(yield* outbox.listByCommandId(commandId), 1);
          }).pipe(Effect.provide(runtimeLayer(f.dbPath), { local: true })),
        );
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );
}

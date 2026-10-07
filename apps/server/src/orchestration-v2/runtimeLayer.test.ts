import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import * as Sink from "effect/Sink";
import { makeGitVcsDriverCore } from "../vcs/GitVcsDriverCore.ts";
import * as ThreadManagement from "./ThreadManagementService.ts";
import * as EventStore from "./EventStore.ts";
import { awaitThreadCreationCleanup } from "./ThreadDeletion.ts";
import { makeProviderFailure } from "./ProviderFailure.ts";
import {
  legacyBootstrapCreateCommandId,
  legacyBootstrapBirth,
  legacyPreparationGeneration,
  legacyPreparationEffectId,
  canonicalLegacyPayload,
  legacyPayloadHash,
  transitionLegacyPreparation,
} from "./LegacyBootstrap.ts";
import {
  LegacyNoTerminalControl,
  LegacyOwnedTerminalControl,
  type LegacyPreparation,
  type LegacyPreparationUpdate,
  type LegacyGuardRejectionDeleteCommand,
} from "./RecordedTypes.ts";
import { makeCommandObservationQuery } from "./CommandObservation.ts";
import {
  OrchestrationCommandObservation,
  type OrchestrationV2Command,
  type ThreadTurnDispatchGuard,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { limitRecoveryCommand } from "./UsageLimitRecoveryWorker.ts";
import * as SourceControlProviderRegistry from "../sourceControl/SourceControlProviderRegistry.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import {
  type ApplicationStoredEvent,
  CheckpointId,
  CheckpointRef,
  CommandId,
  ContextTransferId,
  EventId,
  MessageId,
  NodeId,
  RuntimeRequestId,
  TurnItemId,
  TurnId,
  type ModelSelection,
  type OrchestrationV2Run,
  ProjectId,
  type PullRequestDetail,
  type PullRequestComment,
  PullRequestOperationError,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  PlanId,
  ProviderThreadId,
  ProviderTurnId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Context from "effect/Context";
import * as FileSystem from "effect/FileSystem";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import * as CheckpointStore from "../checkpointing/CheckpointStore.ts";
import * as GitWorkflow from "../git/GitWorkflowService.ts";
import * as ServerConfig from "../config.ts";
import {
  makeSqlitePersistenceLive,
  SqlitePersistenceMemory,
} from "../persistence/Layers/Sqlite.ts";
import * as OrchestrationEventStore from "../persistence/Services/OrchestrationEventStore.ts";
import * as ProjectEnrichmentService from "../project/ProjectEnrichmentService.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as McpSessionRegistryTestkit from "../mcp/McpSessionRegistry.testkit.ts";
import * as ProviderInstanceRegistry from "../provider/Services/ProviderInstanceRegistry.ts";
import type { ProviderInstance } from "../provider/ProviderDriver.ts";
import * as VcsDriverRegistry from "../vcs/VcsDriverRegistry.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import * as WorkspacePaths from "../workspace/WorkspacePaths.ts";
import * as TerminalManager from "../terminal/Manager.ts";
import type * as PtyAdapter from "../terminal/PtyAdapter.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as LegacyV1ThreadImporter from "./legacy/LegacyV1ThreadImporter.ts";
import * as Orchestrator from "./Orchestrator.ts";
import { ROLLBACK_FAILED_MESSAGE } from "./CheckpointRollbackService.ts";
import * as EffectWorker from "./EffectWorker.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EventSink from "./EventSink.ts";
import * as ProviderRuntimeRecoveryService from "./ProviderRuntimeRecoveryService.ts";
import * as ProjectionMaintenance from "./ProjectionMaintenance.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as PullRequestWatchReactor from "./PullRequestWatchReactor.ts";
import * as PullRequestService from "../pullRequest/PullRequestService.ts";
import * as ProjectStore from "./ProjectStore.ts";
import type { ProviderAdapterV2SessionRuntime, ProviderAdapterV2Shape } from "./ProviderAdapter.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import {
  OrchestrationEventInfrastructureLayerLive,
  OrchestrationV2EventSinkLayerLive,
  OrchestrationV2LayerLive,
  ProjectServiceLayerLive,
} from "./runtimeLayer.ts";
import { shellStreamItemFromThreadShell } from "./ShellStream.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";
import * as ThreadCommandExecutor from "./ThreadCommandExecutor.ts";

const PlatformTestLayer = Layer.merge(
  NodeServices.layer,
  Layer.mock(SourceControlProviderRegistry.SourceControlProviderRegistry)({
    resolveLink: () => Effect.die("unused title link"),
  }),
);

const ServerConfigLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3-orchestration-v2-runtime-layer-",
});

const modelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5.4",
} satisfies ModelSelection;
const alternateInstanceId = ProviderInstanceId.make("codex_alternate");

const VcsDriverRegistryTestLayer = VcsDriverRegistry.layer.pipe(
  Layer.provide(VcsProcess.layer),
  Layer.provide(ServerConfigLayer),
  Layer.provide(PlatformTestLayer),
);

const CheckpointStoreTestLayer = CheckpointStore.layer.pipe(
  Layer.provide(VcsDriverRegistryTestLayer),
);
const GitWorkflowTestLayer = Layer.mock(GitWorkflow.GitWorkflowService)({
  pruneWorktrees: () => Effect.void,
  createWorktree: () => Effect.succeed({} as never),
});
const ProjectServiceTestLayer = Layer.mock(ProjectService.ProjectService)({
  getById: () => Effect.succeed(Option.none()),
});

const driver = ProviderDriverKind.make("codex");
const orchestrationAdapter = {
  instanceId: modelSelection.instanceId,
  driver,
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
  openSession: () => Effect.die("sessions are not used by lifecycle tests"),
} as ProviderAdapterV2Shape;
const providerInstance = {
  instanceId: modelSelection.instanceId,
  driverKind: driver,
  continuationIdentity: {
    driverKind: driver,
    continuationKey: "codex:test",
  },
  displayName: "Codex test",
  enabled: true,
  // No supportedRuntimeModes: every runtime mode runs as stored.
  snapshot: { getSnapshot: Effect.succeed({}) } as unknown as ProviderInstance["snapshot"],
  orchestrationAdapter,
  textGeneration: {} as ProviderInstance["textGeneration"],
} satisfies ProviderInstance;
const alternateProviderInstance = {
  ...providerInstance,
  instanceId: alternateInstanceId,
  continuationIdentity: {
    driverKind: driver,
    continuationKey: "codex:test:alternate",
  },
  displayName: "Codex alternate test",
  orchestrationAdapter: {
    ...orchestrationAdapter,
    instanceId: alternateInstanceId,
  },
} satisfies ProviderInstance;

const TestProviderInstanceRegistry = Layer.succeed(
  ProviderInstanceRegistry.ProviderInstanceRegistry,
  {
    getInstance: (instanceId) =>
      Effect.succeed(
        [providerInstance, alternateProviderInstance].find(
          (instance) => instanceId === instance.instanceId,
        ),
      ),
    listInstances: Effect.succeed([providerInstance, alternateProviderInstance]),
    listUnavailable: Effect.succeed([]),
    streamChanges: Stream.empty,
    subscribeChanges: Effect.never,
  },
);

/** Seed a project row the way a committed `project.created` event folds into it. */
const seedProject = (input: {
  readonly projectId: ProjectId;
  readonly title: string;
  readonly workspaceRoot: string;
  readonly defaultModelSelection: ModelSelection | null;
  readonly createdAt: string;
}) =>
  Effect.flatMap(ProjectStore.ProjectStoreV2, (projects) =>
    projects.apply({
      sequence: 0,
      eventId: EventId.make(`seed:${input.projectId}`),
      aggregateKind: "project",
      aggregateId: input.projectId,
      occurredAt: input.createdAt,
      commandId: null,
      causationEventId: null,
      correlationId: null,
      metadata: {},
      type: "project.created",
      payload: {
        projectId: input.projectId,
        title: input.title,
        workspaceRoot: input.workspaceRoot,
        defaultModelSelection: input.defaultModelSelection,
        scripts: [],
        createdAt: input.createdAt,
        updatedAt: input.createdAt,
      },
    }),
  );

/** Move a seeded project the way a committed `project.meta-updated` event does. */
const moveProject = (projectId: ProjectId, workspaceRoot: string, updatedAt: string) =>
  Effect.flatMap(ProjectStore.ProjectStoreV2, (projects) =>
    projects.apply({
      sequence: 0,
      eventId: EventId.make(`move:${projectId}:${workspaceRoot}`),
      aggregateKind: "project",
      aggregateId: projectId,
      occurredAt: updatedAt,
      commandId: null,
      causationEventId: null,
      correlationId: null,
      metadata: {},
      type: "project.meta-updated",
      payload: { projectId, workspaceRoot, updatedAt },
    }),
  );

const TestLayer = Layer.mergeAll(
  OrchestrationV2LayerLive,
  OrchestrationV2EventSinkLayerLive,
  ProjectStore.layer,
  ProjectionStore.layer,
  EffectOutbox.layer,
  ThreadCommandExecutor.layer,
).pipe(
  Layer.provide(McpSessionRegistryTestkit.layer),
  Layer.provide(SqlitePersistenceMemory),
  Layer.provide(CheckpointStoreTestLayer),
  Layer.provide(ServerConfigLayer),
  Layer.provide(ServerSettings.layerTest()),
  Layer.provide(TestProviderInstanceRegistry),
  Layer.provide(GitWorkflowTestLayer),
  Layer.provide(ProjectServiceTestLayer),
  Layer.provide(PlatformTestLayer),
);

const LegacyImportTestLayer = OrchestrationV2LayerLive.pipe(
  Layer.provide(McpSessionRegistryTestkit.layer),
  Layer.provideMerge(SqlitePersistenceMemory),
  Layer.provide(CheckpointStoreTestLayer),
  Layer.provide(ServerConfigLayer),
  Layer.provide(ServerSettings.layerTest()),
  Layer.provide(TestProviderInstanceRegistry),
  Layer.provide(GitWorkflowTestLayer),
  Layer.provide(ProjectServiceTestLayer),
  Layer.provide(PlatformTestLayer),
);

const ProjectDeletionTestLayer = Layer.mergeAll(
  OrchestrationV2LayerLive.pipe(Layer.provide(ProjectServiceLayerLive)),
  ProjectServiceLayerLive,
  OrchestrationV2EventSinkLayerLive,
  ThreadCommandExecutor.layer,
).pipe(
  Layer.provide(
    Layer.mock(ProjectEnrichmentService.ProjectEnrichmentService)({
      peek: () =>
        Effect.succeed({
          repositoryIdentity: null,
          faviconPath: null,
          repositoryIdentityResolved: false,
        }),
      getAvailable: () =>
        Effect.succeed({
          repositoryIdentity: null,
          faviconPath: null,
          repositoryIdentityResolved: false,
        }),
      invalidate: () => Effect.void,
    }),
  ),
  Layer.provide(
    Layer.mock(WorkspacePaths.WorkspacePaths)({
      normalizeWorkspaceRoot: (workspaceRoot) => Effect.succeed(workspaceRoot),
    }),
  ),
  Layer.provide(McpSessionRegistryTestkit.layer),
  Layer.provide(SqlitePersistenceMemory),
  Layer.provide(CheckpointStoreTestLayer),
  Layer.provide(ServerConfigLayer),
  Layer.provide(ServerSettings.layerTest()),
  Layer.provide(TestProviderInstanceRegistry),
  Layer.provide(GitWorkflowTestLayer),
  Layer.provide(PlatformTestLayer),
);

it.layer(ProjectDeletionTestLayer)("project deletion during thread commands", (it) => {
  it.effect("waits for an in-flight thread update before planning deletion", () =>
    Effect.gen(function* () {
      const projects = yield* ProjectService.ProjectService;
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const eventSink = yield* EventSink.EventSinkV2;
      const executor = yield* ThreadCommandExecutor.ThreadCommandExecutor;
      const projectId = ProjectId.make("runtime-project-delete-concurrent");
      const threadId = ThreadId.make("runtime-thread-delete-concurrent");
      const updateCommandId = CommandId.make("runtime-thread-delete-concurrent-update");
      yield* projects.create({
        commandId: CommandId.make("runtime-project-delete-concurrent-create"),
        projectId,
        title: "Concurrent deletion",
        workspaceRoot: "/work/concurrent-deletion",
      });
      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make("runtime-thread-delete-concurrent-create"),
        createdBy: "user",
        creationSource: "web",
        threadId,
        projectId,
        title: "Original title",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
      });

      const updateReady = yield* Deferred.make<void>();
      const releaseUpdate = yield* Deferred.make<void>();
      const deletionQueued = yield* Deferred.make<void>();
      const commitCommand = eventSink.commitCommand;
      const withLock = executor.withLock;
      let threadLockRequests = 0;
      const commitSpy = vi
        .spyOn(eventSink, "commitCommand")
        .mockImplementation((input) =>
          input.commandId === updateCommandId
            ? Deferred.succeed(updateReady, undefined).pipe(
                Effect.andThen(Deferred.await(releaseUpdate)),
                Effect.andThen(commitCommand(input)),
              )
            : commitCommand(input),
        );
      const observeLock: ThreadCommandExecutor.ThreadCommandExecutor["Service"]["withLock"] = (
        key,
        effect,
      ) => {
        if (key !== threadId || ++threadLockRequests !== 2) return withLock(key, effect);
        return Deferred.succeed(deletionQueued, undefined).pipe(
          Effect.andThen(withLock(key, effect)),
        );
      };
      const lockSpy = vi.spyOn(executor, "withLock").mockImplementation(observeLock);
      yield* Effect.gen(function* () {
        const updateFiber = yield* orchestrator
          .dispatch({
            type: "thread.metadata.update",
            commandId: updateCommandId,
            threadId,
            title: "Updated before deletion",
          })
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Effect.raceFirst(
          Deferred.await(updateReady),
          Fiber.join(updateFiber).pipe(
            Effect.andThen(Effect.die("The update completed before reaching its commit barrier.")),
          ),
        );
        const deleteFiber = yield* projects
          .delete({
            commandId: CommandId.make("runtime-project-delete-concurrent-delete"),
            projectId,
            force: true,
          })
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Effect.raceFirst(
          Deferred.await(deletionQueued),
          Fiber.join(deleteFiber).pipe(
            Effect.andThen(Effect.die("Project deletion bypassed the in-flight thread command.")),
          ),
        );
        yield* Deferred.succeed(releaseUpdate, undefined);
        yield* Fiber.join(updateFiber);
        const deletedProject = yield* Fiber.join(deleteFiber);
        const projection = yield* orchestrator.getThreadProjection(threadId);
        assert.isNotNull(deletedProject.deletedAt);
        assert.isNotNull(projection.thread.deletedAt);
        assert.equal(projection.thread.title, "Updated before deletion");
      }).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            commitSpy.mockRestore();
            lockSpy.mockRestore();
          }),
        ),
      );
    }),
  );
});

const SharedApplicationDataPlaneTestLayer = Layer.mergeAll(
  OrchestrationV2LayerLive.pipe(Layer.provide(ProjectServiceLayerLive)),
  ProjectServiceLayerLive,
  OrchestrationV2EventSinkLayerLive,
  OrchestrationEventInfrastructureLayerLive,
).pipe(
  Layer.provide(
    Layer.mock(WorkspacePaths.WorkspacePaths)({
      normalizeWorkspaceRoot: (workspaceRoot) => Effect.succeed(workspaceRoot),
    }),
  ),
  Layer.provide(
    Layer.succeed(ProjectEnrichmentService.ProjectEnrichmentService, {
      peek: () =>
        Effect.succeed({
          repositoryIdentity: null,
          faviconPath: null,
          repositoryIdentityResolved: false,
        }),
      request: () => Effect.void,
      getAvailable: () =>
        Effect.succeed({
          repositoryIdentity: null,
          faviconPath: null,
          repositoryIdentityResolved: false,
        }),
      invalidate: () => Effect.void,
      subscribeChanges: Effect.never,
    }),
  ),
  Layer.provide(McpSessionRegistryTestkit.layer),
  Layer.provideMerge(SqlitePersistenceMemory),
  Layer.provide(CheckpointStoreTestLayer),
  Layer.provide(ServerConfigLayer),
  Layer.provide(ServerSettings.layerTest()),
  Layer.provide(TestProviderInstanceRegistry),
  Layer.provide(GitWorkflowTestLayer),
  Layer.provide(PlatformTestLayer),
);

it.layer(TestLayer)("OrchestrationV2LayerLive", (it) => {
  it.effect("emits model updates separately from provider switches", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threadId = ThreadId.make("runtime-layer-model-selection-events");

      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-model-selection-events-create"),
        threadId,
        projectId: ProjectId.make("runtime-layer-model-selection-events-project"),
        title: "Model selection events",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
      });

      const sameInstance = yield* orchestrator.dispatch({
        type: "thread.model-selection.set",
        commandId: CommandId.make("runtime-layer-model-selection-events-update"),
        threadId,
        modelSelection: { ...modelSelection, model: "gpt-5.5" },
      });
      assert.deepEqual(
        sameInstance.storedEvents.map((stored) => stored.event.type),
        ["thread.model-selection-updated"],
      );

      const differentInstance = yield* orchestrator.dispatch({
        type: "thread.model-selection.set",
        commandId: CommandId.make("runtime-layer-model-selection-events-switch"),
        threadId,
        modelSelection: { instanceId: alternateInstanceId, model: "gpt-5.5" },
      });
      assert.deepEqual(
        differentInstance.storedEvents.map((stored) => stored.event.type),
        ["thread.provider-switched"],
      );
    }),
  );

  /**
   * A thread with one ready checkpoint and no queued turn start. Every provider
   * rollback on it fails, so each rollback effect retries until it gives up.
   */
  const seedFailingRollbackThread = (name: string) =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const eventSink = yield* EventSink.EventSinkV2;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const threadId = ThreadId.make(name);
      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make(`${name}-create`),
        threadId,
        projectId: ProjectId.make(`${name}-project`),
        title: "Rollback failure",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        // Its own path, so other rollback tests keep an isolated worktree.
        worktreePath: `/tmp/t3-${name}`,
      });
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make(`${name}-message`),
        threadId,
        messageId: MessageId.make(`${name}-message`),
        text: "Create the provider thread and checkpoint scope.",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
      });
      const scope = (yield* orchestrator.getThreadProjection(threadId)).checkpointScopes[0]!;
      const now = yield* DateTime.now;
      const checkpointId = CheckpointId.make(`${name}-checkpoint`);
      yield* eventSink.write({
        commandId: CommandId.make(`${name}-seed`),
        events: [
          {
            id: EventId.make(`${name}-checkpoint-event`),
            type: "checkpoint.captured",
            threadId,
            occurredAt: now,
            payload: {
              id: checkpointId,
              threadId,
              scopeId: scope.id,
              runId: null,
              nodeId: scope.nodeId,
              parentCheckpointId: null,
              ordinalWithinScope: 0,
              appRunOrdinal: null,
              ref: CheckpointRef.make(`refs/t3/${name}`),
              status: "ready",
              files: [],
              capturedAt: now,
            },
          },
        ],
      });
      // These tests cover rollback only, so drop the first message's start.
      yield* outbox.cancelUnsettled({
        threadId,
        effectTypes: ["provider-turn.start"],
        reason: "not under test",
      });
      return {
        threadId,
        rollback: (commandId: CommandId) =>
          orchestrator.dispatch({
            type: "checkpoint.rollback",
            commandId,
            threadId,
            checkpointId,
            scopeId: scope.id,
            restoreFiles: false,
          }),
      };
    });

  it.effect("projects a rollback that fails every attempt and clears it on the next one", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const { threadId, rollback } = yield* seedFailingRollbackThread("runtime-rollback-failure");

      const rollbackCommandId = CommandId.make("runtime-rollback-failure-rollback");
      yield* rollback(rollbackCommandId);
      // Retries back off on the clock; advance it until the worker gives up.
      for (let attempt = 0; attempt < 5; attempt++) {
        yield* worker.drain();
        yield* TestClock.adjust("30 seconds");
      }

      const [rollbackEffect] = yield* outbox.listByCommandId(rollbackCommandId);
      assert.equal(rollbackEffect?.status, "failed");
      const failed = yield* orchestrator.getThreadProjection(threadId);
      assert.deepEqual(failed.thread.rollbackFailure, {
        requestId: rollbackCommandId,
        message: ROLLBACK_FAILED_MESSAGE,
      });

      yield* rollback(CommandId.make("runtime-rollback-failure-retry"));
      const retried = yield* orchestrator.getThreadProjection(threadId);
      assert.isNull(retried.thread.rollbackFailure);
    }),
  );

  it.effect("ignores a late failure from a rollback that a newer one superseded", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const { threadId, rollback } = yield* seedFailingRollbackThread(
        "runtime-rollback-superseded",
      );

      const olderCommandId = CommandId.make("runtime-rollback-superseded-older");
      yield* rollback(olderCommandId);
      // Spend every attempt but the last.
      for (let attempt = 0; attempt < 4; attempt++) {
        yield* worker.drain();
        yield* TestClock.adjust("30 seconds");
      }
      const newerCommandId = CommandId.make("runtime-rollback-superseded-newer");
      yield* rollback(newerCommandId);
      // The older rollback's last attempt fails after the newer one started.
      yield* worker.drain();

      const [olderEffect] = yield* outbox.listByCommandId(olderCommandId);
      assert.equal(olderEffect?.status, "failed");
      const superseded = yield* orchestrator.getThreadProjection(threadId);
      assert.isNull(superseded.thread.rollbackFailure);

      for (let attempt = 0; attempt < 5; attempt++) {
        yield* TestClock.adjust("30 seconds");
        yield* worker.drain();
      }
      const [newerEffect] = yield* outbox.listByCommandId(newerCommandId);
      assert.equal(newerEffect?.status, "failed");
      const failed = yield* orchestrator.getThreadProjection(threadId);
      assert.deepEqual(failed.thread.rollbackFailure, {
        requestId: newerCommandId,
        message: ROLLBACK_FAILED_MESSAGE,
      });
    }),
  );

  it.effect("rejects non-ready rollback targets before persisting events or effects", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const eventSink = yield* EventSink.EventSinkV2;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const threadId = ThreadId.make("runtime-rollback-readiness");
      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-rollback-readiness-create"),
        threadId,
        projectId: ProjectId.make("runtime-rollback-readiness-project"),
        title: "Rollback readiness",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: process.cwd(),
      });
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-rollback-readiness-message"),
        threadId,
        messageId: MessageId.make("runtime-rollback-readiness-message"),
        text: "Create the provider thread and checkpoint scope.",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
      });
      const projection = yield* orchestrator.getThreadProjection(threadId);
      const scope = projection.checkpointScopes[0]!;
      const now = yield* DateTime.now;

      for (const status of ["missing", "error", "stale", "ready"] as const) {
        const checkpointId = CheckpointId.make("runtime-rollback-checkpoint");
        const commandId = CommandId.make(`runtime-rollback-${status}`);
        yield* eventSink.write({
          commandId: CommandId.make(`runtime-rollback-${status}-seed`),
          events: [
            {
              id: EventId.make(`runtime-rollback-${status}-event`),
              type: "checkpoint.captured",
              threadId,
              occurredAt: now,
              payload: {
                id: checkpointId,
                threadId,
                scopeId: scope.id,
                runId: null,
                nodeId: scope.nodeId,
                parentCheckpointId: null,
                ordinalWithinScope: 0,
                appRunOrdinal: null,
                ref: CheckpointRef.make(`refs/t3/runtime-rollback-${status}`),
                status,
                files: [],
                capturedAt: now,
              },
            },
          ],
        });
        const previousSequence = yield* orchestrator.getThreadEventSequence(threadId);
        const rollback = orchestrator.dispatch({
          type: "checkpoint.rollback",
          commandId,
          threadId,
          checkpointId,
          scopeId: scope.id,
        });

        if (status === "ready") {
          const accepted = yield* rollback;
          assert.deepEqual(
            accepted.storedEvents.map((stored) => stored.event.type),
            ["thread.metadata-updated", "checkpoint.rollback-requested"],
          );
          assert.deepEqual(
            (yield* outbox.listByCommandId(commandId)).map((effect) => effect.request.type),
            ["provider-thread.rollback"],
          );
          const path = yield* Path.Path.pipe(Effect.provide(NodeServices.layer));
          yield* orchestrator.dispatch({
            type: "thread.create",
            createdBy: "user",
            creationSource: "web",
            commandId: CommandId.make("runtime-rollback-ancestor-create"),
            threadId: ThreadId.make("runtime-rollback-ancestor"),
            projectId: ProjectId.make("runtime-rollback-readiness-project"),
            title: "Ancestor workspace owner",
            modelSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: path.dirname(process.cwd()),
          });
          const overlapCommandId = CommandId.make("runtime-rollback-overlap");
          const overlapSequence = yield* orchestrator.getThreadEventSequence(threadId);
          const overlap = yield* orchestrator
            .dispatch({
              type: "checkpoint.rollback",
              commandId: overlapCommandId,
              threadId,
              checkpointId,
              scopeId: scope.id,
            })
            .pipe(Effect.flip);
          assert.match(String(overlap.cause), /isolated worktree/);
          assert.equal(yield* orchestrator.getThreadEventSequence(threadId), overlapSequence);
          assert.deepEqual(yield* outbox.listByCommandId(overlapCommandId), []);
          yield* orchestrator.dispatch({
            type: "thread.metadata.update",
            commandId: CommandId.make("runtime-rollback-share"),
            threadId,
            worktreePath: null,
          });
          const sharedCommandId = CommandId.make("runtime-rollback-shared");
          const sequence = yield* orchestrator.getThreadEventSequence(threadId);
          const shared = yield* orchestrator
            .dispatch({
              type: "checkpoint.rollback",
              commandId: sharedCommandId,
              threadId,
              checkpointId,
              scopeId: scope.id,
            })
            .pipe(Effect.flip);
          assert.match(String(shared.cause), /isolated worktree/);
          assert.equal(yield* orchestrator.getThreadEventSequence(threadId), sequence);
          assert.deepEqual(yield* outbox.listByCommandId(sharedCommandId), []);
          const conversationOnly = yield* orchestrator.dispatch({
            type: "checkpoint.rollback",
            commandId: CommandId.make("runtime-rollback-conversation"),
            threadId,
            checkpointId,
            scopeId: scope.id,
            restoreFiles: false,
          });
          assert.deepEqual(
            conversationOnly.storedEvents.map((stored) => stored.event.type),
            ["thread.metadata-updated", "checkpoint.rollback-requested"],
          );
        } else {
          const error = yield* rollback.pipe(Effect.flip);
          assert.instanceOf(error, Orchestrator.OrchestratorDispatchError);
          assert.equal(
            error.cause,
            `Checkpoint ${checkpointId} is ${status} and cannot be restored.`,
          );
          assert.equal(yield* orchestrator.getThreadEventSequence(threadId), previousSequence);
          assert.deepEqual(
            yield* eventSink.readByCommandId({ commandId }).pipe(Stream.runCollect),
            [],
          );
          assert.deepEqual(yield* outbox.listByCommandId(commandId), []);
        }
      }
    }).pipe(Effect.provide(Layer.fresh(TestLayer))),
  );

  it.effect("resolves delivery intent against the active run and starts after it completes", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const eventSink = yield* EventSink.EventSinkV2;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const sessions = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const threadId = ThreadId.make("runtime-delivery-intent");
      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-delivery-intent-create"),
        threadId,
        projectId: ProjectId.make("runtime-delivery-intent-project"),
        title: "Delivery intent",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: process.cwd(),
      });
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-delivery-intent-first"),
        threadId,
        messageId: MessageId.make("runtime-delivery-intent-first"),
        text: "Start work.",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
      });
      const initial = yield* orchestrator.getThreadProjection(threadId);
      const run = initial.runs[0]!;
      const providerThread = initial.providerThreads[0]!;
      const now = yield* DateTime.now;
      const providerSession = {
        id: providerThread.providerSessionId!,
        driver,
        providerInstanceId: modelSelection.instanceId,
        status: "running" as const,
        cwd: process.cwd(),
        model: modelSelection.model,
        capabilities: CodexProviderCapabilitiesV2,
        createdAt: now,
        updatedAt: now,
        lastError: null,
      };
      const providerTurn = {
        id: ProviderTurnId.make("runtime-delivery-intent-turn"),
        providerThreadId: providerThread.id,
        nodeId: run.rootNodeId!,
        runAttemptId: run.activeAttemptId,
        nativeTurnRef: null,
        ordinal: 1,
        status: "running" as const,
        startedAt: now,
        completedAt: null,
      };
      yield* eventSink.write({
        commandId: CommandId.make("runtime-delivery-intent-running"),
        events: [
          {
            id: EventId.make("runtime-delivery-intent-run-event"),
            type: "run.updated",
            threadId,
            runId: run.id,
            occurredAt: now,
            payload: { ...run, status: "running", startedAt: now },
          },
          {
            id: EventId.make("runtime-delivery-intent-session-event"),
            type: "provider-session.attached",
            threadId,
            occurredAt: now,
            payload: providerSession,
          },
          {
            id: EventId.make("runtime-delivery-intent-turn-event"),
            type: "provider-turn.updated",
            threadId,
            runId: run.id,
            occurredAt: now,
            payload: providerTurn,
          },
        ],
      });
      const sessionSpy = vi
        .spyOn(sessions, "get")
        .mockReturnValue(
          Effect.succeed(Option.some({ providerSession } as ProviderAdapterV2SessionRuntime)),
        );
      yield* Effect.addFinalizer(() => Effect.sync(() => sessionSpy.mockRestore()));

      const steerCommandId = CommandId.make("runtime-delivery-intent-auto");
      const steerMessageId = MessageId.make("runtime-delivery-intent-auto");
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        createdBy: "user",
        creationSource: "web",
        commandId: steerCommandId,
        threadId,
        messageId: steerMessageId,
        text: "Include this in the active work.",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
        deliveryIntent: "auto",
      });
      const steered = yield* orchestrator.getThreadProjection(threadId);
      assert.lengthOf(steered.runs, 1);
      assert.equal(
        steered.messages.find((message) => message.id === steerMessageId)?.runId,
        run.id,
      );
      assert.deepEqual(
        (yield* outbox.listByCommandId(steerCommandId)).map((effect) => effect.request),
        [
          {
            type: "provider-turn.steer",
            providerSessionId: providerSession.id,
            providerThreadId: providerThread.id,
            providerTurnId: providerTurn.id,
            messageId: steerMessageId,
          },
        ],
      );

      yield* eventSink.write({
        commandId: CommandId.make("runtime-delivery-intent-completed"),
        events: [
          {
            id: EventId.make("runtime-delivery-intent-run-completed"),
            type: "run.updated",
            threadId,
            runId: run.id,
            occurredAt: now,
            payload: { ...run, status: "completed", startedAt: now, completedAt: now },
          },
          {
            id: EventId.make("runtime-delivery-intent-turn-completed"),
            type: "provider-turn.updated",
            threadId,
            runId: run.id,
            occurredAt: now,
            payload: { ...providerTurn, status: "completed", completedAt: now },
          },
        ],
      });
      const nextCommandId = CommandId.make("runtime-delivery-intent-next");
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        createdBy: "user",
        creationSource: "web",
        commandId: nextCommandId,
        threadId,
        messageId: MessageId.make("runtime-delivery-intent-next"),
        text: "The previous run finished before this arrived.",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
        deliveryIntent: "restart",
      });
      const restarted = yield* orchestrator.getThreadProjection(threadId);
      assert.deepEqual(
        restarted.runs.map((candidate) => candidate.status),
        ["completed", "starting"],
      );
      assert.deepEqual(
        (yield* outbox.listByCommandId(nextCommandId)).map((effect) => effect.request.type),
        ["provider-turn.start"],
      );
    }),
  );

  it.effect("answers an async question after its provider exits and commits the answer once", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const eventSink = yield* EventSink.EventSinkV2;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("runtime-async-question");
      const requestId = RuntimeRequestId.make("runtime-async-question-request");
      const nodeId = NodeId.make("runtime-async-question-node");
      const itemId = TurnItemId.make("runtime-async-question-item");
      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make("runtime-async-question-create"),
        createdBy: "user",
        creationSource: "web",
        threadId,
        projectId: ProjectId.make("runtime-async-question-project"),
        title: "Async question",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: process.cwd(),
      });
      yield* eventSink.write({
        commandId: CommandId.make("runtime-async-question-seed"),
        events: [
          {
            id: EventId.make("runtime-async-question-node-event"),
            type: "node.updated",
            threadId,
            nodeId,
            occurredAt: now,
            payload: {
              id: nodeId,
              threadId,
              runId: null,
              parentNodeId: null,
              rootNodeId: nodeId,
              kind: "user_input_request",
              status: "waiting",
              countsForRun: false,
              providerThreadId: null,
              providerTurnId: null,
              nativeItemRef: null,
              runtimeRequestId: requestId,
              checkpointScopeId: null,
              startedAt: now,
              completedAt: null,
            },
          },
          {
            id: EventId.make("runtime-async-question-request-event"),
            type: "runtime-request.updated",
            threadId,
            nodeId,
            occurredAt: now,
            payload: {
              id: requestId,
              nodeId,
              providerTurnId: null,
              nativeRequestRef: null,
              kind: "user_input",
              status: "pending",
              responseCapability: { type: "message" },
              createdAt: now,
              resolvedAt: null,
            },
          },
          {
            id: EventId.make("runtime-async-question-item-event"),
            type: "turn-item.updated",
            threadId,
            nodeId,
            occurredAt: now,
            payload: {
              id: itemId,
              type: "user_input_request",
              threadId,
              runId: null,
              nodeId,
              providerThreadId: null,
              providerTurnId: null,
              nativeItemRef: null,
              parentItemId: null,
              ordinal: 0,
              status: "waiting",
              title: null,
              startedAt: now,
              completedAt: null,
              updatedAt: now,
              requestId,
              responseMode: "message",
              questions: [{ id: "color", header: "Color", question: "Which color?", options: [] }],
            },
          },
        ],
      });
      const invalid = yield* orchestrator
        .dispatch({
          type: "runtime-request.respond",
          commandId: CommandId.make("runtime-async-question-blank"),
          threadId,
          requestId,
          answers: { color: " " },
        })
        .pipe(Effect.result);
      assert.equal(invalid._tag, "Failure");
      const unanswered = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(unanswered.runtimeRequests[0]?.status, "pending");
      assert.deepEqual(unanswered.messages, []);

      const command = {
        type: "runtime-request.respond" as const,
        commandId: CommandId.make("runtime-async-question-answer"),
        threadId,
        requestId,
        answers: { color: "  Blue  " },
      };
      const accepted = yield* orchestrator.dispatch(command);
      const repeated = yield* orchestrator.dispatch(command);
      assert.equal(repeated.sequence, accepted.sequence);
      const answered = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(answered.runtimeRequests[0]?.status, "resolved");
      assert.deepEqual(answered.runtimeRequests[0]?.answers, command.answers);
      assert.equal(answered.nodes.find((node) => node.id === nodeId)?.status, "completed");
      assert.equal(answered.turnItems.find((item) => item.id === itemId)?.status, "completed");
      const answeredItem = answered.turnItems.find((item) => item.id === itemId);
      assert.equal(answeredItem?.type, "user_input_request");
      if (answeredItem?.type === "user_input_request") {
        assert.deepEqual(answeredItem.questionAnswer, {
          requestId,
          answers: command.answers,
          attachmentsByQuestionId: {},
          questionTextById: { color: "Which color?" },
        });
      }
      assert.equal(answered.messages.length, 1);
      assert.equal(answered.messages[0]?.text, "Which color?\nBlue");
      assert.equal(answered.messages[0]?.role, "user");
      assert.equal(answered.runs.length, 1);

      const duplicate = yield* orchestrator
        .dispatch({
          ...command,
          commandId: CommandId.make("runtime-async-question-duplicate"),
        })
        .pipe(Effect.result);
      assert.equal(duplicate._tag, "Failure");
      assert.equal((yield* orchestrator.getThreadProjection(threadId)).messages.length, 1);
    }),
  );

  it.effect("dismisses message-capable questions directly and while settling", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const eventSink = yield* EventSink.EventSinkV2;

      const seedQuestion = Effect.fn("runtimeLayerTest.seedQuestion")(function* (name: string) {
        const threadId = ThreadId.make(`${name}-thread`);
        const requestId = RuntimeRequestId.make(`${name}-request`);
        const nodeId = NodeId.make(`${name}-node`);
        const itemId = TurnItemId.make(`${name}-item`);
        const now = yield* DateTime.now;
        yield* orchestrator.dispatch({
          type: "thread.create",
          commandId: CommandId.make(`${name}-create`),
          createdBy: "user",
          creationSource: "web",
          threadId,
          projectId: ProjectId.make(`${name}-project`),
          title: "Dismissible question",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: process.cwd(),
        });
        yield* eventSink.write({
          commandId: CommandId.make(`${name}-seed`),
          events: [
            {
              id: EventId.make(`${name}-node-event`),
              type: "node.updated",
              threadId,
              nodeId,
              occurredAt: now,
              payload: {
                id: nodeId,
                threadId,
                runId: null,
                parentNodeId: null,
                rootNodeId: nodeId,
                kind: "user_input_request",
                status: "waiting",
                countsForRun: false,
                providerThreadId: null,
                providerTurnId: null,
                nativeItemRef: null,
                runtimeRequestId: requestId,
                checkpointScopeId: null,
                startedAt: now,
                completedAt: null,
              },
            },
            {
              id: EventId.make(`${name}-request-event`),
              type: "runtime-request.updated",
              threadId,
              nodeId,
              occurredAt: now,
              payload: {
                id: requestId,
                nodeId,
                providerTurnId: null,
                nativeRequestRef: null,
                kind: "user_input",
                status: "pending",
                responseCapability: { type: "message" },
                createdAt: now,
                resolvedAt: null,
              },
            },
            {
              id: EventId.make(`${name}-item-event`),
              type: "turn-item.updated",
              threadId,
              nodeId,
              occurredAt: now,
              payload: {
                id: itemId,
                type: "user_input_request",
                threadId,
                runId: null,
                nodeId,
                providerThreadId: null,
                providerTurnId: null,
                nativeItemRef: null,
                parentItemId: null,
                ordinal: 0,
                status: "waiting",
                title: null,
                startedAt: now,
                completedAt: null,
                updatedAt: now,
                requestId,
                responseMode: "message",
                questions: [{ id: "choice", header: "Choice", question: "Continue?", options: [] }],
              },
            },
          ],
        });
        return { threadId, requestId, nodeId, itemId };
      });

      const dismissed = yield* seedQuestion("runtime-dismiss-question");
      yield* orchestrator.dispatch({
        type: "thread.user-input.dismiss",
        commandId: CommandId.make("runtime-dismiss-question-command"),
        threadId: dismissed.threadId,
        requestId: dismissed.requestId,
      });
      const dismissedProjection = yield* orchestrator.getThreadProjection(dismissed.threadId);
      assert.equal(dismissedProjection.runtimeRequests[0]?.status, "resolved");
      assert.equal(dismissedProjection.runtimeRequests[0]?.decision, "cancel");
      assert.equal(
        dismissedProjection.nodes.find((node) => node.id === dismissed.nodeId)?.status,
        "cancelled",
      );
      assert.equal(
        dismissedProjection.turnItems.find((item) => item.id === dismissed.itemId)?.status,
        "cancelled",
      );
      assert.lengthOf(dismissedProjection.messages, 0);

      const settled = yield* seedQuestion("runtime-settle-question");
      yield* orchestrator.dispatch({
        type: "thread.settle",
        commandId: CommandId.make("runtime-settle-question-command"),
        threadId: settled.threadId,
      });
      const settledProjection = yield* orchestrator.getThreadProjection(settled.threadId);
      assert.equal(settledProjection.thread.settledOverride, "settled");
      assert.equal(settledProjection.runtimeRequests[0]?.status, "resolved");
      assert.equal(settledProjection.runtimeRequests[0]?.decision, "cancel");
      assert.equal(
        settledProjection.nodes.find((node) => node.id === settled.nodeId)?.status,
        "cancelled",
      );
      assert.equal(
        settledProjection.turnItems.find((item) => item.id === settled.itemId)?.status,
        "cancelled",
      );
    }),
  );

  it.effect("merges an explicit provider-finished run while checkpoint capture is pending", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const eventSink = yield* EventSink.EventSinkV2;
      const now = yield* DateTime.now;
      const projectId = ProjectId.make("runtime-layer-waiting-merge-project");
      const targetThreadId = ThreadId.make("runtime-layer-waiting-merge-target");
      const sourceThreadId = ThreadId.make("runtime-layer-waiting-merge-source");
      const baseRunId = RunId.make("runtime-layer-waiting-merge-base-run");
      const sourceRunId = RunId.make("runtime-layer-waiting-merge-source-run");
      const sourceProviderThreadId = ProviderThreadId.make(
        "runtime-layer-waiting-merge-provider-thread",
      );
      const forkTransferId = ContextTransferId.make("runtime-layer-waiting-merge-fork-transfer");

      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-waiting-merge-create-target"),
        threadId: targetThreadId,
        projectId,
        title: "Waiting merge target",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
      });
      const target = yield* orchestrator.getThreadProjection(targetThreadId);

      yield* eventSink.write({
        commandId: CommandId.make("runtime-layer-waiting-merge-seed"),
        events: [
          {
            id: EventId.make("runtime-layer-waiting-merge-source-thread-event"),
            type: "thread.created",
            threadId: sourceThreadId,
            providerInstanceId: modelSelection.instanceId,
            occurredAt: now,
            payload: {
              ...target.thread,
              id: sourceThreadId,
              title: "Waiting merge source",
              activeProviderThreadId: null,
              lineage: {
                parentThreadId: targetThreadId,
                relationshipToParent: "fork",
                rootThreadId: targetThreadId,
              },
              forkedFrom: {
                type: "run",
                threadId: targetThreadId,
                runId: baseRunId,
              },
              createdAt: now,
              updatedAt: now,
            },
          },
          {
            id: EventId.make("runtime-layer-waiting-merge-fork-transfer-event"),
            type: "context-transfer.created",
            threadId: sourceThreadId,
            providerInstanceId: modelSelection.instanceId,
            occurredAt: now,
            payload: {
              id: forkTransferId,
              type: "fork",
              sourceThreadId: targetThreadId,
              targetThreadId: sourceThreadId,
              sourcePoint: { threadId: targetThreadId, runId: baseRunId },
              basePoint: null,
              sourceProviderInstanceId: modelSelection.instanceId,
              targetProviderInstanceId: modelSelection.instanceId,
              targetRunId: null,
              status: "consumed",
              resolution: null,
              createdBy: "user",
              error: null,
              createdAt: now,
              updatedAt: now,
              consumedAt: now,
            },
          },
          {
            id: EventId.make("runtime-layer-waiting-merge-provider-thread-event"),
            type: "provider-thread.updated",
            threadId: sourceThreadId,
            driver,
            providerInstanceId: modelSelection.instanceId,
            occurredAt: now,
            payload: {
              id: sourceProviderThreadId,
              driver,
              providerInstanceId: modelSelection.instanceId,
              providerSessionId: null,
              appThreadId: sourceThreadId,
              ownerNodeId: null,
              nativeThreadRef: {
                driver,
                nativeId: "native-waiting-merge-source",
                strength: "strong",
              },
              nativeConversationHeadRef: null,
              status: "idle",
              firstRunOrdinal: 1,
              lastRunOrdinal: 1,
              handoffIds: [],
              forkedFrom: null,
              createdAt: now,
              updatedAt: now,
            },
          },
          {
            id: EventId.make("runtime-layer-waiting-merge-source-run-event"),
            type: "run.created",
            threadId: sourceThreadId,
            runId: sourceRunId,
            providerInstanceId: modelSelection.instanceId,
            occurredAt: now,
            payload: {
              id: sourceRunId,
              threadId: sourceThreadId,
              ordinal: 1,
              providerInstanceId: modelSelection.instanceId,
              modelSelection,
              providerThreadId: sourceProviderThreadId,
              userMessageId: MessageId.make("runtime-layer-waiting-merge-message"),
              rootNodeId: null,
              activeAttemptId: null,
              status: "waiting",
              queuePosition: null,
              requestedAt: now,
              startedAt: now,
              completedAt: null,
              checkpointId: null,
              contextHandoffId: null,
            },
          },
        ],
      });

      yield* orchestrator.dispatch({
        type: "thread.merge_back",
        createdBy: "user",
        creationSource: "mobile",
        commandId: CommandId.make("runtime-layer-waiting-merge"),
        sourceThreadId,
        targetThreadId,
        sourcePoint: { type: "run", runId: sourceRunId },
        createdAt: now,
      });

      const mergedTarget = yield* orchestrator.getThreadProjection(targetThreadId);
      const transfer = mergedTarget.contextTransfers.find(
        (candidate) => candidate.type === "merge_back",
      );
      assert.isDefined(transfer);
      assert.equal(transfer.status, "pending");
      assert.equal(transfer.sourceThreadId, sourceThreadId);
      assert.equal(transfer.targetThreadId, targetThreadId);
      assert.equal(transfer.sourcePoint.runId, sourceRunId);
      assert.isUndefined(transfer.sourcePoint.checkpointId);
      assert.equal(transfer.sourcePoint.providerThreadRef?.nativeId, "native-waiting-merge-source");
      assert.equal(transfer.basePoint?.runId, baseRunId);
      assert.isNull(transfer.error);
    }),
  );
});

it.layer(LegacyImportTestLayer)("OrchestrationV2 legacy import", (it) => {
  it.effect("hydrates imported transcripts before commands and propagates hydration failures", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
      const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threadManagement = yield* ThreadManagementService.ThreadManagementService;
      const metadataThreadId = ThreadId.make("runtime-layer-legacy-metadata-thread");
      const failureThreadId = ThreadId.make("runtime-layer-legacy-failure-thread");
      const projectId = ProjectId.make("runtime-layer-legacy-project");

      yield* sql`
        INSERT INTO projection_projects (
          project_id,
          title,
          workspace_root,
          default_model_selection_json,
          scripts_json,
          created_at,
          updated_at,
          deleted_at
        ) VALUES (
          ${projectId},
          'Legacy project',
          '/tmp/runtime-layer-legacy-project',
          '{"instanceId":"codex","model":"gpt-5.4"}',
          '[]',
          '2026-01-01T00:00:00.000Z',
          '2026-01-04T00:00:00.000Z',
          NULL
        )
      `;
      yield* sql`
        INSERT INTO projection_threads (
          thread_id,
          project_id,
          title,
          model_selection_json,
          runtime_mode,
          interaction_mode,
          branch,
          worktree_path,
          latest_turn_id,
          created_at,
          updated_at,
          archived_at,
          settled_override,
          settled_at,
          deleted_at
        ) VALUES
          (
            ${metadataThreadId},
            ${projectId},
            'Legacy metadata title',
            '{"instanceId":"codex","model":"gpt-5.4"}',
            'full-access',
            'default',
            'main',
            '/tmp/runtime-layer-legacy-project',
            NULL,
            '2026-01-01T00:00:00.000Z',
            '2026-01-04T00:00:00.000Z',
            NULL,
            NULL,
            NULL,
            NULL
          ),
          (
            ${failureThreadId},
            ${projectId},
            'Legacy failure title',
            '{"instanceId":"codex","model":"gpt-5.4"}',
            'full-access',
            'default',
            'main',
            '/tmp/runtime-layer-legacy-project',
            NULL,
            '2026-01-01T00:00:00.000Z',
            '2026-01-04T00:00:00.000Z',
            NULL,
            NULL,
            NULL,
            NULL
          )
      `;
      yield* sql`
        INSERT INTO projection_thread_messages (
          message_id,
          thread_id,
          turn_id,
          role,
          text,
          attachments_json,
          is_streaming,
          created_at,
          updated_at
        ) VALUES
          (
            'message:runtime-layer-legacy:1',
            ${metadataThreadId},
            NULL,
            'user',
            'First imported question',
            '[]',
            0,
            '2026-01-01T01:00:00.000Z',
            '2026-01-01T01:00:00.000Z'
          ),
          (
            'message:runtime-layer-legacy:2',
            ${metadataThreadId},
            NULL,
            'assistant',
            'First imported answer',
            '[]',
            0,
            '2026-01-02T01:00:00.000Z',
            '2026-01-02T01:00:00.000Z'
          ),
          (
            'message:runtime-layer-legacy:3',
            ${metadataThreadId},
            NULL,
            'user',
            'Latest imported question',
            '[]',
            0,
            '2026-01-03T01:00:00.000Z',
            '2026-01-03T01:00:00.000Z'
          ),
          (
            'message:runtime-layer-legacy:failure',
            ${failureThreadId},
            NULL,
            'user',
            'Imported context must load before archive',
            '[]',
            0,
            '2026-01-03T01:00:00.000Z',
            '2026-01-03T01:00:00.000Z'
          )
      `;

      yield* importer.reconcileShells;
      assert.isTrue((yield* maintenance.verify).valid);

      yield* threadManagement.dispatch({
        type: "thread.metadata.update",
        commandId: CommandId.make("runtime-layer-legacy-metadata-update"),
        threadId: metadataThreadId,
        title: "Updated after import",
      });
      const updatedProjection = yield* threadManagement.getThreadProjection(metadataThreadId);
      assert.equal(updatedProjection.thread.title, "Updated after import");
      assert.deepEqual(
        updatedProjection.messages.map((message) => message.text),
        ["First imported question", "First imported answer", "Latest imported question"],
      );

      yield* sql`
        ALTER TABLE projection_thread_messages
        RENAME TO projection_thread_messages_unavailable
      `;
      const { projectionFailure, hydrationFailure } = yield* Effect.all({
        projectionFailure: threadManagement.getThreadProjection(failureThreadId).pipe(Effect.flip),
        hydrationFailure: threadManagement
          .dispatch({
            type: "thread.archive",
            commandId: CommandId.make("runtime-layer-legacy-failed-archive"),
            threadId: failureThreadId,
          })
          .pipe(Effect.flip),
      }).pipe(
        Effect.ensuring(
          sql`
            ALTER TABLE projection_thread_messages_unavailable
            RENAME TO projection_thread_messages
          `.pipe(Effect.orDie),
        ),
      );
      assert.instanceOf(projectionFailure, Orchestrator.OrchestratorProjectionError);
      assert.instanceOf(projectionFailure.cause, LegacyV1ThreadImporter.LegacyV1ThreadImportError);
      assert.instanceOf(hydrationFailure, Orchestrator.OrchestratorDispatchError);
      assert.instanceOf(hydrationFailure.cause, LegacyV1ThreadImporter.LegacyV1ThreadImportError);

      const projectionAfterFailure = yield* orchestrator.getThreadProjection(failureThreadId);
      assert.isNull(projectionAfterFailure.thread.archivedAt);

      yield* threadManagement.dispatch({
        type: "thread.archive",
        commandId: CommandId.make("runtime-layer-legacy-retried-archive"),
        threadId: failureThreadId,
      });
      const projectionAfterRetry = yield* threadManagement.getThreadProjection(failureThreadId);
      assert.isNotNull(projectionAfterRetry.thread.archivedAt);
    }),
  );
});

it.layer(TestLayer)("OrchestrationV2LayerLive lifecycle", (it) => {
  it.effect("applies lifecycle commands idempotently and emits archive/removal shell deltas", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threadId = ThreadId.make("runtime-layer-lifecycle-thread");
      const projectId = ProjectId.make("runtime-layer-lifecycle-project");
      const project = {
        projectId,
        title: "Lifecycle project",
        workspaceRoot: "/workspace/project",
        defaultModelSelection: null,
        createdAt: "2026-09-07T00:00:00.000Z",
      } as const;
      yield* seedProject(project);
      const create = {
        type: "thread.create" as const,
        createdBy: "user" as const,
        creationSource: "web" as const,
        commandId: CommandId.make("runtime-layer-lifecycle-create"),
        threadId,
        projectId,
        title: "Lifecycle thread",
        modelSelection,
        runtimeMode: "full-access" as const,
        interactionMode: "default" as const,
        branch: null,
        worktreePath: null,
      };

      const firstCreate = yield* orchestrator.dispatch(create);
      const retriedCreate = yield* orchestrator.dispatch(create);
      assert.equal(retriedCreate.sequence, firstCreate.sequence);
      assert.lengthOf(retriedCreate.storedEvents, 1);

      yield* orchestrator.dispatch({
        type: "thread.metadata.update",
        commandId: CommandId.make("runtime-layer-lifecycle-metadata"),
        threadId,
        title: "Renamed lifecycle thread",
        branch: "feature/v2",
        worktreePath: "/tmp/t3-v2-worktree",
      });
      const staleWorkspaceUpdate = yield* orchestrator
        .dispatch({
          type: "thread.metadata.update",
          commandId: CommandId.make("runtime-layer-lifecycle-stale-workspace"),
          threadId,
          branch: "feature/stale",
          worktreePath: "/tmp/stale-worktree",
          expectedWorktreePath: null,
        })
        .pipe(Effect.flip);
      assert.instanceOf(staleWorkspaceUpdate, Orchestrator.OrchestratorDispatchError);
      const projectionAfterStaleWorkspaceUpdate = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(projectionAfterStaleWorkspaceUpdate.thread.branch, "feature/v2");
      assert.equal(projectionAfterStaleWorkspaceUpdate.thread.worktreePath, "/tmp/t3-v2-worktree");
      const pullRequestSnapshot = yield* orchestrator.getShellSnapshot();
      const pullRequest = {
        projectId,
        repository: "owner/repository",
        number: 24,
        url: "https://github.com/owner/repository/pull/24",
      };
      yield* moveProject(projectId, "/workspace/moved", "2026-09-07T00:01:00.000Z");
      const staleProjectWorkspace = yield* orchestrator
        .dispatch({
          type: "thread.pull-request.sync",
          commandId: CommandId.make("runtime-layer-lifecycle-pr-sync-stale-project-workspace"),
          threadId,
          projectId,
          snapshotSequence: pullRequestSnapshot.snapshotSequence,
          expected: {
            workspaceRoot: "/workspace/project",
            branch: "feature/v2",
            worktreePath: "/tmp/t3-v2-worktree",
            linkedPullRequest: null,
            branchPullRequest: null,
          },
          branchPullRequest: pullRequest,
        })
        .pipe(Effect.flip);
      assert.instanceOf(staleProjectWorkspace, Orchestrator.OrchestratorDispatchError);
      yield* moveProject(projectId, project.workspaceRoot, "2026-09-07T00:02:00.000Z");
      yield* orchestrator.dispatch({
        type: "thread.pull-request.sync",
        commandId: CommandId.make("runtime-layer-lifecycle-pr-sync"),
        threadId,
        projectId,
        snapshotSequence: pullRequestSnapshot.snapshotSequence,
        expected: {
          workspaceRoot: "/workspace/project",
          branch: "feature/v2",
          worktreePath: "/tmp/t3-v2-worktree",
          linkedPullRequest: null,
          branchPullRequest: null,
        },
        branchPullRequest: pullRequest,
      });
      assert.deepEqual(
        (yield* orchestrator.getThreadProjection(threadId)).thread.branchPullRequest,
        pullRequest,
      );
      const stalePullRequestSync = yield* orchestrator
        .dispatch({
          type: "thread.pull-request.sync",
          commandId: CommandId.make("runtime-layer-lifecycle-pr-sync-stale"),
          threadId,
          projectId,
          snapshotSequence: pullRequestSnapshot.snapshotSequence,
          expected: {
            workspaceRoot: "/workspace/project",
            branch: "feature/v2",
            worktreePath: "/tmp/t3-v2-worktree",
            linkedPullRequest: null,
            branchPullRequest: null,
          },
          branchPullRequest: null,
        })
        .pipe(Effect.flip);
      assert.instanceOf(stalePullRequestSync, Orchestrator.OrchestratorDispatchError);
      yield* orchestrator.dispatch({
        type: "thread.runtime-mode.set",
        commandId: CommandId.make("runtime-layer-lifecycle-runtime"),
        threadId,
        runtimeMode: "approval-required",
      });
      yield* orchestrator.dispatch({
        type: "thread.interaction-mode.set",
        commandId: CommandId.make("runtime-layer-lifecycle-interaction"),
        threadId,
        interactionMode: "plan",
      });
      yield* orchestrator.dispatch({
        type: "thread.model-selection.set",
        commandId: CommandId.make("runtime-layer-lifecycle-model"),
        threadId,
        modelSelection: { ...modelSelection, model: "gpt-5.5" },
      });
      yield* orchestrator.dispatch({
        type: "thread.active.reorder",
        commandId: CommandId.make("runtime-layer-lifecycle-active-order"),
        threadId,
        orderKey: "a0",
      });
      assert.equal((yield* orchestrator.getThreadProjection(threadId)).thread.activeOrderKey, "a0");

      // Automatic settlement (#8600): a stale snapshot loses to any change
      // made after it, and a fresh one settles like a user settle would.
      const preAutoProjection = yield* orchestrator.getThreadProjection(threadId);
      const staleAutoSettle = yield* orchestrator
        .dispatch({
          type: "thread.auto-settle",
          commandId: CommandId.make("runtime-layer-lifecycle-auto-settle-stale"),
          threadId,
          snapshotAt: DateTime.makeUnsafe(
            DateTime.toEpochMillis(preAutoProjection.thread.updatedAt) - 1,
          ),
        })
        .pipe(Effect.flip);
      assert.instanceOf(staleAutoSettle, Orchestrator.OrchestratorDispatchError);
      yield* orchestrator.dispatch({
        type: "thread.auto-settle",
        commandId: CommandId.make("runtime-layer-lifecycle-auto-settle"),
        threadId,
        snapshotAt: preAutoProjection.thread.updatedAt,
      });
      const autoSettledProjection = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(autoSettledProjection.thread.settledOverride, "settled");
      assert.isNull(autoSettledProjection.thread.activeOrderKey);
      yield* orchestrator.dispatch({
        type: "thread.unsettle",
        commandId: CommandId.make("runtime-layer-lifecycle-auto-unsettle"),
        threadId,
        reason: "user",
      });
      // An explicit un-settle outranks the sweep even with a fresh snapshot.
      const postUnsettleProjection = yield* orchestrator.getThreadProjection(threadId);
      const overriddenAutoSettle = yield* orchestrator
        .dispatch({
          type: "thread.auto-settle",
          commandId: CommandId.make("runtime-layer-lifecycle-auto-settle-overridden"),
          threadId,
          snapshotAt: postUnsettleProjection.thread.updatedAt,
        })
        .pipe(Effect.flip);
      assert.instanceOf(overriddenAutoSettle, Orchestrator.OrchestratorDispatchError);

      yield* orchestrator.dispatch({
        type: "thread.settle",
        commandId: CommandId.make("runtime-layer-lifecycle-settle"),
        threadId,
      });
      const settledProjection = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(settledProjection.thread.settledOverride, "settled");
      assert.isNotNull(settledProjection.thread.settledAt);

      yield* orchestrator.dispatch({
        type: "thread.unsettle",
        commandId: CommandId.make("runtime-layer-lifecycle-unsettle"),
        threadId,
        reason: "user",
      });
      const activeProjection = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(activeProjection.thread.settledOverride, "active");
      assert.isNull(activeProjection.thread.settledAt);
      assert.isNotNull(activeProjection.thread.unsettledAt);
      const activeShell = (yield* orchestrator.getShellSnapshot()).threads.find(
        (thread) => thread.id === threadId,
      );
      assert.deepEqual(activeShell?.unsettledAt, activeProjection.thread.unsettledAt);

      const archive = yield* orchestrator.dispatch({
        type: "thread.archive",
        commandId: CommandId.make("runtime-layer-lifecycle-archive"),
        threadId,
      });
      const archivedShell = yield* orchestrator.getShellSnapshot();
      assert.notInclude(
        archivedShell.threads.map((thread) => thread.id),
        threadId,
      );
      assert.include(
        archivedShell.archivedThreads.map((thread) => thread.id),
        threadId,
      );
      const activeOnlyShell = yield* orchestrator.getShellSnapshot({ location: "active" });
      assert.notInclude(
        activeOnlyShell.threads.map((thread) => thread.id),
        threadId,
      );
      assert.lengthOf(activeOnlyShell.archivedThreads, 0);
      const archiveOnlyShell = yield* orchestrator.getShellSnapshot({ location: "archive" });
      assert.lengthOf(archiveOnlyShell.threads, 0);
      assert.include(
        archiveOnlyShell.archivedThreads.map((thread) => thread.id),
        threadId,
      );
      assert.deepEqual(
        shellStreamItemFromThreadShell({
          stored: archive.storedEvents[0]!,
          shell: yield* orchestrator.getThreadShell(threadId),
        }),
        {
          kind: "thread.removed",
          sequence: archive.sequence,
          location: "active",
          threadId,
        },
      );

      const remove = yield* orchestrator.dispatch({
        type: "thread.delete",
        commandId: CommandId.make("runtime-layer-lifecycle-delete"),
        threadId,
      });
      const deletedShell = yield* orchestrator.getShellSnapshot();
      assert.notInclude(
        deletedShell.threads.map((thread) => thread.id),
        threadId,
      );
      assert.notInclude(
        deletedShell.archivedThreads.map((thread) => thread.id),
        threadId,
      );
      assert.deepEqual(
        shellStreamItemFromThreadShell({
          stored: remove.storedEvents[0]!,
          shell: yield* orchestrator.getThreadShell(threadId),
        }),
        {
          kind: "thread.removed",
          sequence: remove.sequence,
          location: "active",
          threadId,
        },
      );

      const projection = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(projection.thread.title, "Renamed lifecycle thread");
      assert.equal(projection.thread.branch, "feature/v2");
      assert.equal(projection.thread.worktreePath, "/tmp/t3-v2-worktree");
      assert.equal(projection.thread.runtimeMode, "approval-required");
      assert.equal(projection.thread.interactionMode, "plan");
      assert.equal(projection.thread.modelSelection.model, "gpt-5.5");
      assert.isNotNull(projection.thread.archivedAt);
      assert.isNotNull(projection.thread.deletedAt);
    }),
  );

  it.effect("persists linked pull requests through projection rebuilds and unlinking", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
      const threadId = ThreadId.make("runtime-layer-linked-pull-request-thread");
      const linkedPullRequest = {
        projectId: ProjectId.make("runtime-layer-linked-pull-request-project"),
        repository: "pingdotgg/t3code",
        number: 8160,
        url: "https://github.com/pingdotgg/t3code/pull/8160",
      } as const;

      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-linked-pull-request-create"),
        threadId,
        projectId: linkedPullRequest.projectId,
        title: "Linked pull request thread",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
      });
      yield* orchestrator.dispatch({
        type: "thread.metadata.update",
        commandId: CommandId.make("runtime-layer-linked-pull-request-link"),
        threadId,
        linkedPullRequest,
      });

      assert.deepEqual(
        (yield* orchestrator.getThreadProjection(threadId)).thread.linkedPullRequest,
        linkedPullRequest,
      );
      const linkedShell = yield* orchestrator.getThreadShell(threadId);
      assert.isNotNull(linkedShell);
      assert.deepEqual(linkedShell.linkedPullRequest, linkedPullRequest);

      const rebuilt = yield* maintenance.rebuild;
      assert.isTrue(rebuilt.valid);
      assert.deepEqual(
        (yield* orchestrator.getThreadProjection(threadId)).thread.linkedPullRequest,
        linkedPullRequest,
      );

      yield* orchestrator.dispatch({
        type: "thread.metadata.update",
        commandId: CommandId.make("runtime-layer-linked-pull-request-unlink"),
        threadId,
        linkedPullRequest: null,
      });
      assert.isNull((yield* orchestrator.getThreadProjection(threadId)).thread.linkedPullRequest);
      const unlinkedShell = yield* orchestrator.getThreadShell(threadId);
      assert.isNotNull(unlinkedShell);
      assert.isNull(unlinkedShell.linkedPullRequest);
    }),
  );

  it.effect("keeps the branch pull request when linking another pull request", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
      const threadId = ThreadId.make("branch-pr-link");
      const projectId = ProjectId.make("branch-pr-project");
      yield* seedProject({
        projectId,
        title: "PR links",
        workspaceRoot: "/workspace/pr-links",
        defaultModelSelection: null,
        createdAt: "2026-09-17T00:00:00.000Z",
      });
      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("branch-pr-create"),
        threadId,
        projectId,
        title: "PR links",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: "feature/pr-links",
        worktreePath: null,
      });
      const snapshot = yield* orchestrator.getShellSnapshot();
      yield* orchestrator.dispatch({
        type: "thread.pull-request.sync",
        commandId: CommandId.make("branch-pr-discover"),
        threadId,
        projectId,
        snapshotSequence: snapshot.snapshotSequence,
        expected: {
          workspaceRoot: "/workspace/pr-links",
          branch: "feature/pr-links",
          worktreePath: null,
          linkedPullRequest: null,
          branchPullRequest: null,
        },
        branchPullRequest: {
          projectId,
          repository: "pingdotgg/t3code",
          number: 1,
          url: "https://github.com/pingdotgg/t3code/pull/1",
        },
      });
      for (const [index, number] of [2, 2, 1, 3].entries()) {
        yield* orchestrator.dispatch({
          type: "thread.pull-request.link",
          commandId: CommandId.make(`branch-pr-link-${index}`),
          threadId,
          host: "GitHub.com",
          repository: "Pingdotgg/T3code",
          number,
          url: `https://github.com/pingdotgg/t3code/pull/${number}`,
          source: "manual",
        });
        assert.deepEqual(
          (yield* orchestrator.getThreadShell(threadId))?.pullRequests?.map((link) => link.number),
          number === 3 ? [1, 2, 3] : [1, 2],
        );
      }
      yield* orchestrator.dispatch({
        type: "thread.pull-request.unlink",
        commandId: CommandId.make("branch-pr-unlink"),
        threadId,
        host: "github.com",
        repository: "pingdotgg/t3code",
        number: 1,
      });
      yield* orchestrator.dispatch({
        type: "thread.pull-request.link",
        commandId: CommandId.make("branch-pr-link-after-unlink"),
        threadId,
        host: "github.com",
        repository: "pingdotgg/t3code",
        number: 4,
        url: "https://github.com/pingdotgg/t3code/pull/4",
        source: "manual",
      });
      assert.isTrue((yield* maintenance.rebuild).valid);
      assert.deepEqual(
        (yield* orchestrator.getThreadShell(threadId))?.pullRequests?.map((link) => link.number),
        [2, 3, 4],
      );
    }),
  );

  it.effect("retains multiple pull requests and dismissed stack members through rebuilds", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
      const threadId = ThreadId.make("runtime-multiple-pull-requests");
      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("multi-pr-create"),
        threadId,
        projectId: ProjectId.make("multi-pr-project"),
        title: "Stack",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
      });
      const key = { host: "GitHub.com", repository: "Pingdotgg/T3code" };
      for (const number of [1, 2]) {
        yield* orchestrator.dispatch({
          type: "thread.pull-request.link",
          commandId: CommandId.make(`multi-pr-link-${number}`),
          threadId,
          ...key,
          number,
          url: `https://github.com/pingdotgg/t3code/pull/${number}`,
          source: number === 1 ? "manual" : "stack",
        });
      }
      const linked = yield* orchestrator.getThreadShell(threadId);
      assert.deepEqual(
        linked?.pullRequests?.map(({ host, repository, number }) => ({ host, repository, number })),
        [1, 2].map((number) => ({ host: "github.com", repository: "pingdotgg/t3code", number })),
      );
      yield* orchestrator.dispatch({
        type: "thread.pull-request.unlink",
        commandId: CommandId.make("multi-pr-dismiss"),
        threadId,
        ...key,
        number: 2,
      });
      yield* orchestrator.dispatch({
        type: "thread.pull-request.unlink",
        commandId: CommandId.make("multi-pr-unlink"),
        threadId,
        ...key,
        number: 1,
      });
      assert.deepEqual(
        (yield* orchestrator.getThreadShell(threadId))?.pullRequests?.map(({ number, source }) => ({
          number,
          source,
        })),
        [{ number: 2, source: "stack-dismissed" }],
      );
      yield* orchestrator.dispatch({
        type: "thread.pull-request.link",
        commandId: CommandId.make("multi-pr-rediscover"),
        threadId,
        ...key,
        number: 2,
        url: "https://github.com/pingdotgg/t3code/pull/2",
        source: "stack",
      });
      assert.equal(
        (yield* orchestrator.getThreadShell(threadId))?.pullRequests?.[0]?.source,
        "stack-dismissed",
      );
      assert.isTrue((yield* maintenance.rebuild).valid);
      assert.equal(
        (yield* orchestrator.getThreadShell(threadId))?.pullRequests?.[0]?.source,
        "stack-dismissed",
      );
      yield* orchestrator.dispatch({
        type: "thread.pull-request.link",
        commandId: CommandId.make("multi-pr-restore"),
        threadId,
        ...key,
        number: 2,
        url: "https://github.com/pingdotgg/t3code/pull/2",
        source: "manual",
      });
      assert.equal(
        (yield* orchestrator.getThreadShell(threadId))?.pullRequests?.[0]?.source,
        "manual",
      );
    }),
  );

  it.effect("starts, records, and stops a pull request watch", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
      const threadId = ThreadId.make("runtime-pull-request-watch");
      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("pr-watch-create"),
        threadId,
        projectId: ProjectId.make("pr-watch-project"),
        title: "Watch",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
      });
      const key = { host: "github.com", repository: "pingdotgg/t3code", number: 7 };
      const url = "https://github.com/pingdotgg/t3code/pull/7";
      const watchOf = Effect.map(
        orchestrator.getThreadShell(threadId),
        (thread) => thread?.pullRequests?.[0]?.watch,
      );

      // Watching an unlinked pull request links it in the same command.
      yield* orchestrator.dispatch({
        type: "thread.pull-request.watch",
        commandId: CommandId.make("pr-watch-start"),
        threadId,
        ...key,
        watching: true,
        link: { url, source: "agent" },
      });
      assert.equal(
        (yield* orchestrator.getThreadShell(threadId))?.pullRequests?.[0]?.source,
        "agent",
      );
      const started = yield* watchOf;
      assert.isDefined(started);
      if (started === undefined) return;

      // A legacy client re-linking the same pull request keeps its watch.
      yield* orchestrator.dispatch({
        type: "thread.metadata.update",
        commandId: CommandId.make("pr-watch-legacy-relink"),
        threadId,
        linkedPullRequest: { projectId: ProjectId.make("pr-watch-project"), ...key, url },
      });
      assert.deepEqual(yield* watchOf, started);

      const recorded = { ...started, headSha: "abc123", failedChecks: ["lint"], wakes: 1 };
      yield* orchestrator.dispatch({
        type: "thread.pull-request-watch.sync",
        commandId: CommandId.make("pr-watch-record"),
        threadId,
        ...key,
        startedAt: started.startedAt,
        watch: recorded,
      });
      assert.deepEqual(yield* watchOf, recorded);
      assert.isTrue((yield* maintenance.rebuild).valid);
      assert.deepEqual(yield* watchOf, recorded);

      yield* orchestrator.dispatch({
        type: "thread.pull-request.watch",
        commandId: CommandId.make("pr-watch-stop"),
        threadId,
        ...key,
        watching: false,
      });
      // A wake read before the stop must neither wake the agent nor bring the watch back.
      const late = yield* orchestrator
        .dispatch({
          type: "thread.pull-request-watch.sync",
          commandId: CommandId.make("pr-watch-late-record"),
          threadId,
          ...key,
          startedAt: started.startedAt,
          watch: { ...recorded, wakes: 2 },
          wake: {
            messageId: MessageId.make("pr-watch-late-wake"),
            text: "Update",
            notification: { source: { kind: "monitor" }, outcome: "updated", summary: "#7" },
          },
        })
        .pipe(Effect.flip);
      assert.equal(late._tag, "OrchestratorDispatchError");
      assert.isUndefined(yield* watchOf);
      const { messages } = yield* orchestrator.getThreadRecords(threadId, ["messages"]);
      assert.deepEqual(messages, []);
    }),
  );

  it.effect("ends a watch it cannot read, and tells the agent", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threadId = ThreadId.make("runtime-pull-request-watch-unreadable");
      const projectId = ProjectId.make("pr-watch-unreadable-project");
      yield* seedProject({
        projectId,
        title: "Watch unreadable",
        workspaceRoot: "/workspace/watch-unreadable",
        defaultModelSelection: null,
        createdAt: "2026-10-01T00:00:00.000Z",
      });
      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("pr-watch-unreadable-create"),
        threadId,
        projectId,
        title: "Watch unreadable",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
      });
      yield* orchestrator.dispatch({
        type: "thread.pull-request.watch",
        commandId: CommandId.make("pr-watch-unreadable-start"),
        threadId,
        host: "github.com",
        repository: "pingdotgg/t3code",
        number: 8,
        watching: true,
        link: { url: "https://github.com/pingdotgg/t3code/pull/8", source: "agent" },
      });
      const reactor = yield* PullRequestWatchReactor.make.pipe(
        Effect.provide(
          Layer.mergeAll(
            NodeServices.layer,
            Layer.mock(PullRequestService.PullRequestService)({
              detail: () => Effect.die("host unreachable"),
              activity: () => Effect.die("host unreachable"),
            }),
          ),
        ),
      );
      for (let pass = 0; pass < 15; pass += 1) yield* reactor.sweep;

      const thread = yield* orchestrator.getThreadShell(threadId);
      assert.isUndefined(thread?.pullRequests?.[0]?.watch);
      const { messages } = yield* orchestrator.getThreadRecords(threadId, ["messages"]);
      assert.deepEqual(
        messages.flatMap((message) => message.notification?.summary ?? []),
        ["#8: stopped watching, could not read it"],
      );
    }),
  );

  it.effect.each([
    "single page",
    "paginated",
    "page failure",
    "repeated cursor",
    "missing comments",
    "thread list truncated",
  ])("wakes a watched thread once: %s", (mode) =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threadId = ThreadId.make(`runtime-pull-request-watch-wake-${mode}`);
      const projectId = ProjectId.make(`pr-watch-wake-project-${mode}`);
      yield* seedProject({
        projectId,
        title: "Watch wake",
        workspaceRoot: "/workspace/watch",
        defaultModelSelection: null,
        createdAt: "2026-10-01T00:00:00.000Z",
      });
      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make(`pr-watch-wake-create-${mode}`),
        threadId,
        projectId,
        title: "Watch wake",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
      });
      const key = { host: "github.com", repository: "pingdotgg/t3code", number: 7 };
      const url = "https://github.com/pingdotgg/t3code/pull/7";
      yield* orchestrator.dispatch({
        type: "thread.pull-request.link",
        commandId: CommandId.make(`pr-watch-wake-link-${mode}`),
        threadId,
        ...key,
        url,
        source: "agent",
      });
      yield* orchestrator.dispatch({
        type: "thread.pull-request.watch",
        commandId: CommandId.make(`pr-watch-wake-start-${mode}`),
        threadId,
        ...key,
        watching: true,
      });

      const at = "2026-10-02T12:00:00.000Z";
      const detail: PullRequestDetail = {
        provider: "github",
        capabilities: {
          diff: true,
          comment: true,
          actions: [],
          mergeMethods: [],
          search: false,
          review: { inlineComment: false, reply: false, resolve: false, verdicts: [] },
          reviewers: { request: false, listCandidates: false },
        },
        viewerPermissions: {
          actions: [],
          comment: true,
          resolve: true,
          verdicts: [],
          requestReviewers: false,
        },
        projectId,
        projectTitle: "Watch wake",
        workspaceRoot: "/workspace/watch",
        repository: key.repository,
        number: key.number,
        title: "Watched pull request",
        body: "",
        url,
        author: { login: "agent-user", name: null, avatarUrl: null },
        state: "open",
        isDraft: false,
        mergeability: "mergeable",
        additions: 1,
        deletions: 0,
        changedFiles: 1,
        headBranch: "feature",
        headSha: "abc1234def",
        baseBranch: "main",
        createdAt: at,
        updatedAt: at,
        mergedAt: null,
        closedAt: null,
        reviewers: [],
        labels: [],
        checks: [{ name: "lint", status: "failure", description: null, url: null }],
        mergeCapabilities: { merge: true, squash: true, rebase: true },
        viewer: "agent-user",
      };
      const initialWatch = (yield* orchestrator.getThreadShell(threadId))?.pullRequests?.[0]?.watch;
      assert.isDefined(initialWatch);
      const remark: PullRequestComment = {
        id: "review-1",
        kind: "review-comment",
        author: { login: "reviewer", name: null, avatarUrl: null },
        body: "One more thing.",
        createdAt: "2999-01-01T00:00:03.000Z",
        url: null,
        path: "src/index.ts",
        reviewState: null,
      };
      const firstTen = Array.from({ length: 10 }, (_, index) => ({
        ...remark,
        id: `old-${index}`,
        createdAt: "1900-01-01T00:00:00.000Z",
      }));
      const eleventh = {
        ...remark,
        id: "reply-11",
        body: "Eleventh reply.",
        createdAt: "2999-01-01T00:00:01.000Z",
      };
      const twelfth = {
        ...remark,
        id: "reply-12",
        body: "Twelfth reply.",
        createdAt: "2999-01-01T00:00:02.000Z",
      };
      const incomplete = mode !== "single page" && mode !== "paginated";
      let recovering = false;
      let pagesRead = 0;
      const reactor = yield* PullRequestWatchReactor.make.pipe(
        Effect.provide(
          Layer.mergeAll(
            NodeServices.layer,
            Layer.mock(PullRequestService.PullRequestService)({
              detail: () => Effect.succeed(detail),
              activity: () =>
                Effect.succeed({
                  comments:
                    mode === "single page"
                      ? [remark]
                      : [...firstTen, { ...remark, kind: "issue-comment" }],
                  commentCount: mode === "single page" ? 1 : 13,
                  commentsTruncated: mode !== "single page",
                  reviewThreadsTruncated: mode === "thread list truncated" && !recovering,
                  reviewThreads:
                    mode === "single page"
                      ? []
                      : [
                          {
                            id: "review-thread",
                            path: "src/index.ts",
                            line: 1,
                            side: "right",
                            isResolved: false,
                            isOutdated: false,
                            comments: firstTen,
                            commentCount: 12,
                            nextCommentsCursor: "after-10",
                          },
                        ],
                  commits: [],
                }),
              threadComments: (input) => {
                pagesRead += 1;
                assert.equal(input.threadId, "review-thread");
                if (input.cursor === "after-10") {
                  return Effect.succeed({ comments: [eleventh], nextCursor: "after-11" });
                }
                assert.equal(input.cursor, "after-11");
                if (!recovering && mode === "page failure") {
                  return Effect.fail(
                    new PullRequestOperationError({
                      operation: "threadComments",
                      detail: "Page unavailable",
                    }),
                  );
                }
                if (!recovering && mode === "repeated cursor") {
                  return Effect.succeed({ comments: [eleventh], nextCursor: "after-11" });
                }
                if (!recovering && mode === "missing comments") {
                  return Effect.succeed({ comments: [], nextCursor: null });
                }
                // Overlapping pages must not report the same reply twice.
                return Effect.succeed({ comments: [eleventh, twelfth], nextCursor: null });
              },
            }),
          ),
        ),
      );
      if (incomplete) {
        yield* reactor.sweep;
        const held = (yield* orchestrator.getThreadShell(threadId))?.pullRequests?.[0]?.watch;
        assert.equal(held?.remarksThrough, initialWatch?.remarksThrough);
        assert.deepEqual(held?.remarkIds, initialWatch?.remarkIds);
        assert.deepEqual(held?.failedChecks, ["lint"]);
        const { messages } = yield* orchestrator.getThreadRecords(threadId, ["messages"]);
        assert.deepEqual(
          messages.map((message) => message.notification?.summary),
          ["#7: checks failed"],
        );
        // Keep the two notifications ordered independently of their random message IDs.
        yield* TestClock.adjust("1 millis");
        recovering = true;
      }
      yield* reactor.sweep;
      // A thread whose count has not moved is not paged again.
      const pagesBefore = pagesRead;
      yield* reactor.sweep;
      assert.equal(pagesRead, pagesBefore);

      const { messages } = yield* orchestrator.getThreadRecords(threadId, ["messages"]);
      assert.deepEqual(
        messages.flatMap((message) =>
          message.notification === undefined ? [] : [message.notification.summary],
        ),
        incomplete
          ? ["#7: checks failed", "#7: new comments"]
          : ["#7: checks failed, new comments"],
      );
      if (mode !== "single page") {
        const wake = messages.at(-1);
        assert.include(wake?.text ?? "", "3 new comments");
        assert.include(wake?.text ?? "", "Eleventh reply.");
        assert.include(wake?.text ?? "", "Twelfth reply.");
      }
      const watch = (yield* orchestrator.getThreadShell(threadId))?.pullRequests?.[0]?.watch;
      assert.equal(watch?.remarksThrough, remark.createdAt);
      assert.deepEqual(watch?.remarkIds, [remark.id]);
      assert.deepEqual(
        { headSha: watch?.headSha, failedChecks: watch?.failedChecks, wakes: watch?.wakes },
        { headSha: "abc1234def", failedChecks: ["lint"], wakes: incomplete ? 1 : 0 },
      );
    }),
  );

  it.effect("persists rejected command receipts across retries", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const command = {
        type: "thread.archive" as const,
        commandId: CommandId.make("runtime-layer-rejected-command"),
        threadId: ThreadId.make("runtime-layer-missing-thread"),
      };

      const first = yield* orchestrator.dispatch(command).pipe(Effect.flip);
      const retry = yield* orchestrator.dispatch(command).pipe(Effect.flip);

      assert.equal(first._tag, "OrchestratorProjectionError");
      assert.equal(retry._tag, "OrchestratorCommandPreviouslyRejectedError");
    }),
  );

  it.effect(
    "admits restart continuations once and rejects a stale continuation behind newer work",
    () =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const eventSink = yield* EventSink.EventSinkV2;
        const threadId = ThreadId.make("runtime-layer-restart-continuation");
        yield* orchestrator.dispatch({
          type: "thread.create",
          createdBy: "user",
          creationSource: "web",
          commandId: CommandId.make("restart-create"),
          threadId,
          projectId: ProjectId.make("restart-project"),
          title: "Restart",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: "/tmp/runtime-layer-restart",
        });
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          createdBy: "user",
          creationSource: "web",
          commandId: CommandId.make("restart-user-message"),
          threadId,
          messageId: MessageId.make("restart-user-message"),
          text: "Original work",
          attachments: [],
          modelSelection,
          dispatchMode: { type: "start_immediately" },
        });
        const original = (yield* orchestrator.getThreadProjection(threadId)).runs[0]!;
        const now = yield* DateTime.now;
        yield* eventSink.commitCommand({
          commandId: CommandId.make("restart-cancel"),
          threadId,
          commandType: "provider-runtime.reconcile",
          acceptedAt: now,
          events: [
            {
              id: EventId.make("restart-cancel-event"),
              type: "run.updated",
              threadId,
              runId: original.id,
              occurredAt: now,
              payload: { ...original, status: "cancelled", completedAt: now },
            },
          ],
          effects: [],
        });
        const command = {
          type: "message.dispatch" as const,
          createdBy: "agent" as const,
          creationSource: "server" as const,
          commandId: CommandId.make("restart-automatic-message"),
          threadId,
          messageId: MessageId.make("restart-automatic-message"),
          text: "Continue where you left off.",
          attachments: [],
          modelSelection,
          dispatchMode: { type: "start_immediately" as const },
          restartContinuationOfRunId: original.id,
        };
        yield* orchestrator.dispatch(command);
        yield* orchestrator.dispatch(command);
        const admitted = yield* orchestrator.getThreadProjection(threadId);
        assert.lengthOf(admitted.runs, 2);
        assert.equal(admitted.runs[1]?.restartContinuationOfRunId, original.id);
        // A differently identified stale delivery still must not create another run.
        yield* orchestrator.dispatch({
          ...command,
          commandId: CommandId.make("restart-stale-race"),
          messageId: MessageId.make("restart-stale-race"),
        });
        const raced = yield* orchestrator.getThreadProjection(threadId);
        assert.lengthOf(raced.runs, 2);
        assert.isFalse(raced.messages.some((message) => message.id === "restart-stale-race"));
      }),
  );

  it.effect("does not admit a restart continuation of a failed run that lost background work", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const eventSink = yield* EventSink.EventSinkV2;
      const threadId = ThreadId.make("runtime-layer-restart-failed-source");
      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("restart-failed-create"),
        threadId,
        projectId: ProjectId.make("restart-project"),
        title: "Restart",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: "/tmp/runtime-layer-restart-failed",
      });
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("restart-failed-user-message"),
        threadId,
        messageId: MessageId.make("restart-failed-user-message"),
        text: "Original work",
        attachments: [],
        modelSelection,
        dispatchMode: { type: "start_immediately" },
      });
      const original = (yield* orchestrator.getThreadProjection(threadId)).runs[0]!;
      const now = yield* DateTime.now;
      yield* eventSink.commitCommand({
        commandId: CommandId.make("restart-failed-reconcile"),
        threadId,
        commandType: "provider-runtime.reconcile",
        acceptedAt: now,
        events: [
          {
            id: EventId.make("restart-failed-run"),
            type: "run.updated",
            threadId,
            runId: original.id,
            occurredAt: now,
            payload: { ...original, status: "failed", completedAt: now },
          },
          {
            id: EventId.make("restart-failed-work"),
            type: "run.background-work-cancelled",
            threadId,
            runId: original.id,
            occurredAt: now,
            payload: {
              runId: original.id,
              restartCancelledBackgroundWork: [{ kind: "shell", label: "sleep 25" }],
            },
          },
        ],
        effects: [],
      });
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        createdBy: "agent",
        creationSource: "server",
        commandId: CommandId.make("restart-failed-continuation"),
        threadId,
        messageId: MessageId.make("restart-failed-continuation"),
        text: "Note: the T3 server restarted.",
        attachments: [],
        modelSelection,
        dispatchMode: { type: "start_immediately" },
        restartContinuationOfRunId: original.id,
      });
      const projection = yield* orchestrator.getThreadProjection(threadId);
      assert.lengthOf(projection.runs, 1);
      assert.equal(projection.runs[0]?.status, "failed");
    }),
  );

  it.effect("rejects settling a thread while a run is active", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threadId = ThreadId.make("runtime-layer-active-settle-thread");

      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-active-settle-create"),
        threadId,
        projectId: ProjectId.make("runtime-layer-active-settle-project"),
        title: "Active settle",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: "/tmp/runtime-layer-active-settle",
      });
      yield* orchestrator.dispatch({
        type: "thread.settle",
        commandId: CommandId.make("runtime-layer-active-settle-initial"),
        threadId,
      });
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-active-settle-message"),
        threadId,
        messageId: MessageId.make("runtime-layer-active-settle-message"),
        text: "Keep this run active.",
        attachments: [],
        modelSelection,
        dispatchMode: { type: "start_immediately" },
      });

      const nonEmptyClaim = yield* orchestrator
        .dispatch({
          type: "thread.metadata.update",
          commandId: CommandId.make("runtime-layer-active-settle-empty-claim"),
          threadId,
          worktreePath: "/tmp/reassigned-after-message",
          expectedEmpty: true,
        })
        .pipe(Effect.flip);
      assert.instanceOf(nonEmptyClaim, Orchestrator.OrchestratorDispatchError);

      const error = yield* orchestrator
        .dispatch({
          type: "thread.settle",
          commandId: CommandId.make("runtime-layer-active-settle"),
          threadId,
        })
        .pipe(Effect.flip);

      assert.equal(error._tag, "OrchestratorDispatchError");
      const projection = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(projection.runs[0]?.status, "starting");
      assert.isNull(projection.thread.settledOverride);
      assert.isNull(projection.thread.settledAt);
      assert.isNotNull(projection.thread.unsettledAt);
    }),
  );

  it.effect("settles past held automatic runs but not held user messages", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const eventSink = yield* EventSink.EventSinkV2;
      const threadId = ThreadId.make("runtime-layer-settle-automatic-queued");

      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("settle-automatic-create"),
        threadId,
        projectId: ProjectId.make("settle-automatic-project"),
        title: "Settle automatic queued",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: "/tmp/runtime-layer-settle-automatic",
      });
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("settle-automatic-active"),
        threadId,
        messageId: MessageId.make("settle-automatic-active"),
        text: "Active",
        attachments: [],
        modelSelection,
        dispatchMode: { type: "start_immediately" },
      });
      const queueMessage = (id: string, automatic: boolean) =>
        orchestrator.dispatch({
          type: "message.dispatch",
          createdBy: automatic ? "agent" : "user",
          creationSource: automatic ? "provider" : "web",
          ...(automatic
            ? {
                notification: {
                  source: { kind: "background_task" as const },
                  outcome: "updated" as const,
                  summary: "Background activity updated",
                },
              }
            : {}),
          commandId: CommandId.make(id),
          threadId,
          messageId: MessageId.make(id),
          text: id,
          attachments: [],
          modelSelection,
          dispatchMode: { type: "queue_after_active" },
        });
      yield* queueMessage("settle-automatic-notification", true);

      // Simulate a restart: the active run ends and recovery holds the queue.
      const holdQueueAfterRestart = (commandId: string) =>
        Effect.gen(function* () {
          const projection = yield* orchestrator.getThreadProjection(threadId);
          const now = yield* DateTime.now;
          yield* eventSink.commitCommand({
            commandId: CommandId.make(commandId),
            threadId,
            commandType: "provider-runtime.reconcile",
            acceptedAt: now,
            events: projection.runs
              .filter((run) => run.status === "starting" || run.status === "queued")
              .map((run) => ({
                id: EventId.make(`${commandId}:${run.id}`),
                type: "run.updated" as const,
                threadId,
                runId: run.id,
                occurredAt: now,
                payload:
                  run.status === "queued"
                    ? { ...run, queueHeld: true }
                    : { ...run, status: "cancelled" as const, completedAt: now },
              })),
            effects: [],
          });
        });
      yield* holdQueueAfterRestart("settle-automatic-restart");

      yield* orchestrator.dispatch({
        type: "thread.settle",
        commandId: CommandId.make("settle-automatic-settle"),
        threadId,
      });
      const settled = yield* orchestrator.getThreadProjection(threadId);
      assert.isNotNull(settled.thread.settledAt);
      assert.isTrue(settled.runs.every((run) => run.status === "cancelled"));

      // A held message the user typed still blocks settling.
      yield* orchestrator.dispatch({
        type: "thread.unsettle",
        commandId: CommandId.make("settle-automatic-unsettle"),
        threadId,
        reason: "user",
      });
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("settle-automatic-active-2"),
        threadId,
        messageId: MessageId.make("settle-automatic-active-2"),
        text: "Active again",
        attachments: [],
        modelSelection,
        dispatchMode: { type: "start_immediately" },
      });
      yield* queueMessage("settle-automatic-user-queued", false);
      yield* holdQueueAfterRestart("settle-automatic-restart-2");
      const error = yield* orchestrator
        .dispatch({
          type: "thread.settle",
          commandId: CommandId.make("settle-automatic-settle-2"),
          threadId,
        })
        .pipe(Effect.flip);
      assert.equal(error._tag, "OrchestratorDispatchError");
    }),
  );

  it.effect("cancels queued work when a thread is archived", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threadId = ThreadId.make("runtime-layer-archive-queued-thread");

      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-archive-queued-create"),
        threadId,
        projectId: ProjectId.make("runtime-layer-archive-queued-project"),
        title: "Archive queued work",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: "/tmp/runtime-layer-archive-queued",
      });
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-archive-queued-active-message"),
        threadId,
        messageId: MessageId.make("runtime-layer-archive-queued-active-message"),
        text: "Keep the provider occupied.",
        attachments: [],
        modelSelection,
        dispatchMode: { type: "start_immediately" },
      });
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-archive-queued-next-message"),
        threadId,
        messageId: MessageId.make("runtime-layer-archive-queued-next-message"),
        text: "Do not run this after archive.",
        attachments: [],
        modelSelection,
        dispatchMode: { type: "queue_after_active" },
      });

      const beforeArchive = yield* orchestrator.getThreadProjection(threadId);
      const activeRun = beforeArchive.runs.find((run) => run.status === "starting");
      const queuedRun = beforeArchive.runs.find((run) => run.status === "queued");
      assert.isDefined(activeRun);
      assert.isDefined(queuedRun);

      yield* orchestrator.dispatch({
        type: "thread.archive",
        commandId: CommandId.make("runtime-layer-archive-queued-archive"),
        threadId,
      });

      const archived = yield* orchestrator.getThreadProjection(threadId);
      assert.isNotNull(archived.thread.archivedAt);
      assert.equal(archived.runs.find((run) => run.id === queuedRun.id)?.status, "cancelled");
      assert.equal(
        archived.attempts.find((attempt) => attempt.runId === queuedRun.id)?.status,
        "cancelled",
      );
      assert.equal(archived.nodes.find((node) => node.runId === queuedRun.id)?.status, "cancelled");
      assert.equal(yield* orchestrator.resumeQueuedRuns, 0);

      const promoteError = yield* orchestrator
        .dispatch({
          type: "queued-message.promote-to-steer",
          commandId: CommandId.make("runtime-layer-archive-queued-promote"),
          threadId,
          queuedRunId: queuedRun.id,
          targetRunId: activeRun.id,
        })
        .pipe(Effect.flip);
      assert.equal(promoteError._tag, "OrchestratorDispatchError");

      const afterPromotion = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(afterPromotion.runs.find((run) => run.id === queuedRun.id)?.status, "cancelled");
    }),
  );

  it.effect.each([false, true])(
    "promotes only one queued run after each terminal run (notification: %s)",
    (automatic) =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const eventSink = yield* EventSink.EventSinkV2;
        const threadId = ThreadId.make(`runtime-layer-serialized-queue-thread-${automatic}`);

        yield* orchestrator.dispatch({
          type: "thread.create",
          createdBy: "user",
          creationSource: "web",
          commandId: CommandId.make(`runtime-layer-serialized-queue-create-${automatic}`),
          threadId,
          projectId: ProjectId.make(`runtime-layer-serialized-queue-project-${automatic}`),
          title: "Serialized queue",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: process.cwd(),
        });
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          createdBy: "user",
          creationSource: "web",
          commandId: CommandId.make(`runtime-layer-serialized-queue-active-${automatic}`),
          threadId,
          messageId: MessageId.make(`runtime-layer-serialized-queue-active-${automatic}`),
          text: "Active",
          attachments: [],
          modelSelection,
          dispatchMode: { type: "start_immediately" },
        });
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          createdBy: automatic ? "agent" : "user",
          creationSource: automatic ? "provider" : "web",
          ...(automatic
            ? {
                notification: {
                  source: { kind: "monitor" as const },
                  outcome: "updated" as const,
                  summary: "Monitor updated",
                  detail: "Build is green",
                },
              }
            : {}),
          commandId: CommandId.make(`runtime-layer-serialized-queue-first-${automatic}`),
          threadId,
          messageId: MessageId.make(`runtime-layer-serialized-queue-first-${automatic}`),
          text: "First queued",
          attachments: [],
          modelSelection,
          dispatchMode: { type: "queue_after_active" },
        });
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          createdBy: "user",
          creationSource: "web",
          commandId: CommandId.make(`runtime-layer-serialized-queue-second-${automatic}`),
          threadId,
          messageId: MessageId.make(`runtime-layer-serialized-queue-second-${automatic}`),
          text: "Second queued",
          attachments: [],
          modelSelection,
          dispatchMode: { type: "queue_after_active" },
        });

        const before = yield* orchestrator.getThreadProjection(threadId);
        const activeRun = before.runs.find((run) => run.status === "starting");
        const queuedRuns = before.runs
          .filter((run) => run.status === "queued")
          .toSorted((left, right) => left.ordinal - right.ordinal);
        const firstQueuedRun = queuedRuns[0];
        const secondQueuedRun = queuedRuns[1];
        assert.isDefined(activeRun);
        assert.isDefined(firstQueuedRun);
        assert.isDefined(secondQueuedRun);
        assert.isFalse(
          before.turnItems.some(
            (item) =>
              item.type === "user_message" &&
              (item.messageId === firstQueuedRun.userMessageId ||
                item.messageId === secondQueuedRun.userMessageId),
          ),
          "queued messages must not exist as turn items before dispatch",
        );

        const promotedRunIds = yield* Queue.unbounded<RunId>();
        const afterSequence = yield* orchestrator.getThreadEventSequence(threadId);
        yield* eventSink.stream({ threadId, afterSequence }).pipe(
          Stream.runForEach((stored) =>
            stored.event.type === "run.updated" && stored.event.payload.status === "starting"
              ? Queue.offer(promotedRunIds, stored.event.payload.id)
              : Effect.void,
          ),
          Effect.forkScoped,
        );
        yield* Effect.yieldNow;

        const activeCompletedAt = yield* DateTime.now;
        yield* eventSink.write({
          events: [
            {
              id: EventId.make(`runtime-layer-serialized-queue-active-completed-${automatic}`),
              type: "run.updated",
              threadId,
              runId: activeRun.id,
              ...(activeRun.rootNodeId === null ? {} : { nodeId: activeRun.rootNodeId }),
              providerInstanceId: activeRun.providerInstanceId,
              occurredAt: activeCompletedAt,
              payload: {
                ...activeRun,
                status: "completed",
                completedAt: activeCompletedAt,
              },
            },
          ],
        });

        assert.equal(yield* Queue.take(promotedRunIds), firstQueuedRun.id);
        const afterFirstPromotion = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(
          afterFirstPromotion.runs.find((run) => run.id === firstQueuedRun.id)?.status,
          "starting",
        );
        assert.equal(
          afterFirstPromotion.runs.find((run) => run.id === secondQueuedRun.id)?.status,
          "queued",
        );
        const promotedMessageItem = afterFirstPromotion.turnItems.find(
          (item) =>
            item.runId === firstQueuedRun.id &&
            (item.type === "user_message" || item.type === "notification"),
        );
        assert.isDefined(promotedMessageItem);
        if (automatic) {
          assert.equal(promotedMessageItem.type, "notification");
          assert.equal(
            afterFirstPromotion.messages.find(
              (message) => message.id === firstQueuedRun.userMessageId,
            )?.text,
            "First queued",
          );
          assert.equal(
            afterFirstPromotion.messages.find(
              (message) => message.id === firstQueuedRun.userMessageId,
            )?.notification?.summary,
            "Monitor updated",
          );
          assert.isFalse(
            afterFirstPromotion.turnItems.some(
              (item) =>
                item.type === "user_message" && item.messageId === firstQueuedRun.userMessageId,
            ),
          );
        } else {
          assert.equal(promotedMessageItem.type, "user_message");
        }
        assert.isTrue(
          promotedMessageItem.startedAt !== null &&
            DateTime.toEpochMillis(promotedMessageItem.startedAt) >=
              DateTime.toEpochMillis(activeCompletedAt),
        );

        const promotedFirst = afterFirstPromotion.runs.find((run) => run.id === firstQueuedRun.id);
        assert.isDefined(promotedFirst);
        const firstCompletedAt = yield* DateTime.now;
        yield* eventSink.write({
          events: [
            {
              id: EventId.make(`runtime-layer-serialized-queue-first-completed-${automatic}`),
              type: "run.updated",
              threadId,
              runId: promotedFirst.id,
              ...(promotedFirst.rootNodeId === null ? {} : { nodeId: promotedFirst.rootNodeId }),
              providerInstanceId: promotedFirst.providerInstanceId,
              occurredAt: firstCompletedAt,
              payload: {
                ...promotedFirst,
                status: "completed",
                completedAt: firstCompletedAt,
              },
            },
          ],
        });

        assert.equal(yield* Queue.take(promotedRunIds), secondQueuedRun.id);
        const afterSecondPromotion = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(
          afterSecondPromotion.runs.find((run) => run.id === firstQueuedRun.id)?.status,
          "completed",
        );
        assert.equal(
          afterSecondPromotion.runs.find((run) => run.id === secondQueuedRun.id)?.status,
          "starting",
        );
      }),
  );

  it.effect("starts a wake's work clock from the run that ran before it", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const eventSink = yield* EventSink.EventSinkV2;
      const threadId = ThreadId.make("runtime-layer-wake-work-start-thread");
      const messageId = (key: string) => MessageId.make(`runtime-layer-wake-work-start-${key}`);

      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-wake-work-start-create"),
        threadId,
        projectId: ProjectId.make("runtime-layer-wake-work-start-project"),
        title: "Wake work start",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: process.cwd(),
      });
      const dispatch = (key: string, wake: boolean) =>
        orchestrator.dispatch({
          type: "message.dispatch",
          createdBy: wake ? "agent" : "user",
          creationSource: wake ? "provider" : "web",
          ...(wake
            ? {
                notification: {
                  source: { kind: "background_task" as const },
                  outcome: "updated" as const,
                  summary: "Background activity updated",
                },
              }
            : {}),
          commandId: CommandId.make(`runtime-layer-wake-work-start-${key}`),
          threadId,
          messageId: messageId(key),
          text: key,
          attachments: [],
          modelSelection,
          dispatchMode:
            key === "prompt" ? { type: "start_immediately" } : { type: "queue_after_active" },
        });
      const runFor = (key: string) =>
        Effect.map(orchestrator.getThreadProjection(threadId), ({ runs }) => {
          const run = runs.find((candidate) => candidate.userMessageId === messageId(key));
          assert.isDefined(run);
          return run;
        });
      yield* dispatch("prompt", false);
      yield* dispatch("queued", false);
      yield* dispatch("early-wake", true);
      // The early wake now runs ahead of the older queued prompt, as a
      // delegated result does when it jumps the queue.
      yield* orchestrator.dispatch({
        type: "queued-run.reorder",
        commandId: CommandId.make("runtime-layer-wake-work-start-reorder"),
        threadId,
        runId: (yield* runFor("queued")).id,
        beforeRunId: null,
      });
      yield* dispatch("late-wake", true);
      // A queued wake has no clock yet: what runs before it is still unknown.
      assert.isUndefined((yield* runFor("early-wake")).workStartedAt);

      const startedRunIds = yield* Queue.unbounded<RunId>();
      const afterSequence = yield* orchestrator.getThreadEventSequence(threadId);
      yield* eventSink.stream({ threadId, afterSequence }).pipe(
        Stream.runForEach((stored) =>
          stored.event.type === "run.updated" && stored.event.payload.status === "starting"
            ? Queue.offer(startedRunIds, stored.event.payload.id)
            : Effect.void,
        ),
        Effect.forkScoped,
      );
      yield* Effect.yieldNow;

      const now = yield* DateTime.now;
      // Runs start and settle the way the provider would report them.
      const settle = (key: string, startedAt: DateTime.Utc) =>
        Effect.gen(function* () {
          const run = yield* runFor(key);
          yield* eventSink.write({
            events: [
              {
                id: EventId.make(`runtime-layer-wake-work-start-${key}-completed`),
                type: "run.updated",
                threadId,
                runId: run.id,
                ...(run.rootNodeId === null ? {} : { nodeId: run.rootNodeId }),
                providerInstanceId: run.providerInstanceId,
                occurredAt: startedAt,
                payload: { ...run, status: "completed", startedAt, completedAt: startedAt },
              },
            ],
          });
        });
      const millis = (value: DateTime.Utc | undefined) =>
        value === undefined ? undefined : DateTime.toEpochMillis(value);

      yield* settle("prompt", now);
      assert.equal(yield* Queue.take(startedRunIds), (yield* runFor("early-wake")).id);
      assert.equal(millis((yield* runFor("early-wake")).workStartedAt), millis(now));

      yield* settle("early-wake", DateTime.add(now, { seconds: 1 }));
      assert.equal(yield* Queue.take(startedRunIds), (yield* runFor("queued")).id);
      assert.isUndefined((yield* runFor("queued")).workStartedAt);

      // The queued prompt starts long after it was requested; the wake after it
      // counts from that start, not from the request.
      const queuedStartedAt = DateTime.add(now, { minutes: 10 });
      yield* settle("queued", queuedStartedAt);
      assert.equal(yield* Queue.take(startedRunIds), (yield* runFor("late-wake")).id);
      assert.equal(millis((yield* runFor("late-wake")).workStartedAt), millis(queuedStartedAt));
    }),
  );

  it.effect.each(["usage_limit", "provider_error"] as const)(
    "handles a queued message after a %s failure",
    (failureClass) =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const eventSink = yield* EventSink.EventSinkV2;
        const threadId = ThreadId.make(`runtime-layer-failed-queue-${failureClass}`);

        yield* orchestrator.dispatch({
          type: "thread.create",
          createdBy: "user",
          creationSource: "web",
          commandId: CommandId.make(`${threadId}:create`),
          threadId,
          projectId: ProjectId.make(`${threadId}:project`),
          title: "Failed queue",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: process.cwd(),
        });
        for (const index of [0, 1]) {
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            createdBy: "user",
            creationSource: "web",
            commandId: CommandId.make(`${threadId}:message:${index}`),
            threadId,
            messageId: MessageId.make(`${threadId}:message:${index}`),
            text: index === 0 ? "Active" : "Queued",
            attachments: [],
            modelSelection,
            dispatchMode: { type: index === 0 ? "start_immediately" : "queue_after_active" },
          });
        }

        const before = yield* orchestrator.getThreadProjection(threadId);
        const activeRun = before.runs.find((run) => run.status === "starting");
        const queuedRun = before.runs.find((run) => run.status === "queued");
        assert.isDefined(activeRun);
        assert.isDefined(queuedRun);
        assert.isNotNull(activeRun.rootNodeId);

        const promotedRunIds = yield* Queue.unbounded<RunId>();
        const heldRunIds = yield* Queue.unbounded<RunId>();
        const afterSequence = yield* orchestrator.getThreadEventSequence(threadId);
        yield* eventSink.stream({ threadId, afterSequence }).pipe(
          Stream.runForEach((stored) =>
            stored.event.type !== "run.updated"
              ? Effect.void
              : stored.event.payload.status === "starting"
                ? Queue.offer(promotedRunIds, stored.event.payload.id)
                : stored.event.payload.queueHeld === true
                  ? Queue.offer(heldRunIds, stored.event.payload.id)
                  : Effect.void,
          ),
          Effect.forkScoped,
        );
        yield* Effect.yieldNow;

        const now = yield* DateTime.now;
        yield* eventSink.write({
          events: [
            {
              id: EventId.make(`${threadId}:error`),
              type: "turn-item.updated",
              threadId,
              runId: activeRun.id,
              nodeId: activeRun.rootNodeId,
              providerInstanceId: activeRun.providerInstanceId,
              occurredAt: now,
              payload: {
                id: TurnItemId.make(`${threadId}:error`),
                type: "error",
                threadId,
                runId: activeRun.id,
                nodeId: activeRun.rootNodeId,
                providerThreadId: activeRun.providerThreadId,
                providerTurnId: null,
                nativeItemRef: null,
                parentItemId: null,
                ordinal: 2,
                status: "failed",
                title: "Provider failure",
                startedAt: now,
                completedAt: now,
                updatedAt: now,
                failure: {
                  class: failureClass,
                  message: "Provider failed.",
                  code: "provider_failed",
                  retryable: null,
                  ...(failureClass === "usage_limit"
                    ? { resetAt: DateTime.formatIso(DateTime.add(now, { hours: 1 })) }
                    : {}),
                },
              },
            },
            {
              id: EventId.make(`${threadId}:failed`),
              type: "run.updated",
              threadId,
              runId: activeRun.id,
              nodeId: activeRun.rootNodeId,
              providerInstanceId: activeRun.providerInstanceId,
              occurredAt: now,
              payload: { ...activeRun, status: "failed", completedAt: now },
            },
          ],
        });

        if (failureClass === "provider_error") {
          assert.equal(yield* Queue.take(heldRunIds), queuedRun.id);
          const held = yield* orchestrator.getThreadProjection(threadId);
          assert.equal(held.runs.find((run) => run.id === queuedRun.id)?.status, "queued");
          yield* orchestrator.dispatch({
            type: "queue.resume",
            commandId: CommandId.make(`${threadId}:resume`),
            threadId,
          });
          assert.equal(yield* Queue.take(promotedRunIds), queuedRun.id);
          return;
        }
        yield* orchestrator.resumeQueuedRuns;
        const after = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(after.runs.find((run) => run.id === queuedRun.id)?.status, "queued");
        assert.isFalse(after.turnItems.some((item) => item.runId === queuedRun.id));
        const shell = (yield* orchestrator.getShellSnapshot()).threads.find(
          (thread) => thread.id === threadId,
        );
        assert.equal(shell?.latestRunId, activeRun.id);
        assert.equal(shell?.status, "failed");
        assert.equal(shell?.lastErrorClass, "usage_limit");
      }),
  );

  it.effect("keeps the queue after a user interrupts the active run", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threadId = ThreadId.make("runtime-layer-interrupted-queue");
      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make(`${threadId}:create`),
        threadId,
        projectId: ProjectId.make(`${threadId}:project`),
        title: "Interrupted queue",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: process.cwd(),
      });
      for (const [index, text] of ["Active", "Queued"].entries()) {
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          createdBy: "user",
          creationSource: "web",
          commandId: CommandId.make(`${threadId}:message:${index}`),
          threadId,
          messageId: MessageId.make(`${threadId}:message:${index}`),
          text,
          attachments: [],
          modelSelection,
          dispatchMode: { type: index === 0 ? "start_immediately" : "queue_after_active" },
        });
      }
      const before = yield* orchestrator.getThreadProjection(threadId);
      const activeRun = before.runs[0]!;
      const queuedRun = before.runs[1]!;
      yield* orchestrator.dispatch({
        type: "run.interrupt",
        commandId: CommandId.make(`${threadId}:interrupt`),
        threadId,
        runId: activeRun.id,
        holdQueue: true,
      });

      yield* orchestrator.resumeQueuedRuns;
      const after = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(after.runs.find((run) => run.id === activeRun.id)?.status, "interrupted");
      assert.equal(after.runs.find((run) => run.id === queuedRun.id)?.status, "queued");
      assert.isTrue(after.runs.find((run) => run.id === queuedRun.id)?.queueHeld);
      assert.isFalse(after.turnItems.some((item) => item.runId === queuedRun.id));
    }),
  );

  it.effect.each(["startup", "shutdown"] as const)(
    "preserves and holds queued messages across %s until explicitly resumed",
    (trigger) =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const recovery = yield* ProviderRuntimeRecoveryService.ProviderRuntimeRecoveryService;
        const threadId = ThreadId.make(`queue-hold-${trigger}`);
        yield* orchestrator.dispatch({
          type: "thread.create",
          createdBy: "user",
          creationSource: "web",
          commandId: CommandId.make(`${threadId}:create`),
          threadId,
          projectId: ProjectId.make(`${threadId}:project`),
          title: "Recover queue",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: process.cwd(),
        });
        for (const [index, text] of ["Active", "First queued", "Second queued"].entries()) {
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            createdBy: "user",
            creationSource: "web",
            commandId: CommandId.make(`${threadId}:message:${index}`),
            threadId,
            messageId: MessageId.make(`${threadId}:message:${index}`),
            text,
            attachments: [],
            modelSelection,
            dispatchMode: { type: index === 0 ? "start_immediately" : "queue_after_active" },
          });
        }
        const before = yield* orchestrator.getThreadProjection(threadId);
        const queued = before.runs.filter((run) => run.status === "queued");
        assert.equal(queued.length, 2);
        yield* recovery.reconcile(trigger);
        // A second boot must preserve the hold, even when only queued work remains.
        yield* recovery.reconcile("startup");
        const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
        assert.isTrue((yield* maintenance.rebuild).valid);
        assert.equal(yield* orchestrator.resumeQueuedRuns, 0);
        const held = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(
          held.runs.map((run) => run.status),
          ["cancelled", "queued", "queued"],
        );
        for (const run of queued) {
          assert.deepEqual(
            held.runs.find((row) => row.id === run.id),
            { ...run, queueHeld: true },
          );
          assert.deepEqual(
            held.messages.find((row) => row.id === run.userMessageId),
            before.messages.find((row) => row.id === run.userMessageId),
          );
          assert.equal(
            held.attempts.find((row) => row.id === run.activeAttemptId)?.status,
            "pending",
          );
          assert.equal(held.nodes.find((row) => row.id === run.rootNodeId)?.status, "pending");
        }
        // Editing and reordering are allowed without releasing the hold.
        const first = queued[0]!;
        const second = queued[1]!;
        yield* orchestrator.dispatch({
          type: "queued-run.edit",
          commandId: CommandId.make(`${threadId}:edit`),
          threadId,
          runId: second.id,
          text: "Edited second message",
        });
        yield* orchestrator.dispatch({
          type: "queued-run.reorder",
          commandId: CommandId.make(`${threadId}:reorder`),
          threadId,
          runId: second.id,
          beforeRunId: first.id,
        });
        assert.equal(yield* orchestrator.resumeQueuedRuns, 0);
        const resume = {
          type: "queue.resume" as const,
          commandId: CommandId.make(`${threadId}:resume`),
          threadId,
        };
        yield* orchestrator.dispatch(resume);
        yield* orchestrator.dispatch(resume);
        const resumed = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(resumed.runs.find((run) => run.id === second.id)?.status, "starting");
        assert.equal(resumed.runs.find((run) => run.id === first.id)?.status, "queued");
        assert.isFalse(resumed.runs.some((run) => run.status === "queued" && run.queueHeld));
        assert.equal(
          resumed.messages.find((row) => row.id === second.userMessageId)?.text,
          "Edited second message",
        );
        assert.equal(resumed.runs.length, 3, "resume retries must not duplicate messages or runs");
      }),
  );

  it.effect("edits and removes queued runs", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threadId = ThreadId.make("runtime-layer-queued-edit-thread");

      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-queued-edit-create"),
        threadId,
        projectId: ProjectId.make("runtime-layer-queued-edit-project"),
        title: "Edit queued work",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: "/tmp/runtime-layer-queued-edit",
      });
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-queued-edit-active-message"),
        threadId,
        messageId: MessageId.make("runtime-layer-queued-edit-active-message"),
        text: "Keep the provider occupied.",
        attachments: [],
        modelSelection,
        dispatchMode: { type: "start_immediately" },
      });
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-queued-edit-queued-message"),
        threadId,
        messageId: MessageId.make("runtime-layer-queued-edit-queued-message"),
        text: "Original queued text.",
        attachments: [],
        modelSelection,
        dispatchMode: { type: "queue_after_active" },
      });

      const before = yield* orchestrator.getThreadProjection(threadId);
      const queuedRun = before.runs.find((run) => run.status === "queued");
      assert.isDefined(queuedRun);

      yield* orchestrator.dispatch({
        type: "queued-run.edit",
        commandId: CommandId.make("runtime-layer-queued-edit-edit"),
        threadId,
        runId: queuedRun.id,
        text: "Updated queued text.",
      });

      const afterEdit = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(
        afterEdit.messages.find((message) => message.id === queuedRun.userMessageId)?.text,
        "Updated queued text.",
      );
      const editedItem = afterEdit.turnItems.find(
        (item) => item.type === "user_message" && item.messageId === queuedRun.userMessageId,
      );
      assert.isUndefined(editedItem, "editing queue state must not create a timeline turn item");

      yield* orchestrator.dispatch({
        type: "queued-run.edit",
        commandId: CommandId.make("runtime-layer-queued-edit-attachments"),
        threadId,
        runId: queuedRun.id,
        text: "Updated queued text with an attachment.",
        attachments: [
          {
            type: "image",
            id: "runtime-layer-queued-edit-attachment",
            name: "screenshot.png",
            mimeType: "image/png",
            sizeBytes: 128,
          },
        ],
      });
      const afterAttachmentEdit = yield* orchestrator.getThreadProjection(threadId);
      assert.deepEqual(
        afterAttachmentEdit.messages
          .find((message) => message.id === queuedRun.userMessageId)
          ?.attachments.map((attachment) => attachment.id),
        ["runtime-layer-queued-edit-attachment"],
      );

      yield* orchestrator.dispatch({
        type: "queued-run.edit",
        commandId: CommandId.make("runtime-layer-queued-edit-text-only"),
        threadId,
        runId: queuedRun.id,
        text: "Text-only edit keeps attachments.",
      });
      const afterTextOnlyEdit = yield* orchestrator.getThreadProjection(threadId);
      assert.deepEqual(
        afterTextOnlyEdit.messages
          .find((message) => message.id === queuedRun.userMessageId)
          ?.attachments.map((attachment) => attachment.id),
        ["runtime-layer-queued-edit-attachment"],
        "an edit without attachments must leave the stored attachments untouched",
      );

      const emptyEditError = yield* orchestrator
        .dispatch({
          type: "queued-run.edit",
          commandId: CommandId.make("runtime-layer-queued-edit-empty"),
          threadId,
          runId: queuedRun.id,
          text: "   ",
        })
        .pipe(Effect.flip);
      assert.equal(emptyEditError._tag, "OrchestratorCommandRejectedError");

      yield* orchestrator.dispatch({
        type: "queued-run.cancel",
        commandId: CommandId.make("runtime-layer-queued-edit-cancel"),
        threadId,
        runId: queuedRun.id,
      });

      const afterCancel = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(afterCancel.runs.find((run) => run.id === queuedRun.id)?.status, "cancelled");
      assert.equal(
        afterCancel.attempts.find((attempt) => attempt.runId === queuedRun.id)?.status,
        "cancelled",
      );
      assert.equal(
        afterCancel.nodes.find((node) => node.runId === queuedRun.id)?.status,
        "cancelled",
      );
      assert.isFalse(
        afterCancel.visibleTurnItems.some(
          (row) => row.item.type === "user_message" && row.item.runId === queuedRun.id,
        ),
        "removed queued message must not surface as a transcript row",
      );
      assert.equal(yield* orchestrator.resumeQueuedRuns, 0);

      const cancelAgainError = yield* orchestrator
        .dispatch({
          type: "queued-run.cancel",
          commandId: CommandId.make("runtime-layer-queued-edit-cancel-again"),
          threadId,
          runId: queuedRun.id,
        })
        .pipe(Effect.flip);
      assert.equal(cancelAgainError._tag, "OrchestratorDispatchError");
    }),
  );
});

it.layer(SharedApplicationDataPlaneTestLayer)("pending provider interruption", (it) => {
  it.effect("interrupts a pending provider start without launching provider work", () =>
    Effect.gen(function* () {
      const projects = yield* ProjectService.ProjectService;
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threadManagement = yield* ThreadManagementService.ThreadManagementService;
      const effectWorker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const projectId = ProjectId.make("runtime-layer-pending-interrupt-project");
      const threadId = ThreadId.make("runtime-layer-pending-interrupt-thread");

      yield* projects.create({
        commandId: CommandId.make("runtime-layer-pending-interrupt-project-create"),
        projectId,
        title: "Pending interrupt project",
        workspaceRoot: "/tmp/runtime-layer-pending-interrupt-project",
      });
      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-pending-interrupt-create"),
        threadId,
        projectId,
        title: "Pending interrupt",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
      });
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-pending-interrupt-message"),
        threadId,
        messageId: MessageId.make("runtime-layer-pending-interrupt-message"),
        text: "Do not reach the provider.",
        attachments: [],
        modelSelection,
        dispatchMode: { type: "start_immediately" },
      });

      const starting = yield* orchestrator.getThreadProjection(threadId);
      const run = starting.runs[0];
      assert.isDefined(run);
      assert.equal(run.status, "starting");

      const interrupt = yield* threadManagement.interruptThread({
        projectId,
        commandId: CommandId.make("runtime-layer-pending-interrupt-command"),
        threadId,
        runId: run.id,
        reason: "Cancelled before provider start",
      });
      assert.equal(interrupt.type, "interrupt_requested");

      const interrupted = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(interrupted.runs[0]?.status, "interrupted");
      assert.equal(interrupted.attempts[0]?.status, "interrupted");
      assert.equal(
        interrupted.nodes.find((node) => node.kind === "root_turn")?.status,
        "interrupted",
      );
      assert.deepEqual(
        interrupted.turnItems.filter((item) => item.runId === run.id).map((item) => item.type),
        ["user_message", "run_interrupt_request", "run_interrupt_result"],
      );
      assert.deepEqual(interrupted.providerTurns, []);
      assert.isFalse(yield* effectWorker.runOnce);
    }),
  );
});

it.layer(SharedApplicationDataPlaneTestLayer)("snooze projection", (it) => {
  it.effect("carries snooze state through the V2 shell projection", () =>
    Effect.gen(function* () {
      const projects = yield* ProjectService.ProjectService;
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projectId = ProjectId.make("runtime-layer-snoozed-project");
      const threadId = ThreadId.make("runtime-layer-snoozed-thread");
      const snoozedUntil = "2099-07-25T09:00:00.000Z";

      yield* projects.create({
        commandId: CommandId.make("runtime-layer-snoozed-project-create"),
        projectId,
        title: "Snoozed shell projection",
        workspaceRoot: "/tmp/runtime-layer-snoozed-project",
      });
      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-snoozed-thread-create"),
        threadId,
        projectId,
        title: "Snoozed thread",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
      });
      yield* orchestrator.dispatch({
        type: "thread.snooze",
        commandId: CommandId.make("runtime-layer-snoozed-thread-snooze"),
        threadId,
        snoozedUntil,
      });

      const firstProjection = yield* orchestrator.getThreadProjection(threadId);
      const firstSnoozedAt = firstProjection.thread.snoozedAt;
      const firstUpdatedAt = firstProjection.thread.updatedAt;
      assert.isNotNull(firstSnoozedAt);

      yield* orchestrator.dispatch({
        type: "thread.snooze",
        commandId: CommandId.make("runtime-layer-snoozed-thread-snooze-again"),
        threadId,
        snoozedUntil,
      });

      const shell = yield* orchestrator.getShellSnapshot();
      const thread = shell.threads.find((candidate) => candidate.id === threadId);
      assert.isDefined(thread);
      assert.equal(DateTime.formatIso(thread.snoozedUntil!), snoozedUntil);
      assert.deepEqual(thread.snoozedAt, firstSnoozedAt);
      assert.deepEqual(thread.updatedAt, firstUpdatedAt);

      yield* orchestrator.dispatch({
        type: "message.dispatch",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-snoozed-message"),
        threadId,
        messageId: MessageId.make("runtime-layer-snoozed-message"),
        text: "Wake this thread.",
        attachments: [],
        modelSelection,
        dispatchMode: { type: "start_immediately" },
      });

      const awakened = yield* orchestrator.getThreadProjection(threadId);
      assert.isNull(awakened.thread.snoozedUntil);
      assert.isNull(awakened.thread.snoozedAt);
    }),
  );
});

it.layer(SharedApplicationDataPlaneTestLayer)("visited projection", (it) => {
  it.effect("carries the visited watermark through the V2 shell projection", () =>
    Effect.gen(function* () {
      const projects = yield* ProjectService.ProjectService;
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projectId = ProjectId.make("runtime-layer-visited-project");
      const threadId = ThreadId.make("runtime-layer-visited-thread");
      const visitedAt = "2026-07-24T01:00:00.000Z";

      yield* projects.create({
        commandId: CommandId.make("runtime-layer-visited-project-create"),
        projectId,
        title: "Visited shell projection",
        workspaceRoot: "/tmp/runtime-layer-visited-project",
      });
      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-visited-thread-create"),
        threadId,
        projectId,
        title: "Visited thread",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
      });
      const created = yield* orchestrator.getThreadProjection(threadId);
      assert.isNull(created.thread.lastVisitedAt);
      const createdUpdatedAt = created.thread.updatedAt;

      yield* TestClock.adjust("1 second");
      yield* orchestrator.dispatch({
        type: "thread.visit",
        commandId: CommandId.make("runtime-layer-visited-thread-visit"),
        threadId,
        visitedAt,
      });
      const visited = yield* orchestrator.getThreadProjection(threadId);
      assert.isNotNull(visited.thread.lastVisitedAt);
      assert.equal(DateTime.formatIso(visited.thread.lastVisitedAt!), visitedAt);
      // Visiting records read state, not activity: updatedAt must not move.
      assert.deepEqual(visited.thread.updatedAt, createdUpdatedAt);

      // Monotonic: an older watermark (a replay or a stale device) cannot
      // rewind the marker.
      yield* orchestrator.dispatch({
        type: "thread.visit",
        commandId: CommandId.make("runtime-layer-visited-thread-visit-stale"),
        threadId,
        visitedAt: "2026-07-24T00:30:00.000Z",
      });
      const afterStaleVisit = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(DateTime.formatIso(afterStaleVisit.thread.lastVisitedAt!), visitedAt);

      const shell = yield* orchestrator.getShellSnapshot();
      const thread = shell.threads.find((candidate) => candidate.id === threadId);
      assert.isDefined(thread);
      assert.equal(DateTime.formatIso(thread!.lastVisitedAt!), visitedAt);
      assert.deepEqual(thread!.updatedAt, createdUpdatedAt);

      // No completed run yet → nothing to mark unread against.
      const markUnread = yield* orchestrator
        .dispatch({
          type: "thread.mark-unread",
          commandId: CommandId.make("runtime-layer-visited-thread-mark-unread"),
          threadId,
        })
        .pipe(Effect.flip);
      assert.instanceOf(markUnread, Orchestrator.OrchestratorDispatchError);

      // A read receipt must not decode any transcript, including inherited or
      // unreadable historical rows. It only advances the thread's watermark.
      const sql = yield* SqlClient.SqlClient;
      yield* sql`INSERT INTO orchestration_v2_projection_turn_items (
        turn_item_id, thread_id, ordinal, type, status, updated_at, payload_json
      ) VALUES (
        'item:visited:unreadable-history', ${threadId}, 1, 'dynamic_tool', 'completed',
        ${visitedAt}, '{broken'
      )`;
      const nextVisitedAt = "2026-07-24T02:00:00.000Z";
      yield* orchestrator.dispatch({
        type: "thread.visit",
        commandId: CommandId.make("runtime-layer-visited-without-history"),
        threadId,
        visitedAt: nextVisitedAt,
      });
      const [watermark] = yield* sql<{ readonly visited_at: string; readonly updated_at: string }>`
        SELECT json_extract(payload_json, '$.lastVisitedAt') AS visited_at, updated_at
        FROM orchestration_v2_projection_threads WHERE thread_id = ${threadId}
      `;
      assert.equal(watermark!.visited_at, nextVisitedAt);
      assert.equal(watermark!.updated_at, DateTime.formatIso(createdUpdatedAt));
      const invalidVisit = yield* orchestrator
        .dispatch({
          type: "thread.visit",
          commandId: CommandId.make("runtime-layer-visited-invalid-timestamp"),
          threadId,
          visitedAt: "invalid-timestamp",
        })
        .pipe(Effect.flip);
      assert.instanceOf(invalidVisit, Orchestrator.OrchestratorDispatchError);
    }),
  );
});

it.layer(SharedApplicationDataPlaneTestLayer)("shared application data plane", (it) => {
  it.effect("orders retained project transactions and V2 thread transactions in one source", () =>
    Effect.gen(function* () {
      const projects = yield* ProjectService.ProjectService;
      const applicationEvents = yield* OrchestrationEventStore.OrchestrationEventStore;
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const sql = yield* SqlClient.SqlClient;
      const projectId = ProjectId.make("runtime-layer-shared-project");
      const threadId = ThreadId.make("runtime-layer-shared-thread");
      const projectInput = {
        commandId: CommandId.make("runtime-layer-shared-project-create"),
        projectId,
        title: "Shared application source",
        workspaceRoot: "/tmp/runtime-layer-shared-project",
      };

      const created = yield* projects.create(projectInput);
      const retried = yield* projects.create(projectInput);
      assert.deepEqual(retried, created);

      const delivered = yield* Queue.unbounded<ApplicationStoredEvent>();
      yield* applicationEvents.streamApplicationEvents().pipe(
        Stream.take(2),
        Stream.runForEach((event) => Queue.offer(delivered, event)),
        Effect.forkScoped,
      );

      const projectEvent = yield* Queue.take(delivered);
      assert.isTrue("aggregateKind" in projectEvent && projectEvent.aggregateId === projectId);

      const threadResult = yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-shared-thread-create"),
        threadId,
        projectId,
        title: "Shared thread",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
      });
      const threadEvent = yield* Queue.take(delivered);

      assert.equal(threadEvent.sequence, threadResult.sequence);
      assert.isAbove(threadEvent.sequence, projectEvent.sequence);
      assert.isTrue("aggregateKind" in projectEvent);
      assert.isTrue("event" in threadEvent);
      assert.equal((yield* projects.getById(projectId))._tag, "Some");

      const retainedReceipts = yield* sql<{
        readonly aggregate_kind: string;
        readonly aggregate_id: string;
      }>`
        SELECT aggregate_kind, aggregate_id
        FROM orchestration_command_receipts
        ORDER BY result_sequence ASC
      `;
      assert.deepEqual(retainedReceipts, [
        { aggregate_kind: "project", aggregate_id: projectId },
        { aggregate_kind: "thread", aggregate_id: threadId },
      ]);

      const retiredWrites = yield* sql<{ readonly count: number }>`
        SELECT
          (SELECT COUNT(*) FROM orchestration_v2_events) +
          (SELECT COUNT(*) FROM orchestration_v2_command_receipts) AS count
      `;
      assert.equal(retiredWrites[0]?.count, 0);
    }),
  );
});

it.layer(TestLayer)("usage-limit recovery", (it) => {
  it.effect.each(["interrupted", "usage_limit"] as const)(
    "manually resumes an %s run ahead of its queued message only once",
    (reason) =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const events = yield* EventSink.EventSinkV2;
        const threadId = ThreadId.make(`manual-resume:${reason}`);
        const projectId = ProjectId.make(`manual-resume:project:${reason}`);
        const now = yield* DateTime.now;
        const createdAt = DateTime.formatIso(now);
        yield* seedProject({
          projectId,
          title: "Resume project",
          workspaceRoot: process.cwd(),
          defaultModelSelection: modelSelection,
          createdAt,
        });
        yield* orchestrator.dispatch({
          type: "thread.create",
          commandId: CommandId.make(`manual-resume:create:${reason}`),
          threadId,
          projectId,
          title: "Interrupted thread",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          createdBy: "user",
          creationSource: "web",
        });
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(`manual-resume:start:${reason}`),
          threadId,
          messageId: MessageId.make(`manual-resume:start:${reason}`),
          text: "Start work.",
          attachments: [],
          dispatchMode: { type: "defer_start" },
          createdBy: "user",
          creationSource: "web",
        });
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(`manual-resume:queue:${reason}`),
          threadId,
          messageId: MessageId.make(`manual-resume:queue:${reason}`),
          text: "Follow up.",
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "user",
          creationSource: "web",
        });
        const [source, queued] = (yield* orchestrator.getThreadProjection(threadId)).runs as [
          OrchestrationV2Run,
          OrchestrationV2Run,
        ];
        // A user stop holds the queue as the run ends (thread.turn.interrupt with
        // holdQueue). Without the hold, the terminal-run worker may start the
        // queued run before the resume below, depending on fiber scheduling.
        yield* events.write({
          events: [
            {
              id: EventId.make(`manual-resume:hold:${reason}`),
              type: "run.updated",
              threadId,
              occurredAt: now,
              payload: { ...queued, queueHeld: true },
            },
            {
              id: EventId.make(`manual-resume:stop:${reason}`),
              type: "run.updated",
              threadId,
              occurredAt: now,
              payload: {
                ...source,
                status: reason === "interrupted" ? "interrupted" : "failed",
                completedAt: now,
              },
            },
          ],
        });
        let scheduledResume: ReturnType<typeof limitRecoveryCommand> = null;
        if (reason === "usage_limit") {
          const resetAt = DateTime.formatIso(DateTime.add(now, { minutes: 1 }));
          yield* events.write({
            events: [
              {
                id: EventId.make(`manual-resume:error:${reason}`),
                type: "turn-item.updated",
                threadId,
                occurredAt: now,
                payload: {
                  id: TurnItemId.make(`manual-resume:error:${reason}`),
                  type: "error",
                  threadId,
                  runId: source.id,
                  nodeId: source.rootNodeId,
                  providerThreadId: null,
                  providerTurnId: null,
                  nativeItemRef: null,
                  parentItemId: null,
                  ordinal: 2,
                  status: "failed",
                  title: "Usage limit reached",
                  startedAt: now,
                  completedAt: now,
                  updatedAt: now,
                  failure: {
                    class: "usage_limit",
                    message: "Plan limit reached.",
                    code: "usageLimitExceeded",
                    retryable: null,
                    resetAt,
                  },
                },
              },
            ],
          });
          const shell = (yield* orchestrator.getShellSnapshot()).threads.find(
            (thread) => thread.id === threadId,
          )!;
          yield* orchestrator.dispatch(
            limitRecoveryCommand(shell, true, DateTime.toEpochMillis(now))!,
          );
          const armed = (yield* orchestrator.getShellSnapshot()).threads.find(
            (thread) => thread.id === threadId,
          )!;
          scheduledResume = limitRecoveryCommand(armed, true, Date.parse(resetAt));
          assert.isNotNull(scheduledResume);
        }
        const resume = (suffix: string) => ({
          type: "message.dispatch" as const,
          commandId: CommandId.make(`manual-resume:${suffix}:${reason}`),
          threadId,
          messageId: MessageId.make(`manual-resume:${suffix}:${reason}`),
          manualContinuationOfRunId: source.id,
          text: "Continue where you left off.",
          attachments: [],
          dispatchMode: { type: "start_immediately" as const },
          createdBy: "user" as const,
          creationSource: "web" as const,
        });
        yield* orchestrator.dispatch(resume("first"));
        const after = yield* orchestrator.getThreadProjection(threadId);
        assert.lengthOf(after.runs, 3);
        assert.equal(after.runs[1]?.status, "queued");
        assert.equal(after.runs[2]?.status, "starting");
        assert.equal(
          (yield* orchestrator.dispatch(resume("second")).pipe(Effect.exit))._tag,
          "Failure",
        );
        if (scheduledResume !== null) {
          yield* TestClock.adjust("1 minute");
          yield* orchestrator.dispatch(scheduledResume);
        }
        assert.lengthOf((yield* orchestrator.getThreadProjection(threadId)).runs, 3);
      }),
  );

  it.effect.each([
    "resume",
    "queued-resume",
    "cancel",
    "rearm",
    "snooze-race",
    "new-message",
    "archive",
    "settle",
    "replacement",
    "manual-snooze",
    "manual-snooze-after-recovery",
    "invalid-snooze",
    "snooze-only",
    "snooze-resume",
    "cancel-resume-keep-snooze",
    "wake-preserve-resume",
    "independent-patches",
    "expired-snooze",
    "wake",
  ] as const)("guards a scheduled usage-limit continuation against %s", (scenario) =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const events = yield* EventSink.EventSinkV2;
      const threadId = ThreadId.make(`recovery:${scenario}`);
      const projectId = ProjectId.make(`recovery:project:${scenario}`);
      yield* seedProject({
        projectId,
        title: "Recovery project",
        workspaceRoot: process.cwd(),
        defaultModelSelection: modelSelection,
        createdAt: DateTime.formatIso(yield* DateTime.now),
      });
      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make(`recovery:create:${scenario}`),
        threadId,
        projectId,
        title: "Limited thread",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        createdBy: "user",
        creationSource: "web",
      });
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make(`recovery:message:${scenario}`),
        threadId,
        messageId: MessageId.make(`recovery:message:${scenario}`),
        text: "Work on this.",
        attachments: [],
        dispatchMode: { type: "defer_start" },
        createdBy: "user",
        creationSource: "web",
      });
      if (scenario === "queued-resume") {
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(`recovery:queued:${scenario}`),
          threadId,
          messageId: MessageId.make(`recovery:queued:${scenario}`),
          text: "Run after recovery.",
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "user",
          creationSource: "web",
        });
      }
      const projection = yield* orchestrator.getThreadProjection(threadId);
      const run = projection.runs[0]!;
      const now = yield* DateTime.now;
      const resetAt =
        scenario === "invalid-snooze"
          ? "not-a-date"
          : DateTime.formatIso(DateTime.add(now, { minutes: 1 })).replace(
              "Z",
              scenario === "wake" ? "+00:00" : "Z",
            );
      yield* events.write({
        commandId: CommandId.make(`recovery:failure:${scenario}`),
        events: [
          {
            id: EventId.make(`recovery:run:${scenario}`),
            type: "run.updated",
            threadId,
            occurredAt: now,
            payload: { ...run, status: "failed", completedAt: now },
          },
          {
            id: EventId.make(`recovery:error:${scenario}`),
            type: "turn-item.updated",
            threadId,
            occurredAt: now,
            payload: {
              id: TurnItemId.make(`recovery:error:${scenario}`),
              type: "error",
              threadId,
              runId: run.id,
              nodeId: run.rootNodeId,
              providerThreadId: null,
              providerTurnId: null,
              nativeItemRef: null,
              parentItemId: null,
              ordinal: 2,
              status: "failed",
              title: "Usage limit reached",
              startedAt: now,
              completedAt: now,
              updatedAt: now,
              failure: {
                class: "usage_limit",
                message: "Plan limit reached.",
                code: "usageLimitExceeded",
                retryable: null,
                resetAt,
              },
            },
          },
        ],
      });
      if (scenario === "queued-resume") {
        const queuedRun = projection.runs[1]!;
        yield* events.write({
          events: [
            {
              id: EventId.make("recovery:held:queued-resume"),
              type: "run.updated",
              threadId,
              runId: queuedRun.id,
              occurredAt: now,
              payload: { ...queuedRun, queueHeld: true },
            },
          ],
        });
        const resumeHeldQueue = yield* orchestrator
          .dispatch({
            type: "queue.resume",
            commandId: CommandId.make("recovery:resume-held:queued-resume"),
            threadId,
          })
          .pipe(Effect.exit);
        assert.equal(resumeHeldQueue._tag, "Failure");
        assert.isTrue((yield* orchestrator.getThreadProjection(threadId)).runs[1]?.queueHeld);
      }
      const shell = (yield* orchestrator.getShellSnapshot()).threads.find(
        (thread) => thread.id === threadId,
      )!;
      assert.isNull(limitRecoveryCommand(shell, false, DateTime.toEpochMillis(now)));
      if (scenario === "invalid-snooze") {
        const result = yield* orchestrator
          .dispatch({
            type: "thread.metadata.update",
            commandId: CommandId.make("recovery:invalid-snooze"),
            threadId,
            limitRecovery: { runId: run.id, resetAt, snooze: true },
          })
          .pipe(Effect.exit);
        assert.equal(result._tag, "Failure");
        const current = yield* orchestrator.getThreadProjection(threadId);
        assert.isNull(current.thread.limitRecovery ?? null);
        assert.isNull(current.thread.snoozedUntil);
        return;
      }
      const snooze = [
        "snooze-only",
        "manual-snooze-after-recovery",
        "snooze-resume",
        "wake",
        "cancel-resume-keep-snooze",
        "wake-preserve-resume",
      ].includes(scenario);
      const autoResume = scenario !== "snooze-only" && scenario !== "wake";
      const arm = limitRecoveryCommand(shell, autoResume, DateTime.toEpochMillis(now), snooze);
      assert.isNotNull(arm);
      yield* orchestrator.dispatch(arm!);
      let armedShell = (yield* orchestrator.getShellSnapshot()).threads.find(
        (thread) => thread.id === threadId,
      )!;
      assert.deepEqual(armedShell.limitRecovery, {
        runId: run.id,
        resetAt,
        autoResume,
        snooze,
        requestId: arm!.commandId,
      });
      if (snooze)
        assert.equal(DateTime.toEpochMillis(armedShell.snoozedUntil!), Date.parse(resetAt));
      if (scenario === "cancel-resume-keep-snooze" || scenario === "wake-preserve-resume") {
        yield* TestClock.adjust("10 seconds");
        yield* orchestrator.dispatch({
          type: "thread.metadata.update",
          commandId: CommandId.make(`recovery:independent-choice:${scenario}`),
          threadId,
          limitRecovery: {
            runId: run.id,
            resetAt,
            autoResume: scenario === "wake-preserve-resume",
            snooze: scenario === "cancel-resume-keep-snooze",
          },
        });
        armedShell = (yield* orchestrator.getShellSnapshot()).threads.find(
          (thread) => thread.id === threadId,
        )!;
        if (scenario === "cancel-resume-keep-snooze") {
          assert.equal(DateTime.toEpochMillis(armedShell.snoozedUntil!), Date.parse(resetAt));
          // Failed runtime timestamps advance with metadata. Acknowledging the
          // same failed run must not turn cancellation into an early wake.
          assert.equal(
            DateTime.toEpochMillis(armedShell.snoozedAt!),
            DateTime.toEpochMillis(armedShell.updatedAt),
          );
          assert.isFalse(armedShell.limitRecovery!.autoResume);
        } else {
          assert.isNull(armedShell.snoozedUntil);
          assert.isNull(armedShell.snoozedAt);
          assert.isTrue(armedShell.limitRecovery!.autoResume);
        }
      }
      if (scenario === "independent-patches") {
        yield* TestClock.adjust("10 seconds");
        yield* orchestrator.dispatch({
          type: "thread.metadata.update",
          commandId: CommandId.make("recovery:patch-snooze"),
          threadId,
          limitRecovery: { runId: run.id, resetAt, snooze: true },
        });
        let current = yield* orchestrator.getThreadProjection(threadId);
        assert.isTrue(current.thread.limitRecovery!.autoResume);
        assert.isTrue(current.thread.limitRecovery!.snooze);
        // This is also the payload an older auto-resume-only client sends.
        yield* TestClock.adjust("10 seconds");
        yield* orchestrator.dispatch({
          type: "thread.metadata.update",
          commandId: CommandId.make("recovery:patch-cancel-resume"),
          threadId,
          limitRecovery: { runId: run.id, resetAt, autoResume: false },
        });
        current = yield* orchestrator.getThreadProjection(threadId);
        assert.isFalse(current.thread.limitRecovery!.autoResume);
        assert.isTrue(current.thread.limitRecovery!.snooze);
        assert.equal(DateTime.toEpochMillis(current.thread.snoozedUntil!), Date.parse(resetAt));
        assert.equal(
          DateTime.toEpochMillis(current.thread.snoozedAt!),
          DateTime.toEpochMillis(current.thread.updatedAt),
        );
        yield* orchestrator.dispatch({
          type: "thread.metadata.update",
          commandId: CommandId.make("recovery:patch-resume"),
          threadId,
          limitRecovery: { runId: run.id, resetAt, autoResume: true },
        });
        armedShell = (yield* orchestrator.getShellSnapshot()).threads.find(
          (thread) => thread.id === threadId,
        )!;
        assert.isTrue(armedShell.limitRecovery!.autoResume);
        assert.isTrue(armedShell.limitRecovery!.snooze);
      }
      if (scenario === "manual-snooze" || scenario === "manual-snooze-after-recovery") {
        yield* orchestrator.dispatch({
          type: "thread.snooze",
          commandId: CommandId.make(`recovery:manual-snooze:${scenario}`),
          threadId,
          snoozedUntil: resetAt,
        });
        yield* orchestrator.dispatch({
          type: "thread.metadata.update",
          commandId: CommandId.make(`recovery:manual-cancel:${scenario}`),
          threadId,
          limitRecovery: { runId: run.id, resetAt, autoResume: false, snooze: false },
        });
        assert.equal(
          DateTime.toEpochMillis(
            (yield* orchestrator.getThreadProjection(threadId)).thread.snoozedUntil!,
          ),
          Date.parse(resetAt),
        );
        yield* orchestrator.dispatch({
          type: "thread.unsnooze",
          commandId: CommandId.make(`recovery:manual-wake:${scenario}`),
          threadId,
          reason: "user",
        });
        assert.isNull((yield* orchestrator.getThreadProjection(threadId)).thread.snoozedUntil);
      }
      if (scenario === "wake") {
        yield* orchestrator.dispatch({
          type: "thread.metadata.update",
          commandId: CommandId.make(`recovery:wake:${scenario}`),
          threadId,
          limitRecovery: { runId: run.id, resetAt, autoResume: false, snooze: false },
        });
        assert.isNull((yield* orchestrator.getThreadProjection(threadId)).thread.snoozedUntil);
      }
      assert.isNull(limitRecoveryCommand(armedShell, true, DateTime.toEpochMillis(now)));
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make(`recovery:early:${scenario}`),
        messageId: MessageId.make(`recovery:early:${scenario}`),
        threadId,
        usageLimitContinuationOfRunId: run.id,
        text: "Continue where you left off.",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
        createdBy: "user",
        creationSource: "server",
      });
      assert.lengthOf(
        (yield* orchestrator.getThreadProjection(threadId)).runs,
        scenario === "queued-resume" ? 2 : 1,
      );
      yield* TestClock.adjust("1 minute");
      const resume = limitRecoveryCommand(
        armedShell,
        true,
        DateTime.toEpochMillis(yield* DateTime.now),
      );
      if (autoResume && scenario !== "cancel-resume-keep-snooze") assert.isNotNull(resume);
      else assert.isNull(resume);
      if (scenario === "snooze-race") {
        const wakeAt = DateTime.formatIso(DateTime.add(yield* DateTime.now, { minutes: 1 }));
        yield* orchestrator.dispatch({
          type: "thread.snooze",
          commandId: CommandId.make("recovery:raced-snooze"),
          threadId,
          snoozedUntil: wakeAt,
        });
        yield* orchestrator.dispatch(resume!);
        assert.lengthOf((yield* orchestrator.getThreadProjection(threadId)).runs, 1);
        yield* TestClock.adjust("1 minute");
        const current = (yield* orchestrator.getShellSnapshot()).threads.find(
          (thread) => thread.id === threadId,
        )!;
        const freshResume = limitRecoveryCommand(
          current,
          true,
          DateTime.toEpochMillis(yield* DateTime.now),
        );
        assert.isNotNull(freshResume);
        assert.notEqual(freshResume!.commandId, resume!.commandId);
        yield* orchestrator.dispatch(freshResume!);
        yield* orchestrator.dispatch(freshResume!);
        assert.lengthOf((yield* orchestrator.getThreadProjection(threadId)).runs, 2);
      }
      if (scenario === "expired-snooze") {
        const staleSnooze = yield* orchestrator
          .dispatch({
            type: "thread.metadata.update",
            commandId: CommandId.make("recovery:expired-snooze"),
            threadId,
            limitRecovery: { runId: run.id, resetAt, snooze: true },
          })
          .pipe(Effect.exit);
        assert.equal(staleSnooze._tag, "Failure");
        const current = yield* orchestrator.getThreadProjection(threadId);
        assert.isNull(current.thread.snoozedUntil);
        assert.isFalse(current.thread.limitRecovery!.snooze);
        assert.isTrue(current.thread.limitRecovery!.autoResume);
      }

      if (scenario === "cancel" || scenario === "rearm")
        yield* orchestrator.dispatch({
          type: "thread.metadata.update",
          commandId: CommandId.make(`recovery:cancel:${scenario}`),
          threadId,
          limitRecovery: { runId: run.id, resetAt, autoResume: false },
        });
      if (scenario === "archive")
        yield* orchestrator.dispatch({
          type: "thread.archive",
          commandId: CommandId.make(`recovery:archive:${scenario}`),
          threadId,
        });
      if (scenario === "new-message")
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(`recovery:new-message:${scenario}`),
          threadId,
          messageId: MessageId.make(`recovery:new-message:${scenario}`),
          text: "I will continue manually.",
          attachments: [],
          dispatchMode: { type: "defer_start" },
          createdBy: "user",
          creationSource: "web",
        });
      if (scenario === "settle")
        yield* orchestrator.dispatch({
          type: "thread.settle",
          commandId: CommandId.make(`recovery:settle:${scenario}`),
          threadId,
        });
      if (scenario === "replacement") {
        const current = yield* orchestrator.getThreadProjection(threadId);
        const error = current.turnItems.find((item) => item.type === "error")!;
        if (error.type !== "error") throw new Error("Expected provider error");
        yield* events.write({
          commandId: CommandId.make(`recovery:replacement:${scenario}`),
          events: [
            {
              id: EventId.make(`recovery:replacement:${scenario}`),
              type: "turn-item.updated",
              threadId,
              occurredAt: yield* DateTime.now,
              payload: {
                ...error,
                failure: {
                  ...error.failure,
                  class: "provider_error",
                  message: "A replacement failure.",
                },
              },
            },
          ],
        });
      }
      if (scenario === "rearm") {
        yield* orchestrator.dispatch(resume!);
        yield* orchestrator.dispatch({
          type: "thread.metadata.update",
          commandId: CommandId.make(`recovery:rearm:${scenario}`),
          threadId,
          limitRecovery: { runId: run.id, resetAt, autoResume: true },
        });
        yield* orchestrator.dispatch(resume!);
        assert.lengthOf((yield* orchestrator.getThreadProjection(threadId)).runs, 1);
        const rearmedShell = (yield* orchestrator.getShellSnapshot()).threads.find(
          (thread) => thread.id === threadId,
        )!;
        const freshResume = limitRecoveryCommand(
          rearmedShell,
          true,
          DateTime.toEpochMillis(yield* DateTime.now),
        );
        assert.isNotNull(freshResume);
        assert.notEqual(freshResume!.commandId, resume!.commandId);
        yield* orchestrator.dispatch(freshResume!);
        yield* orchestrator.dispatch(freshResume!);
        assert.lengthOf((yield* orchestrator.getThreadProjection(threadId)).runs, 2);
      }
      const before = yield* orchestrator.getThreadProjection(threadId);
      if (resume !== null) {
        yield* orchestrator.dispatch(resume);
        yield* orchestrator.dispatch(resume);
      }
      const after = yield* orchestrator.getThreadProjection(threadId);
      assert.lengthOf(
        after.runs,
        before.runs.length +
          (scenario === "resume" ||
          scenario === "queued-resume" ||
          scenario === "snooze-resume" ||
          scenario === "wake-preserve-resume" ||
          scenario === "independent-patches" ||
          scenario === "expired-snooze"
            ? 1
            : 0),
      );
      assert.lengthOf(
        after.messages,
        before.messages.length +
          (scenario === "resume" ||
          scenario === "queued-resume" ||
          scenario === "snooze-resume" ||
          scenario === "wake-preserve-resume" ||
          scenario === "independent-patches" ||
          scenario === "expired-snooze"
            ? 1
            : 0),
      );
      if (scenario === "queued-resume") {
        assert.equal(after.runs[1]?.status, "queued");
        assert.isTrue(after.runs[1]?.queueHeld);
        const continuation = after.runs[2]!;
        const completedAt = yield* DateTime.now;
        yield* events.write({
          events: [
            {
              id: EventId.make("recovery:continuation-completed:queued-resume"),
              type: "run.updated",
              threadId,
              runId: continuation.id,
              occurredAt: completedAt,
              payload: { ...continuation, status: "completed", completedAt },
            },
          ],
        });
        yield* orchestrator.dispatch({
          type: "queue.resume",
          commandId: CommandId.make("recovery:resume-held-after-limit:queued-resume"),
          threadId,
        });
        const resumed = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(resumed.runs[1]?.status, "starting");
        assert.isFalse(resumed.runs[1]?.queueHeld);
      }
    }),
  );
});

class LegacyGuardPty implements PtyAdapter.PtyProcess {
  readonly pid: number;
  constructor(pid: number) {
    this.pid = pid;
  }
  readonly writes: string[] = [];
  readonly kills: (string | undefined)[] = [];
  readonly exits = new Set<(event: PtyAdapter.PtyExitEvent) => void>();
  write(data: string) {
    this.writes.push(data);
  }
  resize() {}
  kill(signal?: string) {
    this.kills.push(signal);
    for (const exit of this.exits) exit({ exitCode: 0, signal: 15 });
  }
  onData(_callback: (data: string) => void) {
    return () => {};
  }
  onExit(callback: (event: PtyAdapter.PtyExitEvent) => void) {
    this.exits.add(callback);
    return () => {
      this.exits.delete(callback);
    };
  }
}
class LegacyGuardTerminalFixture extends Context.Service<
  LegacyGuardTerminalFixture,
  {
    readonly manager: TerminalManager.TerminalManager["Service"];
    readonly baseDir: string;
    readonly processes: LegacyGuardPty[];
  }
>()("t3/orchestration-v2/runtimeLayer.test/LegacyGuardTerminalFixture") {}
const LegacyGuardTerminalFixtureLayer = Layer.effect(
  LegacyGuardTerminalFixture,
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "legacy-guard-owner-" });
    const processes: LegacyGuardPty[] = [];
    const manager = yield* TerminalManager.makeWithOptions({
      logsDir: `${baseDir}/terminals`,
      shellResolver: () => "/bin/sh",
      env: {},
      processKillGraceMs: 1,
      processTable: Effect.succeed([]),
      subprocessInspector: () =>
        Effect.succeed({ hasRunningSubprocess: false, childCommand: null, processIds: [] }),
      ptyAdapter: {
        spawn: () =>
          Effect.sync(() => {
            const process = new LegacyGuardPty(91001 + processes.length);
            processes.push(process);
            return process;
          }),
      },
    });
    return { manager, baseDir, processes };
  }),
).pipe(Layer.provide(ProcessRunner.layer), Layer.provide(PlatformTestLayer));
const LegacyGuardTerminalOwnerLayer = Layer.effect(
  TerminalManager.TerminalManager,
  Effect.map(LegacyGuardTerminalFixture, (fixture) => fixture.manager),
).pipe(Layer.provideMerge(LegacyGuardTerminalFixtureLayer));
const LegacyGuardOwnerTestLayer = ThreadManagement.layer.pipe(
  Layer.provideMerge(TestLayer),
  Layer.provideMerge(LegacyGuardTerminalOwnerLayer),
  Layer.provideMerge(SqlitePersistenceMemory),
);
const legacyGuardOwnerFixture = Effect.fn("legacyGuardOwnerFixture")(function* (
  name: string,
  mode: "started" | "no_script" | "opted_out" = "started",
) {
  const owner = yield* LegacyGuardTerminalFixture;
  const engine = yield* Orchestrator.OrchestratorV2;
  const management = yield* ThreadManagement.ThreadManagementService;
  const sink = yield* EventSink.EventSinkV2;
  const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
  const query = yield* makeCommandObservationQuery();
  const threadId = ThreadId.make(`guard-owner:${name}`);
  const projectId = ProjectId.make(`guard-owner-project:${name}`);
  const releaseCommandId = CommandId.make(`guard-owner-C:${name}`);
  const messageId = MessageId.make(`guard-owner-M:${name}`);
  yield* seedProject({
    projectId,
    title: name,
    workspaceRoot: owner.baseDir,
    defaultModelSelection: modelSelection,
    createdAt: "2026-10-01T00:00:00.000Z",
  });
  const observed = yield* query.observe({ threadId, commandId: releaseCommandId, messageId });
  const createCommandId = legacyBootstrapCreateCommandId(threadId, releaseCommandId);
  const policy = {
    version: 1 as const,
    threadId,
    projectId,
    messageId,
    createCommandId,
    birthCommandId: CommandId.make(`${createCommandId}:initial-message`),
    releaseCommandId,
    payloadHash: `synthetic-owner:${name}`,
    ownsNewThread: true,
    dispatchGuard: {
      observedSnapshotSequence: observed.snapshotSequence,
      expectedModelSelection: modelSelection,
      expectedSessionStatus: null,
      expectedActiveTurnId: null,
      expectedLatestTurnId: null,
      requireIdle: true as const,
    },
  };
  yield* engine.dispatch({
    type: "thread.create",
    commandId: createCommandId,
    threadId,
    projectId,
    title: name,
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    createdBy: "user",
    creationSource: "web",
    legacyBootstrap: policy,
  });
  yield* engine.dispatch({
    type: "message.dispatch",
    commandId: policy.birthCommandId,
    threadId,
    messageId,
    text: "Synthetic owner fixture",
    attachments: [],
    createdBy: "user",
    creationSource: "web",
    dispatchMode: {
      type: "defer_start",
      workspaceStrategy: { type: "root" },
      ...(mode === "opted_out" ? { runSetupScript: false } : {}),
    },
    legacyBootstrap: policy,
  });
  const born = (yield* engine.getThreadProjection(threadId)).runs[0]!;
  const receivingPolicy = { ...policy, runId: born.id };
  const collect = (commandId: CommandId) =>
    sink.readByCommandId({ commandId }).pipe(
      Stream.runCollect,
      Effect.map((events) => Array.from(events)),
    );
  const proof = legacyBootstrapBirth({
    policy: receivingPolicy,
    claimEvents: yield* collect(createCommandId),
    birthEvents: yield* collect(policy.birthCommandId),
  });
  if (proof.type !== "valid") return yield* Effect.die("Missing authentic owner fixture birth");
  const claim = yield* receipts.getByCommandId(createCommandId);
  const birth = yield* receipts.getByCommandId(policy.birthCommandId);
  if (Option.isNone(claim) || Option.isNone(birth))
    return yield* Effect.die("Missing authentic owner fixture receipts");
  const generation = legacyPreparationGeneration({
    runId: born.id,
    birthEventId: proof.birthEventId,
    birthSequence: proof.sequence,
  });
  const preparation: LegacyPreparation = {
    version: 1,
    policy: receivingPolicy,
    generation,
    projectWorkspaceRoot: owner.baseDir,
    commonDirectory: null,
    claimEventId: proof.claimEventId,
    claimSequence: proof.claimSequence,
    claimReceiptSequence: claim.value.resultSequence,
    birthEventId: proof.birthEventId,
    birthSequence: proof.sequence,
    birthReceiptSequence: birth.value.resultSequence,
    setup: mode === "opted_out" ? { status: "opted_out" } : { status: "unresolved" },
    steps: [],
  };
  const progress = (commandId: CommandId, update: LegacyPreparationUpdate) =>
    engine
      .dispatch({
        type: "prepared-run.progress",
        commandId,
        threadId,
        runId: born.id,
        phase: "setup",
        legacyPreparationUpdate: update,
      })
      .pipe(Effect.asVoid);
  yield* progress(CommandId.make(`${createCommandId}:preparation:${generation}:initialize`), {
    type: "initialize",
    preparation,
  });
  if (mode !== "started") {
    if (mode === "no_script")
      yield* progress(CommandId.make(`${createCommandId}:preparation:${generation}:setup-policy`), {
        type: "setup-policy",
        setup: { status: "no_script" },
      });
    const noControl = yield* Schema.decodeUnknownEffect(LegacyNoTerminalControl)({
      version: 1,
      type: "no_control",
      policy: receivingPolicy,
      runId: born.id,
      threadId,
      claimEventId: proof.claimEventId,
      claimSequence: proof.claimSequence,
      claimReceiptSequence: claim.value.resultSequence,
      birthEventId: proof.birthEventId,
      birthSequence: proof.sequence,
      birthReceiptSequence: birth.value.resultSequence,
      preparationGeneration: generation,
      workspacePath: owner.baseDir,
      projectWorkspaceRoot: owner.baseDir,
    });
    const command: LegacyGuardRejectionDeleteCommand = {
      type: "legacy-bootstrap.guard-rejection-delete",
      commandId: CommandId.make(`${createCommandId}:guard-rejection-delete`),
      threadId,
      runId: born.id,
      legacyBootstrap: receivingPolicy,
      legacyNoControl: noControl,
    };
    const dispatchD = management.dispatchLegacyGuardRejectionDelete;
    if (dispatchD === undefined) return yield* Effect.die("Production private D route is missing");
    return {
      ...owner,
      ownedProcess: undefined,
      engine,
      query,
      receipts,
      sink,
      threadId,
      policy: receivingPolicy,
      binding: undefined,
      command,
      write: () => Effect.void,
      reject: () =>
        engine
          .dispatch({
            type: "prepared-run.release",
            commandId: releaseCommandId,
            threadId,
            runId: born.id,
            legacyBootstrap: receivingPolicy,
          })
          .pipe(Effect.flip),
      dispatchD,
    };
  }
  const terminalId = `legacy-setup:${generation}`;
  const controlGeneration = legacyPayloadHash(
    canonicalLegacyPayload({ preparationGeneration: generation, terminalId }),
  );
  const binding = yield* Schema.decodeUnknownEffect(LegacyOwnedTerminalControl)({
    version: 1,
    policy: receivingPolicy,
    runId: born.id,
    threadId,
    claimEventId: proof.claimEventId,
    claimSequence: proof.claimSequence,
    claimReceiptSequence: claim.value.resultSequence,
    birthEventId: proof.birthEventId,
    birthSequence: proof.sequence,
    birthReceiptSequence: birth.value.resultSequence,
    preparationGeneration: generation,
    terminalId,
    generation: controlGeneration,
  });
  const step = (effect: LegacyPreparation["steps"][number]["effect"]) => {
    const effectId = legacyPreparationEffectId({ generation, effect });
    const commandId = CommandId.make(
      `${createCommandId}:preparation:${generation}:${effectId}:intent`,
    );
    return {
      effectId,
      effect,
      inputHash: legacyPayloadHash(canonicalLegacyPayload(effect)),
      intentCommandId: commandId,
      intentEventId: EventId.make(`${commandId}:event`),
      state: "intent" as const,
    };
  };
  const outcome = (
    intent: ReturnType<typeof step>,
    evidence: NonNullable<LegacyPreparation["steps"][number]["evidence"]>,
    state: "known_succeeded" | "known_started",
  ) => {
    const commandId = CommandId.make(
      `${createCommandId}:preparation:${generation}:${intent.effectId}:outcome`,
    );
    return progress(commandId, {
      type: "outcome",
      step: {
        ...intent,
        state,
        evidence,
        outcomeCommandId: commandId,
        outcomeEventId: EventId.make(`${commandId}:event`),
      },
    });
  };
  const commandLine = "synthetic bytes only\r";
  let openIntent: ReturnType<typeof step> | undefined;
  yield* owner.manager.open(
    { threadId, terminalId, cwd: owner.baseDir, cols: 80, rows: 24 },
    {
      binding,
      beforeSpawn: (plan) =>
        Effect.gen(function* () {
          const script = {
            id: "synthetic-setup",
            name: "Synthetic setup",
            command: "synthetic bytes only",
            async: true,
            runOnWorktreeCreate: false,
          };
          const definition = {
            ...script,
            definitionHash: legacyPayloadHash(canonicalLegacyPayload(script)),
            projectCwd: owner.baseDir,
            cwd: plan.cwd,
            terminalId,
            generation: controlGeneration,
            shell: plan.shell,
            shellArgs: plan.shellArgs,
            commandLine,
            completionToken: null,
            env: {
              T3CODE_PROJECT_ROOT: owner.baseDir,
              COLORTERM: "" as const,
              NO_COLOR: "1" as const,
              FORCE_COLOR: "0" as const,
            },
          };
          yield* progress(
            CommandId.make(`${createCommandId}:preparation:${generation}:setup-policy`),
            { type: "setup-policy", setup: { status: "resolved", definition } },
          );
          openIntent = step({ kind: "setup.open", input: definition });
          yield* progress(openIntent.intentCommandId, { type: "intent", step: openIntent });
        }),
      afterSpawn: (spawn) =>
        openIntent === undefined
          ? Effect.die("Spawn without owner intent")
          : outcome(
              openIntent,
              {
                type: "terminal_generation",
                terminalId,
                generation: controlGeneration,
                shell: spawn.shell,
                shellArgs: spawn.shellArgs,
              },
              "known_succeeded",
            ),
    },
  );
  const writeIntent = step({
    kind: "setup.write",
    input: {
      terminalId,
      generation: controlGeneration,
      commandLine,
      completionToken: null,
      definitionHash: legacyPayloadHash(
        canonicalLegacyPayload({
          id: "synthetic-setup",
          name: "Synthetic setup",
          command: "synthetic bytes only",
          async: true,
          runOnWorktreeCreate: false,
        }),
      ),
    },
  });
  const write = (before: Effect.Effect<void> = Effect.void) =>
    owner.manager.write(
      { threadId, terminalId, data: commandLine },
      {
        legacyOwnedControl: binding,
        beforeWrite: () =>
          progress(writeIntent.intentCommandId, { type: "intent", step: writeIntent }).pipe(
            Effect.andThen(before),
          ),
        afterWrite: (status, inputCount) =>
          status !== "accepted"
            ? Effect.die("Synthetic PTY refused")
            : outcome(
                writeIntent,
                { type: "terminal_write", terminalId, generation: controlGeneration, inputCount },
                "known_started",
              ),
      },
    );
  const reject = () =>
    engine
      .dispatch({
        type: "prepared-run.release",
        commandId: releaseCommandId,
        threadId,
        runId: born.id,
        legacyBootstrap: receivingPolicy,
      })
      .pipe(Effect.flip);
  const command: LegacyGuardRejectionDeleteCommand = {
    type: "legacy-bootstrap.guard-rejection-delete",
    commandId: CommandId.make(`${createCommandId}:guard-rejection-delete`),
    threadId,
    runId: born.id,
    legacyBootstrap: receivingPolicy,
    legacyOwnedControl: binding,
  };
  const dispatchD = management.dispatchLegacyGuardRejectionDelete;
  if (dispatchD === undefined) return yield* Effect.die("Production private D route is missing");
  return {
    ...owner,
    ownedProcess: owner.processes.at(-1)!,
    engine,
    query,
    receipts,
    sink,
    threadId,
    policy: receivingPolicy,
    binding,
    command,
    write,
    reject,
    dispatchD,
  };
});
it.layer(LegacyGuardOwnerTestLayer, { excludeTestServices: true })(
  "Legacy guard cleanup actual owner lock",
  (it) => {
    it.effect.each((["no_script", "opted_out"] as const).map((mode) => ({ mode })))(
      "private no-control D persists authentic ordered proof for $mode without terminal effects",
      ({ mode }) =>
        Effect.gen(function* () {
          const f = yield* legacyGuardOwnerFixture(`absence:${mode}`, mode);
          const count = f.processes.length;
          yield* f.reject();
          const original = (yield* f.engine.getThreadProjection(f.threadId)).runs[0]!
            .legacyReleaseDecision;
          const result = yield* f.dispatchD(f.command);
          const events = Array.from(
            yield* f.sink
              .readByCommandId({ commandId: f.command.commandId })
              .pipe(Stream.runCollect),
          );
          assert.deepEqual(
            events.map((stored) => stored.event.type),
            ["run.updated", "thread.deleted"],
          );
          assert.equal(events[1]!.sequence, result.sequence);
          assert.equal(events[0]!.sequence + 1, result.sequence);
          const run = (yield* f.engine.getThreadProjection(f.threadId)).runs[0]!;
          const publicReplay = Array.from(
            yield* f.engine
              .streamStoredEventsFrom({
                threadId: f.threadId,
                afterSequence: events[0]!.sequence - 1,
              })
              .pipe(Stream.take(2), Stream.runCollect),
          );
          assert.deepEqual(
            publicReplay.map((stored) => stored.sequence),
            events.map((stored) => stored.sequence),
          );
          assert.equal(publicReplay[0]!.event.type, "run.updated");
          for (const stored of publicReplay)
            assert.notProperty(stored.event.payload, "legacyReleaseDecision");
          for (const stored of publicReplay)
            assert.notProperty(stored.event.payload, "legacyPreparation");
          assert.equal(run.legacyReleaseDecision?.deletion?.type, "no_control");
          const { deletion, ...unchanged } = run.legacyReleaseDecision!;
          assert.deepEqual(unchanged, original);
          assert.equal(deletion?.evidenceEventId, events[0]!.event.id);
          const outbox = yield* EffectOutbox.EffectOutboxV2;
          assert.isEmpty(
            (yield* outbox.listByThreadId(f.threadId)).filter(
              (effect) =>
                effect.commandId === f.command.commandId &&
                effect.request.type === "terminal.cleanup",
            ),
          );
          assert.equal(f.processes.length, count);
          assert.equal((yield* f.dispatchD(f.command)).sequence, result.sequence);
          const store = yield* EventStore.EventStoreV2;
          const history = Array.from(
            yield* store.read({ threadId: f.threadId }).pipe(Stream.runCollect),
          );
          const memory = yield* Effect.gen(function* () {
            return yield* ProjectionStore.ProjectionStoreV2;
          }).pipe(Effect.provide(Layer.fresh(ProjectionStore.layerMemory)));
          for (const stored of history) yield* memory.apply(stored.event);
          assert.deepEqual(
            (yield* memory.getThreadProjection(f.threadId)).runs[0]!.legacyReleaseDecision,
            run.legacyReleaseDecision,
          );
          const { legacyReleaseDecision: _decision, ...publicRun } = run;
          const stale = {
            type: "run.updated" as const,
            id: EventId.make(`${f.threadId}:stale-public`),
            threadId: f.threadId,
            runId: run.id,
            occurredAt: yield* DateTime.now,
            payload: publicRun,
          };
          yield* memory.apply(stale);
          yield* f.sink.write({ events: [stale] });
          assert.deepEqual(
            (yield* memory.getThreadProjection(f.threadId)).runs[0]!.legacyReleaseDecision,
            run.legacyReleaseDecision,
          );
          assert.deepEqual(
            (yield* f.engine.getThreadProjection(f.threadId)).runs[0]!.legacyReleaseDecision,
            run.legacyReleaseDecision,
          );
          const tampered = {
            ...stale,
            id: EventId.make(`${f.threadId}:tampered`),
            payload: {
              ...run,
              legacyReleaseDecision: {
                ...run.legacyReleaseDecision!,
                deletion: { ...deletion!, workspacePath: `${deletion!.workspacePath}/replaced` },
              },
            },
          };
          assert.equal((yield* memory.apply(tampered).pipe(Effect.result))._tag, "Failure");
          assert.equal(
            (yield* f.sink.write({ events: [tampered] }).pipe(Effect.result))._tag,
            "Failure",
          );
          assert.deepEqual(
            (yield* f.engine.getThreadProjection(f.threadId)).runs[0]!.legacyReleaseDecision,
            run.legacyReleaseDecision,
          );
          const sql = yield* SqlClient.SqlClient;
          const dbPath = `${f.baseDir}/D-${mode}.sqlite`;
          yield* sql`VACUUM INTO ${dbPath}`;
          const reopenedLayers = ProjectionMaintenance.layer.pipe(
            Layer.provideMerge(
              Layer.mergeAll(EventStore.layer, ProjectionStore.layer, CommandReceiptStore.layer),
            ),
            Layer.provideMerge(makeSqlitePersistenceLive(dbPath)),
            Layer.provide(PlatformTestLayer),
          );
          const readReopened = Effect.gen(function* () {
            const projection = yield* ProjectionStore.ProjectionStoreV2;
            const reopenedStore = yield* EventStore.EventStoreV2;
            const reopenedReceipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
            assert.deepEqual(
              (yield* projection.getThreadProjection(f.threadId)).runs[0]!.legacyReleaseDecision,
              run.legacyReleaseDecision,
            );
            const raw = Array.from(
              yield* reopenedStore
                .readByCommandId({ commandId: f.command.commandId })
                .pipe(Stream.runCollect),
            );
            assert.deepEqual(
              raw.map((stored) => stored.event.type),
              ["run.updated", "thread.deleted"],
            );
            const receipt = yield* reopenedReceipts.getByCommandId(f.command.commandId);
            assert.isTrue(Option.isSome(receipt));
            if (Option.isSome(receipt)) assert.equal(receipt.value.resultSequence, result.sequence);
            const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
            assert.isTrue((yield* maintenance.rebuild).valid);
            assert.deepEqual(
              (yield* projection.getThreadProjection(f.threadId)).runs[0]!.legacyReleaseDecision,
              run.legacyReleaseDecision,
            );
          });
          yield* Effect.scoped(readReopened.pipe(Effect.provide(Layer.fresh(reopenedLayers))));
          yield* Effect.scoped(readReopened.pipe(Effect.provide(Layer.fresh(reopenedLayers))));
          const input = f.command.legacyNoControl!;
          for (const changed of [
            { ...input, workspacePath: `${input.workspacePath}/other` },
            { ...input, preparationGeneration: "other" },
            { ...input, birthEventId: EventId.make("other-birth") },
            { ...input, policy: { ...input.policy, payloadHash: "other" } },
          ]) {
            assert.equal(
              (yield* f
                .dispatchD({
                  type: f.command.type,
                  commandId: f.command.commandId,
                  threadId: f.threadId,
                  runId: f.command.runId,
                  legacyBootstrap: f.policy,
                  legacyNoControl: changed,
                })
                .pipe(Effect.result))._tag,
              "Failure",
            );
          }
          const observed = yield* f.query.observe({
            threadId: f.threadId,
            commandId: f.policy.releaseCommandId,
            messageId: f.policy.messageId,
          });
          assert.equal(observed.commandStatus, "rejected");
          assert.isNull(observed.turn);
        }),
    );
    it.effect(
      "private no-control D refuses any actual control and never falls back from missing bound proof",
      () =>
        Effect.gen(function* () {
          const f = yield* legacyGuardOwnerFixture("absence-refusal", "no_script");
          yield* f.reject();
          yield* f.manager.open({
            threadId: f.threadId,
            terminalId: "unrelated",
            cwd: f.baseDir,
            cols: 80,
            rows: 24,
          });
          const process = f.processes.at(-1)!;
          assert.equal((yield* f.dispatchD(f.command).pipe(Effect.result))._tag, "Failure");
          assert.isTrue(Option.isNone(yield* f.receipts.getByCommandId(f.command.commandId)));
          assert.isNull((yield* f.engine.getThreadProjection(f.threadId)).thread.deletedAt);
          assert.deepEqual(process.kills, []);
          assert.deepEqual(process.writes, []);
        }),
    );
    it.effect(
      "private no-control D precommit refusal leaves no receipt tombstone or projected deletion proof",
      () =>
        Effect.gen(function* () {
          const f = yield* legacyGuardOwnerFixture("absence-precommit", "no_script");
          yield* f.reject();
          const commit = f.sink.commitCommand;
          const spy = vi.spyOn(f.sink, "commitCommand").mockImplementation((input) =>
            input.commandId === f.command.commandId
              ? Effect.fail(
                  new EventSink.EventSinkWriteError({
                    eventCount: input.events.length,
                    commandId: input.commandId,
                    cause: "Synthetic precommit refusal",
                  }),
                )
              : commit(input),
          );
          yield* Effect.gen(function* () {
            assert.equal((yield* f.dispatchD(f.command).pipe(Effect.result))._tag, "Failure");
            assert.isTrue(Option.isNone(yield* f.receipts.getByCommandId(f.command.commandId)));
            assert.isEmpty(
              Array.from(
                yield* f.sink
                  .readByCommandId({ commandId: f.command.commandId })
                  .pipe(Stream.runCollect),
              ),
            );
            const projection = yield* f.engine.getThreadProjection(f.threadId);
            assert.isNull(projection.thread.deletedAt);
            assert.isUndefined(projection.runs[0]!.legacyReleaseDecision?.deletion);
          }).pipe(Effect.ensuring(Effect.sync(() => spy.mockRestore())));
        }),
    );
    it.effect(
      "private no-control D rolls back recorded proof receipt and tombstone together after projection refusal",
      () =>
        Effect.gen(function* () {
          const f = yield* legacyGuardOwnerFixture("absence-atomic", "no_script");
          yield* f.reject();
          const sql = yield* SqlClient.SqlClient;
          yield* sql`CREATE TEMP TRIGGER legacy_d_test_refusal BEFORE UPDATE ON orchestration_v2_projection_threads
      WHEN NEW.deleted_at IS NOT NULL BEGIN SELECT RAISE(ABORT, 'synthetic D refusal'); END`;
          yield* Effect.gen(function* () {
            assert.equal((yield* f.dispatchD(f.command).pipe(Effect.result))._tag, "Failure");
            assert.isTrue(Option.isNone(yield* f.receipts.getByCommandId(f.command.commandId)));
            assert.isEmpty(
              Array.from(
                yield* f.sink
                  .readByCommandId({ commandId: f.command.commandId })
                  .pipe(Stream.runCollect),
              ),
            );
            const projection = yield* f.engine.getThreadProjection(f.threadId);
            assert.isNull(projection.thread.deletedAt);
            assert.isUndefined(projection.runs[0]!.legacyReleaseDecision?.deletion);
            const outbox = yield* EffectOutbox.EffectOutboxV2;
            assert.isEmpty(
              (yield* outbox.listByThreadId(f.threadId)).filter(
                (effect) => effect.commandId === f.command.commandId,
              ),
            );
          }).pipe(
            Effect.ensuring(sql`DROP TRIGGER IF EXISTS legacy_d_test_refusal`.pipe(Effect.orDie)),
          );
          const result = yield* f.dispatchD(f.command);
          assert.equal((yield* f.dispatchD(f.command)).sequence, result.sequence);
        }),
    );
    it.effect(
      "private D refuses replaced and unavailable physical controls before any tombstone or outbox",
      () =>
        Effect.gen(function* () {
          const f = yield* legacyGuardOwnerFixture("replaced");
          if (f.binding === undefined || f.ownedProcess === undefined)
            return yield* Effect.die("Started owner fixture is unavailable");
          yield* f.write();
          yield* f.reject();
          yield* f.manager.restart({
            threadId: f.threadId,
            terminalId: f.binding.terminalId,
            cwd: f.baseDir,
            cols: 80,
            rows: 24,
          });
          const result = yield* f.dispatchD(f.command).pipe(Effect.result);
          assert.equal(result._tag, "Failure");
          assert.isTrue(Option.isNone(yield* f.receipts.getByCommandId(f.command.commandId)));
          assert.isNull((yield* f.engine.getThreadProjection(f.threadId)).thread.deletedAt);
          const outbox = yield* EffectOutbox.EffectOutboxV2;
          assert.isEmpty(
            (yield* outbox.listByThreadId(f.threadId)).filter(
              (effect) => effect.commandId === f.command.commandId,
            ),
          );
          const replacement = f.processes.at(-1)!;
          assert.deepEqual(replacement.writes, []);
          assert.deepEqual(replacement.kills, []);
          const observed = yield* f.query.observe({
            threadId: f.threadId,
            commandId: f.policy.releaseCommandId,
            messageId: f.policy.messageId,
          });
          assert.equal(observed.commandStatus, "rejected");
          assert.isNull(observed.turn);
        }),
    );
    it.effect(
      "private D holds owner before executor while write journals and fences replacement through commit readback",
      () =>
        Effect.gen(function* () {
          const f = yield* legacyGuardOwnerFixture("lock-order");
          if (f.binding === undefined || f.ownedProcess === undefined)
            return yield* Effect.die("Started owner fixture is unavailable");
          const insideWrite = yield* Deferred.make<void>();
          const finishWrite = yield* Deferred.make<void>();
          const writeFiber = yield* f
            .write(
              Deferred.succeed(insideWrite, undefined).pipe(
                Effect.andThen(Deferred.await(finishWrite)),
              ),
            )
            .pipe(Effect.forkChild({ startImmediately: true }));
          yield* Deferred.await(insideWrite);
          yield* f.reject();
          const dRequested = yield* Deferred.make<void>();
          const guard = f.manager.withLegacyOwnedControlGuard;
          if (guard === undefined) return yield* Effect.die("Production control guard is missing");
          const guardSpy = vi
            .spyOn(f.manager, "withLegacyOwnedControlGuard")
            .mockImplementation(
              <A, E, R>(binding: LegacyOwnedTerminalControl, body: Effect.Effect<A, E, R>) =>
                Deferred.succeed(dRequested, undefined).pipe(Effect.andThen(guard(binding, body))),
            );
          const dCommit = yield* Deferred.make<void>();
          const finishD = yield* Deferred.make<void>();
          const commit = f.sink.commitCommand;
          const commitSpy = vi
            .spyOn(f.sink, "commitCommand")
            .mockImplementation((input) =>
              input.commandId !== f.command.commandId
                ? commit(input)
                : Deferred.succeed(dCommit, undefined).pipe(
                    Effect.andThen(Deferred.await(finishD)),
                    Effect.andThen(commit(input)),
                  ),
            );
          yield* Effect.gen(function* () {
            const d = yield* f
              .dispatchD(f.command)
              .pipe(Effect.forkChild({ startImmediately: true }));
            yield* Deferred.await(dRequested);
            yield* Deferred.succeed(finishWrite, undefined);
            yield* Fiber.join(writeFiber);
            yield* Effect.raceFirst(
              Deferred.await(dCommit),
              Fiber.join(d).pipe(Effect.andThen(Effect.die("D did not reach authentic commit"))),
            );
            const replacementRequested = yield* Deferred.make<void>();
            const replacement = yield* Deferred.succeed(replacementRequested, undefined).pipe(
              Effect.andThen(
                f.manager.restart({
                  threadId: f.threadId,
                  terminalId: f.binding.terminalId,
                  cwd: f.baseDir,
                  cols: 80,
                  rows: 24,
                }),
              ),
              Effect.forkChild({ startImmediately: true }),
            );
            yield* Deferred.await(replacementRequested);
            yield* Effect.yieldNow;
            assert.deepEqual(f.ownedProcess.writes, ["synthetic bytes only\r"]);
            assert.deepEqual(f.ownedProcess.kills, []);
            yield* Deferred.succeed(finishD, undefined);
            const result = yield* Fiber.join(d);
            assert.isNotNull((yield* f.engine.getThreadProjection(f.threadId)).thread.deletedAt);
            const receipt = yield* f.receipts.getByCommandId(f.command.commandId);
            assert.isTrue(Option.isSome(receipt));
            if (Option.isSome(receipt)) assert.equal(receipt.value.resultSequence, result.sequence);
            yield* Fiber.join(replacement);
            assert.deepEqual(f.processes.at(-1)!.kills, []);
            const replay = yield* f.dispatchD(f.command);
            assert.equal(replay.sequence, result.sequence);
            const outbox = yield* EffectOutbox.EffectOutboxV2;
            const cleanup = (yield* outbox.listByThreadId(f.threadId)).filter(
              (effect) =>
                effect.commandId === f.command.commandId &&
                effect.request.type === "terminal.cleanup",
            );
            assert.lengthOf(cleanup, 1);
            if (cleanup[0]?.request.type === "terminal.cleanup")
              assert.deepEqual(cleanup[0].request.legacyOwnedControl, f.binding);
            const observed = yield* f.query.observe({
              threadId: f.threadId,
              commandId: f.policy.releaseCommandId,
              messageId: f.policy.messageId,
            });
            assert.equal(observed.commandStatus, "rejected");
            assert.isNull(observed.turn);
          }).pipe(
            Effect.ensuring(
              Effect.all([
                Deferred.succeed(finishWrite, undefined),
                Deferred.succeed(finishD, undefined),
                Effect.sync(() => {
                  guardSpy.mockRestore();
                  commitSpy.mockRestore();
                }),
              ]),
            ),
          );
        }),
    );
  },
);

const QueueGuardTestLayer = TestLayer.pipe(Layer.provideMerge(SqlitePersistenceMemory));
const queueGuardFixture = Effect.fn("queueGuardFixture")(function* (name: string) {
  const engine = yield* Orchestrator.OrchestratorV2;
  const query = yield* makeCommandObservationQuery();
  const threadId = ThreadId.make(`guard:${name}`);
  yield* seedProject({
    projectId: ProjectId.make(`guard:project:${name}`),
    title: "Guard fixture",
    workspaceRoot: "/repo",
    defaultModelSelection: modelSelection,
    createdAt: "2026-10-01T00:00:00.000Z",
  });
  yield* engine.dispatch({
    type: "thread.create",
    createdBy: "user",
    creationSource: "web",
    commandId: CommandId.make(`guard:create:${name}`),
    threadId,
    projectId: ProjectId.make(`guard:project:${name}`),
    title: "Guard fixture",
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
  });
  const commandId = CommandId.make(`guard:start:${name}`);
  const messageId = MessageId.make(`guard:message:${name}`);
  const observe = () => query.observe({ threadId, commandId, messageId });
  const initial = yield* observe();
  const guard: ThreadTurnDispatchGuard = {
    observedSnapshotSequence: initial.snapshotSequence,
    expectedModelSelection: modelSelection,
    expectedSessionStatus: null,
    expectedActiveTurnId: null,
    expectedLatestTurnId: null,
    requireIdle: true,
  };
  const command = {
    type: "message.dispatch",
    createdBy: "user",
    creationSource: "web",
    commandId,
    threadId,
    messageId,
    text: "Bounded queue task",
    attachments: [],
    dispatchMode: { type: "start_immediately" },
    dispatchGuard: guard,
  } satisfies OrchestrationV2Command;
  return { engine, query, threadId, initial, observe, command };
});
it.layer(QueueGuardTestLayer)("V2 queue dispatch guard and historical observation", (it) => {
  it.effect(
    "legacy release refuses absent preparation evidence without creating a provider start",
    () =>
      Effect.gen(function* () {
        const f = yield* queueGuardFixture("legacy-release-missing-ledger");
        const createCommandId = legacyBootstrapCreateCommandId(f.threadId, f.command.commandId);
        const policy = {
          version: 1 as const,
          createCommandId,
          birthCommandId: CommandId.make(`${createCommandId}:initial-message`),
          releaseCommandId: f.command.commandId,
          projectId: ProjectId.make("guard:project:legacy-release-missing-ledger"),
          threadId: f.threadId,
          messageId: f.command.messageId,
          payloadHash: "missing-ledger",
          ownsNewThread: false,
        };
        yield* f.engine.dispatch({
          type: "thread.metadata.update",
          commandId: createCommandId,
          threadId: f.threadId,
          expectedEmpty: true,
          legacyBootstrap: policy,
        });
        yield* f.engine.dispatch({
          ...f.command,
          commandId: policy.birthCommandId,
          dispatchGuard: undefined,
          dispatchMode: { type: "defer_start", workspaceStrategy: { type: "root" } },
          legacyBootstrap: policy,
        });
        const run = (yield* f.engine.getThreadProjection(f.threadId)).runs[0]!;
        const result = yield* f.engine
          .dispatch({
            type: "prepared-run.release",
            commandId: policy.releaseCommandId,
            threadId: f.threadId,
            runId: run.id,
            legacyBootstrap: { ...policy, runId: run.id },
          })
          .pipe(Effect.result);
        assert.equal(result._tag, "Failure");
        assert.equal(
          (yield* f.engine.getThreadProjection(f.threadId)).runs[0]!.status,
          "preparing",
        );
        const outbox = yield* EffectOutbox.EffectOutboxV2;
        assert.isEmpty(
          (yield* outbox.listByThreadId(f.threadId)).filter(
            (effect) => effect.request.type === "provider-turn.start",
          ),
        );
        const observed = yield* f.observe();
        assert.equal(observed.commandStatus, "rejected");
        assert.isNull(observed.turn);
      }),
  );
  it.effect(
    "legacy preparation journal joins exact intents and refuses collisions or missing outcomes before another effect",
    () =>
      Effect.gen(function* () {
        const f = yield* queueGuardFixture("legacy-journal");
        const createCommandId = legacyBootstrapCreateCommandId(f.threadId, f.command.commandId);
        const policy = {
          version: 1 as const,
          createCommandId,
          birthCommandId: CommandId.make(`${createCommandId}:initial-message`),
          releaseCommandId: f.command.commandId,
          projectId: ProjectId.make("guard:project:legacy-journal"),
          threadId: f.threadId,
          messageId: f.command.messageId,
          payloadHash: "legacy-journal-payload",
          ownsNewThread: false,
        };
        yield* f.engine.dispatch({
          type: "thread.metadata.update",
          commandId: createCommandId,
          threadId: f.threadId,
          expectedEmpty: true,
          legacyBootstrap: policy,
        });
        yield* f.engine.dispatch({
          ...f.command,
          commandId: policy.birthCommandId,
          dispatchGuard: undefined,
          dispatchMode: {
            type: "defer_start",
            workspaceStrategy: { type: "worktree", baseRef: "main", branch: "legacy" },
            runSetupScript: false,
          },
          legacyBootstrap: policy,
        });
        const run = (yield* f.engine.getThreadProjection(f.threadId)).runs[0]!;
        const receivingPolicy = run.legacyBootstrap;
        if (receivingPolicy === undefined) return yield* Effect.die("Missing native birth policy");
        const sink = yield* EventSink.EventSinkV2;
        const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
        const claimEvents = Array.from(
          yield* sink.readByCommandId({ commandId: createCommandId }).pipe(Stream.runCollect),
        );
        const birthEvents = Array.from(
          yield* sink.readByCommandId({ commandId: policy.birthCommandId }).pipe(Stream.runCollect),
        );
        const proof = legacyBootstrapBirth({ policy: receivingPolicy, claimEvents, birthEvents });
        const claimReceipt = yield* receipts.getByCommandId(createCommandId);
        const birthReceipt = yield* receipts.getByCommandId(policy.birthCommandId);
        if (proof.type !== "valid" || Option.isNone(claimReceipt) || Option.isNone(birthReceipt))
          return yield* Effect.die("Missing authenticated birth");
        const generation = legacyPreparationGeneration({
          runId: run.id,
          birthEventId: proof.birthEventId,
          birthSequence: proof.sequence,
        });
        const preparation: LegacyPreparation = {
          version: 1,
          policy: receivingPolicy,
          generation,
          claimEventId: proof.claimEventId,
          claimSequence: proof.claimSequence,
          claimReceiptSequence: claimReceipt.value.resultSequence,
          birthEventId: proof.birthEventId,
          birthSequence: proof.sequence,
          birthReceiptSequence: birthReceipt.value.resultSequence,
          projectWorkspaceRoot: "/repo",
          commonDirectory: "/repo/.git",
          setup: { status: "opted_out" },
          steps: [],
        };
        yield* f.engine.dispatch({
          type: "prepared-run.progress",
          commandId: CommandId.make(`${createCommandId}:preparation:${generation}:initialize`),
          threadId: f.threadId,
          runId: run.id,
          phase: "worktree",
          legacyPreparationUpdate: { type: "initialize", preparation },
        });
        const effect = {
          kind: "worktree.add" as const,
          input: {
            cwd: "/repo",
            args: ["worktree", "add", "-b", "legacy", "/owned", "a".repeat(40)],
            worktreePath: "/owned",
            commonDirectory: "/repo/.git",
            baseCommitOid: "a".repeat(40),
            targetRef: "refs/heads/legacy",
          },
        };
        const effectId = legacyPreparationEffectId({ generation, effect });
        const stem = `${createCommandId}:preparation:${generation}:${effectId}`;
        const step = {
          effectId,
          effect,
          inputHash: legacyPayloadHash(canonicalLegacyPayload(effect)),
          intentCommandId: CommandId.make(`${stem}:intent`),
          intentEventId: EventId.make(`${stem}:intent:event`),
          state: "intent" as const,
        };
        const intent = {
          type: "prepared-run.progress" as const,
          commandId: step.intentCommandId,
          threadId: f.threadId,
          runId: run.id,
          phase: "worktree" as const,
          legacyPreparationUpdate: { type: "intent" as const, step },
        };
        const accepted = yield* f.engine.dispatch(intent);
        assert.equal((yield* f.engine.dispatch(intent)).sequence, accepted.sequence);
        const recorded = (yield* f.engine.getThreadProjection(f.threadId)).runs[0]!
          .legacyPreparation;
        assert.deepEqual(recorded?.steps, [step]);
        const collision = yield* f.engine
          .dispatch({
            ...intent,
            legacyPreparationUpdate: {
              type: "intent",
              step: {
                ...step,
                effect: { ...effect, input: { ...effect.input, targetRef: "refs/heads/another" } },
              },
            },
          })
          .pipe(Effect.flip);
        assert.equal(collision._tag, "OrchestratorDispatchError");
        const laterEffect = { ...effect, kind: "worktree.submodules" as const };
        const laterId = legacyPreparationEffectId({ generation, effect: laterEffect });
        const laterStem = `${createCommandId}:preparation:${generation}:${laterId}`;
        const missingOutcome = yield* f.engine
          .dispatch({
            ...intent,
            commandId: CommandId.make(`${laterStem}:intent`),
            legacyPreparationUpdate: {
              type: "intent",
              step: {
                effectId: laterId,
                effect: laterEffect,
                inputHash: legacyPayloadHash(canonicalLegacyPayload(laterEffect)),
                intentCommandId: CommandId.make(`${laterStem}:intent`),
                intentEventId: EventId.make(`${laterStem}:intent:event`),
                state: "intent",
              },
            },
          })
          .pipe(Effect.flip);
        assert.equal(missingOutcome._tag, "OrchestratorDispatchError");
        assert.deepEqual(
          (yield* f.engine.getThreadProjection(f.threadId)).runs[0]!.legacyPreparation?.steps,
          [step],
        );
        const outcomeId = CommandId.make(`${stem}:outcome`);
        const invalidFailure = transitionLegacyPreparation({
          current: recorded,
          commandId: outcomeId,
          update: {
            type: "outcome",
            step: {
              ...step,
              state: "known_completed_failure",
              outcomeCommandId: outcomeId,
              outcomeEventId: EventId.make(`${outcomeId}:event`),
              evidence: {
                type: "worktree_claim",
                claim: {
                  path: "/owned",
                  realPath: "/owned",
                  device: "1",
                  inode: "2",
                  parentRealPath: "/",
                  gitDirectory: "/repo/.git/worktrees/owned",
                  commonDirectory: "/repo/.git",
                  registeredPath: "/owned",
                  headRef: "refs/heads/legacy",
                  headOid: "a".repeat(40),
                },
              },
            },
          },
        });
        assert.equal(invalidFailure.type, "rejected");
        const outcome = {
          ...intent,
          commandId: outcomeId,
          legacyPreparationUpdate: {
            type: "outcome" as const,
            step: {
              ...step,
              state: "unknown" as const,
              outcomeCommandId: outcomeId,
              outcomeEventId: EventId.make(`${outcomeId}:event`),
              evidence: { type: "unknown" as const, reason: "outcome_lost" as const },
            },
          },
        };
        const unknownReceipt = yield* f.engine.dispatch(outcome);
        assert.equal((yield* f.engine.dispatch(outcome)).sequence, unknownReceipt.sequence);
        yield* sink.write({
          events: [
            {
              id: EventId.make("legacy-journal:stale-run"),
              type: "run.updated",
              threadId: f.threadId,
              runId: run.id,
              occurredAt: yield* DateTime.now,
              payload: { ...run, legacyPreparation: recorded },
            },
          ],
        });
        assert.equal(
          (yield* f.engine.getThreadProjection(f.threadId)).runs[0]!.legacyPreparation?.steps[0]
            ?.state,
          "unknown",
        );
        const store = yield* EventStore.EventStoreV2;
        const history = Array.from(
          yield* store.read({ threadId: f.threadId }).pipe(Stream.runCollect),
        );
        const memory = yield* Effect.gen(function* () {
          return yield* ProjectionStore.ProjectionStoreV2;
        }).pipe(Effect.provide(Layer.fresh(ProjectionStore.layerMemory)));
        for (const stored of history) yield* memory.apply(stored.event);
        assert.equal(
          (yield* memory.getThreadProjection(f.threadId)).runs[0]!.legacyPreparation?.steps[0]
            ?.state,
          "unknown",
        );
        const originalC = yield* f.observe();
        assert.equal(originalC.commandStatus, "not_found");
        assert.isNull(originalC.turn);
        const outbox = yield* EffectOutbox.EffectOutboxV2;
        assert.isEmpty(
          (yield* outbox.listByThreadId(f.threadId)).filter(
            (pending) => pending.request.type === "provider-turn.start",
          ),
        );
      }),
  );
  it.effect(
    "correlates original bootstrap C only through its authentic birth and committed release",
    () =>
      Effect.gen(function* () {
        const f = yield* queueGuardFixture("legacy-release");
        const createCommandId = legacyBootstrapCreateCommandId(f.threadId, f.command.commandId);
        const birthCommandId = CommandId.make(`${createCommandId}:initial-message`);
        const policy = {
          version: 1 as const,
          createCommandId,
          birthCommandId,
          releaseCommandId: f.command.commandId,
          projectId: ProjectId.make("guard:project:legacy-release"),
          threadId: f.threadId,
          messageId: f.command.messageId,
          payloadHash: "fixture-hash",
          ownsNewThread: false,
        };
        const claim = {
          type: "thread.metadata.update" as const,
          commandId: createCommandId,
          threadId: f.threadId,
          expectedEmpty: true,
          legacyBootstrap: policy,
        };
        yield* f.engine.dispatch(claim);
        const birth = {
          ...f.command,
          commandId: birthCommandId,
          dispatchGuard: undefined,
          dispatchMode: {
            type: "defer_start" as const,
            workspaceStrategy: { type: "root" as const },
          },
          legacyBootstrap: policy,
        };
        yield* f.engine.dispatch(birth);
        const preparing = (yield* f.engine.getThreadProjection(f.threadId)).runs[0]!;
        const sink = yield* EventSink.EventSinkV2;
        const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
        const receivingPolicy = preparing.legacyBootstrap;
        if (receivingPolicy === undefined)
          return yield* Effect.die("Missing receiving birth policy");
        const proof = legacyBootstrapBirth({
          policy: receivingPolicy,
          claimEvents: Array.from(
            yield* sink.readByCommandId({ commandId: createCommandId }).pipe(Stream.runCollect),
          ),
          birthEvents: Array.from(
            yield* sink.readByCommandId({ commandId: birthCommandId }).pipe(Stream.runCollect),
          ),
        });
        const claimReceipt = yield* receipts.getByCommandId(createCommandId);
        const birthReceipt = yield* receipts.getByCommandId(birthCommandId);
        if (proof.type !== "valid" || Option.isNone(claimReceipt) || Option.isNone(birthReceipt))
          return yield* Effect.die("Missing authentic receiving claim/birth receipts");
        const generation = legacyPreparationGeneration({
          runId: preparing.id,
          birthEventId: proof.birthEventId,
          birthSequence: proof.sequence,
        });
        yield* f.engine.dispatch({
          type: "prepared-run.progress",
          commandId: CommandId.make(`${createCommandId}:preparation:${generation}:initialize`),
          threadId: f.threadId,
          runId: preparing.id,
          phase: "setup",
          legacyPreparationUpdate: {
            type: "initialize",
            preparation: {
              version: 1,
              policy: receivingPolicy,
              generation,
              claimEventId: proof.claimEventId,
              claimSequence: proof.claimSequence,
              claimReceiptSequence: claimReceipt.value.resultSequence,
              birthEventId: proof.birthEventId,
              birthSequence: proof.sequence,
              birthReceiptSequence: birthReceipt.value.resultSequence,
              projectWorkspaceRoot: "/repo",
              commonDirectory: null,
              setup: { status: "unresolved" },
              steps: [],
            },
          },
        });
        yield* f.engine.dispatch({
          type: "prepared-run.progress",
          commandId: CommandId.make(`${createCommandId}:preparation:${generation}:setup-policy`),
          threadId: f.threadId,
          runId: preparing.id,
          phase: "setup",
          legacyPreparationUpdate: { type: "setup-policy", setup: { status: "no_script" } },
        });
        const before = yield* f.observe();
        assert.equal(before.commandStatus, "not_found");
        assert.equal(before.correlation, "missing");
        assert.isNull(before.turn);
        const release = {
          type: "prepared-run.release" as const,
          commandId: f.command.commandId,
          threadId: f.threadId,
          runId: preparing.id,
          legacyBootstrap: { ...policy, runId: preparing.id },
        };
        const receipt = yield* f.engine.dispatch(release);
        const released = yield* f.observe();
        assert.equal(released.commandStatus, "accepted");
        assert.equal(released.acceptedSequence, receipt.sequence);
        assert.equal(released.correlation, "pending");
        assert.equal(released.turn?.state, "pending");
        assert.isNull(released.turn?.turnId);
        assert.notProperty(
          (yield* f.engine.getThreadProjection(f.threadId)).messages[0]!,
          "queuedToolBoundaryEligible",
        );
      }),
  );
  it.effect(
    "new legacy birth records truthful guard failure and rejected C before any deletion disposition",
    () =>
      Effect.gen(function* () {
        const f = yield* queueGuardFixture("legacy-new-guard-failure");
        const threadId = ThreadId.make(`${f.threadId}:new`);
        const createCommandId = legacyBootstrapCreateCommandId(threadId, f.command.commandId);
        const policy = {
          version: 1 as const,
          createCommandId,
          birthCommandId: CommandId.make(`${createCommandId}:initial-message`),
          releaseCommandId: f.command.commandId,
          projectId: ProjectId.make("guard:project:legacy-new-guard-failure"),
          threadId,
          messageId: f.command.messageId,
          payloadHash: "new-guard-fixture",
          ownsNewThread: true,
          dispatchGuard: f.command.dispatchGuard,
        };
        yield* f.engine.dispatch({
          type: "thread.create",
          commandId: createCommandId,
          threadId,
          projectId: policy.projectId,
          title: "New legacy shell",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          createdBy: "user",
          creationSource: "web",
          legacyBootstrap: policy,
        });
        yield* f.engine.dispatch({
          ...f.command,
          threadId,
          commandId: policy.birthCommandId,
          dispatchGuard: undefined,
          dispatchMode: { type: "defer_start", workspaceStrategy: { type: "root" } },
          legacyBootstrap: policy,
        });
        const born = (yield* f.engine.getThreadProjection(threadId)).runs[0]!;
        const rejected = yield* f.engine
          .dispatch({
            type: "prepared-run.release",
            commandId: policy.releaseCommandId,
            threadId,
            runId: born.id,
            legacyBootstrap: { ...policy, runId: born.id },
          })
          .pipe(Effect.flip);
        assert.equal(rejected._tag, "OrchestratorDispatchError");
        assert.equal((rejected.cause as { _tag: string })._tag, "DispatchGuardRejected");
        const failed = yield* f.engine.getThreadProjection(threadId);
        assert.equal(failed.runs[0]!.status, "failed");
        assert.isNotNull(failed.runs[0]!.completedAt);
        assert.equal(failed.attempts[0]!.status, "failed");
        assert.equal(failed.nodes.find((node) => node.id === born.rootNodeId)?.status, "failed");
        const preparationItem = failed.turnItems.find(
          (item) => item.runId === born.id && item.type === "command_execution",
        );
        assert.equal(preparationItem?.status, "failed");
        assert.equal(preparationItem?.title, "Dispatch guard rejected");
        assert.notProperty(failed.runs[0]!, "legacyPreparationFailureKnown");
        assert.isEmpty(failed.turnItems.filter((item) => item.type === "error"));
        assert.isNull(failed.thread.deletedAt);
        const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
        const receipt = yield* receipts.getByCommandId(policy.releaseCommandId);
        assert.isTrue(Option.isSome(receipt));
        const sink = yield* EventSink.EventSinkV2;
        const events = Array.from(
          yield* sink
            .readByCommandId({ commandId: policy.releaseCommandId })
            .pipe(Stream.runCollect),
        );
        assert.deepEqual(
          events.map((stored) => stored.event.type),
          ["run-attempt.updated", "node.updated", "turn-item.updated", "run.updated"],
        );
        if (Option.isSome(receipt)) {
          assert.equal(receipt.value.status, "rejected");
          assert.equal(receipt.value.resultSequence, events.at(-1)?.sequence);
        }
        const observation = yield* f.query.observe({
          threadId,
          commandId: policy.releaseCommandId,
          messageId: policy.messageId,
        });
        assert.equal(observation.commandStatus, "rejected");
        assert.isNull(observation.turn);
        const outbox = yield* EffectOutbox.EffectOutboxV2;
        assert.isEmpty(
          (yield* outbox.listByThreadId(threadId)).filter(
            (effect) => effect.request.type === "provider-turn.start",
          ),
        );
      }),
  );
  it.effect(
    "strict legacy final guard rejects its authentic preparing birth without claiming successful C or deleting the shell",
    () =>
      Effect.gen(function* () {
        const f = yield* queueGuardFixture("legacy-strict-final-guard");
        const createCommandId = legacyBootstrapCreateCommandId(f.threadId, f.command.commandId);
        const policy = {
          version: 1 as const,
          createCommandId,
          birthCommandId: CommandId.make(`${createCommandId}:initial-message`),
          releaseCommandId: f.command.commandId,
          projectId: ProjectId.make("guard:project:legacy-strict-final-guard"),
          threadId: f.threadId,
          messageId: f.command.messageId,
          payloadHash: "strict-guard-fixture",
          ownsNewThread: false,
          dispatchGuard: f.command.dispatchGuard,
        };
        yield* f.engine.dispatch({
          type: "thread.metadata.update",
          commandId: createCommandId,
          threadId: f.threadId,
          expectedEmpty: true,
          legacyBootstrap: policy,
        });
        yield* f.engine.dispatch({
          ...f.command,
          commandId: policy.birthCommandId,
          dispatchGuard: undefined,
          dispatchMode: { type: "defer_start", workspaceStrategy: { type: "root" } },
          legacyBootstrap: policy,
        });
        const preparing = (yield* f.engine.getThreadProjection(f.threadId)).runs[0]!;
        const release = {
          type: "prepared-run.release" as const,
          commandId: f.command.commandId,
          threadId: f.threadId,
          runId: preparing.id,
          legacyBootstrap: { ...policy, runId: preparing.id },
        };
        const rejected = yield* f.engine.dispatch(release).pipe(Effect.flip);
        assert.equal(rejected._tag, "OrchestratorDispatchError");
        assert.include(
          (rejected.cause as { reason: string }).reason,
          "target changed after observation",
        );
        const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
        const receipt = yield* receipts.getByCommandId(f.command.commandId);
        assert.isTrue(Option.isSome(receipt));
        if (Option.isSome(receipt)) {
          assert.equal(receipt.value.status, "rejected");
          assert.equal(receipt.value.commandType, "prepared-run.release");
        }
        const observation = yield* f.observe();
        assert.equal(observation.commandStatus, "rejected");
        assert.isNull(observation.turn);
        const retained = yield* f.engine.getThreadProjection(f.threadId);
        assert.isNull(retained.thread.deletedAt);
        assert.equal(retained.runs[0]!.status, "preparing");
        const decision = retained.runs[0]!.legacyReleaseDecision;
        assert.isDefined(decision);
        assert.equal(decision?.status, "rejected");
        assert.equal(decision?.policy.releaseCommandId, f.command.commandId);
        const sink = yield* EventSink.EventSinkV2;
        const rejectionEvents = Array.from(
          yield* sink.readByCommandId({ commandId: f.command.commandId }).pipe(Stream.runCollect),
        );
        assert.equal(rejectionEvents.length, 1);
        assert.equal(rejectionEvents[0]?.event.type, "run.updated");
        assert.equal(rejectionEvents[0]?.event.id, decision?.evidenceEventId);
        if (Option.isSome(receipt))
          assert.equal(rejectionEvents[0]?.sequence, receipt.value.resultSequence);
        assert.isAtLeast(decision!.observed.lastEventSequence, decision!.birthSequence);
        assert.isNull(retained.runs[0]!.startedAt);
        const outbox = yield* EffectOutbox.EffectOutboxV2;
        assert.isEmpty(
          (yield* outbox.listByThreadId(f.threadId)).filter(
            (effect) => effect.request.type === "provider-turn.start",
          ),
        );
        const replay = yield* f.engine.dispatch(release).pipe(Effect.flip);
        assert.equal(replay._tag, "OrchestratorCommandPreviouslyRejectedError");
      }),
  );
  it.effect(
    "same-thread recreation waits for exact durable deletion cleanup and the serialized create rejects an unfenced caller",
    () =>
      Effect.gen(function* () {
        const f = yield* queueGuardFixture("creation-fence");
        const deletionId = CommandId.make("guard:creation-fence:delete");
        yield* f.engine.dispatch({
          type: "thread.delete",
          commandId: deletionId,
          threadId: f.threadId,
        });
        const outbox = yield* EffectOutbox.EffectOutboxV2;
        const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
        const create = {
          type: "thread.create" as const,
          createdBy: "user" as const,
          creationSource: "web" as const,
          commandId: CommandId.make("guard:creation-fence:recreate"),
          threadId: f.threadId,
          projectId: ProjectId.make("guard:project:creation-fence"),
          title: "Recreated after cleanup",
          modelSelection,
          runtimeMode: "full-access" as const,
          interactionMode: "default" as const,
          branch: null,
          worktreePath: null,
        };
        const unsafe = yield* f.engine
          .dispatch({ ...create, commandId: CommandId.make("guard:creation-fence:unfenced") })
          .pipe(Effect.flip);
        assert.equal(unsafe._tag, "OrchestratorDispatchError");
        assert.include(String(unsafe.cause), "recreation is fenced");
        const recreation = yield* awaitThreadCreationCleanup(outbox, f.threadId).pipe(
          Effect.andThen(f.engine.dispatch(create)),
          Effect.forkChild,
        );
        assert.isUndefined(recreation.pollUnsafe());
        assert.isTrue(Option.isNone(yield* receipts.getByCommandId(create.commandId)));
        assert.isNotNull((yield* f.engine.getThreadProjection(f.threadId)).thread.deletedAt);
        const cleanup = yield* outbox.claimNext({
          workerId: "creation-fence-worker",
          leaseDurationMs: 30_000,
        });
        assert.isTrue(Option.isSome(cleanup));
        if (Option.isSome(cleanup)) {
          assert.equal(cleanup.value.commandId, deletionId);
          assert.equal(cleanup.value.request.type, "terminal.cleanup");
          yield* outbox.succeed({ effectId: cleanup.value.id, workerId: "creation-fence-worker" });
        }
        const created = yield* Fiber.join(recreation);
        assert.isTrue(
          created.storedEvents.some((stored) => stored.event.type === "thread.created"),
        );
        assert.isNull((yield* f.engine.getThreadProjection(f.threadId)).thread.deletedAt);
        assert.equal((yield* f.engine.getThreadProjection(f.threadId)).thread.title, create.title);
      }),
  );
  it.effect.each(
    (["deleted", "unknown", "delete-persistence-failed"] as const).map((outcome) => ({
      outcome,
      name: `legacy bootstrap ${outcome === "deleted" ? "known failure tombstones only its proven shell" : outcome === "unknown" ? "unknown failure retains its shell" : "delete persistence failure retains the durable failed shell without disposition"}`,
    })),
  )("$name", ({ outcome }) =>
    Effect.gen(function* () {
      const known = outcome !== "unknown";
      const deleted = outcome === "deleted";
      const name = `legacy-failure-${outcome}`;
      const f = yield* queueGuardFixture(name);
      const owner = yield* LegacyGuardTerminalFixture;
      const fs = yield* FileSystem.FileSystem;
      const commonDirectory = `${owner.baseDir}/.git`;
      const worktreePath = `${owner.baseDir}/owned-worktree`;
      yield* fs.makeDirectory(commonDirectory);
      yield* moveProject(
        ProjectId.make(`guard:project:${name}`),
        owner.baseDir,
        "2026-10-05T00:00:00.000Z",
      );
      const threadId = ThreadId.make(`bootstrap:${name}`);
      const createCommandId = legacyBootstrapCreateCommandId(threadId, f.command.commandId);
      const policy = {
        version: 1 as const,
        createCommandId,
        birthCommandId: CommandId.make(`${createCommandId}:initial-message`),
        releaseCommandId: f.command.commandId,
        projectId: ProjectId.make(`guard:project:${name}`),
        threadId,
        messageId: f.command.messageId,
        payloadHash: "fixture-hash",
        ownsNewThread: true,
      };
      yield* f.engine.dispatch({
        type: "thread.create",
        commandId: createCommandId,
        threadId,
        projectId: policy.projectId,
        title: "Bootstrap fixture",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        createdBy: "user",
        creationSource: "web",
        legacyBootstrap: policy,
      });
      yield* f.engine.dispatch({
        ...f.command,
        threadId,
        commandId: policy.birthCommandId,
        dispatchGuard: undefined,
        dispatchMode: {
          type: "defer_start",
          workspaceStrategy: { type: "worktree", baseRef: "main", startFromOrigin: false },
          runSetupScript: false,
        },
        legacyBootstrap: policy,
      });
      const run = (yield* f.engine.getThreadProjection(threadId)).runs[0]!;
      const sink = yield* EventSink.EventSinkV2;
      const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
      const collect = (commandId: CommandId) =>
        sink.readByCommandId({ commandId }).pipe(
          Stream.runCollect,
          Effect.map((events) => Array.from(events)),
        );
      const receivingPolicy = { ...policy, runId: run.id };
      const proof = legacyBootstrapBirth({
        policy: receivingPolicy,
        claimEvents: yield* collect(createCommandId),
        birthEvents: yield* collect(policy.birthCommandId),
      });
      const claim = yield* receipts.getByCommandId(createCommandId);
      const ownedBirthReceipt = yield* receipts.getByCommandId(policy.birthCommandId);
      if (proof.type !== "valid" || Option.isNone(claim) || Option.isNone(ownedBirthReceipt))
        return yield* Effect.die("Actual registered claim/birth proof missing");
      const generation = legacyPreparationGeneration({
        runId: run.id,
        birthEventId: proof.birthEventId,
        birthSequence: proof.sequence,
      });
      let preparation: LegacyPreparation = {
        version: 1,
        policy: receivingPolicy,
        claimEventId: proof.claimEventId,
        claimSequence: proof.claimSequence,
        claimReceiptSequence: claim.value.resultSequence,
        birthEventId: proof.birthEventId,
        birthSequence: proof.sequence,
        birthReceiptSequence: ownedBirthReceipt.value.resultSequence,
        generation,
        projectWorkspaceRoot: owner.baseDir,
        commonDirectory,
        setup: { status: "opted_out" },
        steps: [],
      };
      const progress = (commandId: CommandId, update: LegacyPreparationUpdate) =>
        Effect.gen(function* () {
          const result = yield* f.engine.dispatch({
            type: "prepared-run.progress",
            commandId,
            threadId,
            runId: run.id,
            phase: "worktree",
            legacyPreparationUpdate: update,
          });
          const recorded = (yield* collect(commandId))[0];
          const receipt = yield* receipts.getByCommandId(commandId);
          if (
            recorded?.event.type !== "run.updated" ||
            recorded.sequence !== result.sequence ||
            Option.isNone(receipt) ||
            receipt.value.status !== "accepted" ||
            recorded.event.payload.legacyPreparation === undefined
          )
            return yield* Effect.die("Actual owner journal acceptance/readback missing");
          preparation = recorded.event.payload.legacyPreparation;
        });
      yield* progress(CommandId.make(`${createCommandId}:preparation:${generation}:initialize`), {
        type: "initialize",
        preparation,
      });
      let intent: LegacyPreparation["steps"][number] | undefined;
      const spawner = ChildProcessSpawner.make((command) =>
        Effect.gen(function* () {
          if (!ChildProcess.isStandardCommand(command))
            return yield* Effect.die("Unexpected synthetic pipeline");
          const args = [...command.args];
          if (args.includes("add")) {
            yield* fs.makeDirectory(worktreePath);
            yield* fs.writeFileString(`${worktreePath}/partial`, "unknown entered material");
          }
          const stdout = args.includes("--git-common-dir")
            ? `${commonDirectory}\n`
            : args.includes("rev-parse")
              ? `${"9".repeat(40)}\n`
              : "";
          return ChildProcessSpawner.makeHandle({
            pid: ChildProcessSpawner.ProcessId(1),
            exitCode: Effect.succeed(
              ChildProcessSpawner.ExitCode(
                args.includes("add") || args.includes("--get-regexp") ? 1 : 0,
              ),
            ),
            isRunning: Effect.succeed(false),
            kill: () => Effect.void,
            unref: Effect.succeed(Effect.void),
            stdin: Sink.drain,
            stdout: Stream.encodeText(Stream.make(stdout)),
            stderr: Stream.empty,
            all: Stream.empty,
            getInputFd: () => Sink.drain,
            getOutputFd: () => Stream.empty,
          });
        }),
      );
      const driver = yield* makeGitVcsDriverCore().pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Effect.provide(ServerConfigLayer),
      );
      yield* driver
        .createWorktree(
          {
            cwd: owner.baseDir,
            refName: "main",
            newRefName: known ? "invalid?" : "valid",
            path: worktreePath,
          },
          {
            legacyPreparation: {
              beforeEffect: (planned) =>
                Effect.gen(function* () {
                  const { kind, ...input } = planned;
                  const effect = { kind, input };
                  const effectId = legacyPreparationEffectId({ generation, effect });
                  const commandId = CommandId.make(
                    `${createCommandId}:preparation:${generation}:${effectId}:intent`,
                  );
                  intent = {
                    effect,
                    effectId,
                    inputHash: legacyPayloadHash(canonicalLegacyPayload(effect)),
                    intentCommandId: commandId,
                    intentEventId: EventId.make(`${commandId}:event`),
                    state: "intent",
                  };
                  yield* progress(commandId, { type: "intent", step: intent });
                }),
              neverInvoked: (planned, reason) =>
                Effect.gen(function* () {
                  if (intent === undefined) return yield* Effect.die("Missing actual owner intent");
                  const { kind, ...input } = planned;
                  if (
                    canonicalLegacyPayload(intent.effect) !==
                    canonicalLegacyPayload({ kind, input })
                  )
                    return yield* Effect.die("Owner effect input changed");
                  const commandId = CommandId.make(
                    `${createCommandId}:preparation:${generation}:${intent.effectId}:outcome`,
                  );
                  yield* progress(commandId, {
                    type: "outcome",
                    step: {
                      ...intent,
                      state: "known_no_effect_failure",
                      outcomeCommandId: commandId,
                      outcomeEventId: EventId.make(`${commandId}:event`),
                      evidence: { type: "never_invoked", owner: "git", reason },
                    },
                  });
                }),
              afterEffect: () =>
                Effect.gen(function* () {
                  if (intent === undefined || known)
                    return yield* Effect.die("Unexpected executing owner outcome");
                  const commandId = CommandId.make(
                    `${createCommandId}:preparation:${generation}:${intent.effectId}:outcome`,
                  );
                  yield* progress(commandId, {
                    type: "outcome",
                    step: {
                      ...intent,
                      state: "unknown",
                      outcomeCommandId: commandId,
                      outcomeEventId: EventId.make(`${commandId}:event`),
                      evidence: { type: "unknown", reason: "partial_material" },
                    },
                  });
                }),
            },
          },
        )
        .pipe(Effect.result);
      yield* f.engine.dispatch({
        type: "prepared-run.fail",
        commandId: CommandId.make(`${createCommandId}:fail`),
        threadId,
        runId: run.id,
        legacyPreparationFailureKnown: known,
        failure: makeProviderFailure({
          cause: "worktree exploded",
          message: "worktree exploded",
          class: "validation_error",
          retryable: false,
        }),
      });
      const deletion = {
        type: "legacy-bootstrap.failure-delete" as const,
        commandId: CommandId.make(`${createCommandId}:failure-delete`),
        threadId,
        runId: run.id,
        legacyBootstrap: { ...policy, runId: run.id },
        legacyNoControl: yield* Schema.decodeEffect(LegacyNoTerminalControl)({
          version: 1,
          type: "no_control",
          policy: receivingPolicy,
          threadId,
          runId: run.id,
          claimEventId: proof.claimEventId,
          claimSequence: proof.claimSequence,
          claimReceiptSequence: claim.value.resultSequence,
          birthEventId: proof.birthEventId,
          birthSequence: proof.sequence,
          birthReceiptSequence: ownedBirthReceipt.value.resultSequence,
          preparationGeneration: generation,
          workspacePath: owner.baseDir,
          projectWorkspaceRoot: owner.baseDir,
        }),
      };
      const commit = sink.commitCommand;
      const failedCommit =
        outcome === "delete-persistence-failed"
          ? vi.spyOn(sink, "commitCommand").mockImplementation((input) =>
              input.commandId === deletion.commandId
                ? Effect.fail(
                    new EventSink.EventSinkWriteError({
                      eventCount: input.events.length,
                      commandId: input.commandId,
                      cause: "delete persistence failed",
                    }),
                  )
                : commit(input),
            )
          : undefined;
      if (deleted) {
        const deleted = yield* f.engine.dispatchLegacyFailureDelete!(deletion);
        assert.isTrue(deleted.storedEvents.some(({ event }) => event.type === "thread.deleted"));
        const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
        const receipt = yield* receipts.getByCommandId(deletion.commandId);
        assert.isTrue(Option.isSome(receipt));
        if (Option.isSome(receipt)) assert.equal(receipt.value.resultSequence, deleted.sequence);
        const replayed = yield* f.engine.dispatchLegacyFailureDelete!(deletion);
        assert.equal(replayed.sequence, deleted.sequence);
      } else {
        const rejected = yield* f.engine.dispatchLegacyFailureDelete!(deletion).pipe(
          Effect.flip,
          Effect.ensuring(Effect.sync(() => failedCommit?.mockRestore())),
        );
        assert.equal(rejected._tag, "OrchestratorDispatchError");
        assert.isNull((yield* f.engine.getThreadProjection(threadId)).thread.deletedAt);
      }
      const observed = yield* f.query.observe({
        threadId,
        commandId: policy.releaseCommandId,
        messageId: policy.messageId,
      });
      assert.equal(observed.commandStatus, "not_found");
      assert.equal(observed.correlation, "missing");
      assert.isNull(observed.turn);
      assert.equal(observed.target === null, deleted);
      const events = yield* EventSink.EventSinkV2;
      const birth = yield* events
        .readByCommandId({ commandId: policy.birthCommandId })
        .pipe(Stream.runCollect);
      assert.isTrue(Array.from(birth).some(({ event }) => event.type === "run.created"));
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      assert.isFalse(
        (yield* outbox.listByThreadId(threadId)).some(
          ({ request }) => request.type === "provider-turn.start",
        ),
      );
    }).pipe(
      Effect.provide(
        Layer.fresh(LegacyGuardOwnerTestLayer).pipe(Layer.provideMerge(NodeServices.layer)),
      ),
    ),
  );
  it.effect("blocks durable attention flags and live background work", () =>
    Effect.gen(function* () {
      const f = yield* queueGuardFixture("attention");
      const sink = yield* EventSink.EventSinkV2;
      const now = yield* DateTime.now;
      const nodeId = NodeId.make("guard:attention:node");
      yield* f.engine.dispatch({
        ...f.command,
        commandId: CommandId.make("guard:attention:root"),
        messageId: MessageId.make("guard:attention:root"),
        dispatchGuard: undefined,
      });
      const projection = yield* f.engine.getThreadProjection(f.threadId);
      const completedRun = projection.runs[0]!;
      const providerThreadId = ProviderThreadId.make("guard:attention:provider-thread");
      yield* sink.write({
        events: [
          {
            id: EventId.make("guard:attention:root-completed"),
            type: "run.updated",
            threadId: f.threadId,
            runId: completedRun.id,
            occurredAt: now,
            payload: { ...completedRun, status: "completed", startedAt: now, completedAt: now },
          },
          ...(["approval", "user_input"] as const).map((kind) => ({
            id: EventId.make(`guard:attention:${kind}`),
            type: "runtime-request.updated" as const,
            threadId: f.threadId,
            occurredAt: now,
            payload: {
              id: RuntimeRequestId.make(`guard:attention:${kind}`),
              nodeId,
              providerTurnId: null,
              nativeRequestRef: null,
              kind: kind === "approval" ? ("command" as const) : ("user_input" as const),
              status: "pending" as const,
              responseCapability: { type: "not_resumable" as const, reason: "fixture" },
              createdAt: now,
              resolvedAt: null,
            },
          })),
          {
            id: EventId.make("guard:attention:plan"),
            type: "plan.updated",
            threadId: f.threadId,
            occurredAt: now,
            payload: {
              id: PlanId.make("guard:attention:plan"),
              threadId: f.threadId,
              runId: null,
              nodeId,
              status: "active",
              kind: "proposed_plan",
              markdown: "Review this plan",
            },
          },
          {
            id: EventId.make("guard:attention:thread-binding"),
            type: "thread.metadata-updated",
            threadId: f.threadId,
            occurredAt: now,
            payload: { ...projection.thread, activeProviderThreadId: providerThreadId },
          },
          {
            id: EventId.make("guard:attention:roster"),
            type: "provider-thread.updated",
            threadId: f.threadId,
            occurredAt: now,
            payload: {
              id: providerThreadId,
              driver: ProviderDriverKind.make("codex"),
              providerInstanceId: modelSelection.instanceId,
              providerSessionId: null,
              appThreadId: f.threadId,
              ownerNodeId: null,
              nativeThreadRef: null,
              nativeConversationHeadRef: null,
              status: "active",
              firstRunOrdinal: null,
              lastRunOrdinal: null,
              handoffIds: [],
              forkedFrom: null,
              pendingBackgroundTasks: [{ taskId: "live-task", kind: "background_task" }],
              createdAt: now,
              updatedAt: now,
            },
          },
        ],
      });
      const current = yield* f.observe();
      for (const blocker of [
        "pending_approval",
        "pending_user_input",
        "actionable_plan",
        "background_work",
      ] as const)
        assert.include(current.target!.blockers, blocker);
      const before = current.snapshotSequence;
      const rejected = yield* f.engine
        .dispatch({
          ...f.command,
          dispatchGuard: {
            ...f.command.dispatchGuard,
            observedSnapshotSequence: before,
            expectedLatestTurnId: current.target!.latestTurnId,
          },
        })
        .pipe(Effect.flip);
      assert.include((rejected.cause as { readonly reason: string }).reason, "pending_approval");
      assert.equal((yield* f.observe()).snapshotSequence, before);
    }),
  );
  it.effect.each(["starting", "running", "ready"] as const)(
    "blocks a freshly observed %s session with active work",
    (status) =>
      Effect.gen(function* () {
        const f = yield* queueGuardFixture(`session-${status}`);
        const sink = yield* EventSink.EventSinkV2;
        const now = yield* DateTime.now;
        if (status === "ready") {
          yield* f.engine.dispatch({
            ...f.command,
            commandId: CommandId.make("guard:session-ready:running"),
            messageId: MessageId.make("guard:session-ready:running"),
            dispatchGuard: undefined,
          });
          const run = (yield* f.engine.getThreadProjection(f.threadId)).runs[0]!;
          yield* sink.write({
            events: [
              {
                id: EventId.make("guard:session-ready:run"),
                type: "run.updated",
                threadId: f.threadId,
                runId: run.id,
                occurredAt: now,
                payload: { ...run, status: "running", startedAt: now },
              },
            ],
          });
        }
        yield* sink.write({
          events: [
            {
              id: EventId.make(`guard:session-${status}:session`),
              type: "provider-session.attached",
              threadId: f.threadId,
              occurredAt: now,
              payload: {
                id: ProviderSessionId.make(`guard:session-${status}`),
                driver: ProviderDriverKind.make("codex"),
                providerInstanceId: modelSelection.instanceId,
                status,
                cwd: "/repo",
                model: modelSelection.model,
                capabilities: CodexProviderCapabilitiesV2,
                createdAt: now,
                updatedAt: now,
                lastError: null,
              },
            },
          ],
        });
        const current = yield* f.observe();
        const target = current.target!;
        assert.include(
          target.blockers,
          status === "starting"
            ? "session_starting"
            : status === "running"
              ? "session_running"
              : "active_turn",
        );
        assert.equal(
          (yield* f.engine
            .dispatch({
              ...f.command,
              dispatchGuard: {
                ...f.command.dispatchGuard,
                observedSnapshotSequence: current.snapshotSequence,
                expectedSessionStatus: target.sessionStatus,
                expectedActiveTurnId: target.activeTurnId,
                expectedLatestTurnId: target.latestTurnId,
              },
            })
            .pipe(Effect.exit))._tag,
          "Failure",
        );
        assert.equal((yield* f.observe()).snapshotSequence, current.snapshotSequence);
      }),
  );

  it.effect("allows a new model on the observed instance and binds its accepted run", () =>
    Effect.gen(function* () {
      const f = yield* queueGuardFixture("same-instance-model");
      const desired = {
        ...modelSelection,
        model: "gpt-6.1-sol",
        options: [{ id: "reasoningEffort", value: "medium" }],
      };
      yield* f.engine.dispatch({ ...f.command, modelSelection: desired });
      assert.equal((yield* f.observe()).commandStatus, "accepted");
      assert.deepEqual(
        (yield* f.engine.getThreadProjection(f.threadId)).runs[0]!.modelSelection,
        desired,
      );
    }),
  );
  it.effect("requires command birth binding and detects ambiguous projected runs", () =>
    Effect.gen(function* () {
      const f = yield* queueGuardFixture("ambiguous");
      yield* f.engine.dispatch(f.command);
      const missing = yield* f.query.observe({
        threadId: f.threadId,
        commandId: CommandId.make("guard:never-dispatched"),
        messageId: f.command.messageId,
      });
      assert.equal(missing.commandStatus, "not_found");
      assert.equal(missing.correlation, "missing");
      assert.isNull(missing.turn);
      const sql = yield* SqlClient.SqlClient;
      const duplicate = RunId.make("guard:ambiguous:duplicate");
      yield* sql`INSERT INTO orchestration_v2_projection_runs (run_id,thread_id,ordinal,provider,provider_instance_id,provider_thread_id,status,requested_at,completed_at,payload_json)
      SELECT ${duplicate},thread_id,ordinal+1,provider,provider_instance_id,provider_thread_id,status,requested_at,completed_at,json_set(payload_json,'$.id',${duplicate},'$.ordinal',ordinal+1)
      FROM orchestration_v2_projection_runs WHERE thread_id=${f.threadId}`;
      const ambiguous = yield* f.observe();
      assert.equal(ambiguous.correlation, "ambiguous");
      assert.isNull(ambiguous.turn);
    }),
  );
  it.effect("admits one of two commands sharing an observed idle state", () =>
    Effect.gen(function* () {
      const f = yield* queueGuardFixture("race");
      const results = yield* Effect.all(
        [
          f.engine.dispatch(f.command).pipe(Effect.exit),
          f.engine
            .dispatch({
              ...f.command,
              commandId: CommandId.make("guard:race:second"),
              messageId: MessageId.make("guard:race:second-message"),
            })
            .pipe(Effect.exit),
        ],
        { concurrency: 2 },
      );
      assert.lengthOf(
        results.filter((result) => result._tag === "Success"),
        1,
      );
      assert.lengthOf(
        results.filter((result) => result._tag === "Failure"),
        1,
      );
    }),
  );

  it.effect("accepts once and replays a receipt before rechecking its busy guard", () =>
    Effect.gen(function* () {
      const f = yield* queueGuardFixture("replay");
      assert.equal(f.initial.commandStatus, "not_found");
      assert.isTrue(f.initial.target?.idle);
      const accepted = yield* f.engine.dispatch(f.command);
      const pending = yield* f.observe();
      assert.equal(pending.commandStatus, "accepted");
      assert.equal(pending.acceptedSequence, accepted.sequence);
      assert.equal(pending.correlation, "pending");
      assert.equal(pending.turn?.state, "pending");
      assert.isFalse(pending.target?.idle);
      assert.deepEqual(yield* f.engine.dispatch(f.command), accepted);
      yield* Schema.decodeUnknownEffect(OrchestrationCommandObservation)(pending);
    }),
  );
  it.effect("rejects a stale target without events and preserves rejected replay", () =>
    Effect.gen(function* () {
      const f = yield* queueGuardFixture("stale");
      yield* f.engine.dispatch({
        type: "thread.runtime-mode.set",
        commandId: CommandId.make("guard:change"),
        threadId: f.threadId,
        runtimeMode: "approval-required",
      });
      const before = (yield* f.observe()).snapshotSequence;
      assert.equal((yield* f.engine.dispatch(f.command).pipe(Effect.exit))._tag, "Failure");
      const rejected = yield* f.observe();
      assert.equal(rejected.snapshotSequence, before);
      assert.equal(rejected.commandStatus, "rejected");
      assert.isNull(rejected.acceptedSequence);
      assert.equal(
        (yield* f.engine.dispatch(f.command).pipe(Effect.flip))._tag,
        "OrchestratorCommandPreviouslyRejectedError",
      );
    }),
  );
  it.effect("allows unrelated aggregate changes and rejects model-option mismatch", () =>
    Effect.gen(function* () {
      const f = yield* queueGuardFixture("unrelated");
      yield* queueGuardFixture("other");
      yield* f.engine.dispatch(f.command);
      const mismatch = yield* queueGuardFixture("options");
      const result = yield* mismatch.engine
        .dispatch({
          ...mismatch.command,
          dispatchGuard: {
            ...mismatch.command.dispatchGuard,
            expectedModelSelection: {
              ...modelSelection,
              options: [{ id: "reasoningEffort", value: "high" }],
            },
          },
        })
        .pipe(Effect.exit);
      assert.equal(result._tag, "Failure");
      assert.equal((yield* mismatch.observe()).snapshotSequence, mismatch.initial.snapshotSequence);
    }),
  );
  it.effect("holds settled targets until explicit reopen", () =>
    Effect.gen(function* () {
      const f = yield* queueGuardFixture("settled");
      yield* f.engine.dispatch({
        type: "thread.settle",
        commandId: CommandId.make("guard:settle"),
        threadId: f.threadId,
      });
      const settled = yield* f.observe();
      assert.include(settled.target?.blockers ?? [], "settled");
      assert.equal(
        (yield* f.engine
          .dispatch({
            ...f.command,
            dispatchGuard: {
              ...f.command.dispatchGuard,
              observedSnapshotSequence: settled.snapshotSequence,
            },
          })
          .pipe(Effect.exit))._tag,
        "Failure",
      );
      yield* f.engine.dispatch({
        type: "thread.unsettle",
        reason: "user",
        commandId: CommandId.make("guard:unsettle"),
        threadId: f.threadId,
      });
      const reopened = yield* f.observe();
      assert.isTrue(reopened.target?.idle);
      yield* f.engine.dispatch({
        ...f.command,
        commandId: CommandId.make("guard:reopen:start"),
        dispatchGuard: {
          ...f.command.dispatchGuard,
          observedSnapshotSequence: reopened.snapshotSequence,
        },
      });
    }),
  );
  it.effect("rejects future observations and cross-instance selections without events", () =>
    Effect.gen(function* () {
      for (const reason of ["future", "instance"] as const) {
        const f = yield* queueGuardFixture(reason);
        const command =
          reason === "future"
            ? {
                ...f.command,
                dispatchGuard: {
                  ...f.command.dispatchGuard,
                  observedSnapshotSequence: f.initial.snapshotSequence + 1,
                },
              }
            : {
                ...f.command,
                modelSelection: { ...modelSelection, instanceId: alternateInstanceId },
              };
        assert.equal((yield* f.engine.dispatch(command).pipe(Effect.exit))._tag, "Failure");
        assert.equal((yield* f.observe()).snapshotSequence, f.initial.snapshotSequence);
      }
    }),
  );
  it.effect(
    "correlates a historical run after later work and rejects cross-thread/message reuse",
    () =>
      Effect.gen(function* () {
        const f = yield* queueGuardFixture("historical");
        yield* f.engine.dispatch(f.command);
        const sink = yield* EventSink.EventSinkV2;
        const now = yield* DateTime.now;
        const run = (yield* f.engine.getThreadProjection(f.threadId)).runs[0]!;
        yield* sink.write({
          events: [
            {
              id: EventId.make("guard:historical:completed"),
              type: "run.updated",
              threadId: f.threadId,
              runId: run.id,
              occurredAt: now,
              payload: { ...run, status: "completed", startedAt: now, completedAt: now },
            },
          ],
        });
        const current = yield* f.observe();
        yield* f.engine.dispatch({
          ...f.command,
          commandId: CommandId.make("guard:historical:new"),
          messageId: MessageId.make("guard:historical:new-message"),
          dispatchGuard: {
            ...f.command.dispatchGuard,
            observedSnapshotSequence: current.snapshotSequence,
            expectedLatestTurnId: current.target!.latestTurnId,
          },
        });
        const historical = yield* f.observe();
        assert.equal(historical.correlation, "exact");
        assert.equal(historical.turn?.state, "completed");
        assert.equal(historical.turn?.turnId, TurnId.make(run.id));
        assert.equal(
          (yield* f.query.observe({
            threadId: f.threadId,
            commandId: f.command.commandId,
            messageId: MessageId.make("wrong-message"),
          })).correlation,
          "mismatched",
        );
        assert.equal(
          (yield* f.query.observe({
            threadId: ThreadId.make("wrong-thread"),
            commandId: f.command.commandId,
            messageId: f.command.messageId,
          })).correlation,
          "mismatched",
        );
      }),
  );
  it.effect("keeps an accepted receipt unresolved when its projected run is unavailable", () =>
    Effect.gen(function* () {
      const f = yield* queueGuardFixture("missing-run");
      yield* f.engine.dispatch(f.command);
      const sql = yield* SqlClient.SqlClient;
      yield* sql`DELETE FROM orchestration_v2_projection_runs WHERE thread_id = ${f.threadId}`;
      const observed = yield* f.observe();
      assert.equal(observed.commandStatus, "accepted");
      assert.equal(observed.correlation, "pending");
      assert.isNull(observed.turn);
    }),
  );
});

import * as SourceControlProviderRegistry from "../../sourceControl/SourceControlProviderRegistry.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import {
  CommandId,
  EventId,
  MessageId,
  NodeId,
  RuntimeRequestId,
  type ModelSelection,
  type OrchestrationV2ExecutionNode,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderTurnId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as CheckpointStore from "../../checkpointing/CheckpointStore.ts";
import * as GitWorkflow from "../../git/GitWorkflowService.ts";
import * as ServerConfig from "../../config.ts";
import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import * as ProjectService from "../../project/ProjectService.ts";
import * as ServerSettings from "../../serverSettings.ts";
import * as McpSessionRegistryTestkit from "../../mcp/McpSessionRegistry.testkit.ts";
import * as ProviderInstanceRegistry from "../../provider/Services/ProviderInstanceRegistry.ts";
import type { ProviderInstance } from "../../provider/ProviderDriver.ts";
import * as VcsDriverRegistry from "../../vcs/VcsDriverRegistry.ts";
import * as VcsProcess from "../../vcs/VcsProcess.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as EffectOutbox from "../../orchestration-v2/EffectOutbox.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import { queuedToolBoundaryTarget } from "../../orchestration-v2/QueuedToolBoundary.ts";
import * as ProjectStore from "../../orchestration-v2/ProjectStore.ts";
import type {
  ProviderAdapterV2SessionRuntime,
  ProviderAdapterV2Shape,
} from "../../orchestration-v2/ProviderAdapter.ts";
import * as ProviderSessionManager from "../../orchestration-v2/ProviderSessionManager.ts";
import {
  OrchestrationV2EventSinkLayerLive,
  OrchestrationV2LayerLive,
} from "../../orchestration-v2/runtimeLayer.ts";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as ThreadCommandExecutor from "../../orchestration-v2/ThreadCommandExecutor.ts";

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

const selfSettlementFixture = Effect.fnUntraced(function* (prefix: string) {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const sink = yield* EventSink.EventSinkV2;
  const sessions = yield* ProviderSessionManager.ProviderSessionManagerV2;
  const threadId = ThreadId.make(prefix);
  const projectId = ProjectId.make(`${prefix}-project`);
  yield* seedProject({
    projectId,
    title: "Self settlement project",
    workspaceRoot: process.cwd(),
    defaultModelSelection: modelSelection,
    createdAt: DateTime.formatIso(yield* DateTime.now),
  });
  yield* orchestrator.dispatch({
    type: "thread.create",
    commandId: CommandId.make(`${prefix}-create`),
    threadId,
    createdBy: "user",
    creationSource: "web",
    projectId,
    title: "Self settlement",
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
  });
  yield* orchestrator.dispatch({
    type: "message.dispatch",
    commandId: CommandId.make(`${prefix}-start`),
    threadId,
    createdBy: "user",
    creationSource: "web",
    messageId: MessageId.make(`${prefix}-message`),
    text: "Finish this task",
    attachments: [],
    dispatchMode: { type: "start_immediately" },
  });
  const initial = yield* orchestrator.getThreadProjection(threadId);
  const run = initial.runs[0]!;
  const provider = initial.providerThreads[0]!;
  const callerSpy = vi
    .spyOn(sessions, "isMcpCallerAttached")
    .mockImplementation((input) =>
      Effect.succeed(
        input.threadId === threadId &&
          input.mcpCredentialId === "self-credential" &&
          input.providerSessionId === provider.providerSessionId,
      ),
    );
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      callerSpy.mockRestore();
    }),
  );
  const request = {
    threadId,
    commandId: CommandId.make(`${prefix}-request`),
    mcpCredentialId: "self-credential",
    providerInstanceId: modelSelection.instanceId,
  };
  const now = yield* DateTime.now;
  const providerSession = {
    id: provider.providerSessionId!,
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
  yield* sink.write({
    events: [
      {
        id: EventId.make(`${prefix}-attached`),
        type: "provider-session.attached",
        threadId,
        occurredAt: now,
        payload: providerSession,
      },
    ],
  });
  return { orchestrator, sink, sessions, threadId, run, provider, providerSession, request };
});

const queuedToolFixture = Effect.fnUntraced(function* (prefix: string) {
  yield* seedProject({
    projectId: ProjectId.make(`${prefix}-project`),
    title: "Tool boundary",
    workspaceRoot: `/synthetic/${prefix}`,
    defaultModelSelection: modelSelection,
    createdAt: "2026-10-04T00:00:00.000Z",
  });
  const fixture = yield* selfSettlementFixture(prefix);
  const { orchestrator, sink, sessions, threadId, run, provider, providerSession } = fixture;
  const now = yield* DateTime.now;
  const turnId = ProviderTurnId.make(`${prefix}-turn`);
  const sessionSpy = vi.spyOn(sessions, "get").mockReturnValue(
    Effect.succeed(
      Option.some({
        providerSession,
        providerSessionId: providerSession.id,
        instanceId: modelSelection.instanceId,
        driver,
        events: Stream.empty,
        ensureThread: () => Effect.die("Unexpected ensure"),
        resumeThread: () => Effect.die("Unexpected resume"),
        startTurn: () => Effect.die("Unexpected start"),
        steerTurn: () => Effect.die("Effects are not executed by this test"),
        interruptTurn: () => Effect.die("Unexpected interrupt"),
        respondToRuntimeRequest: () => Effect.die("Unexpected response"),
        readThreadSnapshot: () => Effect.die("Unexpected snapshot"),
        rollbackThread: () => Effect.die("Unexpected rollback"),
        forkThread: () => Effect.die("Unexpected fork"),
      } satisfies ProviderAdapterV2SessionRuntime),
    ),
  );
  yield* Effect.addFinalizer(() => Effect.sync(() => sessionSpy.mockRestore()));
  const root = (yield* orchestrator.getThreadProjection(threadId)).nodes.find(
    (node) => node.id === run.rootNodeId,
  )!;
  yield* sink.write({
    events: [
      {
        id: EventId.make(`${prefix}-running`),
        type: "run.updated",
        threadId,
        runId: run.id,
        occurredAt: now,
        payload: { ...run, status: "running", startedAt: now },
      },
      {
        id: EventId.make(`${prefix}-root-running`),
        type: "node.updated",
        threadId,
        runId: run.id,
        occurredAt: now,
        payload: { ...root, status: "running", providerTurnId: turnId },
      },
      {
        id: EventId.make(`${prefix}-turn`),
        type: "provider-turn.updated",
        threadId,
        runId: run.id,
        occurredAt: now,
        payload: {
          id: turnId,
          providerThreadId: provider.id,
          nodeId: run.rootNodeId!,
          runAttemptId: run.activeAttemptId,
          nativeTurnRef: null,
          ordinal: 1,
          status: "running",
          startedAt: now,
          completedAt: null,
        },
      },
    ],
  });
  const queue = (
    text: string,
    selection: ModelSelection = modelSelection,
    eligibility: boolean | null = true,
  ) =>
    orchestrator.dispatch({
      type: "message.dispatch",
      threadId,
      commandId: CommandId.make(`${prefix}-queue-${text}`),
      ...(eligibility === null ? {} : { queuedToolBoundaryEligible: eligibility }),
      messageId: MessageId.make(`${prefix}-${text}`),
      createdBy: "user",
      creationSource: "web",
      text,
      modelSelection: selection,
      attachments: [],
      dispatchMode: { type: "queue_after_active" },
    });
  const tool = (
    id: string,
    status: OrchestrationV2ExecutionNode["status"] = "completed",
  ): OrchestrationV2ExecutionNode => ({
    id: NodeId.make(`${prefix}-${id}`),
    threadId,
    runId: run.id,
    parentNodeId: run.rootNodeId,
    rootNodeId: run.rootNodeId!,
    kind: "tool_call",
    status,
    countsForRun: false,
    providerThreadId: provider.id,
    providerTurnId: turnId,
    nativeItemRef: { driver, nativeId: id, strength: "strong" },
    runtimeRequestId: null,
    checkpointScopeId: null,
    startedAt: now,
    completedAt: status === "completed" ? now : null,
  });
  let eventOrdinal = 0;
  const writeTool = (node: OrchestrationV2ExecutionNode) =>
    sink.write({
      events: [
        {
          id: EventId.make(`${prefix}-node-event-${++eventOrdinal}`),
          type: "node.updated",
          threadId,
          runId: node.runId ?? run.id,
          occurredAt: now,
          payload: node,
        },
      ],
    });
  const react = (
    node: OrchestrationV2ExecutionNode,
    beforeLock: Effect.Effect<void> = Effect.void,
  ) =>
    Effect.gen(function* () {
      const reacted = yield* Deferred.make<void>();
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const executor = yield* ThreadCommandExecutor.ThreadCommandExecutor;
      const preflight = store.hasQueuedToolBoundaryWork;
      const withLock = executor.withLock;
      const preflightSpy = vi
        .spyOn(store, "hasQueuedToolBoundaryWork")
        .mockImplementation((id, runId) =>
          preflight(id, runId).pipe(
            Effect.tap((ready) =>
              id === threadId && !ready ? Deferred.succeed(reacted, undefined) : Effect.void,
            ),
          ),
        );
      let intercepted = false;
      const observedLock: ThreadCommandExecutor.ThreadCommandExecutor["Service"]["withLock"] = (
        id,
        effect,
      ) =>
        Effect.suspend(() => {
          const intercept = id === threadId && !intercepted;
          if (intercept) intercepted = true;
          return (intercept ? beforeLock : Effect.void).pipe(
            Effect.andThen(withLock(id, effect)),
            Effect.ensuring(id === threadId ? Deferred.succeed(reacted, undefined) : Effect.void),
          );
        });
      const lockSpy = vi.spyOn(executor, "withLock").mockImplementation(observedLock);
      return yield* Effect.gen(function* () {
        const stored = yield* writeTool(node);
        yield* Deferred.await(reacted);
        return stored[0]!;
      }).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            preflightSpy.mockRestore();
            lockSpy.mockRestore();
          }),
        ),
      );
    });
  return { ...fixture, turnId, now, queue, tool, writeTool, react, sessionSpy };
});

it.layer(TestLayer)("OrchestrationV2LayerLive", (it) => {
  it.effect.each([null, false] as const)(
    "retains legacy eligibility %s for a separate terminal turn",
    (eligibility) =>
      Effect.gen(function* () {
        const f = yield* queuedToolFixture(`queue-legacy-${eligibility}`);
        try {
          yield* f.queue("Legacy", modelSelection, eligibility);
          const before = (yield* f.orchestrator.getThreadProjection(f.threadId)).runs.find(
            (run) => run.status === "queued",
          )!;
          assert.equal(before.queuedToolBoundaryEligible, eligibility === null ? undefined : false);
          const boundary = yield* f.react(f.tool("legacy-tool"));
          const after = yield* f.orchestrator.getThreadProjection(f.threadId);
          assert.equal(after.runs.find((run) => run.id === before.id)?.status, "queued");
          const outbox = yield* EffectOutbox.EffectOutboxV2;
          assert.isEmpty(
            yield* outbox.listByCommandId(
              CommandId.make(
                `command:queue-tool-boundary:${before.id}:${f.turnId}:${boundary.sequence}`,
              ),
            ),
          );
        } finally {
          f.sessionSpy.mockRestore();
        }
      }),
  );
  it.effect("revalidates eligibility against run birth after a projection update", () =>
    Effect.gen(function* () {
      const f = yield* queuedToolFixture("queue-birth-policy");
      try {
        yield* f.queue("Legacy", modelSelection, false);
        const run = (yield* f.orchestrator.getThreadProjection(f.threadId)).runs.find(
          (run) => run.status === "queued",
        )!;
        yield* f.sink.write({
          events: [
            {
              id: EventId.make("queue-birth-policy-skew"),
              type: "run.updated",
              threadId: f.threadId,
              runId: run.id,
              occurredAt: f.now,
              payload: { ...run, queuedToolBoundaryEligible: true },
            },
          ],
        });
        yield* f.react(f.tool("birth-tool"));
        assert.equal(
          (yield* f.orchestrator.getThreadProjection(f.threadId)).runs.find(
            (candidate) => candidate.id === run.id,
          )?.status,
          "queued",
        );
      } finally {
        f.sessionSpy.mockRestore();
      }
    }),
  );
  it.effect("does not leapfrog a legacy queue head or continue past one", () =>
    Effect.gen(function* () {
      const f = yield* queuedToolFixture("queue-mixed-eligibility");
      try {
        yield* f.queue("Legacy", modelSelection, null);
        yield* f.queue("Eligible");
        yield* f.react(f.tool("mixed-first"));
        let projection = yield* f.orchestrator.getThreadProjection(f.threadId);
        assert.lengthOf(
          projection.runs.filter((run) => run.status === "queued"),
          2,
        );
        const eligible = projection.runs.find(
          (run) => run.userMessageId === MessageId.make("queue-mixed-eligibility-Eligible"),
        )!;
        yield* f.orchestrator.dispatch({
          type: "queued-run.reorder",
          commandId: CommandId.make("mixed-reorder"),
          threadId: f.threadId,
          runId: eligible.id,
          beforeRunId: projection.runs.find(
            (run) => run.status === "queued" && run.id !== eligible.id,
          )!.id,
        });
        const boundary = yield* f.react(f.tool("mixed-second"));
        projection = yield* f.orchestrator.getThreadProjection(f.threadId);
        assert.equal(projection.runs.find((run) => run.id === eligible.id)?.status, "cancelled");
        assert.lengthOf(
          projection.runs.filter((run) => run.status === "queued"),
          1,
        );
        const outbox = yield* EffectOutbox.EffectOutboxV2;
        const effects = yield* outbox.listByCommandId(
          CommandId.make(
            `command:queue-tool-boundary:${eligible.id}:${f.turnId}:${boundary.sequence}`,
          ),
        );
        assert.isTrue(
          effects.some(
            (effect) =>
              effect.request.type === "provider-turn.steer" &&
              effect.request.messageId === eligible.userMessageId,
          ),
        );
      } finally {
        f.sessionSpy.mockRestore();
      }
    }),
  );
  it.effect(
    "delivers queued owner messages FIFO at a native tool boundary while the root turn runs",
    () =>
      Effect.gen(function* () {
        const prefix = "queue-tool-boundary";
        yield* seedProject({
          projectId: ProjectId.make(`${prefix}-project`),
          title: "Tool boundary",
          workspaceRoot: `/synthetic/${prefix}`,
          defaultModelSelection: modelSelection,
          createdAt: "2026-10-04T00:00:00.000Z",
        });
        const { orchestrator, sink, sessions, threadId, run, provider, providerSession } =
          yield* selfSettlementFixture(prefix);
        const now = yield* DateTime.now;
        const turnId = ProviderTurnId.make(`${prefix}-turn`);
        const sessionSpy = vi.spyOn(sessions, "get").mockReturnValue(
          Effect.succeed(
            Option.some({
              providerSession,
              providerSessionId: providerSession.id,
              instanceId: modelSelection.instanceId,
              driver,
              events: Stream.empty,
              ensureThread: () => Effect.die("Unexpected ensure"),
              resumeThread: () => Effect.die("Unexpected resume"),
              startTurn: () => Effect.die("Unexpected start"),
              steerTurn: () => Effect.die("Effects are not executed by this test"),
              interruptTurn: () => Effect.die("Unexpected interrupt"),
              respondToRuntimeRequest: () => Effect.die("Unexpected response"),
              readThreadSnapshot: () => Effect.die("Unexpected snapshot"),
              rollbackThread: () => Effect.die("Unexpected rollback"),
              forkThread: () => Effect.die("Unexpected fork"),
            } satisfies ProviderAdapterV2SessionRuntime),
          ),
        );
        yield* Effect.addFinalizer(() => Effect.sync(() => sessionSpy.mockRestore()));
        yield* sink.write({
          events: [
            {
              id: EventId.make(`${prefix}-running`),
              type: "run.updated",
              threadId,
              runId: run.id,
              occurredAt: now,
              payload: { ...run, status: "running", startedAt: now },
            },
            {
              id: EventId.make(`${prefix}-turn`),
              type: "provider-turn.updated",
              threadId,
              runId: run.id,
              occurredAt: now,
              payload: {
                id: turnId,
                providerThreadId: provider.id,
                nodeId: run.rootNodeId!,
                runAttemptId: run.activeAttemptId,
                nativeTurnRef: null,
                ordinal: 1,
                status: "running",
                startedAt: now,
                completedAt: null,
              },
            },
          ],
        });
        const texts = Array.from({ length: 12 }, (_, index) => `Queued-${index + 1}`);
        for (const text of texts) {
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            threadId,
            commandId: CommandId.make(`${prefix}-${text}`),
            queuedToolBoundaryEligible: true,
            messageId: MessageId.make(`${prefix}-${text}`),
            createdBy: "user",
            creationSource: "web",
            text,
            attachments: [],
            dispatchMode: { type: "queue_after_active" },
          });
        }
        const queued = (yield* orchestrator.getThreadProjection(threadId)).runs.filter(
          (candidate) => candidate.status === "queued",
        );
        yield* orchestrator.dispatch({
          type: "queued-run.reorder",
          commandId: CommandId.make(`${prefix}-reorder`),
          threadId,
          runId: queued.at(-1)!.id,
          beforeRunId: queued[0]!.id,
        });
        const expectedMessageIds = [texts.at(-1)!, ...texts.slice(0, -1)].map((text) =>
          MessageId.make(`${prefix}-${text}`),
        );
        const delivered = yield* Queue.unbounded<MessageId>();
        yield* sink
          .stream({ threadId, afterSequence: yield* sink.latestSequence({ threadId }) })
          .pipe(
            Stream.runForEach((stored) =>
              stored.event.type === "turn-item.updated" &&
              stored.event.payload.type === "user_message" &&
              stored.event.payload.inputIntent === "promoted_queued_to_steer"
                ? Queue.offer(delivered, stored.event.payload.messageId)
                : Effect.void,
            ),
            Effect.forkScoped,
          );
        const completed = yield* sink.write({
          events: [
            {
              id: EventId.make(`${prefix}-tool-completed`),
              type: "node.updated",
              threadId,
              runId: run.id,
              occurredAt: now,
              payload: {
                id: NodeId.make(`${prefix}-tool`),
                threadId,
                runId: run.id,
                parentNodeId: run.rootNodeId,
                rootNodeId: run.rootNodeId!,
                kind: "tool_call",
                status: "completed",
                countsForRun: false,
                providerThreadId: provider.id,
                providerTurnId: turnId,
                nativeItemRef: { driver, nativeId: "tool-1", strength: "strong" },
                runtimeRequestId: null,
                checkpointScopeId: null,
                startedAt: now,
                completedAt: now,
              },
            },
          ],
        });
        assert.deepEqual(
          yield* Effect.forEach(expectedMessageIds, () => Queue.take(delivered)),
          expectedMessageIds,
        );
        const after = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(after.runs.find((candidate) => candidate.id === run.id)?.status, "running");
        assert.isFalse(after.runs.some((candidate) => candidate.status === "queued"));
        const outbox = yield* EffectOutbox.EffectOutboxV2;
        yield* outbox.cancelUnsettled({
          threadId,
          effectTypes: ["provider-turn.start"],
          reason: "Fixture start is already represented by the running provider turn",
        });
        for (const queuedRun of queued) {
          const effects = yield* outbox.listByCommandId(
            CommandId.make(
              `command:queue-tool-boundary:${queuedRun.id}:${turnId}:${completed[0]!.sequence}`,
            ),
          );
          assert.deepEqual(
            effects.map((effect) => effect.request.type),
            ["provider-turn.steer"],
          );
        }
        const workerId = `${prefix}-claim-worker`;
        for (const expectedMessageId of expectedMessageIds) {
          const effect = Option.getOrThrow(
            yield* outbox.claimNext({ workerId, leaseDurationMs: 30_000 }),
          );
          assert.equal(effect.threadId, threadId);
          assert.equal(effect.request.type, "provider-turn.steer");
          if (effect.request.type !== "provider-turn.steer") return;
          assert.equal(effect.request.messageId, expectedMessageId);
          assert.isTrue(yield* outbox.succeed({ effectId: effect.id, workerId }));
        }
      }),
    { timeout: 6000 },
  );

  it.effect("waits for the last foreground tool then leaves post-boundary additions queued", () =>
    Effect.gen(function* () {
      const f = yield* queuedToolFixture("queue-boundary-parallel");
      yield* f.queue("First");
      yield* f.writeTool(f.tool("second-tool", "waiting"));
      yield* f.react(f.tool("first-tool"));
      assert.equal(
        (yield* f.orchestrator.getThreadProjection(f.threadId)).runs.at(-1)?.status,
        "queued",
      );
      const completed = yield* f.react(
        f.tool("second-tool"),
        f.queue("Later").pipe(Effect.orDie, Effect.asVoid),
      );
      const after = yield* f.orchestrator.getThreadProjection(f.threadId);
      assert.equal(after.messages.find((message) => message.text === "First")?.runId, f.run.id);
      assert.equal(after.runs.at(-1)?.status, "queued");
      const later = after.runs.at(-1)!;
      yield* f.react(f.tool("second-tool"));
      assert.equal(
        (yield* f.orchestrator.getThreadProjection(f.threadId)).runs.at(-1)?.status,
        "queued",
      );
      yield* f.react(f.tool("fresh-tool"));
      assert.equal(
        (yield* f.orchestrator.getThreadProjection(f.threadId)).messages.find(
          (message) => message.text === "Later",
        )?.runId,
        f.run.id,
      );
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      assert.deepEqual(
        yield* outbox.listByCommandId(
          CommandId.make(
            `command:queue-tool-boundary:${later.id}:${f.turnId}:${completed.sequence}`,
          ),
        ),
        [],
      );
    }),
  );

  it.effect("keeps content edited after a boundary queued until a fresh completion", () =>
    Effect.gen(function* () {
      const f = yield* queuedToolFixture("queue-boundary-edit");
      yield* f.queue("First");
      yield* f.queue("Second");
      const first = (yield* f.orchestrator.getThreadProjection(f.threadId)).runs.find(
        (run) => run.status === "queued",
      )!;
      yield* f.react(
        f.tool("before-edit"),
        f.orchestrator
          .dispatch({
            type: "queued-run.edit",
            threadId: f.threadId,
            commandId: CommandId.make("queue-boundary-edit-update"),
            runId: first.id,
            text: "Edited",
            attachments: [],
          })
          .pipe(Effect.orDie, Effect.asVoid),
      );
      assert.equal(
        (yield* f.orchestrator.getThreadProjection(f.threadId)).runs.filter(
          (run) => run.status === "queued",
        ).length,
        2,
      );
      yield* f.react(f.tool("after-edit"));
      assert.isFalse(
        (yield* f.orchestrator.getThreadProjection(f.threadId)).runs.some(
          (run) => run.status === "queued",
        ),
      );
    }),
  );

  it.effect.each([
    "subagent",
    "old-turn",
    "old-run",
    "held",
    "approval",
    "runtime-mode",
    "interaction-mode",
    "incompatible-model",
    "maintenance",
    "restart-only",
    "live-restart-only",
  ] as const)(
    "keeps the FIFO queue intact at a %s boundary",
    (condition) =>
      Effect.gen(function* () {
        const f = yield* queuedToolFixture(`queue-boundary-${condition}`);
        yield* f.queue(
          condition === "maintenance" ? "/compact" : "First",
          condition === "incompatible-model"
            ? { ...modelSelection, model: "different-model" }
            : modelSelection,
        );
        yield* f.queue("Second");
        let node = f.tool("boundary");
        const projection = yield* f.orchestrator.getThreadProjection(f.threadId);
        const first = projection.runs.find((run) => run.status === "queued")!;
        if (condition === "subagent") {
          const child = { ...f.tool("subagent-root", "running"), kind: "subagent" as const };
          yield* f.writeTool(child);
          node = { ...node, parentNodeId: child.id };
        } else if (condition === "old-turn") {
          node = { ...node, providerTurnId: ProviderTurnId.make("previous-turn") };
        } else if (condition === "old-run") {
          node = { ...node, runId: RunId.make("previous-run") };
        } else if (condition === "held") {
          yield* f.sink.write({
            events: [
              {
                id: EventId.make("queue-boundary-held-event"),
                type: "run.updated",
                threadId: f.threadId,
                runId: first.id,
                occurredAt: f.now,
                payload: { ...first, queueHeld: true },
              },
            ],
          });
        } else if (condition === "approval") {
          yield* f.sink.write({
            events: [
              {
                id: EventId.make("queue-boundary-approval-event"),
                type: "runtime-request.updated",
                threadId: f.threadId,
                runId: f.run.id,
                occurredAt: f.now,
                payload: {
                  id: RuntimeRequestId.make("queue-boundary-approval-request"),
                  nodeId: f.run.rootNodeId!,
                  providerTurnId: f.turnId,
                  nativeRequestRef: null,
                  kind: "permission",
                  status: "pending",
                  responseCapability: { type: "live", providerSessionId: f.providerSession.id },
                  createdAt: f.now,
                  resolvedAt: null,
                },
              },
            ],
          });
        } else if (condition === "runtime-mode" || condition === "interaction-mode") {
          yield* f.orchestrator.dispatch(
            condition === "runtime-mode"
              ? {
                  type: "thread.runtime-mode.set",
                  threadId: f.threadId,
                  commandId: CommandId.make(`${f.threadId}-mode`),
                  runtimeMode: "approval-required",
                }
              : {
                  type: "thread.interaction-mode.set",
                  threadId: f.threadId,
                  commandId: CommandId.make(`${f.threadId}-mode`),
                  interactionMode: "plan",
                },
          );
        } else if (condition === "restart-only" || condition === "live-restart-only") {
          const providerSession = {
            ...f.providerSession,
            capabilities: {
              ...f.providerSession.capabilities,
              turns: {
                ...f.providerSession.capabilities.turns,
                supportsActiveSteering: false,
                supportsSteeringByInterruptRestart: true,
              },
            },
          };
          if (condition === "restart-only")
            yield* f.sink.write({
              events: [
                {
                  id: EventId.make(`${f.threadId}-capabilities`),
                  type: "provider-session.updated",
                  threadId: f.threadId,
                  occurredAt: f.now,
                  payload: providerSession,
                },
              ],
            });
          f.sessionSpy.mockReturnValue(
            Effect.succeed(
              Option.some({
                ...Option.getOrThrow(yield* f.sessions.get(f.providerSession.id)),
                providerSession,
              }),
            ),
          );
        }
        const stored = yield* f.react(node);
        const after = yield* f.orchestrator.getThreadProjection(f.threadId);
        assert.equal(after.runs.filter((run) => run.status === "queued").length, 2);
        assert.equal(after.runs.find((run) => run.id === f.run.id)?.status, "running");
        assert.isFalse(
          after.turnItems.some(
            (item) =>
              item.type === "user_message" && item.inputIntent === "promoted_queued_to_steer",
          ),
        );
        const outbox = yield* EffectOutbox.EffectOutboxV2;
        assert.deepEqual(
          yield* outbox.listByCommandId(
            CommandId.make(
              `command:queue-tool-boundary:${first.id}:${f.turnId}:${stored.sequence}`,
            ),
          ),
          [],
        );
      }),
    { timeout: 6000 },
  );

  it.effect(
    "preserves terminal fallback for an incompatible FIFO head",
    () =>
      Effect.gen(function* () {
        const f = yield* queuedToolFixture("queue-boundary-terminal-fallback");
        yield* f.queue("Different", { ...modelSelection, model: "different-model" });
        yield* f.queue("Second");
        yield* f.react(f.tool("tool"));
        const queued = (yield* f.orchestrator.getThreadProjection(f.threadId)).runs.filter(
          (run) => run.status === "queued",
        );
        const started = yield* Queue.unbounded<RunId>();
        yield* f.sink
          .stream({ threadId: f.threadId, afterSequence: yield* f.sink.latestSequence() })
          .pipe(
            Stream.runForEach((stored) =>
              stored.event.type === "run.updated" && stored.event.payload.status === "starting"
                ? Queue.offer(started, stored.event.payload.id)
                : Effect.void,
            ),
            Effect.forkScoped,
          );
        yield* f.sink.write({
          events: [
            {
              id: EventId.make(`${f.threadId}-terminal`),
              type: "run.updated",
              threadId: f.threadId,
              runId: f.run.id,
              occurredAt: f.now,
              payload: { ...f.run, status: "completed", startedAt: f.now, completedAt: f.now },
            },
          ],
        });
        assert.equal(yield* Queue.take(started), queued[0]!.id);
        const after = yield* f.orchestrator.getThreadProjection(f.threadId);
        assert.equal(after.runs.find((run) => run.id === queued[1]!.id)?.status, "queued");
      }),
    { timeout: 6000 },
  );

  it.effect(
    "excludes live child tools and synthetic terminal tool states from root boundaries",
    () =>
      Effect.gen(function* () {
        const f = yield* queuedToolFixture("queue-boundary-child-tools");
        yield* f.queue("First");
        const child = { ...f.tool("child-root", "running"), kind: "subagent" as const };
        yield* f.writeTool(child);
        yield* f.writeTool({ ...f.tool("child-tool", "running"), parentNodeId: child.id });
        yield* f.react(f.tool("root-tool"));
        assert.equal(
          (yield* f.orchestrator.getThreadProjection(f.threadId)).messages.find(
            (message) => message.text === "First",
          )?.runId,
          f.run.id,
        );
        yield* f.queue("Second");
        for (const status of ["failed", "cancelled", "interrupted"] as const) {
          const node = f.tool(`synthetic-${status}`, status);
          yield* f.writeTool(node);
          assert.isNull(
            queuedToolBoundaryTarget(yield* f.orchestrator.getThreadProjection(f.threadId), node),
          );
        }
        assert.equal(
          (yield* f.orchestrator.getThreadProjection(f.threadId)).runs.at(-1)?.status,
          "queued",
        );
        yield* f.react(f.tool("fresh-root-tool"));
        assert.equal(
          (yield* f.orchestrator.getThreadProjection(f.threadId)).messages.find(
            (message) => message.text === "Second",
          )?.runId,
          f.run.id,
        );
      }),
    { timeout: 6000 },
  );

  it.effect(
    "defers self settlement through checkpoint completion and replays the original run after successor work",
    () =>
      Effect.gen(function* () {
        const { orchestrator, sink, threadId, run, provider, providerSession, request } =
          yield* selfSettlementFixture("runtime-self-settle");
        const accepted = yield* orchestrator.requestSelfSettlement(request);
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              orchestrator.requestSelfSettlement({
                ...request,
                mcpCredentialId: "another-credential",
              }),
            ),
          ),
        );
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              orchestrator.requestSelfSettlement({
                ...request,
                providerInstanceId: alternateInstanceId,
              }),
            ),
          ),
        );
        assert.equal(accepted.runId, run.id);
        assert.equal(
          (yield* orchestrator.getThreadProjection(threadId)).runs[0]!.status,
          "starting",
        );
        assert.equal(
          (yield* orchestrator.getThreadProjection(threadId)).thread.settledOverride,
          null,
        );
        const now = yield* DateTime.now;
        yield* sink.write({
          events: [
            {
              id: EventId.make("self-waiting"),
              type: "run.updated",
              threadId,
              runId: run.id,
              occurredAt: now,
              payload: { ...run, status: "waiting" },
            },
          ],
        });
        assert.equal(
          (yield* orchestrator.getThreadProjection(threadId)).thread.selfSettlement?.runId,
          run.id,
        );
        assert.equal(
          (yield* orchestrator.getThreadProjection(threadId)).thread.settledOverride,
          null,
        );
        const settledEvents = yield* Queue.unbounded<void>();
        const afterSequence = yield* orchestrator.getThreadEventSequence(threadId);
        yield* sink.stream({ threadId, afterSequence }).pipe(
          Stream.runForEach((stored) =>
            stored.event.type === "thread.settled"
              ? Queue.offer(settledEvents, undefined)
              : Effect.void,
          ),
          Effect.forkScoped,
        );
        yield* sink.write({
          events: [
            {
              id: EventId.make("self-completed"),
              type: "run.updated",
              threadId,
              runId: run.id,
              occurredAt: now,
              payload: { ...run, status: "completed", completedAt: now },
            },
          ],
        });
        yield* Queue.take(settledEvents);
        const settled = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(settled.thread.settledOverride, "settled");
        assert.isNull(settled.thread.selfSettlement);
        assert.isFalse(
          settled.providerSessions.some((session) => session.id === provider.providerSessionId),
        );
        const outbox = yield* EffectOutbox.EffectOutboxV2;
        assert.deepEqual(
          (yield* outbox.listByCommandId(CommandId.make(`${request.commandId}:settle`))).map(
            (effect) => effect.request,
          ),
          [
            {
              type: "provider-session.detach",
              providerSessionId: providerSession.id,
              detail: "Thread settled.",
            },
          ],
        );
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("self-successor"),
          threadId,
          createdBy: "user",
          creationSource: "web",
          messageId: MessageId.make("self-successor-message"),
          text: "New work",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
        });
        assert.deepEqual(yield* orchestrator.requestSelfSettlement(request), accepted);
        const successor = yield* orchestrator.getThreadProjection(threadId);
        assert.notEqual(successor.runs.at(-1)!.id, run.id);
        assert.isNull(successor.thread.selfSettlement);
        assert.equal(successor.thread.settledOverride, null);
      }),
  );

  it.effect.each(["cancelled", "failed", "queue", "steer"] as const)(
    "does not self settle after %s even if the original run later completes",
    (invalidation) =>
      Effect.gen(function* () {
        const prefix = `self-invalidate-${invalidation}`;
        const { orchestrator, sink, sessions, threadId, run, provider, providerSession, request } =
          yield* selfSettlementFixture(prefix);
        const executor = yield* ThreadCommandExecutor.ThreadCommandExecutor;
        const outbox = yield* EffectOutbox.EffectOutboxV2;
        const now = yield* DateTime.now;
        yield* orchestrator.requestSelfSettlement(request);

        const completeReaction = Effect.fnUntraced(function* (
          status: "cancelled" | "failed" | "completed",
        ) {
          const reacted = yield* Deferred.make<void>();
          const withLock = executor.withLock;
          // For this parentless thread, the terminal reactor first finalizes delivery,
          // then consumes settlement and promotes the queue under a second lock.
          let completedLocks = 0;
          const observeLock: ThreadCommandExecutor.ThreadCommandExecutor["Service"]["withLock"] = (
            key,
            effect,
          ) =>
            withLock(key, effect).pipe(
              Effect.tap(() =>
                key === threadId && ++completedLocks === 2
                  ? Deferred.succeed(reacted, undefined)
                  : Effect.void,
              ),
            );
          const lockSpy = vi.spyOn(executor, "withLock").mockImplementation(observeLock);
          yield* Effect.gen(function* () {
            yield* sink.write({
              events: [
                {
                  id: EventId.make(`${prefix}-${status}`),
                  type: "run.updated",
                  threadId,
                  runId: run.id,
                  occurredAt: now,
                  payload: { ...run, status, completedAt: now },
                },
              ],
            });
            yield* Deferred.await(reacted);
          }).pipe(Effect.ensuring(Effect.sync(() => lockSpy.mockRestore())));
        });

        if (invalidation === "cancelled" || invalidation === "failed") {
          yield* completeReaction(invalidation);
        } else {
          if (invalidation === "steer") {
            yield* sink.write({
              events: [
                {
                  id: EventId.make(`${prefix}-running`),
                  type: "run.updated",
                  threadId,
                  runId: run.id,
                  occurredAt: now,
                  payload: { ...run, status: "running", startedAt: now },
                },
                {
                  id: EventId.make(`${prefix}-turn`),
                  type: "provider-turn.updated",
                  threadId,
                  runId: run.id,
                  occurredAt: now,
                  payload: {
                    id: ProviderTurnId.make(`${prefix}-turn`),
                    providerThreadId: provider.id,
                    nodeId: run.rootNodeId!,
                    runAttemptId: run.activeAttemptId,
                    nativeTurnRef: null,
                    ordinal: 1,
                    status: "running",
                    startedAt: now,
                    completedAt: null,
                  },
                },
              ],
            });
            const sessionSpy = vi
              .spyOn(sessions, "get")
              .mockReturnValue(
                Effect.succeed(Option.some({ providerSession } as ProviderAdapterV2SessionRuntime)),
              );
            yield* Effect.addFinalizer(() => Effect.sync(() => sessionSpy.mockRestore()));
          }
          const commandId = CommandId.make(`${prefix}-new-work`);
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId,
            threadId,
            createdBy: "user",
            creationSource: "web",
            messageId: MessageId.make(`${prefix}-new-work`),
            text: "Keep working",
            attachments: [],
            dispatchMode: {
              type: invalidation === "queue" ? "queue_after_active" : "start_immediately",
            },
            ...(invalidation === "steer" ? { deliveryIntent: "auto" as const } : {}),
          });
          const updated = yield* orchestrator.getThreadProjection(threadId);
          if (invalidation === "queue") {
            assert.equal(updated.runs.at(-1)?.status, "queued");
          } else {
            assert.lengthOf(updated.runs, 1);
            assert.equal(
              (yield* outbox.listByCommandId(commandId))[0]?.request.type,
              "provider-turn.steer",
            );
          }
        }
        assert.isNull((yield* orchestrator.getThreadProjection(threadId)).thread.selfSettlement);
        yield* completeReaction("completed");
        const projection = yield* orchestrator.getThreadProjection(threadId);
        assert.isNull(projection.thread.settledOverride);
        assert.isNull(projection.thread.selfSettlement);
        assert.deepEqual(
          yield* outbox.listByCommandId(CommandId.make(`${request.commandId}:settle`)),
          [],
        );
        assert.lengthOf(
          Array.from(
            yield* sink
              .readByCommandId({
                commandId: CommandId.make(`${request.commandId}:settle`),
              })
              .pipe(Stream.runCollect),
          ),
          0,
        );
      }),
  );
});

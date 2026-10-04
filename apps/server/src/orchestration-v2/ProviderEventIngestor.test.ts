import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  MessageId,
  CommandId,
  CheckpointScopeId,
  CheckpointId,
  CheckpointRef,
  type ModelSelection,
  NodeId,
  type OrchestrationV2AppThread,
  OrchestrationV2DomainEvent,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2CheckpointScope,
  type OrchestrationV2PlanArtifact,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2Run,
  type OrchestrationV2RunAttempt,
  type OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2TurnItem,
  type OrchestrationV2Subagent,
  ProviderDriverKind,
  ProviderInstanceId,
  PlanId,
  RunAttemptId,
  RunId,
  RuntimeRequestId,
  TurnItemId,
  ThreadId,
  EnvironmentId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as Ref from "effect/Ref";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import { HttpServer } from "effect/unstable/http";
import * as NetAddress from "effect/unstable/net/NetAddress";

import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import * as EventSink from "./EventSink.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import { ClaudeProviderCapabilitiesV2 } from "./Adapters/ClaudeAdapterV2.ts";
import * as EventStore from "./EventStore.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as IdAllocator from "./IdAllocator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as TurnItemPositionStore from "./TurnItemPositionStore.ts";
import * as ProviderEventIngestor from "./ProviderEventIngestor.ts";
import * as RunExecutionService from "./RunExecutionService.ts";
import * as RunFinalizationService from "./RunFinalizationService.ts";
import {
  ProviderAdapterProtocolError,
  type ProviderRuntimeBinding,
  type ProviderAdapterV2Event,
  type ProviderAdapterV2SessionRuntime,
  type ProviderAdapterV2Shape,
} from "./ProviderAdapter.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as McpSessionRegistry from "../mcp/McpSessionRegistry.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as CheckpointService from "./CheckpointService.ts";
import * as CheckpointCaptureService from "./CheckpointCaptureService.ts";
import * as ServerSettings from "../serverSettings.ts";
import { makeAssistantStreamingFilter } from "./assistantStreaming.ts";
import {
  readProviderEventOrigin,
  stampProviderEvent,
  type ClaudeBufferedSubagentCompletionDerivationV1,
} from "./ProviderEventOrigin.ts";
import { makeProviderFailure } from "./ProviderFailure.ts";
import {
  makeProviderEventRoutingState,
  type ProviderEventRouteIdentity,
  routeProviderEvent,
  selectInheritedBackgroundTurnItems,
} from "./RunExecutionService.ts";

const TestDatabaseLayer = SqlitePersistenceMemory;
const TestStoresLayer = Layer.merge(EventStore.layer, ProjectionStore.layer).pipe(
  Layer.provide(TestDatabaseLayer),
);

const TestEventSinkLayer = EventSink.layer.pipe(
  Layer.provide(Layer.mergeAll(TestStoresLayer, TestDatabaseLayer)),
);

const TestLayer = Layer.mergeAll(
  TestStoresLayer,
  TestEventSinkLayer,
  EffectOutbox.layer.pipe(Layer.provide(TestDatabaseLayer)),
  TurnItemPositionStore.layer.pipe(Layer.provide(TestDatabaseLayer)),
  IdAllocator.layer,
  ProviderEventIngestor.layer.pipe(
    Layer.provide(Layer.mergeAll(TestStoresLayer, TestEventSinkLayer, IdAllocator.layer)),
  ),
);
const modelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5.4",
} satisfies ModelSelection;
const CODEX_DRIVER = ProviderDriverKind.make("codex");

function threadCreatedEvent(
  now: DateTime.Utc,
): Effect.Effect<
  Extract<OrchestrationV2DomainEvent, { readonly type: "thread.created" }>,
  IdAllocator.IdAllocatorV2Error,
  IdAllocator.IdAllocatorV2
> {
  return Effect.gen(function* () {
    const idAllocator = yield* IdAllocator.IdAllocatorV2;
    const projectId = yield* idAllocator.allocate.project({
      fixtureName: "provider-event-ingestor",
    });
    const threadId = yield* idAllocator.allocate.thread({
      fixtureName: "provider-event-ingestor",
      projectId,
    });
    const providerThreadId = idAllocator.derive.providerThread({
      driver: CODEX_DRIVER,
      nativeThreadId: "native-thread",
    });
    const thread: OrchestrationV2AppThread = {
      createdBy: "user",
      creationSource: "web",
      id: threadId,
      projectId,
      title: "Provider event ingestor",
      providerInstanceId: modelSelection.instanceId,
      modelSelection: modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      branchPullRequest: null,
      activeOrderKey: null,
      activeProviderThreadId: providerThreadId,
      lineage: {
        parentThreadId: null,
        relationshipToParent: null,
        rootThreadId: threadId,
      },
      forkedFrom: null,
      createdAt: now,
      updatedAt: now,
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      lastVisitedAt: null,
      deletedAt: null,
    };

    return {
      id: yield* idAllocator.allocate.event({ threadId }),
      type: "thread.created",
      threadId,
      occurredAt: now,
      payload: thread,
    };
  });
}

const layer = it.layer(TestLayer);

const sameRuntimeBinding = (left: ProviderRuntimeBinding, right: ProviderRuntimeBinding) =>
  left.threadId === right.threadId &&
  left.providerThreadId === right.providerThreadId &&
  left.providerSessionId === right.providerSessionId &&
  left.instanceId === right.instanceId &&
  left.runtimeGeneration === right.runtimeGeneration &&
  left.nativeThreadId === right.nativeThreadId;

const makeBufferedOutputFixture = Effect.fn("makeBufferedOutputFixture")(function* (
  provider = "codex",
) {
  const ids = yield* IdAllocator.IdAllocatorV2;
  const sink = yield* EventSink.EventSinkV2;
  const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const positions = yield* TurnItemPositionStore.TurnItemPositionStoreV2;
  const store = yield* EventStore.EventStoreV2;
  const now = yield* DateTime.now;
  const created = yield* threadCreatedEvent(now);
  const threadId = created.threadId;
  const driver = ProviderDriverKind.make(provider);
  const instanceId = ProviderInstanceId.make(provider);
  const selection = {
    instanceId,
    model:
      provider === "claudeAgent"
        ? "claude-sonnet"
        : provider === "opencode"
          ? "anthropic/claude-sonnet"
          : "gpt-5.4",
  };
  const providerSessionId = yield* ids.allocate.providerSession({
    providerInstanceId: instanceId,
    threadId,
  });
  const providerThreadId = ids.derive.providerThread({
    driver,
    nativeThreadId: `${threadId}:output`,
  });
  const providerTurnId = ids.derive.providerTurn({ driver, nativeTurnId: `${threadId}:turn` });
  const runId = RunId.make(`${threadId}:run`);
  const attemptId = RunAttemptId.make(`${threadId}:attempt`);
  const nodeId = NodeId.make(`${threadId}:root`);
  const appThread = {
    ...created.payload,
    providerInstanceId: instanceId,
    modelSelection: selection,
    activeProviderThreadId: providerThreadId,
  };
  const session: OrchestrationV2ProviderSession = {
    id: providerSessionId,
    driver,
    providerInstanceId: instanceId,
    status: "running",
    cwd: "/fixture",
    model: selection.model,
    capabilities:
      provider === "claudeAgent" ? ClaudeProviderCapabilitiesV2 : CodexProviderCapabilitiesV2,
    createdAt: now,
    updatedAt: now,
    lastError: null,
  };
  const providerThread: OrchestrationV2ProviderThread = {
    id: providerThreadId,
    driver,
    providerInstanceId: instanceId,
    providerSessionId,
    appThreadId: threadId,
    ownerNodeId: nodeId,
    nativeThreadRef: { driver, nativeId: `${threadId}:output`, strength: "strong" },
    nativeConversationHeadRef: null,
    status: "active",
    firstRunOrdinal: 1,
    lastRunOrdinal: 1,
    handoffIds: [],
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
  };
  const run: OrchestrationV2Run = {
    id: runId,
    threadId,
    ordinal: 1,
    providerInstanceId: instanceId,
    modelSelection: selection,
    providerThreadId,
    userMessageId: MessageId.make(`${threadId}:user`),
    rootNodeId: nodeId,
    activeAttemptId: attemptId,
    status: "running",
    requestedAt: now,
    startedAt: now,
    completedAt: null,
    checkpointId: null,
    contextHandoffId: null,
  };
  const attempt: OrchestrationV2RunAttempt = {
    id: attemptId,
    runId,
    attemptOrdinal: 1,
    rootNodeId: nodeId,
    providerInstanceId: instanceId,
    providerThreadId,
    providerTurnId,
    nativeThreadId: `${threadId}:output`,
    reason: "initial",
    status: "running",
    startedAt: now,
    completedAt: null,
  };
  const turn: OrchestrationV2ProviderTurn = {
    id: providerTurnId,
    providerThreadId,
    nodeId,
    runAttemptId: attemptId,
    nativeTurnRef: { driver, nativeId: `${threadId}:turn`, strength: "strong" },
    ordinal: 1,
    status: "running",
    startedAt: now,
    completedAt: null,
  };
  const node: OrchestrationV2ExecutionNode = {
    id: nodeId,
    threadId,
    runId,
    parentNodeId: null,
    rootNodeId: nodeId,
    kind: "root_turn",
    status: "running",
    countsForRun: true,
    providerThreadId,
    providerTurnId,
    nativeItemRef: null,
    runtimeRequestId: null,
    checkpointScopeId: null,
    startedAt: now,
    completedAt: null,
  };
  const seed: Array<OrchestrationV2DomainEvent> = [{ ...created, payload: appThread }];
  for (const event of [
    { type: "provider-session.attached" as const, payload: session },
    { type: "provider-thread.updated" as const, payload: providerThread },
    { type: "run.updated" as const, payload: run },
    { type: "run-attempt.updated" as const, payload: attempt },
    { type: "provider-turn.updated" as const, payload: turn },
    { type: "node.updated" as const, payload: node },
  ])
    seed.push({
      id: yield* ids.allocate.event({ threadId }),
      threadId,
      runId,
      occurredAt: now,
      ...event,
    });
  yield* sink.write({ events: seed });
  const binding = {
    threadId,
    providerThreadId,
    providerSessionId,
    instanceId,
    nativeThreadId: `${threadId}:output`,
    runtimeGeneration: "fixture-output-generation",
  };
  const registered = yield* sink.registerProviderRuntime({
    expectedBinding: { ...binding, driver, runtimeGeneration: null },
    expectedEvidenceRevision: 0,
    actualBinding: binding,
  });
  assert.isTrue(registered.committed);
  const handle = {};
  const resident = yield* Ref.make({ handle, binding });
  const revalidateCurrentOwner = Effect.gen(function* () {
    const current = yield* Ref.get(resident);
    if (current.handle !== handle || !sameRuntimeBinding(current.binding, binding))
      return yield* Effect.fail("The synthetic resident runtime was replaced.");
  });
  const input = { providerSessionId, providerInstanceId: instanceId, threadId, runId, nodeId };
  const owner = { binding, runId, attemptId, providerTurnId };
  const origin = {
    producer: {
      token: handle,
      driver,
      instanceId,
      providerSessionId,
      runtimeGeneration: binding.runtimeGeneration,
      revalidateCurrent: revalidateCurrentOwner,
    },
    turn: owner,
  };
  const stamp = <Event extends ProviderAdapterV2Event>(event: Event) =>
    stampProviderEvent(event, origin);
  const assistantNode: OrchestrationV2ExecutionNode = {
    ...node,
    id: NodeId.make(`${threadId}:assistant-node`),
    parentNodeId: node.id,
    kind: "assistant_message",
    countsForRun: false,
  };
  const rawNode = stamp({ type: "node.updated" as const, driver, node: assistantNode });
  const rawMessage = (text: string) =>
    stamp({
      type: "message.updated" as const,
      driver,
      message: {
        createdBy: "agent" as const,
        creationSource: "provider" as const,
        id: MessageId.make(`${threadId}:assistant`),
        threadId,
        runId,
        nodeId: assistantNode.id,
        role: "assistant" as const,
        text,
        attachments: [],
        streaming: true,
        createdAt: now,
        updatedAt: now,
      },
    });
  const capture = (text: string) =>
    Effect.gen(function* () {
      yield* ingestor.captureAssistantOutput({
        ...input,
        owner,
        revalidateCurrentOwner,
        event: rawNode,
      });
      const event = rawMessage(text);
      yield* ingestor.captureAssistantOutput({ ...input, owner, revalidateCurrentOwner, event });
      const filter = makeAssistantStreamingFilter("paragraph");
      assert.isNull(filter(rawNode, DateTime.toEpochMillis(now)));
      assert.isNull(filter(event, DateTime.toEpochMillis(now)));
      const projection = yield* projections.getThreadProjection(threadId);
      assert.lengthOf(projection.messages, 0);
      assert.isFalse(projection.nodes.some((candidate) => candidate.id === assistantNode.id));
    });
  const flush = (target?: typeof providerTurnId) =>
    ingestor.flushAssistantOutput({
      binding,
      ...(target === undefined ? {} : { providerTurnId: target }),
      revalidateCurrentOwner,
    });
  const updateSession = (status: OrchestrationV2ProviderSession["status"]) =>
    ingestor.ingestNormalized({
      ...input,
      event: {
        type: "provider_session.updated",
        driver,
        providerSession: {
          ...session,
          status,
          lastError: status === "error" ? "transport closed" : null,
        },
      },
    });
  return {
    ids,
    sink,
    ingestor,
    projections,
    positions,
    store,
    now,
    threadId,
    driver,
    instanceId,
    selection,
    providerSessionId,
    providerThread,
    providerTurnId,
    run,
    attempt,
    node,
    session,
    binding,
    owner,
    resident,
    revalidateCurrentOwner,
    input,
    origin,
    stamp,
    assistantNode,
    rawNode,
    rawMessage,
    capture,
    flush,
    updateSession,
  };
});

const runBufferedOutputFixture = Effect.fn("runBufferedOutputFixture")(function* (
  fixture: Omit<Effect.Success<ReturnType<typeof makeBufferedOutputFixture>>, "origin">,
  text: string,
  terminal: Extract<ProviderAdapterV2Event, { readonly type: "turn.terminal" }>,
  options: {
    readonly beforeTerminal?: Effect.Effect<
      void,
      EventSink.EventSinkV2Error | IdAllocator.IdAllocatorV2Error
    >;
    readonly events?: Stream.Stream<ProviderAdapterV2Event, unknown>;
    readonly runtimeGeneration?: string;
    readonly session?: ProviderAdapterV2SessionRuntime;
  } = {},
) {
  const subscriptionClosed = yield* Deferred.make<void>();
  const checkpointScope: OrchestrationV2CheckpointScope = {
    id: CheckpointScopeId.make(`${fixture.threadId}:scope`),
    threadId: fixture.threadId,
    runId: fixture.run.id,
    nodeId: fixture.node.id,
    parentScopeId: null,
    providerThreadId: fixture.providerThread.id,
    kind: "root_run",
    ordinalWithinParent: 0,
    advancesAppRunCount: true,
    cwd: "/fixture",
    createdAt: fixture.now,
  };
  yield* fixture.sink.write({
    events: [
      {
        id: yield* fixture.ids.allocate.event({ threadId: fixture.threadId }),
        type: "checkpoint-scope.created",
        threadId: fixture.threadId,
        runId: fixture.run.id,
        nodeId: fixture.node.id,
        occurredAt: fixture.now,
        payload: checkpointScope,
      },
    ],
  });
  const serviceContext = yield* Layer.build(
    RunExecutionService.layer.pipe(
      Layer.provide(
        Layer.mock(CheckpointService.CheckpointServiceV2)({ captureBaseline: () => Effect.void }),
      ),
      Layer.provide(ServerSettings.layerTest({ responseStreamingMode: "paragraph" })),
    ),
  ).pipe(
    Effect.provideService(RunFinalizationService.RunFinalizationObserver, {
      refresh: () => Effect.void,
      refreshAfterTurn: () => Effect.void,
    }),
  );
  const service = yield* RunExecutionService.RunExecutionServiceV2.pipe(
    Effect.provide(serviceContext),
  );
  const currentTurn = (yield* fixture.projections.getThreadRecords(fixture.threadId, [
    "providerTurns",
  ])).providerTurns.find((turn) => turn.id === fixture.providerTurnId)!;
  const terminalTurn = {
    id: fixture.providerTurnId,
    providerThreadId: fixture.providerThread.id,
    nodeId: fixture.node.id,
    runAttemptId: fixture.attempt.id,
    nativeTurnRef: {
      driver: fixture.driver,
      nativeId: `${fixture.threadId}:turn`,
      strength: "strong" as const,
    },
    ordinal: currentTurn.ordinal,
    status: terminal.status,
    startedAt: fixture.now,
    completedAt: fixture.now,
  };
  const events =
    options.events ??
    Stream.concat(
      Stream.concat(
        Stream.fromIterable([fixture.rawNode, fixture.rawMessage(text)]),
        Stream.fromEffect(options.beforeTerminal ?? Effect.void).pipe(Stream.drain),
      ),
      Stream.fromIterable([
        fixture.stamp({
          type: "provider_turn.updated" as const,
          driver: fixture.driver,
          providerTurn: terminalTurn,
        }),
        fixture.stamp(terminal),
      ]),
    );
  const managedSession: ProviderAdapterV2SessionRuntime | undefined =
    options.session === undefined
      ? undefined
      : Object.create(options.session, {
          subscribeEvents: {
            value: options.session.subscribeEvents!.pipe(
              Effect.map((subscription) => ({
                ...subscription,
                close: subscription.close.pipe(
                  Effect.andThen(Deferred.succeed(subscriptionClosed, undefined)),
                  Effect.asVoid,
                ),
              })),
            ),
          },
        });
  yield* service.startRootRun({
    commandId: CommandId.make(`${fixture.threadId}:terminal`),
    appThread: (yield* fixture.projections.getThreadProjection(fixture.threadId)).thread,
    providerSessionId: fixture.providerSessionId,
    session:
      managedSession ??
      ({
        providerSessionId: fixture.providerSessionId,
        instanceId: fixture.instanceId,
        driver: fixture.driver,
        runtimeGeneration: options.runtimeGeneration ?? fixture.binding.runtimeGeneration,
        events,
        subscribeEvents: Effect.succeed({
          events,
          close: Deferred.succeed(subscriptionClosed, undefined).pipe(Effect.asVoid),
        }),
        startTurn: () => Effect.void,
      } as unknown as ProviderAdapterV2SessionRuntime),
    run: fixture.run,
    rootNode: fixture.node,
    checkpointScope,
    providerThread: fixture.providerThread,
    attempt: fixture.attempt,
    attemptId: fixture.attempt.id,
    providerTurnOrdinal: currentTurn.ordinal,
    message: {
      messageId: fixture.run.userMessageId,
      text: "Produce a partial answer.",
      attachments: [],
      createdBy: "user",
      creationSource: "web",
    },
    modelSelection: fixture.selection,
    runtimePolicy: { runtimeMode: "full-access", interactionMode: "default", cwd: "/fixture" },
  });
  yield* Deferred.await(subscriptionClosed).pipe(Effect.timeout("2 seconds"));
});

const replaceBufferedOutputProducer = Effect.fn("replaceBufferedOutputProducer")(function* (
  fixture: Effect.Success<ReturnType<typeof makeBufferedOutputFixture>>,
) {
  const prior = yield* fixture.sink.readCurrentProviderRuntimeOwner(fixture.threadId);
  assert.isNotNull(prior);
  const binding = { ...fixture.binding, runtimeGeneration: "fixture-replacement-generation" };
  const handle = {};
  yield* Ref.set(fixture.resident, { handle, binding });
  const registered = yield* fixture.sink.registerProviderRuntime({
    expectedBinding: prior!.binding,
    expectedEvidenceRevision: prior!.evidenceRevision,
    actualBinding: binding,
    expectedRunId: fixture.run.id,
    expectedRunAttemptId: fixture.attempt.id,
  });
  assert.isTrue(registered.committed);
  const revalidateCurrent = Effect.gen(function* () {
    const current = yield* Ref.get(fixture.resident);
    if (current.handle !== handle || !sameRuntimeBinding(current.binding, binding))
      return yield* Effect.fail("The replacement source was superseded.");
  });
  const origin = {
    producer: {
      token: handle,
      driver: fixture.driver,
      instanceId: fixture.instanceId,
      providerSessionId: fixture.providerSessionId,
      runtimeGeneration: binding.runtimeGeneration,
      revalidateCurrent,
    },
    turn: { ...fixture.owner, binding },
  };
  const stamp = <Event extends ProviderAdapterV2Event>(event: Event) =>
    stampProviderEvent(event, origin);
  return { binding, origin, stamp };
});

const makeCarriedSubagentFixture = Effect.fn("makeCarriedSubagentFixture")(function* () {
  const fixture = yield* makeBufferedOutputFixture("claudeAgent");
  const parent = (yield* fixture.projections.getThreadProjection(fixture.threadId)).thread;
  const startedAt = DateTime.add(fixture.now, { seconds: -20 });
  const updatedAt = DateTime.add(fixture.now, { seconds: -5 });
  const completedAt = DateTime.add(fixture.now, { seconds: 1 });
  const launchRunId = RunId.make(`${fixture.threadId}:launch-run`);
  const launchAttemptId = RunAttemptId.make(`${fixture.threadId}:launch-attempt`);
  const launchTurnId = fixture.ids.derive.providerTurn({
    driver: fixture.driver,
    nativeTurnId: `${fixture.threadId}:launch-turn`,
  });
  const launchRootId = NodeId.make(`${fixture.threadId}:launch-root`);
  const taskId = NodeId.make(`${fixture.threadId}:known-task`);
  const childId = ThreadId.make(`${fixture.threadId}:known-child`);
  const childRootId = NodeId.make(`${fixture.threadId}:known-child-root`);
  const nativeTaskRef = {
    driver: fixture.driver,
    nativeId: "known-native-task",
    strength: "strong" as const,
  };
  const task: OrchestrationV2Subagent = {
    id: taskId,
    threadId: fixture.threadId,
    runId: launchRunId,
    parentNodeId: launchRootId,
    origin: "provider_native",
    createdBy: "agent",
    driver: fixture.driver,
    providerInstanceId: fixture.instanceId,
    providerThreadId: null,
    childThreadId: childId,
    nativeTaskRef,
    prompt: "Finish the carried task.",
    title: "Known task",
    model: null,
    result: null,
    status: "running",
    startedAt,
    completedAt: null,
    updatedAt,
  };
  const launchRoot: OrchestrationV2ExecutionNode = {
    ...fixture.node,
    id: launchRootId,
    rootNodeId: launchRootId,
    runId: launchRunId,
    providerTurnId: launchTurnId,
    status: "completed",
    startedAt,
    completedAt: startedAt,
  };
  const taskNode: OrchestrationV2ExecutionNode = {
    ...launchRoot,
    id: taskId,
    parentNodeId: launchRootId,
    kind: "subagent",
    countsForRun: false,
    status: "running",
    nativeItemRef: nativeTaskRef,
    completedAt: null,
  };
  const childRoot: OrchestrationV2ExecutionNode = {
    ...taskNode,
    id: childRootId,
    threadId: childId,
    runId: null,
    parentNodeId: null,
    rootNodeId: childRootId,
    kind: "root_turn",
    providerThreadId: null,
    providerTurnId: null,
  };
  const card: Extract<OrchestrationV2TurnItem, { readonly type: "subagent" }> = {
    id: TurnItemId.make(`${fixture.threadId}:task-card`),
    threadId: fixture.threadId,
    runId: launchRunId,
    nodeId: taskId,
    providerThreadId: fixture.providerThread.id,
    providerTurnId: launchTurnId,
    nativeItemRef: nativeTaskRef,
    parentItemId: null,
    ordinal: 101,
    status: "running",
    title: task.title,
    startedAt,
    completedAt: null,
    updatedAt,
    type: "subagent",
    subagentId: taskId,
    origin: task.origin,
    driver: fixture.driver,
    providerInstanceId: fixture.instanceId,
    childThreadId: childId,
    prompt: task.prompt,
    result: null,
  };
  const child: OrchestrationV2AppThread = {
    ...parent,
    id: childId,
    activeProviderThreadId: null,
    createdBy: "agent",
    creationSource: "provider",
    createdAt: startedAt,
    updatedAt: startedAt,
    lineage: {
      parentThreadId: fixture.threadId,
      relationshipToParent: "subagent",
      rootThreadId: fixture.threadId,
    },
    forkedFrom: { type: "node", nodeId: taskId },
  };
  const currentRun = { ...fixture.run, ordinal: 2 };
  const currentProvider = { ...fixture.providerThread, lastRunOrdinal: 2 };
  const currentTurn = (yield* fixture.projections.getThreadRecords(fixture.threadId, [
    "providerTurns",
  ])).providerTurns.find((turn) => turn.id === fixture.providerTurnId)!;
  function seed(
    event: OrchestrationV2DomainEvent["type"],
    payload: OrchestrationV2DomainEvent["payload"],
    threadId = fixture.threadId,
    runId?: RunId,
  ) {
    return Effect.gen(function* () {
      return yield* Schema.decodeUnknownEffect(OrchestrationV2DomainEvent)({
        id: yield* fixture.ids.allocate.event({ threadId }),
        type: event,
        threadId,
        ...(runId === undefined ? {} : { runId }),
        occurredAt: updatedAt,
        payload,
      });
    });
  }
  yield* fixture.sink.write({
    events: [
      yield* seed("run.updated", currentRun),
      yield* seed("provider-thread.updated", currentProvider),
      yield* seed(
        "provider-turn.updated",
        { ...currentTurn, ordinal: 2 },
        fixture.threadId,
        currentRun.id,
      ),
      yield* seed("run.created", {
        ...fixture.run,
        id: launchRunId,
        ordinal: 1,
        rootNodeId: launchRootId,
        activeAttemptId: launchAttemptId,
        status: "completed",
        startedAt,
        completedAt: startedAt,
      }),
      yield* seed("run-attempt.created", {
        ...fixture.attempt,
        id: launchAttemptId,
        runId: launchRunId,
        rootNodeId: launchRootId,
        providerTurnId: launchTurnId,
        status: "completed",
        startedAt,
        completedAt: startedAt,
      }),
      yield* seed("node.updated", launchRoot, fixture.threadId, launchRunId),
      yield* seed(
        "provider-turn.updated",
        {
          id: launchTurnId,
          providerThreadId: fixture.providerThread.id,
          nodeId: launchRootId,
          runAttemptId: launchAttemptId,
          nativeTurnRef: null,
          ordinal: 1,
          status: "completed",
          startedAt,
          completedAt: startedAt,
        },
        fixture.threadId,
        launchRunId,
      ),
      yield* seed("thread.created", child, childId),
      yield* seed("node.updated", taskNode, fixture.threadId, launchRunId),
      yield* seed("node.updated", childRoot, childId),
      yield* seed("subagent.updated", task, fixture.threadId, launchRunId),
      yield* seed("turn-item.updated", card, fixture.threadId, launchRunId),
    ],
  });
  const summary = "The genuine carried task result.";
  const childResult = {
    messageId: MessageId.make(`${fixture.threadId}:child-result-message`),
    turnItemId: TurnItemId.make(`${fixture.threadId}:child-result-item`),
    nativeItemRef: {
      driver: fixture.driver,
      nativeId: "task:known-native-task:result",
      strength: "strong" as const,
    },
  };
  const historical = {
    ...fixture.origin.producer,
    token: {},
    runtimeGeneration: "historical-query",
    revalidateCurrent: Effect.fail("The original query is obsolete."),
  };
  const resultToken = stampProviderEvent(
    { type: "result", session_id: fixture.binding.nativeThreadId },
    { producer: fixture.origin.producer },
  );
  const notificationToken = stampProviderEvent(
    {
      type: "task_notification",
      task_id: nativeTaskRef.nativeId,
      session_id: fixture.binding.nativeThreadId,
    },
    { producer: historical },
  );
  const completedTask: OrchestrationV2Subagent = {
    ...task,
    status: "completed",
    result: summary,
    completedAt,
    updatedAt: completedAt,
  };
  const subjectCurrent = yield* Ref.make(completedTask);
  const derivation: ClaudeBufferedSubagentCompletionDerivationV1 = {
    kind: "claude_buffered_subagent_completion",
    executor: fixture.owner,
    result: {
      token: resultToken,
      producer: fixture.origin.producer,
      nativeThreadId: fixture.binding.nativeThreadId,
    },
    notification: {
      token: notificationToken,
      producer: historical,
      nativeThreadId: fixture.binding.nativeThreadId,
      nativeTaskId: nativeTaskRef.nativeId,
      toolUseId: "known-tool-use",
      summary,
      status: "completed",
    },
    subject: {
      subagentId: task.id,
      parentThreadId: fixture.threadId,
      runId: task.runId,
      parentNodeId: launchRootId,
      providerThreadId: task.providerThreadId,
      childThreadId: childId,
      childRootNodeId: childRootId,
      nativeTaskRef,
      startedAt: task.startedAt,
      expectedUpdatedAt: task.updatedAt,
    },
    childResult,
    revalidateDerivation: Ref.get(subjectCurrent).pipe(
      Effect.flatMap((current) =>
        current === completedTask ? Effect.void : Effect.fail("The known task was reopened."),
      ),
    ),
  };
  const origin = { producer: fixture.origin.producer, derivation };
  const stamp = <Event extends ProviderAdapterV2Event>(event: Event) =>
    stampProviderEvent(event, origin);
  const events: ReadonlyArray<ProviderAdapterV2Event> = [
    stamp({
      type: "node.updated",
      driver: fixture.driver,
      node: { ...taskNode, status: "completed", completedAt },
    }),
    stamp({
      type: "node.updated",
      driver: fixture.driver,
      node: { ...childRoot, status: "completed", completedAt },
    }),
    stamp({ type: "subagent.updated", driver: fixture.driver, subagent: completedTask }),
    stamp({
      type: "turn_item.updated",
      driver: fixture.driver,
      turnItem: {
        ...card,
        status: "completed",
        result: summary,
        completedAt,
        updatedAt: completedAt,
      },
    }),
    stamp({
      type: "message.updated",
      driver: fixture.driver,
      message: {
        createdBy: "agent",
        creationSource: "provider",
        id: childResult.messageId,
        threadId: childId,
        runId: null,
        nodeId: childRootId,
        role: "assistant",
        text: summary,
        attachments: [],
        streaming: false,
        createdAt: completedAt,
        updatedAt: completedAt,
      },
    }),
    stamp({
      type: "turn_item.updated",
      driver: fixture.driver,
      turnItem: {
        id: childResult.turnItemId,
        threadId: childId,
        runId: null,
        nodeId: childRootId,
        providerThreadId: null,
        providerTurnId: null,
        nativeItemRef: childResult.nativeItemRef,
        parentItemId: null,
        ordinal: 101,
        status: "completed",
        title: null,
        startedAt: completedAt,
        completedAt,
        updatedAt: completedAt,
        type: "assistant_message",
        messageId: childResult.messageId,
        text: summary,
        streaming: false,
      },
    }),
  ];
  const ingest = (
    event: ProviderAdapterV2Event,
    revalidateCurrentOwner = fixture.revalidateCurrentOwner,
  ) => fixture.ingestor.ingestNormalized({ ...fixture.input, event, revalidateCurrentOwner });
  return {
    ...fixture,
    run: currentRun,
    providerThread: currentProvider,
    task,
    taskNode,
    childRoot,
    card,
    childId,
    childRootId,
    launchRunId,
    launchTurnId,
    subjectCurrent,
    derivation,
    origin,
    summary,
    events,
    ingest,
  };
});

const runManagedCarriedSubagentFixture = Effect.fn("runManagedCarriedSubagentFixture")(function* (
  fixture: Effect.Success<ReturnType<typeof makeCarriedSubagentFixture>>,
  family: ReadonlyArray<ProviderAdapterV2Event> = fixture.events,
) {
  const queue = yield* Queue.unbounded<ProviderAdapterV2Event>();
  const started = yield* Deferred.make<void>();
  const pending = fixture.projections.getThreadProjection(fixture.threadId).pipe(
    Effect.map((projection) =>
      projection.subagents.some((task) => task.id === fixture.task.id && task.status === "running"),
    ),
    Effect.orDie,
  );
  const unused = () =>
    Effect.fail(
      new ProviderAdapterProtocolError({
        driver: fixture.driver,
        detail: "This synthetic conversation does not execute that native method.",
      }),
    );
  const adapter: ProviderAdapterV2Shape = {
    instanceId: fixture.instanceId,
    driver: fixture.driver,
    getCapabilities: () => Effect.succeed(ClaudeProviderCapabilitiesV2),
    planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
    openSession: (input) =>
      Effect.gen(function* () {
        assert.equal(input.providerSessionId, fixture.providerSessionId);
        yield* Effect.addFinalizer(() => Queue.shutdown(queue).pipe(Effect.asVoid));
        return {
          instanceId: fixture.instanceId,
          driver: fixture.driver,
          providerSessionId: fixture.providerSessionId,
          runtimeGeneration: fixture.binding.runtimeGeneration,
          providerSession: fixture.session,
          events: Stream.fromQueue(queue),
          hasPendingBackgroundWork: pending,
          hasPendingBackgroundWorkForThread: () => pending,
          ensureThread: () => Effect.succeed(fixture.providerThread),
          resumeThread: () => Effect.succeed(fixture.providerThread),
          startTurn: (turn) =>
            Effect.gen(function* () {
              assert.equal(turn.runId, fixture.run.id);
              assert.equal(turn.attemptId, fixture.attempt.id);
              assert.equal(turn.providerTurnOrdinal, 2);
              yield* Deferred.succeed(started, undefined);
            }),
          steerTurn: unused,
          interruptTurn: unused,
          respondToRuntimeRequest: unused,
          readThreadSnapshot: unused,
          rollbackThread: unused,
          forkThread: unused,
        } satisfies ProviderAdapterV2SessionRuntime;
      }),
  };
  const mcp = Layer.effect(
    McpSessionRegistry.McpSessionRegistry,
    McpSessionRegistry.__testing.make(),
  ).pipe(
    Layer.provide(
      Layer.succeed(
        HttpServer.HttpServer,
        HttpServer.HttpServer.of({
          address: NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", 43123),
          serve: (() => Effect.void) as HttpServer.HttpServer["Service"]["serve"],
        }),
      ),
    ),
    Layer.provide(
      Layer.succeed(
        ServerEnvironment.ServerEnvironment,
        ServerEnvironment.ServerEnvironment.of({
          getEnvironmentId: Effect.succeed(EnvironmentId.make("environment-carried-subagent")),
          getDescriptor: Effect.die("This synthetic HTTP endpoint has no listener."),
        }),
      ),
    ),
    Layer.provide(NodeServices.layer),
  );
  const managerLayer = ProviderSessionManager.layerWithOptions({ idleTimeoutMs: 60_000 }).pipe(
    Layer.provide(ProviderAdapterRegistry.makeSingleLayer(adapter)),
    Layer.provide(mcp),
    Layer.provide(NodeServices.layer),
  );
  yield* Effect.gen(function* () {
    const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
    const runtime = yield* manager.open({
      threadId: fixture.threadId,
      providerSessionId: fixture.providerSessionId,
      modelSelection: fixture.selection,
      runtimePolicy: { runtimeMode: "full-access", interactionMode: "default", cwd: null },
    });
    yield* manager.registerRuntimeBinding({
      threadId: fixture.threadId,
      providerSessionId: fixture.providerSessionId,
      providerThreadId: fixture.providerThread.id,
      runId: fixture.run.id,
      attemptId: fixture.attempt.id,
    });
    const terminal = fixture.stamp({
      type: "turn.terminal" as const,
      driver: fixture.driver,
      providerThreadId: fixture.providerThread.id,
      providerTurnId: fixture.providerTurnId,
      runOrdinal: fixture.run.ordinal,
      status: "completed" as const,
      failure: null,
      threadDisposition: "reusable" as const,
    });
    const currentTurn = (yield* fixture.projections.getThreadRecords(fixture.threadId, [
      "providerTurns",
    ])).providerTurns.find((turn) => turn.id === fixture.providerTurnId)!;
    const acknowledgement = fixture.stamp({
      type: "provider_turn.updated" as const,
      driver: fixture.driver,
      providerTurn: currentTurn,
    });
    const witness = yield* runtime.subscribeEvents!;
    const published = [acknowledgement, ...family, terminal];
    const witnessed = yield* witness.events.pipe(
      Stream.take(published.length),
      Stream.runCollect,
      Effect.forkChild,
    );
    yield* Deferred.await(started).pipe(
      Effect.andThen(Queue.offerAll(queue, published)),
      Effect.forkChild,
    );
    yield* runBufferedOutputFixture(fixture, "unused", terminal, { session: runtime });
    const received = yield* Fiber.join(witnessed).pipe(Effect.timeout("2 seconds"));
    assert.deepEqual(received, published);
    for (let index = 0; index < published.length; index++)
      assert.strictEqual(received[index], published[index]);
    yield* witness.close;
    assert.equal(
      yield* runtime.hasPendingBackgroundWorkForThread!(fixture.providerThread),
      family.length !== fixture.events.length,
    );
    if (family.length === fixture.events.length) {
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const workerId = `${fixture.threadId}:checkpoint-worker`;
      const claimed = Option.getOrThrow(
        yield* outbox.claimNext({ workerId, leaseDurationMs: 60_000 }),
      );
      assert.equal(claimed.id, `effect:checkpoint.capture:${fixture.run.id}`);
      assert.equal(claimed.threadId, fixture.threadId);
      assert.deepEqual(claimed.request, {
        type: "checkpoint.capture",
        runId: fixture.run.id,
        scopeId: CheckpointScopeId.make(`${fixture.threadId}:scope`),
      });
      const checkpoint = (ordinalWithinScope: number, runId: RunId | null = null) => ({
        id: CheckpointId.make(`${fixture.threadId}:checkpoint:${ordinalWithinScope}`),
        threadId: fixture.threadId,
        scopeId: CheckpointScopeId.make(`${fixture.threadId}:scope`),
        runId,
        nodeId: fixture.node.id,
        parentCheckpointId: null,
        ordinalWithinScope,
        appRunOrdinal: runId === null ? null : fixture.run.ordinal,
        ref: CheckpointRef.make(`${fixture.threadId}:checkpoint-ref:${ordinalWithinScope}`),
        status: "ready" as const,
        files: [],
        capturedAt: fixture.now,
      });
      const captureLayer = CheckpointCaptureService.layer.pipe(
        Layer.provide(
          Layer.mock(CheckpointService.CheckpointServiceV2)({
            materializeBaselineCheckpoint: ({ ordinalWithinScope }) =>
              Effect.succeed(checkpoint(ordinalWithinScope)),
            capture: ({ ordinalWithinScope, runId }) =>
              Effect.succeed(checkpoint(ordinalWithinScope, runId)),
          }),
        ),
      );
      yield* Effect.gen(function* () {
        const finalization = yield* RunFinalizationService.RunFinalizationService;
        const captured = yield* finalization.finalize({
          threadId: claimed.threadId,
          runId: fixture.run.id,
          scopeId: CheckpointScopeId.make(`${fixture.threadId}:scope`),
        });
        assert.equal(captured.kind, "captured");
        if (captured.kind === "captured") assert.isTrue(captured.commit.committed);
      }).pipe(Effect.provide(RunFinalizationService.layer.pipe(Layer.provide(captureLayer))));
      assert.isTrue(yield* outbox.succeed({ effectId: claimed.id, workerId }));
    }
  }).pipe(Effect.provide(managerLayer));
});

it.effect(
  "atomically persists a complete carried Claude task family without reattributing its launch or child",
  () =>
    Effect.gen(function* () {
      const fixture = yield* makeCarriedSubagentFixture();
      const evidence = yield* fixture.sink.readCurrentProviderRuntimeOwner(fixture.threadId);
      for (const event of fixture.events.slice(0, -1)) assert.isEmpty(yield* fixture.ingest(event));
      const before = yield* fixture.projections.getThreadProjection(fixture.threadId);
      assert.equal(before.subagents.find((task) => task.id === fixture.task.id)?.status, "running");
      assert.equal(before.nodes.find((node) => node.id === fixture.taskNode.id)?.status, "running");
      assert.equal(before.turnItems.find((item) => item.id === fixture.card.id)?.status, "running");
      assert.isEmpty((yield* fixture.projections.getThreadProjection(fixture.childId)).messages);
      assert.isTrue(
        yield* fixture.ingestor.hasBufferedAssistantOutput({
          binding: fixture.binding,
          providerTurnId: fixture.providerTurnId,
        }),
      );
      assert.equal(
        (yield* fixture.flush(fixture.providerTurnId).pipe(Effect.result))._tag,
        "Failure",
      );
      const committed = yield* fixture.ingest(fixture.events.at(-1)!);
      assert.lengthOf(committed, 6);
      const parent = yield* fixture.projections.getThreadProjection(fixture.threadId);
      const child = yield* fixture.projections.getThreadProjection(fixture.childId);
      assert.equal(
        parent.subagents.find((task) => task.id === fixture.task.id)?.result,
        fixture.summary,
      );
      assert.equal(
        parent.subagents.find((task) => task.id === fixture.task.id)?.runId,
        fixture.launchRunId,
      );
      assert.equal(
        parent.nodes.find((node) => node.id === fixture.taskNode.id)?.providerTurnId,
        fixture.launchTurnId,
      );
      assert.equal(
        parent.turnItems.find((item) => item.id === fixture.card.id)?.providerTurnId,
        fixture.launchTurnId,
      );
      assert.equal(
        child.nodes.find((node) => node.id === fixture.childRootId)?.status,
        "completed",
      );
      assert.equal(child.messages[0]?.text, fixture.summary);
      assert.isNull(child.messages[0]!.runId);
      assert.isNull(child.turnItems[0]!.runId);
      assert.isNull(child.turnItems[0]!.providerTurnId);
      assert.isTrue(
        committed
          .filter((entry) => entry.event.threadId === fixture.childId)
          .every((entry) => entry.event.runId === undefined),
      );
      assert.equal(parent.runs.find((run) => run.id === fixture.run.id)?.status, "running");
      assert.equal(
        (yield* fixture.sink.readCurrentProviderRuntimeOwner(fixture.threadId))?.evidenceRevision,
        evidence?.evidenceRevision,
      );
      assert.isFalse(
        yield* fixture.ingestor.hasBufferedAssistantOutput({ binding: fixture.binding }),
      );
      const old = readProviderEventOrigin(fixture.derivation.notification.token)!;
      assert.equal(old.producer.runtimeGeneration, "historical-query");
      assert.isUndefined(old.derivation);
      assert.equal((yield* old.producer.revalidateCurrent.pipe(Effect.result))._tag, "Failure");
    }).pipe(Effect.provide(TestLayer)),
);

it.effect(
  "composes Manager fanout and root ingestion with qualified carried Claude child persistence before terminal",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fixture = yield* makeCarriedSubagentFixture();
        yield* runManagedCarriedSubagentFixture(fixture);
        const parent = yield* fixture.projections.getThreadProjection(fixture.threadId);
        const child = yield* fixture.projections.getThreadProjection(fixture.childId);
        assert.equal(parent.runs.find((run) => run.id === fixture.run.id)?.status, "completed");
        assert.equal(
          parent.subagents.find((task) => task.id === fixture.task.id)?.status,
          "completed",
        );
        assert.equal(
          parent.subagents.find((task) => task.id === fixture.task.id)?.runId,
          fixture.launchRunId,
        );
        assert.equal(
          child.nodes.find((node) => node.id === fixture.childRootId)?.status,
          "completed",
        );
        assert.equal(child.messages[0]?.text, fixture.summary);
        assert.isNull(child.messages[0]!.runId);
        const history = yield* fixture.store.read({}).pipe(Stream.runCollect);
        const result = history.find(
          (entry) =>
            entry.event.type === "message.updated" &&
            entry.event.payload.id === fixture.derivation.childResult!.messageId,
        )!;
        const terminal = history.find(
          (entry) =>
            entry.event.type === "run.updated" &&
            entry.event.payload.id === fixture.run.id &&
            entry.event.payload.status === "completed",
        )!;
        assert.isBelow(result.sequence, terminal.sequence);
        assert.isUndefined(readProviderEventOrigin(fixture.events[4]!)?.turn);
        assert.strictEqual(
          readProviderEventOrigin(fixture.events[4]!)?.derivation?.result.token,
          fixture.derivation.result.token,
        );
      }),
    ).pipe(Effect.provide(TestLayer)),
);

it.effect(
  "holds a completed root terminal when its carried Claude child family is incomplete",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fixture = yield* makeCarriedSubagentFixture();
        yield* runManagedCarriedSubagentFixture(fixture, fixture.events.slice(0, -1));
        const parent = yield* fixture.projections.getThreadProjection(fixture.threadId);
        const child = yield* fixture.projections.getThreadProjection(fixture.childId);
        assert.equal(parent.runs.find((run) => run.id === fixture.run.id)?.status, "running");
        assert.equal(
          parent.subagents.find((task) => task.id === fixture.task.id)?.status,
          "running",
        );
        assert.equal(
          child.nodes.find((node) => node.id === fixture.childRootId)?.status,
          "running",
        );
        assert.isEmpty(child.messages);
        assert.isEmpty(child.turnItems);
        assert.isTrue(
          yield* fixture.ingestor.hasBufferedAssistantOutput({ binding: fixture.binding }),
        );
      }),
    ).pipe(Effect.provide(TestLayer)),
);

it.effect(
  "holds a root terminal when its current carried Claude derivation becomes invalid before routing",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fixture = yield* makeCarriedSubagentFixture();
        yield* Ref.set(fixture.subjectCurrent, {
          ...fixture.task,
          updatedAt: DateTime.add(fixture.now, { seconds: 2 }),
        });
        const terminal = fixture.stamp({
          type: "turn.terminal" as const,
          driver: fixture.driver,
          providerThreadId: fixture.providerThread.id,
          providerTurnId: fixture.providerTurnId,
          runOrdinal: fixture.run.ordinal,
          status: "completed" as const,
          failure: null,
          threadDisposition: "reusable" as const,
        });
        yield* runBufferedOutputFixture(fixture, "unused", terminal, {
          events: Stream.fromIterable([...fixture.events, terminal]),
        });
        const parent = yield* fixture.projections.getThreadProjection(fixture.threadId);
        const child = yield* fixture.projections.getThreadProjection(fixture.childId);
        assert.equal(parent.runs.find((run) => run.id === fixture.run.id)?.status, "running");
        assert.equal(
          parent.subagents.find((task) => task.id === fixture.task.id)?.status,
          "running",
        );
        assert.equal(
          child.nodes.find((node) => node.id === fixture.childRootId)?.status,
          "running",
        );
        assert.isEmpty(child.messages);
        assert.isEmpty(child.turnItems);
      }),
    ).pipe(Effect.provide(TestLayer)),
);

it.effect("rejects a carried Claude completion if its known task reopens before publication", () =>
  Effect.gen(function* () {
    const fixture = yield* makeCarriedSubagentFixture();
    for (const event of fixture.events.slice(0, -1)) yield* fixture.ingest(event);
    const reopened = { ...fixture.task, updatedAt: DateTime.add(fixture.now, { seconds: 2 }) };
    yield* Ref.set(fixture.subjectCurrent, reopened);
    yield* fixture.sink.write({
      events: [
        {
          id: yield* fixture.ids.allocate.event({ threadId: fixture.threadId }),
          type: "subagent.updated",
          threadId: fixture.threadId,
          runId: fixture.launchRunId,
          nodeId: fixture.task.id,
          occurredAt: reopened.updatedAt,
          payload: reopened,
        },
      ],
    });
    assert.equal(
      (yield* fixture.ingest(fixture.events.at(-1)!).pipe(Effect.result))._tag,
      "Failure",
    );
    const parent = yield* fixture.projections.getThreadProjection(fixture.threadId);
    assert.deepEqual(
      parent.subagents.find((task) => task.id === fixture.task.id),
      reopened,
    );
    assert.equal(parent.nodes.find((node) => node.id === fixture.taskNode.id)?.status, "running");
    assert.isEmpty((yield* fixture.projections.getThreadProjection(fixture.childId)).messages);
    assert.isTrue(yield* fixture.ingestor.hasBufferedAssistantOutput({ binding: fixture.binding }));
  }).pipe(Effect.provide(TestLayer)),
);

it.effect(
  "rejects all carried Claude artifacts when the current executor is replaced inside publication",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fixture = yield* makeCarriedSubagentFixture();
        for (const event of fixture.events.slice(0, -1)) yield* fixture.ingest(event);
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const published = yield* fixture
          .ingest(
            fixture.events.at(-1)!,
            Deferred.succeed(entered, undefined).pipe(
              Effect.andThen(Deferred.await(release)),
              Effect.andThen(fixture.revalidateCurrentOwner),
            ),
          )
          .pipe(Effect.result, Effect.forkChild);
        yield* Deferred.await(entered);
        const current = yield* Ref.get(fixture.resident);
        yield* Ref.set(fixture.resident, {
          ...current,
          binding: { ...current.binding, runtimeGeneration: "replacement-reservation" },
        });
        yield* Deferred.succeed(release, undefined);
        assert.equal((yield* Fiber.join(published))._tag, "Failure");
        const parent = yield* fixture.projections.getThreadProjection(fixture.threadId);
        const child = yield* fixture.projections.getThreadProjection(fixture.childId);
        assert.equal(
          parent.subagents.find((task) => task.id === fixture.task.id)?.status,
          "running",
        );
        assert.equal(
          parent.turnItems.find((item) => item.id === fixture.card.id)?.status,
          "running",
        );
        assert.equal(
          child.nodes.find((node) => node.id === fixture.childRootId)?.status,
          "running",
        );
        assert.isEmpty(child.messages);
        assert.isEmpty(child.turnItems);
        assert.isTrue(
          yield* fixture.ingestor.hasBufferedAssistantOutput({ binding: fixture.binding }),
        );
      }),
    ).pipe(Effect.provide(TestLayer)),
);

it.effect(
  "rejects a carried Claude completion when durable subject state changed despite unchanged local capture",
  () =>
    Effect.gen(function* () {
      const fixture = yield* makeCarriedSubagentFixture();
      for (const event of fixture.events.slice(0, -1)) yield* fixture.ingest(event);
      const updated = { ...fixture.task, updatedAt: DateTime.add(fixture.now, { seconds: 2 }) };
      yield* fixture.sink.write({
        events: [
          {
            id: yield* fixture.ids.allocate.event({ threadId: fixture.threadId }),
            type: "subagent.updated",
            threadId: fixture.threadId,
            runId: fixture.launchRunId,
            nodeId: fixture.task.id,
            occurredAt: updated.updatedAt,
            payload: updated,
          },
        ],
      });
      yield* fixture.derivation.revalidateDerivation;
      const before = yield* fixture.sink.latestSequence();
      assert.equal(
        (yield* fixture.ingest(fixture.events.at(-1)!).pipe(Effect.result))._tag,
        "Failure",
      );
      assert.equal(yield* fixture.sink.latestSequence(), before);
      assert.deepEqual(
        (yield* fixture.projections.getThreadProjection(fixture.threadId)).subagents.find(
          (task) => task.id === fixture.task.id,
        ),
        updated,
      );
      assert.isEmpty((yield* fixture.projections.getThreadProjection(fixture.childId)).messages);
      assert.isTrue(
        yield* fixture.ingestor.hasBufferedAssistantOutput({ binding: fixture.binding }),
      );
    }).pipe(Effect.provide(TestLayer)),
);

it.effect(
  "requires matching captured Claude conversation and raw SDK result association before buffering",
  () =>
    Effect.gen(function* () {
      const fixture = yield* makeCarriedSubagentFixture();
      const original = fixture.events[0]!;
      const invalid = [
        { ...fixture.derivation, result: { ...fixture.derivation.result, token: {} } },
        {
          ...fixture.derivation,
          result: { ...fixture.derivation.result, nativeThreadId: "another-conversation" },
        },
        {
          ...fixture.derivation,
          notification: {
            ...fixture.derivation.notification,
            nativeThreadId: "another-conversation",
          },
        },
      ];
      const before = yield* fixture.sink.latestSequence();
      for (const derivation of invalid) {
        const event = stampProviderEvent(
          { ...original },
          { producer: fixture.origin.producer, derivation },
        );
        assert.equal((yield* fixture.ingest(event).pipe(Effect.result))._tag, "Failure");
        assert.isFalse(
          yield* fixture.ingestor.hasBufferedAssistantOutput({ binding: fixture.binding }),
        );
      }
      assert.equal(yield* fixture.sink.latestSequence(), before);
      assert.equal(
        (yield* fixture.projections.getThreadProjection(fixture.threadId)).subagents.find(
          (task) => task.id === fixture.task.id,
        )?.status,
        "running",
      );
      assert.strictEqual(
        readProviderEventOrigin(original)?.derivation?.result.token,
        fixture.derivation.result.token,
      );
    }).pipe(Effect.provide(TestLayer)),
);

it.effect("rejects a carried Claude family from a superseded current executor attempt", () =>
  Effect.gen(function* () {
    const fixture = yield* makeCarriedSubagentFixture();
    for (const event of fixture.events.slice(0, -1)) yield* fixture.ingest(event);
    const attemptId = RunAttemptId.make(`${fixture.threadId}:new-executor-attempt`);
    yield* fixture.sink.write({
      events: [
        {
          id: yield* fixture.ids.allocate.event({ threadId: fixture.threadId }),
          type: "run.updated",
          threadId: fixture.threadId,
          runId: fixture.run.id,
          occurredAt: fixture.now,
          payload: { ...fixture.run, activeAttemptId: attemptId },
        },
        {
          id: yield* fixture.ids.allocate.event({ threadId: fixture.threadId }),
          type: "run-attempt.updated",
          threadId: fixture.threadId,
          runId: fixture.run.id,
          occurredAt: fixture.now,
          payload: {
            ...fixture.attempt,
            id: attemptId,
            attemptOrdinal: 2,
            providerTurnId: null,
            reason: "steering_restart",
          },
        },
      ],
    });
    const before = yield* fixture.sink.latestSequence();
    assert.equal(
      (yield* fixture.ingest(fixture.events.at(-1)!).pipe(Effect.result))._tag,
      "Failure",
    );
    assert.equal(yield* fixture.sink.latestSequence(), before);
    const parent = yield* fixture.projections.getThreadProjection(fixture.threadId);
    assert.equal(parent.runs.find((run) => run.id === fixture.run.id)?.activeAttemptId, attemptId);
    assert.equal(parent.subagents.find((task) => task.id === fixture.task.id)?.status, "running");
    assert.isEmpty((yield* fixture.projections.getThreadProjection(fixture.childId)).messages);
    assert.isTrue(yield* fixture.ingestor.hasBufferedAssistantOutput({ binding: fixture.binding }));
  }).pipe(Effect.provide(TestLayer)),
);

it.effect("never extends a carried Claude derivation to an ordinary root terminal", () =>
  Effect.gen(function* () {
    const fixture = yield* makeCarriedSubagentFixture();
    const event = stampProviderEvent(
      {
        type: "turn.terminal" as const,
        driver: fixture.driver,
        providerThreadId: fixture.providerThread.id,
        providerTurnId: fixture.providerTurnId,
        runOrdinal: fixture.run.ordinal,
        status: "completed" as const,
        failure: null,
        threadDisposition: "reusable" as const,
      },
      fixture.origin,
    );
    const before = yield* fixture.sink.latestSequence();
    assert.equal((yield* fixture.ingest(event).pipe(Effect.result))._tag, "Failure");
    assert.equal(yield* fixture.sink.latestSequence(), before);
    assert.isFalse(
      yield* fixture.ingestor.hasBufferedAssistantOutput({ binding: fixture.binding }),
    );
    assert.equal(
      (yield* fixture.projections.getThreadProjection(fixture.threadId)).runs.find(
        (run) => run.id === fixture.run.id,
      )?.status,
      "running",
    );
    assert.equal(
      readProviderEventOrigin(fixture.derivation.notification.token)?.producer.runtimeGeneration,
      "historical-query",
    );
  }).pipe(Effect.provide(TestLayer)),
);

it.effect("persists buffered assistant text when an active OpenCode session exits", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fixture = yield* makeBufferedOutputFixture("opencode");
      yield* runBufferedOutputFixture(fixture, "Answer preserved across provider exit.", {
        type: "turn.terminal",
        driver: fixture.driver,
        providerThreadId: fixture.providerThread.id,
        providerTurnId: fixture.providerTurnId,
        runOrdinal: 1,
        status: "failed",
        failureItemOrdinal: 102,
        failure: makeProviderFailure({
          message: "OpenCode event stream exited unexpectedly.",
          class: "transport_error",
        }),
        threadDisposition: "broken",
      });
      yield* fixture.updateSession("error");
      const projection = yield* fixture.projections.getThreadProjection(fixture.threadId);
      assert.equal(projection.providerSessions[0]?.status, "error");
      assert.equal(projection.providerTurns[0]?.status, "failed");
      assert.equal(projection.runs[0]?.status, "failed");
      assert.equal(projection.attempts[0]?.status, "failed");
      assert.equal(projection.providerThreads[0]?.status, "error");
      assert.equal(projection.messages[0]?.text, "Answer preserved across provider exit.");
      assert.equal(projection.messages[0]?.runId, fixture.run.id);
      assert.equal(projection.messages[0]?.nodeId, fixture.assistantNode.id);
      assert.deepEqual(
        projection.nodes.find((node) => node.id === fixture.assistantNode.id),
        fixture.assistantNode,
      );
      assert.isFalse(projection.messages[0]!.streaming);
      const history = yield* fixture.store
        .read({ threadId: fixture.threadId })
        .pipe(Stream.runCollect);
      const output = history.find((entry) => entry.event.type === "message.updated")!;
      const companion = history.find(
        (entry) =>
          entry.event.type === "node.updated" &&
          entry.event.payload.id === fixture.assistantNode.id,
      )!;
      const deadSession = history.find(
        (entry) =>
          entry.event.type === "provider-session.updated" && entry.event.payload.status === "error",
      )!;
      const failed = history.find(
        (entry) =>
          entry.event.type === "provider-turn.updated" && entry.event.payload.status === "failed",
      )!;
      const run = history.find(
        (entry) => entry.event.type === "run.updated" && entry.event.payload.status === "failed",
      )!;
      assert.isBelow(companion.sequence, output.sequence);
      assert.isBelow(output.sequence, deadSession.sequence);
      assert.isBelow(output.sequence, failed.sequence);
      assert.isBelow(output.sequence, run.sequence);
    }),
  ).pipe(Effect.provide(TestLayer)),
);

it.effect("does not attribute a turnless session exit to the active turn", () =>
  Effect.gen(function* () {
    const fixture = yield* makeBufferedOutputFixture();
    yield* fixture.capture("Durable but not terminal.");
    const output = yield* fixture.flush();
    assert.deepEqual(
      output.map((entry) => entry.event.type),
      ["node.updated", "message.updated"],
    );
    yield* fixture.updateSession("stopped");
    const projection = yield* fixture.projections.getThreadProjection(fixture.threadId);
    assert.equal(projection.providerSessions[0]?.status, "stopped");
    assert.equal(projection.providerTurns[0]?.status, "running");
    assert.isNull(projection.providerTurns[0]!.completedAt);
    assert.equal(projection.runs[0]?.status, "running");
    assert.equal(projection.messages[0]?.text, "Durable but not terminal.");
    assert.isFalse(projection.messages[0]!.streaming);
    const history = yield* fixture.store
      .read({ threadId: fixture.threadId })
      .pipe(Stream.runCollect);
    assert.isFalse(
      history.some(
        (entry) =>
          entry.event.type === "provider-turn.updated" && entry.event.payload.status !== "running",
      ),
    );
  }).pipe(Effect.provide(TestLayer)),
);

it.effect("flushes reasoning with only the companion node referenced by its output", () =>
  Effect.gen(function* () {
    const fixture = yield* makeBufferedOutputFixture();
    const node: OrchestrationV2ExecutionNode = {
      ...fixture.assistantNode,
      id: NodeId.make(`${fixture.threadId}:reasoning-node`),
      kind: "reasoning",
    };
    const unrelated = {
      ...fixture.rawNode,
      node: { ...fixture.assistantNode, id: NodeId.make(`${fixture.threadId}:unreferenced-node`) },
    };
    const item: Extract<OrchestrationV2TurnItem, { readonly type: "reasoning" }> = {
      id: TurnItemId.make(`${fixture.threadId}:reasoning`),
      threadId: fixture.threadId,
      runId: fixture.run.id,
      nodeId: node.id,
      providerThreadId: fixture.providerThread.id,
      providerTurnId: fixture.providerTurnId,
      nativeItemRef: null,
      parentItemId: null,
      ordinal: 101,
      status: "running",
      title: null,
      startedAt: fixture.now,
      completedAt: null,
      updatedAt: fixture.now,
      type: "reasoning",
      text: "Reasoning retained before a stream exit.",
      streaming: true,
    };
    const positionedItem = yield* fixture.positions.normalize(item);
    if (positionedItem.type !== "reasoning")
      return yield* Effect.die("Position normalization must preserve the reasoning item.");
    const capture = (
      event: Parameters<typeof fixture.ingestor.captureAssistantOutput>[0]["event"],
    ) =>
      fixture.ingestor.captureAssistantOutput({
        ...fixture.input,
        owner: fixture.owner,
        revalidateCurrentOwner: fixture.revalidateCurrentOwner,
        event,
      });
    yield* capture(fixture.stamp(unrelated));
    yield* capture(fixture.stamp({ type: "node.updated", driver: fixture.driver, node }));
    const event = { type: "turn_item.updated" as const, driver: fixture.driver, turnItem: item };
    yield* capture(fixture.stamp(event));
    assert.isNull(
      makeAssistantStreamingFilter("paragraph")(event, DateTime.toEpochMillis(fixture.now)),
    );
    const output = yield* fixture.flush(fixture.providerTurnId);
    assert.deepEqual(
      output.map((entry) => entry.event.type),
      ["node.updated", "turn-item.updated"],
    );
    const projection = yield* fixture.projections.getThreadProjection(fixture.threadId);
    assert.deepEqual(
      projection.nodes.find((candidate) => candidate.id === node.id),
      node,
    );
    assert.isFalse(projection.nodes.some((candidate) => candidate.id === unrelated.node.id));
    assert.deepEqual(
      projection.turnItems.find((candidate) => candidate.id === item.id),
      { ...positionedItem, streaming: false },
    );
    assert.equal(projection.runs[0]?.status, "running");
    assert.equal(projection.providerTurns[0]?.status, "running");
    assert.lengthOf(yield* fixture.flush(), 0);
  }).pipe(Effect.provide(TestLayer)),
);

it.effect("flushes buffered text when readiness clears attribution before a turnless exit", () =>
  Effect.gen(function* () {
    const fixture = yield* makeBufferedOutputFixture();
    yield* fixture.capture("Ready did not complete this turn.");
    yield* fixture.flush();
    yield* fixture.updateSession("ready");
    const ready = yield* fixture.projections.getThreadProjection(fixture.threadId);
    assert.equal(ready.providerSessions[0]?.status, "ready");
    assert.equal(ready.messages[0]?.text, "Ready did not complete this turn.");
    assert.isFalse(ready.messages[0]!.streaming);
    assert.lengthOf(yield* fixture.flush(), 0);
    yield* fixture.updateSession("stopped");
    const stopped = yield* fixture.projections.getThreadProjection(fixture.threadId);
    assert.equal(stopped.providerTurns[0]?.id, fixture.providerTurnId);
    assert.equal(stopped.providerTurns[0]?.status, "running");
    assert.isNull(stopped.providerTurns[0]!.completedAt);
    assert.equal(stopped.runs[0]?.status, "running");
    const history = yield* fixture.store
      .read({ threadId: fixture.threadId })
      .pipe(Stream.runCollect);
    assert.lengthOf(
      history.filter((entry) => entry.event.type === "message.updated"),
      1,
    );
  }).pipe(Effect.provide(TestLayer)),
);

it.effect("keeps buffered output held when its resident generation is replaced", () =>
  Effect.gen(function* () {
    const fixture = yield* makeBufferedOutputFixture();
    yield* fixture.capture("Old runtime text must not become current output.");
    yield* Ref.set(fixture.resident, {
      handle: {},
      binding: { ...fixture.binding, runtimeGeneration: "replacement" },
    });
    const result = yield* fixture.flush(fixture.providerTurnId).pipe(Effect.result);
    assert.equal(result._tag, "Failure");
    assert.lengthOf((yield* fixture.projections.getThreadProjection(fixture.threadId)).messages, 0);
    const actualOutput = yield* fixture.flush(fixture.providerTurnId).pipe(Effect.result);
    assert.equal(actualOutput._tag, "Failure");
  }).pipe(Effect.provide(TestLayer)),
);

it.effect("rejects a buffered flush after the current attempt is superseded", () =>
  Effect.gen(function* () {
    const fixture = yield* makeBufferedOutputFixture();
    yield* fixture.capture("Superseded output cannot overwrite the current attempt.");
    const currentAttemptId = RunAttemptId.make(`${fixture.threadId}:new-attempt`);
    yield* fixture.sink.write({
      events: [
        {
          id: yield* fixture.ids.allocate.event({ threadId: fixture.threadId }),
          type: "run.updated",
          threadId: fixture.threadId,
          runId: fixture.run.id,
          occurredAt: fixture.now,
          payload: { ...fixture.run, activeAttemptId: currentAttemptId },
        },
        {
          id: yield* fixture.ids.allocate.event({ threadId: fixture.threadId }),
          type: "run-attempt.updated",
          threadId: fixture.threadId,
          runId: fixture.run.id,
          occurredAt: fixture.now,
          payload: {
            ...fixture.attempt,
            id: currentAttemptId,
            attemptOrdinal: 2,
            providerTurnId: null,
            reason: "steering_restart",
          },
        },
      ],
    });
    assert.equal(
      (yield* fixture.flush(fixture.providerTurnId).pipe(Effect.result))._tag,
      "Failure",
    );
    assert.lengthOf((yield* fixture.projections.getThreadProjection(fixture.threadId)).messages, 0);
  }).pipe(Effect.provide(TestLayer)),
);

it.effect("durably flushes buffered text before an attributed turn abort settles", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fixture = yield* makeBufferedOutputFixture();
      yield* runBufferedOutputFixture(fixture, "Partial answer before abort.", {
        type: "turn.terminal",
        driver: fixture.driver,
        providerThreadId: fixture.providerThread.id,
        providerTurnId: fixture.providerTurnId,
        runOrdinal: 1,
        status: "interrupted",
        failure: null,
        threadDisposition: "reusable",
      });
      const projection = yield* fixture.projections.getThreadProjection(fixture.threadId);
      assert.equal(projection.messages[0]?.text, "Partial answer before abort.");
      assert.equal(projection.messages[0]?.nodeId, fixture.assistantNode.id);
      assert.isFalse(projection.messages[0]!.streaming);
      assert.equal(projection.runs[0]?.status, "interrupted");
      assert.equal(projection.providerTurns[0]?.status, "interrupted");
      const history = yield* fixture.store
        .read({ threadId: fixture.threadId })
        .pipe(Stream.runCollect);
      const message = history.find((entry) => entry.event.type === "message.updated")!;
      const turn = history.find(
        (entry) =>
          entry.event.type === "provider-turn.updated" &&
          entry.event.payload.status === "interrupted",
      )!;
      const run = history.find(
        (entry) =>
          entry.event.type === "run.updated" && entry.event.payload.status === "interrupted",
      )!;
      assert.isBelow(message.sequence, turn.sequence);
      assert.isBelow(message.sequence, run.sequence);
    }),
  ).pipe(Effect.provide(TestLayer)),
);

it.effect("holds an attributed terminal when buffered output loses its current attempt", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fixture = yield* makeBufferedOutputFixture();
      const currentAttemptId = RunAttemptId.make(`${fixture.threadId}:new-attempt`);
      const supersede = Effect.gen(function* () {
        yield* fixture.sink.write({
          events: [
            {
              id: yield* fixture.ids.allocate.event({ threadId: fixture.threadId }),
              type: "run.updated",
              threadId: fixture.threadId,
              runId: fixture.run.id,
              occurredAt: fixture.now,
              payload: { ...fixture.run, activeAttemptId: currentAttemptId },
            },
            {
              id: yield* fixture.ids.allocate.event({ threadId: fixture.threadId }),
              type: "run-attempt.updated",
              threadId: fixture.threadId,
              runId: fixture.run.id,
              occurredAt: fixture.now,
              payload: {
                ...fixture.attempt,
                id: currentAttemptId,
                attemptOrdinal: 2,
                providerTurnId: null,
                reason: "steering_restart",
              },
            },
          ],
        });
      });
      yield* runBufferedOutputFixture(
        fixture,
        "Undurable old output cannot settle the current run.",
        {
          type: "turn.terminal",
          driver: fixture.driver,
          providerThreadId: fixture.providerThread.id,
          providerTurnId: fixture.providerTurnId,
          runOrdinal: 1,
          status: "interrupted",
          failure: null,
          threadDisposition: "reusable",
        },
        { beforeTerminal: supersede },
      );
      const projection = yield* fixture.projections.getThreadProjection(fixture.threadId);
      assert.equal(projection.runs[0]?.activeAttemptId, currentAttemptId);
      assert.equal(projection.runs[0]?.status, "running");
      assert.equal(projection.attempts[0]?.status, "running");
      assert.equal(projection.providerTurns[0]?.status, "running");
      assert.lengthOf(projection.messages, 0);
      assert.isFalse(projection.nodes.some((node) => node.id === fixture.assistantNode.id));
      const history = yield* fixture.store
        .read({ threadId: fixture.threadId })
        .pipe(Stream.runCollect);
      assert.isFalse(
        history.some(
          (entry) =>
            (entry.event.type === "run.updated" ||
              entry.event.type === "run-attempt.updated" ||
              entry.event.type === "provider-turn.updated") &&
            entry.event.payload.completedAt !== null,
        ),
      );
    }),
  ).pipe(Effect.provide(TestLayer)),
);

it.effect("preserves captured origin across filtered prefixes before terminal tail flush", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fixture = yield* makeBufferedOutputFixture();
      const text = "A complete paragraph.\n\nTail retained until the terminal.";
      yield* runBufferedOutputFixture(fixture, text, {
        type: "turn.terminal",
        driver: fixture.driver,
        providerThreadId: fixture.providerThread.id,
        providerTurnId: fixture.providerTurnId,
        runOrdinal: 1,
        status: "interrupted",
        failure: null,
        threadDisposition: "reusable",
      });
      const projection = yield* fixture.projections.getThreadProjection(fixture.threadId);
      assert.equal(projection.messages[0]?.text, text);
      assert.isFalse(projection.messages[0]!.streaming);
      const history = yield* fixture.store
        .read({ threadId: fixture.threadId })
        .pipe(Stream.runCollect);
      const messages = history.flatMap((entry) =>
        entry.event.type === "message.updated"
          ? [{ sequence: entry.sequence, payload: entry.event.payload }]
          : [],
      );
      assert.lengthOf(messages, 2);
      assert.equal(messages[0]!.payload.text, "A complete paragraph.\n\n");
      assert.isTrue(messages[0]!.payload.streaming);
      assert.equal(messages[1]!.payload.text, text);
      assert.isFalse(messages[1]!.payload.streaming);
      const terminal = history.find(
        (entry) =>
          entry.event.type === "provider-turn.updated" &&
          entry.event.payload.status === "interrupted",
      )!;
      assert.isBelow(messages[1]!.sequence, terminal.sequence);
      assert.lengthOf(
        history.filter(
          (entry) =>
            entry.event.type === "node.updated" &&
            entry.event.payload.id === fixture.assistantNode.id,
        ),
        1,
      );
    }),
  ).pipe(Effect.provide(TestLayer)),
);

it.effect("rejects old producer output queued before capture after runtime replacement", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fixture = yield* makeBufferedOutputFixture();
      const queue = yield* Queue.make<ProviderAdapterV2Event, Cause.Done>();
      const terminal = fixture.stamp({
        type: "turn.terminal" as const,
        driver: fixture.driver,
        providerThreadId: fixture.providerThread.id,
        providerTurnId: fixture.providerTurnId,
        runOrdinal: 1,
        status: "interrupted" as const,
        failure: null,
        threadDisposition: "reusable" as const,
      });
      yield* Queue.offerAll(queue, [
        fixture.rawNode,
        fixture.rawMessage("Old queued text."),
        terminal,
      ]);
      yield* Queue.end(queue);
      const replacement = yield* replaceBufferedOutputProducer(fixture);
      yield* runBufferedOutputFixture(fixture, "unused", terminal, {
        events: Stream.fromQueue(queue),
        runtimeGeneration: replacement.binding.runtimeGeneration,
      });
      const projection = yield* fixture.projections.getThreadProjection(fixture.threadId);
      assert.lengthOf(projection.messages, 0);
      assert.isFalse(projection.nodes.some((node) => node.id === fixture.assistantNode.id));
      assert.equal(projection.runs[0]?.status, "running");
      assert.equal(projection.providerTurns[0]?.status, "running");
      assert.equal(
        (yield* fixture.sink.readCurrentProviderRuntimeOwner(fixture.threadId))?.binding
          .runtimeGeneration,
        replacement.binding.runtimeGeneration,
      );
    }),
  ).pipe(Effect.provide(TestLayer)),
);

it.effect("accepts replacement-source output after its current binding is registered", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fixture = yield* makeBufferedOutputFixture();
      const replacement = yield* replaceBufferedOutputProducer(fixture);
      const terminal = replacement.stamp({
        type: "turn.terminal" as const,
        driver: fixture.driver,
        providerThreadId: fixture.providerThread.id,
        providerTurnId: fixture.providerTurnId,
        runOrdinal: 1,
        status: "interrupted" as const,
        failure: null,
        threadDisposition: "reusable" as const,
      });
      const turn = replacement.stamp({
        type: "provider_turn.updated" as const,
        driver: fixture.driver,
        providerTurn: {
          id: fixture.providerTurnId,
          providerThreadId: fixture.providerThread.id,
          nodeId: fixture.node.id,
          runAttemptId: fixture.attempt.id,
          nativeTurnRef: {
            driver: fixture.driver,
            nativeId: `${fixture.threadId}:turn`,
            strength: "strong" as const,
          },
          ordinal: 1,
          status: "interrupted" as const,
          startedAt: fixture.now,
          completedAt: fixture.now,
        },
      });
      const node = replacement.stamp({ ...fixture.rawNode });
      const message = replacement.stamp({ ...fixture.rawMessage("Fresh replacement output.") });
      yield* runBufferedOutputFixture(fixture, "unused", terminal, {
        events: Stream.fromIterable([node, message, turn, terminal]),
        runtimeGeneration: replacement.binding.runtimeGeneration,
      });
      const projection = yield* fixture.projections.getThreadProjection(fixture.threadId);
      assert.equal(projection.messages[0]?.text, "Fresh replacement output.");
      assert.isFalse(projection.messages[0]!.streaming);
      assert.equal(projection.runs[0]?.status, "interrupted");
      assert.equal(
        readProviderEventOrigin(message)?.producer.token,
        replacement.origin.producer.token,
      );
      assert.equal(
        readProviderEventOrigin(message)?.turn?.binding.runtimeGeneration,
        replacement.binding.runtimeGeneration,
      );
    }),
  ).pipe(Effect.provide(TestLayer)),
);

it.effect(
  "rejects streamed publication when its producer is replaced inside the owner transaction",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fixture = yield* makeBufferedOutputFixture();
        yield* fixture.capture("Streaming publication must retain its original source.");
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const published = yield* fixture.ingestor
          .ingestNormalized({
            ...fixture.input,
            event: fixture.rawMessage("Streaming publication must retain its original source."),
            revalidateCurrentOwner: Deferred.succeed(entered, undefined).pipe(
              Effect.andThen(Deferred.await(release)),
              Effect.andThen(fixture.revalidateCurrentOwner),
            ),
          })
          .pipe(Effect.result, Effect.forkChild);
        yield* Deferred.await(entered);
        const current = yield* Ref.get(fixture.resident);
        yield* Ref.set(fixture.resident, {
          ...current,
          binding: { ...current.binding, runtimeGeneration: "reserved-replacement" },
        });
        yield* Deferred.succeed(release, undefined);
        assert.equal((yield* Fiber.join(published))._tag, "Failure");
        const projection = yield* fixture.projections.getThreadProjection(fixture.threadId);
        assert.lengthOf(projection.messages, 0);
        assert.isFalse(projection.nodes.some((node) => node.id === fixture.assistantNode.id));
        assert.equal(
          (yield* fixture.flush(fixture.providerTurnId).pipe(Effect.result))._tag,
          "Failure",
        );
      }),
    ).pipe(Effect.provide(TestLayer)),
);

it.effect("retains ordinary unstamped assistant publication without claiming a native origin", () =>
  Effect.gen(function* () {
    const fixture = yield* makeBufferedOutputFixture();
    const event = {
      ...fixture.rawMessage("Ordinary output."),
      message: { ...fixture.rawMessage("Ordinary output.").message, streaming: false },
    };
    assert.isUndefined(readProviderEventOrigin(event));
    yield* fixture.sink.write({
      events: yield* fixture.ingestor.normalize({
        ...fixture.input,
        event: { ...fixture.rawNode },
      }),
    });
    yield* fixture.ingestor.ingestNormalized({ ...fixture.input, event });
    assert.equal(
      (yield* fixture.projections.getThreadProjection(fixture.threadId)).messages[0]?.text,
      "Ordinary output.",
    );
    assert.isUndefined(readProviderEventOrigin(event));
    const evidence = yield* fixture.sink.readCurrentProviderRuntimeOwner(fixture.threadId);
    assert.equal(evidence?.binding.runtimeGeneration, fixture.binding.runtimeGeneration);
    assert.isNull(evidence?.observation ?? null);
  }).pipe(Effect.provide(TestLayer)),
);

it.effect("rejects a late assistant node terminal after its current attempt is superseded", () =>
  Effect.gen(function* () {
    const fixture = yield* makeBufferedOutputFixture();
    yield* fixture.capture("Output already belongs to the old attempt.");
    yield* fixture.flush(fixture.providerTurnId);
    const currentAttemptId = RunAttemptId.make(`${fixture.threadId}:late-node-new-attempt`);
    yield* fixture.sink.write({
      events: [
        {
          id: yield* fixture.ids.allocate.event({ threadId: fixture.threadId }),
          type: "run.updated",
          threadId: fixture.threadId,
          runId: fixture.run.id,
          occurredAt: fixture.now,
          payload: { ...fixture.run, activeAttemptId: currentAttemptId },
        },
        {
          id: yield* fixture.ids.allocate.event({ threadId: fixture.threadId }),
          type: "run-attempt.updated",
          threadId: fixture.threadId,
          runId: fixture.run.id,
          occurredAt: fixture.now,
          payload: {
            ...fixture.attempt,
            id: currentAttemptId,
            attemptOrdinal: 2,
            providerTurnId: null,
            reason: "steering_restart",
          },
        },
      ],
    });
    const terminal = fixture.stamp({
      type: "node.updated" as const,
      driver: fixture.driver,
      node: { ...fixture.assistantNode, status: "interrupted" as const, completedAt: fixture.now },
    });
    const result = yield* fixture.ingestor
      .ingestNormalized({ ...fixture.input, event: terminal })
      .pipe(Effect.result);
    assert.equal(result._tag, "Failure");
    const projection = yield* fixture.projections.getThreadProjection(fixture.threadId);
    assert.equal(
      projection.nodes.find((node) => node.id === fixture.assistantNode.id)?.status,
      "running",
    );
    assert.equal(projection.runs[0]?.activeAttemptId, currentAttemptId);
    assert.equal(projection.runs[0]?.status, "running");
    assert.equal(projection.messages[0]?.text, "Output already belongs to the old attempt.");
  }).pipe(Effect.provide(TestLayer)),
);

it.effect("defers a first captured assistant node until its genuine output is published", () =>
  Effect.gen(function* () {
    const fixture = yield* makeBufferedOutputFixture();
    const initial = yield* fixture.ingestor.ingestNormalized({
      ...fixture.input,
      event: fixture.rawNode,
    });
    assert.lengthOf(initial, 0);
    assert.isFalse(
      (yield* fixture.projections.getThreadProjection(fixture.threadId)).nodes.some(
        (node) => node.id === fixture.assistantNode.id,
      ),
    );
    const snapshot = fixture.rawMessage("The output creates its actual companion.");
    const final = fixture.stamp({
      ...snapshot,
      message: { ...snapshot.message, streaming: false },
    });
    yield* fixture.ingestor.captureAssistantOutput({
      ...fixture.input,
      owner: fixture.owner,
      revalidateCurrentOwner: fixture.revalidateCurrentOwner,
      event: final,
    });
    const stored = yield* fixture.ingestor.ingestNormalized({ ...fixture.input, event: final });
    assert.deepEqual(
      stored.map((entry) => entry.event.type),
      ["node.updated", "message.updated"],
    );
    const projection = yield* fixture.projections.getThreadProjection(fixture.threadId);
    assert.deepEqual(
      projection.nodes.find((node) => node.id === fixture.assistantNode.id),
      fixture.assistantNode,
    );
    assert.equal(projection.messages[0]?.text, final.message.text);
    assert.isFalse(
      yield* fixture.ingestor.hasBufferedAssistantOutput({
        binding: fixture.binding,
        providerTurnId: fixture.providerTurnId,
      }),
    );
  }).pipe(Effect.provide(TestLayer)),
);

it.effect("publishes a current reasoning node terminal without replaying its durable item", () =>
  Effect.gen(function* () {
    const fixture = yield* makeBufferedOutputFixture();
    const node: OrchestrationV2ExecutionNode = {
      ...fixture.assistantNode,
      id: NodeId.make(`${fixture.threadId}:final-reasoning-node`),
      kind: "reasoning",
    };
    const raw = fixture.stamp({ type: "node.updated" as const, driver: fixture.driver, node });
    assert.lengthOf(yield* fixture.ingestor.ingestNormalized({ ...fixture.input, event: raw }), 0);
    const item: Extract<OrchestrationV2TurnItem, { readonly type: "reasoning" }> = {
      id: TurnItemId.make(`${fixture.threadId}:final-reasoning`),
      threadId: fixture.threadId,
      runId: fixture.run.id,
      nodeId: node.id,
      providerThreadId: fixture.providerThread.id,
      providerTurnId: fixture.providerTurnId,
      nativeItemRef: null,
      parentItemId: null,
      ordinal: 101,
      status: "completed",
      title: null,
      startedAt: fixture.now,
      completedAt: fixture.now,
      updatedAt: fixture.now,
      type: "reasoning",
      text: "Reasoning already committed.",
      streaming: false,
    };
    const positionedItem = yield* fixture.positions.normalize(item);
    const output = fixture.stamp({
      type: "turn_item.updated" as const,
      driver: fixture.driver,
      turnItem: item,
    });
    yield* fixture.ingestor.captureAssistantOutput({
      ...fixture.input,
      owner: fixture.owner,
      revalidateCurrentOwner: fixture.revalidateCurrentOwner,
      event: output,
    });
    yield* fixture.ingestor.ingestNormalized({ ...fixture.input, event: output });
    const terminal = fixture.stamp({
      type: "node.updated" as const,
      driver: fixture.driver,
      node: { ...node, status: "completed" as const, completedAt: fixture.now },
    });
    const stored = yield* fixture.ingestor.ingestNormalized({ ...fixture.input, event: terminal });
    assert.deepEqual(
      stored.map((entry) => entry.event.type),
      ["node.updated"],
    );
    const projection = yield* fixture.projections.getThreadProjection(fixture.threadId);
    assert.deepEqual(
      projection.nodes.find((candidate) => candidate.id === node.id),
      terminal.node,
    );
    assert.deepEqual(
      projection.turnItems.find((candidate) => candidate.id === item.id),
      positionedItem,
    );
    const history = yield* fixture.store
      .read({ threadId: fixture.threadId })
      .pipe(Stream.runCollect);
    assert.lengthOf(
      history.filter(
        (entry) => entry.event.type === "turn-item.updated" && entry.event.payload.id === item.id,
      ),
      1,
    );
    assert.lengthOf(
      history.filter(
        (entry) => entry.event.type === "node.updated" && entry.event.payload.id === node.id,
      ),
      2,
    );
  }).pipe(Effect.provide(TestLayer)),
);

it.effect("allows an unstamped ordinary terminal after captured output is fully durable", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fixture = yield* makeBufferedOutputFixture();
      const snapshot = fixture.rawMessage("No native tail remains held.");
      const final = fixture.stamp({
        ...snapshot,
        message: { ...snapshot.message, streaming: false },
      });
      const terminal = {
        type: "turn.terminal" as const,
        driver: fixture.driver,
        providerThreadId: fixture.providerThread.id,
        providerTurnId: fixture.providerTurnId,
        runOrdinal: 1,
        status: "interrupted" as const,
        failure: null,
        threadDisposition: "reusable" as const,
      };
      assert.isUndefined(readProviderEventOrigin(terminal));
      yield* runBufferedOutputFixture(fixture, "unused", terminal, {
        events: Stream.fromIterable([fixture.rawNode, final, terminal]),
      });
      const projection = yield* fixture.projections.getThreadProjection(fixture.threadId);
      assert.equal(projection.messages[0]?.text, final.message.text);
      assert.isFalse(projection.messages[0]!.streaming);
      assert.equal(projection.runs[0]?.status, "interrupted");
      assert.isFalse(
        yield* fixture.ingestor.hasBufferedAssistantOutput({
          binding: fixture.binding,
          providerTurnId: fixture.providerTurnId,
        }),
      );
      assert.isUndefined(readProviderEventOrigin(terminal));
      const history = yield* fixture.store
        .read({ threadId: fixture.threadId })
        .pipe(Stream.runCollect);
      assert.lengthOf(
        history.filter((entry) => entry.event.type === "message.updated"),
        1,
      );
      const output = history.find((entry) => entry.event.type === "message.updated")!;
      const run = history.find(
        (entry) =>
          entry.event.type === "run.updated" && entry.event.payload.status === "interrupted",
      )!;
      assert.isBelow(output.sequence, run.sequence);
    }),
  ).pipe(Effect.provide(TestLayer)),
);

it.effect("normalizes runtime identity for the actual known provider instance", () =>
  Effect.gen(function* () {
    const now = yield* DateTime.now;
    const eventSink = yield* EventSink.EventSinkV2;
    const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
    const ids = yield* IdAllocator.IdAllocatorV2;
    const created = yield* threadCreatedEvent(now);
    const threadId = created.threadId;
    const providerSessionId = yield* ids.allocate.providerSession({
      providerInstanceId: modelSelection.instanceId,
      threadId,
    });
    const providerThreadId = ids.derive.providerThread({
      driver: CODEX_DRIVER,
      nativeThreadId: "native-thread",
    });
    yield* eventSink.write({
      events: [
        created,
        {
          id: yield* ids.allocate.event({ threadId }),
          type: "provider-session.attached",
          threadId,
          occurredAt: now,
          payload: {
            id: providerSessionId,
            driver: CODEX_DRIVER,
            providerInstanceId: modelSelection.instanceId,
            status: "ready",
            cwd: process.cwd(),
            model: modelSelection.model,
            capabilities: CodexProviderCapabilitiesV2,
            createdAt: now,
            updatedAt: now,
            lastError: null,
          },
        },
        {
          id: yield* ids.allocate.event({ threadId }),
          type: "provider-thread.updated",
          threadId,
          occurredAt: now,
          payload: {
            id: providerThreadId,
            driver: CODEX_DRIVER,
            providerInstanceId: modelSelection.instanceId,
            providerSessionId,
            appThreadId: threadId,
            ownerNodeId: null,
            nativeThreadRef: {
              driver: CODEX_DRIVER,
              nativeId: "native-thread",
              strength: "strong",
            },
            nativeConversationHeadRef: null,
            status: "idle",
            firstRunOrdinal: null,
            lastRunOrdinal: null,
            handoffIds: [],
            forkedFrom: null,
            createdAt: now,
            updatedAt: now,
          },
        },
      ],
    });
    const normalized = yield* ingestor.normalize({
      threadId,
      providerSessionId,
      providerInstanceId: modelSelection.instanceId,
      event: {
        type: "runtime_identity.observed",
        driver: CODEX_DRIVER,
        binding: {
          threadId,
          providerThreadId,
          providerSessionId,
          instanceId: modelSelection.instanceId,
          nativeThreadId: "native-thread",
          runtimeGeneration: "actual-identity-incarnation",
        },
        attestation: {
          runtimeGeneration: "actual-identity-incarnation",
          requested: {
            providerInstanceId: modelSelection.instanceId,
            providerDriver: CODEX_DRIVER,
            model: modelSelection.model,
            serviceTier: null,
          },
          observed: {
            model: {
              status: "observed",
              value: "native-reported-model",
              sourceEvent: "thread/read",
            },
            backend: { status: "unknown" },
            account: { status: "unknown" },
            serviceTier: { status: "unknown" },
          },
        },
      },
    });
    assert.lengthOf(normalized, 1);
    assert.equal(normalized[0]?.type, "provider-session.updated");
  }).pipe(Effect.provide(TestLayer)),
);

it.effect("records accepted billed turn usage once without billing the context window", () => {
  const recorded: Array<Readonly<Record<string, unknown>>> = [];
  const analytics = Layer.succeed(ProviderEventIngestor.ProviderTurnAnalytics, {
    record: (properties: Readonly<Record<string, unknown>>) =>
      Effect.sync(() => {
        recorded.push(properties);
      }),
  });
  return Effect.gen(function* () {
    const now = yield* DateTime.now;
    const eventSink = yield* EventSink.EventSinkV2;
    const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
    const idAllocator = yield* IdAllocator.IdAllocatorV2;
    const threadEvent = yield* threadCreatedEvent(now);
    yield* eventSink.write({ events: [threadEvent] });
    const providerSessionId = yield* idAllocator.allocate.providerSession({
      providerInstanceId: modelSelection.instanceId,
      threadId: threadEvent.threadId,
    });
    const providerThreadId = idAllocator.derive.providerThread({
      driver: CODEX_DRIVER,
      nativeThreadId: "billing-thread",
    });
    const providerTurn = {
      id: idAllocator.derive.providerTurn({ driver: CODEX_DRIVER, nativeTurnId: "billing-turn" }),
      providerThreadId,
      nodeId: NodeId.make("node:billing-turn"),
      runAttemptId: null,
      nativeTurnRef: null,
      ordinal: 1,
      status: "completed" as const,
      startedAt: now,
      completedAt: DateTime.makeUnsafe(DateTime.toEpochMillis(now) + 120),
      tokenUsage: {
        inputTokens: 4000,
        cachedInputTokens: 3000,
        outputTokens: 100,
        usedTokens: 4100,
        updatedAt: DateTime.formatIso(now),
      },
      turnTokenUsage: {
        usageStatus: "complete" as const,
        usageScope: "main_agent" as const,
        hasSubagents: false,
        inputTokens: 40,
        cachedInputTokens: 30,
        outputTokens: 10,
      },
    };
    const input = {
      providerSessionId,
      providerInstanceId: modelSelection.instanceId,
      threadId: threadEvent.threadId,
      analyticsContext: {
        modelSelection,
        runtimeMode: "full-access" as const,
        interactionMode: "default" as const,
      },
      event: { type: "provider_turn.updated" as const, driver: CODEX_DRIVER, providerTurn },
    };
    yield* ingestor.ingestNormalized(input);
    yield* ingestor.ingestNormalized(input);
    const ignored = yield* ingestor.ingestNormalized({
      ...input,
      event: {
        ...input.event,
        providerTurn: {
          ...providerTurn,
          id: idAllocator.derive.providerTurn({
            driver: CODEX_DRIVER,
            nativeTurnId: "stale-billing-turn",
          }),
        },
      },
      writeIfRunCurrent: {
        runId: RunId.make("missing-run"),
        activeAttemptId: RunAttemptId.make("stale-attempt"),
        expectedStatus: "running",
      },
    });
    assert.isEmpty(ignored);
    assert.lengthOf(recorded, 1);
    assert.deepEqual(recorded[0], {
      provider: CODEX_DRIVER,
      terminalStatus: "completed",
      usageStatus: "complete",
      usageScope: "main_agent",
      hasSubagents: false,
      inputTokens: 40,
      cachedInputTokens: 30,
      outputTokens: 10,
      model: modelSelection.model,
      mixedModels: false,
      runtimeMode: "full-access",
      interactionMode: "default",
      durationMs: 120,
    });
  }).pipe(Effect.provide(TestLayer.pipe(Layer.provide(analytics))));
});

layer("ProviderEventIngestorV2", (it) => {
  it.effect("normalizes provider events through the real event log and projection store", () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const eventSink = yield* EventSink.EventSinkV2;
      const eventStore = yield* EventStore.EventStoreV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const threadEvent = yield* threadCreatedEvent(now);
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId: threadEvent.threadId,
      });
      const providerThread: OrchestrationV2ProviderThread = {
        id: idAllocator.derive.providerThread({
          driver: CODEX_DRIVER,
          nativeThreadId: "native-thread",
        }),
        driver: CODEX_DRIVER,
        providerInstanceId: modelSelection.instanceId,
        providerSessionId,
        appThreadId: threadEvent.threadId,
        ownerNodeId: null,
        nativeThreadRef: {
          driver: CODEX_DRIVER,
          nativeId: "native-thread",
          strength: "strong",
        },
        nativeConversationHeadRef: null,
        status: "idle",
        firstRunOrdinal: null,
        lastRunOrdinal: null,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      };

      yield* eventSink.write({ events: [threadEvent] });
      const storedEvents = yield* ingestor.ingestNormalized({
        providerSessionId,
        providerInstanceId: modelSelection.instanceId,
        threadId: threadEvent.threadId,
        event: {
          type: "provider_thread.updated",
          driver: CODEX_DRIVER,
          providerThread,
        },
      });

      const projection = yield* projectionStore.getThreadProjection(threadEvent.threadId);
      const storedDomainEvents = yield* eventStore.read({}).pipe(Stream.runCollect);
      const afterFirstEvent = yield* eventStore
        .read({ afterSequence: 1, threadId: threadEvent.threadId })
        .pipe(Stream.runCollect);
      const latestThreadSequence = yield* eventStore.latestSequence({
        threadId: threadEvent.threadId,
      });

      assert.equal(storedEvents.length, 1);
      assert.equal(storedEvents[0]?.event.type, "provider-thread.updated");
      assert.deepEqual(
        projection.providerThreads.map((thread) => thread.id),
        [providerThread.id],
      );
      assert.deepEqual(
        Array.from(storedDomainEvents).map((stored) => stored.event.type),
        ["thread.created", "provider-thread.updated"],
      );
      assert.deepEqual(
        Array.from(storedDomainEvents).map((stored) => stored.sequence),
        [1, 2],
      );
      assert.deepEqual(
        Array.from(afterFirstEvent).map((stored) => stored.event.type),
        ["provider-thread.updated"],
      );
      assert.equal(latestThreadSequence, 2);
    }),
  );

  it.effect("carries plan-step durations through consecutive and restarted ingestion", () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(Date.parse("2026-09-07T00:00:00.000Z"));
      const now = yield* DateTime.now;
      const eventSink = yield* EventSink.EventSinkV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const threadEvent = yield* threadCreatedEvent(now);
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId: threadEvent.threadId,
      });
      const planId = PlanId.make("plan:provider-event-duration");
      const nodeId = NodeId.make("node:provider-event-duration");
      type TodoListPlan = Extract<OrchestrationV2PlanArtifact, { readonly kind: "todo_list" }>;
      const plan = (steps: TodoListPlan["steps"]): TodoListPlan => ({
        id: planId,
        threadId: threadEvent.threadId,
        runId: null,
        nodeId,
        kind: "todo_list",
        status: "active",
        steps,
      });
      const ingest = (
        service: ProviderEventIngestor.ProviderEventIngestorV2["Service"],
        steps: TodoListPlan["steps"],
      ) =>
        service.ingestNormalized({
          providerSessionId,
          providerInstanceId: modelSelection.instanceId,
          threadId: threadEvent.threadId,
          event: { type: "plan.updated", driver: CODEX_DRIVER, plan: plan(steps) },
        });

      yield* eventSink.write({ events: [threadEvent] });
      yield* ingest(ingestor, [
        { id: "duplicate-a", text: "Verify", status: "running" },
        { id: "duplicate-b", text: "Verify", status: "pending" },
        { id: "fallback", text: "Report", status: "pending" },
      ]);
      yield* TestClock.adjust("3 seconds");
      yield* ingest(ingestor, [
        { id: "duplicate-a", text: "Verify", status: "completed" },
        { id: "duplicate-b", text: "Verify", status: "pending" },
        { id: "fallback", text: "Report", status: "pending" },
      ]);

      const restartedIngestor = yield* ProviderEventIngestor.ProviderEventIngestorV2.pipe(
        Effect.provide(
          Layer.fresh(ProviderEventIngestor.layer).pipe(
            Layer.provide(
              Layer.succeed(ProjectionStore.ProjectionStoreV2, {
                ...projectionStore,
                getThreadProjection: () => Effect.die("Plan timing must not load thread history"),
              }),
            ),
          ),
        ),
      );
      yield* TestClock.adjust("4 seconds");
      yield* ingest(restartedIngestor, [
        { id: "duplicate-a", text: "Verify", status: "completed" },
        { id: "duplicate-b", text: "Verify", status: "completed" },
        { id: "fallback", text: "Report", status: "pending" },
      ]);
      yield* TestClock.adjust("5 seconds");
      yield* ingest(restartedIngestor, [
        { id: "duplicate-a", text: "Verify", status: "completed" },
        { id: "duplicate-b", text: "Verify", status: "completed" },
        { id: "fallback", text: "Report", status: "completed" },
      ]);

      const projection = yield* projectionStore.getThreadProjection(threadEvent.threadId);
      const persisted = projection.plans.find(
        (candidate): candidate is TodoListPlan =>
          candidate.kind === "todo_list" && candidate.id === planId,
      );
      assert.deepEqual(
        persisted?.steps.map(({ id, text, status, durationMs }) => ({
          id,
          text,
          status,
          durationMs,
        })),
        [
          { id: "duplicate-a", text: "Verify", status: "completed", durationMs: 3_000 },
          { id: "duplicate-b", text: "Verify", status: "completed", durationMs: 4_000 },
          { id: "fallback", text: "Report", status: "completed", durationMs: 5_000 },
        ],
      );

      yield* ingest(restartedIngestor, [
        { id: "duplicate-a", text: "Inserted task", status: "running" },
        { id: "duplicate-b", text: "Verify", status: "completed" },
        { id: "fallback", text: "Different completed task", status: "completed" },
      ]);
      yield* TestClock.adjust("2 seconds");
      yield* ingest(restartedIngestor, [
        { id: "duplicate-a", text: "Inserted task", status: "completed" },
        { id: "duplicate-b", text: "Verify", status: "completed" },
        { id: "fallback", text: "Different completed task", status: "completed" },
      ]);
      const updated = yield* projectionStore.getThreadProjection(threadEvent.threadId);
      const changedPlan = updated.plans.find(
        (candidate): candidate is TodoListPlan =>
          candidate.kind === "todo_list" && candidate.id === planId,
      );
      assert.deepEqual(
        changedPlan?.steps.map((step) => step.durationMs),
        [2_000, 4_000, undefined],
      );
    }),
  );

  it.effect(
    "treats successful provider terminal markers as non-persisted orchestration control signals",
    () =>
      Effect.gen(function* () {
        const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const projectId = yield* idAllocator.allocate.project({
          fixtureName: "provider-event-terminal",
        });
        const threadId = yield* idAllocator.allocate.thread({
          fixtureName: "provider-event-terminal",
          projectId,
        });
        const providerSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });
        const normalized = yield* ingestor.normalize({
          providerSessionId,
          providerInstanceId: modelSelection.instanceId,
          threadId,
          event: {
            type: "turn.terminal",
            driver: CODEX_DRIVER,
            providerThreadId: idAllocator.derive.providerThread({
              driver: CODEX_DRIVER,
              nativeThreadId: "native-thread",
            }),
            providerTurnId: idAllocator.derive.providerTurn({
              driver: CODEX_DRIVER,
              nativeTurnId: "native-turn",
            }),
            runOrdinal: 1,
            status: "completed",
            failure: null,
            threadDisposition: "reusable",
          },
        });

        assert.deepEqual(normalized, []);
      }),
  );

  it.effect("persists an interrupted run's inherited terminal through the live run router", () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const eventSink = yield* EventSink.EventSinkV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const threadEvent = yield* threadCreatedEvent(now);
      const priorRunId = RunId.make("run:provider-event-inherited:prior");
      const currentRunId = RunId.make("run:provider-event-inherited:current");
      const itemId = TurnItemId.make("turn-item:provider-event-inherited");
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId: threadEvent.threadId,
      });
      const providerThreadId = idAllocator.derive.providerThread({
        driver: CODEX_DRIVER,
        nativeThreadId: "native-thread-inherited",
      });
      const providerTurnId = idAllocator.derive.providerTurn({
        driver: CODEX_DRIVER,
        nativeTurnId: "native-turn-inherited",
      });
      const runningItem = {
        id: itemId,
        threadId: threadEvent.threadId,
        runId: priorRunId,
        nodeId: NodeId.make("node:provider-event-inherited"),
        providerThreadId,
        providerTurnId,
        nativeItemRef: null,
        parentItemId: null,
        ordinal: 101,
        status: "running",
        title: "Inherited background command",
        startedAt: now,
        completedAt: null,
        updatedAt: now,
        type: "command_execution",
        input: "sleep 60",
      } satisfies OrchestrationV2TurnItem;
      const terminalItem = {
        ...runningItem,
        status: "completed" as const,
        completedAt: now,
        updatedAt: now,
      };

      yield* eventSink.write({ events: [threadEvent] });
      yield* ingestor.ingestNormalized({
        providerSessionId,
        providerInstanceId: modelSelection.instanceId,
        threadId: threadEvent.threadId,
        runId: priorRunId,
        event: { type: "turn_item.updated", driver: CODEX_DRIVER, turnItem: runningItem },
      });

      const identity: ProviderEventRouteIdentity = {
        threadId: threadEvent.threadId,
        runId: currentRunId,
        attemptId: RunAttemptId.make("attempt:provider-event-inherited:current"),
        providerThreadId,
      };
      const inheritedBackgroundTurnItems = selectInheritedBackgroundTurnItems({
        threadId: threadEvent.threadId,
        currentProviderThreadId: providerThreadId,
        currentRunOrdinal: 2,
        runs: [
          {
            id: priorRunId,
            threadId: threadEvent.threadId,
            ordinal: 1,
            status: "interrupted",
          } as OrchestrationV2Run,
          {
            id: currentRunId,
            threadId: threadEvent.threadId,
            ordinal: 2,
            status: "running",
          } as OrchestrationV2Run,
        ],
        turnItems: [runningItem],
      });
      const routeState = makeProviderEventRoutingState({
        identity,
        inheritedBackgroundTurnItems,
        providerTurnId: null,
      });
      const terminalEvent = {
        type: "turn_item.updated",
        driver: CODEX_DRIVER,
        turnItem: terminalItem,
      } as const;
      const [accepted] = routeProviderEvent(terminalEvent, identity, routeState);
      assert.isTrue(accepted);

      const stored = yield* ingestor.ingestNormalized({
        providerSessionId,
        providerInstanceId: modelSelection.instanceId,
        threadId: threadEvent.threadId,
        runId: currentRunId,
        event: terminalEvent,
      });
      const projection = yield* projectionStore.getThreadProjection(threadEvent.threadId);
      const persisted = projection.turnItems.find((item) => item.id === itemId);

      assert.equal(stored.length, 1);
      assert.equal(stored[0]?.event.type, "turn-item.updated");
      assert.equal(persisted?.runId, priorRunId);
      assert.equal(persisted?.threadId, threadEvent.threadId);
      assert.equal(persisted?.status, "completed");
    }),
  );

  it.effect("persists a completed run's late background terminal exactly once", () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const eventSink = yield* EventSink.EventSinkV2;
      const eventStore = yield* EventStore.EventStoreV2;
      const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const threadEvent = yield* threadCreatedEvent(now);
      const priorRunId = RunId.make("run:provider-event-completed:prior");
      const currentRunId = RunId.make("run:provider-event-completed:current");
      const itemId = TurnItemId.make("turn-item:provider-event-completed");
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId: threadEvent.threadId,
      });
      const providerThreadId = idAllocator.derive.providerThread({
        driver: CODEX_DRIVER,
        nativeThreadId: "native-thread-completed",
      });
      const providerTurnId = idAllocator.derive.providerTurn({
        driver: CODEX_DRIVER,
        nativeTurnId: "native-turn-completed",
      });
      const runningItem = {
        id: itemId,
        threadId: threadEvent.threadId,
        runId: priorRunId,
        nodeId: NodeId.make("node:provider-event-completed"),
        providerThreadId,
        providerTurnId,
        nativeItemRef: null,
        parentItemId: null,
        ordinal: 101,
        status: "running",
        title: "Completed run background command",
        startedAt: now,
        completedAt: null,
        updatedAt: now,
        type: "command_execution",
        input: "sleep 60",
      } satisfies OrchestrationV2TurnItem;
      const terminalEvent = {
        type: "turn_item.updated",
        driver: CODEX_DRIVER,
        turnItem: {
          ...runningItem,
          status: "completed" as const,
          completedAt: now,
          updatedAt: now,
        },
      } as const;

      yield* eventSink.write({ events: [threadEvent] });
      yield* ingestor.ingestNormalized({
        providerSessionId,
        providerInstanceId: modelSelection.instanceId,
        threadId: threadEvent.threadId,
        runId: priorRunId,
        event: { type: "turn_item.updated", driver: CODEX_DRIVER, turnItem: runningItem },
      });

      const priorIdentity: ProviderEventRouteIdentity = {
        threadId: threadEvent.threadId,
        runId: priorRunId,
        attemptId: RunAttemptId.make("attempt:provider-event-completed:prior"),
        providerThreadId,
      };
      const currentIdentity: ProviderEventRouteIdentity = {
        threadId: threadEvent.threadId,
        runId: currentRunId,
        attemptId: RunAttemptId.make("attempt:provider-event-completed:current"),
        providerThreadId,
      };
      const inheritedBackgroundTurnItems = selectInheritedBackgroundTurnItems({
        threadId: threadEvent.threadId,
        currentProviderThreadId: providerThreadId,
        currentRunOrdinal: 2,
        runs: [
          {
            id: priorRunId,
            threadId: threadEvent.threadId,
            ordinal: 1,
            status: "completed",
          } as OrchestrationV2Run,
          {
            id: currentRunId,
            threadId: threadEvent.threadId,
            ordinal: 2,
            status: "running",
          } as OrchestrationV2Run,
        ],
        turnItems: [runningItem],
      });
      const routers = [
        {
          identity: priorIdentity,
          state: makeProviderEventRoutingState({
            identity: priorIdentity,
            providerTurnId: providerTurnId,
          }),
        },
        {
          identity: currentIdentity,
          state: makeProviderEventRoutingState({
            identity: currentIdentity,
            inheritedBackgroundTurnItems,
            providerTurnId: null,
          }),
        },
      ];
      const acceptedRouters = routers.filter(
        ({ identity, state }) => routeProviderEvent(terminalEvent, identity, state)[0],
      );

      yield* Effect.forEach(
        acceptedRouters,
        ({ identity }) =>
          ingestor.ingestNormalized({
            providerSessionId,
            providerInstanceId: modelSelection.instanceId,
            threadId: threadEvent.threadId,
            runId: identity.runId,
            event: terminalEvent,
          }),
        { concurrency: 1 },
      );

      const storedEvents = yield* eventStore
        .read({ threadId: threadEvent.threadId })
        .pipe(Stream.runCollect);
      const storedTerminals = Array.from(storedEvents).filter(
        (stored) =>
          stored.event.type === "turn-item.updated" &&
          stored.event.payload.id === itemId &&
          stored.event.payload.status === "completed",
      );

      assert.equal(storedTerminals.length, 1);
      assert.equal(acceptedRouters.length, 1);
      assert.equal(acceptedRouters[0]?.identity.runId, priorRunId);
    }),
  );

  for (const terminal of ["completed", "interrupted", "failed", "cancelled", "control"] as const) {
    it.effect(`dismisses only native questions when a provider turn ends with ${terminal}`, () =>
      Effect.gen(function* () {
        const now = yield* DateTime.now;
        const eventSink = yield* EventSink.EventSinkV2;
        const eventStore = yield* EventStore.EventStoreV2;
        const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
        const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const threadEvent = yield* threadCreatedEvent(now);
        const threadId = threadEvent.threadId;
        const providerSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });
        const providerThreadId = idAllocator.derive.providerThread({
          driver: CODEX_DRIVER,
          nativeThreadId: `${threadId}:questions`,
        });
        const providerTurnId = idAllocator.derive.providerTurn({
          driver: CODEX_DRIVER,
          nativeTurnId: `${threadId}:questions`,
        });
        const otherTurnId = idAllocator.derive.providerTurn({
          driver: CODEX_DRIVER,
          nativeTurnId: `${threadId}:other-turn`,
        });
        const specs = [
          { key: "native", responseCapability: { type: "live", providerSessionId } },
          {
            key: "unavailable",
            responseCapability: { type: "not_resumable", reason: "Turn ended" },
          },
          { key: "message", responseCapability: { type: "message" } },
          { key: "answered", responseCapability: { type: "live", providerSessionId } },
          { key: "other-turn", responseCapability: { type: "live", providerSessionId } },
          { key: "approval", responseCapability: { type: "live", providerSessionId } },
        ] as const;
        const fixtures = specs.map((spec, ordinal) => {
          const nodeId = NodeId.make(`${threadId}:${spec.key}`);
          const resolved = spec.key === "answered";
          const request: OrchestrationV2RuntimeRequest = {
            id: RuntimeRequestId.make(`${threadId}:${spec.key}`),
            nodeId,
            providerTurnId: spec.key === "other-turn" ? otherTurnId : providerTurnId,
            nativeRequestRef: null,
            kind: spec.key === "approval" ? "command" : "user_input",
            status: resolved ? "resolved" : "pending",
            responseCapability: spec.responseCapability,
            createdAt: now,
            resolvedAt: resolved ? now : null,
          };
          const node: OrchestrationV2ExecutionNode = {
            id: nodeId,
            threadId,
            runId: null,
            parentNodeId: null,
            rootNodeId: nodeId,
            kind: spec.key === "approval" ? "approval_request" : "user_input_request",
            status: resolved ? "completed" : "waiting",
            countsForRun: false,
            providerThreadId,
            providerTurnId: request.providerTurnId,
            nativeItemRef: null,
            runtimeRequestId: request.id,
            checkpointScopeId: null,
            startedAt: now,
            completedAt: resolved ? now : null,
          };
          const item: OrchestrationV2TurnItem = {
            id: TurnItemId.make(`${threadId}:${spec.key}`),
            threadId,
            runId: null,
            nodeId,
            providerThreadId,
            providerTurnId: request.providerTurnId,
            nativeItemRef: null,
            parentItemId: null,
            ordinal,
            status: resolved ? "completed" : "waiting",
            title: "Which option?",
            startedAt: now,
            completedAt: resolved ? now : null,
            updatedAt: now,
            ...(spec.key === "approval"
              ? { type: "approval_request", requestId: request.id, requestKind: "command" }
              : {
                  type: "user_input_request",
                  requestId: request.id,
                  questions: [],
                  ...(spec.key === "message" ? { responseMode: "message" as const } : {}),
                }),
          };
          return { key: spec.key, request, node, item };
        });
        const seedEvents: Array<OrchestrationV2DomainEvent> = [threadEvent];
        for (const fixture of fixtures) {
          for (const payload of [
            { type: "runtime-request.updated" as const, payload: fixture.request },
            { type: "node.updated" as const, payload: fixture.node },
            { type: "turn-item.updated" as const, payload: fixture.item },
          ]) {
            seedEvents.push({
              id: yield* idAllocator.allocate.event({ threadId }),
              threadId,
              occurredAt: now,
              ...payload,
            });
          }
        }
        yield* eventSink.write({ events: seedEvents });
        const input = {
          providerSessionId,
          providerInstanceId: modelSelection.instanceId,
          threadId,
          event:
            terminal === "control"
              ? {
                  type: "turn.terminal" as const,
                  driver: CODEX_DRIVER,
                  providerThreadId,
                  providerTurnId,
                  runOrdinal: 1,
                  status: "completed" as const,
                  failure: null,
                  threadDisposition: "reusable" as const,
                }
              : {
                  type: "provider_turn.updated" as const,
                  driver: CODEX_DRIVER,
                  providerTurn: {
                    id: providerTurnId,
                    providerThreadId,
                    nodeId: NodeId.make(`${threadId}:root`),
                    runAttemptId: null,
                    nativeTurnRef: null,
                    ordinal: 1,
                    status: terminal,
                    startedAt: now,
                    completedAt: now,
                  },
                },
        };
        const stored = yield* ingestor.ingestNormalized(input);
        const projection = yield* projectionStore.getThreadProjection(threadId);
        for (const fixture of fixtures) {
          const closed = fixture.key === "native" || fixture.key === "unavailable";
          const request = projection.runtimeRequests.find(
            (item) => item.id === fixture.request.id,
          )!;
          const node = projection.nodes.find((item) => item.id === fixture.node.id)!;
          const item = projection.turnItems.find((item) => item.id === fixture.item.id)!;
          if (closed) {
            assert.equal(request.status, "cancelled");
            assert.isNotNull(request.resolvedAt);
            assert.equal(node.status, "cancelled");
            assert.isNotNull(node.completedAt);
            assert.equal(item.status, "cancelled");
            assert.isNotNull(item.completedAt);
          } else {
            assert.equal(request.status, fixture.request.status);
            assert.equal(node.status, fixture.node.status);
            assert.equal(item.status, fixture.item.status);
          }
        }
        assert.equal(
          stored.filter((entry) => entry.event.type === "runtime-request.updated").length,
          2,
        );
        assert.isEmpty(
          (yield* projectionStore.getPendingNativeUserInputs(threadId, providerTurnId))
            .runtimeRequests,
        );
        const replayed = yield* eventStore.read({ threadId }).pipe(Stream.runCollect);
        assert.equal(
          replayed.filter(
            (entry) =>
              entry.event.type === "runtime-request.updated" &&
              entry.event.payload.status === "cancelled",
          ).length,
          2,
        );
        const repeated = yield* ingestor.ingestNormalized(input);
        assert.isFalse(repeated.some((entry) => entry.event.type === "runtime-request.updated"));
      }),
    );
  }

  it.effect(
    "preserves an answer committed after terminal normalization reads a pending question",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const eventSink = yield* EventSink.EventSinkV2;
          const eventStore = yield* EventStore.EventStoreV2;
          const projections = yield* ProjectionStore.ProjectionStoreV2;
          const idAllocator = yield* IdAllocator.IdAllocatorV2;
          const now = yield* DateTime.now;
          const threadEvent = yield* threadCreatedEvent(now);
          const threadId = threadEvent.threadId;
          const providerSessionId = yield* idAllocator.allocate.providerSession({
            providerInstanceId: modelSelection.instanceId,
            threadId,
          });
          const providerThreadId = idAllocator.derive.providerThread({
            driver: CODEX_DRIVER,
            nativeThreadId: `${threadId}:race`,
          });
          const providerTurnId = idAllocator.derive.providerTurn({
            driver: CODEX_DRIVER,
            nativeTurnId: `${threadId}:race`,
          });
          const nodeId = NodeId.make(`${threadId}:question`);
          const request: OrchestrationV2RuntimeRequest = {
            id: RuntimeRequestId.make(`${threadId}:question`),
            nodeId,
            providerTurnId,
            nativeRequestRef: null,
            kind: "user_input",
            status: "pending",
            responseCapability: { type: "live", providerSessionId },
            createdAt: now,
            resolvedAt: null,
          };
          const node: OrchestrationV2ExecutionNode = {
            id: nodeId,
            threadId,
            runId: null,
            parentNodeId: null,
            rootNodeId: nodeId,
            kind: "user_input_request",
            status: "waiting",
            countsForRun: false,
            providerThreadId,
            providerTurnId,
            nativeItemRef: null,
            runtimeRequestId: request.id,
            checkpointScopeId: null,
            startedAt: now,
            completedAt: null,
          };
          const item: OrchestrationV2TurnItem = {
            id: TurnItemId.make(`${threadId}:question`),
            threadId,
            runId: null,
            nodeId,
            providerThreadId,
            providerTurnId,
            nativeItemRef: null,
            parentItemId: null,
            ordinal: 1,
            status: "waiting",
            title: "Which option?",
            startedAt: now,
            completedAt: null,
            updatedAt: now,
            type: "user_input_request",
            requestId: request.id,
            questions: [],
          };
          const seedEvents: Array<OrchestrationV2DomainEvent> = [threadEvent];
          for (const payload of [
            { type: "runtime-request.updated" as const, payload: request },
            { type: "node.updated" as const, payload: node },
            { type: "turn-item.updated" as const, payload: item },
          ])
            seedEvents.push({
              id: yield* idAllocator.allocate.event({ threadId }),
              threadId,
              occurredAt: now,
              ...payload,
            });
          yield* eventSink.write({ events: seedEvents });
          const normalized = yield* Deferred.make<void>();
          const releaseTerminalWrite = yield* Deferred.make<void>();
          const gatedSink = EventSink.EventSinkV2.of({
            ...eventSink,
            write: (input) =>
              Deferred.succeed(normalized, undefined).pipe(
                Effect.andThen(Deferred.await(releaseTerminalWrite)),
                Effect.andThen(eventSink.write(input)),
              ),
          });
          const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2.pipe(
            Effect.provide(Layer.fresh(ProviderEventIngestor.layer)),
            Effect.provideService(EventSink.EventSinkV2, gatedSink),
          );
          const terminal = yield* ingestor
            .ingestNormalized({
              providerSessionId,
              providerInstanceId: modelSelection.instanceId,
              threadId,
              event: {
                type: "provider_turn.updated",
                driver: CODEX_DRIVER,
                providerTurn: {
                  id: providerTurnId,
                  providerThreadId,
                  nodeId,
                  runAttemptId: null,
                  nativeTurnRef: null,
                  ordinal: 1,
                  status: "completed",
                  startedAt: now,
                  completedAt: now,
                },
              },
            })
            .pipe(Effect.forkScoped);
          yield* Deferred.await(normalized);
          const answers = { decision: "Use the existing workspace" };
          const responseEvents: Array<OrchestrationV2DomainEvent> = [];
          for (const payload of [
            {
              type: "runtime-request.updated" as const,
              payload: { ...request, status: "resolved" as const, answers, resolvedAt: now },
            },
            {
              type: "node.updated" as const,
              payload: { ...node, status: "completed" as const, completedAt: now },
            },
            {
              type: "turn-item.updated" as const,
              payload: { ...item, status: "completed" as const, completedAt: now },
            },
          ])
            responseEvents.push({
              id: yield* idAllocator.allocate.event({ threadId }),
              threadId,
              occurredAt: now,
              ...payload,
            });
          yield* eventSink.write({ events: responseEvents });
          yield* Deferred.succeed(releaseTerminalWrite, undefined);
          const committedTerminal = yield* Fiber.join(terminal);
          assert.deepEqual(
            committedTerminal.map((stored) => stored.event.type),
            ["provider-turn.updated"],
          );
          const projection = yield* projections.getThreadProjection(threadId);
          const answered = projection.runtimeRequests.find((entry) => entry.id === request.id)!;
          assert.equal(answered.status, "resolved");
          assert.deepEqual(answered.answers, answers);
          assert.equal(projection.nodes.find((entry) => entry.id === nodeId)!.status, "completed");
          assert.equal(
            projection.turnItems.find((entry) => entry.id === item.id)!.status,
            "completed",
          );
          const history = yield* eventStore.read({ threadId }).pipe(Stream.runCollect);
          assert.isFalse(
            history.some(
              (stored) =>
                stored.event.type === "runtime-request.updated" &&
                stored.event.payload.status === "cancelled",
            ),
          );
        }),
      ),
  );

  it.effect("persists a failed provider terminal as one expected error item", () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const retryStartedAt = DateTime.makeUnsafe(DateTime.toEpochMillis(now) - 5_000);
      const eventSink = yield* EventSink.EventSinkV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const threadEvent = yield* threadCreatedEvent(now);
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId: threadEvent.threadId,
      });
      const providerThreadId = idAllocator.derive.providerThread({
        driver: CODEX_DRIVER,
        nativeThreadId: "native-thread-failed",
      });
      const providerTurnId = idAllocator.derive.providerTurn({
        driver: CODEX_DRIVER,
        nativeTurnId: "native-turn-failed",
      });

      yield* eventSink.write({ events: [threadEvent] });
      const stored = yield* ingestor.ingestNormalized({
        providerSessionId,
        providerInstanceId: modelSelection.instanceId,
        threadId: threadEvent.threadId,
        event: {
          type: "turn.terminal",
          driver: CODEX_DRIVER,
          providerThreadId,
          providerTurnId,
          runOrdinal: 1,
          failureItemOrdinal: 102,
          status: "failed",
          failure: makeProviderFailure({
            message: "Invalid reasoning effort.",
            code: "invalid_request",
            class: "validation_error",
          }),
          retry: {
            attempt: 3,
            maxAttempts: 3,
            retryDelayMs: 2_000,
          },
          retryStartedAt,
          threadDisposition: "reusable",
        },
      });

      const projection = yield* projectionStore.getThreadProjection(threadEvent.threadId);
      const errorItems = projection.visibleTurnItems.filter(
        (candidate) => candidate.item.type === "error",
      );

      assert.equal(stored.length, 1);
      assert.equal(stored[0]?.event.type, "turn-item.updated");
      assert.equal(errorItems.length, 1);
      const errorItem = errorItems[0]?.item;
      assert.equal(errorItem?.type, "error");
      if (errorItem?.type !== "error") return;
      assert.equal(errorItem.failure.message, "Invalid reasoning effort.");
      assert.equal(errorItem.failure.code, "invalid_request");
      assert.deepEqual(errorItem.retry, {
        attempt: 3,
        maxAttempts: 3,
        retryDelayMs: 2_000,
      });
      const errorStartedAt = errorItem.startedAt;
      assert.ok(errorStartedAt);
      assert.equal(DateTime.toEpochMillis(errorStartedAt), DateTime.toEpochMillis(retryStartedAt));
      assert.equal(errorItem.providerThreadId, providerThreadId);
      assert.equal(errorItem.providerTurnId, providerTurnId);
    }),
  );

  it.effect("routes provider-owned child artifacts to their child app thread", () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const rootEvent = yield* threadCreatedEvent(now);
      if (rootEvent.type !== "thread.created") {
        throw new Error("Expected a thread.created fixture event");
      }
      const childThreadId = idAllocator.derive.threadFromProviderThread({
        driver: CODEX_DRIVER,
        nativeThreadId: "native-subagent-thread",
      });
      const childRootNodeId = NodeId.make("node:subagent-root");
      const childThread: OrchestrationV2AppThread = {
        ...rootEvent.payload,
        id: childThreadId,
        title: "inspect package",
        activeProviderThreadId: null,
        lineage: {
          parentThreadId: rootEvent.threadId,
          relationshipToParent: "subagent",
          rootThreadId: rootEvent.threadId,
        },
        forkedFrom: {
          type: "node",
          nodeId: NodeId.make("node:parent-subagent"),
        },
      };
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId: rootEvent.threadId,
      });

      const threadEvents = yield* ingestor.normalize({
        providerSessionId,
        providerInstanceId: modelSelection.instanceId,
        threadId: rootEvent.threadId,
        event: {
          type: "app_thread.created",
          driver: CODEX_DRIVER,
          appThread: childThread,
        },
      });
      const messageEvents = yield* ingestor.normalize({
        providerSessionId,
        providerInstanceId: modelSelection.instanceId,
        threadId: rootEvent.threadId,
        event: {
          type: "message.updated",
          driver: CODEX_DRIVER,
          message: {
            createdBy: "agent",
            creationSource: "provider",
            id: MessageId.make("message:subagent-response"),
            threadId: childThreadId,
            runId: null,
            nodeId: childRootNodeId,
            role: "assistant",
            text: "Subagent result",
            attachments: [],
            streaming: false,
            createdAt: now,
            updatedAt: now,
          },
        },
      });

      assert.equal(threadEvents[0]?.type, "thread.created");
      assert.equal(threadEvents[0]?.threadId, childThreadId);
      assert.equal(messageEvents[0]?.type, "message.updated");
      assert.equal(messageEvents[0]?.threadId, childThreadId);
    }),
  );
});

for (const scenario of [
  "current",
  "stale_attempt",
  "conflicting_turn",
  "foreign_root",
  "replaced_runtime",
  "non_primary",
  "guarded_non_primary",
] as const) {
  it.effect(`primary turn association preserves accepted ownership (${scenario})`, () =>
    Effect.gen(function* () {
      const fixture = yield* makeBufferedOutputFixture();
      const projection = yield* fixture.projections.getThreadProjection(fixture.threadId);
      const turn = projection.providerTurns.find((item) => item.id === fixture.providerTurnId)!;
      const unboundAttempt = { ...fixture.attempt, providerTurnId: null };
      const unboundRoot = { ...fixture.node, providerTurnId: null };
      const seed = [
        { type: "run-attempt.updated" as const, payload: unboundAttempt },
        { type: "node.updated" as const, payload: unboundRoot },
      ];
      yield* fixture.sink.write({
        events: yield* Effect.forEach(seed, (event) =>
          Effect.gen(function* () {
            return {
              ...event,
              id: yield* fixture.ids.allocate.event({ threadId: fixture.threadId }),
              threadId: fixture.threadId,
              runId: fixture.run.id,
              occurredAt: fixture.now,
            };
          }),
        ),
      });
      if (scenario === "stale_attempt") {
        yield* fixture.sink.write({
          events: [
            {
              id: yield* fixture.ids.allocate.event({ threadId: fixture.threadId }),
              threadId: fixture.threadId,
              runId: fixture.run.id,
              occurredAt: fixture.now,
              type: "run.updated",
              payload: {
                ...fixture.run,
                activeAttemptId: RunAttemptId.make(`${fixture.run.id}:replacement`),
              },
            },
          ],
        });
      }
      if (scenario === "conflicting_turn") {
        yield* fixture.sink.write({
          events: [
            {
              id: yield* fixture.ids.allocate.event({ threadId: fixture.threadId }),
              threadId: fixture.threadId,
              runId: fixture.run.id,
              occurredAt: fixture.now,
              type: "run-attempt.updated",
              payload: {
                ...unboundAttempt,
                providerTurnId: fixture.ids.derive.providerTurn({
                  driver: fixture.driver,
                  nativeTurnId: "different-accepted-turn",
                }),
              },
            },
          ],
        });
      }
      if (scenario === "replaced_runtime") {
        yield* Ref.set(fixture.resident, { handle: {}, binding: fixture.binding });
      }
      const before = yield* fixture.projections.getThreadProjection(fixture.threadId);
      const nonPrimary = scenario === "non_primary" || scenario === "guarded_non_primary";
      const backgroundNativeTurnId = `${fixture.threadId}:background-only-turn`;
      const event = stampProviderEvent(
        {
          type: "provider_turn.updated" as const,
          driver: fixture.driver,
          threadId: fixture.threadId,
          providerTurn: {
            ...turn,
            ...(scenario === "foreign_root"
              ? { nodeId: NodeId.make(`${fixture.node.id}:foreign`) }
              : {}),
            ...(nonPrimary
              ? {
                  id: fixture.ids.derive.providerTurn({
                    driver: fixture.driver,
                    nativeTurnId: backgroundNativeTurnId,
                  }),
                  nativeTurnRef: {
                    driver: fixture.driver,
                    nativeId: backgroundNativeTurnId,
                    strength: "strong" as const,
                  },
                  ordinal:
                    Math.max(
                      ...projection.providerTurns
                        .filter((item) => item.providerThreadId === turn.providerThreadId)
                        .map((item) => item.ordinal),
                    ) + 1,
                  runAttemptId: null,
                }
              : {}),
          },
        },
        nonPrimary ? { producer: fixture.origin.producer } : fixture.origin,
      );
      if (nonPrimary) assert.isUndefined(readProviderEventOrigin(event)?.turn);
      const outcome = yield* fixture.ingestor
        .ingestNormalized({
          ...fixture.input,
          event,
          revalidateCurrentOwner: fixture.revalidateCurrentOwner,
          ...(scenario === "non_primary"
            ? {}
            : {
                writeIfRunCurrent: {
                  runId: fixture.run.id,
                  activeAttemptId: fixture.attempt.id,
                  expectedStatus: "running" as const,
                },
              }),
        })
        .pipe(Effect.result);
      const after = yield* fixture.projections.getThreadProjection(fixture.threadId);
      if (scenario === "current") {
        assert.equal(outcome._tag, "Success");
        if (outcome._tag === "Success")
          assert.deepEqual(
            outcome.success.map((stored) => stored.event.type),
            ["provider-turn.updated", "run-attempt.updated", "node.updated"],
          );
        assert.deepEqual(
          after.attempts.find((item) => item.id === fixture.attempt.id),
          fixture.attempt,
        );
        assert.deepEqual(
          after.nodes.find((item) => item.id === fixture.node.id),
          fixture.node,
        );
        assert.equal(after.runs.find((item) => item.id === fixture.run.id)?.status, "running");
      } else if (scenario === "non_primary" || scenario === "guarded_non_primary") {
        assert.equal(outcome._tag, "Success");
        if (outcome._tag === "Success")
          assert.deepEqual(
            outcome.success.map((stored) => stored.event.type),
            ["provider-turn.updated"],
          );
        assert.deepEqual(after.attempts, before.attempts);
        assert.deepEqual(after.nodes, before.nodes);
      } else {
        if (scenario === "replaced_runtime") assert.equal(outcome._tag, "Failure");
        else {
          assert.equal(outcome._tag, "Success");
          if (outcome._tag === "Success") assert.isEmpty(outcome.success);
        }
        assert.deepEqual(after, before);
      }
    }).pipe(Effect.provide(TestLayer)),
  );
}

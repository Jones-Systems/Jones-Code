import { assert, it } from "@effect/vitest";
import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem";
import {
  CommandId,
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
  TurnItemId,
  type ModelSelection,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2ServerCommand,
  type OrchestrationV2Subagent,
  type OrchestrationV2TurnItem,
  type ServerProvider,
} from "@t3tools/contracts";
import { latestExecutedRun } from "@t3tools/shared/orchestrationV2ThreadError";
import { makeSubagentChildThread } from "@t3tools/provider-core/server/subagentProjection";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as Sqlite from "../../persistence/Sqlite.ts";
import * as Sink from "../../orchestration-v2/EventSink.ts";
import * as Events from "../../orchestration-v2/EventStore.ts";
import * as Projections from "../../orchestration-v2/ProjectionStore.ts";
import * as Receipts from "../../orchestration-v2/CommandReceiptStore.ts";
import * as Locks from "../../orchestration-v2/ThreadCommandExecutor.ts";
import * as Adapters from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as Switch from "../../orchestration-v2/ProviderSwitchService.ts";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import type {
  RecordedAppThread,
  RecordedLifecycleEvent,
  RecordedRun,
} from "../../orchestration-v2/RecordedTypes.ts";
import * as Registry from "../../provider/ProviderRegistry.ts";
import * as Settings from "../../serverSettings.ts";
import * as Evidence from "./CodexFailoverEvidence.ts";
import * as Store from "./CodexFailoverStore.ts";
import * as Failover from "./CodexFailover.ts";
import * as Lineage from "./CodexFailoverLineage.ts";

const threadId = ThreadId.make("thread:failover-service");
const originId = RunId.make("run:failed");
const attemptId = RunAttemptId.make("attempt:failed");
const providerTurnId = ProviderTurnId.make("turn:failed");
const providerThreadId = ProviderThreadId.make("native:source");
const source = ProviderInstanceId.make("codex-A");
const target = ProviderInstanceId.make("codex-B");
const third = ProviderInstanceId.make("codex-C");
const driver = ProviderDriverKind.make("codex");
const saved: ModelSelection = { instanceId: source, model: "saved-model" };
const captured: ModelSelection = {
  instanceId: source,
  model: "origin-model",
  options: [{ id: "effort", value: "high" }],
};
const stores = Layer.merge(Events.layer, Projections.layer).pipe(
  Layer.provideMerge(Sqlite.layerMemory),
);
const testLayer = Layer.mergeAll(
  stores,
  Sink.layer.pipe(Layer.provide(stores)),
  Receipts.layer.pipe(Layer.provide(stores)),
  Locks.layer,
  Settings.layerTest(),
  NodeFileSystem.layer,
);
const catalog: ReadonlyArray<ServerProvider> = [source, target].map((instanceId) => ({
  instanceId,
  driver,
  enabled: true,
  installed: true,
  version: "synthetic",
  status: "ready",
  auth: { status: "authenticated" },
  checkedAt: "2026-10-26T00:00:00Z",
  models: ["saved-model", "origin-model", "queued-model"].map((slug) => ({
    slug,
    name: slug,
    isCustom: true,
    capabilities: {
      optionDescriptors: [
        { id: "effort", label: "Effort", type: "select", options: [{ id: "high", label: "High" }] },
      ],
    },
  })),
  slashCommands: [],
  skills: [],
}));
const registry = Registry.ProviderRegistry.of({
  getProviders: Effect.succeed(catalog),
  refresh: () => Effect.succeed(catalog),
  refreshInstance: () => Effect.succeed(catalog),
  refreshAfterUpdate: () => Effect.succeed(catalog),
  refreshWorkspaceSnapshot: () => Effect.succeed(catalog),
  getProviderMaintenanceCapabilitiesForInstance: () => Effect.die("unused"),
  setProviderMaintenanceActionState: () => Effect.succeed(catalog),
  streamChanges: Stream.empty,
});
const adapters = Adapters.ProviderAdapterRegistryV2.of({
  get: (instanceId) => Effect.fail(new Adapters.ProviderAdapterRegistryLookupError({ instanceId })),
  list: () => Effect.succeed([source, target]),
  getMetadata: () =>
    Effect.succeed({
      driver,
      continuationKey: "shared-history",
      enabled: true,
      capabilities: CodexProviderCapabilitiesV2,
    }),
});
const transitions = Switch.ProviderSwitchServiceV2.of({
  plan: () =>
    Effect.succeed({
      instanceChanged: true,
      modelChanged: false,
      targetProviderThreadId: null,
      releaseProviderSessionIds: [],
      transition: { type: "restart_and_resume" },
    }),
});
const seed = Effect.gen(function* () {
  const now = yield* DateTime.now;
  const sink = yield* Sink.EventSinkV2;
  const thread: RecordedAppThread = {
    id: threadId,
    projectId: ProjectId.make("project:failover-service"),
    title: "Failover service fixture",
    modelSelection: saved,
    providerInstanceId: source,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: providerThreadId,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
    forkedFrom: null,
    createdBy: "user",
    creationSource: "web",
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    deletedAt: null,
    settledAt: null,
    settledOverride: null,
    lastVisitedAt: now,
  };
  const run: RecordedRun = {
    id: originId,
    threadId,
    ordinal: 1,
    providerInstanceId: source,
    modelSelection: captured,
    userMessageId: MessageId.make("message:origin"),
    providerThreadId,
    rootNodeId: NodeId.make("node:origin"),
    activeAttemptId: attemptId,
    delegatedTaskId: NodeId.make("task:delegated"),
    status: "failed",
    requestedAt: now,
    startedAt: now,
    completedAt: now,
    checkpointId: null,
    contextHandoffId: null,
  };
  const events: ReadonlyArray<RecordedLifecycleEvent> = [
    {
      id: EventId.make("seed:thread"),
      type: "thread.created",
      threadId,
      occurredAt: now,
      payload: thread,
    },
    {
      id: EventId.make("seed:run"),
      type: "run.created",
      threadId,
      runId: originId,
      occurredAt: now,
      payload: run,
    },
    {
      id: EventId.make("seed:attempt"),
      type: "run-attempt.created",
      threadId,
      runId: originId,
      occurredAt: now,
      payload: {
        id: attemptId,
        runId: originId,
        attemptOrdinal: 1,
        rootNodeId: run.rootNodeId!,
        providerInstanceId: source,
        providerThreadId,
        providerTurnId,
        providerSettlement: {
          runAttemptId: attemptId,
          providerTurnId,
          status: "failed",
          completedAt: now,
        },
        reason: "initial",
        status: "failed",
        startedAt: now,
        completedAt: now,
      },
    },
    {
      id: EventId.make("seed:provider"),
      type: "provider-thread.updated",
      threadId,
      occurredAt: now,
      payload: {
        id: providerThreadId,
        driver,
        providerInstanceId: source,
        providerSessionId: null,
        appThreadId: threadId,
        ownerNodeId: null,
        nativeThreadRef: { driver, nativeId: "owned-native-history", strength: "strong" },
        nativeConversationHeadRef: null,
        status: "idle",
        firstRunOrdinal: 1,
        lastRunOrdinal: 1,
        handoffIds: [],
        forkedFrom: null,
        pendingBackgroundTasks: [],
        createdAt: now,
        updatedAt: now,
      },
    },
  ];
  yield* sink.write({ events });
  const recorder = yield* Evidence.make;
  yield* recorder.record({
    providerTurnId,
    threadId,
    runId: originId,
    attemptId,
    providerThreadId,
    providerInstanceId: source,
    status: "failed",
    evidenceKind: "provider_result",
    rawErrorInfo: "usageLimitExceeded",
    occurredAt: DateTime.formatIso(now),
    capacityExhausted: false,
  });
  return { thread, run, now, sink, store: yield* Store.make };
});
const makeService = Effect.fn("CodexFailover.test.makeService")(function* (
  options: {
    readonly noRegistry?: boolean;
    readonly cascade?: boolean;
    readonly refuseContinuation?: boolean;
    readonly refuseSwitch?: boolean;
    readonly refuseAfterAccept?: boolean;
  } = {},
) {
  const fixture = yield* seed;
  const receipts = yield* Receipts.CommandReceiptStoreV2;
  const projections = yield* Projections.ProjectionStoreV2;
  const dispatched: OrchestrationV2ServerCommand[] = [];
  const queues: RunId[] = [];
  let contextFor: (
    command: Extract<OrchestrationV2ServerCommand, { type: "message.dispatch" }>,
  ) => Failover.CodexFailoverCommitContext | undefined = () => undefined;
  let sourceFor: (command: {
    commandId: CommandId;
    threadId: ThreadId;
  }) => Effect.Effect<RunId | undefined, Failover.CodexFailoverError> = () =>
    Effect.sync(() => undefined);
  const wrappedSink: Sink.EventSinkV2["Service"] = {
    ...fixture.sink,
    commitCommand: (envelope) => {
      if (options.refuseSwitch && envelope.commandType === "jones.codex-failover.switch")
        return fixture.sink.commitCommand({
          ...envelope,
          codexFailover: {
            revalidate: (value) =>
              Effect.gen(function* () {
                yield* envelope.codexFailover!.revalidate!(value);
                return yield* new Failover.CodexFailoverError({ reason: "switch_refused" });
              }),
          },
        });
      return fixture.sink.commitCommand(envelope);
    },
  };
  const program = Failover.make({
    dispatch: (command) =>
      Effect.gen(function* () {
        dispatched.push(command);
        if (command.type !== "message.dispatch") return;
        if (options.refuseContinuation) return yield* Effect.fail("synthetic refusal");
        const inheritedFrom = yield* sourceFor(command);
        const current = yield* projections.getThreadRecords(threadId, ["runs", "providerThreads"]);
        const origin = current.runs.find((run) => run.id === inheritedFrom) ?? fixture.run;
        const continuedProviderThreadId = options.cascade
          ? ProviderThreadId.make(`native:${command.modelSelection!.instanceId}`)
          : origin.providerThreadId;
        const continued: RecordedRun = {
          ...origin,
          id: RunId.make(
            dispatched.length === 1 ? "run:continued" : `run:continued:${dispatched.length}`,
          ),
          ordinal: 10 + dispatched.length,
          providerThreadId: continuedProviderThreadId,
          providerInstanceId: command.modelSelection!.instanceId,
          modelSelection: command.modelSelection!,
          userMessageId: command.messageId,
          delegatedTaskId: inheritedFrom === origin.id ? origin.delegatedTaskId : undefined,
          status: "running",
          completedAt: null,
        };
        const commit = contextFor(command)!;
        yield* fixture.sink.commitCommand({
          commandId: command.commandId,
          threadId,
          commandType: command.type,
          acceptedAt: fixture.now,
          effects: [],
          events: [
            {
              id: EventId.make(
                dispatched.length === 1 ? "event:continue" : `event:continue:${dispatched.length}`,
              ),
              type: "run.created",
              threadId,
              runId: continued.id,
              occurredAt: fixture.now,
              payload: continued,
            },
            ...(options.cascade
              ? [
                  {
                    id: EventId.make(`event:provider:${dispatched.length}`),
                    type: "provider-thread.updated" as const,
                    threadId,
                    occurredAt: fixture.now,
                    payload: {
                      ...current.providerThreads[0]!,
                      id: continuedProviderThreadId!,
                      providerInstanceId: command.modelSelection!.instanceId,
                      firstRunOrdinal: continued.ordinal,
                      lastRunOrdinal: continued.ordinal,
                    },
                  },
                ]
              : []),
          ],
          codexFailover: options.refuseAfterAccept
            ? {
                ...commit,
                accept: (value) =>
                  Effect.gen(function* () {
                    yield* commit.accept!(value);
                    return yield* new Failover.CodexFailoverError({
                      reason: "continuation_refused",
                    });
                  }),
              }
            : commit,
        });
      }),
    queueAfterFailure: (_threadId, runId) =>
      Effect.sync(() => {
        queues.push(runId);
      }),
  }).pipe(
    Effect.provideService(Sink.EventSinkV2, wrappedSink),
    Effect.provideService(Adapters.ProviderAdapterRegistryV2, {
      ...adapters,
      list: () => Effect.succeed(options.cascade ? [source, target, third] : [source, target]),
    }),
    Effect.provideService(Switch.ProviderSwitchServiceV2, transitions),
  );
  const service = yield* options.noRegistry
    ? program
    : program.pipe(
        Effect.provideService(Registry.ProviderRegistry, {
          ...registry,
          getProviders: Effect.succeed(
            options.cascade ? [...catalog, { ...catalog[1]!, instanceId: third }] : catalog,
          ),
        }),
      );
  contextFor = service.commitContext;
  sourceFor = service.continuationSourceRunId;
  return { ...fixture, service, dispatched, queues, receipts, projections };
});
const queuedCommand = (id: string, selection?: ModelSelection, explicit?: boolean) => ({
  type: "message.dispatch" as const,
  threadId,
  commandId: CommandId.make(`queued:${id}`),
  messageId: MessageId.make(`message:${id}`),
  text: "Synthetic follower",
  attachments: [],
  createdBy: "user" as const,
  creationSource: "web" as const,
  dispatchMode: { type: "queue_after_active" as const },
  ...(selection === undefined ? {} : { modelSelection: selection }),
  ...(explicit === undefined ? {} : { modelSelectionExplicit: explicit }),
});
const queueRun = Effect.fn("CodexFailover.test.queueRun")(function* (
  ctx: Effect.Success<ReturnType<typeof makeService>>,
  id: string,
  selection?: ModelSelection,
  explicit?: boolean,
) {
  const command = queuedCommand(id, selection, explicit);
  const existing = yield* ctx.projections.getThreadRecords(threadId, ["runs"]);
  const ordinal = Math.max(...existing.runs.map((run) => run.ordinal)) + 1;
  const run: RecordedRun = {
    ...ctx.run,
    id: RunId.make(`run:${id}`),
    ordinal,
    userMessageId: command.messageId,
    modelSelection: selection ?? { ...captured, model: "queued-model" },
    providerInstanceId: selection?.instanceId ?? source,
    status: "queued",
    startedAt: null,
    completedAt: null,
  };
  yield* ctx.sink.commitCommand({
    commandId: command.commandId,
    threadId,
    commandType: command.type,
    acceptedAt: ctx.now,
    effects: [],
    events: [
      {
        id: EventId.make(`event:${id}`),
        type: "run.created",
        threadId,
        runId: run.id,
        occurredAt: ctx.now,
        payload: run,
      },
    ],
    codexFailover: ctx.service.commitContext(command),
  });
  return run;
});

type ServiceFixture = Effect.Success<ReturnType<typeof makeService>>;
type NativeChild = {
  readonly thread: RecordedAppThread;
  readonly provider: OrchestrationV2ProviderThread;
  readonly turn: OrchestrationV2ProviderTurn;
  readonly task: OrchestrationV2Subagent;
  readonly item: Extract<OrchestrationV2TurnItem, { type: "subagent" }>;
};
const nativeChild = Effect.fn("CodexFailover.test.nativeChild")(function* (
  ctx: ServiceFixture,
  id: string,
  parent?: NativeChild,
) {
  const parentThread = parent?.thread ?? ctx.thread;
  const parentProvider =
    parent?.provider ??
    (yield* ctx.projections.getThreadRecords(threadId, ["providerThreads"])).providerThreads[0]!;
  const childThreadId = ThreadId.make(`thread:${id}`);
  const nodeId = NodeId.make(`node:${id}`);
  const provider: OrchestrationV2ProviderThread = {
    ...parentProvider,
    id: ProviderThreadId.make(`provider:${id}`),
    appThreadId: childThreadId,
    nativeThreadRef: { driver, nativeId: id, strength: "strong" },
    forkedFrom: {
      providerThreadId: parentProvider.id,
      providerTurnId: parent?.turn.id ?? providerTurnId,
    },
    firstRunOrdinal: null,
    lastRunOrdinal: null,
    pendingBackgroundTasks: [],
  };
  // Use the same child-thread materializer and links as CodexAdapterV2.
  const thread = makeSubagentChildThread({
    parentThread,
    childThreadId,
    parentNodeId: nodeId,
    activeProviderThreadId: provider.id,
    providerInstanceId: source,
    modelSelection: captured,
    title: id,
    now: ctx.now,
    createdBy: "agent",
    creationSource: "provider",
  });
  const task: OrchestrationV2Subagent = {
    id: nodeId,
    threadId: parentThread.id,
    runId: parent === undefined ? originId : null,
    parentNodeId: parent?.turn.nodeId ?? ctx.run.rootNodeId!,
    origin: "provider_native",
    createdBy: "agent",
    driver,
    providerInstanceId: source,
    providerThreadId: provider.id,
    childThreadId,
    nativeTaskRef: { driver, nativeId: `spawn:${id}`, strength: "strong" },
    prompt: id,
    title: id,
    model: captured.model,
    status: "running",
    result: null,
    startedAt: ctx.now,
    completedAt: null,
    updatedAt: ctx.now,
  };
  const turn: OrchestrationV2ProviderTurn = {
    id: ProviderTurnId.make(`turn:${id}`),
    providerThreadId: provider.id,
    nodeId: NodeId.make(`root:${id}`),
    runAttemptId: null,
    nativeTurnRef: { driver, nativeId: `turn:${id}`, strength: "strong" },
    ordinal: 1,
    status: "running",
    startedAt: ctx.now,
    completedAt: null,
  };
  const item: NativeChild["item"] = {
    id: TurnItemId.make(`item:${id}`),
    threadId: task.threadId,
    runId: task.runId,
    nodeId,
    providerThreadId: provider.id,
    providerTurnId: parent?.turn.id ?? providerTurnId,
    nativeItemRef: task.nativeTaskRef,
    parentItemId: null,
    ordinal: 1,
    type: "subagent",
    subagentId: task.id,
    origin: task.origin,
    driver,
    providerInstanceId: source,
    childThreadId,
    prompt: task.prompt,
    result: null,
    status: "running",
    title: id,
    startedAt: ctx.now,
    completedAt: null,
    updatedAt: ctx.now,
  };
  yield* ctx.sink.write({
    events: [
      {
        id: EventId.make(`${id}:thread`),
        type: "thread.created",
        threadId: childThreadId,
        occurredAt: ctx.now,
        payload: thread,
      },
      {
        id: EventId.make(`${id}:provider`),
        type: "provider-thread.updated",
        threadId: childThreadId,
        occurredAt: ctx.now,
        payload: provider,
      },
      {
        id: EventId.make(`${id}:turn`),
        type: "provider-turn.updated",
        threadId: childThreadId,
        occurredAt: ctx.now,
        payload: turn,
      },
      {
        id: EventId.make(`${id}:task`),
        type: "subagent.updated",
        threadId: task.threadId,
        occurredAt: ctx.now,
        payload: task,
      },
      {
        id: EventId.make(`${id}:item`),
        type: "turn-item.updated",
        threadId: item.threadId,
        occurredAt: ctx.now,
        payload: item,
      },
    ],
  });
  return { thread, provider, turn, task, item } satisfies NativeChild;
});
const settleNativeTask = Effect.fn("CodexFailover.test.settleNativeTask")(function* (
  ctx: ServiceFixture,
  child: NativeChild,
) {
  // The adapter commits the terminal task before its terminal subagent turn item.
  yield* ctx.sink.write({
    events: [
      {
        id: EventId.make(`${child.task.id}:turn:done`),
        type: "provider-turn.updated",
        threadId: child.thread.id,
        occurredAt: ctx.now,
        payload: { ...child.turn, status: "completed", completedAt: ctx.now },
      },
      {
        id: EventId.make(`${child.task.id}:task:done`),
        type: "subagent.updated",
        threadId: child.task.threadId,
        occurredAt: ctx.now,
        payload: { ...child.task, status: "completed", completedAt: ctx.now },
      },
    ],
  });
  yield* ctx.service.afterChildWorkSettled(child.task.threadId);
});
const settleItem = Effect.fn("CodexFailover.test.settleItem")(function* (
  ctx: ServiceFixture,
  item: OrchestrationV2TurnItem,
  suffix = "done",
) {
  const stored = yield* ctx.sink.write({
    events: [
      {
        id: EventId.make(`${item.id}:${suffix}`),
        type: "turn-item.updated",
        threadId: item.threadId,
        occurredAt: ctx.now,
        payload: { ...item, status: "completed", completedAt: ctx.now },
      },
    ],
  });
  for (const event of stored) {
    assert.isTrue(Failover.settlesTurnItem(event));
    yield* ctx.service.afterChildWorkSettled(event.event.threadId);
  }
});

it.effect("prefix gating and explicit queued intent avoid failover lookups and markers", () =>
  Effect.gen(function* () {
    const ctx = yield* makeService();
    const ordinary = queuedCommand("ordinary", captured, true);
    assert.isUndefined(ctx.service.commitContext(ordinary));
    assert.isUndefined(yield* ctx.service.continuationSourceRunId(ordinary));
    assert.isUndefined(ctx.service.commitContext({ ...ordinary, type: "thread.metadata.update" }));
    const inherited = yield* queueRun(ctx, "inherited", undefined);
    const explicit = yield* queueRun(ctx, "explicit", captured, true);
    const marked = yield* queueRun(ctx, "marked", captured, false);
    const claude = yield* queueRun(
      ctx,
      "claude",
      { instanceId: ProviderInstanceId.make("claude"), model: "claude" },
      false,
    );
    assert.deepEqual(
      (yield* ctx.store.listInheritedQueuedRuns(threadId)).map((row) => row.runId),
      [inherited.id, marked.id],
    );
    assert.isNull(yield* ctx.service.queuedSelection(threadId, explicit));
    assert.isNull(yield* ctx.service.queuedSelection(threadId, claude));
  }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "continuation commits follower overrides and delegated lineage while keeping saved and captured models separate",
  () =>
    Effect.gen(function* () {
      const ctx = yield* makeService();
      const follower = yield* queueRun(ctx, "follower", undefined);
      const fixed = yield* queueRun(ctx, "fixed", captured, true);
      assert.isTrue(yield* ctx.service.holdsChildResult(threadId, ctx.run));
      yield* ctx.service.afterFailedRun(threadId, originId);
      const step = yield* ctx.store.getStepByOrigin(originId);
      assert.equal(step?.status, "continued");
      assert.deepEqual(step?.savedSelection, { ...saved, instanceId: target });
      assert.deepEqual(step?.continuationSelection, { ...captured, instanceId: target });
      assert.deepEqual(yield* ctx.service.queuedSelection(threadId, follower), {
        ...follower.modelSelection,
        instanceId: target,
      });
      assert.isNull(yield* ctx.service.queuedSelection(threadId, fixed));
      const records = yield* ctx.projections.getThreadRecords(threadId, ["runs"]);
      assert.deepEqual(records.thread.modelSelection, step?.savedSelection);
      assert.equal(
        records.runs.find((row) => row.id === "run:continued")?.delegatedTaskId,
        ctx.run.delegatedTaskId,
      );
      assert.isFalse(yield* ctx.service.holdsChildResult(threadId, ctx.run));
      assert.lengthOf(ctx.dispatched, 1);
      assert.deepEqual(yield* ctx.store.listOpenEvidence(), []);
    }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "queued inherited work follows accepted A → B → C continuations with distinct provider threads",
  () =>
    Effect.gen(function* () {
      const ctx = yield* makeService({ cascade: true });
      const follower = yield* queueRun(ctx, "cascade-follower");
      const fixed = yield* queueRun(ctx, "cascade-explicit", captured, true);
      const markerless = yield* queueRun(ctx, "cascade-markerless");
      yield* ctx.store.deleteQueuedRuns([markerless.id]);
      yield* ctx.service.afterFailedRun(threadId, originId);
      const firstStep = (yield* ctx.store.getStepByOrigin(originId))!;
      assert.equal(firstStep.status, "continued");
      assert.equal(
        Option.getOrThrow(yield* ctx.receipts.getByCommandId(firstStep.continueCommandId)).status,
        "accepted",
      );
      assert.equal((yield* ctx.service.queuedSelection(threadId, follower))?.instanceId, target);
      const records = yield* ctx.projections.getThreadRecords(threadId, ["runs", "attempts"]);
      const continued = records.runs.find(
        (run) => run.userMessageId === firstStep.continueMessageId,
      )!;
      assert.notEqual(continued.providerThreadId, follower.providerThreadId);
      const nextAttemptId = RunAttemptId.make("attempt:cascade-B");
      const nextTurnId = ProviderTurnId.make("turn:cascade-B");
      const failed = {
        ...continued,
        activeAttemptId: nextAttemptId,
        status: "failed" as const,
        completedAt: ctx.now,
      };
      yield* ctx.sink.write({
        events: [
          {
            id: EventId.make("cascade:failure"),
            type: "run.updated",
            threadId,
            runId: failed.id,
            occurredAt: ctx.now,
            payload: failed,
          },
          {
            id: EventId.make("cascade:attempt"),
            type: "run-attempt.created",
            threadId,
            runId: failed.id,
            occurredAt: ctx.now,
            payload: {
              ...records.attempts[0]!,
              id: nextAttemptId,
              runId: failed.id,
              rootNodeId: failed.rootNodeId!,
              providerInstanceId: target,
              providerThreadId: failed.providerThreadId!,
              providerTurnId: nextTurnId,
              providerSettlement: {
                runAttemptId: nextAttemptId,
                providerTurnId: nextTurnId,
                status: "failed",
                completedAt: ctx.now,
              },
            },
          },
        ],
      });
      const recorder = yield* Evidence.make;
      yield* recorder.record({
        providerTurnId: nextTurnId,
        threadId,
        runId: failed.id,
        attemptId: nextAttemptId,
        providerThreadId: failed.providerThreadId!,
        providerInstanceId: target,
        status: "failed",
        evidenceKind: "provider_result",
        rawErrorInfo: "usageLimitExceeded",
        occurredAt: DateTime.formatIso(ctx.now),
        capacityExhausted: false,
      });
      yield* ctx.service.afterFailedRun(threadId, failed.id);
      const secondStep = (yield* ctx.store.getStepByOrigin(failed.id))!;
      assert.equal(secondStep.status, "continued");
      const receipt = yield* ctx.receipts.getByCommandId(secondStep.continueCommandId);
      assert.isTrue(Option.isSome(receipt));
      assert.equal(Option.getOrThrow(receipt).status, "accepted");
      assert.deepEqual(yield* ctx.service.queuedSelection(threadId, follower), {
        ...follower.modelSelection,
        instanceId: third,
      });
      assert.isNull(yield* ctx.service.queuedSelection(threadId, fixed));
      assert.isNull(yield* ctx.service.queuedSelection(threadId, markerless));
      const final = yield* ctx.projections.getThreadRecords(threadId, ["runs"]);
      const last = final.runs.find((run) => run.userMessageId === secondStep.continueMessageId)!;
      assert.notEqual(last.providerThreadId, continued.providerThreadId);
      assert.notEqual(last.providerThreadId, follower.providerThreadId);
      assert.equal(last.delegatedTaskId, ctx.run.delegatedTaskId);
      assert.deepEqual(final.thread.modelSelection, { ...saved, instanceId: third });
      assert.equal(
        (yield* ctx.store.listInheritedQueuedRuns(threadId)).find(
          (row) => row.runId === follower.id,
        )?.overrideSwitchCommandId,
        secondStep.switchCommandId,
      );
      assert.lengthOf(ctx.dispatched, 2);
    }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "a stale completed task hint cannot hide a newer deferred failure with a rounded native clock",
  () =>
    Effect.gen(function* () {
      const ctx = yield* makeService();
      const earlier: RecordedRun = {
        ...ctx.run,
        id: RunId.make("run:earlier-task"),
        ordinal: 1,
        delegatedTaskId: NodeId.make("task:earlier"),
        status: "completed",
        completedAt: DateTime.add(ctx.now, { milliseconds: 500 }),
      };
      const failed: RecordedRun = {
        ...ctx.run,
        ordinal: 2,
        requestedAt: DateTime.add(ctx.now, { milliseconds: 600 }),
        startedAt: DateTime.add(ctx.now, { milliseconds: 600 }),
        completedAt: ctx.now,
      };
      yield* ctx.sink.write({
        events: [
          {
            id: EventId.make("rounded-failure"),
            type: "run.updated",
            threadId,
            runId: failed.id,
            occurredAt: ctx.now,
            payload: failed,
          },
          {
            id: EventId.make("earlier-task"),
            type: "run.created",
            threadId,
            runId: earlier.id,
            occurredAt: ctx.now,
            payload: earlier,
          },
        ],
      });
      assert.equal(latestExecutedRun([earlier, failed])?.id, earlier.id);
      assert.notEqual(earlier.delegatedTaskId, failed.delegatedTaskId);
      yield* nativeChild(ctx, "deferred-task-native-work");
      yield* ctx.service.afterFailedRun(threadId, failed.id);
      assert.equal((yield* ctx.store.getEvidence(providerTurnId))?.state, "deferred");
      assert.isTrue(yield* ctx.service.holdsChildResult(threadId, earlier));
      assert.isTrue(yield* ctx.service.holdsChildResult(threadId, failed));
      assert.isFalse(
        yield* ctx.service.holdsChildResult(threadId, earlier, earlier.delegatedTaskId),
      );
      assert.isTrue(yield* ctx.service.holdsChildResult(threadId, earlier, failed.delegatedTaskId));
      assert.isFalse(
        yield* ctx.service.holdsChildResult(threadId, earlier, NodeId.make("task:unrelated")),
      );
      assert.isFalse(yield* ctx.service.holdsChildResult(ThreadId.make("other-child"), earlier));
      assert.lengthOf(ctx.dispatched, 0);
      yield* ctx.store.markEvidence(providerTurnId, {
        state: "decided",
        decision: "exhausted",
        updatedAt: DateTime.formatIso(ctx.now),
      });
      assert.isFalse(yield* ctx.service.holdsChildResult(threadId, earlier));
    }).pipe(Effect.provide(testLayer)),
);

it.effect.each(["running", "completed"] as const)(
  "a newer %s delegated task does not inherit an older task's failover hold",
  (status) =>
    Effect.gen(function* () {
      const ctx = yield* makeService();
      const newer: RecordedRun = {
        ...ctx.run,
        id: RunId.make("run:newer-task"),
        ordinal: 2,
        delegatedTaskId: NodeId.make("task:newer"),
        status,
        requestedAt: DateTime.add(ctx.now, { seconds: 1 }),
        startedAt: DateTime.add(ctx.now, { seconds: 1 }),
        completedAt: status === "completed" ? DateTime.add(ctx.now, { seconds: 2 }) : null,
      };
      yield* ctx.sink.write({
        events: [
          {
            id: EventId.make("newer-task"),
            type: "run.created",
            threadId,
            runId: newer.id,
            occurredAt: ctx.now,
            payload: newer,
          },
        ],
      });
      assert.isFalse(yield* ctx.service.holdsChildResult(threadId, ctx.run));
      assert.isFalse(yield* ctx.service.holdsChildResult(threadId, newer));
      assert.isTrue(
        yield* ctx.service.holdsChildResult(threadId, ctx.run, ctx.run.delegatedTaskId),
      );
      assert.isFalse(yield* ctx.service.holdsChildResult(threadId, newer, newer.delegatedTaskId));
    }).pipe(Effect.provide(testLayer)),
);

it.effect("refused switch revalidation rolls back its receipt, step and switched decision", () =>
  Effect.gen(function* () {
    const ctx = yield* makeService({ refuseSwitch: true });
    yield* ctx.service.afterFailedRun(threadId, originId);
    assert.isNull(yield* ctx.store.getStepByOrigin(originId));
    assert.isTrue(
      Option.isNone(
        yield* ctx.receipts.getByCommandId(
          CommandId.make(`jones:codex-failover:${originId}:switch`),
        ),
      ),
    );
    assert.equal((yield* ctx.store.getEvidence(providerTurnId))?.decision, "admission_error");
    assert.deepEqual(
      (yield* ctx.projections.getThreadRecords(threadId, [])).thread.modelSelection,
      saved,
    );
    assert.lengthOf(ctx.dispatched, 0);
  }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "refusal after continuation accept rolls back every override and continued status, then abandons",
  () =>
    Effect.gen(function* () {
      const ctx = yield* makeService({ refuseAfterAccept: true });
      const follower = yield* queueRun(ctx, "follower");
      yield* ctx.service.afterFailedRun(threadId, originId);
      const step = yield* ctx.store.getStepByOrigin(originId);
      assert.equal(step?.status, "abandoned");
      assert.isNull(yield* ctx.store.getQueuedOverride(follower.id));
      assert.isTrue(Option.isNone(yield* ctx.receipts.getByCommandId(step!.continueCommandId)));
      assert.deepEqual(
        (yield* ctx.projections.getThreadRecords(threadId, [])).thread.modelSelection,
        { ...saved, instanceId: target },
      );
      assert.isFalse(yield* ctx.service.holdsChildResult(threadId, ctx.run));
    }).pipe(Effect.provide(testLayer)),
);

it.effect("a newer selection revision refuses continuation and leaves no run or override", () =>
  Effect.gen(function* () {
    const ctx = yield* makeService({ refuseContinuation: true });
    const follower = yield* queueRun(ctx, "follower");
    yield* ctx.service.afterFailedRun(threadId, originId);
    const step = (yield* ctx.store.getStepByOrigin(originId))!;
    yield* ctx.store.setStepStatus(step.switchCommandId, "switched", DateTime.formatIso(ctx.now));
    const thread = (yield* ctx.projections.getThreadRecords(threadId, [])).thread;
    yield* ctx.sink.write({
      events: [
        {
          id: EventId.make("later-selection"),
          type: "thread.model-selection-updated",
          threadId,
          occurredAt: ctx.now,
          payload: thread,
        },
      ],
    });
    const command = {
      ...queuedCommand("refused", step.continuationSelection, true),
      commandId: step.continueCommandId,
      messageId: step.continueMessageId,
      dispatchMode: { type: "start_immediately" as const },
      creationSource: "server" as const,
    };
    const result = yield* Effect.exit(
      ctx.sink.commitCommand({
        commandId: command.commandId,
        threadId,
        commandType: command.type,
        acceptedAt: ctx.now,
        effects: [],
        events: [
          {
            id: EventId.make("refused:event"),
            type: "run.created",
            threadId,
            runId: RunId.make("refused:run"),
            occurredAt: ctx.now,
            payload: { ...ctx.run, id: RunId.make("refused:run"), ordinal: 10, status: "running" },
          },
        ],
        codexFailover: ctx.service.commitContext(command),
      }),
    );
    assert.isTrue(Exit.isFailure(result));
    assert.isNull(yield* ctx.store.getQueuedOverride(follower.id));
    assert.equal((yield* ctx.store.getStepByOrigin(originId))?.status, "switched");
    assert.isTrue(Option.isNone(yield* ctx.receipts.getByCommandId(command.commandId)));
  }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "an unrelated settle trigger leaves newly recorded evidence for the failed-run reactor",
  () =>
    Effect.gen(function* () {
      const ctx = yield* makeService();
      yield* ctx.service.afterChildWorkSettled(threadId);
      assert.equal((yield* ctx.store.getEvidence(providerTurnId))?.state, "recorded");
      assert.lengthOf(ctx.dispatched, 0);
      yield* ctx.service.afterFailedRun(threadId, originId);
      assert.equal((yield* ctx.store.getStepByOrigin(originId))?.status, "continued");
    }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "deferred native background work holds child results until a settle trigger re-evaluates it",
  () =>
    Effect.gen(function* () {
      const ctx = yield* makeService();
      const provider = (yield* ctx.projections.getThreadRecords(threadId, ["providerThreads"]))
        .providerThreads[0]!;
      yield* ctx.sink.write({
        events: [
          {
            id: EventId.make("background:active"),
            type: "provider-thread.updated",
            threadId,
            occurredAt: ctx.now,
            payload: {
              ...provider,
              pendingBackgroundTasks: [{ taskId: "native-child", kind: "subagent" }],
            },
          },
        ],
      });
      yield* ctx.service.afterFailedRun(threadId, originId);
      assert.equal((yield* ctx.store.getEvidence(providerTurnId))?.state, "deferred");
      assert.isTrue(yield* ctx.service.holdsChildResult(threadId, ctx.run));
      assert.lengthOf(ctx.dispatched, 0);
      yield* ctx.sink.write({
        events: [
          {
            id: EventId.make("background:settled"),
            type: "provider-thread.updated",
            threadId,
            occurredAt: ctx.now,
            payload: { ...provider, pendingBackgroundTasks: [] },
          },
        ],
      });
      yield* ctx.service.afterChildWorkSettled(threadId);
      assert.equal((yield* ctx.store.getStepByOrigin(originId))?.status, "continued");
      assert.deepEqual(ctx.queues, [originId]);
    }).pipe(Effect.provide(testLayer)),
);

it.effect("startup catches missed evidence and cleans finished queue markers", () =>
  Effect.gen(function* () {
    const ctx = yield* makeService();
    yield* ctx.store.insertInheritedQueuedRun({
      runId: originId,
      threadId,
      inheritedAt: DateTime.formatIso(ctx.now),
    });
    yield* ctx.service.recoverCodexFailover;
    assert.equal((yield* ctx.store.getStepByOrigin(originId))?.status, "continued");
    assert.deepEqual(yield* ctx.store.listInheritedQueuedRuns(), []);
    assert.deepEqual(ctx.queues, [originId]);
  }).pipe(Effect.provide(testLayer)),
);

it.effect("the final native subagent turn item wakes deferred failover exactly once", () =>
  Effect.gen(function* () {
    const ctx = yield* makeService();
    const child = yield* nativeChild(ctx, "child");
    yield* ctx.service.afterFailedRun(threadId, originId);
    assert.equal((yield* ctx.store.getEvidence(providerTurnId))?.state, "deferred");
    yield* settleNativeTask(ctx, child);
    assert.equal((yield* ctx.store.getEvidence(providerTurnId))?.state, "deferred");
    assert.lengthOf(ctx.dispatched, 0);
    yield* settleItem(ctx, child.item);
    assert.equal((yield* ctx.store.getStepByOrigin(originId))?.status, "continued");
    yield* settleItem(ctx, child.item, "duplicate");
    assert.lengthOf(ctx.dispatched, 1);
    assert.deepEqual(ctx.queues, [originId, originId]);
  }).pipe(Effect.provide(testLayer)),
);

const deferredGrandchild = Effect.fn("CodexFailover.test.deferredGrandchild")(function* (
  ctx: ServiceFixture,
) {
  const child = yield* nativeChild(ctx, "child");
  const grandchild = yield* nativeChild(ctx, "grandchild", child);
  const work: Extract<OrchestrationV2TurnItem, { type: "command_execution" }> = {
    ...grandchild.item,
    id: TurnItemId.make("grandchild:command"),
    threadId: grandchild.thread.id,
    runId: null,
    nodeId: grandchild.turn.nodeId,
    providerThreadId: grandchild.provider.id,
    providerTurnId: grandchild.turn.id,
    type: "command_execution",
    input: "synthetic-work",
  };
  yield* ctx.sink.write({
    events: [
      {
        id: EventId.make("grandchild:command:active"),
        type: "turn-item.updated",
        threadId: grandchild.thread.id,
        occurredAt: ctx.now,
        payload: work,
      },
    ],
  });
  yield* settleNativeTask(ctx, child);
  yield* settleItem(ctx, child.item);
  yield* ctx.service.afterFailedRun(threadId, originId);
  assert.equal((yield* ctx.store.getEvidence(providerTurnId))?.state, "deferred");
  assert.lengthOf(ctx.dispatched, 0);
  return { child, grandchild, work };
});

it.effect(
  "completed native parents retain ownership and a grandchild's final item wakes all ancestors",
  () =>
    Effect.gen(function* () {
      const ctx = yield* makeService();
      const { child, grandchild, work } = yield* deferredGrandchild(ctx);
      // An independently owned app task under the native child does not share its adapter.
      const unrelated = yield* nativeChild(ctx, "app-owned", child);
      yield* ctx.sink.write({
        events: [
          {
            id: EventId.make("unrelated:task"),
            type: "subagent.updated",
            threadId: child.thread.id,
            occurredAt: ctx.now,
            payload: { ...unrelated.task, origin: "app_owned" },
          },
          {
            id: EventId.make("unrelated:item"),
            type: "turn-item.updated",
            threadId: child.thread.id,
            occurredAt: ctx.now,
            payload: { ...unrelated.item, origin: "app_owned" },
          },
        ],
      });
      const lineage = yield* Lineage.make();
      assert.deepEqual(yield* lineage.settleThreadIds(unrelated.thread.id), [unrelated.thread.id]);
      yield* settleNativeTask(ctx, grandchild);
      yield* settleItem(ctx, grandchild.item);
      assert.equal((yield* ctx.store.getEvidence(providerTurnId))?.state, "deferred");
      assert.lengthOf(ctx.dispatched, 0);
      const before = ctx.queues.length;
      yield* settleItem(ctx, work);
      assert.equal((yield* ctx.store.getStepByOrigin(originId))?.status, "continued");
      assert.deepEqual(ctx.queues.slice(before), [originId]);
      yield* settleItem(ctx, work, "duplicate");
      assert.lengthOf(ctx.dispatched, 1);
    }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "Stop during a native grandchild defer prevents continuation on the final item wake",
  () =>
    Effect.gen(function* () {
      const ctx = yield* makeService();
      const { grandchild, work } = yield* deferredGrandchild(ctx);
      yield* ctx.sink.write({
        events: [
          {
            id: EventId.make("stop:during-defer"),
            type: "turn-item.updated",
            threadId,
            occurredAt: ctx.now,
            payload: {
              ...work,
              id: TurnItemId.make("stop:request"),
              threadId,
              runId: originId,
              providerThreadId,
              providerTurnId,
              type: "run_interrupt_request",
              status: "completed",
              completedAt: ctx.now,
              message: "Stop",
            },
          },
        ],
      });
      yield* settleNativeTask(ctx, grandchild);
      yield* settleItem(ctx, grandchild.item);
      yield* settleItem(ctx, work);
      yield* settleItem(ctx, work, "duplicate");
      assert.equal((yield* ctx.store.getEvidence(providerTurnId))?.decision, "stopped");
      assert.isNull(yield* ctx.store.getStepByOrigin(originId));
      assert.lengthOf(ctx.dispatched, 0);
    }).pipe(Effect.provide(testLayer)),
);

it.effect("missing or mismatched native ownership evidence keeps failover deferred", () =>
  Effect.gen(function* () {
    const ctx = yield* makeService();
    const child = yield* nativeChild(ctx, "child");
    yield* settleNativeTask(ctx, child);
    yield* settleItem(ctx, child.item);
    yield* ctx.sink.write({
      events: [
        {
          id: EventId.make("child:missing"),
          type: "subagent.updated",
          threadId,
          occurredAt: ctx.now,
          payload: {
            ...child.task,
            status: "completed",
            completedAt: ctx.now,
            childThreadId: ThreadId.make("thread:missing"),
          },
        },
      ],
    });
    yield* ctx.service.afterFailedRun(threadId, originId);
    assert.equal((yield* ctx.store.getEvidence(providerTurnId))?.state, "deferred");
    assert.lengthOf(ctx.dispatched, 0);
    yield* ctx.sink.write({
      events: [
        {
          id: EventId.make("child:restore"),
          type: "subagent.updated",
          threadId,
          occurredAt: ctx.now,
          payload: { ...child.task, status: "completed", completedAt: ctx.now },
        },
        {
          id: EventId.make("child:wrong-owner"),
          type: "provider-thread.updated",
          threadId: child.thread.id,
          occurredAt: ctx.now,
          payload: { ...child.provider, forkedFrom: null },
        },
      ],
    });
    const lineage = yield* Lineage.make();
    assert.deepEqual(yield* lineage.settleThreadIds(child.thread.id), [child.thread.id]);
    yield* ctx.service.afterChildWorkSettled(threadId);
    assert.equal((yield* ctx.store.getEvidence(providerTurnId))?.state, "deferred");
    assert.lengthOf(ctx.dispatched, 0);
  }).pipe(Effect.provide(testLayer)),
);

it.effect("completed native history on another account does not block the current account", () =>
  Effect.gen(function* () {
    const ctx = yield* makeService();
    const child = yield* nativeChild(ctx, "old-account-child");
    yield* settleNativeTask(ctx, child);
    yield* settleItem(ctx, child.item);
    const records = yield* ctx.projections.getThreadRecords(threadId, [
      "runs",
      "providerThreads",
      "providerTurns",
      "subagents",
      "turnItems",
    ]);
    const current = {
      ...records.providerThreads[0]!,
      id: ProviderThreadId.make("current-account"),
      providerInstanceId: target,
    };
    const lineage = yield* Lineage.make();
    assert.isTrue(
      yield* lineage.sourceQuiet(
        { ...records, providerThreads: [...records.providerThreads, current] },
        current.id,
      ),
    );
  }).pipe(Effect.provide(testLayer)),
);

it.effect("native ownership cycles are finite and never qualify a source as quiet", () =>
  Effect.gen(function* () {
    const ctx = yield* makeService();
    const child = yield* nativeChild(ctx, "child");
    yield* settleNativeTask(ctx, child);
    yield* settleItem(ctx, child.item);
    const backEdge: OrchestrationV2Subagent = {
      ...child.task,
      id: NodeId.make("cycle:back-edge"),
      threadId: child.thread.id,
      runId: null,
      childThreadId: threadId,
      providerThreadId,
      status: "completed",
      completedAt: ctx.now,
    };
    const rootProvider = (yield* ctx.projections.getThreadRecords(threadId, ["providerThreads"]))
      .providerThreads[0]!;
    yield* ctx.sink.write({
      events: [
        {
          id: EventId.make("cycle:root"),
          type: "thread.created",
          threadId,
          occurredAt: ctx.now,
          payload: {
            ...ctx.thread,
            lineage: {
              ...ctx.thread.lineage,
              parentThreadId: child.thread.id,
              relationshipToParent: "subagent",
            },
            forkedFrom: { type: "node", nodeId: backEdge.id },
          },
        },
        {
          id: EventId.make("cycle:provider"),
          type: "provider-thread.updated",
          threadId,
          occurredAt: ctx.now,
          payload: {
            ...rootProvider,
            forkedFrom: { providerThreadId: child.provider.id },
          },
        },
        {
          id: EventId.make("cycle:task"),
          type: "subagent.updated",
          threadId: child.thread.id,
          occurredAt: ctx.now,
          payload: backEdge,
        },
      ],
    });
    const lineage = yield* Lineage.make();
    const cycle = yield* ctx.projections.getThreadRecords(threadId, [
      "runs",
      "providerThreads",
      "providerTurns",
      "subagents",
      "turnItems",
    ]);
    assert.equal(cycle.thread.lineage.parentThreadId, child.thread.id);
    assert.isFalse(yield* lineage.sourceQuiet(cycle, providerThreadId));
    const owners = yield* lineage.settleThreadIds(child.thread.id);
    assert.equal(new Set(owners).size, owners.length);
    assert.isAtMost(owners.length, 2);
    yield* ctx.service.afterFailedRun(threadId, originId);
    yield* ctx.service.afterChildWorkSettled(child.thread.id);
    assert.equal((yield* ctx.store.getEvidence(providerTurnId))?.state, "deferred");
    assert.lengthOf(ctx.dispatched, 0);
  }).pipe(Effect.provide(testLayer)),
);

it.effect("startup never continues evidence older than 24 hours", () =>
  Effect.gen(function* () {
    const ctx = yield* makeService();
    const sql = yield* SqlClient.SqlClient;
    const stale = DateTime.formatIso(DateTime.subtract(ctx.now, { hours: 24, seconds: 1 }));
    yield* sql`UPDATE jones_codex_failover_evidence SET occurred_at = ${stale}`;
    yield* ctx.service.recoverCodexFailover;
    assert.equal((yield* ctx.store.getEvidence(providerTurnId))?.decision, "stale");
    assert.lengthOf(ctx.dispatched, 0);
    assert.isFalse(yield* ctx.service.holdsChildResult(threadId, ctx.run));
    assert.deepEqual(ctx.queues, [originId]);
  }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "startup abandons a rejected continuation without reusing its command ID and advances through the failure queue path",
  () =>
    Effect.gen(function* () {
      const ctx = yield* makeService({ refuseContinuation: true });
      yield* ctx.service.afterFailedRun(threadId, originId);
      const step = (yield* ctx.store.getStepByOrigin(originId))!;
      yield* ctx.store.setStepStatus(step.switchCommandId, "switched", DateTime.formatIso(ctx.now));
      yield* ctx.receipts.upsert({
        commandId: step.continueCommandId,
        threadId,
        commandType: "message.dispatch",
        acceptedAt: ctx.now,
        resultSequence: 0,
        status: "rejected",
        error: "Synthetic prior refusal",
      });
      assert.isTrue(yield* ctx.service.holdsChildResult(threadId, ctx.run));
      const before = ctx.dispatched.length;
      yield* ctx.service.recoverCodexFailover;
      assert.lengthOf(ctx.dispatched, before);
      assert.equal((yield* ctx.store.getStepByOrigin(originId))?.status, "abandoned");
      assert.equal(
        Option.getOrThrow(yield* ctx.receipts.getByCommandId(step.continueCommandId)).status,
        "rejected",
      );
      assert.deepEqual(ctx.queues, [originId]);
    }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "startup replays a switched step with absent receipt using exactly its persisted IDs",
  () =>
    Effect.gen(function* () {
      const ctx = yield* makeService({ refuseContinuation: true });
      yield* ctx.service.afterFailedRun(threadId, originId);
      const step = (yield* ctx.store.getStepByOrigin(originId))!;
      yield* ctx.store.setStepStatus(step.switchCommandId, "switched", DateTime.formatIso(ctx.now));
      yield* ctx.service.recoverCodexFailover;
      assert.lengthOf(ctx.dispatched, 2);
      assert.equal(ctx.dispatched[1]!.commandId, step.continueCommandId);
      assert.equal(
        ctx.dispatched[1]!.type === "message.dispatch" ? ctx.dispatched[1]!.messageId : undefined,
        step.continueMessageId,
      );
      assert.equal((yield* ctx.store.getStepByOrigin(originId))?.status, "abandoned");
      assert.deepEqual(ctx.queues, [originId]);
    }).pipe(Effect.provide(testLayer)),
);

it.effect("startup abandons an absent-receipt step when the saved-selection revision changed", () =>
  Effect.gen(function* () {
    const ctx = yield* makeService({ refuseContinuation: true });
    yield* ctx.service.afterFailedRun(threadId, originId);
    const step = (yield* ctx.store.getStepByOrigin(originId))!;
    yield* ctx.store.setStepStatus(step.switchCommandId, "switched", DateTime.formatIso(ctx.now));
    const thread = (yield* ctx.projections.getThreadRecords(threadId, [])).thread;
    yield* ctx.sink.write({
      events: [
        {
          id: EventId.make("recovery:newer-selection"),
          type: "thread.model-selection-updated",
          threadId,
          occurredAt: ctx.now,
          payload: thread,
        },
      ],
    });
    yield* ctx.service.recoverCodexFailover;
    assert.lengthOf(ctx.dispatched, 1);
    assert.equal((yield* ctx.store.getStepByOrigin(originId))?.status, "abandoned");
    assert.deepEqual(ctx.queues, [originId]);
  }).pipe(Effect.provide(testLayer)),
);

it.effect("startup closes a switched step whose continuation already has an accepted receipt", () =>
  Effect.gen(function* () {
    const ctx = yield* makeService();
    yield* ctx.service.afterFailedRun(threadId, originId);
    const step = (yield* ctx.store.getStepByOrigin(originId))!;
    yield* ctx.store.setStepStatus(step.switchCommandId, "switched", DateTime.formatIso(ctx.now));
    yield* ctx.service.recoverCodexFailover;
    assert.lengthOf(ctx.dispatched, 1);
    assert.equal((yield* ctx.store.getStepByOrigin(originId))?.status, "continued");
    assert.deepEqual(ctx.queues, [originId]);
  }).pipe(Effect.provide(testLayer)),
);

it.effect("without ProviderRegistry every hook fails closed", () =>
  Effect.gen(function* () {
    const ctx = yield* makeService({ noRegistry: true });
    assert.isUndefined(ctx.service.commitContext(queuedCommand("inherited")));
    assert.isUndefined(
      yield* ctx.service.continuationSourceRunId({
        threadId,
        commandId: CommandId.make("jones:codex-failover:unknown:continue"),
      }),
    );
    assert.isFalse(yield* ctx.service.holdsChildResult(threadId, ctx.run));
    yield* ctx.service.afterFailedRun(threadId, originId);
    yield* ctx.service.recoverCodexFailover;
    assert.lengthOf(ctx.dispatched, 0);
    assert.deepEqual(ctx.queues, []);
    assert.equal((yield* ctx.store.getEvidence(providerTurnId))?.state, "recorded");
  }).pipe(Effect.provide(testLayer)),
);

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { ProviderDriverKind, ProviderInstanceId, type ThreadId } from "@t3tools/contracts";
import type { ProviderAdapterV2Shape } from "@t3tools/provider-core/server/ProviderAdapter";
import type { ProviderInstance } from "@t3tools/provider-core/server/driver";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as CheckpointStore from "../../checkpointing/CheckpointStore.ts";
import * as ServerConfig from "../../config.ts";
import * as GitWorkflow from "../../git/GitWorkflowService.ts";
import * as McpSessionRegistryTestkit from "../../mcp/McpSessionRegistry.testkit.ts";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as EffectOutbox from "../../orchestration-v2/EffectOutbox.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as ProjectStore from "../../orchestration-v2/ProjectStore.ts";
import * as ProviderRuntimeRecovery from "../../orchestration-v2/ProviderRuntimeRecoveryService.ts";
import * as RuntimeLayer from "../../orchestration-v2/runtimeLayer.ts";
import * as ThreadCommandExecutor from "../../orchestration-v2/ThreadCommandExecutor.ts";
import * as AuthSessions from "../../persistence/AuthSessions.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as ProjectService from "../../project/ProjectService.ts";
import * as ProviderInstanceRegistry from "../../provider/ProviderInstanceRegistry.ts";
import * as ServerSettings from "../../serverSettings.ts";
import * as SourceControlProviderRegistry from "../../sourceControl/SourceControlProviderRegistry.ts";
import * as VcsDriverRegistry from "../../vcs/VcsDriverRegistry.ts";
import * as VcsProcess from "../../vcs/VcsProcess.ts";
import * as Fixture from "./OrphanedRunRecords.testkit.ts";

// The production runtime layer, so the startup recovery pass runs with
// whatever hooks the server composes into it.
const PlatformTestLayer = Layer.merge(
  NodeServices.layer,
  Layer.mock(SourceControlProviderRegistry.SourceControlProviderRegistry)({
    resolveLink: () => Effect.die("unused title link"),
  }),
);
const ServerConfigLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3-orphaned-run-records-",
});
const CheckpointStoreTestLayer = CheckpointStore.layer.pipe(
  Layer.provide(
    VcsDriverRegistry.layer.pipe(
      Layer.provide(VcsProcess.layer),
      Layer.provide(ServerConfigLayer),
      Layer.provide(PlatformTestLayer),
    ),
  ),
);
const driver = ProviderDriverKind.make("codex");
const instanceId = ProviderInstanceId.make("codex");
const providerInstance = {
  instanceId,
  driverKind: driver,
  continuationIdentity: { driverKind: driver, continuationKey: "codex:test" },
  displayName: "Codex test",
  enabled: true,
  snapshot: { getSnapshot: Effect.succeed({}) } as unknown as ProviderInstance["snapshot"],
  orchestrationAdapter: {
    instanceId,
    driver,
    getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
    planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
    openSession: () => Effect.die("startup recovery opens no provider session"),
  } as ProviderAdapterV2Shape,
  textGeneration: {} as ProviderInstance["textGeneration"],
} satisfies ProviderInstance;

const TestLayer = Layer.mergeAll(
  RuntimeLayer.layer,
  RuntimeLayer.layerEventSink,
  ProjectStore.layer,
  ProjectionStore.layer,
  EffectOutbox.layer,
  ThreadCommandExecutor.layer,
  AuthSessions.layer,
).pipe(
  Layer.provide(McpSessionRegistryTestkit.layer),
  Layer.provide(McpProviderSessions.layer),
  Layer.provideMerge(SqlitePersistence.layerMemory),
  Layer.provide(CheckpointStoreTestLayer),
  Layer.provide(ServerConfigLayer),
  Layer.provide(ServerSettings.layerTest()),
  Layer.provide(
    Layer.succeed(ProviderInstanceRegistry.ProviderInstanceRegistry, {
      getCatalogSource: (id) => (id === instanceId ? providerInstance : undefined),
      getInstance: (id) => Effect.succeed(id === instanceId ? providerInstance : undefined),
      listInstances: Effect.succeed([providerInstance]),
      listUnavailable: Effect.succeed([]),
      streamChanges: Stream.empty,
      subscribeChanges: Effect.never,
    }),
  ),
  Layer.provide(
    Layer.mock(GitWorkflow.GitWorkflowService)({
      pruneWorktrees: () => Effect.void,
      createWorktree: () => Effect.succeed({} as never),
    }),
  ),
  Layer.provide(Layer.mock(ProjectService.ProjectService)({ getById: () => Effect.succeedNone })),
  Layer.provide(PlatformTestLayer),
);

const statusesOf = (threadId: ThreadId) =>
  Effect.gen(function* () {
    const projection = yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadProjection(
      threadId,
    );
    const status = new Map<string, string>();
    for (const record of [
      ...projection.runs,
      ...projection.nodes,
      ...projection.providerTurns,
      ...projection.turnItems,
      ...projection.runtimeRequests,
    ]) {
      status.set(record.id, record.status);
    }
    return { projection, status };
  });

it.layer(TestLayer)("startup recovery of records left open under ended runs", (it) => {
  it.effect(
    "closes them through the recovery command path, leaves open-by-design work alone, and repeats as a no-op",
    () =>
      Effect.gen(function* () {
        const sink = yield* EventSink.EventSinkV2;
        const events = yield* EventStore.EventStoreV2;
        const recovery = yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService;
        const now = yield* DateTime.now;
        const ended = Fixture.endedRunThread("startup-ended", now);
        const runless = Fixture.runlessSubagentThread("startup-runless", now);
        yield* sink.write({ events: [...ended.events, ...runless.events] });

        yield* recovery.reconcile("startup");

        const endedState = yield* statusesOf(ended.threadId);
        for (const id of [
          ...ended.orphaned.providerTurnIds,
          ...ended.orphaned.nodeIds,
          ...ended.orphaned.turnItemIds,
        ]) {
          assert.strictEqual(endedState.status.get(id), "interrupted", id);
        }
        const runlessState = yield* statusesOf(runless.threadId);
        for (const id of [...runless.orphaned.providerTurnIds, ...runless.orphaned.nodeIds]) {
          assert.strictEqual(runlessState.status.get(id), "interrupted", id);
        }
        const reasoning = endedState.projection.turnItems.find(
          (item) => item.id === ended.orphaned.turnItemIds[0],
        );
        assert.strictEqual(reasoning?.type === "reasoning" && reasoning.streaming, false);

        const [heldRunId] = ended.preserved.runIds;
        const heldRun = endedState.projection.runs.find((run) => run.id === heldRunId);
        assert.strictEqual(heldRun?.status, "queued");
        assert.strictEqual(heldRun?.queueHeld, true);
        const [todoNodeId, questionNodeId, heldRootNodeId] = ended.preserved.nodeIds;
        assert.strictEqual(endedState.status.get(todoNodeId!), "running");
        assert.strictEqual(endedState.status.get(questionNodeId!), "waiting");
        assert.strictEqual(endedState.status.get(heldRootNodeId!), "pending");
        const [todoItemId, questionItemId] = ended.preserved.turnItemIds;
        assert.strictEqual(endedState.status.get(todoItemId!), "running");
        assert.strictEqual(endedState.status.get(questionItemId!), "waiting");
        assert.strictEqual(endedState.status.get(ended.preserved.runtimeRequestIds[0]!), "pending");

        // A later restart allocates a fresh command id, so only an empty candidate set keeps it quiet.
        const sequenceAfterFirstPass = yield* events.latestSequence();
        yield* TestClock.adjust("1 minute");
        yield* recovery.reconcile("startup");
        assert.strictEqual(yield* events.latestSequence(), sequenceAfterFirstPass);
      }),
  );
});

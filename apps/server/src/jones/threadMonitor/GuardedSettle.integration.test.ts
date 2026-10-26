import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  NodeId,
  PlanId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderSessionId,
  RunId,
  RuntimeRequestId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2DomainEvent,
  type ThreadPullRequestLink,
} from "@t3tools/contracts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Result from "effect/Result";
import * as Stream from "effect/Stream";
import * as ServerConfig from "../../config.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as EffectOutbox from "../../orchestration-v2/EffectOutbox.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as ProviderAdapterRegistry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ThreadCommandExecutor from "../../orchestration-v2/ThreadCommandExecutor.ts";
import * as ProviderReplayHarness from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";

const instanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId, model: "gpt-5.4" };
const adapter = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("guard tests never launch providers"),
} as ProviderAdapter.ProviderAdapterV2["Service"];
// Scope owns exact scratch cleanup, including assertion failure and interruption.
const config = Layer.unwrap(
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const parent = path.resolve(import.meta.dirname, "../../../../..", ".t3");
    yield* fs.makeDirectory(parent, { recursive: true });
    const directory = yield* fs.makeTempDirectoryScoped({
      directory: parent,
      prefix: "monitor-guard-",
    });
    return ServerConfig.layerTest(process.cwd(), directory);
  }).pipe(Effect.orDie),
).pipe(Layer.provide(NodeServices.layer), Layer.orDie);
const database = SqlitePersistence.layerMemory;
const TestLayer = ProviderReplayHarness.layerWithRegistryAndServices(
  { name: "monitor-guard" },
  ProviderAdapterRegistry.layerFromAdapters([adapter]),
  {
    databaseLayer: database,
    serverConfigLayer: config,
    runEffectWorker: false,
    runContinuationWorker: false,
  },
);
const create = Effect.fnUntraced(function* (name: string) {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const threadId = ThreadId.make(`monitor:${name}`);
  yield* orchestrator.dispatch({
    type: "thread.create",
    commandId: CommandId.make(`create:${name}`),
    threadId,
    projectId: ProjectId.make(`project:${name}`),
    title: "Monitor guard",
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    createdBy: "user",
    creationSource: "web",
  });
  return threadId;
});
const guard = Effect.fnUntraced(function* (
  threadId: ThreadId,
  commandId: string,
  expected?: string,
) {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const shell = yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadShell(threadId);
  assert.ok(shell);
  return yield* orchestrator.dispatch({
    type: "thread.settle",
    commandId: CommandId.make(commandId),
    threadId,
    guard: {
      policy: "preserve_pending_work",
      expectedUpdatedAt: expected ?? DateTime.formatIso(shell.updatedAt),
    },
  });
});
const assertGuardRejection = Effect.fnUntraced(function* (
  threadId: ThreadId,
  commandId: string,
  message: string,
  expected?: string,
) {
  const error = yield* guard(threadId, commandId, expected).pipe(Effect.flip);
  assert.equal(error._tag, "OrchestratorDispatchError");
  if (error._tag !== "OrchestratorDispatchError")
    return yield* Effect.die(new Error("Expected guarded policy rejection"));
  assert.equal(error.cause, message);
});
const emit = (event: OrchestrationV2DomainEvent) =>
  Effect.flatMap(EventSink.EventSinkV2, (sink) => sink.write({ events: [event] }));
const assertNoEffects = Effect.fnUntraced(function* (commandId: string) {
  const events = yield* (yield* EventSink.EventSinkV2)
    .readByCommandId({ commandId: CommandId.make(commandId) })
    .pipe(Stream.runCollect);
  assert.equal(Array.from(events).length, 0);
  assert.deepEqual(
    yield* (yield* EffectOutbox.EffectOutboxV2).listByCommandId(CommandId.make(commandId)),
    [],
  );
});
const request = Effect.fnUntraced(function* (
  threadId: ThreadId,
  kind: "permission" | "user_input",
) {
  const now = yield* DateTime.now;
  const nodeId = NodeId.make(`node:${threadId}`);
  yield* emit({
    id: EventId.make(`node:${threadId}`),
    type: "node.updated",
    threadId,
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
      runtimeRequestId: RuntimeRequestId.make(`request:${threadId}`),
      checkpointScopeId: null,
      startedAt: now,
      completedAt: null,
    },
  });
  yield* emit({
    id: EventId.make(`request:${threadId}`),
    type: "runtime-request.updated",
    threadId,
    occurredAt: now,
    payload: {
      id: RuntimeRequestId.make(`request:${threadId}`),
      nodeId,
      providerTurnId: null,
      nativeRequestRef: null,
      kind,
      status: "pending",
      responseCapability:
        kind === "user_input"
          ? { type: "message" }
          : { type: "live", providerSessionId: ProviderSessionId.make("approval-session") },
      createdAt: now,
      resolvedAt: null,
    },
  });
});

it.layer(TestLayer)("monitor guarded settlement under real dispatch", (it) => {
  it.effect(
    "accepts an idle thread, preserves exact receipt replay, and restores the actual settle",
    () =>
      Effect.gen(function* () {
        const threadId = yield* create("valid");
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        yield* orchestrator.dispatch({
          type: "thread.pin",
          threadId,
          commandId: CommandId.make("pin-valid"),
          orderKey: "n",
        });
        const first = yield* guard(threadId, "settle-valid");
        assert.ok(first.storedEvents.some((event) => event.event.type === "thread.settled"));
        const replay = yield* guard(threadId, "settle-valid", "2000-01-01T00:00:00.000Z");
        assert.equal(replay.sequence, first.sequence);
        const settled = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(settled.thread.settledOverride, "settled");
        yield* orchestrator.dispatch({
          type: "thread.settlement.restore",
          threadId,
          commandId: CommandId.make("restore-valid"),
          settleCommandId: CommandId.make("settle-valid"),
        });
        const restored = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(restored.thread.settledOverride, "active");
        assert.equal(restored.thread.pinOrderKey, "n");
      }),
  );

  it.effect(
    "accepts terminal completed work when no request, plan, goal or background work remains",
    () =>
      Effect.gen(function* () {
        const threadId = yield* create("completed-valid");
        const now = yield* DateTime.now;
        const runId = RunId.make("completed-valid-run");
        yield* emit({
          id: EventId.make("completed-valid-run"),
          type: "run.created",
          threadId,
          occurredAt: now,
          payload: {
            id: runId,
            threadId,
            ordinal: 1,
            providerInstanceId: instanceId,
            modelSelection,
            providerThreadId: null,
            userMessageId: MessageId.make("completed-valid-message"),
            rootNodeId: null,
            activeAttemptId: null,
            status: "completed",
            requestedAt: now,
            startedAt: now,
            completedAt: now,
            checkpointId: null,
            contextHandoffId: null,
          },
        });
        const result = yield* guard(threadId, "settle-completed-valid");
        assert.ok(result.storedEvents.some((event) => event.event.type === "thread.settled"));
        assert.equal(
          (yield* (yield* ProjectionStore.ProjectionStoreV2).getThread(threadId)).settledOverride,
          "settled",
        );
      }),
  );

  it.effect.each(["user_input", "permission"] as const)(
    "rejects a raced %s before cancellation or detach effects",
    (kind) =>
      Effect.gen(function* () {
        const threadId = yield* create(`race-${kind}`);
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const expected = DateTime.formatIso((yield* projections.getThread(threadId)).updatedAt);
        const locks = yield* ThreadCommandExecutor.ThreadCommandExecutor;
        const admitted = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const held = yield* locks
          .withLock(
            threadId,
            Effect.gen(function* () {
              yield* Deferred.succeed(admitted, undefined);
              yield* Deferred.await(release);
            }),
          )
          .pipe(Effect.forkChild);
        yield* Deferred.await(admitted);
        const settle = yield* guard(threadId, `refuse-${kind}`, expected).pipe(
          Effect.result,
          Effect.forkChild,
        );
        yield* request(threadId, kind);
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(held);
        const result = yield* Fiber.join(settle);
        assert.ok(Result.isFailure(result));
        assert.equal(result.failure._tag, "OrchestratorDispatchError");
        if (result.failure._tag === "OrchestratorDispatchError")
          assert.equal(result.failure.cause, "Resolve every pending request before settling.");
        yield* assertNoEffects(`refuse-${kind}`);
        const current = yield* projections.getThreadProjection(threadId);
        assert.equal(current.runtimeRequests[0]?.status, "pending");
        assert.isNull(current.thread.settledAt);
        if (kind === "user_input") {
          yield* (yield* Orchestrator.OrchestratorV2).dispatch({
            type: "thread.settle",
            commandId: CommandId.make("legacy-nonblocking"),
            threadId,
          });
          const legacy = yield* projections.getThreadProjection(threadId);
          assert.equal(legacy.runtimeRequests[0]?.status, "resolved");
          assert.equal(legacy.runtimeRequests[0]?.decision, "cancel");
          assert.equal(legacy.nodes[0]?.status, "cancelled");
        }
      }),
  );

  it.effect("does not stop or detach an active run when guarded settlement is attempted", () =>
    Effect.gen(function* () {
      const threadId = yield* create("active");
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        threadId,
        commandId: CommandId.make("active-message"),
        messageId: MessageId.make("active-message"),
        text: "Keep working",
        attachments: [],
        modelSelection,
        dispatchMode: { type: "start_immediately" },
        createdBy: "user",
        creationSource: "web",
      });
      const before = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(before.runs[0]?.status, "starting");
      yield* assertGuardRejection(
        threadId,
        "refuse-active",
        "Thread has active or queued work, including automatic work.",
      );
      yield* assertNoEffects("refuse-active");
      const after = yield* orchestrator.getThreadProjection(threadId);
      assert.deepEqual(after.runs, before.runs);
      assert.deepEqual(after.providerThreads, before.providerThreads);
      assert.isNull(after.thread.settledAt);
    }),
  );

  it.effect("rejects every automatic queued run while preserving legacy cancellation", () =>
    Effect.gen(function* () {
      const threadId = yield* create("automatic");
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        threadId,
        commandId: CommandId.make("automatic-active"),
        messageId: MessageId.make("automatic-active"),
        text: "Active",
        attachments: [],
        modelSelection,
        dispatchMode: { type: "start_immediately" },
        createdBy: "user",
        creationSource: "web",
      });
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        threadId,
        commandId: CommandId.make("automatic-message"),
        messageId: MessageId.make("automatic-message"),
        text: "Background wake",
        attachments: [],
        modelSelection,
        dispatchMode: { type: "queue_after_active" },
        createdBy: "agent",
        creationSource: "provider",
        notification: {
          source: { kind: "background_task" },
          outcome: "updated",
          summary: "Background wake",
        },
      });
      const queuedProjection = yield* orchestrator.getThreadProjection(threadId);
      const now = yield* DateTime.now;
      yield* (yield* EventSink.EventSinkV2).commitCommand({
        commandId: CommandId.make("automatic-hold"),
        threadId,
        commandType: "provider-runtime.reconcile",
        acceptedAt: now,
        effects: [],
        events: queuedProjection.runs.map((run) => ({
          id: EventId.make(`hold:${run.id}`),
          type: "run.updated" as const,
          threadId,
          runId: run.id,
          occurredAt: now,
          payload:
            run.status === "queued"
              ? { ...run, queueHeld: true }
              : { ...run, status: "cancelled" as const, completedAt: now },
        })),
      });
      const before = yield* orchestrator.getThreadProjection(threadId);
      assert.ok(before.runs.some((run) => run.status === "queued"));
      yield* assertGuardRejection(
        threadId,
        "refuse-automatic",
        "Thread has active or queued work, including automatic work.",
      );
      yield* assertNoEffects("refuse-automatic");
      assert.ok(
        (yield* orchestrator.getThreadProjection(threadId)).runs.some(
          (run) => run.status === "queued",
        ),
      );
      yield* orchestrator.dispatch({
        type: "thread.settle",
        threadId,
        commandId: CommandId.make("legacy-automatic"),
      });
      assert.ok(
        (yield* orchestrator.getThreadProjection(threadId)).runs.every(
          (run) => run.status === "cancelled",
        ),
      );
    }),
  );

  it.effect("rejects stale snapshots and newly actionable plans without settlement effects", () =>
    Effect.gen(function* () {
      const threadId = yield* create("plan");
      yield* assertGuardRejection(
        threadId,
        "refuse-stale",
        "Thread changed after the monitor snapshot. Refresh before settling.",
        "2000-01-01T00:00:00.000Z",
      );
      yield* assertNoEffects("refuse-stale");
      const now = yield* DateTime.now;
      yield* emit({
        id: EventId.make("plan-active"),
        type: "plan.updated",
        threadId,
        occurredAt: now,
        payload: {
          id: PlanId.make("actual-plan"),
          threadId,
          runId: null,
          nodeId: NodeId.make("plan-node"),
          kind: "proposed_plan",
          status: "active",
          markdown: "Implement this current plan",
        },
      });
      yield* assertGuardRejection(threadId, "refuse-plan", "Thread has an actionable plan.");
      yield* assertNoEffects("refuse-plan");
    }),
  );

  it.effect("rejects a pull request watch without changing or disposing the watch", () =>
    Effect.gen(function* () {
      const threadId = yield* create("watch");
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      const link: ThreadPullRequestLink = {
        host: "github.com",
        repository: "Jones-Systems/Jones-Code",
        number: 1,
        url: "https://github.com/Jones-Systems/Jones-Code/pull/1",
        source: "manual",
        linkedAt: DateTime.formatIso(now),
        snapshot: null,
        stack: null,
        watch: {
          startedAt: DateTime.formatIso(now),
          headSha: null,
          failedChecks: [],
          passedChecks: [],
          passed: false,
          remarksThrough: DateTime.formatIso(now),
          remarkIds: [],
          conflicting: false,
          wakes: 0,
        },
      };
      const before = yield* projections.getThread(threadId);
      yield* emit({
        id: EventId.make("watch-metadata"),
        type: "thread.metadata-updated",
        threadId,
        occurredAt: now,
        payload: { ...before, pullRequests: [link] },
      });
      yield* assertGuardRejection(
        threadId,
        "refuse-watch",
        "Thread has background work or a pull request watch.",
      );
      yield* assertNoEffects("refuse-watch");
      assert.deepEqual((yield* projections.getThread(threadId)).pullRequests, [link]);
    }),
  );

  it.effect("rejects incomplete child coverage and accepts a completed settled child", () =>
    Effect.gen(function* () {
      const threadId = yield* create("parent");
      const childId = yield* create("child");
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      const child = yield* projections.getThread(childId);
      yield* emit({
        id: EventId.make("child-lineage"),
        type: "thread.metadata-updated",
        threadId: childId,
        occurredAt: now,
        payload: {
          ...child,
          lineage: {
            rootThreadId: threadId,
            parentThreadId: threadId,
            relationshipToParent: "subagent",
          },
        },
      });
      const task = {
        id: NodeId.make("child-task"),
        threadId,
        runId: null,
        parentNodeId: NodeId.make("parent-node"),
        origin: "app_owned" as const,
        createdBy: "agent" as const,
        driver: adapter.driver,
        providerInstanceId: instanceId,
        providerThreadId: null,
        childThreadId: childId,
        nativeTaskRef: null,
        prompt: "Synthetic task",
        title: null,
        model: null,
        status: "completed" as const,
        result: "Completed",
        startedAt: now,
        completedAt: now,
        updatedAt: now,
      };
      yield* emit({
        id: EventId.make("completed-child-task"),
        type: "subagent.updated",
        threadId,
        occurredAt: now,
        payload: task,
      });
      yield* assertGuardRejection(
        threadId,
        "refuse-child-coverage",
        "Complete child-work coverage is unavailable. Manage this thread in Jones Code.",
      );
      yield* assertNoEffects("refuse-child-coverage");
      yield* (yield* Orchestrator.OrchestratorV2).dispatch({
        type: "thread.settle",
        threadId: childId,
        commandId: CommandId.make("settle-child"),
      });
      yield* guard(threadId, "settle-parent-after-child");
      assert.equal((yield* projections.getThread(threadId)).settledOverride, "settled");
    }),
  );

  it.effect.each(["goal", "background", "item"] as const)(
    "rejects current %s work from real projections",
    (kind) =>
      Effect.gen(function* () {
        const threadId = yield* create(kind);
        const now = yield* DateTime.now;
        if (kind === "item") {
          yield* emit({
            id: EventId.make("active-item"),
            type: "turn-item.updated",
            threadId,
            occurredAt: now,
            payload: {
              id: TurnItemId.make("active-item"),
              threadId,
              runId: null,
              nodeId: NodeId.make("active-item-node"),
              providerThreadId: null,
              providerTurnId: null,
              nativeItemRef: null,
              parentItemId: null,
              ordinal: 1,
              type: "command_execution",
              status: "running",
              title: "Background command",
              input: "bounded synthetic command",
              startedAt: now,
              completedAt: null,
              updatedAt: now,
            },
          });
        } else {
          yield* emit({
            id: EventId.make(`provider-${kind}`),
            type: "provider-thread.updated",
            threadId,
            occurredAt: now,
            payload: {
              id: ProviderThreadId.make(`provider-${kind}`),
              driver: adapter.driver,
              providerInstanceId: instanceId,
              providerSessionId: null,
              appThreadId: threadId,
              ownerNodeId: null,
              nativeThreadRef: null,
              nativeConversationHeadRef: null,
              status: "idle",
              firstRunOrdinal: null,
              lastRunOrdinal: null,
              handoffIds: [],
              forkedFrom: null,
              createdAt: now,
              updatedAt: now,
              ...(kind === "goal"
                ? { goal: { objective: "Keep working", status: "active" } }
                : { pendingBackgroundTasks: [{ kind: "background_task", taskId: "actual-task" }] }),
            },
          });
        }
        yield* assertGuardRejection(
          threadId,
          `refuse-${kind}`,
          kind === "goal"
            ? "Thread has an unfinished goal."
            : "Thread has background work or a pull request watch.",
        );
        yield* assertNoEffects(`refuse-${kind}`);
        assert.isNull(
          (yield* (yield* ProjectionStore.ProjectionStoreV2).getThread(threadId)).settledAt,
        );
      }),
  );
});

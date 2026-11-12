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
  ProviderTurnId,
  RunId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2Run,
  type OrchestrationV2Subagent,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import type { ProviderAdapterV2Shape } from "@t3tools/provider-core/server/ProviderAdapter";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import * as Stream from "effect/Stream";
import * as ServerConfig from "../../config.ts";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as EffectOutbox from "../../orchestration-v2/EffectOutbox.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as ProviderAdapterRegistry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as ProviderRegistry from "../../provider/ProviderRegistry.ts";
import * as CodexFailoverStore from "../codexFailover/CodexFailoverStore.ts";
import { makeJonesTablePresence } from "../persistence/JonesTablePresence.ts";
import { makeDependencyStore } from "../threadResultDependencies/DependencyStore.ts";

const instanceId = ProviderInstanceId.make("codex");
const driver = ProviderDriverKind.make("codex");
const modelSelection = { instanceId, model: "gpt-5.1-codex" };
const adapter = {
  instanceId,
  driver,
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("Notice disposition tests never launch a provider"),
} as ProviderAdapterV2Shape;
const database = SqlitePersistence.layerMemory;
const config = Layer.unwrap(
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "jones-notice-disposition-" });
    return ServerConfig.layerTest(process.cwd(), directory);
  }).pipe(Effect.orDie),
).pipe(Layer.provide(NodeServices.layer), Layer.orDie);
const testLayer = Layer.mergeAll(
  database,
  ProjectionStore.layer.pipe(Layer.provide(database)),
  EffectOutbox.layer.pipe(Layer.provide(database)),
  ProviderReplayHarness.layerWithRegistry(
    { name: "delegated-notice-disposition" },
    ProviderAdapterRegistry.layerFromAdapters([adapter]),
    { databaseLayer: database, serverConfigLayer: config, runEffectWorker: false },
  ).pipe(
    Layer.provide(
      Layer.mock(ProviderRegistry.ProviderRegistry)({ getProviders: Effect.succeed([]) }),
    ),
  ),
);

const fixture = (
  input: {
    readonly cohorts?: number;
    readonly later?: "failed_before_start" | "cancelled_before_start" | "cancelled_after_start";
    readonly initialStatus?: "completed" | "failed";
  } = {},
) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const store = yield* ProjectionStore.ProjectionStoreV2;
    const sink = yield* EventSink.EventSinkV2;
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    const now = yield* DateTime.now;
    const old = DateTime.add(now, { days: -8 });
    const parentId = ThreadId.make("thread:notice-parent");
    const childId = ThreadId.make("thread:notice-child");
    const projectId = ProjectId.make("project:notice-disposition");
    let eventOrdinal = 0;
    const put = (event: OrchestrationV2DomainEvent) => store.apply(event);
    const event = <T extends OrchestrationV2DomainEvent["type"]>(
      type: T,
      threadId: ThreadId,
      payload: Extract<OrchestrationV2DomainEvent, { readonly type: T }>["payload"],
    ) =>
      ({
        id: EventId.make(`fixture:notice:${++eventOrdinal}`),
        type,
        threadId,
        occurredAt: now,
        payload,
      }) as OrchestrationV2DomainEvent;
    for (const threadId of [parentId, childId]) {
      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make(`create:${threadId}`),
        threadId,
        projectId,
        title: "Synthetic delegated notice disposition",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        createdBy: "user",
        creationSource: "web",
      });
    }
    const run = (
      threadId: ThreadId,
      name: string,
      ordinal: number,
      status: OrchestrationV2Run["status"],
      started = true,
    ): OrchestrationV2Run => ({
      id: RunId.make(`run:${name}`),
      threadId,
      ordinal,
      providerInstanceId: instanceId,
      modelSelection,
      providerThreadId: null,
      userMessageId: MessageId.make(`message:${name}`),
      rootNodeId: NodeId.make(`root:${name}`),
      activeAttemptId: null,
      status,
      queueHeld: status === "queued",
      queuePosition: status === "queued" ? ordinal : null,
      requestedAt: DateTime.add(old, { minutes: ordinal }),
      startedAt: started ? DateTime.add(old, { minutes: ordinal }) : null,
      completedAt:
        status === "queued" || status === "running"
          ? null
          : DateTime.add(old, { minutes: ordinal + 1 }),
      checkpointId: null,
      contextHandoffId: null,
    });
    const node = (
      owner: OrchestrationV2Run,
      id: NodeId,
      kind: OrchestrationV2ExecutionNode["kind"],
      status: OrchestrationV2ExecutionNode["status"],
    ): OrchestrationV2ExecutionNode => ({
      id,
      threadId: owner.threadId,
      runId: owner.id,
      parentNodeId: kind === "root_turn" ? null : owner.rootNodeId,
      rootNodeId: owner.rootNodeId!,
      kind,
      status,
      countsForRun: kind === "root_turn",
      providerThreadId: null,
      providerTurnId: null,
      nativeItemRef: null,
      runtimeRequestId: null,
      checkpointScopeId: null,
      startedAt: owner.startedAt,
      completedAt: status === "running" || status === "pending" ? null : owner.completedAt,
    });
    const message = (
      owner: OrchestrationV2Run,
      text: string,
      ownership?: OrchestrationV2ConversationMessage["delegatedCompletion"],
    ): OrchestrationV2ConversationMessage => ({
      id: owner.userMessageId,
      threadId: owner.threadId,
      runId: owner.id,
      nodeId: owner.rootNodeId,
      createdBy: ownership === undefined ? "user" : "system",
      creationSource: ownership === undefined ? "web" : "server",
      role: "user",
      text,
      attachments: [],
      streaming: false,
      createdAt: owner.requestedAt,
      updatedAt: owner.requestedAt,
      ...(ownership === undefined ? {} : { delegatedCompletion: ownership }),
    });
    const putRun = (row: OrchestrationV2Run) => put(event("run.updated", row.threadId, row));
    const putMessage = (row: OrchestrationV2ConversationMessage) =>
      put(event("message.updated", row.threadId, row));
    const putTask = (row: OrchestrationV2Subagent) =>
      put(event("subagent.updated", row.threadId, row));
    const seedRun = (row: OrchestrationV2Run) =>
      Effect.gen(function* () {
        yield* putRun(row);
        const status =
          row.status === "queued" || row.status === "preparing"
            ? "pending"
            : row.status === "starting"
              ? "running"
              : row.status;
        yield* put(
          event("node.updated", row.threadId, node(row, row.rootNodeId!, "root_turn", status)),
        );
      });
    const task = (
      owner: OrchestrationV2Run,
      id: NodeId,
      childThreadId: ThreadId | null,
      status: OrchestrationV2Subagent["status"],
    ): OrchestrationV2Subagent => ({
      id,
      threadId: owner.threadId,
      runId: owner.id,
      parentNodeId: owner.rootNodeId!,
      origin: "app_owned",
      createdBy: "agent",
      driver,
      providerInstanceId: instanceId,
      providerThreadId: null,
      childThreadId,
      nativeTaskRef: null,
      prompt: "Synthetic delegated task",
      title: null,
      model: null,
      completionWake: "settled_only",
      completionDelivery: { state: "claimed", observedByRunId: null },
      status,
      result: status === "failed" ? "Grandchild failed" : null,
      startedAt: old,
      completedAt: status === "failed" ? old : null,
      updatedAt: old,
    });
    const parentRun = {
      ...run(parentId, "parent", 1, "completed"),
      delegatedCompletion: { disposition: "disposed", nextGeneration: 1, delivery: null } as const,
    };
    yield* seedRun(parentRun);
    const ancestor = task(parentRun, NodeId.make("task:notice-child"), childId, "running");
    yield* putTask(ancestor);
    yield* put(
      event("node.updated", parentId, node(parentRun, ancestor.id, "subagent", "running")),
    );
    const ancestorItem: OrchestrationV2TurnItem = {
      id: TurnItemId.make("turn-item:notice-child"),
      type: "subagent",
      threadId: parentId,
      runId: parentRun.id,
      nodeId: ancestor.id,
      providerThreadId: null,
      providerTurnId: null,
      nativeItemRef: null,
      parentItemId: null,
      ordinal: 1,
      status: "running",
      title: null,
      startedAt: old,
      completedAt: null,
      updatedAt: old,
      subagentId: ancestor.id,
      origin: "app_owned",
      driver,
      providerInstanceId: instanceId,
      childThreadId: childId,
      prompt: ancestor.prompt,
      result: null,
    };
    yield* put(event("turn-item.updated", parentId, ancestorItem));
    const childThread = yield* store.getThread(childId);
    yield* put({
      id: EventId.make(`fixture:notice:${++eventOrdinal}`),
      type: "thread.metadata-updated",
      threadId: childId,
      occurredAt: now,
      payload: {
        ...childThread,
        lineage: {
          rootThreadId: parentId,
          parentThreadId: parentId,
          relationshipToParent: "subagent",
        },
        forkedFrom: { type: "node", nodeId: ancestor.id },
      },
    });
    const cohorts: Array<{
      origin: OrchestrationV2Run;
      delivery: OrchestrationV2Run;
      tasks: ReadonlyArray<OrchestrationV2Subagent>;
    }> = [];
    for (let index = 0; index < (input.cohorts ?? 1); index++) {
      const origin = run(
        childId,
        `child-origin:${index}`,
        index * 2 + 1,
        input.initialStatus ?? "completed",
      );
      const delivery = run(childId, `notice:${index}`, index * 2 + 2, "queued", false);
      const tasks = Array.from({ length: 3 }, (_, ordinal) =>
        task(origin, NodeId.make(`task:grandchild:${index}:${ordinal}`), null, "failed"),
      );
      const taskIds = tasks.map((row) => row.id);
      const withCohort = {
        ...origin,
        delegatedCompletion: {
          disposition: "open",
          nextGeneration: 2,
          delivery: { generation: 1, messageId: delivery.userMessageId, taskIds },
        } as const,
      };
      yield* seedRun(withCohort);
      yield* seedRun(delivery);
      yield* putMessage(
        message(delivery, "Three delegated tasks failed", {
          parentRunId: origin.id,
          generation: 1,
          taskIds,
        }),
      );
      for (const row of tasks) yield* putTask(row);
      cohorts.push({ origin: withCohort, delivery, tasks });
    }
    const laterKind = input.later ?? "failed_before_start";
    const later = run(
      childId,
      "child-later",
      cohorts.length * 2 + 1,
      laterKind === "failed_before_start" ? "failed" : "cancelled",
      laterKind === "cancelled_after_start",
    );
    yield* seedRun(later);
    const original = cohorts[0]!.origin;
    const resultMessage: OrchestrationV2ConversationMessage = {
      ...message(original, "Original executed result"),
      id: MessageId.make("message:executed-result"),
      role: "assistant",
      createdBy: "agent",
      creationSource: "provider",
    };
    yield* putMessage(resultMessage);
    const projection = () => store.getThreadProjection(childId);
    const cancel = (selected: OrchestrationV2Run) =>
      orchestrator.dispatch({
        type: "queued-run.cancel",
        commandId: CommandId.make(`cancel:${selected.id}`),
        threadId: childId,
        runId: selected.id,
      });
    const assertWaiting = Effect.gen(function* () {
      assert.isTrue(yield* orchestrator.delegatedTaskResultPending(childId));
      yield* orchestrator.finalizeDelegatedTask(childId);
      const parent = yield* store.getThreadProjection(parentId);
      assert.equal(parent.subagents.find((row) => row.id === ancestor.id)?.status, "running");
      assert.equal(parent.subagents.find((row) => row.id === ancestor.id)?.result, null);
      assert.equal(parent.nodes.find((row) => row.id === ancestor.id)?.status, "running");
      assert.equal(parent.turnItems.find((row) => row.id === ancestorItem.id)?.status, "running");
      const shell = yield* orchestrator.getThreadShell(parentId);
      assert.isTrue(
        shell?.pendingBackgroundTasks?.some(
          (row) => row.kind === "subagent" && row.childThreadId === childId,
        ) ?? false,
      );
    });
    const waitForAncestor = Effect.gen(function* () {
      const afterSequence = yield* sink.latestSequence();
      return yield* sink.stream({ threadId: parentId, afterSequence }).pipe(
        Stream.filter(
          (stored) =>
            stored.event.type === "subagent.updated" &&
            stored.event.payload.id === ancestor.id &&
            stored.event.payload.result !== null,
        ),
        Stream.runHead,
        Effect.forkScoped,
      );
    });
    const assertFinalized = (status: "completed" | "cancelled", text: string, resultRun: RunId) =>
      Effect.gen(function* () {
        assert.isFalse(yield* orchestrator.delegatedTaskResultPending(childId));
        const parent = yield* store.getThreadProjection(parentId);
        const finished = parent.subagents.find((row) => row.id === ancestor.id)!;
        assert.equal(finished.status, status);
        assert.equal(finished.result, text);
        assert.isNotNull(finished.completedAt);
        assert.equal(parent.nodes.find((row) => row.id === ancestor.id)?.status, status);
        const item = parent.turnItems.find((row) => row.id === ancestorItem.id)!;
        assert.equal(item.status, status);
        assert.equal(item.type === "subagent" ? item.result : null, text);
        assert.lengthOf(
          parent.contextTransfers.filter(
            (row) => row.type === "subagent_result" && row.sourcePoint.runId === resultRun,
          ),
          1,
        );
        assert.deepEqual(
          (yield* orchestrator.getThreadShell(parentId))!.pendingBackgroundTasks,
          [],
        );
      });
    return {
      orchestrator,
      outbox,
      now,
      parentId,
      childId,
      cohorts,
      later,
      putRun,
      seedRun,
      putMessage,
      putTask,
      run,
      message,
      projection,
      cancel,
      assertWaiting,
      waitForAncestor,
      assertFinalized,
    };
  });

it.effect(
  "Form: cancelling the held failure notice finalizes the ancestor from the last executed run",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* f.assertWaiting;
      const completion = yield* f.waitForAncestor;
      const cohort = f.cohorts[0]!;
      const accepted = yield* f.cancel(cohort.delivery);
      assert.isTrue(Option.isSome(yield* Fiber.join(completion)));
      const child = yield* f.projection();
      assert.equal(child.runs.find((row) => row.id === cohort.delivery.id)?.status, "cancelled");
      assert.equal(child.runs.find((row) => row.id === f.later.id)?.status, "failed");
      assert.equal(
        child.runs.find((row) => row.id === cohort.origin.id)?.delegatedCompletion?.disposition,
        "disposed",
      );
      assert.isTrue(child.subagents.every((row) => row.completionDelivery?.state === "disposed"));
      assert.lengthOf(
        accepted.storedEvents.filter(
          ({ event }) =>
            event.type === "run.updated" &&
            event.payload.id === cohort.delivery.id &&
            event.payload.status === "cancelled",
        ),
        1,
      );
      yield* f.assertFinalized("completed", "Original executed result", cohort.origin.id);
    }).pipe(Effect.provide(testLayer)),
);

it.effect.each(["cancelled_before_start", "cancelled_after_start"] as const)(
  "Revitalize: two held cohorts with a later %s run finalize only after both cancels",
  (later) =>
    Effect.gen(function* () {
      const f = yield* fixture({ cohorts: 2, later });
      yield* f.assertWaiting;
      const [first, second] = f.cohorts;
      yield* f.cancel(first!.delivery);
      const partial = yield* f.projection();
      assert.equal(partial.runs.find((row) => row.id === first!.delivery.id)?.status, "cancelled");
      assert.equal(partial.runs.find((row) => row.id === second!.delivery.id)?.status, "queued");
      assert.equal(
        partial.runs.find((row) => row.id === second!.origin.id)?.delegatedCompletion?.disposition,
        "open",
      );
      assert.isTrue(
        partial.subagents
          .filter((row) => row.runId === second!.origin.id)
          .every((row) => row.completionDelivery?.state === "claimed"),
      );
      yield* f.assertWaiting;
      const completion = yield* f.waitForAncestor;
      yield* f.cancel(second!.delivery);
      assert.isTrue(Option.isSome(yield* Fiber.join(completion)));
      const child = yield* f.projection();
      assert.isTrue(
        f.cohorts.every(
          (cohort) =>
            child.runs.find((row) => row.id === cohort.delivery.id)?.status === "cancelled",
        ),
      );
      assert.isTrue(child.subagents.every((row) => row.completionDelivery?.state === "disposed"));
      yield* f.assertFinalized(
        later === "cancelled_after_start" ? "cancelled" : "completed",
        later === "cancelled_after_start"
          ? "Child task ended with status cancelled."
          : "Child task completed without an assistant result.",
        later === "cancelled_after_start" ? f.later.id : second!.origin.id,
      );
    }).pipe(Effect.provide(testLayer)),
);

it.effect("cancelling a superseded held delivery also cancels the named run and its root", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    const cohort = f.cohorts[0]!;
    const replacement = f.run(f.childId, "replacement-notice", 4, "queued", false);
    yield* f.seedRun(replacement);
    yield* f.putMessage(
      f.message(replacement, "Replacement delivery", {
        parentRunId: cohort.origin.id,
        generation: 2,
        taskIds: cohort.tasks.map((row) => row.id),
      }),
    );
    yield* f.putRun({
      ...cohort.origin,
      delegatedCompletion: {
        disposition: "open",
        nextGeneration: 3,
        delivery: {
          generation: 2,
          messageId: replacement.userMessageId,
          taskIds: cohort.tasks.map((row) => row.id),
        },
      },
    });
    yield* f.assertWaiting;
    const completion = yield* f.waitForAncestor;
    const accepted = yield* f.cancel(cohort.delivery);
    assert.equal(
      (yield* f.projection()).runs.find((row) => row.id === cohort.delivery.id)?.status,
      "cancelled",
    );
    assert.isTrue(Option.isSome(yield* Fiber.join(completion)));
    const child = yield* f.projection();
    for (const selected of [cohort.delivery, replacement]) {
      assert.equal(child.runs.find((row) => row.id === selected.id)?.status, "cancelled");
      assert.equal(child.runs.find((row) => row.id === selected.id)?.queuePosition, null);
      assert.equal(child.nodes.find((row) => row.id === selected.rootNodeId)?.status, "cancelled");
      assert.lengthOf(
        accepted.storedEvents.filter(
          ({ event }) =>
            event.type === "run.updated" &&
            event.payload.id === selected.id &&
            event.payload.status === "cancelled",
        ),
        1,
      );
    }
    const replayed = yield* f.cancel(cohort.delivery);
    assert.isTrue(replayed.replayed);
    assert.equal(replayed.sequence, accepted.sequence);
    yield* f.assertFinalized("completed", "Original executed result", cohort.origin.id);
  }).pipe(Effect.provide(testLayer)),
);

it.effect.each([-8, 0])(
  "automatic delivery cancellation preserves a human held message aged %s days with identical notice text",
  (days) =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const human = {
        ...f.run(f.childId, "human-held", 4, "queued", false),
        requestedAt: DateTime.add(f.now, { days }),
      };
      yield* f.seedRun(human);
      const message = f.message(human, "Three delegated tasks failed");
      yield* f.putMessage(message);
      const completion = yield* f.waitForAncestor;
      yield* f.cancel(f.cohorts[0]!.delivery);
      assert.isTrue(Option.isSome(yield* Fiber.join(completion)));
      const child = yield* f.projection();
      assert.equal(child.runs.find((row) => row.id === human.id)?.status, "queued");
      assert.isTrue(child.runs.find((row) => row.id === human.id)?.queueHeld);
      assert.deepEqual(
        child.messages.find((row) => row.id === message.id),
        message,
      );
      yield* f.assertFinalized("completed", "Original executed result", f.cohorts[0]!.origin.id);
    }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "cancelling an old human notice lookalike does not dispose automatic completion duties",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const human = f.run(f.childId, "human-lookalike", 4, "queued", false);
      yield* f.seedRun(human);
      yield* f.putMessage(f.message(human, "Three delegated tasks failed"));
      yield* f.cancel(human);
      const child = yield* f.projection();
      const cohort = f.cohorts[0]!;
      assert.equal(child.runs.find((row) => row.id === human.id)?.status, "cancelled");
      assert.equal(child.runs.find((row) => row.id === cohort.delivery.id)?.status, "queued");
      assert.equal(
        child.runs.find((row) => row.id === cohort.origin.id)?.delegatedCompletion?.disposition,
        "open",
      );
      assert.isTrue(child.subagents.every((row) => row.completionDelivery?.state === "claimed"));
      yield* f.assertWaiting;
    }).pipe(Effect.provide(testLayer)),
);

it.effect.each([
  "runnable_notice",
  "live_grandchild",
  "restart_continuation",
  "failover",
  "outgoing_dependency",
] as const)("cancelling only automatic delivery preserves Waiting for %s", (duty) =>
  Effect.gen(function* () {
    const f = yield* fixture({
      initialStatus: duty === "failover" ? "failed" : "completed",
      later: duty === "failover" ? "cancelled_before_start" : "failed_before_start",
    });
    const cohort = f.cohorts[0]!;
    if (duty === "runnable_notice") {
      const runnable = {
        ...f.run(f.childId, "runnable-notice", 4, "queued", false),
        queueHeld: false,
      };
      yield* f.seedRun(runnable);
      yield* f.putMessage(
        f.message(runnable, "Three delegated tasks failed", {
          parentRunId: f.later.id,
          generation: 1,
          taskIds: [],
        }),
      );
    }
    if (duty === "live_grandchild") {
      yield* f.putTask({ ...cohort.tasks[0]!, status: "running", result: null, completedAt: null });
    }
    if (duty === "restart_continuation") {
      yield* f.outbox.enqueue([
        {
          id: `effect:restart-continuation:${f.later.id}`,
          commandId: CommandId.make("command:notice-restart"),
          threadId: f.childId,
          request: { type: "provider-runtime.continue", sourceRunId: f.later.id },
        },
      ]);
    }
    if (duty === "failover") {
      const store = yield* CodexFailoverStore.make;
      const sql = yield* SqlClient.SqlClient;
      const time = DateTime.formatIso(f.now);
      const providerTurnId = ProviderTurnId.make("turn:notice-failover");
      yield* sql`INSERT INTO jones_codex_failover_evidence
          (provider_turn_id, thread_id, run_id, attempt_id, provider_thread_id,
           provider_instance_id, disposition, occurred_at)
          VALUES (${providerTurnId}, ${f.childId}, ${cohort.origin.id},
            'attempt:notice-failover', 'provider-thread:notice-failover',
            ${instanceId}, 'usage_limit', ${time})`;
      assert.isTrue(
        yield* store.insertStep({
          switchCommandId: CommandId.make("jones:codex-failover:notice-switch"),
          threadId: f.childId,
          chainRootRunId: cohort.origin.id,
          originRunId: cohort.origin.id,
          originProviderTurnId: providerTurnId,
          sourceInstanceId: instanceId,
          sourceFailure: "usage_limit",
          sourceResetAt: null,
          targetInstanceId: ProviderInstanceId.make("codex_next"),
          savedSelection: modelSelection,
          continuationSelection: {
            ...modelSelection,
            instanceId: ProviderInstanceId.make("codex_next"),
          },
          continueCommandId: CommandId.make("jones:codex-failover:notice-continue"),
          continueMessageId: MessageId.make("message:notice-continue"),
          status: "switched",
          createdAt: time,
          updatedAt: time,
        }),
      );
      assert.isTrue(
        yield* store.markEvidence(providerTurnId, {
          state: "decided",
          decision: "switched",
          updatedAt: time,
        }),
      );
    }
    if (duty === "outgoing_dependency") {
      const sql = yield* SqlClient.SqlClient;
      const time = DateTime.formatIso(f.now);
      yield* sql`INSERT INTO jones_thread_result_dependencies
          (dependency_id, waiter_thread_id, waiter_run_id, target_thread_id, target_label, operation_id, state, observation_json, changed_sequence, created_at, updated_at)
          VALUES ('dependency:notice-duty', ${f.childId}, ${cohort.origin.id}, ${f.parentId}, 'Synthetic target', 'operation:notice-duty', 'awaiting', ${JSON.stringify("receiver_working")}, 0, ${time}, ${time})`;
      const dependencies = makeDependencyStore(sql, makeJonesTablePresence(sql));
      assert.equal((yield* dependencies.duties(f.childId)).outgoing, 1);
    }
    yield* f.assertWaiting;
    yield* f.cancel(cohort.delivery);
    yield* f.assertWaiting;
    const child = yield* f.projection();
    assert.equal(child.runs.find((row) => row.id === cohort.delivery.id)?.status, "cancelled");
    assert.isTrue(child.subagents.every((row) => row.completionDelivery?.state === "disposed"));
    if (duty === "live_grandchild") assert.equal(child.subagents[0]?.status, "running");
    if (duty === "runnable_notice") {
      assert.isTrue(
        child.runs.some(
          (row) =>
            row.userMessageId === MessageId.make("message:runnable-notice") &&
            (row.status === "queued" || row.status === "starting") &&
            !row.queueHeld,
        ),
      );
    }
    if (duty === "restart_continuation") {
      assert.equal(
        (yield* f.outbox.get(`effect:restart-continuation:${f.later.id}`)).pipe(
          Option.map((row) => row.status),
          Option.getOrNull,
        ),
        "pending",
      );
    }
  }).pipe(Effect.provide(testLayer)),
);

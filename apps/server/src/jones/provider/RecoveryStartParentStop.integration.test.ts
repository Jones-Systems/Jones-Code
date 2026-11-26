import { assert, it } from "@effect/vitest";
import {
  CheckpointScopeId,
  CommandId,
  EnvironmentId,
  EventId,
  MessageId,
  NodeId,
  type OrchestrationV2AppThread,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2Run,
  type OrchestrationV2RunAttempt,
  type OrchestrationV2Subagent,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  RunAttemptId,
  RunId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";
import { vi } from "vite-plus/test";

import * as Environment from "../../environment/ServerEnvironment.ts";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as Receipts from "../../orchestration-v2/CommandReceiptStore.ts";
import * as Outbox from "../../orchestration-v2/EffectOutbox.ts";
import * as Worker from "../../orchestration-v2/EffectWorker.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as Projections from "../../orchestration-v2/ProjectionStore.ts";
import * as Registry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as Recovery from "../../orchestration-v2/ProviderRuntimeRecoveryService.ts";
import * as Replay from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as Sqlite from "../../persistence/Sqlite.ts";
import * as Goals from "../goals/continuity/GoalContinuity.ts";
import * as NativeStartAdmission from "../nativeCreation/NativeStartAdmission.ts";
import * as Control from "../orchestration/ControlAdmission.ts";
import * as Restart from "../orchestration/RestartPreparation.ts";
import { pruneEventLogLikeUpstream } from "../persistence/prunedEventLog.testkit.ts";
import * as Refusals from "../startRefusal/Store.ts";
import * as RecoveryStartDenial from "./RecoveryStartDenial.ts";
import { StartAdmission } from "./StartAdmission.ts";
import * as UnattendedStartAdmission from "./UnattendedStartAdmission.ts";

const providerInstanceId = ProviderInstanceId.make("codex");
const driver = ProviderDriverKind.make("codex");
const database = Sqlite.layerMemory;
const openSession = vi.fn(() => Effect.die("A denied start must not reach the adapter."));
const PublicationGate = Context.Reference<
  | {
      readonly started: Deferred.Deferred<void>;
      readonly release: Deferred.Deferred<void>;
    }
  | undefined
>("test/RecoveryStartParentStop/PublicationGate", { defaultValue: () => undefined });
const eventStoreLayer = Layer.effect(
  EventStore.EventStoreV2,
  Effect.map(EventStore.EventStoreV2, (store) =>
    EventStore.EventStoreV2.of({
      ...store,
      publishCommitted: (events) =>
        Effect.gen(function* () {
          const gate = yield* PublicationGate;
          if (gate !== undefined) {
            yield* Deferred.succeed(gate.started, undefined);
            yield* Deferred.await(gate.release);
          }
          yield* store.publishCommitted(events);
        }),
    }),
  ),
).pipe(Layer.provide(EventStore.layer));
const replay = Replay.layerWithRegistryAndServices(
  { name: "recovery-parent-stop" },
  Registry.layerFromAdapters([
    {
      instanceId: providerInstanceId,
      driver,
      getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
      planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
      openSession,
    },
  ]),
  {
    databaseLayer: database,
    eventStoreLayer,
    runEffectWorker: false,
    continueThreadsAfterServerUpdate: true,
  },
);
const runtime = Layer.mergeAll(
  Recovery.layer,
  NativeStartAdmission.layer,
  RecoveryStartDenial.layer,
).pipe(
  Layer.provide(UnattendedStartAdmission.layer),
  Layer.provideMerge(
    Layer.mergeAll(
      replay,
      IdAllocator.layer,
      Layer.mergeAll(Receipts.layer, Refusals.layer, Control.layer).pipe(Layer.provide(database)),
      Layer.mock(Restart.RestartPreparation)({ assertThreadOpen: () => Effect.void }),
      Layer.mock(Goals.GoalContinuity)({
        context: () => {
          throw new Error("Unexpected goal continuity context in unattended recovery.");
        },
        requirementFor: () =>
          Effect.succeed({ outcome: "absent", gate: { outcome: "clear", fenceRevision: "0" } }),
      }),
      Layer.succeed(Environment.ServerEnvironmentIdentity, {
        getEnvironmentId: Effect.succeed(EnvironmentId.make("recovery-parent-stop")),
      }),
    ),
  ),
);

const parentId = ThreadId.make("thread:recovery-parent");
const childId = ThreadId.make("thread:recovery-child");
const otherId = ThreadId.make("thread:independent-sender");
const taskId = NodeId.make("node:recovery-task");
const originalTaskId = NodeId.make("node:original-task");
const eventId = (name: string) => EventId.make(`event:recovery-parent-stop:${name}`);
const time = Date.parse("2026-11-19T12:00:00Z");

const fixture = Effect.fnUntraced(function* (
  source: "delegated" | "independent" | "ambiguous" | "wrong-child",
  queued: boolean,
  stopHasEvent: boolean,
) {
  yield* TestClock.setTime(time);
  const now = yield* DateTime.now;
  const sink = yield* EventSink.EventSinkV2;
  const modelSelection = { instanceId: providerInstanceId, model: "test" };
  const actor = { createdBy: "agent", creationSource: "mcp" } as const;
  const parent: OrchestrationV2AppThread = {
    ...actor,
    id: parentId,
    projectId: ProjectId.make("project:recovery-parent-stop"),
    title: "Parent",
    providerInstanceId,
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: parentId },
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  };
  const provider: OrchestrationV2ProviderThread = {
    id: ProviderThreadId.make("provider:recovery-child"),
    appThreadId: childId,
    ownerNodeId: null,
    providerInstanceId,
    providerSessionId: ProviderSessionId.make("session:recovery-child"),
    driver,
    nativeThreadRef: null,
    nativeConversationHeadRef: null,
    status: "not_loaded",
    firstRunOrdinal: 1,
    lastRunOrdinal: 1,
    handoffIds: [],
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
  };
  const child: OrchestrationV2AppThread = {
    ...parent,
    id: childId,
    title: "Child",
    activeProviderThreadId: provider.id,
    lineage: { parentThreadId: parentId, relationshipToParent: "subagent", rootThreadId: parentId },
    forkedFrom: { type: "node", nodeId: originalTaskId },
  };
  const run: OrchestrationV2Run = {
    id: RunId.make("run:recovery-child"),
    threadId: childId,
    ordinal: 1,
    providerInstanceId,
    modelSelection,
    providerThreadId: provider.id,
    userMessageId: MessageId.make("message:recovery-child"),
    rootNodeId: NodeId.make("node:recovery-child"),
    activeAttemptId: RunAttemptId.make("attempt:recovery-child"),
    status: "starting",
    queuePosition: null,
    requestedAt: now,
    startedAt: null,
    completedAt: null,
    checkpointId: null,
    contextHandoffId: null,
    ...(source === "delegated" || source === "wrong-child" ? { delegatedTaskId: taskId } : {}),
  };
  const attempt: OrchestrationV2RunAttempt = {
    id: run.activeAttemptId!,
    runId: run.id,
    attemptOrdinal: 1,
    rootNodeId: run.rootNodeId!,
    providerInstanceId,
    providerThreadId: provider.id,
    providerTurnId: null,
    reason: "initial",
    status: "pending",
    startedAt: null,
    completedAt: null,
  };
  const root: OrchestrationV2ExecutionNode = {
    id: attempt.rootNodeId,
    threadId: childId,
    runId: run.id,
    parentNodeId: null,
    rootNodeId: attempt.rootNodeId,
    kind: "root_turn",
    status: "pending",
    countsForRun: true,
    providerThreadId: provider.id,
    providerTurnId: null,
    nativeItemRef: null,
    runtimeRequestId: null,
    checkpointScopeId: CheckpointScopeId.make("scope:recovery-child"),
    startedAt: null,
    completedAt: null,
  };
  const owner: OrchestrationV2Run = {
    ...run,
    id: RunId.make("run:recovery-parent"),
    threadId: parentId,
    userMessageId: MessageId.make("message:recovery-parent"),
    providerThreadId: null,
    rootNodeId: null,
    activeAttemptId: null,
    status: "completed",
    startedAt: now,
    completedAt: now,
    delegatedTaskId: undefined,
  };
  const task: OrchestrationV2Subagent = {
    id: taskId,
    threadId: parentId,
    childThreadId: source === "wrong-child" ? otherId : childId,
    runId: owner.id,
    parentNodeId: NodeId.make("node:recovery-parent"),
    origin: "app_owned",
    createdBy: "agent",
    driver,
    providerInstanceId,
    providerThreadId: null,
    nativeTaskRef: null,
    prompt: "Continue the delegated work",
    title: null,
    model: modelSelection.model,
    status: "running",
    result: null,
    startedAt: now,
    completedAt: null,
    updatedAt: now,
    completionDelivery: { state: "disposed", observedByRunId: null },
  };
  const parentEvent = { threadId: parentId, occurredAt: now, providerInstanceId };
  const childEvent = { threadId: childId, occurredAt: now, providerInstanceId };
  yield* sink.write({
    events: [
      { ...parentEvent, id: eventId("parent"), type: "thread.created", payload: parent },
      { ...parentEvent, id: eventId("owner"), type: "run.created", payload: owner },
      ...(stopHasEvent
        ? [
            {
              ...parentEvent,
              id: eventId("parent-queue"),
              type: "run.created" as const,
              payload: {
                ...owner,
                id: RunId.make("run:parent-queued"),
                ordinal: 2,
                status: "queued" as const,
                queuePosition: 1,
                startedAt: null,
                completedAt: null,
              },
            },
          ]
        : []),
      {
        ...parentEvent,
        id: eventId("original-task"),
        type: "subagent.updated",
        payload: {
          ...task,
          id: originalTaskId,
          childThreadId: childId,
          status: "completed",
          result: "Original task finished",
          completedAt: now,
        },
      },
      { ...childEvent, id: eventId("child"), type: "thread.created", payload: child },
      {
        ...childEvent,
        id: eventId("provider"),
        type: "provider-thread.updated",
        payload: provider,
      },
    ],
  });
  const message: OrchestrationV2ConversationMessage = {
    ...actor,
    ...(source === "ambiguous"
      ? {}
      : { senderThreadId: source === "independent" ? otherId : childId }),
    id: run.userMessageId,
    threadId: childId,
    runId: run.id,
    nodeId: run.rootNodeId,
    role: "user",
    text: "Continue after the restart",
    attachments: [],
    streaming: false,
    createdAt: now,
    updatedAt: now,
  };
  const commandId = CommandId.make("command:recovery-child:send");
  const startCommandId = queued ? CommandId.make("command:recovery-child:promote") : commandId;
  const originalId = `effect:${startCommandId}:provider-turn.start:${run.id}`;
  const original = {
    id: originalId,
    commandId: startCommandId,
    threadId: childId,
    request: { type: "provider-turn.start", runId: run.id },
  } satisfies Outbox.PendingOrchestrationEffectV2;
  const runEvent = { ...childEvent, runId: run.id };
  const send = Effect.gen(function* () {
    yield* sink.write({
      events: [
        { ...parentEvent, id: eventId("follow-up-task"), type: "subagent.updated", payload: task },
      ],
    });
    yield* sink.commitCommand({
      commandId,
      commandType: "message.dispatch",
      threadId: childId,
      acceptedAt: now,
      events: [
        {
          ...runEvent,
          id: eventId("start"),
          type: "run.created",
          payload: queued ? { ...run, status: "queued", queuePosition: 1 } : run,
        },
        { ...runEvent, id: eventId("attempt"), type: "run-attempt.created", payload: attempt },
        { ...runEvent, id: eventId("root"), type: "node.updated", payload: root },
        {
          ...runEvent,
          id: eventId("scope"),
          type: "checkpoint-scope.created",
          payload: {
            id: root.checkpointScopeId!,
            threadId: childId,
            runId: run.id,
            nodeId: root.id,
            parentScopeId: null,
            providerThreadId: provider.id,
            kind: "root_run",
            ordinalWithinParent: 1,
            advancesAppRunCount: true,
            cwd: process.cwd(),
            createdAt: now,
          },
        },
        { ...runEvent, id: eventId("input"), type: "message.updated", payload: message },
      ],
      effects: queued ? [] : [original],
    });
  });
  const promote = sink.writeWithEffects({
    commandId: startCommandId,
    queuePromotion: { commandId: startCommandId, threadId: childId, runId: run.id },
    events: [
      {
        ...runEvent,
        id: eventId("promotion-input"),
        type: "turn-item.updated",
        payload: {
          ...actor,
          id: TurnItemId.make("item:recovery-child"),
          threadId: childId,
          runId: run.id,
          nodeId: run.rootNodeId,
          providerThreadId: provider.id,
          providerTurnId: null,
          nativeItemRef: null,
          parentItemId: null,
          ordinal: 1,
          status: "completed",
          title: null,
          startedAt: now,
          completedAt: now,
          updatedAt: now,
          type: "user_message",
          messageId: message.id,
          text: message.text,
          attachments: message.attachments,
          inputIntent: "queued_turn",
        },
      },
      { ...runEvent, id: eventId("promotion"), type: "run.updated", payload: run },
    ],
    effects: [original],
  });
  const stopCommandId = CommandId.make("command:recovery-parent:stop");
  const stopEffectId = `effect:${stopCommandId}:delegated-tasks.stop`;
  const stopWorkerId = "parked-parent-stop";
  const outbox = yield* Outbox.EffectOutboxV2;
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const recordStop = Effect.gen(function* () {
    // A repeated idle parent Stop can enqueue the fan-out with no new parent event.
    const stopped = yield* orchestrator.dispatch({
      type: "thread.stop",
      commandId: stopCommandId,
      threadId: parentId,
    });
    assert.equal(stopped.storedEvents.length > 0, stopHasEvent);
  });
  const parkStop = Effect.gen(function* () {
    const parked = yield* outbox.claimNext({ workerId: stopWorkerId, urgentOnly: true });
    assert.deepInclude(Option.getOrThrow(parked), { id: stopEffectId, status: "running" });
  });
  return {
    run,
    attempt,
    provider,
    send,
    promote,
    recordStop,
    parkStop,
    stopEffectId,
    stopWorkerId,
    originalId,
  };
});

const updateChildTitle = Effect.fnUntraced(function* () {
  const sink = yield* EventSink.EventSinkV2;
  const projections = yield* Projections.ProjectionStoreV2;
  const child = (yield* projections.getThread(childId))!;
  return yield* sink.write({
    events: [
      {
        id: eventId("child-title"),
        threadId: childId,
        type: "thread.metadata-updated",
        occurredAt: yield* DateTime.now,
        payload: { ...child, title: "Updated child title" },
      },
    ],
  });
});

it.effect.each([
  { name: "an unreadable retired integration effect", evidence: "foreign", settles: true },
  { name: "an unreadable restart for another run", evidence: "other-run", settles: true },
  { name: "an unreadable restart for this run", evidence: "same-run", settles: false },
  { name: "an unattributable restart", evidence: "unknown-run", settles: false },
  { name: "a missing reconciliation receipt", evidence: "missing-receipt", settles: false },
  { name: "a pruned reconciliation event", evidence: "pruned-receipt-events", settles: false },
] as const)("keeps claims live with $name", ({ evidence, settles }) =>
  Effect.gen(function* () {
    const f = yield* fixture("delegated", false, false);
    const outbox = yield* Outbox.EffectOutboxV2;
    const projections = yield* Projections.ProjectionStoreV2;
    const recovery = yield* Recovery.ProviderRuntimeRecoveryService;
    const recoveryStartDenial = yield* RecoveryStartDenial.RecoveryStartDenial;
    const receipts = yield* Receipts.CommandReceiptStoreV2;
    const sql = yield* SqlClient.SqlClient;
    yield* f.send;
    yield* TestClock.setTime(time + 1);
    assert.equal((yield* recovery.reconcile("startup")).terminalizedRuns, 0);
    const replacement = (yield* outbox.listByThreadId(childId)).find(
      (row) => row.status === "pending",
    )!;
    const receiptId = CommandId.make(
      replacement.id.slice(`effect:restart-resend:${f.run.id}:`.length),
    );
    const receipt = Option.getOrThrow(yield* receipts.getByCommandId(receiptId));
    const retiredId = "effect:retained-unreadable";
    const retiredRow = sql`SELECT * FROM orchestration_v2_effect_outbox WHERE effect_id = ${retiredId}`;
    if (evidence === "missing-receipt") {
      yield* sql`DELETE FROM orchestration_command_receipts WHERE command_id = ${receiptId}`;
    } else if (evidence === "pruned-receipt-events") {
      yield* updateChildTitle();
      assert.include(
        yield* pruneEventLogLikeUpstream(receipt.resultSequence),
        receipt.resultSequence,
      );
      assert.deepEqual(Option.getOrThrow(yield* receipts.getByCommandId(receiptId)), receipt);
    } else {
      const effectType =
        evidence === "foreign" ? "fixture.retired-integration" : "provider-turn.restart";
      const payload = {
        type: effectType,
        ...(evidence === "other-run" ? { runId: "run:retired" } : {}),
        ...(evidence === "same-run" ? { runId: f.run.id } : {}),
      };
      yield* sql`INSERT INTO orchestration_v2_effect_outbox
        (effect_id, command_id, thread_id, effect_type, payload_json, status, attempt_count,
         available_at, created_at, updated_at, completed_at)
        SELECT ${retiredId}, ${CommandId.make("command:retained-unreadable")}, thread_id,
          ${effectType}, ${evidence === "unknown-run" ? "{" : JSON.stringify(payload)},
          'cancelled', 0, available_at, created_at, updated_at, completed_at
        FROM orchestration_v2_effect_outbox WHERE effect_id = ${f.originalId}`;
    }
    const retiredBefore = yield* retiredRow;
    yield* f.recordStop;
    yield* f.parkStop;
    yield* TestClock.setTime(time + 2);
    const job = {
      id: "effect:ordinary-cleanup",
      commandId: CommandId.make("command:ordinary-cleanup"),
      threadId: childId,
      request: { type: "terminal.cleanup" },
    } satisfies Outbox.PendingOrchestrationEffectV2;
    yield* outbox.enqueue([job]);
    const workerId = "lineage-test";
    const poll = outbox.claimNext({ workerId, recoveryStartDenial }).pipe(Effect.result);
    const first = yield* poll;
    const second = yield* poll;
    const child = yield* projections.getThreadProjection(childId);
    assert.deepEqual(
      {
        claims: [first, second].map((entry) =>
          Result.isSuccess(entry)
            ? Option.getOrUndefined(entry.success)?.id
            : entry.failure.operation,
        ),
        replacement: Option.getOrThrow(yield* outbox.get(replacement.id)).status,
        job: Option.getOrThrow(yield* outbox.get(job.id)).status,
        run: child.runs.find(({ id }) => id === f.run.id)?.status,
        attempt: child.attempts.find(({ id }) => id === f.attempt.id)?.status,
        root: child.nodes.find(({ id }) => id === f.run.rootNodeId)?.status,
        notices: child.turnItems.filter((item) => item.title === "Recovered turn did not start")
          .length,
      },
      {
        claims: [job.id, undefined],
        replacement: "failed",
        job: "running",
        run: settles ? "failed" : "starting",
        attempt: settles ? "failed" : "pending",
        root: settles ? "failed" : "pending",
        notices: settles ? 1 : 0,
      },
    );
    assert.deepInclude(Option.getOrThrow(yield* outbox.get(replacement.id)), {
      attemptCount: 0,
      leaseOwner: null,
    });
    assert.isEmpty(child.providerTurns);
    assert.deepEqual(yield* retiredRow, retiredBefore);
    assert.isTrue(yield* outbox.succeed({ effectId: job.id, workerId }));
  }).pipe(Effect.provide(runtime)),
);

it.effect.each(["ordinary", "empty"] as const)(
  "returns a %s claim during deferred event publication",
  (kind) =>
    Effect.gen(function* () {
      yield* fixture("delegated", false, false);
      const outbox = yield* Outbox.EffectOutboxV2;
      const recoveryStartDenial = yield* RecoveryStartDenial.RecoveryStartDenial;
      const gate = { started: yield* Deferred.make<void>(), release: yield* Deferred.make<void>() };
      const jobId = "effect:publication-test-cleanup";
      if (kind === "ordinary")
        yield* outbox.enqueue([
          {
            id: jobId,
            commandId: CommandId.make("command:publication-test-cleanup"),
            threadId: parentId,
            request: { type: "terminal.cleanup" },
          },
        ]);
      const publication = yield* updateChildTitle().pipe(
        Effect.provideService(PublicationGate, gate),
        Effect.forkChild({ startImmediately: true }),
      );
      const done = yield* Deferred.make<string | undefined>();
      const { observed, claim } = yield* Effect.gen(function* () {
        yield* Deferred.await(gate.started);
        const claim = yield* outbox
          .claimNext({ workerId: "publication-test", recoveryStartDenial })
          .pipe(
            Effect.map((row) => Option.getOrUndefined(row)?.id),
            Effect.tap((id) => Deferred.succeed(done, id)),
            Effect.forkChild,
          );
        const withinPublication = yield* Deferred.await(done).pipe(
          Effect.timeoutOption("1 second"),
          Effect.forkChild({ startImmediately: true }),
        );
        yield* TestClock.adjust("1 second");
        return { observed: yield* Fiber.join(withinPublication), claim };
      }).pipe(Effect.ensuring(Deferred.succeed(gate.release, undefined)));
      yield* Fiber.join(publication);
      const expected = kind === "ordinary" ? jobId : undefined;
      assert.equal(yield* Fiber.join(claim), expected);
      assert.deepEqual(observed, Option.some(expected));
    }).pipe(Effect.provide(runtime)),
);

it.effect.each([
  { name: "Stop before claim", source: "delegated", stop: "before claim", attempts: 0, sends: 0 },
  {
    name: "a failed Stop fan-out and an interrupted denial write",
    source: "delegated",
    stop: "failed fan-out",
    attempts: 0,
    sends: 0,
  },
  {
    name: "a previously leased replacement",
    source: "delegated",
    stop: "after prior lease",
    attempts: 1,
    sends: 0,
  },
  { name: "Stop after capture", source: "delegated", stop: "after capture", attempts: 1, sends: 0 },
  { name: "ordinary recovery", source: "delegated", stop: "none", attempts: 1, sends: 1 },
  { name: "a newer send", source: "delegated", stop: "before send", attempts: 1, sends: 1 },
  {
    name: "an independent sender",
    source: "independent",
    stop: "before claim",
    attempts: 1,
    sends: 1,
  },
  {
    name: "an ambiguous old sender",
    source: "ambiguous",
    stop: "before claim",
    attempts: 0,
    sends: 0,
  },
  {
    name: "a task for a different child",
    source: "wrong-child",
    stop: "none",
    attempts: 0,
    sends: 0,
  },
  {
    name: "an old send promoted after Stop",
    source: "delegated",
    stop: "before promotion",
    attempts: 0,
    sends: 0,
  },
  {
    name: "a queue admission after a recorded Stop event",
    source: "delegated",
    stop: "before queued send",
    attempts: 1,
    sends: 1,
  },
] as const)("gates a recovered start for $name", ({ source, stop, attempts, sends }) =>
  Effect.gen(function* () {
    const queued = stop === "before promotion" || stop === "before queued send";
    const f = yield* fixture(source, queued, stop === "before queued send");
    const outbox = yield* Outbox.EffectOutboxV2;
    const projections = yield* Projections.ProjectionStoreV2;
    const recovery = yield* Recovery.ProviderRuntimeRecoveryService;
    const recoveryStartDenial = yield* RecoveryStartDenial.RecoveryStartDenial;
    const admission = yield* StartAdmission;
    const realExecutor = yield* Worker.OrchestrationEffectExecutorV2;
    const captured = yield* Deferred.make<void>();
    const beforeNative = yield* Deferred.make<void>();
    const opensBefore = openSession.mock.calls.length;
    let nativeSends = 0;
    if (stop === "before send" || stop === "before queued send") yield* f.recordStop;
    yield* f.send;
    if (stop === "before promotion") yield* f.recordStop;
    if (queued) yield* f.promote;
    yield* TestClock.setTime(time + 1);
    assert.equal((yield* recovery.reconcile("startup")).terminalizedRuns, 0);
    const recoveryRows = yield* outbox.listByThreadId(childId);
    const replacement = recoveryRows.find((row) => row.status === "pending");
    assert.isDefined(replacement);
    assert.deepInclude(
      recoveryRows.find((row) => row.id === f.originalId),
      {
        status: "cancelled",
        attemptCount: 0,
      },
    );
    if (stop === "after prior lease") {
      const workerId = "prior-lease";
      const claim = Option.getOrThrow(yield* outbox.claimNext({ workerId }));
      assert.equal(claim.id, replacement!.id);
      assert.isTrue(
        yield* outbox.retry({ effectId: claim.id, workerId, claim, error: "fixture", delayMs: 0 }),
      );
    }
    if (stop === "before claim" || stop === "failed fan-out" || stop === "after prior lease")
      yield* f.recordStop;
    if (stop !== "none" && stop !== "after capture") yield* f.parkStop;
    if (stop === "failed fan-out")
      assert.isTrue(
        yield* outbox.fail({
          effectId: f.stopEffectId,
          workerId: f.stopWorkerId,
          error: "The fixture blocked fan-out.",
        }),
      );
    const executor = Layer.succeed(Worker.OrchestrationEffectExecutorV2, {
      execute: (effect, options) =>
        Effect.gen(function* () {
          assert.equal(effect.id, replacement!.id);
          const token = yield* admission.capture({
            threadId: childId,
            run: f.run,
            attemptId: f.attempt.id,
            providerThread: f.provider,
          });
          if (stop === "after capture") {
            yield* Deferred.succeed(captured, undefined);
            yield* Deferred.await(beforeNative);
            const refusal = yield* token.revalidate.pipe(Effect.flip);
            assert.include(refusal.message, "no longer has current execution authority");
            return yield* realExecutor.execute(effect, options);
          }
          yield* token.revalidate;
          nativeSends++;
        }).pipe(
          Effect.mapError(
            (cause) =>
              new Worker.OrchestrationEffectExecutionError({
                effectType: effect.request.type,
                effectId: effect.id,
                cause,
              }),
          ),
        ),
    });
    yield* Effect.gen(function* () {
      const worker = yield* Worker.OrchestrationEffectWorkerV2;
      if (stop === "failed fan-out") {
        const sql = yield* SqlClient.SqlClient;
        const sink = yield* EventSink.EventSinkV2;
        const sequence = yield* sink.latestSequence({ threadId: childId });
        yield* sql`CREATE TEMP TRIGGER reject_recovery_denial
          BEFORE UPDATE OF status ON orchestration_v2_projection_runs WHEN NEW.status = 'failed'
          BEGIN SELECT RAISE(ABORT, 'fixture could not persist run denial'); END`;
        assert.equal((yield* Effect.exit(worker.runOnce()))._tag, "Failure");
        assert.deepInclude(Option.getOrThrow(yield* outbox.get(replacement!.id)), {
          status: "pending",
          attemptCount: 0,
          completedAt: null,
        });
        assert.equal((yield* projections.getRun(childId, f.run.id))?.status, "starting");
        assert.equal(yield* sink.latestSequence({ threadId: childId }), sequence);
        yield* sql`DROP TRIGGER reject_recovery_denial`;
      }
      if (stop === "after capture") {
        const dispatch = yield* worker.runOnce().pipe(Effect.forkChild);
        yield* Effect.raceFirst(
          Deferred.await(captured),
          Fiber.join(dispatch).pipe(
            Effect.andThen(Effect.die("The worker settled before reaching start admission.")),
          ),
        );
        assert.deepInclude(Option.getOrThrow(yield* outbox.get(replacement!.id)), {
          status: "running",
          attemptCount: 1,
        });
        yield* f.recordStop;
        yield* f.parkStop;
        yield* Deferred.succeed(beforeNative, undefined);
        assert.isTrue(yield* Fiber.join(dispatch));
        assert.deepInclude(Option.getOrThrow(yield* outbox.get(replacement!.id)), {
          status: "succeeded",
          attemptCount: 1,
        });
      } else {
        assert.equal(yield* worker.runOnce(), sends > 0);
      }
      yield* TestClock.setTime(time + 60_000);
      assert.isFalse(yield* worker.runOnce());
    }).pipe(
      Effect.provide(
        Worker.layerWithOptions({ workerId: "recovered-start-test" }).pipe(
          Layer.provide(executor),
          Layer.provide(
            Layer.succeed(RecoveryStartDenial.RecoveryStartDenial, recoveryStartDenial),
          ),
        ),
      ),
    );
    assert.deepInclude(Option.getOrThrow(yield* outbox.get(replacement!.id)), {
      status: sends > 0 || stop === "after capture" ? "succeeded" : "failed",
      attemptCount: attempts,
      leaseOwner: null,
    });
    assert.equal(openSession.mock.calls.length, opensBefore);
    assert.equal(nativeSends, sends);
    const fanOutStatus = stop === "failed fan-out" ? "failed" : "running";
    if (stop !== "none") {
      assert.equal(Option.getOrThrow(yield* outbox.get(f.stopEffectId)).status, fanOutStatus);
      assert.isFalse(yield* projections.hasRunInterruptRequest(childId, f.run.id));
    }
    if (sends === 0 && stop !== "after prior lease") {
      const child = yield* projections.getThreadProjection(childId);
      assert.deepInclude(
        child.runs.find((run) => run.id === f.run.id),
        {
          status: "failed",
          startedAt: null,
        },
      );
      const attempt = child.attempts.find((attempt) => attempt.id === f.attempt.id)!;
      assert.deepInclude(attempt, { status: "failed", startedAt: null, providerTurnId: null });
      assert.isNull(attempt.providerSettlement ?? null);
      assert.deepInclude(
        child.nodes.find((node) => node.id === f.run.rootNodeId),
        {
          status: "failed",
          startedAt: null,
          providerTurnId: null,
        },
      );
      assert.isEmpty(child.providerTurns);
      assert.isTrue(
        child.turnItems.some(
          (item) =>
            item.runId === f.run.id &&
            item.type === "error" &&
            item.title ===
              (stop === "after capture" ? "Turn failed to start" : "Recovered turn did not start"),
        ),
      );
    }
    if (stop === "after prior lease") {
      const child = yield* projections.getThreadProjection(childId);
      assert.deepInclude(
        child.runs.find((run) => run.id === f.run.id),
        { status: "starting" },
      );
      assert.deepInclude(
        child.attempts.find((attempt) => attempt.id === f.attempt.id),
        {
          status: "pending",
          providerTurnId: null,
        },
      );
      assert.isEmpty(child.providerTurns);
    }
    if (source === "delegated" && (stop === "before claim" || stop === "failed fan-out")) {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const laterCommandId = CommandId.make("command:recovery-child:later-user-message");
      const laterMessageId = MessageId.make("message:recovery-child:later-user-message");
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: laterCommandId,
        threadId: childId,
        messageId: laterMessageId,
        text: "Start a new turn after the parent Stop",
        attachments: [],
        dispatchMode: { type: "queue_after_active" },
        createdBy: "user",
        creationSource: "web",
      });
      const child = yield* projections.getThreadProjection(childId);
      assert.deepEqual(
        {
          recoveryRun: child.runs.find((run) => run.id === f.run.id)?.status,
          recoveryAttempt: child.attempts.find((attempt) => attempt.id === f.attempt.id)?.status,
          recoveryRoot: child.nodes.find((node) => node.id === f.run.rootNodeId)?.status,
          laterRun: child.runs.find((run) => run.userMessageId === laterMessageId)?.status,
          laterEffects: (yield* outbox.listByCommandId(laterCommandId)).filter(
            (effect) => effect.request.type === "provider-turn.start",
          ).length,
        },
        {
          recoveryRun: "failed",
          recoveryAttempt: "failed",
          recoveryRoot: "failed",
          laterRun: "starting",
          laterEffects: 1,
        },
      );
      assert.equal(Option.getOrThrow(yield* outbox.get(f.stopEffectId)).status, fanOutStatus);
      const parentCommandId = CommandId.make("command:recovery-parent:later-user-message");
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: parentCommandId,
        threadId: parentId,
        messageId: MessageId.make("message:recovery-parent:later-user-message"),
        text: "Continue the parent thread",
        attachments: [],
        dispatchMode: { type: "queue_after_active" },
        createdBy: "user",
        creationSource: "web",
      });
      assert.deepInclude(
        (yield* outbox.listByCommandId(parentCommandId)).find(
          (effect) => effect.request.type === "provider-turn.start",
        ),
        { status: "pending", attemptCount: 0 },
      );
    }
  }).pipe(Effect.provide(runtime)),
);

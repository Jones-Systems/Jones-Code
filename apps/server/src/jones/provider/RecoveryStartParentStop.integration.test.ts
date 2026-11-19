import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  EventId,
  MessageId,
  NodeId,
  type OrchestrationV2AppThread,
  type OrchestrationV2ConversationMessage,
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
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as TestClock from "effect/testing/TestClock";

import * as Environment from "../../environment/ServerEnvironment.ts";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as Receipts from "../../orchestration-v2/CommandReceiptStore.ts";
import * as Outbox from "../../orchestration-v2/EffectOutbox.ts";
import * as Worker from "../../orchestration-v2/EffectWorker.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
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
import * as Refusals from "../startRefusal/Store.ts";
import { StartAdmission } from "./StartAdmission.ts";
import * as UnattendedStartAdmission from "./UnattendedStartAdmission.ts";

const providerInstanceId = ProviderInstanceId.make("codex");
const driver = ProviderDriverKind.make("codex");
const database = Sqlite.layerMemory;
const replay = Replay.layerWithRegistryAndServices(
  { name: "recovery-parent-stop" },
  Registry.layerFromAdapters([
    {
      instanceId: providerInstanceId,
      driver,
      getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
      planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
      openSession: () => Effect.die("The test observes the admitted dispatch in the worker."),
    },
  ]),
  { databaseLayer: database, runEffectWorker: false, continueThreadsAfterServerUpdate: true },
);
const runtime = Layer.merge(Recovery.layer, NativeStartAdmission.layer).pipe(
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
    const parked = yield* outbox.claimNext({ workerId: "parked-parent-stop", urgentOnly: true });
    assert.deepInclude(Option.getOrThrow(parked), { id: stopEffectId, status: "running" });
  });
  return { run, attempt, provider, send, promote, recordStop, parkStop, stopEffectId, originalId };
});

it.effect.each([
  { name: "Stop before claim", source: "delegated", stop: "before claim", attempts: 0, sends: 0 },
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
    const admission = yield* StartAdmission;
    const captured = yield* Deferred.make<void>();
    const beforeNative = yield* Deferred.make<void>();
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
    if (stop === "before claim") yield* f.recordStop;
    if (stop !== "none" && stop !== "after capture") yield* f.parkStop;
    const executor = Layer.succeed(Worker.OrchestrationEffectExecutorV2, {
      execute: (effect) =>
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
          status: "failed",
          attemptCount: 1,
        });
      } else {
        assert.equal(yield* worker.runOnce(), attempts > 0);
      }
      yield* TestClock.setTime(time + 60_000);
      assert.isFalse(yield* worker.runOnce());
    }).pipe(
      Effect.provide(
        Worker.layerWithOptions({ workerId: "recovered-start-test" }).pipe(Layer.provide(executor)),
      ),
    );
    assert.deepInclude(Option.getOrThrow(yield* outbox.get(replacement!.id)), {
      status: sends > 0 ? "succeeded" : "failed",
      attemptCount: attempts,
      leaseOwner: null,
    });
    assert.equal(nativeSends, sends);
    if (stop !== "none") {
      assert.equal(Option.getOrThrow(yield* outbox.get(f.stopEffectId)).status, "running");
      assert.isFalse(yield* projections.hasRunInterruptRequest(childId, f.run.id));
    }
  }).pipe(Effect.provide(runtime)),
);

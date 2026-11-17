import { assert, it } from "@effect/vitest";
import {
  CheckpointScopeId,
  CommandId,
  EventId,
  NodeId,
  MessageId,
  RunId,
  RuntimeRequestId,
  TurnItemId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2Run,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as IdleWorktreeAdmission from "../jones/orchestration/IdleWorktreeAdmission.ts";
import { makeQueueHoldStore } from "../jones/orchestration/QueueHoldStore.ts";
import { makeJonesTablePresence } from "../jones/persistence/JonesTablePresence.ts";
import { IncomingLinkedMessage } from "../jones/peerTransport/LinkedMessageOrigin.ts";
import { TaskContinuationCommit } from "../jones/taskContinuation/commitContext.ts";
import { TaskContinuationError } from "../jones/taskContinuation/sourceBinding.ts";
import { ThreadTitleAssessment } from "../jones/threadTitle/commitContext.ts";
import * as TitleStore from "../jones/threadTitle/store.ts";
import { workModeFixture } from "../jones/workMode/Fixtures.testkit.ts";
import { workModeCommand } from "../jones/workMode/Policy.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as Receipts from "./CommandReceiptStore.ts";
import * as EventSink from "./EventSink.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ThreadCommands from "./ThreadCommandExecutor.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";

// These cases need the real serialized decider, SQL receipt, projection and outbox transaction.
// No provider is opened and the harness owns its scoped synthetic state.
const isTaskContinuationError = Schema.is(TaskContinuationError);
const isOrchestratorDispatchError = Schema.is(Orchestrator.OrchestratorDispatchError);

const runtime = ProviderReplayHarness.layerWithRegistryAndServices(
  { name: "retained-orchestrator-admission" },
  ProviderAdapterRegistry.layerFromAdapters([]),
  { runEffectWorker: false, recoverOnStartup: false },
);
const layer = Receipts.layer.pipe(Layer.provideMerge(runtime));
const fixture = workModeFixture();
const threadId = fixture.thread.id;
const instanceId = fixture.thread.providerInstanceId;
const seedThread = Effect.gen(function* () {
  const sink = yield* EventSink.EventSinkV2;
  const now = yield* DateTime.now;
  const thread = {
    ...fixture.thread,
    activeProviderThreadId: null,
    createdAt: now,
    updatedAt: now,
  };
  yield* sink.write({
    events: [
      {
        id: EventId.make("seed-thread"),
        type: "thread.created",
        threadId,
        occurredAt: now,
        payload: thread,
      },
    ],
  });
  return thread;
});
const storedFor = (commandId: CommandId) =>
  Effect.gen(function* () {
    const sink = yield* EventSink.EventSinkV2;
    return yield* sink.readByCommandId({ commandId }).pipe(Stream.runCollect);
  });

it.effect(
  "public failure-D without the executing owner's private proof cannot accept or delete",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const receipts = yield* Receipts.CommandReceiptStoreV2;
      const sink = yield* EventSink.EventSinkV2;
      const sql = yield* SqlClient.SqlClient;
      const thread = yield* seedThread;
      const before = yield* sink.latestSequence({ threadId });
      const commandId = CommandId.make("unproven-public-failure-delete");
      const error = yield* orchestrator
        .dispatch({
          type: "legacy-bootstrap.failure-delete",
          commandId,
          threadId,
          runId: RunId.make("unproven-run"),
          legacyBootstrap: {
            version: 1,
            threadId,
            projectId: thread.projectId,
            messageId: MessageId.make("unproven-message"),
            createCommandId: CommandId.make("unproven-create"),
            birthCommandId: CommandId.make("unproven-birth"),
            releaseCommandId: CommandId.make("unproven-release"),
            ownsNewThread: true,
            payloadHash: "synthetic",
          },
        })
        .pipe(Effect.flip);
      assert.isTrue(isOrchestratorDispatchError(error));
      if (isOrchestratorDispatchError(error))
        assert.equal(
          error.cause,
          "Failure D requires the private owner-authenticated dispatch port.",
        );
      assert.isTrue(Option.isNone(yield* receipts.getByCommandId(commandId)));
      assert.isEmpty(yield* storedFor(commandId));
      assert.equal(yield* sink.latestSequence({ threadId }), before);
      assert.isNull((yield* orchestrator.getThreadProjection(threadId)).thread.deletedAt);
      assert.isEmpty(
        yield* sql`SELECT command_id FROM jones_legacy_bootstrap_commands WHERE command_id = ${commandId}`,
      );
    }).pipe(Effect.provide(layer)),
);

const seedRun = (status: "running" | "waiting") =>
  Effect.gen(function* () {
    const sink = yield* EventSink.EventSinkV2;
    const thread = yield* seedThread;
    const now = yield* DateTime.now;
    const base = fixture.runs[0]!;
    const run: OrchestrationV2Run = {
      ...base,
      status,
      requestedAt: now,
      startedAt: now,
      completedAt: null,
    };
    const node: OrchestrationV2ExecutionNode = {
      id: run.rootNodeId!,
      threadId,
      runId: run.id,
      parentNodeId: null,
      rootNodeId: run.rootNodeId!,
      kind: "root_turn",
      status: status === "waiting" ? "completed" : "running",
      countsForRun: true,
      providerThreadId: run.providerThreadId,
      providerTurnId: fixture.providerTurns[0]!.id,
      nativeItemRef: null,
      runtimeRequestId: null,
      checkpointScopeId: null,
      startedAt: now,
      completedAt: status === "waiting" ? now : null,
    };
    const provider = { ...fixture.providerThreads[0]!, updatedAt: now };
    const turn = { ...fixture.providerTurns[0]!, startedAt: now, completedAt: now };
    const events: OrchestrationV2DomainEvent[] = [
      {
        id: EventId.make("seed-provider"),
        type: "provider-thread.updated",
        threadId,
        occurredAt: now,
        payload: provider,
      },
      {
        id: EventId.make("seed-run"),
        type: "run.created",
        threadId,
        occurredAt: now,
        payload: run,
      },
      {
        id: EventId.make("seed-root"),
        type: "node.updated",
        threadId,
        occurredAt: now,
        payload: node,
      },
      {
        id: EventId.make("seed-turn"),
        type: "provider-turn.updated",
        threadId,
        occurredAt: now,
        payload: turn,
      },
      {
        id: EventId.make("seed-attempt"),
        type: "run-attempt.updated",
        threadId,
        occurredAt: now,
        payload: {
          id: run.activeAttemptId!,
          runId: run.id,
          attemptOrdinal: 1,
          rootNodeId: node.id,
          providerInstanceId: instanceId,
          providerThreadId: provider.id,
          providerTurnId: turn.id,
          reason: "initial",
          status: status === "waiting" ? "completed" : "running",
          startedAt: now,
          completedAt: status === "waiting" ? now : null,
        },
      },
      {
        id: EventId.make("seed-owner"),
        type: "thread.metadata-updated",
        threadId,
        occurredAt: now,
        payload: { ...thread, activeProviderThreadId: provider.id },
      },
    ];
    yield* sink.write({ events });
    return { run, node, now };
  });

it.effect(
  "continuation admission failure rolls back the command; retry accepts once and replay stays original",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const receipts = yield* Receipts.CommandReceiptStoreV2;
      const sink = yield* EventSink.EventSinkV2;
      const sql = yield* SqlClient.SqlClient;
      yield* seedThread;
      yield* sql`CREATE TABLE core_decide_acceptance (value INTEGER NOT NULL)`;
      const command = {
        type: "thread.pin" as const,
        threadId,
        commandId: CommandId.make("continuation-pin"),
      };
      const before = yield* sink.latestSequence({ threadId });
      let attempts = 0;
      const context = {
        accept: () =>
          Effect.gen(function* () {
            attempts++;
            assert.isTrue(Option.isSome(yield* receipts.getByCommandId(command.commandId)));
            yield* sql`INSERT INTO core_decide_acceptance (value) VALUES (1)`;
            if (attempts === 1)
              return yield* new TaskContinuationError({ reason: "native_source_conflict" });
          }).pipe(
            Effect.mapError((cause) =>
              isTaskContinuationError(cause)
                ? cause
                : new TaskContinuationError({ reason: "storage_unavailable", cause }),
            ),
          ),
      };
      yield* orchestrator
        .dispatch(command)
        .pipe(Effect.provideService(TaskContinuationCommit, context), Effect.flip);
      assert.isTrue(Option.isNone(yield* receipts.getByCommandId(command.commandId)));
      assert.isEmpty(yield* storedFor(command.commandId));
      assert.equal(yield* sink.latestSequence({ threadId }), before);
      assert.isEmpty(yield* sql`SELECT value FROM core_decide_acceptance`);
      assert.isNull((yield* orchestrator.getThreadProjection(threadId)).thread.pinnedAt ?? null);
      const accepted = yield* orchestrator
        .dispatch(command)
        .pipe(Effect.provideService(TaskContinuationCommit, context));
      const originallyStored = yield* storedFor(command.commandId);
      const replay = yield* orchestrator.dispatch(command).pipe(
        Effect.provideService(TaskContinuationCommit, {
          accept: () => Effect.die("accepted continuation must not be accepted again"),
        }),
      );
      assert.equal(attempts, 2);
      assert.equal(replay.replayed, true);
      assert.equal(replay.sequence, accepted.sequence);
      assert.deepEqual(replay.storedEvents, originallyStored);
      assert.lengthOf(yield* sql`SELECT value FROM core_decide_acceptance`, 1);
      assert.isNotNull((yield* orchestrator.getThreadProjection(threadId)).thread.pinnedAt);
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "authenticated linked origin observes the target block before any rejected receipt or message",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const receipts = yield* Receipts.CommandReceiptStoreV2;
      const sink = yield* EventSink.EventSinkV2;
      const thread = yield* seedThread;
      yield* sink.write({
        events: [
          {
            id: EventId.make("block-linked"),
            type: "thread.metadata-updated",
            threadId,
            occurredAt: thread.updatedAt,
            payload: { ...thread, threadMessagesBlocked: true },
          },
        ],
      });
      const command = workModeCommand({ threadId, generation: "linked-send" });
      const failure = yield* orchestrator
        .dispatch(command)
        .pipe(Effect.provideService(IncomingLinkedMessage, true), Effect.flip);
      assert.instanceOf(failure, Orchestrator.OrchestratorThreadMessagesBlockedError);
      assert.isTrue(Option.isNone(yield* receipts.getByCommandId(command.commandId)));
      assert.isEmpty(yield* storedFor(command.commandId));
      assert.isEmpty((yield* orchestrator.getThreadProjection(threadId)).messages);
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "work-mode skips disabled, contended and evicted-session candidates without reserving commands",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const settings = yield* ServerSettings.ServerSettingsService;
      const receipts = yield* Receipts.CommandReceiptStoreV2;
      const locks = yield* ThreadCommands.ThreadCommandExecutor;
      yield* seedRun("waiting");
      const candidate = { threadId, generation: "stale-generation" };
      yield* settings.updateSettings({ workModeEnabled: false });
      assert.equal(yield* orchestrator.requestWorkMode(candidate), "skipped");
      yield* settings.updateSettings({ workModeEnabled: true });
      assert.equal(
        yield* locks.withLock(threadId, orchestrator.requestWorkMode(candidate)),
        "skipped",
      );
      assert.equal(yield* orchestrator.requestWorkMode(candidate), "skipped");
      assert.isTrue(
        Option.isNone(yield* receipts.getByCommandId(workModeCommand(candidate).commandId)),
      );
      assert.isEmpty((yield* orchestrator.getThreadProjection(threadId)).messages);
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "user input command atomically records a nonblocking request and replays its original three events",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const { run, node } = yield* seedRun("running");
      const command = {
        type: "user_input_request.record" as const,
        commandId: CommandId.make("record-response"),
        threadId,
        runId: run.id,
        rootNodeId: node.id,
        providerInstanceId: instanceId,
        requestId: RuntimeRequestId.make("response"),
        requestNodeId: NodeId.make("response-node"),
        turnItemId: TurnItemId.make("response-card"),
        question: {
          id: "response",
          header: "Review",
          question: "Review the prepared result.",
          options: [],
          required: true,
          allowCustomAnswer: true,
        },
      };
      const accepted = yield* orchestrator.dispatch(command);
      const replay = yield* orchestrator.dispatch(command);
      assert.deepEqual(
        accepted.storedEvents.map(({ event }) => event.type),
        ["node.updated", "turn-item.updated", "runtime-request.updated"],
      );
      assert.equal(replay.replayed, true);
      assert.deepEqual(replay.storedEvents, accepted.storedEvents);
      const projection = yield* orchestrator.getThreadProjection(threadId);
      assert.lengthOf(projection.runtimeRequests, 1);
      assert.deepEqual(projection.runtimeRequests[0]!.responseCapability, { type: "message" });
      assert.equal(
        projection.nodes.find((item) => item.id === command.requestNodeId)?.countsForRun,
        false,
      );
      assert.equal(projection.runs[0]!.status, "running");
      yield* orchestrator
        .dispatch({ ...command, commandId: CommandId.make("collision-response") })
        .pipe(Effect.flip);
      assert.lengthOf((yield* orchestrator.getThreadProjection(threadId)).runtimeRequests, 1);
    }).pipe(Effect.provide(layer)),
);

it.effect.each([false, true])(
  "Stop reconciles a waiting run only without a live checkpoint capture (pending=%s)",
  (pending) =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const sink = yield* EventSink.EventSinkV2;
      const { run } = yield* seedRun("waiting");
      if (pending)
        yield* sink.writeWithEffects({
          events: [],
          effects: [
            {
              id: `effect:checkpoint.capture:${run.id}`,
              commandId: CommandId.make(`command:effect:checkpoint.capture:${run.id}`),
              threadId,
              request: {
                type: "checkpoint.capture",
                runId: run.id,
                scopeId: CheckpointScopeId.make("live-capture"),
              },
            },
          ],
        });
      const command = {
        type: "thread.stop" as const,
        commandId: CommandId.make("stop-waiting"),
        threadId,
      };
      const stopped = yield* orchestrator.dispatch(command);
      const projection = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(projection.runs[0]!.status, pending ? "waiting" : "interrupted");
      assert.equal(projection.attempts[0]!.status, "completed");
      assert.equal(projection.providerTurns[0]!.status, "completed");
      const replay = yield* orchestrator.dispatch(command);
      assert.equal(replay.replayed, true);
      assert.deepEqual(replay.storedEvents, stopped.storedEvents);
    }).pipe(Effect.provide(layer)),
);

it.effect.each([true, false])(
  "title completion uses only its captured request assessment (matching=%s)",
  (matching) =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const sink = yield* EventSink.EventSinkV2;
      const titles = yield* TitleStore.make;
      const thread = yield* seedThread;
      const requestId = CommandId.make("title-request");
      const messageId = fixture.messages[0]!.id;
      yield* sink.write({
        events: [
          {
            id: EventId.make("arm-title"),
            type: "thread.metadata-updated",
            threadId,
            occurredAt: thread.updatedAt,
            payload: { ...thread, titleRegeneration: { requestId, startedAt: thread.updatedAt } },
          },
        ],
      });
      yield* titles.write(threadId, {
        status: "active",
        accepted: [messageId],
        covered: [],
        knownInsufficient: false,
        lastRequestId: null,
        pending: { requestId, evidence: [messageId] },
      });
      const command = {
        type: "thread.title.regeneration.complete" as const,
        commandId: CommandId.make("complete-title"),
        threadId,
        requestId,
        title: "Assessed title",
      };
      yield* orchestrator.dispatch(command).pipe(
        Effect.provideService(ThreadTitleAssessment, {
          requestId: matching ? requestId : CommandId.make("unrelated-title-request"),
          verdict: "sufficient",
        }),
      );
      const state = yield* titles.read(threadId);
      assert.equal(state?.status, matching ? "sufficient" : "active");
      assert.equal(state?.lastRequestId, requestId);
      assert.isNull(state?.pending);
      const beforeReplay = state;
      yield* orchestrator
        .dispatch(command)
        .pipe(Effect.provideService(ThreadTitleAssessment, { requestId, verdict: "insufficient" }));
      assert.deepEqual(yield* titles.read(threadId), beforeReplay);
    }).pipe(Effect.provide(layer)),
);

it.effect("guarded monitor settlement refuses an unfinished retained goal", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const sink = yield* EventSink.EventSinkV2;
    const thread = yield* seedThread;
    yield* sink.write({
      events: [
        {
          id: EventId.make("pending-goal"),
          type: "provider-thread.updated",
          threadId,
          occurredAt: thread.updatedAt,
          payload: {
            ...fixture.providerThreads[0]!,
            goal: { objective: "Finish work", status: "active" },
          },
        },
      ],
    });
    const command = {
      type: "thread.settle" as const,
      threadId,
      commandId: CommandId.make("monitor-settle"),
      guard: {
        policy: "preserve_pending_work" as const,
        expectedUpdatedAt: DateTime.formatIso(thread.updatedAt),
      },
    };
    const error = yield* orchestrator.dispatch(command).pipe(Effect.flip);
    assert.instanceOf(error, Orchestrator.OrchestratorDispatchError);
    if (isOrchestratorDispatchError(error))
      assert.equal(error.cause, "Thread has an unfinished goal.");
    assert.isNull((yield* orchestrator.getThreadProjection(threadId)).thread.settledAt);
    assert.isEmpty(yield* storedFor(command.commandId));
  }).pipe(Effect.provide(layer)),
);

it.effect(
  "idle worktree context commits the original hold and blocks sends without rejected receipts",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const sink = yield* EventSink.EventSinkV2;
      const receipts = yield* Receipts.CommandReceiptStoreV2;
      yield* seedThread;
      const command = {
        type: "thread.metadata.update" as const,
        threadId,
        commandId: CommandId.make("claim-idle"),
        branch: "feature/preparing",
        expectedEmpty: true,
      };
      const intent: IdleWorktreeAdmission.Intent = {
        threadId,
        commandId: command.commandId,
        claimType: "thread.metadata.update",
        requestKey: "idle-worktree",
        generation: "first-generation",
      };
      yield* orchestrator
        .dispatch(command)
        .pipe(Effect.provideService(IdleWorktreeAdmission.LaunchIntent, intent));
      assert.equal(
        (yield* sink.idleWorktreeAdmission!.read(threadId))?.generation,
        intent.generation,
      );
      const message = workModeCommand({ threadId, generation: "blocked-by-preparation" });
      const failure = yield* orchestrator.dispatch(message).pipe(Effect.flip);
      assert.instanceOf(failure, Orchestrator.OrchestratorDispatchError);
      if (isOrchestratorDispatchError(failure))
        assert.instanceOf(failure.cause, IdleWorktreeAdmission.IdleWorktreeAdmissionError);
      assert.isTrue(Option.isNone(yield* receipts.getByCommandId(message.commandId)));
      assert.isEmpty(yield* storedFor(message.commandId));
      const replay = yield* orchestrator.dispatch(command).pipe(
        Effect.provideService(IdleWorktreeAdmission.LaunchIntent, {
          ...intent,
          generation: "replacement-generation",
        }),
      );
      assert.equal(replay.replayed, true);
      assert.equal(
        (yield* sink.idleWorktreeAdmission!.read(threadId))?.generation,
        intent.generation,
      );
    }).pipe(Effect.provide(layer)),
);

it.effect("Stop supersedes a restart hold in the accepting transaction", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const sink = yield* EventSink.EventSinkV2;
    const sql = yield* SqlClient.SqlClient;
    const holds = makeQueueHoldStore(sql, makeJonesTablePresence(sql));
    const { run, now } = yield* seedRun("waiting");
    const queued: OrchestrationV2Run = {
      ...run,
      id: RunId.make("queued-before-stop"),
      ordinal: 2,
      userMessageId: MessageId.make("queued-message"),
      status: "queued",
      queueHeld: false,
      queuePosition: 1,
      rootNodeId: null,
      activeAttemptId: null,
      providerThreadId: null,
      startedAt: null,
    };
    yield* sink.write({
      events: [
        {
          id: EventId.make("queue-before-restart"),
          type: "run.created",
          threadId,
          occurredAt: now,
          payload: queued,
        },
      ],
    });
    yield* sink.commitCommand({
      commandId: CommandId.make("recover-restart"),
      commandType: "provider-runtime.reconcile",
      threadId,
      acceptedAt: now,
      effects: [],
      events: [
        {
          id: EventId.make("restart-hold"),
          type: "run.updated",
          threadId,
          occurredAt: now,
          payload: { ...queued, queueHeld: true },
        },
      ],
    });
    assert.equal((yield* holds.get([queued.id])).get(queued.id)?.reason, "restart");
    yield* orchestrator.dispatch({
      type: "thread.stop",
      threadId,
      commandId: CommandId.make("stop-after-restart"),
    });
    assert.equal((yield* holds.get([queued.id])).get(queued.id)?.reason, "stop");
    const projection = yield* orchestrator.getThreadProjection(threadId);
    assert.equal(projection.runs.find((item) => item.id === queued.id)?.queueHeld, true);
  }).pipe(Effect.provide(layer)),
);

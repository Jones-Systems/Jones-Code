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
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  RunAttemptId,
  RunId,
  ScheduledTaskId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";

import * as Environment from "../../environment/ServerEnvironment.ts";
import * as Receipts from "../../orchestration-v2/CommandReceiptStore.ts";
import * as EffectOutbox from "../../orchestration-v2/EffectOutbox.ts";
import * as EffectWorker from "../../orchestration-v2/EffectWorker.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as ProviderRuntimeRecovery from "../../orchestration-v2/ProviderRuntimeRecoveryService.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as ServerSettings from "../../serverSettings.ts";
import * as Goals from "../goals/continuity/GoalContinuity.ts";
import * as NativeStartAdmission from "../nativeCreation/NativeStartAdmission.ts";
import * as Control from "../orchestration/ControlAdmission.ts";
import * as Restart from "../orchestration/RestartPreparation.ts";
import { EVENT_LOG_PRUNED_THROUGH_PROJECTION } from "../persistence/EventLogPruneBoundary.ts";
import * as Refusals from "../startRefusal/Store.ts";
import { StartAdmission } from "./StartAdmission.ts";
import * as UnattendedStartAdmission from "./UnattendedStartAdmission.ts";

const recoveryStores = Layer.mergeAll(
  IdAllocator.layer,
  EventStore.layer,
  EffectOutbox.layer,
  ProjectionStore.layer,
  Receipts.layer,
  Refusals.layer,
  Control.layer,
).pipe(Layer.provideMerge(SqlitePersistence.layerMemory));
const recoverySink = EventSink.layer.pipe(Layer.provideMerge(recoveryStores));
const unattendedRecoveryLayer = Layer.mergeAll(
  ProviderRuntimeRecovery.layer,
  NativeStartAdmission.layer,
).pipe(
  Layer.provide(UnattendedStartAdmission.layer),
  Layer.provideMerge(
    Layer.mergeAll(
      recoverySink,
      ServerSettings.layerTest({ continueThreadsAfterServerUpdate: true }),
      Layer.mock(Restart.RestartPreparation)({ assertThreadOpen: () => Effect.void }),
      Layer.mock(Goals.GoalContinuity)({
        context: () => {
          throw new Error("Unexpected goal continuity context in unattended recovery.");
        },
        requirementFor: () =>
          Effect.succeed({ outcome: "absent", gate: { outcome: "clear", fenceRevision: "0" } }),
      }),
      Layer.succeed(Environment.ServerEnvironmentIdentity, {
        getEnvironmentId: Effect.succeed(EnvironmentId.make("recovery-regression")),
      }),
    ),
  ),
);

const initialTime = Date.parse("2026-11-18T15:00:00.000Z");
const seedUnattendedStart = Effect.fnUntraced(function* (source: "mcp" | "scheduled") {
  yield* TestClock.setTime(initialTime);
  const now = yield* DateTime.now;
  const sink = yield* EventSink.EventSinkV2;
  const bornQueued = source === "scheduled";
  const threadId = ThreadId.make("thread:unattended-recovery");
  const runId = RunId.make("run:unattended-recovery");
  const attemptId = RunAttemptId.make("attempt:unattended-recovery");
  const rootNodeId = NodeId.make("node:unattended-recovery");
  const providerThreadId = ProviderThreadId.make("provider-thread:unattended-recovery");
  const providerInstanceId = ProviderInstanceId.make("codex");
  const modelSelection = { instanceId: providerInstanceId, model: "gpt-5.4" };
  const driver = ProviderDriverKind.make("codex");
  const actor = bornQueued
    ? { createdBy: "user" as const, creationSource: "web" as const }
    : { createdBy: "agent" as const, creationSource: "mcp" as const };
  const thread: OrchestrationV2AppThread = {
    ...actor,
    id: threadId,
    projectId: ProjectId.make("project:unattended-recovery"),
    title: "Unattended input",
    providerInstanceId,
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: providerThreadId,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  };
  const providerThread: OrchestrationV2ProviderThread = {
    id: providerThreadId,
    appThreadId: threadId,
    ownerNodeId: null,
    providerInstanceId,
    providerSessionId: ProviderSessionId.make("session:unattended-recovery"),
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
  const message: OrchestrationV2ConversationMessage = {
    ...actor,
    ...(bornQueued ? { scheduledTaskId: ScheduledTaskId.make("scheduled:recovery") } : {}),
    id: MessageId.make("message:unattended-recovery"),
    threadId,
    runId,
    nodeId: rootNodeId,
    role: "user",
    text: "Deliver the original unattended input",
    attachments: [],
    streaming: false,
    createdAt: now,
    updatedAt: now,
  };
  const run: OrchestrationV2Run = {
    id: runId,
    threadId,
    ordinal: 1,
    providerInstanceId,
    modelSelection,
    providerThreadId,
    userMessageId: message.id,
    rootNodeId,
    activeAttemptId: attemptId,
    status: "starting",
    queuePosition: null,
    requestedAt: now,
    startedAt: null,
    completedAt: null,
    checkpointId: null,
    contextHandoffId: null,
  };
  const attempt: OrchestrationV2RunAttempt = {
    id: attemptId,
    runId,
    attemptOrdinal: 1,
    rootNodeId,
    providerInstanceId,
    providerThreadId,
    providerTurnId: null,
    reason: "initial",
    status: "pending",
    startedAt: null,
    completedAt: null,
  };
  const birthId = CommandId.make("command:unattended-recovery:birth");
  const startId = bornQueued ? CommandId.make("command:unattended-recovery:promote") : birthId;
  const original: EffectOutbox.PendingOrchestrationEffectV2 = {
    id: `effect:${startId}:provider-turn.start:${runId}`,
    commandId: startId,
    threadId,
    request: { type: "provider-turn.start", runId },
  };
  const base = { threadId, providerInstanceId, occurredAt: now };
  const runBase = { ...base, runId, nodeId: rootNodeId };
  const event = (name: string) => EventId.make(`event:unattended-recovery:${name}`);
  const birth = yield* sink.commitCommand({
    commandId: birthId,
    commandType: "message.dispatch",
    threadId,
    acceptedAt: now,
    events: [
      { ...base, id: event("thread"), type: "thread.created", payload: thread },
      { ...base, id: event("provider"), type: "provider-thread.updated", payload: providerThread },
      {
        ...runBase,
        id: event("run"),
        type: "run.created",
        payload: bornQueued ? { ...run, status: "queued", queuePosition: 1 } : run,
      },
      { ...runBase, id: event("attempt"), type: "run-attempt.created", payload: attempt },
      { ...runBase, id: event("message"), type: "message.updated", payload: message },
    ],
    effects: bornQueued ? [] : [original],
  });
  if (bornQueued) {
    yield* sink.writeWithEffects({
      commandId: startId,
      queuePromotion: { commandId: startId, threadId, runId },
      events: [
        {
          ...runBase,
          id: event("promotion-input"),
          type: "turn-item.updated",
          payload: {
            ...actor,
            id: TurnItemId.make("item:unattended-recovery"),
            threadId,
            runId,
            nodeId: rootNodeId,
            providerThreadId,
            providerTurnId: null,
            nativeItemRef: null,
            parentItemId: null,
            ordinal: 100,
            status: "completed",
            title: null,
            startedAt: now,
            completedAt: now,
            updatedAt: now,
            type: "user_message",
            messageId: message.id,
            scheduledTaskId: message.scheduledTaskId!,
            text: message.text,
            attachments: message.attachments,
            inputIntent: "queued_turn",
          },
        },
        { ...runBase, id: event("promotion"), type: "run.updated", payload: run },
      ],
      effects: [original],
    });
  }
  return { run, attempt, message, providerThread, original, birth };
});

it.effect.each(["mcp", "scheduled"] as const)(
  "delivers one %s start after two reconciliations that the worker never claimed",
  (source) =>
    Effect.gen(function* () {
      const { run, attempt, message, providerThread, original } =
        yield* seedUnattendedStart(source);
      const recovery = yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const admission = yield* StartAdmission;
      const sends: Array<{ readonly effectId: string; readonly text: string }> = [];
      for (const elapsed of [1, 2]) {
        yield* TestClock.setTime(initialTime + elapsed);
        assert.equal((yield* recovery.reconcile("startup")).terminalizedRuns, 0);
      }
      const rows = yield* outbox.listByThreadId(run.threadId);
      assert.lengthOf(rows, 3);
      assert.deepInclude(
        rows.find((row) => row.id === original.id),
        {
          status: "cancelled",
          attemptCount: 0,
        },
      );
      assert.lengthOf(
        rows.filter((row) => row.status === "cancelled" && row.attemptCount === 0),
        2,
      );
      const pending = rows.find((row) => row.status === "pending");
      assert.isDefined(pending);
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const executor = Layer.succeed(EffectWorker.OrchestrationEffectExecutorV2, {
        execute: (row) =>
          Effect.gen(function* () {
            assert.deepEqual(row.request, { type: "provider-turn.start", runId: run.id });
            const claim = yield* admission.capture({
              threadId: run.threadId,
              run,
              attemptId: attempt.id,
              providerThread,
            });
            yield* claim.revalidate;
            const messages = yield* projections.getMessagesById(run.threadId, [run.userMessageId]);
            sends.push({ effectId: row.id, text: messages.get(run.userMessageId)!.text });
          }).pipe(
            Effect.mapError(
              (cause) =>
                new EffectWorker.OrchestrationEffectExecutionError({
                  effectId: row.id,
                  effectType: row.request.type,
                  cause,
                }),
            ),
          ),
      });
      yield* Effect.gen(function* () {
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        assert.isTrue(yield* worker.runOnce());
        assert.isFalse(yield* worker.runOnce());
      }).pipe(Effect.provide(EffectWorker.layer.pipe(Layer.provide(executor))));
      assert.deepEqual(sends, [{ effectId: pending?.id, text: message.text }]);
      assert.deepInclude(Option.getOrUndefined(yield* outbox.get(pending!.id)), {
        status: "succeeded",
        attemptCount: 1,
      });
    }).pipe(Effect.provide(unattendedRecoveryLayer)),
);

it.effect.each([
  "claimed",
  "missing outbox row",
  "missing receipt",
  "missing event",
  "pruned history",
  "conflicting message",
  "native guard",
  "control hold",
] as const)("refuses to resend when a replacement has %s", (scenario) =>
  Effect.gen(function* () {
    const { run, original, birth } = yield* seedUnattendedStart("mcp");
    const recovery = yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService;
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    const sql = yield* SqlClient.SqlClient;
    yield* TestClock.setTime(initialTime + 1);
    assert.equal((yield* recovery.reconcile("startup")).terminalizedRuns, 0);
    const replacement = (yield* outbox.listByThreadId(run.threadId)).find(
      (row) => row.status === "pending",
    );
    assert.isDefined(replacement);
    const effectId = replacement!.id;
    const reconciliationId = effectId.slice(`effect:restart-resend:${run.id}:`.length);
    switch (scenario) {
      case "claimed": {
        const claimed = yield* outbox.claimNext({ workerId: "interrupted-before-run-start" });
        assert.equal(Option.getOrUndefined(claimed)?.id, effectId);
        break;
      }
      case "missing outbox row":
        yield* sql`DELETE FROM orchestration_v2_effect_outbox WHERE effect_id = ${effectId}`;
        break;
      case "missing receipt":
        yield* sql`DELETE FROM orchestration_command_receipts WHERE command_id = ${reconciliationId}`;
        break;
      case "missing event":
        yield* sql`DELETE FROM orchestration_events
          WHERE command_id = ${reconciliationId} AND event_type = 'run.updated'`;
        break;
      case "pruned history":
        yield* sql`INSERT OR REPLACE INTO orchestration_v2_projection_metadata
          (projection_name, schema_version, last_sequence, updated_at)
          VALUES (${EVENT_LOG_PRUNED_THROUGH_PROJECTION}, 1, ${birth.receipt.resultSequence},
            ${DateTime.formatIso(birth.receipt.acceptedAt)})`;
        break;
      case "conflicting message":
        yield* sql`UPDATE orchestration_events
          SET payload_json = json_set(payload_json, '$.userMessageId', 'message:other')
          WHERE command_id = ${reconciliationId} AND event_type = 'run.updated'`;
        break;
      case "native guard":
        yield* sql`UPDATE orchestration_v2_effect_outbox
          SET payload_json = json_object('request', json(payload_json),
            'nativeCreationExecutionReference', json_object(
              'version', 2, 'claimId', 'native-claim', 'stageCommandId', ${original.commandId},
              'stage', 'native_command', 'effectId', effect_id
            ))
          WHERE effect_id = ${effectId}`;
        break;
      case "control hold":
        yield* (yield* Control.ControlAdmission).holdStartup({
          instanceId: run.providerInstanceId,
          threadId: run.threadId,
          nativeThreadId: "native:unknown-outcome",
          producer: null,
        });
        break;
    }
    yield* TestClock.setTime(initialTime + 2);
    assert.equal((yield* recovery.reconcile("startup")).terminalizedRuns, 1);
    const rows = yield* outbox.listByThreadId(run.threadId);
    assert.isTrue(rows.every((row) => row.status === "cancelled"));
    assert.isTrue(rows.every((row) => row.id === original.id || row.id === effectId));
    const projection = yield* ProjectionStore.ProjectionStoreV2;
    assert.equal((yield* projection.getRun(run.threadId, run.id))?.status, "cancelled");
  }).pipe(Effect.provide(unattendedRecoveryLayer)),
);

it.effect("rolls back the recovery receipt and events when the replacement write fails", () =>
  Effect.gen(function* () {
    const { run, original } = yield* seedUnattendedStart("mcp");
    const recovery = yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService;
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    const receipts = yield* Receipts.CommandReceiptStoreV2;
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TEMP TRIGGER fail_recovery_outbox
      BEFORE INSERT ON orchestration_v2_effect_outbox
      WHEN NEW.command_id LIKE 'command:restart-resend:%'
      BEGIN SELECT RAISE(ABORT, 'replacement write failed'); END`;
    yield* TestClock.setTime(initialTime + 1);
    const timestamp = DateTime.formatIso(yield* DateTime.now);
    const commandId = CommandId.make(
      `command:runtime-reconcile:startup:${run.threadId}:${timestamp}`,
    );
    yield* recovery.reconcile("startup").pipe(Effect.flip);
    assert.isTrue(Option.isNone(yield* receipts.getByCommandId(commandId)));
    const events = yield* sql`SELECT 1 FROM orchestration_events WHERE command_id = ${commandId}`;
    assert.lengthOf(events, 0);
    const rows = yield* outbox.listByThreadId(run.threadId);
    assert.lengthOf(rows, 1);
    assert.deepInclude(rows[0], { id: original.id, status: "cancelled", attemptCount: 0 });

    yield* sql`DROP TRIGGER fail_recovery_outbox`;
    assert.equal((yield* recovery.reconcile("startup")).terminalizedRuns, 0);
    assert.isTrue(Option.isSome(yield* receipts.getByCommandId(commandId)));
    const recovered = yield* outbox.listByThreadId(run.threadId);
    assert.lengthOf(
      recovered.filter((row) => row.status === "pending"),
      1,
    );
  }).pipe(Effect.provide(unattendedRecoveryLayer)),
);

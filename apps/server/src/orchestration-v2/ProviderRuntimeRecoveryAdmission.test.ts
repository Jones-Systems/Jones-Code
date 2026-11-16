import { assert, it } from "@effect/vitest";
import { CommandId, EnvironmentId, MessageId, NodeId, ProviderInstanceId, ProviderSessionId,
  ProviderThreadId, RunAttemptId, RunId, ThreadId, type OrchestrationV2ThreadProjection, type OrchestrationV2StoredEvent } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as ServerSettings from "../serverSettings.ts";
import * as Environment from "../environment/ServerEnvironment.ts";
import * as Control from "../jones/orchestration/ControlAdmission.ts";
import * as Restart from "../jones/orchestration/RestartPreparation.ts";
import * as Continuity from "../jones/goals/continuity/GoalContinuity.ts";
import * as Refusals from "../jones/startRefusal/Store.ts";
import * as Receipts from "./CommandReceiptStore.ts";
import * as Outbox from "./EffectOutbox.ts";
import * as Worker from "./EffectWorker.ts";
import * as Sink from "./EventSink.ts";
import * as Projections from "./ProjectionStore.ts";
import * as Recovery from "./ProviderRuntimeRecoveryService.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";

it.effect.each(["original_unsent", "claimed", "crossed", "missing", "native_envelope", "stop",
  "control", "continuity", "binding_changed"] as const)(
  "startup recovery requires original no-effect custody and current admission: %s", (scenario) =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("recovery-admission");
      const runId = RunId.make("recovery-admission-run");
      const attemptId = RunAttemptId.make("recovery-admission-attempt");
      const rootNodeId = NodeId.make("recovery-admission-node");
      const providerThreadId = ProviderThreadId.make("recovery-admission-provider");
      const providerSessionId = ProviderSessionId.make("recovery-admission-session");
      const commandId = CommandId.make("recovery-original-command");
      const providerInstanceId = ProviderInstanceId.make("codex");
      const now = yield* DateTime.now;
      const modelSelection = { instanceId: providerInstanceId, model: "captured" };
      const run = { id: runId, threadId, status: "starting", startedAt: null, rootNodeId,
        activeAttemptId: attemptId, providerThreadId, providerInstanceId, modelSelection,
        userMessageId: MessageId.make("original-user-input"), ordinal: 1, queuePosition: null };
      const attempt = { id: attemptId, runId, rootNodeId, providerThreadId, providerInstanceId,
        attemptOrdinal: 1, reason: "initial", status: "pending", startedAt: null, providerTurnId: null };
      const projection = { thread: { id: threadId, archivedAt: null, deletedAt: null, settledAt: null,
        settledOverride: null, lineage: { parentThreadId: null }, snoozedUntil: null },
        runs: [run], attempts: [attempt], providerThreads: [{ id: providerThreadId,
          appThreadId: threadId, providerSessionId, providerInstanceId, driver: "codex",
          status: "not_loaded", nativeThreadRef: null, ownerNodeId: null }],
        nodes: [{ id: rootNodeId, runId, status: "pending", startedAt: null }], providerSessions: [],
        providerTurns: [], runtimeRequests: [], turnItems: [], subagents: [],
        messages: [{ id: run.userMessageId, threadId, runId, createdBy: "user", text: "Original", attachments: [] }],
      } as unknown as OrchestrationV2ThreadProjection;
      const original = { id: `effect:${commandId}:provider-turn.start:${runId}`, commandId, threadId,
        request: { type: "provider-turn.start", runId }, status: "pending", leaseOwner: null,
        attemptCount: scenario === "claimed" ? 1 : 0, payloadIdentity: "original",
        ...(scenario === "native_envelope" ? { nativeCreationExecutionReference: {} } : {}) };
      const birth = (sequence: number, type: string, payload: unknown) => ({ sequence, commandId,
        event: { type, threadId, payload } });
      let committed: Parameters<Sink.EventSinkV2Shape["commitCommand"]>[0] | undefined;
      const layer = Layer.mergeAll(
        ServerSettings.layerTest({ continueThreadsAfterServerUpdate: false }), IdAllocator.layer,
        Layer.mock(Worker.OrchestrationEffectWorkerV2)({ runRecoveryOnce: Effect.succeed(false) }),
        Layer.mock(Projections.ProjectionStoreV2)({
          getRecoveryThreadIds: () => Effect.succeed([threadId]),
          getRuntimeRecoveryProjection: () => Effect.succeed(projection),
          getThreadRecords: () => Effect.succeed(scenario === "binding_changed"
            ? { ...projection, providerThreads: [{ ...projection.providerThreads[0]!,
              providerSessionId: ProviderSessionId.make("replacement") }] } : projection),
          getMessagesById: () => Effect.succeed(new Map(projection.messages.map((m) => [m.id, m]))),
        }),
        Layer.mock(Outbox.EffectOutboxV2)({
          reconcileAfterProcessLoss: Effect.sync(() => {
            original.status = "cancelled";
            return { requeued: 0, cancelled: 1 };
          }),
          listByThreadId: () => Effect.succeed((scenario === "missing" ? [] : [original]) as never),
          get: () => Effect.succeed(Option.some(original) as never),
          listByCommandId: () => Effect.succeed([]),
        }),
        Layer.mock(Receipts.CommandReceiptStoreV2)({ getByCommandId: () => Effect.succeed(Option.some({
          status: "accepted", threadId, resultSequence: 2 } as never)) }),
        Layer.mock(Refusals.StartRefusals)({ readPrefix: () => Effect.succeed({
          state: scenario === "crossed" ? "crossed" : "unknown" } as never) }),
        Layer.mock(Restart.RestartPreparation)({ assertThreadOpen: () => Effect.void }),
        Layer.mock(Control.ControlAdmission)({ assertOpen: () => scenario === "control"
          ? Effect.die("held original control") : Effect.void }),
        Layer.mock(Continuity.GoalContinuity)({ requirementFor: () => Effect.succeed(scenario === "continuity"
          ? { outcome: "hold", reason: "binding_changed" } as never : { outcome: "absent" } as const) }),
        Layer.succeed(Environment.ServerEnvironmentIdentity, { getEnvironmentId: Effect.succeed(EnvironmentId.make("test")) }),
        Layer.mock(Sink.EventSinkV2)({
          assertRuntimeStopStartAllowed: () => scenario === "stop" ? Effect.die("captured Stop") : Effect.void,
          readByCommandId: () => Stream.fromIterable([birth(1, "run.created", run),
            birth(2, "run-attempt.created", attempt)] as unknown as ReadonlyArray<OrchestrationV2StoredEvent>),
          commitCommand: (input) => { committed = input; return Effect.succeed({ committed: true,
            cancelledEffectCount: 0 } as never); },
        }),
      );
      const summary = yield* Effect.gen(function* () {
        return yield* (yield* Recovery.make).reconcile("startup");
      }).pipe(Effect.provide(layer));
      assert.equal(summary.terminalizedRuns, scenario === "original_unsent" ? 0 : 1);
      const settled = committed?.events.find((event) => event.type === "run.updated");
      assert.equal(settled?.type === "run.updated" ? settled.payload.status : undefined,
        scenario === "original_unsent" ? "queued" : "cancelled");
      assert.isEmpty(committed?.effects ?? []);
      if (scenario === "original_unsent")
        assert.isTrue(settled?.type === "run.updated" && settled.payload.queueHeld);
    }),
);

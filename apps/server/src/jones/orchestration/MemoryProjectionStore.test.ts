import { assert, it } from "@effect/vitest";
import {
  EventId,
  MessageId,
  ThreadId,
  ProjectId,
  NodeId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  ProviderInstanceId,
  RunId,
  type ModelSelection,
  type OrchestrationV2AppThread,
  type OrchestrationV2Run,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import {
  LegacyPreparationFailureDecision,
  type RecordedRun,
} from "../../orchestration-v2/RecordedTypes.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
const decodeLegacyFailureDecision = Schema.decodeEffect(LegacyPreparationFailureDecision);
const providerInstanceId = ProviderInstanceId.make("synthetic-provider");
const modelSelection: ModelSelection = { instanceId: providerInstanceId, model: "synthetic-model" };
function makeThread(threadId: ThreadId, now: DateTime.Utc): OrchestrationV2AppThread {
  return {
    createdBy: "user",
    creationSource: "web",
    id: threadId,
    projectId: ProjectId.make(`project:${threadId}`),
    title: `Thread ${threadId}`,
    providerInstanceId,
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
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
}

const cases = [
  { backend: "Jones reference memory", layer: ProjectionStore.layerMemory },
  {
    backend: "SQLite memory database",
    layer: ProjectionStore.layer.pipe(Layer.provide(SqlitePersistence.layerMemory)),
  },
];
it.effect.each(cases)(
  "$backend: counts archived threads while excluding them from the active index",
  ({ layer }) =>
    Effect.gen(function* () {
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const now = DateTime.makeUnsafe("2026-01-01T00:00:00Z");
      const thread = makeThread(ThreadId.make("memory-parity-thread"), now);
      yield* store.apply({
        id: EventId.make("memory-create"),
        type: "thread.created",
        threadId: thread.id,
        occurredAt: now,
        payload: thread,
      });
      assert.equal(yield* store.countThreads(), 1);
      assert.deepEqual(
        (yield* store.listActiveShellIndex()).map((entry) => [entry.id, entry.mayBeLive]),
        [[thread.id, false]],
      );
      const archived = { ...thread, archivedAt: now };
      yield* store.apply({
        id: EventId.make("memory-archive"),
        type: "thread.archived",
        threadId: thread.id,
        occurredAt: now,
        payload: archived,
      });
      assert.equal(yield* store.countThreads(), 1);
      assert.deepEqual(yield* store.listActiveShellIndex(), []);
      yield* store.apply({
        id: EventId.make("memory-delete"),
        type: "thread.deleted",
        threadId: thread.id,
        occurredAt: now,
        payload: { ...archived, deletedAt: now },
      });
      assert.equal(yield* store.countThreads(), 0);
      assert.deepEqual(yield* store.listActiveShellIndex(), []);
    }).pipe(Effect.provide(layer)),
);
it.effect.each(cases)(
  "$backend: held queued work remains live until its recorded cancellation",
  ({ layer }) =>
    Effect.gen(function* () {
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const now = DateTime.makeUnsafe("2026-01-01T00:00:00Z");
      const thread = makeThread(ThreadId.make("memory-held-thread"), now);
      assert.isNull(yield* store.findThread(thread.id));
      assert.equal(yield* store.getRunCount(thread.id), 0);
      yield* store.apply({
        id: EventId.make("held-create"),
        type: "thread.created",
        threadId: thread.id,
        occurredAt: now,
        payload: thread,
      });
      const run: OrchestrationV2Run = {
        id: RunId.make("held-run"),
        threadId: thread.id,
        ordinal: 1,
        status: "queued",
        queueHeld: true,
        providerInstanceId,
        modelSelection,
        providerThreadId: null,
        userMessageId: MessageId.make("memory-held-message"),
        rootNodeId: null,
        activeAttemptId: null,
        requestedAt: now,
        startedAt: null,
        completedAt: null,
        checkpointId: null,
        contextHandoffId: null,
      };
      yield* store.apply({
        id: EventId.make("held-run-create"),
        type: "run.created",
        threadId: thread.id,
        runId: run.id,
        occurredAt: now,
        payload: run,
      });
      assert.equal((yield* store.findThread(thread.id))?.id, thread.id);
      assert.equal(yield* store.getRunCount(thread.id), 1);
      assert.deepEqual(
        (yield* store.getHeldQueuedRuns(thread.id)).map((entry) => entry.id),
        [run.id],
      );
      assert.equal((yield* store.listActiveShellIndex())[0]?.mayBeLive, true);
      assert.equal(yield* store.canStartQueuedRun(thread.id), false);
      yield* store.apply({
        id: EventId.make("held-run-cancel"),
        type: "run.updated",
        threadId: thread.id,
        runId: run.id,
        occurredAt: now,
        payload: { ...run, status: "cancelled", completedAt: now },
      });
      assert.deepEqual(yield* store.getHeldQueuedRuns(thread.id), []);
      assert.equal((yield* store.listActiveShellIndex())[0]?.mayBeLive, false);
      const fork = makeThread(ThreadId.make("memory-held-fork"), now);
      yield* store.apply({
        id: EventId.make("held-run-fork"),
        type: "thread.created",
        threadId: fork.id,
        occurredAt: now,
        payload: { ...fork, forkedFrom: { type: "run", threadId: thread.id, runId: run.id } },
      });
      yield* store.apply({
        id: EventId.make("held-run-rewind"),
        type: "run.updated",
        threadId: thread.id,
        runId: run.id,
        occurredAt: now,
        payload: { ...run, status: "rolled_back", completedAt: now },
      });
      assert.deepEqual(yield* store.getForkSourceRunIds(fork.id), [run.id]);
      assert.equal(yield* store.getRunCount(fork.id), 0);
    }).pipe(Effect.provide(layer)),
);

it.effect.each(cases)(
  "$backend: projects only the displayed run's attributed provider settlement",
  ({ layer }) =>
    Effect.gen(function* () {
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const now = DateTime.makeUnsafe("2026-01-01T00:00:00Z");
      const thread = makeThread(ThreadId.make("memory-settlement-thread"), now);
      const runId = RunId.make("memory-settlement-run");
      const attemptId = RunAttemptId.make("memory-settlement-attempt");
      const rootNodeId = NodeId.make("memory-settlement-root");
      const providerThreadId = ProviderThreadId.make("memory-settlement-provider-thread");
      const providerTurnId = ProviderTurnId.make("memory-settlement-provider-turn");
      yield* store.apply({
        id: EventId.make("memory-settlement-create"),
        type: "thread.created",
        threadId: thread.id,
        occurredAt: now,
        payload: thread,
      });
      const run: OrchestrationV2Run = {
        id: runId,
        threadId: thread.id,
        ordinal: 1,
        status: "waiting",
        providerInstanceId,
        modelSelection,
        providerThreadId,
        userMessageId: MessageId.make("memory-settlement-message"),
        rootNodeId,
        activeAttemptId: attemptId,
        requestedAt: now,
        startedAt: now,
        completedAt: null,
        checkpointId: null,
        contextHandoffId: null,
      };
      yield* store.apply({
        id: EventId.make("memory-settlement-run-create"),
        type: "run.created",
        threadId: thread.id,
        runId,
        occurredAt: now,
        payload: run,
      });
      const providerSettlement = {
        runAttemptId: attemptId,
        providerTurnId,
        status: "completed" as const,
        completedAt: now,
      };
      yield* store.apply({
        id: EventId.make("memory-settlement-attempt-create"),
        type: "run-attempt.created",
        threadId: thread.id,
        runId,
        occurredAt: now,
        payload: {
          id: attemptId,
          runId,
          attemptOrdinal: 1,
          rootNodeId,
          providerInstanceId,
          providerThreadId,
          providerTurnId,
          reason: "initial",
          status: "completed",
          startedAt: now,
          completedAt: now,
          providerSettlement,
        },
      });
      assert.deepEqual(
        (yield* store.getThreadShell(thread.id))?.latestRunProviderSettlement,
        providerSettlement,
      );
      assert.deepEqual(
        (yield* store.getShellSnapshot()).threads[0]?.latestRunProviderSettlement,
        providerSettlement,
      );
      assert.deepEqual(
        (yield* store.getThreadSnapshotWindow(thread.id, { rowLimit: 1 })).projection.attempts[0]
          ?.providerSettlement,
        providerSettlement,
      );
      assert.deepEqual(
        (yield* store.getRunProviderRecords(thread.id, runId, rootNodeId)).attempts[0]
          ?.providerSettlement,
        providerSettlement,
      );
      yield* store.apply({
        id: EventId.make("memory-settlement-unattributed-run"),
        type: "run.updated",
        threadId: thread.id,
        runId,
        occurredAt: now,
        payload: { ...run, activeAttemptId: null },
      });
      assert.isUndefined((yield* store.getThreadShell(thread.id))?.latestRunProviderSettlement);
      assert.isUndefined((yield* store.getShellSnapshot()).threads[0]?.latestRunProviderSettlement);
    }).pipe(Effect.provide(layer)),
);

const integrityMessages = (value: unknown): ReadonlyArray<string> =>
  typeof value === "object" && value !== null
    ? [
        ...("message" in value && typeof value.message === "string" ? [value.message] : []),
        ...("cause" in value ? integrityMessages(value.cause) : []),
      ]
    : [];

it.effect.each(cases)(
  "$backend: rejects unauthenticated workspace-failure provenance and keeps the run unchanged",
  ({ layer }) =>
    Effect.gen(function* () {
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const now = DateTime.makeUnsafe("2026-01-01T00:00:00Z");
      const thread = makeThread(ThreadId.make("memory-integrity-thread"), now);
      const runId = RunId.make("memory-integrity-run");
      yield* store.apply({
        id: EventId.make("memory-integrity-create"),
        type: "thread.created",
        threadId: thread.id,
        occurredAt: now,
        payload: thread,
      });
      const run: OrchestrationV2Run = {
        id: runId,
        threadId: thread.id,
        ordinal: 1,
        status: "preparing",
        providerInstanceId,
        modelSelection,
        providerThreadId: null,
        userMessageId: MessageId.make("memory-integrity-message"),
        rootNodeId: null,
        activeAttemptId: null,
        requestedAt: now,
        startedAt: null,
        completedAt: null,
        checkpointId: null,
        contextHandoffId: null,
      };
      yield* store.apply({
        id: EventId.make("memory-integrity-run-create"),
        type: "run.created",
        threadId: thread.id,
        runId,
        occurredAt: now,
        payload: run,
      });
      const anchors = {
        claimEventId: "memory-integrity:claim",
        claimSequence: 1,
        claimReceiptSequence: 1,
        birthEventId: "memory-integrity:birth",
        birthSequence: 2,
        birthReceiptSequence: 2,
      };
      // A schema-valid decision that no recorded legacy preparation authenticates.
      const decision = yield* decodeLegacyFailureDecision({
        version: 1,
        status: "known_workspace_failure",
        policy: {
          version: 1,
          createCommandId: "memory-integrity:create",
          birthCommandId: "memory-integrity:create:initial-message",
          releaseCommandId: "memory-integrity:release",
          projectId: thread.projectId,
          threadId: thread.id,
          messageId: run.userMessageId,
          payloadHash: "memory-integrity",
          ownsNewThread: true,
          runId,
        },
        ...anchors,
        preparationGeneration: "memory-integrity-generation",
        projectWorkspaceRoot: "/memory-integrity",
        workspacePath: "/memory-integrity",
        failedEffectId: "memory-integrity-effect",
        failedInputHash: "memory-integrity-hash",
        outcomeCommandId: "memory-integrity:outcome",
        outcomeEventId: "memory-integrity:outcome:event",
        outcomeEventSequence: 3,
        outcomeReceiptSequence: 3,
        failureCommandId: "memory-integrity:create:fail",
        evidenceEventId: "memory-integrity-forged",
      });
      const forged: RecordedRun = {
        ...run,
        status: "failed",
        completedAt: now,
        legacyPreparationFailureDecision: decision,
      };
      const exit = yield* store
        .apply({
          id: EventId.make("memory-integrity-forged"),
          type: "run.updated",
          threadId: thread.id,
          runId,
          occurredAt: now,
          payload: forged,
        })
        .pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(exit));
      if (Exit.isFailure(exit))
        assert.isTrue(
          integrityMessages(Cause.squash(exit.cause)).some((message) =>
            /Malformed or replaced legacy workspace-failure provenance/.test(message),
          ),
        );
      const [stored] = (yield* store.getThreadProjection(thread.id)).runs;
      assert.equal(stored?.status, "preparing");
      assert.isNull(stored?.completedAt);
      if (store.legacyRecords !== undefined && stored !== undefined)
        assert.isUndefined(
          (yield* store.legacyRecords.run(stored)).legacyPreparationFailureDecision,
        );
    }).pipe(Effect.provide(layer)),
);

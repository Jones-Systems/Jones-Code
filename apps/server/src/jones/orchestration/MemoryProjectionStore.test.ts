import { assert, it } from "@effect/vitest";
import {
  EventId,
  MessageId,
  ThreadId,
  ProjectId,
  ProviderInstanceId,
  RunId,
  type ModelSelection,
  type OrchestrationV2AppThread,
  type OrchestrationV2Run,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
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
    }).pipe(Effect.provide(layer)),
);

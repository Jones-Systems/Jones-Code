import { assert, it } from "@effect/vitest";
import {
  CheckpointScopeId,
  CommandId,
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Run,
} from "@t3tools/contracts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { SqliteReaderAdmissionError } from "@t3tools/shared/jones/sqliteReaders/admissionFailure";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as SqlError from "effect/sql/SqlError";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as ProviderEventIngestor from "../../orchestration-v2/ProviderEventIngestor.ts";
import * as ThreadCommandExecutor from "../../orchestration-v2/ThreadCommandExecutor.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as GitVcsDriver from "../../vcs/GitVcsDriver.ts";
import {
  isAdmissionUnavailable,
  isTransientLocalStorageFailure,
  makeTerminalPublisher,
  recoverAcceptedTerminal,
} from "./AcceptedTerminalRecovery.ts";

const driver = ProviderDriverKind.make("codex");
const threadId = ThreadId.make("thread:accepted-terminal");
const runId = RunId.make("run:accepted-terminal");
const attemptId = RunAttemptId.make("attempt:accepted-terminal");
const providerThreadId = ProviderThreadId.make("provider-thread:accepted-terminal");
const providerTurnId = ProviderTurnId.make("provider-turn:accepted-terminal");
const providerSessionId = ProviderSessionId.make("session:accepted-terminal");
const providerInstanceId = ProviderInstanceId.make("codex");
const nodeId = NodeId.make("node:accepted-terminal");
const now = DateTime.makeUnsafe("2026-11-11T23:00:00Z");
const admission = () => new SqliteReaderAdmissionError({ reason: "queue-timeout" });
const busy = () =>
  new EventSink.EventSinkWriteError({
    eventCount: 1,
    cause: new SqlError.SqlError({ reason: new SqlError.LockTimeoutError({ cause: null }) }),
  });
const terminal = {
  type: "turn.terminal",
  driver,
  providerThreadId,
  providerTurnId,
  runOrdinal: 1,
  status: "completed",
  failure: null,
  threadDisposition: "reusable",
  providerTurn: {
    id: providerTurnId,
    providerThreadId,
    nodeId,
    runAttemptId: attemptId,
    nativeTurnRef: { driver, nativeId: "original-native-turn", strength: "strong" },
    ordinal: 1,
    status: "completed",
    startedAt: now,
    completedAt: now,
  },
} satisfies Extract<ProviderAdapter.ProviderAdapterV2Event, { readonly type: "turn.terminal" }>;
const thread: OrchestrationV2AppThread = {
  id: threadId,
  projectId: ProjectId.make("project:accepted-terminal"),
  title: "Accepted terminal",
  createdBy: "user",
  creationSource: "web",
  providerInstanceId,
  modelSelection: { instanceId: providerInstanceId, model: "synthetic" },
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  branchPullRequest: null,
  activeOrderKey: null,
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
const run: OrchestrationV2Run = {
  id: runId,
  threadId,
  ordinal: 1,
  providerInstanceId,
  modelSelection: thread.modelSelection,
  providerThreadId,
  userMessageId: MessageId.make("message:accepted-terminal"),
  rootNodeId: nodeId,
  activeAttemptId: attemptId,
  status: "running",
  requestedAt: now,
  startedAt: now,
  completedAt: null,
  checkpointId: null,
  contextHandoffId: null,
};
const event = (id: string, payload: OrchestrationV2Run): OrchestrationV2DomainEvent => ({
  id: EventId.make(id),
  type: "run.updated",
  threadId,
  runId,
  occurredAt: now,
  payload,
});
const stores = Layer.merge(EventStore.layer, ProjectionStore.layer).pipe(
  Layer.provideMerge(SqlitePersistence.layerMemory),
);
const sinkLayer = EventSink.layer.pipe(Layer.provide(stores));
const databaseLayer = Layer.mergeAll(stores, sinkLayer);
const seed = Effect.gen(function* () {
  const sink = yield* EventSink.EventSinkV2;
  yield* sink.write({
    events: [
      {
        id: EventId.make("event:accepted-thread"),
        type: "thread.created",
        threadId,
        occurredAt: now,
        payload: thread,
      },
      event("event:accepted-run", run),
    ],
  });
});

it.effect.each([5, 6, 261, 262])(
  "only native busy/locked code %s authorizes storage retry",
  (errcode) =>
    Effect.sync(() => {
      assert.isTrue(isAdmissionUnavailable(Cause.fail({ cause: admission() })));
      assert.isTrue(isTransientLocalStorageFailure(Cause.die({ cause: { errcode } })));
      assert.isFalse(isTransientLocalStorageFailure({ errno: 5 }));
      assert.isFalse(
        isTransientLocalStorageFailure({ cause: new Error("SQLITE_BUSY queue-timeout") }),
      );
      for (const code of [10, 13, 19])
        assert.isFalse(isTransientLocalStorageFailure({ cause: { errcode: code } }));
    }),
);

it.effect.each([1, 3])(
  "real normalization retries only pending-input read admission (%s failures)",
  (failures) => {
    let reads = 0;
    let writes = 0;
    const dependencies = Layer.mergeAll(
      IdAllocator.layer,
      ThreadCommandExecutor.layer,
      Layer.mock(GitVcsDriver.GitVcsDriver)({}),
      Layer.mock(EventSink.EventSinkV2)({
        write: () =>
          Effect.sync(() => {
            writes++;
            return [];
          }),
      }),
      Layer.mock(ProjectionStore.ProjectionStoreV2)({
        getPendingNativeUserInputs: () =>
          Effect.suspend(() =>
            ++reads <= failures
              ? Effect.fail(
                  new ProjectionStore.ProjectionStoreReadError({ threadId, cause: admission() }),
                )
              : Effect.succeed({ runtimeRequests: [], nodes: [], turnItems: [] }),
          ),
      }),
    );
    return Effect.gen(function* () {
      const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
      const result = yield* ingestor
        .ingestNormalized({
          providerSessionId,
          providerInstanceId,
          threadId,
          runId,
          nodeId,
          event: { type: "provider_turn.updated", driver, providerTurn: terminal.providerTurn },
        })
        .pipe(Effect.exit);
      assert.equal(reads, failures === 1 ? 2 : 3);
      assert.equal(writes, failures === 1 ? 1 : 0);
      assert.equal(Exit.isSuccess(result), failures === 1);
    }).pipe(Effect.provide(ProviderEventIngestor.layer.pipe(Layer.provide(dependencies))));
  },
);

it.effect("a model read after subagent publication retries only the read and writes once", () => {
  let reads = 0;
  let writes = 0;
  const dependencies = Layer.mergeAll(
    IdAllocator.layer,
    ThreadCommandExecutor.layer,
    Layer.mock(GitVcsDriver.GitVcsDriver)({}),
    Layer.mock(EventSink.EventSinkV2)({
      write: (input) =>
        Effect.sync(() => {
          writes++;
          return input.events.map((event, index) => ({
            event,
            sequence: index + 1,
            commandId: null,
          }));
        }),
    }),
    Layer.mock(ProjectionStore.ProjectionStoreV2)({
      getThread: () =>
        Effect.suspend(() =>
          ++reads === 1
            ? Effect.fail(
                new ProjectionStore.ProjectionStoreReadError({ threadId, cause: admission() }),
              )
            : Effect.succeed(thread),
        ),
    }),
  );
  return Effect.gen(function* () {
    const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
    yield* ingestor.ingestNormalized({
      providerSessionId,
      providerInstanceId,
      threadId,
      runId,
      nodeId,
      event: {
        type: "subagent.updated",
        driver,
        subagent: {
          id: NodeId.make("node:accepted-child"),
          threadId,
          runId,
          parentNodeId: nodeId,
          origin: "provider_native",
          createdBy: "agent",
          driver,
          providerInstanceId,
          providerThreadId: null,
          childThreadId: threadId,
          nativeTaskRef: null,
          prompt: "Synthetic task",
          title: null,
          model: thread.modelSelection.model,
          status: "running",
          result: null,
          startedAt: now,
          completedAt: null,
          updatedAt: now,
        },
      },
    });
    assert.equal(writes, 1);
    assert.equal(reads, 2);
  }).pipe(Effect.provide(ProviderEventIngestor.layer.pipe(Layer.provide(dependencies))));
});

it.effect(
  "uncertain terminal commit is read back exactly once with one checkpoint and one settlement publication",
  () => {
    let writes = 0;
    let publications = 0;
    let readbacks = 0;
    const uncertainSink = Layer.effect(
      EventSink.EventSinkV2,
      Effect.gen(function* () {
        const sink = yield* EventSink.EventSinkV2;
        return EventSink.EventSinkV2.of({
          ...sink,
          readByCommandId: (input) =>
            Stream.unwrap(
              Effect.sync(() =>
                ++readbacks === 1
                  ? Stream.fail(
                      new EventSink.EventSinkStreamError({ threadId, cause: admission() }),
                    )
                  : sink.readByCommandId(input),
              ),
            ),
          writeIfRunCurrent: (input) => {
            writes++;
            return sink.writeIfRunCurrent(input).pipe(
              Effect.tap(() =>
                Effect.sync(() => {
                  publications++;
                }),
              ),
              Effect.andThen(Effect.fail(busy())),
            );
          },
        });
      }),
    ).pipe(Layer.provide(sinkLayer));
    return Effect.gen(function* () {
      yield* seed;
      const publish = yield* makeTerminalPublisher;
      const request = {
        type: "current" as const,
        input: {
          threadId,
          runId,
          activeAttemptId: attemptId,
          expectedStatus: "running" as const,
          events: [
            event("event:accepted-original", { ...run, status: "waiting" }),
            {
              id: EventId.make("event:accepted-turn"),
              type: "provider-turn.updated" as const,
              threadId,
              runId,
              occurredAt: now,
              payload: terminal.providerTurn,
            },
          ],
          effects: [
            {
              id: "effect:checkpoint.capture:accepted",
              commandId: CommandId.make("command:checkpoint:accepted"),
              threadId,
              request: {
                type: "checkpoint.capture" as const,
                runId,
                scopeId: CheckpointScopeId.make("scope:accepted"),
              },
            },
          ],
        },
      };
      const first = yield* publish(terminal, "finalization", request).pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(first));
      const unavailable = yield* publish(terminal, "finalization", request).pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(unavailable));
      assert.equal(writes, 1);
      const recovered = yield* publish(terminal, "finalization", {
        ...request,
        input: {
          ...request.input,
          events: [event("event:must-not-replace-original", { ...run, status: "failed" })],
        },
      });
      assert.isTrue(recovered.committed);
      assert.equal(recovered.storedEvents[0]?.event.id, EventId.make("event:accepted-original"));
      assert.equal(writes, 1);
      assert.equal(publications, 1);
      assert.equal(readbacks, 2);
      const storedTurn = recovered.storedEvents.find(
        (row) => row.event.type === "provider-turn.updated",
      );
      assert.equal(
        storedTurn?.event.type === "provider-turn.updated"
          ? storedTurn.event.payload.nativeTurnRef?.nativeId
          : null,
        "original-native-turn",
      );
      const sql = yield* SqlClient.SqlClient;
      const effects = yield* sql<{
        readonly count: number;
      }>`SELECT COUNT(*) AS count FROM orchestration_v2_effect_outbox WHERE effect_id = 'effect:checkpoint.capture:accepted'`;
      assert.equal(effects[0]?.count, 1);
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      assert.equal(
        (yield* projections.getThreadRecords(threadId, ["runs"])).runs[0]?.status,
        "waiting",
      );
    }).pipe(Effect.provide(Layer.mergeAll(stores, uncertainSink)));
  },
);

it.effect(
  "terminal ingestion reconciles its original cleanup even when a fresh pending-input read would be empty",
  () => {
    let reads = 0;
    let writes = 0;
    const uncertainSink = Layer.effect(
      EventSink.EventSinkV2,
      Effect.gen(function* () {
        const sink = yield* EventSink.EventSinkV2;
        return EventSink.EventSinkV2.of({
          ...sink,
          writeIfRunCurrent: (input) => {
            writes++;
            return sink.writeIfRunCurrent(input).pipe(Effect.andThen(Effect.fail(busy())));
          },
        });
      }),
    ).pipe(Layer.provide(sinkLayer));
    const dependencies = Layer.mergeAll(
      IdAllocator.layer,
      ThreadCommandExecutor.layer,
      Layer.mock(GitVcsDriver.GitVcsDriver)({}),
      uncertainSink,
      Layer.mock(ProjectionStore.ProjectionStoreV2)({
        getPendingNativeUserInputs: () =>
          Effect.sync(() => ({
            runtimeRequests: [],
            turnItems: [],
            nodes:
              ++reads === 1
                ? [
                    {
                      id: NodeId.make("node:accepted-question"),
                      threadId,
                      runId,
                      parentNodeId: nodeId,
                      rootNodeId: nodeId,
                      kind: "user_input_request" as const,
                      status: "waiting" as const,
                      countsForRun: false,
                      providerThreadId,
                      providerTurnId,
                      nativeItemRef: null,
                      runtimeRequestId: null,
                      checkpointScopeId: null,
                      startedAt: now,
                      completedAt: null,
                    },
                  ]
                : [],
          })),
      }),
    );
    const ingestorLayer = ProviderEventIngestor.layer.pipe(Layer.provide(dependencies));
    return Effect.gen(function* () {
      yield* seed;
      const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
      const input = {
        providerSessionId,
        providerInstanceId,
        threadId,
        runId,
        nodeId,
        event: terminal,
        writeIfRunCurrent: {
          runId,
          activeAttemptId: attemptId,
          expectedStatus: "running" as const,
        },
      };
      assert.isTrue(Exit.isFailure(yield* ingestor.ingestNormalized(input).pipe(Effect.exit)));
      const recovered = yield* ingestor.ingestNormalized(input);
      assert.equal(reads, 1);
      assert.equal(writes, 1);
      assert.equal(recovered.length, 1);
      assert.equal(
        recovered[0]?.event.type === "node.updated" ? recovered[0].event.payload.status : null,
        "cancelled",
      );
    }).pipe(Effect.provide(Layer.mergeAll(databaseLayer, ingestorLayer)));
  },
);

it.effect.each(["Stop", "successor"] as const)(
  "the real current-attempt guard ends recovery after %s commits",
  (race) =>
    Effect.gen(function* () {
      yield* seed;
      const sink = yield* EventSink.EventSinkV2;
      const publish = yield* makeTerminalPublisher;
      const request = {
        type: "current" as const,
        input: {
          threadId,
          runId,
          activeAttemptId: attemptId,
          expectedStatus: "running" as const,
          events: [event("event:accepted-stale", { ...run, status: "waiting" })],
        },
      };
      const changed: OrchestrationV2Run =
        race === "Stop"
          ? { ...run, status: "cancelled", completedAt: now }
          : { ...run, activeAttemptId: RunAttemptId.make("attempt:successor") };
      yield* sink.write({ events: [event("event:accepted-new-owner", changed)] });
      const result = yield* publish(terminal, "finalization", request);
      assert.isFalse(result.committed);
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      assert.deepEqual(
        (yield* projections.getThreadRecords(threadId, ["runs"])).runs[0]?.activeAttemptId,
        changed.activeAttemptId,
      );
      assert.equal(
        (yield* projections.getThreadRecords(threadId, ["runs"])).runs[0]?.status,
        changed.status,
      );
    }).pipe(Effect.provide(databaseLayer)),
);

it.effect(
  "persistent transient recovery uses the existing backoff and shutdown interrupts it",
  () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const retried = yield* Deferred.make<void>();
      const third = yield* Deferred.make<void>();
      let calls = 0;
      const work = Effect.suspend(() => {
        calls++;
        return (
          calls === 1
            ? Deferred.succeed(started, undefined)
            : calls === 2
              ? Deferred.succeed(retried, undefined)
              : calls === 3
                ? Deferred.succeed(third, undefined)
                : Effect.void
        ).pipe(Effect.andThen(Effect.fail(busy())));
      });
      const fiber = yield* recoverAcceptedTerminal(
        work,
        { runId, attemptId },
        Cause.fail(busy()),
      ).pipe(Effect.forkChild);
      yield* Deferred.await(started);
      yield* TestClock.adjust("1 second");
      yield* Deferred.await(retried);
      assert.equal(calls, 2);
      yield* TestClock.adjust("2 seconds");
      yield* Deferred.await(third);
      assert.equal(calls, 3);
      yield* Fiber.interrupt(fiber);
      const exit = yield* Fiber.await(fiber);
      assert.isTrue(Exit.isFailure(exit));
    }),
);

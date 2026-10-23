import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  RuntimeRequestId,
  TurnItemId,
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
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2Run,
  type OrchestrationV2RunAttempt,
  type ProviderRuntimeEvidenceCapture,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as ProviderEventIngestor from "../../orchestration-v2/ProviderEventIngestor.ts";
import * as ThreadCommandExecutor from "../../orchestration-v2/ThreadCommandExecutor.ts";
import * as GitVcsDriver from "../../vcs/GitVcsDriver.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderEventOrigin from "@t3tools/provider-core/server/jones/ProviderEventOrigin";

const driver = ProviderDriverKind.make("codex");
it.effect.each([
  "current",
  "superseded-same-session",
  "superseded-rebound-session",
  "failed-replacement",
  "missing-intent",
  "wrong-session",
  "wrong-instance",
  "wrong-generation",
  "wrong-conversation",
  "wrong-native-turn",
  "wrong-attempt",
  "wrong-run",
  "retired-producer",
  "replaced-producer",
  "unrelated-successor",
  "advanced-successor",
  "duplicate",
  "history-only",
  "changed-replacement",
  "locator-race",
  "ordinary-write-race",
  "ordinary-current-guard-race",
  "ordinary-owner-guard-race",
  "ordinary-unrelated-successor-race",
  "ordinary-same-session-race",
  "ordinary-same-session-unrelated-race",
] as const)(
  "admits only exact committed restart terminal history: %s",
  (mode) => {
    const ordinaryRace = mode.startsWith("ordinary-");
    const rebindSession =
      mode !== "superseded-same-session" && !mode.startsWith("ordinary-same-session");
    const ordinaryObserved = Deferred.makeUnsafe<void>();
    const releaseOrdinary = Deferred.makeUnsafe<void>();
    const committed = Deferred.makeUnsafe<void>();
    const releaseCommit = Deferred.makeUnsafe<void>();
    const validated = Deferred.makeUnsafe<void>();
    let pauseNextCommit = false;
    const database =
      mode === "locator-race"
        ? Layer.effect(
            SqlClient.SqlClient,
            Effect.map(SqlClient.SqlClient, (sql) => {
              const withTransaction: typeof sql.withTransaction = (effect) =>
                Effect.suspend(() => {
                  const pause = pauseNextCommit;
                  pauseNextCommit = false;
                  return sql
                    .withTransaction(effect)
                    .pipe(
                      Effect.tap(() =>
                        pause
                          ? Deferred.succeed(committed, undefined).pipe(
                              Effect.andThen(Deferred.await(releaseCommit)),
                            )
                          : Effect.void,
                      ),
                    );
                });
              return new Proxy(sql, {
                get: (target, key, receiver) =>
                  key === "withTransaction" ? withTransaction : Reflect.get(target, key, receiver),
              });
            }),
          ).pipe(Layer.provide(SqlitePersistence.layerMemory))
        : SqlitePersistence.layerMemory;
    const stores = Layer.merge(EventStore.layer, ProjectionStore.layer).pipe(
      Layer.provideMerge(database),
    );
    const sinkLayer = EventSink.layer.pipe(Layer.provide(stores));
    const dependencies = Layer.mergeAll(
      stores,
      sinkLayer,
      IdAllocator.layer,
      ThreadCommandExecutor.layer,
      Layer.mock(GitVcsDriver.GitVcsDriver)({
        resolveBranchName: () => Effect.die("Unexpected Git access"),
      }),
    );
    const testLayer = Layer.mergeAll(
      dependencies,
      ProviderEventIngestor.layer.pipe(
        Layer.updateService(EventSink.EventSinkV2, (sink) => ({
          ...sink,
          writeHistoricalTerminal: (input) =>
            sink.writeHistoricalTerminal!(input).pipe(
              Effect.tap((result) =>
                ordinaryRace && !result.handled
                  ? Deferred.succeed(ordinaryObserved, undefined).pipe(
                      Effect.andThen(Deferred.await(releaseOrdinary)),
                    )
                  : Effect.void,
              ),
            ),
        })),
        Layer.provide(dependencies),
      ),
    );
    return Effect.gen(function* () {
      const sink = yield* EventSink.EventSinkV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
      const sql = yield* SqlClient.SqlClient;
      const ids = {
        threadId: ThreadId.make("thread:restart-diagnostic"),
        runId: RunId.make("run:restart-diagnostic"),
        rootNodeId: NodeId.make("node:restart-diagnostic"),
        providerThreadId: ProviderThreadId.make("provider-thread:restart-diagnostic"),
        rootProviderTurnId: ProviderTurnId.make("provider-turn:restart-diagnostic"),
        attemptId: RunAttemptId.make("attempt:restart-diagnostic"),
      };
      const now = DateTime.makeUnsafe("2026-09-01T12:00:00Z");
      const terminalAt = DateTime.makeUnsafe("2026-09-01T12:00:05Z");
      const providerInstanceId = ProviderInstanceId.make("codex");
      const providerSessionId = ProviderSessionId.make("session:raw-terminal");
      const commandId = CommandId.make("command:restart-history");
      const accepts = [
        "current",
        "superseded-same-session",
        "superseded-rebound-session",
        "failed-replacement",
        "duplicate",
        "history-only",
        "locator-race",
        "ordinary-write-race",
        "ordinary-current-guard-race",
        "ordinary-owner-guard-race",
        "ordinary-same-session-race",
      ].includes(mode);
      const replacementAttemptId = RunAttemptId.make("attempt:raw-terminal:replacement");
      const modelSelection = { instanceId: providerInstanceId, model: "test-model" };
      const thread: OrchestrationV2AppThread = {
        createdBy: "user",
        creationSource: "web",
        id: ids.threadId,
        projectId: ProjectId.make("project:raw-terminal"),
        title: "Synthetic replacement",
        providerInstanceId,
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        activeProviderThreadId: ids.providerThreadId,
        lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: ids.threadId },
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
        id: ids.runId,
        threadId: ids.threadId,
        ordinal: 1,
        providerInstanceId,
        modelSelection,
        providerThreadId: ids.providerThreadId,
        userMessageId: MessageId.make("message:raw-terminal"),
        rootNodeId: ids.rootNodeId,
        activeAttemptId: ids.attemptId,
        status: "running",
        requestedAt: now,
        startedAt: now,
        completedAt: null,
        checkpointId: null,
        contextHandoffId: null,
      };
      const attempt: OrchestrationV2RunAttempt = {
        id: ids.attemptId,
        runId: ids.runId,
        attemptOrdinal: 1,
        rootNodeId: ids.rootNodeId,
        providerInstanceId,
        providerThreadId: ids.providerThreadId,
        providerTurnId: ids.rootProviderTurnId,
        providerSettlement: null,
        reason: "initial",
        status: "running",
        startedAt: now,
        completedAt: null,
      };
      const turn = {
        id: ids.rootProviderTurnId,
        providerThreadId: ids.providerThreadId,
        nodeId: ids.rootNodeId,
        runAttemptId: ids.attemptId,
        nativeTurnRef: { driver, nativeId: "old-native-turn", strength: "strong" as const },
        ordinal: 1,
        status: "running" as const,
        startedAt: now,
        completedAt: null,
      };
      const rootNode: OrchestrationV2ExecutionNode = {
        id: ids.rootNodeId,
        threadId: ids.threadId,
        runId: ids.runId,
        parentNodeId: null,
        rootNodeId: ids.rootNodeId,
        kind: "root_turn",
        status: "running",
        countsForRun: true,
        providerThreadId: ids.providerThreadId,
        providerTurnId: ids.rootProviderTurnId,
        nativeItemRef: null,
        runtimeRequestId: null,
        checkpointScopeId: null,
        startedAt: now,
        completedAt: null,
      };
      const providerThread: OrchestrationV2ProviderThread = {
        id: ids.providerThreadId,
        driver,
        providerInstanceId,
        providerSessionId: ProviderSessionId.make("session:raw-terminal"),
        appThreadId: ids.threadId,
        contextUsage: null,
        nativeMetadata: null,
        goal: null,
        pendingBackgroundTasks: [],
        ownerNodeId: ids.rootNodeId,
        nativeThreadRef: { driver, nativeId: "old-native-thread", strength: "strong" },
        runtimeIdentity: {
          runtimeGeneration: "old-generation",
          evidenceRevision: 1,
          requested: ProviderAdapter.requestedRuntimeIdentity(modelSelection, driver),
          observed: ProviderAdapter.unobservedRuntimeIdentity(),
        },
        nativeConversationHeadRef: null,
        status: "active",
        firstRunOrdinal: 1,
        lastRunOrdinal: 1,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      };
      const event = <T extends OrchestrationV2DomainEvent["type"]>(
        key: string,
        type: T,
        payload: Extract<OrchestrationV2DomainEvent, { type: T }>["payload"],
      ) =>
        ({
          id: EventId.make(`event:raw-terminal:${key}`),
          type,
          threadId: ids.threadId,
          runId: ids.runId,
          occurredAt: now,
          payload,
        }) as Extract<OrchestrationV2DomainEvent, { type: T }>;
      yield* sink.write({
        events: [
          event("thread", "thread.created", thread),
          event("run", "run.created", run),
          event("attempt", "run-attempt.created", attempt),
          event("turn", "provider-turn.updated", turn),
          event("node", "node.updated", rootNode),
          event("provider-thread", "provider-thread.updated", providerThread),
        ],
      });
      if (mode === "history-only" || ordinaryRace) {
        const requestId = RuntimeRequestId.make("request:old-question");
        const questionNodeId = NodeId.make("node:old-question");
        yield* sink.write({
          events: [
            event("question", "runtime-request.updated", {
              id: requestId,
              nodeId: questionNodeId,
              providerTurnId: turn.id,
              nativeRequestRef: null,
              kind: "user_input",
              status: "pending",
              responseCapability: { type: "live", providerSessionId },
              createdAt: now,
              resolvedAt: null,
            }),
            event("question-node", "node.updated", {
              ...rootNode,
              id: questionNodeId,
              kind: "user_input_request",
              status: "waiting",
              parentNodeId: rootNode.id,
              countsForRun: false,
              runtimeRequestId: requestId,
            }),
            event("question-item", "turn-item.updated", {
              id: TurnItemId.make("item:old-question"),
              threadId: thread.id,
              runId: run.id,
              nodeId: questionNodeId,
              providerThreadId: providerThread.id,
              providerTurnId: turn.id,
              nativeItemRef: null,
              parentItemId: null,
              ordinal: 1,
              status: "waiting",
              title: "Which option?",
              startedAt: now,
              completedAt: null,
              updatedAt: now,
              type: "user_input_request",
              requestId,
              questions: [],
            }),
          ],
        });
      }
      const successor = {
        ...attempt,
        id: replacementAttemptId,
        attemptOrdinal: 2,
        providerTurnId: null,
        reason: "steering_restart" as const,
        status: "pending" as const,
        startedAt: null,
        completedAt: null,
      };

      const capture = {
        threadId: ids.threadId,
        providerThreadId: ids.providerThreadId,
        providerSessionId:
          mode === "wrong-session" ? ProviderSessionId.make("session:wrong") : providerSessionId,
        providerInstanceId:
          mode === "wrong-instance" ? ProviderInstanceId.make("wrong") : providerInstanceId,
        driver,
        nativeThreadId: mode === "wrong-conversation" ? "wrong-conversation" : "old-native-thread",
        runtimeGeneration: mode === "wrong-generation" ? "wrong-generation" : "old-generation",
        evidenceRevision: 1,
      } satisfies ProviderRuntimeEvidenceCapture;
      const producer = ProviderEventOrigin.makeProviderEventProducer(
        {
          driver,
          instanceId: providerInstanceId,
          providerSessionId: providerSessionId,
          runtimeGeneration: "old-generation",
        },
        Effect.succeed(mode !== "replaced-producer").pipe(
          Effect.tap(() =>
            mode === "locator-race" ? Deferred.succeed(validated, undefined) : Effect.void,
          ),
        ),
      );
      if (mode === "retired-producer") producer.retire();
      const terminalTurn = {
        ...turn,
        nativeTurnRef: {
          ...turn.nativeTurnRef,
          nativeId: mode === "wrong-native-turn" ? "wrong-turn" : "old-native-turn",
        },
        status: "interrupted" as const,
        completedAt: terminalAt,
      };
      const terminalEvent = ProviderEventOrigin.stampProviderEvent(
        {
          type: "provider_turn.updated" as const,
          driver,
          threadId: ids.threadId,
          runtimeEvidence: capture,
          providerTurn: terminalTurn,
        },
        {
          producer: producer.origin,
          turn: {
            binding: capture,
            runId: mode === "wrong-run" ? RunId.make("run:wrong") : ids.runId,
            attemptId:
              mode === "wrong-attempt" ? RunAttemptId.make("attempt:wrong") : ids.attemptId,
            providerTurnId: ids.rootProviderTurnId,
          },
        },
      );
      const rebound = {
        ...providerThread,
        providerSessionId: ProviderSessionId.make("session:replacement"),
        status: "not_loaded" as const,
        updatedAt: terminalAt,
      };
      const replacementRootNodeId = NodeId.make("node:replacement");
      const successorAttempt = {
        ...successor,
        rootNodeId: replacementRootNodeId,
        attemptOrdinal: mode === "unrelated-successor" ? 3 : 2,
      };
      const nextRun = {
        ...run,
        status: "starting" as const,
        activeAttemptId: replacementAttemptId,
        rootNodeId: replacementRootNodeId,
      };
      let pendingRestart: ReturnType<EventSink.EventSinkV2Shape["writeWithEffects"]> | undefined;
      let restartFiber: Fiber.Fiber<unknown, EventSink.EventSinkV2Error> | undefined;
      if (mode !== "current") {
        const restartWrite = sink.writeWithEffects({
          commandId,
          events: [
            event("superseded", "run-attempt.updated", {
              ...attempt,
              status: "superseded",
              completedAt: terminalAt,
            }),
            event("old-node", "node.updated", {
              ...rootNode,
              status: "interrupted",
              completedAt: terminalAt,
            }),
            event("successor", "run-attempt.created", successorAttempt),
            event("successor-node", "node.updated", {
              ...rootNode,
              id: replacementRootNodeId,
              rootNodeId: replacementRootNodeId,
              status: "pending",
              providerTurnId: null,
              startedAt: null,
            }),
            event("starting", "run.updated", nextRun),
            ...(rebindSession ? [event("rebound", "provider-thread.updated", rebound)] : []),
          ],
          effects:
            mode === "missing-intent"
              ? []
              : [
                  {
                    id: `effect:${commandId}:provider-turn.restart:${turn.id}`,
                    commandId,
                    threadId: ids.threadId,
                    request: {
                      type: "provider-turn.restart",
                      providerSessionId: providerSessionId,
                      providerThreadId: providerThread.id,
                      providerTurnId: turn.id,
                      interruptedAttemptId: attempt.id,
                      runId: run.id,
                      ...(!rebindSession
                        ? {}
                        : {
                            sessionTransition: {
                              type: "replace",
                              replacementProviderSessionId: rebound.providerSessionId,
                            },
                          }),
                    },
                  },
                ],
        });
        if (mode === "locator-race") {
          pauseNextCommit = true;
          restartFiber = yield* restartWrite.pipe(Effect.forkScoped);
          yield* Deferred.await(committed);
        } else if (ordinaryRace) pendingRestart = restartWrite;
        else yield* restartWrite;
        if (mode === "failed-replacement")
          yield* sink.write({
            events: [
              event("failed-run", "run.updated", {
                ...nextRun,
                status: "failed",
                completedAt: terminalAt,
              }),
              event("failed-attempt", "run-attempt.updated", {
                ...successorAttempt,
                status: "failed",
                completedAt: terminalAt,
              }),
            ],
          });
        if (mode === "advanced-successor")
          yield* sink.write({
            events: [
              event("advanced", "run.updated", {
                ...nextRun,
                activeAttemptId: RunAttemptId.make("attempt:other"),
              }),
            ],
          });
      }
      if (mode === "changed-replacement")
        yield* sink.write({
          events: [
            event("changed-replacement", "provider-thread.updated", {
              ...rebound,
              runtimeIdentity: {
                ...providerThread.runtimeIdentity!,
                runtimeGeneration: "replacement-generation",
              },
            }),
          ],
        });
      const ingest = ingestor.ingestNormalized({
        providerSessionId: providerSessionId,
        providerInstanceId,
        threadId: ids.threadId,
        runId: ids.runId,
        nodeId: ids.rootNodeId,
        event: terminalEvent,
        ...(mode === "ordinary-current-guard-race"
          ? {
              writeIfRunCurrent: {
                runId: run.id,
                activeAttemptId: attempt.id,
                expectedStatus: "running" as const,
              },
            }
          : {}),
        ...(mode === "ordinary-owner-guard-race"
          ? {
              writeIfProviderThreadOwner: {
                providerThreadId: providerThread.id,
                runId: run.id,
                activeAttemptId: attempt.id,
                expectedLastRunOrdinal: run.ordinal,
              },
            }
          : {}),
      });
      const ordinaryIngestion = ordinaryRace ? yield* ingest.pipe(Effect.forkScoped) : undefined;
      if (ordinaryIngestion !== undefined) {
        yield* Deferred.await(ordinaryObserved);
        yield* pendingRestart!;
        if (
          mode === "ordinary-unrelated-successor-race" ||
          mode === "ordinary-same-session-unrelated-race"
        )
          yield* sink.write({
            events: [
              event("racing-unrelated-successor", "run.updated", {
                ...nextRun,
                activeAttemptId: RunAttemptId.make("attempt:unrelated"),
              }),
            ],
          });
      }
      const before = yield* projections.getThreadRecords(ids.threadId, [
        "runs",
        "attempts",
        "providerThreads",
        "nodes",
        "runtimeRequests",
        "turnItems",
      ]);
      const outboxBefore = yield* sql`SELECT * FROM orchestration_v2_effect_outbox`;
      const sequence = yield* sink.latestSequence();
      const stored =
        ordinaryIngestion !== undefined
          ? yield* Deferred.succeed(releaseOrdinary, undefined).pipe(
              Effect.andThen(Fiber.join(ordinaryIngestion)),
            )
          : mode === "locator-race"
            ? yield* Effect.gen(function* () {
                const ingestion = yield* ingest.pipe(Effect.forkScoped);
                yield* Deferred.await(validated);
                yield* Deferred.succeed(releaseCommit, undefined);
                yield* Fiber.join(restartFiber!);
                return yield* Fiber.join(ingestion);
              })
            : yield* ingest;
      const after = yield* projections.getThreadRecords(ids.threadId, [
        "runs",
        "attempts",
        "providerThreads",
        "nodes",
        "providerTurns",
        "runtimeRequests",
        "turnItems",
      ]);
      assert.deepEqual(after.runs, before.runs);
      assert.deepEqual(after.attempts, before.attempts);
      assert.deepEqual(after.providerThreads, before.providerThreads);
      assert.deepEqual(after.nodes, before.nodes);
      assert.deepEqual(after.runtimeRequests, before.runtimeRequests);
      assert.deepEqual(after.turnItems, before.turnItems);
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_effect_outbox`, outboxBefore);
      assert.equal(after.providerTurns[0]?.status, accepts ? "interrupted" : "running");
      assert.equal(stored.length, accepts ? 1 : 0);
      assert.equal((yield* sink.latestSequence()) - sequence, accepts ? 1 : 0);
      if (mode === "duplicate") {
        const repeated = yield* ingestor.ingestNormalized({
          providerSessionId: providerSessionId,
          providerInstanceId,
          threadId: ids.threadId,
          runId: ids.runId,
          nodeId: ids.rootNodeId,
          event: terminalEvent,
        });
        assert.equal(repeated.length, 0);
        assert.equal((yield* sink.latestSequence()) - sequence, 1);
      }
    }).pipe(Effect.provide(testLayer), Effect.scoped);
  },
  10_000,
);

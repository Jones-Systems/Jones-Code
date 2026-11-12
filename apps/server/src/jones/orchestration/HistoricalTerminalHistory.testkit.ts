import { assert } from "@effect/vitest";
import {
  CheckpointScopeId,
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
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { SqliteReaderAdmissionError } from "@t3tools/shared/jones/sqliteReaders/admissionFailure";
import * as Stream from "effect/Stream";
import * as RunExecutionService from "../../orchestration-v2/RunExecutionService.ts";
import * as CheckpointService from "../../orchestration-v2/CheckpointService.ts";
import * as RunFinalizationService from "../../orchestration-v2/RunFinalizationService.ts";
import * as TurnItemPositionStore from "../../orchestration-v2/TurnItemPositionStore.ts";
import * as ServerSettings from "../../serverSettings.ts";
import * as PreviewAgentActivity from "../../preview/PreviewAgentActivity.ts";
import * as McpAppModelContext from "../../mcpApps/McpAppModelContext.ts";
import { awaitRestartTerminal } from "./RestartPreparation.ts";
import * as SqlError from "effect/sql/SqlError";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as ProviderEventIngestor from "../../orchestration-v2/ProviderEventIngestor.ts";
import * as ThreadCommandExecutor from "../../orchestration-v2/ThreadCommandExecutor.ts";
import * as GitVcsDriver from "../../vcs/GitVcsDriver.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { makeProviderControlTerminalDrain } from "@t3tools/provider-core/server/jones/providerControlOutcome";
import * as ProviderEventOrigin from "@t3tools/provider-core/server/jones/ProviderEventOrigin";

const driver = ProviderDriverKind.make("codex");
export const modes = [
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
] as const;
export const historyScenario = ({
  mode,
  lockFailures = 0,
  normalizationFailure = false,
  consumer = false,
  missingEvidence = false,
  logicalOnly = false,
  uncertainPublication = false,
  readback = "found",
  accountSwitch = false,
  reusedAccount = false,
  replacementChange = "generation",
  logicalCompanion = false,
}: {
  readonly mode: (typeof modes)[number];
  readonly lockFailures?: number;
  readonly normalizationFailure?: boolean;
  readonly consumer?: boolean;
  readonly missingEvidence?: boolean;
  readonly logicalOnly?: boolean;
  readonly uncertainPublication?: boolean;
  readonly readback?: "found" | "missing" | "unavailable" | "mismatched";
  readonly accountSwitch?: boolean;
  readonly reusedAccount?: boolean;
  readonly replacementChange?:
    | "session"
    | "instance"
    | "conversation"
    | "generation"
    | "revision"
    | "old-session"
    | "old-instance";
  readonly logicalCompanion?: boolean;
}) => {
  const ordinaryRace = mode.startsWith("ordinary-") || normalizationFailure;
  const admissionFailure = new ProjectionStore.ProjectionStoreReadError({
    threadId: ThreadId.make("thread:restart-diagnostic"),
    cause: new SqliteReaderAdmissionError({ reason: "queue-timeout" }),
  });
  let pendingReadFailures = normalizationFailure ? 3 : 0;
  const receiptOrder: string[] = [];
  const consumerClosed = Deferred.makeUnsafe<void>();
  let starts = 0;
  let interrupts = 0;
  let uncertainCommitted = false;
  let readbackCalls = 0;
  let retainedInput: import("./HistoricalTerminalHistory.ts").HistoricalTerminalInput | undefined;
  const publicationFailure = new EventSink.EventSinkWriteError({ eventCount: 1, cause: null });
  const rebindSession =
    mode !== "superseded-same-session" && !mode.startsWith("ordinary-same-session");
  const ordinaryObserved = Deferred.makeUnsafe<void>();
  const releaseOrdinary = Deferred.makeUnsafe<void>();
  const committed = Deferred.makeUnsafe<void>();
  const releaseCommit = Deferred.makeUnsafe<void>();
  const validated = Deferred.makeUnsafe<void>();
  let pauseNextCommit = false;
  let historicalTransactionPending = false;
  let historicalWrites = 0;
  let historicalTransactions = 0;
  let remainingLockFailures = lockFailures;
  const lockTimeout = new SqlError.SqlError({
    reason: new SqlError.LockTimeoutError({ cause: null }),
  });
  const published: { readonly id: EventId; readonly sequence: number }[] = [];
  const database =
    mode === "locator-race" || lockFailures > 0
      ? Layer.effect(
          SqlClient.SqlClient,
          Effect.map(SqlClient.SqlClient, (sql) => {
            const withTransaction: typeof sql.withTransaction = (effect) =>
              Effect.suspend(() => {
                if (historicalTransactionPending) {
                  historicalTransactionPending = false;
                  if (!ordinaryRace || historicalWrites > 1) {
                    historicalTransactions++;
                    if (remainingLockFailures > 0) {
                      remainingLockFailures--;
                      return Effect.fail(lockTimeout);
                    }
                  }
                }
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
  const eventStoreLayer = Layer.effect(
    EventStore.EventStoreV2,
    Effect.map(EventStore.EventStoreV2, (store) =>
      EventStore.EventStoreV2.of({
        ...store,
        read: (input) => {
          if (uncertainCommitted && input?.eventId !== undefined) {
            readbackCalls++;
            if (readback === "missing") return Stream.empty;
            if (readback === "mismatched")
              return store.read(input).pipe(
                Stream.map((stored) =>
                  stored.event.type === "provider-turn.updated"
                    ? {
                        ...stored,
                        event: {
                          ...stored.event,
                          payload: { ...stored.event.payload, status: "failed" as const },
                        },
                      }
                    : stored,
                ),
              );
            if (readback === "unavailable")
              return Stream.fail(new EventStore.EventStoreReadEventsError({ cause: null }));
          }
          return store.read(input);
        },
        publishCommitted: (events) =>
          store.publishCommitted(events).pipe(
            Effect.tap(() =>
              Effect.sync(() => {
                published.push(
                  ...events
                    .filter((stored) => "event" in stored)
                    .map((stored) => ({
                      id: stored.event.id,
                      sequence: stored.sequence,
                    })),
                );
              }),
            ),
          ),
      }),
    ),
  ).pipe(Layer.provide(EventStore.layer));
  const stores = Layer.merge(eventStoreLayer, ProjectionStore.layer).pipe(
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
  const ingestionLayer = Layer.mergeAll(
    dependencies,
    ProviderEventIngestor.layer.pipe(
      Layer.updateService(ProjectionStore.ProjectionStoreV2, (projections) => ({
        ...projections,
        getPendingNativeUserInputs: (threadId, turnId) =>
          Effect.suspend(() => {
            if (pendingReadFailures === 0)
              return projections.getPendingNativeUserInputs(threadId, turnId);
            pendingReadFailures--;
            return Deferred.succeed(ordinaryObserved, undefined).pipe(
              Effect.andThen(Deferred.await(releaseOrdinary)),
              Effect.andThen(Effect.fail(admissionFailure)),
            );
          }),
      })),
      Layer.updateService(EventSink.EventSinkV2, (sink) => ({
        ...sink,
        write: (input) =>
          sink.write(input).pipe(
            Effect.flatMap((stored) => {
              if (
                !uncertainPublication ||
                uncertainCommitted ||
                input.historicalTerminal === undefined ||
                stored.length === 0
              )
                return Effect.succeed(stored);
              uncertainCommitted = true;
              return Effect.fail(publicationFailure);
            }),
          ),
        writeHistoricalTerminal: (input) =>
          Effect.suspend(() => {
            historicalWrites++;
            retainedInput = input;
            historicalTransactionPending = true;
            return sink.writeHistoricalTerminal!(input);
          }).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                historicalTransactionPending = false;
              }),
            ),
            Effect.tap((result) =>
              ordinaryRace && !normalizationFailure && !result.handled
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
  const testLayer = Layer.mergeAll(
    ingestionLayer,
    RunExecutionService.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          ingestionLayer,
          McpAppModelContext.layerEmpty,
          PreviewAgentActivity.layer,
          ServerSettings.layerTest(),
          Layer.mock(CheckpointService.CheckpointServiceV2)({ captureBaseline: () => Effect.void }),
          Layer.mock(TurnItemPositionStore.TurnItemPositionStoreV2)({
            allocate: () => Effect.succeed(1),
          }),
          Layer.succeed(RunFinalizationService.RunFinalizationObserver, {
            refresh: () => Effect.void,
            refreshAfterTurn: () => Effect.void,
          }),
        ),
      ),
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
    const accepts =
      [
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
      ].includes(mode) ||
      (normalizationFailure && mode === "superseded-same-session");
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
        ...(missingEvidence ? {} : { runtimeEvidence: capture }),
        providerTurn: terminalTurn,
      },
      {
        producer: producer.origin,
        turn: {
          binding: capture,
          runId: mode === "wrong-run" ? RunId.make("run:wrong") : ids.runId,
          attemptId: mode === "wrong-attempt" ? RunAttemptId.make("attempt:wrong") : ids.attemptId,
          providerTurnId: ids.rootProviderTurnId,
        },
      },
    );
    const rebound = {
      ...providerThread,
      ...(accountSwitch
        ? {
            id: ProviderThreadId.make("provider-thread:replacement-account"),
            providerInstanceId: ProviderInstanceId.make("codex_replacement_account"),
            runtimeIdentity: reusedAccount
              ? {
                  ...providerThread.runtimeIdentity!,
                  runtimeGeneration: "target-account-generation",
                  evidenceRevision: 7,
                }
              : undefined,
          }
        : {}),
      providerSessionId: ProviderSessionId.make("session:replacement"),
      status: "not_loaded" as const,
      updatedAt: terminalAt,
    };
    const replacementRootNodeId = NodeId.make("node:replacement");
    const successorAttempt = {
      ...successor,
      ...(accountSwitch
        ? { providerThreadId: rebound.id, providerInstanceId: rebound.providerInstanceId }
        : {}),
      rootNodeId: replacementRootNodeId,
      attemptOrdinal: mode === "unrelated-successor" ? 3 : 2,
    };
    const nextRun = {
      ...run,
      ...(accountSwitch
        ? {
            providerThreadId: rebound.id,
            providerInstanceId: rebound.providerInstanceId,
            modelSelection: { ...modelSelection, instanceId: rebound.providerInstanceId },
          }
        : {}),
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
            ...(accountSwitch ? { providerThreadId: rebound.id } : {}),
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
            ...(replacementChange.startsWith("old-") ? providerThread : rebound),
            ...(replacementChange === "session" || replacementChange === "old-session"
              ? { providerSessionId: ProviderSessionId.make("session:changed") }
              : replacementChange === "instance" || replacementChange === "old-instance"
                ? { providerInstanceId: ProviderInstanceId.make("codex_changed") }
                : replacementChange === "conversation"
                  ? {
                      nativeThreadRef: {
                        ...rebound.nativeThreadRef!,
                        nativeId: "changed-native-thread",
                      },
                    }
                  : replacementChange === "revision"
                    ? {
                        runtimeIdentity: {
                          ...rebound.runtimeIdentity!,
                          evidenceRevision: rebound.runtimeIdentity!.evidenceRevision + 1,
                        },
                      }
                    : {
                        runtimeIdentity: {
                          ...providerThread.runtimeIdentity!,
                          runtimeGeneration: "replacement-generation",
                        },
                      }),
          }),
        ],
      });
    const logicalTerminalForIngestion = ProviderEventOrigin.stampProviderEvent(
      {
        type: "turn.terminal" as const,
        driver,
        providerThreadId: providerThread.id,
        providerTurnId: turn.id,
        runOrdinal: run.ordinal,
        status: "interrupted" as const,
        failure: null,
        threadDisposition: "reusable" as const,
        providerTurn: terminalTurn,
        runtimeEvidence: capture,
      },
      ProviderEventOrigin.readProviderEventOrigin(terminalEvent)!,
    );
    const ingest = ingestor.ingestNormalized({
      providerSessionId: providerSessionId,
      providerInstanceId,
      threadId: ids.threadId,
      runId: ids.runId,
      nodeId: ids.rootNodeId,
      event: logicalOnly ? logicalTerminalForIngestion : terminalEvent,
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
    const terminalDrain = makeProviderControlTerminalDrain();
    if (consumer) {
      terminalDrain.capture(terminalEvent);
      terminalDrain.capture(logicalTerminalForIngestion);
      const execution = yield* RunExecutionService.RunExecutionServiceV2;
      yield* execution.startRootRun({
        commandId: CommandId.make("command:consumer"),
        appThread: thread,
        providerSessionId,
        session: {
          driver,
          instanceId: providerInstanceId,
          providerSessionId,
          eventOriginMode: "captured",
          events: Stream.empty,
          subscribeEvents: Effect.succeed({
            events: Stream.fromIterable([terminalEvent, logicalTerminalForIngestion]),
            close: Deferred.succeed(consumerClosed, undefined).pipe(Effect.asVoid),
          }),
          startTurn: () =>
            Effect.sync(() => {
              starts++;
            }),
          interruptTurn: () =>
            Effect.sync(() => {
              interrupts++;
            }),
        } as unknown as ProviderAdapter.ProviderAdapterV2SessionRuntime,
        run,
        rootNode,
        providerThread,
        attempt,
        attemptId: attempt.id,
        providerTurnOrdinal: 1,
        checkpointScope: {
          id: CheckpointScopeId.make("checkpoint:history"),
        } as Parameters<
          RunExecutionService.RunExecutionServiceV2["Service"]["startRootRun"]
        >[0]["checkpointScope"],
        shouldFinalizeRun: () => Effect.succeed(false),
        message: {
          messageId: run.userMessageId,
          text: "synthetic",
          attachments: [],
          createdBy: "user",
          creationSource: "web",
        },
        modelSelection,
        runtimePolicy: {
          runtimeMode: "full-access",
          interactionMode: "default",
          cwd: "/synthetic",
          approvalPolicy: "never",
          sandboxPolicy: { type: "readOnly", access: { type: "fullAccess" }, networkAccess: false },
        },
      });
    }
    const ordinaryIngestion =
      ordinaryRace && !consumer ? yield* ingest.pipe(Effect.forkScoped) : undefined;
    if (ordinaryIngestion !== undefined || consumer) {
      yield* Deferred.await(ordinaryObserved);
      receiptOrder.push("pending-read");
      yield* pendingRestart!;
      receiptOrder.push("restart-committed");
      if (
        (normalizationFailure && mode === "advanced-successor") ||
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
    const publishedAfter = () =>
      published.filter((stored) => stored.sequence > sequence).map((stored) => stored.id);
    const waiterRead = yield* Deferred.make<void>();
    const waiter = yield* awaitRestartTerminal({
      target: {
        threadId: ids.threadId,
        providerThreadId: providerThread.id,
        providerTurnId: turn.id,
        interruptedAttemptId: attempt.id,
      },
      projections: {
        ...projections,
        getProviderControlContext: (threadId, target) =>
          projections
            .getProviderControlContext(threadId, target)
            .pipe(Effect.tap(() => Deferred.succeed(waiterRead, undefined))),
      },
      sink,
    }).pipe(Effect.forkScoped);
    yield* Deferred.await(waiterRead);
    assert.isUndefined(waiter.pollUnsafe());
    if (normalizationFailure) receiptOrder.push("waiter-read");
    const result = yield* (
      consumer
        ? Deferred.succeed(releaseOrdinary, undefined).pipe(
            Effect.andThen(Deferred.await(consumerClosed)),
            Effect.as([]),
          )
        : ordinaryIngestion !== undefined
          ? Deferred.succeed(releaseOrdinary, undefined).pipe(
              Effect.andThen(Fiber.join(ordinaryIngestion)),
            )
          : mode === "locator-race"
            ? Effect.gen(function* () {
                const ingestion = yield* ingest.pipe(Effect.forkScoped);
                yield* Deferred.await(validated);
                yield* Deferred.succeed(releaseCommit, undefined);
                yield* Fiber.join(restartFiber!);
                return yield* Fiber.join(ingestion);
              })
            : ingest
    ).pipe(Effect.exit);
    if (logicalCompanion)
      yield* ingestor.ingestNormalized({
        providerSessionId,
        providerInstanceId,
        threadId: ids.threadId,
        runId: ids.runId,
        nodeId: ids.rootNodeId,
        event: logicalTerminalForIngestion,
      });
    const exhausted = lockFailures === 3;
    if (uncertainPublication && readback !== "found") {
      assert.isTrue(Exit.isFailure(result));
      if (Exit.isFailure(result)) {
        const error = Cause.squash(result.cause);
        assert.isTrue(Schema.is(ProviderEventIngestor.ProviderEventPublishError)(error));
        if (Schema.is(ProviderEventIngestor.ProviderEventPublishError)(error))
          assert.strictEqual(error.cause, publicationFailure);
      }
    }
    const rejectedError = normalizationFailure && mode !== "superseded-same-session";
    if (rejectedError) {
      assert.isTrue(Exit.isFailure(result));
      if (Exit.isFailure(result)) {
        const error = Cause.squash(result.cause);
        assert.isTrue(Schema.is(ProviderEventIngestor.ProviderEventNormalizeError)(error));
        if (Schema.is(ProviderEventIngestor.ProviderEventNormalizeError)(error))
          assert.strictEqual(error.cause, admissionFailure);
      }
    } else if (uncertainPublication && readback !== "found") {
      assert.isTrue(Exit.isFailure(result));
    } else if (exhausted) {
      assert.isTrue(Exit.isFailure(result));
      const failure = Exit.isFailure(result) ? Cause.squash(result.cause) : undefined;
      assert.instanceOf(failure, ProviderEventIngestor.ProviderEventPublishError);
      if (!Schema.is(ProviderEventIngestor.ProviderEventPublishError)(failure))
        return assert.fail("Expected the historical write's publication error");
      assert.equal(failure.providerSessionId, providerSessionId);
      assert.equal(failure.eventCount, 1);
      assert.instanceOf(failure.cause, EventSink.EventSinkWriteError);
      assert.strictEqual((failure.cause as EventSink.EventSinkWriteError).cause, lockTimeout);
    } else {
      assert.isTrue(Exit.isSuccess(result));
    }
    const stored = Exit.isSuccess(result) ? result.value : [];
    if (lockFailures > 0) {
      assert.equal(remainingLockFailures, 0);
      assert.equal(historicalTransactions, 3);
      assert.equal(historicalWrites, ordinaryRace ? 4 : 3);
    }
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
    const admitted = accepts && !exhausted && !missingEvidence && !logicalOnly;
    const expectedReasons: Readonly<Record<string, string>> = {
      "wrong-session": "identity",
      "wrong-instance": "identity",
      "wrong-generation": "origin",
      "wrong-conversation": "replacement",
      "wrong-native-turn": "projection_turn",
      "retired-producer": "origin",
      "replaced-producer": "origin",
      "missing-intent": "locator",
      "unrelated-successor": "successor",
      "advanced-successor": "successor",
      "changed-replacement": "replacement",
    };
    const expectedReason = expectedReasons[mode];
    if (expectedReason !== undefined && !exhausted)
      assert.equal(
        sink.supersededTerminalTrace!.read({
          threadId: ids.threadId,
          runId: ids.runId,
          providerThreadId: ids.providerThreadId,
          providerTurnId: ids.rootProviderTurnId,
          interruptedAttemptId: ids.attemptId,
        }),
        expectedReason,
      );
    assert.equal(after.providerTurns[0]?.status, admitted ? "interrupted" : "running");
    if (!consumer && !uncertainPublication) assert.equal(stored.length, admitted ? 1 : 0);
    assert.equal((yield* sink.latestSequence()) - sequence, admitted ? 1 : 0);
    assert.deepEqual(
      publishedAfter(),
      consumer || uncertainPublication
        ? [retainedInput!.terminal.id]
        : stored.map((stored) => stored.event.id),
    );
    if (uncertainPublication) {
      assert.equal(readbackCalls, 1);
      assert.equal(historicalWrites, 2);
    }
    if (normalizationFailure && admitted) {
      assert.strictEqual(retainedInput?.event, terminalEvent);
      const persisted = yield* (yield* EventStore.EventStoreV2)
        .read({
          eventId: retainedInput!.terminal.id,
          limit: 1,
        })
        .pipe(Stream.runCollect);
      assert.equal(persisted.length, 1);
      assert.deepEqual(persisted[0]?.event, retainedInput!.terminal);
    }
    if (admitted && mode !== "current") {
      yield* Fiber.join(waiter);
      if (normalizationFailure) {
        receiptOrder.push("terminal-durable");
        assert.deepEqual(receiptOrder, [
          "pending-read",
          "restart-committed",
          "waiter-read",
          "terminal-durable",
        ]);
        assert.equal(pendingReadFailures, 0);
        assert.deepEqual(after.providerTurns[0]?.completedAt, terminalAt);
        assert.equal(after.providerTurns[0]?.nativeTurnRef?.nativeId, "old-native-turn");
        assert.equal(after.attempts.find((row) => row.id === attempt.id)?.status, "superseded");
      }
    } else {
      assert.isUndefined(waiter.pollUnsafe());
      yield* Fiber.interrupt(waiter);
      yield* Fiber.await(waiter);
    }
    if (consumer) {
      yield* terminalDrain.awaitIngested;
      assert.equal(starts, 1);
      assert.equal(interrupts, 0);
    }
    if (mode === "duplicate" || (lockFailures === 2 && admitted && mode !== "current")) {
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
      assert.equal(publishedAfter().length, 1);
    }
  }).pipe(Effect.provide(testLayer), Effect.scoped);
};

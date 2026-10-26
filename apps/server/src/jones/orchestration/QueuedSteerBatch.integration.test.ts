import * as ThreadManagementService from "../../orchestration-v2/ThreadManagementService.ts";
import * as CommandReceiptStore from "../../orchestration-v2/CommandReceiptStore.ts";
import * as ServerSettings from "../../serverSettings.ts";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  EventId,
  NodeId,
  RuntimeRequestId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  ThreadId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Command,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as SqlClient from "effect/sql/SqlClient";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import { queuedSteerBatchCandidates, validateQueuedSteerBatch } from "./QueuedSteerBatch.ts";
import * as EffectWorker from "../../orchestration-v2/EffectWorker.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as ProviderAdapterRegistry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { queuedToolBoundaryTarget } from "../../orchestration-v2/QueuedToolBoundary.ts";

const makeQueuedProviderFixture = ({
  cwd,
  driver,
  modelSelection,
}: {
  readonly cwd: string;
  readonly driver: ProviderDriverKind;
  readonly modelSelection: { readonly instanceId: ProviderInstanceId; readonly model: string };
}) =>
  Effect.gen(function* () {
    const instanceId = modelSelection.instanceId;
    const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
    const started: ProviderAdapter.ProviderAdapterV2TurnInput[] = [];
    const steered: ProviderAdapter.ProviderAdapterV2TurnMessage[] = [];
    const interrupted: ProviderAdapter.ProviderAdapterV2InterruptInput[] = [];
    const capabilities = CodexProviderCapabilitiesV2;
    const adapter: ProviderAdapter.ProviderAdapterV2["Service"] = {
      instanceId,
      driver,
      getCapabilities: () => Effect.succeed(capabilities),
      planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
      openSession: (input) =>
        Effect.gen(function* () {
          const now = yield* DateTime.now;
          return {
            instanceId,
            driver,
            providerSessionId: input.providerSessionId,
            providerSession: {
              id: input.providerSessionId,
              driver,
              providerInstanceId: instanceId,
              status: "ready",
              cwd,
              model: modelSelection.model,
              capabilities,
              createdAt: now,
              updatedAt: now,
              lastError: null,
            },
            events: Stream.fromQueue(events),
            ensureThread: ({ threadId }) =>
              Effect.succeed({
                id: ProviderThreadId.make(`provider-thread:${threadId}`),
                driver,
                providerInstanceId: instanceId,
                providerSessionId: input.providerSessionId,
                appThreadId: threadId,
                ownerNodeId: null,
                nativeThreadRef: {
                  driver,
                  nativeId: `native-thread:${threadId}`,
                  strength: "strong",
                },
                nativeConversationHeadRef: null,
                status: "idle",
                firstRunOrdinal: null,
                lastRunOrdinal: null,
                handoffIds: [],
                forkedFrom: null,
                createdAt: now,
                updatedAt: now,
              }),
            resumeThread: ({ providerThread }) => Effect.succeed(providerThread),
            startTurn: (turn) =>
              Effect.gen(function* () {
                started.push(turn);
                yield* Queue.offer(events, {
                  type: "provider_turn.updated",
                  driver,
                  providerTurn: {
                    id: ProviderTurnId.make(`provider-turn:${turn.attemptId}`),
                    providerThreadId: turn.providerThread.id,
                    nodeId: turn.rootNodeId,
                    runAttemptId: turn.attemptId,
                    nativeTurnRef: {
                      driver,
                      nativeId: `native:${turn.attemptId}`,
                      strength: "strong",
                    },
                    ordinal: turn.providerTurnOrdinal,
                    status: "running",
                    startedAt: now,
                    completedAt: null,
                  },
                });
              }),
            steerTurn: (turn) =>
              Effect.sync(() => {
                steered.push(turn.message);
              }),
            interruptTurn: (turn) =>
              Effect.sync(() => {
                interrupted.push(turn);
              }),
            respondToRuntimeRequest: () => Effect.void,
            readThreadSnapshot: () => Effect.die("unused"),
            rollbackThread: () => Effect.die("unused"),
            forkThread: () => Effect.die("unused"),
          };
        }),
    };
    return { adapter, events, started, steered, interrupted };
  });

it.effect.each(["manual", "automatic", "completed", "switched"] as const)(
  "originals use one batch effect and preserve delivery on %s",
  (mode) =>
    Effect.scoped(
      Effect.gen(function* () {
        const count = mode === "switched" ? 4 : 22;
        const driver = ProviderDriverKind.make("codex");
        const instanceId = ProviderInstanceId.make("codex");
        const modelSelection = { instanceId, model: "test-model" };
        const cwd = yield* checkpointWorkspace("queued-steer-batch");
        const { adapter, started, steered } = yield* makeQueuedProviderFixture({
          cwd,
          driver,
          modelSelection,
        });
        const database = SqlitePersistence.layerMemory;
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const sql = yield* SqlClient.SqlClient;
          const threadId = ThreadId.make("thread:queued-batch");
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("create"),
            threadId,
            projectId: ProjectId.make("project:batch"),
            title: "Batch",
            modelSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: cwd,
            createdBy: "user",
            creationSource: "web",
          });
          const running = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event: OrchestrationV2DomainEvent) =>
                event.type === "provider-turn.updated" && event.payload.status === "running",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("first"),
            threadId,
            messageId: MessageId.make("message:first"),
            text: "first",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          yield* worker.drain();
          yield* Fiber.join(running);
          const threads = yield* ThreadManagementService.ThreadManagementService.pipe(
            Effect.provide(
              ThreadManagementService.layer.pipe(
                Layer.provide(ServerSettings.layerTest()),
                Layer.provide(CommandReceiptStore.layer.pipe(Layer.provide(database))),
              ),
            ),
          );
          for (let i = 0; i < count; i++) {
            const input = {
              commandId: CommandId.make(`queue:${i}`),
              projectId: ProjectId.make("project:batch"),
              threadId,
              messageId: MessageId.make(`message:${i}`),
              text: `queued input ${i}`,
              attachments: [],
              createdBy: "agent" as const,
              creationSource: "mcp" as const,
              senderThreadId: ThreadId.make(`sender:${i}`),
            };
            if (mode === "automatic") yield* threads.sendToThread({ ...input, mode: "queue" });
            else
              yield* orchestrator.dispatch({
                ...input,
                type: "message.dispatch",
                dispatchMode: { type: "queue_after_active" },
              });
          }
          if (mode === "switched") {
            const projection = yield* orchestrator.getThreadProjection(threadId);
            const sink = yield* EventSink.EventSinkV2;
            const now = yield* DateTime.now;
            yield* sink.write({
              events: projection.runs
                .filter((run) => run.status === "queued")
                .map((run, i) => ({
                  id: EventId.make(`historical-config:${i}`),
                  type: "run.updated" as const,
                  threadId,
                  runId: run.id,
                  occurredAt: now,
                  payload: {
                    ...run,
                    providerInstanceId: ProviderInstanceId.make("previous-account"),
                    providerThreadId: ProviderThreadId.make("previous-thread"),
                    modelSelection: {
                      instanceId: ProviderInstanceId.make("previous-account"),
                      model: "previous-model",
                      options: [{ id: "reasoningEffort", value: "low" }],
                    },
                  },
                })),
            });
          }
          const before = yield* orchestrator.getThreadProjection(threadId);
          const command: Extract<
            OrchestrationV2Command,
            { type: "queued-message.promote-batch-to-steer" }
          > = {
            type: "queued-message.promote-batch-to-steer",
            commandId: CommandId.make("batch"),
            threadId,
            targetRunId: started[0]!.runId,
            items: before.runs
              .filter((run) => run.status === "queued")
              .map((run) => ({
                queuedRunId: run.id,
                messageId: run.userMessageId,
                messageUpdatedAt: DateTime.formatIso(
                  before.messages.find((message) => message.id === run.userMessageId)!.updatedAt,
                ),
              })),
          };
          for (const index of [0, 1]) {
            for (const field of ["instance", "thread", "model", "options"] as const) {
              const changed = {
                ...before,
                runs: before.runs.map((run) =>
                  run.id !== command.items[index]!.queuedRunId
                    ? run
                    : {
                        ...run,
                        ...(field === "instance"
                          ? { providerInstanceId: ProviderInstanceId.make("old-account") }
                          : {}),
                        ...(field === "thread"
                          ? { providerThreadId: ProviderThreadId.make("old-thread") }
                          : {}),
                        ...(field === "model"
                          ? { modelSelection: { ...run.modelSelection, model: "old-model" } }
                          : {}),
                        ...(field === "options"
                          ? {
                              modelSelection: {
                                ...run.modelSelection,
                                options: [{ id: "reasoningEffort", value: "low" }],
                              },
                            }
                          : {}),
                      },
                ),
              };
              assert.isFalse("error" in validateQueuedSteerBatch(changed, command));
              assert.isTrue("error" in validateQueuedSteerBatch(changed, command, true));
              assert.isFalse(
                queuedSteerBatchCandidates(changed, command.targetRunId, true)[index]!.eligible,
              );
            }
          }
          const firstMessage = before.messages.find(
            (message) => message.id === command.items[0]!.messageId,
          )!;
          for (const text of ["/compact", "/logout", "/goal continue", "x".repeat(120_000)]) {
            const invalid = {
              ...before,
              messages: before.messages.map((message) =>
                message.id === firstMessage.id ? { ...message, text } : message,
              ),
            };
            assert.isTrue("error" in validateQueuedSteerBatch(invalid, command));
          }
          assert.isTrue(
            "error" in
              validateQueuedSteerBatch(
                {
                  ...before,
                  runtimeRequests: [
                    {
                      id: RuntimeRequestId.make("pending"),
                      nodeId: before.runs[0]!.rootNodeId!,
                      providerTurnId: before.providerTurns[0]!.id,
                      nativeRequestRef: null,
                      kind: "user_input",
                      status: "pending",
                      responseCapability: { type: "message" },
                      createdAt: firstMessage.createdAt,
                      resolvedAt: null,
                    },
                  ],
                },
                command,
              ),
          );
          assert.isTrue(
            "error" in
              validateQueuedSteerBatch(
                {
                  ...before,
                  thread: {
                    ...before.thread,
                    modelSelection: { ...modelSelection, model: "changed-model" },
                  },
                },
                command,
              ),
          );
          assert.isTrue(
            "error" in
              validateQueuedSteerBatch(
                {
                  ...before,
                  providerSessions: before.providerSessions.map((session) => ({
                    ...session,
                    capabilities: {
                      ...session.capabilities,
                      turns: { ...session.capabilities.turns, supportsActiveSteering: false },
                    },
                  })),
                },
                command,
              ),
          );
          assert.isTrue(
            "error" in
              validateQueuedSteerBatch(before, { ...command, items: [...command.items].reverse() }),
          );
          assert.isTrue(
            "error" in
              validateQueuedSteerBatch(before, {
                ...command,
                items: [command.items[0]!, command.items[0]!],
              }),
          );
          assert.isTrue(
            "error" in
              validateQueuedSteerBatch(before, {
                ...command,
                items: [{ ...command.items[0]!, messageUpdatedAt: "edited" }],
              }),
          );
          const stale = yield* orchestrator
            .dispatch({
              ...command,
              commandId: CommandId.make("stale"),
              items: [{ ...command.items[0]!, messageUpdatedAt: "edited" }],
            })
            .pipe(Effect.result);
          assert.equal(stale._tag, "Failure");
          assert.equal(
            (yield* orchestrator.getThreadProjection(threadId)).runs.filter(
              (run) => run.status === "queued",
            ).length,
            count,
          );
          const sink = yield* EventSink.EventSinkV2;
          if (mode === "automatic") {
            yield* threads.sendToThread({
              commandId: CommandId.make("separate"),
              projectId: ProjectId.make("project:batch"),
              threadId,
              messageId: MessageId.make("separate"),
              text: "Strictly separate",
              attachments: [],
              createdBy: "agent",
              creationSource: "mcp",
              senderThreadId: ThreadId.make("peer"),
              mode: "separate_turn",
            });
            const promoted = yield* orchestrator.streamDomainEvents.pipe(
              Stream.filter(
                (event) =>
                  event.type === "message.updated" &&
                  event.payload.id === command.items.at(-1)!.messageId,
              ),
              Stream.take(1),
              Stream.runDrain,
              Effect.forkScoped,
            );
            const run = before.runs.find((row) => row.id === command.targetRunId)!;
            const now = yield* DateTime.now;
            yield* sink.write({
              events: [
                {
                  id: EventId.make("boundary"),
                  type: "node.updated",
                  threadId,
                  runId: run.id,
                  occurredAt: now,
                  payload: {
                    id: NodeId.make("tool:boundary"),
                    threadId,
                    runId: run.id,
                    parentNodeId: run.rootNodeId,
                    rootNodeId: run.rootNodeId!,
                    kind: "tool_call",
                    status: "completed",
                    countsForRun: false,
                    providerThreadId: run.providerThreadId,
                    providerTurnId: before.providerTurns[0]!.id,
                    nativeItemRef: { driver, nativeId: "tool:boundary", strength: "strong" },
                    runtimeRequestId: null,
                    checkpointScopeId: null,
                    startedAt: now,
                    completedAt: now,
                  },
                },
              ],
            });
            yield* Fiber.join(promoted);
          } else {
            yield* orchestrator.dispatch(command);
            yield* orchestrator.dispatch(command);
          }
          const effects =
            yield* sql`SELECT effect_type FROM orchestration_v2_effect_outbox WHERE effect_type = 'provider-turn.steer-batch'`;
          assert.deepEqual(
            effects.map((effect) => effect.effect_type),
            ["provider-turn.steer-batch"],
          );
          if (mode === "completed") {
            const turn = before.providerTurns[0]!;
            const run = before.runs.find((row) => row.id === command.targetRunId)!;
            const now = yield* DateTime.now;
            yield* sink.write({
              events: [
                {
                  id: EventId.make("turn:completed"),
                  type: "provider-turn.updated",
                  threadId,
                  runId: run.id,
                  occurredAt: now,
                  payload: { ...turn, status: "completed", completedAt: now },
                },
                {
                  id: EventId.make("run:completed"),
                  type: "run.updated",
                  threadId,
                  runId: run.id,
                  occurredAt: now,
                  payload: { ...run, status: "completed", completedAt: now },
                },
              ],
            });
            yield* worker.drain();
            const recovered = yield* orchestrator.getThreadProjection(threadId);
            assert.equal(started.length, 2);
            assert.equal(steered.length, 0);
            assert.equal(started[1]!.message.messageId, command.items[0]!.messageId);
            assert.equal(recovered.runs.filter((row) => row.status === "queued").length, 21);
            return;
          }
          yield* worker.drain();
          assert.equal(steered.length, 1);
          for (let i = 0; i < count; i++)
            assert.include(steered[0]!.text, `sender: thread sender:${i}`);
          const after = yield* orchestrator.getThreadProjection(threadId);
          assert.equal(
            after.runs.filter((run) => run.status === "queued").length,
            mode === "automatic" ? 1 : 0,
          );
          if (mode === "automatic")
            assert.isUndefined(
              after.runs.find((run) => run.userMessageId === "separate")
                ?.queuedToolBoundaryEligible,
            );
          const promoted = after.turnItems.filter(
            (item) =>
              item.type === "user_message" && item.inputIntent === "promoted_queued_to_steer",
          );
          assert.equal(promoted.length, count);
          assert.equal(new Set(promoted.map((item) => item.ordinal)).size, count);
          for (const item of command.items)
            assert.equal(
              after.messages.find((message) => message.id === item.messageId)?.runId,
              command.targetRunId,
            );
        }).pipe(
          Effect.provide(
            Layer.merge(
              database,
              ProviderReplayHarness.layerWithRegistry(
                { name: "queued-steer-batch" },
                ProviderAdapterRegistry.layerFromAdapters([adapter]),
                { databaseLayer: database, runEffectWorker: false },
              ),
            ),
          ),
        );
      }),
    ),
);

it.effect.each(["codex", "cursor"] as const)(
  "keeps an opted-out monitor reply queued through a real tool completion on %s",
  (driverName) =>
    Effect.scoped(
      Effect.gen(function* () {
        const driver = ProviderDriverKind.make(driverName);
        const instanceId = ProviderInstanceId.make(driverName);
        const modelSelection = { instanceId, model: "test-model" };
        const cwd = yield* checkpointWorkspace(`monitor-queue-optout-${driverName}`);
        const { adapter, events, started, steered, interrupted } = yield* makeQueuedProviderFixture(
          { cwd, driver, modelSelection },
        );
        const database = SqlitePersistence.layerMemory;
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const sink = yield* EventSink.EventSinkV2;
          const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
          const sql = yield* SqlClient.SqlClient;
          const threadId = ThreadId.make("thread:monitor-optout");
          const controlThreadId = ThreadId.make("thread:composer-control");
          for (const id of [threadId, controlThreadId]) {
            yield* orchestrator.dispatch({
              type: "thread.create",
              commandId: CommandId.make(`create:${id}`),
              threadId: id,
              projectId: ProjectId.make("project:monitor-optout"),
              title: id,
              modelSelection,
              runtimeMode: "full-access",
              interactionMode: "default",
              branch: null,
              worktreePath: cwd,
              createdBy: "user",
              creationSource: "web",
            });
            const running = yield* orchestrator.streamDomainEvents.pipe(
              Stream.filter(
                (event) =>
                  event.threadId === id &&
                  event.type === "provider-turn.updated" &&
                  event.payload.status === "running",
              ),
              Stream.take(1),
              Stream.runDrain,
              Effect.forkScoped,
            );
            yield* orchestrator.dispatch({
              type: "message.dispatch",
              commandId: CommandId.make(`first:${id}`),
              threadId: id,
              messageId: MessageId.make(`first-message:${id}`),
              text: "Keep the original turn running",
              attachments: [],
              dispatchMode: { type: "start_immediately" },
              createdBy: "user",
              creationSource: "web",
            });
            yield* worker.drain();
            yield* Fiber.join(running);
          }

          // Bind the client opt-out wire shape to actual server delivery without a client dependency.
          // Both modern and legacy context omit eligibility while preserving web provenance.
          const monitorCommand = {
            type: "message.dispatch",
            commandId: CommandId.make("monitor:original-command"),
            threadId,
            messageId: MessageId.make("monitor:original-message"),
            text: "Monitor reply must wait for its own turn",
            attachments: [],
            modelSelection,
            dispatchMode: { type: "queue_after_active" },
            createdBy: "user",
            creationSource: "web",
          } as const satisfies OrchestrationV2Command;
          const accepted = yield* orchestrator.dispatch(monitorCommand);
          const acceptedReceipt = Option.getOrThrow(
            yield* receipts.getByCommandId(monitorCommand.commandId),
          );
          assert.equal(acceptedReceipt.status, "accepted");
          assert.equal(acceptedReceipt.commandId, monitorCommand.commandId);
          assert.equal(acceptedReceipt.commandType, "message.dispatch");
          assert.equal(acceptedReceipt.threadId, threadId);
          assert.equal(acceptedReceipt.resultSequence, accepted.sequence);
          const before = yield* orchestrator.getThreadProjection(threadId);
          const activeRun = before.runs.find((run) => run.status === "running")!;
          const originalTurn = before.providerTurns.find(
            (turn) => turn.runAttemptId === activeRun.activeAttemptId,
          )!;
          const queuedRun = before.runs.find(
            (run) => run.userMessageId === monitorCommand.messageId,
          )!;
          assert.equal(originalTurn.status, "running");
          assert.equal(queuedRun.status, "queued");
          assert.isUndefined(queuedRun.queuedToolBoundaryEligible);
          assert.isTrue(before.providerSessions[0]!.capabilities.turns.supportsActiveSteering);
          assert.isTrue(before.providerSessions[0]!.capabilities.turns.supportsQueuedMessages);

          const controlMessageId = MessageId.make("composer:eligible-message");
          yield* orchestrator.dispatch({
            ...monitorCommand,
            commandId: CommandId.make("composer:eligible-command"),
            threadId: controlThreadId,
            messageId: controlMessageId,
            text: "Eligible composer positive control",
            queuedToolBoundaryEligible: true,
          });

          const completeRootTool = (id: ThreadId) =>
            Effect.gen(function* () {
              const projection = yield* orchestrator.getThreadProjection(id);
              const run = projection.runs.find((row) => row.status === "running")!;
              const turn = projection.providerTurns.find(
                (row) => row.runAttemptId === run.activeAttemptId,
              )!;
              const now = yield* DateTime.now;
              const tool = {
                id: NodeId.make(`tool:${id}`),
                threadId: id,
                runId: run.id,
                parentNodeId: run.rootNodeId,
                rootNodeId: run.rootNodeId!,
                kind: "tool_call",
                countsForRun: false,
                providerThreadId: run.providerThreadId,
                providerTurnId: turn.id,
                nativeItemRef: { driver, nativeId: `tool:${id}`, strength: "strong" },
                runtimeRequestId: null,
                checkpointScopeId: null,
                startedAt: now,
              } as const;
              for (const status of ["running", "completed"] as const) {
                const persisted = yield* orchestrator.streamDomainEvents.pipe(
                  Stream.filter(
                    (event) =>
                      event.threadId === id &&
                      event.type === "node.updated" &&
                      event.payload.id === tool.id &&
                      event.payload.status === status,
                  ),
                  Stream.take(1),
                  Stream.runDrain,
                  Effect.forkScoped,
                );
                yield* Queue.offer(events, {
                  type: "node.updated",
                  driver,
                  node: { ...tool, status, completedAt: status === "completed" ? now : null },
                });
                yield* Fiber.join(persisted);
              }
              return { ...tool, status: "completed" as const, completedAt: now };
            });

          const completedTool = yield* completeRootTool(threadId);
          const atBoundary = yield* orchestrator.getThreadProjection(threadId);
          // With only the opt-out reversed, this exact running root/turn/tool qualifies.
          assert.isNotNull(
            queuedToolBoundaryTarget(
              {
                ...atBoundary,
                runs: atBoundary.runs.map((run) =>
                  run.id === queuedRun.id ? { ...run, queuedToolBoundaryEligible: true } : run,
                ),
              },
              completedTool,
            ),
          );

          // The real boundary consumer uses serial runForEach. This later control promotion
          // proves it finished the monitor boundary, including a no-op, before assertions.
          // Merely awaiting node persistence or draining the effect worker cannot prove that.
          const controlPromoted = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.threadId === controlThreadId &&
                event.type === "message.updated" &&
                event.payload.id === controlMessageId,
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
          yield* completeRootTool(controlThreadId);
          yield* Fiber.join(controlPromoted);
          yield* worker.drain();
          const controlPromotions = yield* sql<{ command_type: string }>`
            SELECT command_type FROM orchestration_command_receipts
            WHERE aggregate_id = ${controlThreadId} AND status = 'accepted'
              AND command_type IN ('queued-message.promote-batch-to-steer', 'queued-message.promote-to-steer')`;
          assert.deepEqual(
            controlPromotions.map((receipt) => receipt.command_type),
            [
              driverName === "codex"
                ? "queued-message.promote-batch-to-steer"
                : "queued-message.promote-to-steer",
            ],
          );
          assert.equal(steered.length, 1);
          assert.include(steered[0]!.text, "Eligible composer positive control");
          assert.notInclude(steered[0]!.text, monitorCommand.text);
          assert.deepEqual(interrupted, []);

          const assertNoMonitorPromotion = Effect.gen(function* () {
            const promotions = yield* sql`
              SELECT command_id FROM orchestration_command_receipts
              WHERE aggregate_id = ${threadId}
                AND command_type IN ('queued-message.promote-batch-to-steer', 'queued-message.promote-to-steer')`;
            assert.deepEqual(promotions, []);
            const controls = yield* sql`
              SELECT effect_id FROM orchestration_v2_effect_outbox
              WHERE thread_id = ${threadId}
                AND effect_type IN ('provider-turn.steer', 'provider-turn.steer-batch', 'provider-turn.interrupt', 'provider-turn.restart')`;
            assert.deepEqual(controls, []);
            const projection = yield* orchestrator.getThreadProjection(threadId);
            assert.isFalse(
              projection.turnItems.some(
                (item) =>
                  item.type === "user_message" && item.inputIntent === "promoted_queued_to_steer",
              ),
            );
          });
          yield* assertNoMonitorPromotion;
          const afterBoundary = yield* orchestrator.getThreadProjection(threadId);
          assert.equal(
            afterBoundary.runs.find((run) => run.id === activeRun.id)?.status,
            "running",
          );
          assert.equal(afterBoundary.runs.find((run) => run.id === queuedRun.id)?.status, "queued");
          assert.equal(
            afterBoundary.messages.find((message) => message.id === monitorCommand.messageId)
              ?.runId,
            queuedRun.id,
          );
          assert.equal(started.filter((turn) => turn.threadId === threadId).length, 1);
          assert.equal((yield* orchestrator.dispatch(monitorCommand)).sequence, accepted.sequence);

          const waiting = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.threadId === threadId &&
                event.type === "run.updated" &&
                event.payload.id === activeRun.id &&
                event.payload.status === "waiting",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
          const queuedStarting = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.threadId === threadId &&
                event.type === "run.updated" &&
                event.payload.id === queuedRun.id &&
                event.payload.status === "starting",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
          const delivered = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.threadId === threadId &&
                event.type === "provider-turn.updated" &&
                event.payload.runAttemptId === queuedRun.activeAttemptId &&
                event.payload.status === "running",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
          const now = yield* DateTime.now;
          yield* Queue.offer(events, {
            type: "turn.terminal",
            driver,
            providerThreadId: originalTurn.providerThreadId,
            providerTurnId: originalTurn.id,
            providerTurn: { ...originalTurn, status: "completed", completedAt: now },
            runOrdinal: activeRun.ordinal,
            status: "completed",
            failure: null,
            threadDisposition: "reusable",
          });
          yield* Fiber.join(waiting);
          yield* worker.drain();
          yield* Fiber.join(queuedStarting);
          yield* worker.drain();
          yield* Fiber.join(delivered);
          yield* worker.drain();
          assert.equal((yield* orchestrator.dispatch(monitorCommand)).sequence, accepted.sequence);
          yield* worker.drain();

          const after = yield* orchestrator.getThreadProjection(threadId);
          const monitorDeliveries = started.filter(
            (turn) =>
              turn.threadId === threadId && turn.message.messageId === monitorCommand.messageId,
          );
          assert.equal(started.filter((turn) => turn.threadId === threadId).length, 2);
          assert.equal(monitorDeliveries.length, 1);
          assert.equal(monitorDeliveries[0]!.runId, queuedRun.id);
          assert.equal(monitorDeliveries[0]!.message.messageId, monitorCommand.messageId);
          assert.equal(monitorDeliveries[0]!.message.text, monitorCommand.text);
          assert.equal(monitorDeliveries[0]!.message.createdBy, "user");
          assert.equal(monitorDeliveries[0]!.message.creationSource, "web");
          assert.deepEqual(monitorDeliveries[0]!.modelSelection, modelSelection);
          assert.equal(monitorDeliveries[0]!.runtimePolicy.runtimeMode, before.thread.runtimeMode);
          assert.equal(
            monitorDeliveries[0]!.runtimePolicy.interactionMode,
            before.thread.interactionMode,
          );
          assert.equal(after.runs.find((run) => run.id === activeRun.id)?.status, "completed");
          assert.equal(after.runs.find((run) => run.id === queuedRun.id)?.status, "running");
          assert.equal(
            after.runs.filter((run) => run.userMessageId === monitorCommand.messageId).length,
            1,
          );
          assert.equal(
            after.messages.filter((message) => message.id === monitorCommand.messageId).length,
            1,
          );
          assert.equal(
            after.messages.find((message) => message.id === monitorCommand.messageId)?.runId,
            queuedRun.id,
          );
          assert.equal(
            after.turnItems.filter(
              (item) =>
                item.type === "user_message" &&
                item.messageId === monitorCommand.messageId &&
                item.runId === queuedRun.id &&
                item.inputIntent === "queued_turn",
            ).length,
            1,
          );
          const receipt = yield* receipts.getByCommandId(monitorCommand.commandId);
          assert.isTrue(Option.isSome(receipt));
          assert.deepEqual(Option.getOrThrow(receipt), acceptedReceipt);
          const births = yield* sink
            .readByCommandId({ commandId: monitorCommand.commandId })
            .pipe(Stream.runCollect);
          assert.equal(
            births.filter(
              (stored) =>
                stored.commandId === monitorCommand.commandId &&
                stored.event.type === "message.updated" &&
                stored.event.payload.id === monitorCommand.messageId,
            ).length,
            1,
          );
          assert.equal(
            births.filter(
              (stored) =>
                stored.commandId === monitorCommand.commandId &&
                stored.event.type === "run.created" &&
                stored.event.payload.id === queuedRun.id &&
                stored.event.payload.userMessageId === monitorCommand.messageId,
            ).length,
            1,
          );
          const starts = yield* sql<{ status: string }>`
            SELECT status FROM orchestration_v2_effect_outbox
            WHERE thread_id = ${threadId} AND effect_type = 'provider-turn.start'
              AND json_extract(payload_json, '$.runId') = ${queuedRun.id}`;
          assert.deepEqual(
            starts.map((effect) => effect.status),
            ["succeeded"],
          );
          yield* assertNoMonitorPromotion;
          assert.equal(steered.length, 1);
          assert.deepEqual(interrupted, []);
        }).pipe(
          Effect.provide(
            Layer.mergeAll(
              database,
              CommandReceiptStore.layer.pipe(Layer.provide(database)),
              ProviderReplayHarness.layerWithRegistry(
                { name: `monitor-queue-optout-${driverName}` },
                ProviderAdapterRegistry.layerFromAdapters([adapter]),
                { databaseLayer: database, runEffectWorker: false },
              ),
            ),
          ),
        );
      }),
    ),
);

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
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as SqlClient from "effect/sql/SqlClient";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import { validateQueuedSteerBatch } from "./QueuedSteerBatch.ts";
import * as EffectWorker from "../../orchestration-v2/EffectWorker.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as ProviderAdapterRegistry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";

it.effect.each(["manual", "automatic", "completed"] as const)(
  "22 originals use one batch effect and preserve delivery on %s",
  (mode) =>
    Effect.scoped(
      Effect.gen(function* () {
        const driver = ProviderDriverKind.make("codex");
        const instanceId = ProviderInstanceId.make("codex");
        const modelSelection = { instanceId, model: "test-model" };
        const cwd = yield* checkpointWorkspace("queued-steer-batch");
        const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
        const started: ProviderAdapter.ProviderAdapterV2TurnInput[] = [];
        const steered: ProviderAdapter.ProviderAdapterV2TurnMessage[] = [];
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
                    nativeThreadRef: { driver, nativeId: "native-thread", strength: "strong" },
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
                interruptTurn: () => Effect.void,
                respondToRuntimeRequest: () => Effect.void,
                readThreadSnapshot: () => Effect.die("unused"),
                rollbackThread: () => Effect.die("unused"),
                forkThread: () => Effect.die("unused"),
              };
            }),
        };
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
          for (let i = 0; i < 22; i++) {
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
            22,
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
          for (let i = 0; i < 22; i++)
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
          assert.equal(promoted.length, 22);
          assert.equal(new Set(promoted.map((item) => item.ordinal)).size, 22);
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
  30_000,
);

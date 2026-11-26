import * as ThreadManagementService from "../../orchestration-v2/ThreadManagementService.ts";
import * as CommandReceiptStore from "../../orchestration-v2/CommandReceiptStore.ts";
import * as ServerSettings from "../../serverSettings.ts";
import { assert, it, vi } from "@effect/vitest";
import {
  AuthSessionId,
  ChatAttachmentId,
  type ChatAttachment,
  CommandId,
  MessageId,
  EventId,
  EnvironmentAuthenticatedPrincipal,
  NodeId,
  RuntimeRequestId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  ThreadId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2Command,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as TestClock from "effect/testing/TestClock";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as AuthSessions from "../../persistence/AuthSessions.ts";
import * as SqlClient from "effect/sql/SqlClient";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import { queuedSteerBatchCandidates, validateQueuedSteerBatch } from "./QueuedSteerBatch.ts";
import * as EffectWorker from "../../orchestration-v2/EffectWorker.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as ProviderAdapterRegistry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as ProviderTurnStartService from "../../orchestration-v2/ProviderTurnStartService.ts";
import * as EffectOutbox from "../../orchestration-v2/EffectOutbox.ts";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { queuedToolBoundaryTarget } from "../../orchestration-v2/QueuedToolBoundary.ts";
import * as RuntimeStop from "../runtime/RuntimeStop.ts";
import { makeRuntimeStopMethods } from "../runtime/RuntimeStopSqlite.ts";
import { withOwnerSelectionIngress } from "../threadConfiguration/ownerIngress.ts";

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
          const lifecycle = input.runtimeLifecycle;
          if (lifecycle === undefined)
            return yield* Effect.die("Synthetic adapter requires the manager's actual lifecycle");
          const runtimeGeneration = yield* lifecycle.reserve(input.threadId);
          yield* lifecycle.admit({ runtimeGeneration, threadId: input.threadId });
          const bindThread = (providerThread: OrchestrationV2ProviderThread) =>
            lifecycle.bind({
              providerThread,
              runtimeGeneration,
              requested: ProviderAdapter.requestedRuntimeIdentity(input.modelSelection, driver),
              observed: ProviderAdapter.unobservedRuntimeIdentity(),
            });
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
            ensureThread: ({ threadId, existingProviderThread }) =>
              bindThread({
                id:
                  existingProviderThread?.id ??
                  ProviderThreadId.make(`provider-thread:${threadId}`),
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
            resumeThread: ({ providerThread }) => bindThread(providerThread),
            startTurn: (turn) =>
              Effect.gen(function* () {
                assert.isDefined(turn.revalidateStartAdmission);
                yield* turn.revalidateStartAdmission!;
                started.push(turn);
                const now = yield* DateTime.now;
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
            captureRuntimeStop: (provider) => {
              const { runtimeGeneration, evidenceRevision } = provider.runtimeIdentity ?? {};
              if (runtimeGeneration === undefined || evidenceRevision === undefined)
                return Effect.succeed(null);
              const binding = ProviderAdapter.runtimeBinding(provider, runtimeGeneration);
              return binding === undefined
                ? Effect.succeed(null)
                : Effect.succeed({
                    binding,
                    evidenceRevision,
                    isCurrent: Effect.succeed(true),
                    stop: Effect.die("This test stops at durable acceptance"),
                  });
            },
          };
        }),
    };
    return { adapter, events, started, steered, interrupted };
  });

it.effect("replay start requires the original worker claim and accepted command receipt", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace("queued-start-admission");
      const driver = ProviderDriverKind.make("codex");
      const modelSelection = { instanceId: ProviderInstanceId.make("codex"), model: "test-model" };
      const { adapter, started } = yield* makeQueuedProviderFixture({
        cwd,
        driver,
        modelSelection,
      });
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const starts = yield* ProviderTurnStartService.ProviderTurnStartServiceV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const outbox = yield* EffectOutbox.EffectOutboxV2;
        const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
        const threadId = ThreadId.make("thread:original-start-admission");
        const commandId = CommandId.make("original-start-admission:start");
        yield* orchestrator.dispatch({
          type: "thread.create",
          commandId: CommandId.make("original-start-admission:create"),
          threadId,
          projectId: ProjectId.make("project:batch"),
          title: "Original admission",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: cwd,
          createdBy: "user",
          creationSource: "web",
        });
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId,
          threadId,
          messageId: MessageId.make("original-start-admission:message"),
          text: "guarded start",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
        });
        const run = (yield* orchestrator.getThreadProjection(threadId)).runs[0]!;
        const receipt = yield* receipts.getByCommandId(commandId);
        assert.isTrue(Option.isSome(receipt));
        if (Option.isNone(receipt)) return;
        assert.equal(receipt.value.status, "accepted");
        const pending = (yield* outbox.listByThreadId(threadId)).filter(
          (effect) => effect.request.type === "provider-turn.start",
        );
        assert.lengthOf(pending, 1);
        assert.equal(pending[0]!.status, "pending");
        assert.equal(pending[0]!.attemptCount, 0);
        const unclaimed = yield* starts.start({ threadId, runId: run.id }).pipe(Effect.result);
        assert.equal(unclaimed._tag, "Failure");
        assert.lengthOf(started, 0);
        assert.equal(
          (yield* orchestrator.getThreadProjection(threadId)).runs[0]!.status,
          "starting",
        );
        yield* worker.drain();
        assert.lengthOf(started, 1);
        assert.equal(started[0]!.runId, run.id);
        assert.equal(started[0]!.attemptId, run.activeAttemptId);
        const completed = yield* outbox.get(pending[0]!.id);
        assert.isTrue(Option.isSome(completed));
        if (Option.isNone(completed)) return;
        assert.equal(completed.value.status, "succeeded");
        assert.equal(completed.value.attemptCount, 1);
        assert.equal(completed.value.commandId, commandId);
        assert.deepEqual(yield* receipts.getByCommandId(commandId), receipt);
        const expiredAdmission = yield* started[0]!.revalidateStartAdmission!.pipe(Effect.result);
        assert.equal(
          expiredAdmission._tag,
          "Failure",
          "A settled claim cannot authorize another start",
        );
      }).pipe(
        Effect.provide(
          // The receipt reader shares the harness database, so it reads the orchestrator's receipts.
          CommandReceiptStore.layer.pipe(
            Layer.provideMerge(
              ProviderReplayHarness.layerWithRegistryAndServices(
                { name: "queued-start-admission" },
                ProviderAdapterRegistry.layerFromAdapters([adapter]),
                { runEffectWorker: false },
              ),
            ),
          ),
        ),
      );
    }),
  ),
);

it.effect.each([
  { action: "resume", intervening: "stop" },
  { action: "resume", intervening: "stop held queue" },
  { action: "reorder", intervening: "stop" },
  { action: "single steer", intervening: "stop" },
  { action: "batch steer", intervening: "stop" },
  { action: "single steer", intervening: "stop before planning" },
  { action: "batch steer", intervening: "stop before planning" },
  { action: "cancel", intervening: "stop before planning" },
  { action: "single steer", intervening: "successor generation" },
  { action: "batch steer", intervening: "successor generation" },
  { action: "batch steer", intervening: "pending request" },
  { action: "batch steer", intervening: "new queue leader" },
  { action: "batch steer", intervening: "edit" },
  { action: "batch steer", intervening: "receipt" },
  { action: "single steer", intervening: "unrelated edit" },
  { action: "batch steer", intervening: "unrelated edit" },
  { action: "resume", intervening: "none" },
  { action: "single steer", intervening: "none" },
  { action: "batch steer", intervening: "none" },
] as const)(
  "queue action $action accepts only its planned inputs ($intervening)",
  ({ action, intervening }) =>
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace("queue-action-stop");
      const threadId = ThreadId.make("thread:queue-action-stop");
      const driver = ProviderDriverKind.make("codex");
      const modelSelection = { instanceId: ProviderInstanceId.make("codex"), model: "test-model" };
      const { adapter, steered } = yield* makeQueuedProviderFixture({
        cwd,
        driver,
        modelSelection,
      });
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const outbox = yield* EffectOutbox.EffectOutboxV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const sink = yield* EventSink.EventSinkV2;
        const sql = yield* SqlClient.SqlClient;
        const read = orchestrator.getThreadProjection(threadId);
        yield* orchestrator.dispatch({
          type: "thread.create",
          commandId: CommandId.make("queue-action:create"),
          threadId,
          projectId: ProjectId.make("project:queue-action"),
          title: "Queue action",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: cwd,
          createdBy: "user",
          creationSource: "web",
        });
        const send = (name: string) =>
          orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make(`queue-action:send:${name}`),
            threadId,
            messageId: MessageId.make(`queue-action:message:${name}`),
            text: name,
            attachments: [],
            createdBy: "user",
            creationSource: "web",
            dispatchMode: { type: "queue_after_active" },
          });
        const running = yield* orchestrator.streamStoredEvents.pipe(
          Stream.filter(
            ({ event }) =>
              event.threadId === threadId &&
              event.type === "run.updated" &&
              event.payload.status === "running",
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
        yield* send("active");
        yield* worker.drain();
        yield* Fiber.join(running);
        yield* send("first queued");
        yield* send("second queued");
        const thirdQueued =
          intervening === "new queue leader" ||
          (action === "batch steer" && intervening === "unrelated edit");
        if (thirdQueued) yield* send("third queued");
        const before = yield* read;
        const target = before.runs[0]!;
        const queued = before.runs.slice(1);
        assert.deepEqual(
          queued.map(({ status }) => status),
          Array.from({ length: thirdQueued ? 3 : 2 }, () => "queued"),
        );
        const stopFirst = intervening === "stop before planning";
        const successor = intervening === "successor generation";
        const preHeld =
          stopFirst ||
          successor ||
          (action === "resume" && ["stop held queue", "none"].includes(intervening));
        const now = yield* DateTime.now;
        const queuedRunIds = queued.map((run) => run.id);
        if (preHeld)
          yield* sink.writeWithEffects({
            effects: [],
            queueHold: { reason: "restart", runIds: queuedRunIds },
            events: queued.map((run) => ({
              id: EventId.make(`queue-action:restart-hold:${run.id}`),
              type: "run.updated",
              threadId,
              runId: run.id,
              occurredAt: now,
              payload: { ...run, queueHeld: true },
            })),
          });
        const captureStop = Effect.gen(function* () {
          const stop = yield* RuntimeStop.CurrentRuntimeStop;
          const sessions = yield* AuthSessions.AuthSessionRepository;
          const principal = {
            sessionId: AuthSessionId.make("queue-action:actor"),
            subject: "Synthetic queue actor",
            method: "browser-session-cookie" as const,
            scopes: new Set(["orchestration:read", "orchestration:operate"] as const),
          };
          yield* sessions.create({
            ...principal,
            scopes: [...principal.scopes],
            client: {
              label: null,
              userAgent: null,
              ipAddress: null,
              deviceType: "bot",
              browser: null,
              os: null,
            },
            issuedAt: now,
            expiresAt: DateTime.add(now, { hours: 1 }),
          });
          yield* Effect.gen(function* () {
            const target = yield* stop.readTarget(threadId);
            assert.equal(target.status, "available");
            if (target.status !== "available") return yield* Effect.die("Missing Stop target");
            const request = {
              commandId: CommandId.make("queue-action:stop"),
              threadId,
              target: target.target,
            };
            if (successor) {
              // Seed the predecessor's durable intent. The same provider and
              // native conversation IDs now belong to the recording runtime.
              yield* sql.withTransaction(
                makeRuntimeStopMethods(sql).record({
                  identity: {
                    request: {
                      ...request,
                      target: {
                        ...request.target,
                        binding: {
                          ...request.target.binding,
                          runtimeGeneration: "queue-action:predecessor",
                        },
                      },
                    },
                    affectedRunIds: queuedRunIds,
                    actor: principal,
                    actorDigest: "synthetic-prior-stop",
                  },
                  revalidate: Effect.void,
                }),
              );
            } else {
              const stopped = yield* stop.stop(request);
              assert.sameMembers([...stopped.affectedRunIds], queuedRunIds);
            }
          }).pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, principal));
          const fenced = yield* read;
          assert.deepEqual(
            fenced.runs.slice(1).map(({ status }) => status),
            preHeld ? ["queued", "queued"] : ["cancelled", "cancelled"],
          );
          const activeTurn = fenced.providerTurns.find((turn) => turn.status === "running");
          assert.equal(activeTurn?.runAttemptId, target.activeAttemptId);
        });
        if (stopFirst || successor) yield* captureStop;

        const openRequest = sink.write({
          events: [
            {
              id: EventId.make("queue-action:question-opened"),
              type: "runtime-request.updated",
              threadId,
              runId: target.id,
              occurredAt: now,
              payload: {
                id: RuntimeRequestId.make("queue-action:question"),
                nodeId: target.rootNodeId!,
                providerTurnId: before.providerTurns[0]!.id,
                nativeRequestRef: null,
                kind: "user_input",
                status: "pending",
                responseCapability: { type: "message" },
                createdAt: now,
                resolvedAt: null,
              },
            },
          ],
        });
        const commandId = CommandId.make("queue-action:mutation");
        const common = { threadId, commandId };
        const batchItems = queued.slice(0, 2).map((run) => ({
          queuedRunId: run.id,
          messageId: run.userMessageId,
          messageUpdatedAt: DateTime.formatIso(
            before.messages.find((message) => message.id === run.userMessageId)!.updatedAt,
          ),
        }));
        const command: OrchestrationV2Command =
          action === "resume"
            ? { ...common, type: "queue.resume" }
            : action === "reorder"
              ? {
                  ...common,
                  type: "queued-run.reorder",
                  runId: queued[1]!.id,
                  beforeRunId: queued[0]!.id,
                }
              : action === "single steer"
                ? {
                    ...common,
                    type: "queued-message.promote-to-steer",
                    queuedRunId: queued[0]!.id,
                    targetRunId: target.id,
                  }
                : action === "cancel"
                  ? { ...common, type: "queued-run.cancel", runId: queued[0]!.id }
                  : {
                      ...common,
                      type: "queued-message.promote-batch-to-steer",
                      targetRunId: target.id,
                      items: batchItems,
                    };
        if (action === "cancel") {
          yield* orchestrator.dispatch(command);
          const after = yield* read;
          assert.include(
            after.runs.find(({ id }) => id === queued[0]!.id),
            {
              status: "cancelled",
            },
          );
          assert.include(
            after.runs.find(({ id }) => id === queued[1]!.id),
            {
              status: "queued",
              queueHeld: true,
            },
          );
          assert.equal(
            after.messages.find(({ id }) => id === queued[0]!.userMessageId)?.runId,
            queued[0]!.id,
          );
          assert.deepEqual(yield* outbox.listByCommandId(commandId), []);
          return;
        }
        if (intervening === "none" || successor) {
          const accepted = yield* orchestrator.dispatch(command);
          const replay = yield* orchestrator.dispatch(command);
          assert.isTrue(replay.replayed);
          assert.equal(replay.sequence, accepted.sequence);
          const after = yield* read;
          const effects = yield* outbox.listByCommandId(commandId);
          if (action === "resume") {
            assert.deepEqual(
              after.runs.slice(1).map((run) => run.queueHeld),
              [false, false],
            );
            assert.deepEqual(effects, []);
          } else {
            const moved = queued.slice(0, action === "single steer" ? 1 : queued.length);
            for (const run of moved) {
              assert.equal(after.runs.find(({ id }) => id === run.id)?.status, "cancelled");
              assert.equal(
                after.messages.find(({ id }) => id === run.userMessageId)?.runId,
                target.id,
              );
            }
            assert.deepEqual(
              effects.map(({ request }) => request.type),
              [action === "single steer" ? "provider-turn.steer" : "provider-turn.steer-batch"],
            );
            yield* worker.drain();
            assert.lengthOf(steered, 1);
          }
          return;
        }

        const planned = yield* Deferred.make<Parameters<typeof sink.commitCommand>[0]>();
        const release = yield* Deferred.make<void>();
        const commit = sink.commitCommand;
        yield* Effect.acquireRelease(
          Effect.sync(() =>
            vi
              .spyOn(sink, "commitCommand")
              .mockImplementation((input) =>
                input.commandId === commandId
                  ? Deferred.succeed(planned, input).pipe(
                      Effect.andThen(Deferred.await(release)),
                      Effect.andThen(commit(input)),
                    )
                  : commit(input),
              ),
          ),
          (spy) => Effect.sync(() => spy.mockRestore()),
        );
        const pending = yield* orchestrator
          .dispatch(command)
          .pipe(Effect.result, Effect.forkScoped);
        const queuedCommit = yield* Deferred.await(planned);
        if (intervening === "receipt") {
          // The same receipt is accepted by another writer after dispatch checked for replay.
          const accepted = yield* commit(queuedCommit);
          assert.isTrue(accepted.committed);
          yield* openRequest;
          const acceptedState = yield* read;
          yield* Deferred.succeed(release, undefined);
          const replay = yield* Fiber.join(pending);
          assert.equal(replay._tag, "Success");
          if (replay._tag === "Success") {
            assert.isTrue(replay.success.replayed);
            assert.equal(replay.success.sequence, accepted.receipt.resultSequence);
          }
          assert.deepEqual((yield* read).runs, acceptedState.runs);
          assert.deepEqual((yield* read).messages, acceptedState.messages);
          assert.equal((yield* outbox.listByCommandId(commandId)).length, 1);
          return;
        }
        if (intervening === "edit" || intervening === "unrelated edit") {
          const run = intervening === "edit" ? queued[0]! : queued.at(-1)!;
          const message = before.messages.find(({ id }) => id === run.userMessageId)!;
          yield* sink.write({
            events: [
              {
                id: EventId.make("queue-action:intervening-edit"),
                type: "message.updated",
                threadId,
                runId: run.id,
                occurredAt: now,
                payload: { ...message, text: "Edited during planning", updatedAt: now },
              },
            ],
          });
        } else if (intervening === "pending request") {
          yield* openRequest;
        } else if (intervening === "new queue leader") {
          const run = queued[2]!;
          yield* sink.write({
            events: [
              {
                id: EventId.make("queue-action:move-to-front"),
                type: "run.updated",
                threadId,
                runId: run.id,
                occurredAt: now,
                payload: { ...run, queuePosition: 1 },
              },
            ],
          });
        } else if (!stopFirst) {
          yield* captureStop;
        }
        const acceptedState = yield* read;
        const readHolds = sql`SELECT run_id, reason, held_at FROM jones_queue_holds
          WHERE thread_id = ${threadId} ORDER BY run_id`;
        const heldBefore = yield* readHolds;
        yield* Deferred.succeed(release, undefined);
        const result = yield* Fiber.join(pending);
        const after = yield* read;
        if (intervening === "unrelated edit") {
          assert.equal(result._tag, "Success");
          const promoted = queued.slice(0, action === "single steer" ? 1 : 2);
          for (const run of promoted)
            assert.include(
              after.messages.find(({ id }) => id === run.userMessageId),
              {
                runId: target.id,
              },
            );
          const { userMessageId } = queued.at(-1)!;
          assert.deepEqual(
            after.messages.find(({ id }) => id === userMessageId),
            acceptedState.messages.find(({ id }) => id === userMessageId),
          );
          assert.equal((yield* outbox.listByCommandId(commandId)).length, 1);
          return;
        }
        const effects = yield* outbox.listByCommandId(commandId);
        const receipts =
          yield* sql`SELECT command_id FROM orchestration_command_receipts WHERE command_id = ${commandId}`;
        const events =
          yield* sql`SELECT event_type FROM orchestration_events WHERE command_id = ${commandId}`;
        if (intervening === "pending request" || intervening === "new queue leader")
          yield* worker.drain();
        assert.deepEqual(
          {
            result: result._tag,
            events: events.length,
            receipts: receipts.length,
            outbox: effects.map(({ request }) => request.type),
            providerInvocations: steered.length,
            queued: after.runs.filter((run) => run.status === "queued").length,
          },
          {
            result: "Failure",
            events: 0,
            receipts: 0,
            outbox: [],
            providerInvocations: 0,
            queued: acceptedState.runs.filter((run) => run.status === "queued").length,
          },
        );
        assert.deepEqual(after.runs, acceptedState.runs, "stale queue command changed run state");
        assert.deepEqual(after.attempts, acceptedState.attempts);
        assert.deepEqual(after.nodes, acceptedState.nodes);
        assert.deepEqual(after.messages, acceptedState.messages, "stale queue command moved input");
        assert.deepEqual(after.turnItems, acceptedState.turnItems);
        assert.deepEqual(yield* readHolds, heldBefore);
      }).pipe(
        Effect.provide(
          ProviderReplayHarness.layerWithRegistryAndServices(
            { name: "queue-action-stop" },
            ProviderAdapterRegistry.layerFromAdapters([adapter]),
            { runEffectWorker: false, databaseLayer: SqlitePersistence.layerMemory },
          ),
        ),
      );
    }).pipe(Effect.scoped),
);

const batchScenarios = [
  "manual",
  "automatic",
  "completed",
  "switched",
  "bounded",
  "saved-speed",
  "automatic-goal-with-saved-speed",
] as const;

it.effect.each(batchScenarios)(
  "originals use one batch effect and preserve delivery on %s",
  (mode) =>
    Effect.scoped(
      Effect.gen(function* () {
        const automaticGoal = mode === "automatic-goal-with-saved-speed";
        const automatic = mode === "automatic" || automaticGoal;
        const count = automaticGoal ? 19 : mode === "switched" || mode === "saved-speed" ? 4 : 22;
        const promotedCount = mode === "bounded" ? 2 : count;
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
          const running = yield* orchestrator.streamStoredEvents.pipe(
            Stream.map((stored) => stored.event),
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
            text: automaticGoal ? "/goal Verify queue delivery" : "first",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          yield* worker.drain();
          // Fail with the actual retained effect error before awaiting a running
          // signal that a failed first start can never publish.
          const failedStarts = yield* sql`
            SELECT effect_type, status, last_error FROM orchestration_v2_effect_outbox
            WHERE thread_id = ${threadId} AND last_error IS NOT NULL`;
          assert.deepEqual(failedStarts, []);
          const initialEffects = yield* sql`
            SELECT effect_type, status, last_error FROM orchestration_v2_effect_outbox
            WHERE thread_id = ${threadId}`;
          assert.lengthOf(started, 1, JSON.stringify(initialEffects));
          yield* Fiber.join(running);
          const savedSelection = {
            ...modelSelection,
            options: [{ id: "serviceTier", value: "ultrafast" }],
          };
          const saveSpeed = () =>
            orchestrator
              .dispatch({
                type: "thread.model-selection.set",
                commandId: CommandId.make("save-speed"),
                threadId,
                modelSelection: savedSelection,
              })
              .pipe(withOwnerSelectionIngress);
          if (mode === "saved-speed") yield* saveSpeed();
          const threads = yield* ThreadManagementService.ThreadManagementService.pipe(
            Effect.provide(
              ThreadManagementService.layer.pipe(
                Layer.provide(ServerSettings.layerTest()),
                Layer.provide(CommandReceiptStore.layer.pipe(Layer.provide(database))),
              ),
            ),
          );
          for (let i = 0; i < count; i++) {
            if (automaticGoal && i === 1) {
              const beforeSave = yield* orchestrator.getThreadProjection(threadId);
              assert.deepEqual(
                beforeSave.runs.find((run) => run.status === "queued")?.modelSelection,
                modelSelection,
              );
              yield* TestClock.adjust("1 millis");
              yield* saveSpeed();
            }
            const input = {
              commandId: CommandId.make(`queue:${i}`),
              projectId: ProjectId.make("project:batch"),
              threadId,
              messageId: MessageId.make(`message:${i}`),
              text: mode === "bounded" && i === 2 ? "x".repeat(120_000) : `queued input ${i}`,
              attachments:
                i === 0
                  ? [
                      {
                        id: ChatAttachmentId.make("batch-file"),
                        type: "file",
                        name: "context.txt",
                        mimeType: "text/plain",
                        sizeBytes: 1,
                      } satisfies ChatAttachment,
                    ]
                  : [],
              createdBy: "agent" as const,
              creationSource: "mcp" as const,
              senderThreadId: ThreadId.make(`sender:${i}`),
            };
            if (automatic) yield* threads.sendToThread({ ...input, mode: "queue" });
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
              .slice(0, promotedCount)
              .map((run) => ({
                queuedRunId: run.id,
                messageId: run.userMessageId,
                messageUpdatedAt: DateTime.formatIso(
                  before.messages.find((message) => message.id === run.userMessageId)!.updatedAt,
                ),
              })),
          };
          if (mode === "saved-speed") {
            assert.deepEqual(before.thread.modelSelection, savedSelection);
            assert.deepEqual(
              before.runs.find((run) => run.id === command.targetRunId)?.modelSelection,
              modelSelection,
            );
            assert.isTrue("error" in validateQueuedSteerBatch(before, command, true));
          }
          if (automaticGoal) {
            assert.deepEqual(before.thread.modelSelection, savedSelection);
            const queued = before.runs.filter((run) => run.status === "queued");
            for (const run of queued) {
              assert.deepEqual(run.modelSelection, savedSelection);
              assert.equal(run.queuedToolBoundaryFollowUpOfRunId, command.targetRunId);
            }
            assert.isFalse("error" in validateQueuedSteerBatch(before, command, true));
          }
          for (const index of [0, 1]) {
            for (const field of ["instance", "thread", "model", "options"] as const) {
              const changed = {
                ...before,
                runs: before.runs.map((run) =>
                  run.id !== command.items[index]!.queuedRunId
                    ? run
                    : {
                        ...run,
                        queuedToolBoundaryFollowUpOfRunId: undefined,
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
          for (const index of [0, 1]) {
            const freshSession = {
              ...before,
              runs: before.runs.map((run) =>
                run.id === command.items[index]!.queuedRunId ? { ...run, freshContext: true } : run,
              ),
            };
            assert.isTrue("error" in validateQueuedSteerBatch(freshSession, command));
            const structured = {
              ...before,
              runs: before.runs.map((run) =>
                run.id === command.items[index]!.queuedRunId
                  ? { ...run, outputSchema: { type: "object" } }
                  : run,
              ),
            };
            assert.isTrue("error" in validateQueuedSteerBatch(structured, command));
            assert.isFalse(
              queuedSteerBatchCandidates(structured, command.targetRunId, true)[index]!.eligible,
            );
          }
          assert.isTrue("error" in validateQueuedSteerBatch(before, { ...command, items: [] }));
          assert.isTrue(
            "error" in
              validateQueuedSteerBatch(
                {
                  ...before,
                  providerSessions: before.providerSessions.map((session) => ({
                    ...session,
                    capabilities: {
                      ...session.capabilities,
                      turns: {
                        ...session.capabilities.turns,
                        activeSteeringAcceptsAttachments: false,
                      },
                    },
                  })),
                },
                command,
              ),
          );
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
                    modelSelection: {
                      ...modelSelection,
                      instanceId: ProviderInstanceId.make("changed-account"),
                    },
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
          if (automatic) {
            if (automaticGoal) {
              yield* threads.sendToThread({
                commandId: CommandId.make("explicit-pick"),
                projectId: ProjectId.make("project:batch"),
                threadId,
                messageId: MessageId.make("explicit-pick"),
                text: "Keep the explicit speed for a separate turn",
                attachments: [],
                createdBy: "agent",
                creationSource: "mcp",
                senderThreadId: ThreadId.make("peer"),
                modelSelection: savedSelection,
                mode: "queue",
              });
            }
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
            const input = {
              commandId: command.commandId,
              threadId,
              targetRunId: command.targetRunId,
              queuedRunIds: before.runs
                .filter((run) => run.status === "queued")
                .map((run) => run.id),
            };
            const refused = yield* threads.promoteQueuedBatch!({
              ...input,
              commandId: CommandId.make("batch:wrong-prefix"),
              queuedRunIds: [...input.queuedRunIds].reverse(),
            }).pipe(Effect.result);
            assert.equal(refused._tag, "Failure");
            const accepted = yield* threads.promoteQueuedBatch!(input);
            assert.equal(accepted.promotedCount, promotedCount);
            assert.equal(accepted.remainingCount, count - promotedCount);
            const replay = yield* orchestrator.dispatch(command);
            assert.equal(replay.sequence, accepted.sequence);
            assert.isTrue(replay.replayed);
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
          assert.equal(started.length, 1);
          assert.deepEqual(steered[0]!.attachments, firstMessage.attachments);
          for (let i = 0; i < promotedCount; i++)
            assert.include(steered[0]!.text, `sender: thread sender:${i}`);
          const after = yield* orchestrator.getThreadProjection(threadId);
          if (mode === "saved-speed" || automaticGoal)
            assert.deepEqual(after.thread.modelSelection, savedSelection);
          assert.equal(
            after.runs.filter((run) => run.status === "queued").length,
            count - promotedCount + (automatic ? 1 : 0) + (automaticGoal ? 1 : 0),
          );
          if (automatic)
            assert.isUndefined(
              after.runs.find((run) => run.userMessageId === "separate")
                ?.queuedToolBoundaryEligible,
            );
          if (automaticGoal) {
            const explicit = after.runs.find((run) => run.userMessageId === "explicit-pick");
            assert.equal(explicit?.status, "queued");
            assert.deepEqual(explicit?.modelSelection, savedSelection);
            assert.isUndefined(explicit?.queuedToolBoundaryFollowUpOfRunId);
          }
          const promoted = after.turnItems.filter(
            (item) =>
              item.type === "user_message" && item.inputIntent === "promoted_queued_to_steer",
          );
          assert.equal(promoted.length, promotedCount);
          assert.equal(new Set(promoted.map((item) => item.ordinal)).size, promotedCount);
          for (const item of promoted) {
            assert.equal(item.type, "user_message");
            if (item.type !== "user_message") continue;
            const original = before.messages.find((message) => message.id === item.messageId)!;
            assert.equal(item.createdBy, original.createdBy);
            assert.equal(item.creationSource, original.creationSource);
            assert.equal(item.senderThreadId, original.senderThreadId);
            assert.deepEqual(item.attachments, original.attachments);
          }
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

          // A duplicate completion cannot spend a later queued message. A fresh tool
          // completion is the serial positive control that proves the duplicate was consumed.
          const control = yield* orchestrator.getThreadProjection(controlThreadId);
          const originalBoundary = control.nodes.find(
            (node) => node.id === NodeId.make(`tool:${controlThreadId}`),
          )!;
          const laterMessageId = MessageId.make("composer:after-original-boundary");
          yield* orchestrator.dispatch({
            ...monitorCommand,
            commandId: CommandId.make("composer:after-original-boundary"),
            threadId: controlThreadId,
            messageId: laterMessageId,
            text: "Wait for a fresh safe boundary",
            queuedToolBoundaryEligible: true,
          });
          const laterRun = (yield* orchestrator.getThreadProjection(controlThreadId)).runs.find(
            (run) => run.userMessageId === laterMessageId,
          )!;
          const duplicate = (yield* sink.write({
            events: [
              {
                id: EventId.make("boundary:duplicate"),
                type: "node.updated",
                threadId: controlThreadId,
                runId: originalBoundary.runId!,
                occurredAt: yield* DateTime.now,
                payload: originalBoundary,
              },
            ],
          }))[0]!;
          assert.isFalse(
            yield* sink.canPromoteQueuedAtToolBoundary({
              threadId: controlThreadId,
              activeRunId: originalBoundary.runId!,
              queuedRunId: laterRun.id,
              messageId: laterMessageId,
              boundary: duplicate,
              runtimeMode: control.thread.runtimeMode,
              interactionMode: control.thread.interactionMode,
            }),
          );
          const laterPromoted = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) => event.type === "message.updated" && event.payload.id === laterMessageId,
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
          const freshNodeId = NodeId.make("tool:fresh-after-duplicate");
          yield* sink.write({
            events: [
              {
                id: EventId.make("boundary:fresh-after-duplicate"),
                type: "node.updated",
                threadId: controlThreadId,
                runId: originalBoundary.runId!,
                occurredAt: yield* DateTime.now,
                payload: {
                  ...originalBoundary,
                  id: freshNodeId,
                  nativeItemRef: { driver, nativeId: "fresh-after-duplicate", strength: "strong" },
                },
              },
            ],
          });
          yield* Fiber.join(laterPromoted);
          yield* worker.drain();
          const promotions = yield* sql<{
            command_id: string;
          }>`SELECT command_id FROM orchestration_command_receipts
            WHERE aggregate_id = ${controlThreadId} AND status = 'accepted'
              AND command_type IN ('queued-message.promote-batch-to-steer', 'queued-message.promote-to-steer')`;
          assert.lengthOf(promotions, 2);
          assert.isFalse(
            promotions.some((receipt) => receipt.command_id.endsWith(`:${duplicate.sequence}`)),
          );
          assert.equal(steered.length, 2);
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

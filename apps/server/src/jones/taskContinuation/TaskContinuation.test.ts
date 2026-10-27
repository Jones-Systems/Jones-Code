import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  ProviderTurnId,
  RunId,
  RuntimeRequestId,
  type OrchestrationV2RuntimeRequest,
  type TaskContinueInput,
} from "@t3tools/contracts";
import type { ProviderAdapterV2Shape } from "@t3tools/provider-core/server/ProviderAdapter";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as SqlClient from "effect/sql/SqlClient";
import * as Config from "../../config.ts";
import * as Mcp from "../../mcp/OrchestratorMcpService.ts";
import { ClaudeProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/ClaudeAdapterV2.ts";
import * as Receipts from "../../orchestration-v2/CommandReceiptStore.ts";
import * as Sink from "../../orchestration-v2/EventSink.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as Projections from "../../orchestration-v2/ProjectionStore.ts";
import * as Registry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as Threads from "../../orchestration-v2/ThreadManagementService.ts";
import * as Harness from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import { TaskContinuationCommit } from "./commitContext.ts";
import { TaskContinuationError } from "./sourceBinding.ts";
import { requirementFor } from "./guard.ts";
import * as Continuation from "./TaskContinuation.ts";
import * as Store from "./TaskContinuationStore.ts";
import { fixture } from "./testkit.ts";

interface Scenario {
  readonly f: ReturnType<typeof fixture>;
  readonly input: TaskContinueInput;
  readonly dispatchCount: () => number;
  readonly hooks: {
    beforeDispatch?: () => Effect.Effect<void>;
    insideCommit?: () => Effect.Effect<void, Projections.ProjectionStoreV2Error>;
    failAfterAccept?: boolean;
    loseResponse?: boolean;
  };
}
type ScenarioServices =
  | Continuation.TaskContinuation
  | Store.TaskContinuationStore
  | Sink.EventSinkV2
  | Projections.ProjectionStoreV2
  | SqlClient.SqlClient
  | Receipts.CommandReceiptStoreV2
  | Orchestrator.OrchestratorV2;

// Real command planning, receipt reservation, projections, outbox and Jones storage share one
// in-memory database. Only the provider process and unrelated MCP status presentation are absent.
const scenario = <E>(
  name: string,
  body: (context: Scenario) => Effect.Effect<void, E, ScenarioServices>,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const f = fixture();
      const config = yield* Harness.makeReplayServerConfig(`task-continuation-${name}`);
      const fs = yield* FileSystem.FileSystem;
      yield* Effect.addFinalizer(() =>
        fs.remove(config.baseDir, { recursive: true }).pipe(Effect.orDie),
      );
      const adapter: ProviderAdapterV2Shape = {
        instanceId: f.provider.providerInstanceId,
        driver: f.provider.driver,
        getCapabilities: () => Effect.succeed(ClaudeProviderCapabilitiesV2),
        planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
        openSession: () => Effect.die("continuation admission tests never open a provider process"),
      };
      const registry = Layer.succeed(Registry.ProviderAdapterRegistryV2, {
        get: () => Effect.succeed(adapter),
        list: () => Effect.succeed([adapter.instanceId]),
        getMetadata: () =>
          Effect.succeed({
            driver: adapter.driver,
            continuationKey: "fixture",
            enabled: true,
            capabilities: ClaudeProviderCapabilitiesV2,
          }),
      });
      const runtime = Harness.layerWithRegistryAndServices({ name }, registry, {
        serverConfigLayer: Layer.succeed(Config.ServerConfig, config),
        runEffectWorker: false,
        configureMcp: false,
      });
      const storage = Store.layer.pipe(Layer.provideMerge(runtime));
      yield* Effect.gen(function* () {
        const sink = yield* Sink.EventSinkV2;
        const projections = yield* Projections.ProjectionStoreV2;
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        yield* sink.write({
          events: [
            f.event("thread.created", f.parent.thread.id, f.parent.thread),
            f.event("run.created", f.parent.thread.id, f.parent.runs[0]!),
            f.event("subagent.updated", f.parent.thread.id, f.task),
            f.event("thread.created", f.child.thread.id, f.child.thread),
            f.event("provider-thread.updated", f.child.thread.id, f.provider),
            f.event("run.created", f.child.thread.id, f.run),
            f.event("run-attempt.created", f.child.thread.id, f.attempt),
            f.event("provider-turn.updated", f.child.thread.id, f.child.providerTurns[0]!),
            f.event("context-transfer.created", f.child.thread.id, f.child.contextTransfers[0]!),
          ],
        });
        let dispatches = 0;
        const hooks: Scenario["hooks"] = {};
        const service = Continuation.layer.pipe(
          Layer.provide(
            Layer.mock(Threads.ThreadManagementService)({
              assertProviderInstanceEnabled: () => Effect.void,
              dispatch: (command) =>
                Effect.gen(function* () {
                  dispatches++;
                  if (hooks.beforeDispatch) yield* hooks.beforeDispatch();
                  const context = yield* TaskContinuationCommit;
                  assert.isDefined(context);
                  const result = yield* orchestrator.dispatch(command).pipe(
                    Effect.provideService(TaskContinuationCommit, {
                      accept: (envelope) =>
                        Effect.gen(function* () {
                          if (hooks.insideCommit) yield* hooks.insideCommit();
                          yield* context!.accept(envelope);
                          if (hooks.failAfterAccept)
                            return yield* new TaskContinuationError({ reason: "child_busy" });
                        }).pipe(
                          Effect.mapError(
                            (cause) => new TaskContinuationError({ reason: "child_busy", cause }),
                          ),
                        ),
                    }),
                  );
                  if (hooks.loseResponse)
                    return yield* new Orchestrator.OrchestratorDispatchError({
                      commandId: command.commandId,
                      commandType: command.type,
                      cause: "synthetic lost response",
                    });
                  return result;
                }),
            }),
          ),
          Layer.provide(
            Layer.mock(Mcp.OrchestratorMcpService)({
              taskStatus: (_scope, taskId) =>
                Effect.gen(function* () {
                  const parent = yield* projections.getThreadRecords(f.parent.thread.id, [
                    "subagents",
                  ]);
                  const task = parent.subagents.find((task) => task.id === taskId)!;
                  assert.isDefined(task);
                  const child = yield* projections.getThreadRecords(task.childThreadId!, ["runs"]);
                  const run = child.runs.findLast((run) => run.delegatedTaskId === taskId)!;
                  return {
                    taskId,
                    childThreadId: task.childThreadId,
                    childRunId: run.id,
                    childNodeId: taskId,
                    status: task.result === null ? ("running" as const) : ("completed" as const),
                    workState:
                      task.result === null ? ("working" as const) : ("result_available" as const),
                    hasPendingChildRuns: task.result === null,
                    latestTerminalRunId: null,
                    latestTerminalStatus: null,
                    latestTerminalSummary: null,
                    latestTerminalResultContextTransferId: null,
                    providerInstanceId: task.providerInstanceId,
                    model: task.model ?? null,
                    summary: task.result,
                    resultContextTransferId: null,
                    hasPendingQuestion: false,
                    pendingApproval: null,
                    waitTimedOut: false,
                  };
                }).pipe(Effect.orDie),
            }),
          ),
        );
        yield* body({
          f,
          input: {
            taskId: f.task.id,
            message: "Continue the original conversation",
            clientRequestId: "round-two",
          },
          hooks,
          dispatchCount: () => dispatches,
        }).pipe(Effect.provide(Layer.mergeAll(service, Receipts.layer)));
      }).pipe(Effect.provide(storage));
    }),
  ).pipe(Effect.provide(NodeServices.layer));

it.effect("accepts one new task in the same child and replays before mutable idle checks", () =>
  scenario("replay", ({ f, input, dispatchCount }) =>
    Effect.gen(function* () {
      const service = yield* Continuation.TaskContinuation;
      const projections = yield* Projections.ProjectionStoreV2;
      const first = yield* service.continue(f.scope, input);
      assert.isFalse(first.replayed);
      assert.notEqual(first.taskId, f.task.id);
      assert.equal(first.childThreadId, f.child.thread.id);
      assert.equal(first.continuation.nativeSessionId, "original-native");
      assert.equal(first.continuation.sourceAttemptId, f.attempt.id);
      assert.equal(first.continuation.verification, "pending");
      // The admitted run makes the child busy; replay still returns the committed task.
      const replay = yield* service.continue(
        { ...f.scope, requestNamespace: "restarted-mcp" },
        input,
      );
      assert.isTrue(replay.replayed);
      assert.equal(replay.taskId, first.taskId);
      assert.equal(dispatchCount(), 1);
      const parent = yield* projections.getThreadRecords(f.parent.thread.id, ["subagents"]);
      assert.deepStrictEqual(
        parent.subagents.find((task) => task.id === f.task.id),
        f.task,
      );
      const followup = parent.subagents.find((task) => task.id === first.taskId)!;
      assert.isNull(followup.result);
      assert.equal(followup.completionWake, "always");
      assert.equal(parent.subagents.length, 2);
      const changed = yield* Effect.result(
        service.continue(f.scope, { ...input, message: "Different body" }),
      );
      assert.equal(changed._tag, "Failure");
      if (changed._tag === "Failure") assert.equal(changed.failure.code, "request_conflict");
      assert.equal(dispatchCount(), 1);
    }),
  ),
);

it.effect(
  "continues a real untagged delegate launch and its tagged follow-up without changing the native conversation",
  () =>
    scenario("production-launch", ({ f, dispatchCount }) =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const sink = yield* Sink.EventSinkV2;
        const projections = yield* Projections.ProjectionStoreV2;
        const service = yield* Continuation.TaskContinuation;
        const store = yield* Store.TaskContinuationStore;
        const parentRun = f.parent.runs[0]!;
        yield* sink.write({
          events: [
            f.event("node.updated", f.parent.thread.id, {
              id: parentRun.rootNodeId!,
              threadId: f.parent.thread.id,
              runId: parentRun.id,
              parentNodeId: null,
              rootNodeId: parentRun.rootNodeId!,
              kind: "root_turn",
              status: "running",
              countsForRun: true,
              providerThreadId: null,
              providerTurnId: null,
              nativeItemRef: null,
              runtimeRequestId: null,
              checkpointScopeId: null,
              startedAt: f.now,
              completedAt: null,
            }),
          ],
        });
        yield* orchestrator.dispatch({
          type: "delegated_task.request",
          commandId: CommandId.make("production-launch"),
          createdBy: "agent",
          creationSource: "mcp",
          parentThreadId: f.parent.thread.id,
          parentRunId: parentRun.id,
          parentNodeId: parentRun.rootNodeId!,
          task: "Production-shaped planner",
          modelSelection: f.run.modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          completionWake: "always",
        });
        const parent = yield* projections.getThreadRecords(f.parent.thread.id, ["subagents"]);
        const task = parent.subagents.find((task) => task.prompt === "Production-shaped planner")!;
        const child = yield* projections.getThreadRecords(task.childThreadId!, [
          "runs",
          "attempts",
          "providerThreads",
          "contextTransfers",
        ]);
        const run = child.runs[0]!;
        const attempt = child.attempts.find((attempt) => attempt.id === run.activeAttemptId)!;
        const provider = child.providerThreads.find(
          (provider) => provider.id === run.providerThreadId,
        )!;
        assert.isUndefined(run.delegatedTaskId);
        assert.isTrue(
          child.contextTransfers.some(
            (transfer) =>
              transfer.type === "subagent_spawn" &&
              transfer.status === "consumed" &&
              transfer.targetRunId === run.id,
          ),
        );
        const turn = {
          ...f.child.providerTurns[0]!,
          id: ProviderTurnId.make("production-turn"),
          providerThreadId: provider.id,
          runAttemptId: attempt.id,
          nodeId: run.rootNodeId!,
        };
        yield* sink.write({
          events: [
            f.event("provider-thread.updated", child.thread.id, {
              ...provider,
              nativeThreadRef: f.provider.nativeThreadRef,
              status: "idle",
              firstRunOrdinal: run.ordinal,
              lastRunOrdinal: run.ordinal,
            }),
            f.event("run-attempt.updated", child.thread.id, {
              ...attempt,
              status: "completed",
              nativeThreadId: "original-native",
              providerTurnId: turn.id,
              startedAt: f.now,
              completedAt: f.now,
            }),
            f.event("provider-turn.updated", child.thread.id, turn),
            f.event("run.updated", child.thread.id, {
              ...run,
              status: "completed",
              completedAt: f.now,
            }),
            f.event("subagent.updated", f.parent.thread.id, {
              ...task,
              status: "completed",
              result: "Immutable planner result",
              completedAt: f.now,
            }),
          ],
        });
        const input = {
          taskId: task.id,
          message: "Reconcile the original plan",
          clientRequestId: "production-reconcile",
        };
        const accepted = yield* service.continue(f.scope, input);
        assert.equal(accepted.continuation.sourceRunId, run.id);
        assert.equal(accepted.continuation.sourceAttemptId, attempt.id);
        assert.equal(accepted.continuation.nativeSessionId, "original-native");
        const continued = yield* projections.getThreadRecords(child.thread.id, [
          "runs",
          "attempts",
          "providerThreads",
        ]);
        const followup = continued.runs.find((run) => run.id === accepted.childRunId)!;
        const followupAttempt = continued.attempts.find(
          (attempt) => attempt.id === followup.activeAttemptId,
        )!;
        assert.equal(followup.delegatedTaskId, accepted.taskId);
        const requirement = yield* requirementFor(store, continued, followup, followupAttempt);
        assert.equal(requirement?.nativeSessionId, "original-native");
        assert.equal((yield* service.continue(f.scope, input)).taskId, accepted.taskId);
        const busy = yield* Effect.result(
          service.continue(f.scope, { ...input, clientRequestId: "production-busy" }),
        );
        assert.equal(busy._tag, "Failure");
        if (busy._tag === "Failure") assert.include(busy.failure.message, "[child_busy]");
        assert.equal(dispatchCount(), 1);
        const parentAfter = yield* projections.getThreadRecords(f.parent.thread.id, ["subagents"]);
        const followupTask = parentAfter.subagents.find((task) => task.id === accepted.taskId)!;
        const followupTurn = {
          ...turn,
          id: ProviderTurnId.make("production-followup-turn"),
          ordinal: turn.ordinal + 1,
          runAttemptId: followupAttempt.id,
          nodeId: followup.rootNodeId!,
        };
        yield* sink.write({
          events: [
            f.event("run.updated", child.thread.id, {
              ...followup,
              status: "completed",
              completedAt: f.now,
            }),
            f.event("run-attempt.updated", child.thread.id, {
              ...followupAttempt,
              status: "completed",
              nativeThreadId: "original-native",
              providerTurnId: followupTurn.id,
              completedAt: f.now,
            }),
            f.event("provider-turn.updated", child.thread.id, followupTurn),
            f.event("subagent.updated", f.parent.thread.id, {
              ...followupTask,
              status: "completed",
              result: "Reconciled",
              completedAt: f.now,
            }),
          ],
        });
        const next = yield* service.continue(f.scope, {
          taskId: accepted.taskId,
          message: "One more clarification",
          clientRequestId: "production-followup",
          expectedNativeSessionId: "original-native",
        });
        assert.equal(next.continuation.sourceRunId, followup.id);
        assert.equal(next.continuation.nativeSessionId, "original-native");
        assert.equal(dispatchCount(), 2);
      }),
    ),
);

it.effect("reconciles an accepted command after its dispatch response is lost", () =>
  scenario("lost-response", ({ f, input, hooks, dispatchCount }) =>
    Effect.gen(function* () {
      hooks.loseResponse = true;
      const service = yield* Continuation.TaskContinuation;
      const accepted = yield* service.continue(f.scope, input);
      assert.isFalse(accepted.replayed);
      assert.equal(accepted.continuation.verification, "pending");
      assert.equal((yield* service.continue(f.scope, input)).taskId, accepted.taskId);
      assert.equal(dispatchCount(), 1);
    }),
  ),
);

it.effect("returns verified only after a durable observation of the guarded attempt", () =>
  scenario("durable-view", ({ f, input }) =>
    Effect.gen(function* () {
      const service = yield* Continuation.TaskContinuation;
      const sink = yield* Sink.EventSinkV2;
      const projections = yield* Projections.ProjectionStoreV2;
      const store = yield* Store.TaskContinuationStore;
      const accepted = yield* service.continue(f.scope, input);
      const current = yield* projections.getThreadRecords(f.child.thread.id, ["runs", "attempts"]);
      const run = current.runs.find((run) => run.id === accepted.childRunId)!;
      const attempt = current.attempts.find((attempt) => attempt.id === run.activeAttemptId)!;
      yield* sink.write({
        events: [
          f.event("run.updated", f.child.thread.id, {
            ...run,
            status: "running",
            startedAt: f.now,
          }),
          f.event("run-attempt.updated", f.child.thread.id, {
            ...attempt,
            status: "running",
            startedAt: f.now,
            nativeThreadId: "original-native",
          }),
        ],
      });
      assert.equal((yield* service.continue(f.scope, input)).continuation.verification, "pending");
      const row = yield* store.get(
        Continuation.requestIdentity(f.parent.thread.id, input).messageId,
      );
      assert.isNotNull(row);
      yield* store.confirm(row!, {
        runId: run.id,
        attemptId: attempt.id,
        nativeSessionId: "original-native",
        runtimeGeneration: "confirmed-query",
      });
      const view = yield* service.continue(f.scope, input);
      assert.isTrue(view.replayed);
      assert.equal(view.continuation.verification, "verified");
      assert.equal(view.continuation.verifiedRunId, run.id);
      assert.equal(view.continuation.account, "unavailable");
    }),
  ),
);

it.effect("reports terminal work without native confirmation as unverified", () =>
  scenario("terminal-unverified", ({ f, input }) =>
    Effect.gen(function* () {
      const service = yield* Continuation.TaskContinuation;
      const sink = yield* Sink.EventSinkV2;
      const projections = yield* Projections.ProjectionStoreV2;
      const accepted = yield* service.continue(f.scope, input);
      const current = yield* projections.getThreadRecords(f.child.thread.id, ["runs"]);
      const run = current.runs.find((run) => run.id === accepted.childRunId)!;
      yield* sink.write({
        events: [
          f.event("run.updated", f.child.thread.id, {
            ...run,
            status: "failed",
            completedAt: f.now,
          }),
        ],
      });
      const view = yield* service.continue(f.scope, input);
      assert.equal(view.continuation.verification, "unverified");
      assert.isNull(view.continuation.verifiedRunId);
    }),
  ),
);

it.effect(
  "rolls receipt, events, outbox and Jones acceptance back after a seam failure and reuses the key",
  () =>
    scenario("rollback", ({ f, input, hooks }) =>
      Effect.gen(function* () {
        const service = yield* Continuation.TaskContinuation;
        const sink = yield* Sink.EventSinkV2;
        const sql = yield* SqlClient.SqlClient;
        const store = yield* Store.TaskContinuationStore;
        const receipts = yield* Receipts.CommandReceiptStoreV2;
        const identity = Continuation.requestIdentity(f.parent.thread.id, input);
        const before = yield* sink.latestSequence();
        hooks.failAfterAccept = true;
        assert.equal((yield* Effect.result(service.continue(f.scope, input)))._tag, "Failure");
        assert.equal(yield* sink.latestSequence(), before);
        assert.isNull(yield* store.get(identity.messageId));
        assert.isTrue(Option.isNone(yield* receipts.getByCommandId(identity.commandId)));
        const pending =
          yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE command_id = ${identity.commandId}`;
        assert.deepStrictEqual(pending, []);
        hooks.failAfterAccept = false;
        assert.isFalse((yield* service.continue(f.scope, input)).replayed);
      }),
    ),
);

it.effect.each(["held-queue", "pending-request", "background-work"] as const)(
  "rechecks %s inside the acceptance transaction without poisoning the request key",
  (race) =>
    scenario(race, ({ f, input, hooks }) =>
      Effect.gen(function* () {
        const projections = yield* Projections.ProjectionStoreV2;
        const service = yield* Continuation.TaskContinuation;
        const receipts = yield* Receipts.CommandReceiptStoreV2;
        const store = yield* Store.TaskContinuationStore;
        const pendingRequest: OrchestrationV2RuntimeRequest = {
          id: RuntimeRequestId.make("raced-request"),
          nodeId: f.attempt.rootNodeId,
          providerTurnId: f.attempt.providerTurnId,
          nativeRequestRef: null,
          kind: "user_input",
          status: "pending",
          responseCapability: { type: "message" },
          createdAt: f.now,
          resolvedAt: null,
        };
        // Inject at the actual EventSink commit seam, after command planning and receipt reservation.
        // These synthetic projection writes share the transaction and disappear when admission refuses.
        hooks.insideCommit = () =>
          projections.apply(
            race === "held-queue"
              ? f.event("run.created", f.child.thread.id, {
                  ...f.run,
                  id: RunId.make("raced-held-run"),
                  ordinal: 3,
                  status: "queued",
                  queueHeld: true,
                })
              : race === "pending-request"
                ? f.event("runtime-request.updated", f.child.thread.id, pendingRequest)
                : f.event("provider-thread.updated", f.child.thread.id, {
                    ...f.provider,
                    pendingBackgroundTasks: [
                      { kind: "background_task", taskId: "raced-background" },
                    ],
                  }),
          );
        const refused = yield* Effect.result(service.continue(f.scope, input));
        assert.equal(refused._tag, "Failure");
        if (refused._tag === "Failure") assert.include(refused.failure.message, "child_busy");
        const identity = Continuation.requestIdentity(f.parent.thread.id, input);
        assert.isTrue(Option.isNone(yield* receipts.getByCommandId(identity.commandId)));
        assert.isNull(yield* store.get(identity.messageId));
        delete hooks.insideCommit;
        assert.isFalse((yield* service.continue(f.scope, input)).replayed);
      }),
    ),
);

it.effect("admits only one body when concurrent calls share a request key", () =>
  scenario("concurrent-conflict", ({ f, input, hooks }) =>
    Effect.gen(function* () {
      const service = yield* Continuation.TaskContinuation;
      const arrived = yield* Ref.make(0);
      const bothReady = yield* Deferred.make<void>();
      hooks.beforeDispatch = () =>
        Effect.gen(function* () {
          if ((yield* Ref.updateAndGet(arrived, (count) => count + 1)) === 2)
            yield* Deferred.succeed(bothReady, undefined);
          yield* Deferred.await(bothReady);
        });
      const results = yield* Effect.all(
        [
          Effect.result(service.continue(f.scope, input)),
          Effect.result(service.continue(f.scope, { ...input, message: "Competing body" })),
        ],
        { concurrency: "unbounded" },
      );
      assert.equal(results.filter((result) => result._tag === "Success").length, 1);
      const refused = results.find((result) => result._tag === "Failure");
      assert.isDefined(refused);
      if (refused?._tag === "Failure") assert.equal(refused.failure.code, "request_conflict");
      const projections = yield* Projections.ProjectionStoreV2;
      assert.equal(
        (yield* projections.getThreadRecords(f.parent.thread.id, ["subagents"])).subagents.length,
        2,
      );
    }),
  ),
);

it.effect("refuses orphan accepted and rejected receipts without redispatching", () =>
  scenario("orphan-receipt", ({ f, input, dispatchCount }) =>
    Effect.gen(function* () {
      const service = yield* Continuation.TaskContinuation;
      const receipts = yield* Receipts.CommandReceiptStoreV2;
      for (const status of ["accepted", "rejected"] as const) {
        const request = { ...input, clientRequestId: `orphan-${status}` };
        const identity = Continuation.requestIdentity(f.parent.thread.id, request);
        yield* receipts.insertIfAbsent({
          commandId: identity.commandId,
          threadId: f.child.thread.id,
          commandType: "message.dispatch",
          acceptedAt: f.now,
          resultSequence: 0,
          status,
          error: null,
        });
        const refused = yield* Effect.result(service.continue(f.scope, request));
        assert.equal(refused._tag, "Failure");
        if (refused._tag === "Failure") {
          assert.equal(
            refused.failure.code,
            status === "accepted" ? "orchestration_error" : "request_conflict",
          );
          assert.include(
            refused.failure.message,
            status === "accepted" ? "acceptance_unknown" : "refused_key",
          );
        }
      }
      assert.equal(dispatchCount(), 0);
    }),
  ),
);

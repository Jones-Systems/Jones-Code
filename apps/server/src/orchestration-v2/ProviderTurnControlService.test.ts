import { assert, it } from "@effect/vitest";
import {
  MessageId,
  type ModelSelection,
  NodeId,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2Run,
  type OrchestrationV2ThreadProjection,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import { TestClock } from "effect/testing";

import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as McpAppModelContext from "../mcpApps/McpAppModelContext.ts";
import * as EventSink from "./EventSink.ts";
import * as SupersededTerminalTrace from "../jones/orchestration/SupersededTerminalTrace.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import {
  ProviderControlDeadline,
  ProviderControlOutcomeUnknown,
  findProviderControlOutcomeUnknown,
} from "@t3tools/provider-core/server/jones/providerControlOutcome";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as ProviderTurnControlService from "./ProviderTurnControlService.ts";
import { STOP_CHILD_BUDGET_MS } from "../jones/provider/stopBudgets.ts";
import {
  GoalNotificationNotDelivered,
  GoalNotificationDeliveryUnknown,
} from "../jones/goals/GoalNotificationDelivery.ts";

const driver = ProviderDriverKind.make("codex");
const providerInstanceId = ProviderInstanceId.make("codex");
const modelSelection = {
  instanceId: providerInstanceId,
  model: "gpt-5.4",
} satisfies ModelSelection;

function makeProjection(input: {
  readonly now: DateTime.Utc;
  readonly threadId: ThreadId;
  readonly providerThread: OrchestrationV2ProviderThread;
  readonly providerTurnId: ProviderTurnId;
  readonly attemptId: RunAttemptId;
}): OrchestrationV2ThreadProjection {
  const runId = RunId.make("run:restart-session");
  const nodeId = NodeId.make("node:restart-session");
  return {
    thread: {
      createdBy: "user",
      creationSource: "web",
      id: input.threadId,
      projectId: ProjectId.make("project:restart-session"),
      title: "Restart session",
      providerInstanceId,
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: "/workspace",
      activeProviderThreadId: input.providerThread.id,
      lineage: {
        parentThreadId: null,
        relationshipToParent: null,
        rootThreadId: input.threadId,
      },
      forkedFrom: null,
      createdAt: input.now,
      updatedAt: input.now,
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      lastVisitedAt: null,
      deletedAt: null,
    },
    runs: [],
    attempts: [
      {
        id: input.attemptId,
        runId,
        attemptOrdinal: 1,
        rootNodeId: nodeId,
        providerInstanceId,
        providerThreadId: input.providerThread.id,
        providerTurnId: input.providerTurnId,
        reason: "initial",
        status: "superseded",
        startedAt: input.now,
        completedAt: input.now,
      },
    ],
    nodes: [],
    subagents: [],
    providerSessions: [],
    providerThreads: [input.providerThread],
    providerTurns: [
      {
        id: input.providerTurnId,
        providerThreadId: input.providerThread.id,
        nodeId,
        runAttemptId: input.attemptId,
        nativeTurnRef: {
          driver,
          nativeId: "native-turn:restart-session",
          strength: "strong",
        },
        ordinal: 1,
        status: "running",
        startedAt: input.now,
        completedAt: null,
      },
    ],
    runtimeRequests: [],
    messages: [],
    plans: [],
    turnItems: [],
    checkpointScopes: [],
    checkpoints: [],
    contextHandoffs: [],
    contextTransfers: [],
    visibleTurnItems: [],
    updatedAt: input.now,
  };
}

it.effect.each([providerInstanceId, ProviderInstanceId.make("codex-replacement")])(
  "interrupts the historical session only for the exact committed restart replacement on %s",
  (replacementInstanceId) =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("thread:restart-session");
      const oldSessionId = ProviderSessionId.make("provider-session:restart-session:old");
      const replacementSessionId = ProviderSessionId.make(
        "provider-session:restart-session:replacement",
      );
      const unrelatedSessionId = ProviderSessionId.make(
        "provider-session:restart-session:unrelated",
      );
      const providerThreadId = ProviderThreadId.make("provider-thread:restart-session");
      const providerTurnId = ProviderTurnId.make("provider-turn:restart-session");
      const attemptId = RunAttemptId.make("run-attempt:restart-session");
      const providerThread: OrchestrationV2ProviderThread = {
        id: providerThreadId,
        driver,
        providerInstanceId: replacementInstanceId,
        // The restart command already projected this replacement binding.
        providerSessionId: replacementSessionId,
        appThreadId: threadId,
        ownerNodeId: null,
        nativeThreadRef: {
          driver,
          nativeId: "native-thread:restart-session",
          strength: "strong",
        },
        nativeConversationHeadRef: {
          driver,
          nativeId: "native-head:restart-session",
          strength: "strong",
        },
        status: "not_loaded",
        firstRunOrdinal: 1,
        lastRunOrdinal: 1,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      };
      const projection = yield* Ref.make(
        makeProjection({ now, threadId, providerThread, providerTurnId, attemptId }),
      );
      const interruptedThread = yield* Ref.make<OrchestrationV2ProviderThread | null>(null);
      let unknownControl: ProviderControlOutcomeUnknown | undefined;
      const providerSession = {
        id: oldSessionId,
        driver,
        providerInstanceId,
        status: "running" as const,
        cwd: "/workspace",
        model: modelSelection.model,
        capabilities: CodexProviderCapabilitiesV2,
        createdAt: now,
        updatedAt: now,
        lastError: null,
      };
      const runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime = {
        instanceId: providerInstanceId,
        driver,
        providerSessionId: oldSessionId,
        providerSession,
        events: Stream.empty,
        ensureThread: () => Effect.die("unused ensureThread"),
        resumeThread: () => Effect.die("unused resumeThread"),
        startTurn: () => Effect.die("unused startTurn"),
        steerTurn: () => Effect.die("unused steerTurn"),
        interruptTurn: ({ providerThread: target, providerTurnId: targetTurnId, preserveGoal }) =>
          Effect.gen(function* () {
            if (unknownControl !== undefined) {
              assert.isNumber(yield* ProviderControlDeadline);
              return yield* new ProviderAdapter.ProviderAdapterInterruptError({
                driver,
                providerThreadId: target.id,
                providerTurnId: targetTurnId,
                cause: unknownControl,
              });
            }
            assert.isTrue(preserveGoal);
            assert.equal(target.providerInstanceId, runtime.instanceId);
            assert.equal(target.providerSessionId, runtime.providerSessionId);
            assert.equal(target.id, providerThreadId);
            assert.deepEqual(target.nativeThreadRef, providerThread.nativeThreadRef);
            assert.deepEqual(
              target.nativeConversationHeadRef,
              providerThread.nativeConversationHeadRef,
            );
            assert.equal(targetTurnId, providerTurnId);
            yield* Effect.all(
              [
                Ref.set(interruptedThread, target),
                Ref.update(projection, (current) => ({
                  ...current,
                  providerTurns: current.providerTurns.map((turn) =>
                    turn.id === providerTurnId
                      ? { ...turn, status: "interrupted" as const, completedAt: now }
                      : turn,
                  ),
                })),
              ],
              { discard: true },
            );
          }),
        respondToRuntimeRequest: () => Effect.die("unused respondToRuntimeRequest"),
        readThreadSnapshot: () => Effect.die("unused readThreadSnapshot"),
        rollbackThread: () => Effect.die("unused rollbackThread"),
        forkThread: () => Effect.die("unused forkThread"),
      };
      const layerProjection = layerProjectionStore((_threadId, target) =>
        Ref.get(projection).pipe(
          Effect.map((current) => ({
            providerThread: current.providerThreads.find(
              (thread) => thread.id === target.providerThreadId,
            ),
            providerTurn: current.providerTurns.find((turn) => turn.id === target.providerTurnId),
            attempt: current.attempts.find((attempt) => attempt.id === target.attemptId),
            message: undefined,
            run: undefined,
          })),
        ),
      );
      const layerSessionManager = layerSessions(oldSessionId, runtime);
      const layerControl = ProviderTurnControlService.layer.pipe(
        Layer.provide(McpAppModelContext.layerEmpty),
        Layer.provide(
          Layer.mergeAll(
            layerProjection,
            layerSessionManager,
            Layer.mock(EventSink.EventSinkV2)({
              supersededTerminalTrace: SupersededTerminalTrace.make(),
              latestSequence: () => Effect.succeed(0),
              stream: () => Stream.never,
            }),
          ),
        ),
      );

      const [ordinaryInterrupt, unrelatedRestart] = yield* Effect.gen(function* () {
        const control = yield* ProviderTurnControlService.ProviderTurnControlServiceV2;
        const ordinary = yield* Effect.exit(
          control.interrupt({
            threadId,
            providerSessionId: oldSessionId,
            providerThreadId,
            providerTurnId,
          }),
        );
        const unrelated = yield* Effect.exit(
          control.interruptAndAwaitTerminal({
            threadId,
            providerSessionId: oldSessionId,
            replacementProviderSessionId: unrelatedSessionId,
            providerThreadId,
            providerTurnId,
            interruptedAttemptId: attemptId,
          }),
        );
        return [ordinary, unrelated] as const;
      }).pipe(Effect.provide(layerControl));

      assert.isTrue(Exit.isFailure(ordinaryInterrupt));
      assert.isTrue(Exit.isFailure(unrelatedRestart));
      assert.isNull(yield* Ref.get(interruptedThread));

      yield* Effect.gen(function* () {
        const control = yield* ProviderTurnControlService.ProviderTurnControlServiceV2;
        yield* control.interruptAndAwaitTerminal({
          threadId,
          providerSessionId: oldSessionId,
          replacementProviderSessionId: replacementSessionId,
          providerThreadId,
          providerTurnId,
          interruptedAttemptId: attemptId,
        });
      }).pipe(Effect.provide(layerControl));

      const interrupted = yield* Ref.get(interruptedThread);
      yield* Ref.set(interruptedThread, null);
      yield* Effect.gen(function* () {
        const control = yield* ProviderTurnControlService.ProviderTurnControlServiceV2;
        yield* control.interruptAndAwaitTerminal({
          threadId,
          providerSessionId: oldSessionId,
          replacementProviderSessionId: replacementSessionId,
          providerThreadId,
          providerTurnId,
          interruptedAttemptId: attemptId,
        });
      }).pipe(Effect.provide(layerControl));
      assert.isNull(
        yield* Ref.get(interruptedThread),
        "persisted terminal evidence must not interrupt again",
      );

      assert.isNotNull(interrupted);
      assert.equal(interrupted?.providerSessionId, oldSessionId);
      assert.equal(interrupted?.providerInstanceId, providerInstanceId);
      const current = yield* Ref.get(projection);
      assert.equal(current.providerTurns[0]?.status, "interrupted");
      assert.deepEqual(current.providerThreads[0], providerThread);
      assert.equal(interrupted?.id, providerThreadId);
      assert.equal(interrupted?.nativeThreadRef?.nativeId, "native-thread:restart-session");
      unknownControl = new ProviderControlOutcomeUnknown({
        driver,
        providerSessionId: oldSessionId,
        nativeThreadId: "native-thread:restart-session",
        operation: "interrupt",
      });
      yield* Ref.update(projection, (current) => ({
        ...current,
        providerThreads: current.providerThreads.map((row) => ({
          ...row,
          providerSessionId: oldSessionId,
        })),
        providerTurns: current.providerTurns.map((row) => ({
          ...row,
          status: "running" as const,
          completedAt: null,
        })),
      }));
      yield* Effect.gen(function* () {
        const control = yield* ProviderTurnControlService.ProviderTurnControlServiceV2;
        const result = yield* Effect.exit(
          control.interrupt({
            threadId,
            providerSessionId: oldSessionId,
            providerThreadId,
            providerTurnId,
          }),
        );
        assert.isTrue(Exit.isFailure(result));
        if (Exit.isFailure(result)) {
          assert.strictEqual(findProviderControlOutcomeUnknown(result.cause), unknownControl);
        }
      }).pipe(Effect.provide(layerControl));
    }),
);

it.effect.each([
  "current",
  "binding_changed",
  "model_changed",
  "batch_changed",
  "runtime_replaced",
  "stop_during_reads",
  "captured_stop_before_native",
  "goal_changed",
  "delivery_unknown",
  "not_delivered",
] as const)("steer retains original target and notification delivery evidence: %s", (scenario) =>
  Effect.gen(function* () {
    const now = yield* DateTime.now;
    const threadId = ThreadId.make("steer-original");
    const sessionId = ProviderSessionId.make("steer-original-session");
    const providerThreadId = ProviderThreadId.make("steer-original-provider-thread");
    const providerTurnId = ProviderTurnId.make("steer-original-turn");
    const attemptId = RunAttemptId.make("steer-original-attempt");
    const runId = RunId.make("steer-original-run");
    const messageId = MessageId.make("steer-original-notification");
    const providerThread = {
      id: providerThreadId,
      appThreadId: threadId,
      providerSessionId: sessionId,
      providerInstanceId,
      driver,
      nativeThreadRef: { driver, nativeId: "original-native", strength: "strong" },
      goal: { status: "active" },
      runtimeIdentity: { runtimeGeneration: "original-generation", evidenceRevision: 1 },
    } as OrchestrationV2ProviderThread;
    const projection = makeProjection({ now, threadId, providerThread, providerTurnId, attemptId });
    const run = {
      id: runId,
      status: "running",
      activeAttemptId: attemptId,
      modelSelection,
      providerThreadId,
    } as OrchestrationV2ThreadProjection["runs"][number];
    const message: OrchestrationV2ThreadProjection["messages"][number] = {
      id: messageId,
      threadId,
      runId,
      text: "Captured notification",
      attachments: [],
      createdBy: "agent",
      creationSource: "server",
      nodeId: null,
      role: "user",
      streaming: false,
      createdAt: now,
      updatedAt: now,
      notification: {
        source: { kind: "delegated_task", taskIds: [NodeId.make("steer-original-task")] },
        outcome: "completed",
        summary: "Child finished",
      },
    };
    const turn = { ...projection.providerTurns[0]!, runAttemptId: attemptId };
    let reads = 0;
    let runtimeReads = 0;
    let stopReads = 0;
    let sent = 0;
    const runtime = {
      steerTurn: (input: ProviderAdapter.ProviderAdapterV2SteerInput) =>
        Effect.gen(function* () {
          sent++;
          assert.equal(input.providerThread.nativeThreadRef?.nativeId, "original-native");
          assert.equal(input.message.text, message.text);
          if (scenario === "delivery_unknown" || scenario === "not_delivered")
            return yield* new ProviderAdapter.ProviderAdapterSteerRunError({
              driver,
              providerThreadId,
              providerTurnId,
              cause:
                scenario === "not_delivered"
                  ? new GoalNotificationNotDelivered()
                  : new Error("native delivery unknown"),
            });
        }),
    } as ProviderAdapter.ProviderAdapterV2SessionRuntime;
    const layer = ProviderTurnControlService.layer.pipe(
      Layer.provide(McpAppModelContext.layerEmpty),
      Layer.provide(
        Layer.mergeAll(
          Layer.mock(EventSink.EventSinkV2)({
            supersededTerminalTrace: SupersededTerminalTrace.make(),
            hasAttributedRuntimeStop: (candidateThread, candidateRun) => {
              assert.equal(candidateThread, threadId);
              assert.equal(candidateRun, runId);
              return Effect.succeed(scenario === "captured_stop_before_native");
            },
          }),
          Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
            get: () =>
              Effect.sync(() =>
                Option.some(
                  scenario === "runtime_replaced" && ++runtimeReads > 1 ? { ...runtime } : runtime,
                ),
              ),
          }),
          Layer.mock(ProjectionStore.ProjectionStoreV2)({
            getProviderControlContext: () =>
              Effect.sync(() => {
                const changed = ++reads > 2;
                return {
                  providerThread:
                    changed && scenario === "binding_changed"
                      ? {
                          ...providerThread,
                          nativeThreadRef: {
                            driver,
                            nativeId: "replacement",
                            strength: "strong" as const,
                          },
                        }
                      : changed && scenario === "goal_changed"
                        ? { ...providerThread, goal: { status: "paused" } as never }
                        : providerThread,
                  providerTurn: turn,
                  attempt: projection.attempts[0],
                  message,
                  run:
                    changed && scenario === "model_changed"
                      ? { ...run, modelSelection: { ...modelSelection, model: "other" } }
                      : run,
                };
              }),
            getThreadRecords: (_id, sections) =>
              Effect.succeed({
                ...projection,
                messages:
                  sections.some((section) => section === "messages") && scenario === "batch_changed"
                    ? [{ ...message, text: "Changed after capture" }]
                    : [message],
                turnItems:
                  sections.some((section) => section === "turnItems") &&
                  ++stopReads > 1 &&
                  scenario === "stop_during_reads"
                    ? [{ type: "run_interrupt_request" } as never]
                    : [],
              }),
          }),
        ),
      ),
    );
    const result = yield* Effect.gen(function* () {
      const controls = yield* ProviderTurnControlService.ProviderTurnControlServiceV2;
      return yield* controls
        .steer({
          threadId,
          providerSessionId: sessionId,
          providerThreadId,
          providerTurnId,
          messageId,
        })
        .pipe(Effect.result);
    }).pipe(Effect.provide(layer));
    const attempted = ["current", "delivery_unknown", "not_delivered"].includes(scenario);
    assert.equal(sent, attempted ? 1 : 0);
    if (scenario === "current") assert.equal(result._tag, "Success");
    else {
      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") {
        if (scenario === "delivery_unknown") {
          assert.instanceOf(result.failure.cause, GoalNotificationDeliveryUnknown);
          assert.isFalse(result.failure.turnCompleted === true);
        }
        if (scenario === "not_delivered") assert.isTrue(result.failure.turnCompleted);
      }
    }
  }),
);

// Restored from receiving: the Stop budget contract of the restored production bound.
const makeStopFixture = (
  interruptTurn: ProviderAdapter.ProviderAdapterV2SessionRuntime["interruptTurn"],
) =>
  Effect.gen(function* () {
    const now = yield* DateTime.now;
    const threadId = ThreadId.make("thread:stop-budget");
    const sessionId = ProviderSessionId.make("provider-session:stop-budget");
    const providerThreadId = ProviderThreadId.make("provider-thread:stop-budget");
    const providerTurnId = ProviderTurnId.make("provider-turn:stop-budget");
    const attemptId = RunAttemptId.make("run-attempt:stop-budget");
    const providerThread: OrchestrationV2ProviderThread = {
      id: providerThreadId,
      driver,
      providerInstanceId,
      providerSessionId: sessionId,
      appThreadId: threadId,
      ownerNodeId: null,
      nativeThreadRef: { driver, nativeId: "native-thread:stop-budget", strength: "strong" },
      nativeConversationHeadRef: {
        driver,
        nativeId: "native-head:stop-budget",
        strength: "strong",
      },
      status: "not_loaded",
      firstRunOrdinal: 1,
      lastRunOrdinal: 1,
      handoffIds: [],
      forkedFrom: null,
      createdAt: now,
      updatedAt: now,
    };
    const projection = makeProjection({ now, threadId, providerThread, providerTurnId, attemptId });
    const runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime = {
      instanceId: providerInstanceId,
      driver,
      providerSessionId: sessionId,
      providerSession: {
        id: sessionId,
        driver,
        providerInstanceId,
        status: "running" as const,
        cwd: "/workspace",
        model: modelSelection.model,
        capabilities: CodexProviderCapabilitiesV2,
        createdAt: now,
        updatedAt: now,
        lastError: null,
      },
      events: Stream.empty,
      ensureThread: () => Effect.die("unused ensureThread"),
      resumeThread: () => Effect.die("unused resumeThread"),
      startTurn: () => Effect.die("unused startTurn"),
      steerTurn: () => Effect.die("unused steerTurn"),
      interruptTurn,
      respondToRuntimeRequest: () => Effect.die("unused respondToRuntimeRequest"),
      readThreadSnapshot: () => Effect.die("unused readThreadSnapshot"),
      rollbackThread: () => Effect.die("unused rollbackThread"),
      forkThread: () => Effect.die("unused forkThread"),
    };
    const layer = ProviderTurnControlService.layer.pipe(
      Layer.provide(McpAppModelContext.layerEmpty),
      Layer.provide(
        Layer.mock(EventSink.EventSinkV2)({
          supersededTerminalTrace: SupersededTerminalTrace.make(),
          latestSequence: () => Effect.succeed(0),
          stream: () => Stream.never,
        }),
      ),
      Layer.provide(
        Layer.mergeAll(
          Layer.mock(ProjectionStore.ProjectionStoreV2)({
            getProviderControlContext: (_threadId, target) =>
              Effect.succeed({
                providerThread: projection.providerThreads.find(
                  (thread) => thread.id === target.providerThreadId,
                ),
                providerTurn: projection.providerTurns.find(
                  (turn) => turn.id === target.providerTurnId,
                ),
                attempt: projection.attempts.find((attempt) => attempt.id === target.attemptId),
                message: undefined,
                run: undefined,
              }),
          }),
          Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
            get: (providerSessionId) =>
              Effect.succeed(
                providerSessionId === sessionId ? Option.some(runtime) : Option.none(),
              ),
          }),
        ),
      ),
    );
    return { layer, threadId, sessionId, providerThreadId, providerTurnId, attemptId };
  });

function layerProjectionStore(
  getProviderControlContext: ProjectionStore.ProjectionStoreV2["Service"]["getProviderControlContext"],
) {
  return Layer.mock(ProjectionStore.ProjectionStoreV2)({ getProviderControlContext });
}

function layerSessions(
  liveSessionId: ProviderSessionId,
  runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime,
) {
  return Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
    get: (providerSessionId) =>
      Effect.succeed(providerSessionId === liveSessionId ? Option.some(runtime) : Option.none()),
  });
}

it.effect(
  "hands the adapter the child budget and reports an adapter stop timeout as unresponsive",
  () =>
    Effect.gen(function* () {
      let adapterDeadline: number | undefined;
      const fixture = yield* makeStopFixture(({ providerThread, providerTurnId }) =>
        Effect.gen(function* () {
          adapterDeadline = yield* ProviderControlDeadline;
          return yield* new ProviderAdapter.ProviderAdapterInterruptTimeoutError({
            driver,
            providerThreadId: providerThread.id,
            providerTurnId,
            timeoutMs: ProviderSessionManager.PROVIDER_INTERRUPT_TIMEOUT_MS,
          });
        }),
      );
      const startedAt = yield* Clock.currentTimeMillis;
      const error = yield* Effect.gen(function* () {
        const control = yield* ProviderTurnControlService.ProviderTurnControlServiceV2;
        return yield* control
          .interrupt({
            threadId: fixture.threadId,
            providerSessionId: fixture.sessionId,
            providerThreadId: fixture.providerThreadId,
            providerTurnId: fixture.providerTurnId,
          })
          .pipe(Effect.flip);
      }).pipe(Effect.provide(fixture.layer));

      assert.equal(adapterDeadline, startedAt + STOP_CHILD_BUDGET_MS);
      assert.equal(error._tag, "ProviderTurnControlError");
      assert.nestedPropertyVal(error, "operation", "interrupt");
      assert.nestedPropertyVal(error, "unresponsive", true);
    }),
);

it.effect("fails a restart interrupt that never answers before the session manager's timeout", () =>
  Effect.gen(function* () {
    const fixture = yield* makeStopFixture(() => Effect.never);
    yield* Effect.gen(function* () {
      const control = yield* ProviderTurnControlService.ProviderTurnControlServiceV2;
      const restarting = yield* control
        .interruptAndAwaitTerminal({
          threadId: fixture.threadId,
          providerSessionId: fixture.sessionId,
          replacementProviderSessionId: ProviderSessionId.make(
            "provider-session:stop-budget:replacement",
          ),
          providerThreadId: fixture.providerThreadId,
          providerTurnId: fixture.providerTurnId,
          interruptedAttemptId: fixture.attemptId,
        })
        .pipe(Effect.flip, Effect.forkScoped);
      yield* TestClock.adjust(ProviderSessionManager.PROVIDER_INTERRUPT_TIMEOUT_MS - 1);
      const error = yield* Fiber.join(restarting);
      assert.equal(error._tag, "ProviderTurnControlError");
      assert.nestedPropertyVal(error, "operation", "restart");
    }).pipe(Effect.scoped, Effect.provide(fixture.layer));
  }),
);

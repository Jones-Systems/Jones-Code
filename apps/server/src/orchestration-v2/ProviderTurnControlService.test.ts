import * as EventSink from "./EventSink.ts";
import { assert, it } from "@effect/vitest";
import {
  type ModelSelection,
  NodeId,
  type OrchestrationV2ProviderThread,
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
      const layerProjection = Layer.succeed(
        ProjectionStore.ProjectionStoreV2,
        ProjectionStore.ProjectionStoreV2.of({
          apply: () => Effect.void,
          hasQueuedToolBoundaryWork: () => Effect.die("unused hasQueuedToolBoundaryWork"),
          getQueuedToolBoundaryContext: () => Effect.die("unused getQueuedToolBoundaryContext"),
          getLimitRecoveryCandidates: () => Effect.die("unused getLimitRecoveryCandidates"),
          getLimitRecoveryWakeAt: () => Effect.die("unused getLimitRecoveryWakeAt"),
          getOpenRemoteDelegatedTasks: Effect.die("unused getOpenRemoteDelegatedTasks"),
          getAgentAwarenessSnapshot: () => Effect.die("unused awareness snapshot"),
          getShellSnapshot: () => Effect.die("unused getShellSnapshot"),
          listActiveSubagentThreadIds: () => Effect.die("unused listActiveSubagentThreadIds"),
          countThreads: () => Effect.die("unused countThreads"),
          listActiveShellIndex: () => Effect.die("unused listActiveShellIndex"),
          listOlderSettledThreads: () => Effect.die("unused listOlderSettledThreads"),
          readShellSnapshot: () => Effect.die("unused readShellSnapshot"),
          getThreadShell: () => Effect.die("unused getThreadShell"),
          getThreadTitles: () => Effect.die("unused getThreadTitles"),
          getThreadLinkMetadata: () => Effect.die("unused getThreadLinkMetadata"),
          findGuestShareThreadId: () => Effect.die("unused findGuestShareThreadId"),
          getThread: () => Ref.get(projection).pipe(Effect.map((state) => state.thread)),
          getSettlementCandidates: () => Effect.die("unused getSettlementCandidates"),
          getThreadsWithPullRequests: () => Effect.die("unused getThreadsWithPullRequests"),
          getThreadWorktreePaths: () => Effect.die("unused getThreadWorktreePaths"),
          getThreadProjection: () => Effect.die("control effects must not load transcript"),
          getTurnStartContext: () => Effect.die("unused"),
          getTurnStartHistory: () => Effect.die("unused"),
          getRuntimeRecoveryProjection: () => Effect.die("unused getRuntimeRecoveryProjection"),
          getPlan: () => Effect.die("unused"),
          hasUnpairedRunInterruptRequest: () => Effect.die("unused interrupt read"),
          getThreadAttachmentIds: () => Effect.die("Unused attachment lookup"),
          searchThread: () => Effect.die("unused"),
          searchThreadStream: () => Stream.empty,
          getThreadHistoryPage: () => Effect.die("unused"),
          getTimelinePage: () => Effect.die("Unused timeline read"),
          getTimelinePages: () => Effect.die("Unused timeline read"),
          getMessageCount: () => Effect.die("unused message count"),
          getSubagentCount: () => Effect.die("unused subagent count"),
          listAppOwnedChildThreadIds: () => Effect.die("unused listAppOwnedChildThreadIds"),
          getNextTurnItemOrdinal: () => Effect.die("unused ordinal read"),
          getTurnItem: () => Effect.die("unused turn item read"),
          getMessagesById: () => Effect.die("unused getMessagesById"),
          getThreadRecords: () => Effect.die("unused record read"),
          getRuntimeRequest: () => Effect.die("unused getRuntimeRequest"),
          getRunningTurnContext: () => Effect.die("unused getRunningTurnContext"),
          getThreadProviderContext: () => Effect.die("unused getThreadProviderContext"),
          getRuntimeResponseContext: () => Effect.die("unused getRuntimeResponseContext"),
          getPendingNativeUserInputs: () => Effect.die("unused getPendingNativeUserInputs"),
          getProviderControlContext: (_threadId, target) =>
            Ref.get(projection).pipe(
              Effect.map((current) => ({
                providerThread: current.providerThreads.find(
                  (thread) => thread.id === target.providerThreadId,
                ),
                providerTurn: current.providerTurns.find(
                  (turn) => turn.id === target.providerTurnId,
                ),
                attempt: current.attempts.find((attempt) => attempt.id === target.attemptId),
                message: undefined,
                run: undefined,
              })),
            ),
          getCheckpointContext: () => Effect.die("not used"),
          getCheckpointRollbackContext: () => Effect.die("unused rollback context"),
          getCheckpointCaptureContext: () => Effect.die("not used"),
          getRunMessage: () => Effect.die("not used"),
          isNodeStored: () => Effect.die("not used"),
          canStartQueuedRun: () => Effect.die("not used"),
          getHeldQueuedRuns: () => Effect.die("not used"),
          getRecoveryThreadIds: () => Effect.die("unused getRecoveryThreadIds"),
          getUnreadableThreadIds: () => Effect.die("unused getUnreadableThreadIds"),
          getThreadSnapshot: () => Effect.die("unused getThreadSnapshot"),
          getThreadSnapshotWindow: () => Effect.die("unused getThreadSnapshotWindow"),
          getPullRequestSnapshot: () => Effect.die("unused getPullRequestSnapshot"),
        }),
      );
      const layerSessionManager = Layer.succeed(
        ProviderSessionManager.ProviderSessionManagerV2,
        ProviderSessionManager.ProviderSessionManagerV2.of({
          isMcpCallerAttached: () => Effect.succeed(false),
          shutdown: Effect.void,
          open: () => Effect.die("unused open"),
          get: (providerSessionId) =>
            Effect.succeed(
              providerSessionId === oldSessionId ? Option.some(runtime) : Option.none(),
            ),
          isLive: (providerSessionId) => Effect.succeed(providerSessionId === oldSessionId),
          close: () => Effect.void,
          closeInstance: () => Effect.void,
          release: () => Effect.void,
          detach: () => Effect.void,
          otherWorkingThreads: () => Effect.succeed([]),
          releaseUnlessBusy: () => Effect.succeed(true),
          subscribePromptSuggestion: () => Stream.empty,
        }),
      );
      const layerControl = ProviderTurnControlService.layer.pipe(
        Layer.provide(
          Layer.mock(EventSink.EventSinkV2)({
            latestSequence: () => Effect.succeed(0),
            stream: () => Stream.never,
          }),
        ),
        Layer.provide(Layer.merge(layerProjection, layerSessionManager)),
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
      Layer.provide(
        Layer.mock(EventSink.EventSinkV2)({
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

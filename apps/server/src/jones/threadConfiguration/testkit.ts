import {
  type ModelSelection,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderThread,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  type ProviderSessionId,
  type ThreadId,
  TurnItemId,
  MessageId,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
export const driver = ProviderDriverKind.make("codex");
export const providerInstanceId = ProviderInstanceId.make("codex-restart-test");
export const initialSelection = {
  instanceId: providerInstanceId,
  model: "restart-model-a",
} satisfies ModelSelection;
export const replacementSelection = {
  instanceId: providerInstanceId,
  model: "restart-model-b",
} satisfies ModelSelection;
const pooledCapabilities: OrchestrationV2ProviderCapabilities = CodexProviderCapabilitiesV2;
export interface ActiveTurn {
  readonly events: Queue.Queue<ProviderAdapter.ProviderAdapterV2Event>;
  readonly input: ProviderAdapter.ProviderAdapterV2TurnInput;
  readonly providerTurnId: ProviderTurnId;
}

export interface RestartAdapterState {
  readonly activeTurn: ActiveTurn | null;
  readonly opened: ReadonlyArray<{
    readonly model: string | null;
    readonly closedBeforeOpen: number;
    readonly cwd: string | null;
  }>;
  readonly started: ReadonlyArray<{
    readonly model: string;
    readonly selection: ModelSelection;
    readonly cwd: string | null;
    readonly attemptId: string;
    readonly sessionId: ProviderSessionId;
    readonly instanceId: ProviderInstanceId;
    readonly input: ProviderAdapter.ProviderAdapterV2TurnInput;
  }>;
  readonly steered?: ReadonlyArray<ProviderAdapter.ProviderAdapterV2SteerInput>;
  readonly interrupted?: ReadonlyArray<ProviderAdapter.ProviderAdapterV2InterruptInput>;
  readonly resumed?: ReadonlyArray<{
    readonly sessionId: ProviderSessionId;
    readonly selection: ModelSelection | undefined;
  }>;
  readonly rejectSelection?: boolean;
  readonly handoffSelection?: boolean;
  readonly failInterrupt?: boolean;
  readonly openGate?: {
    readonly entered: Deferred.Deferred<void>;
    readonly release: Deferred.Deferred<void>;
  };
  readonly closedSessionCount: number;
  readonly detached?: ReadonlyArray<{
    readonly providerSessionId: ProviderSessionId;
    readonly threadId: ThreadId;
  }>;
  readonly failedReplacementOpen: boolean;
}

export function makeRestartAdapter(
  state: Ref.Ref<RestartAdapterState>,
  sessionCapabilities: OrchestrationV2ProviderCapabilities = {
    ...pooledCapabilities,
    sessions: { ...pooledCapabilities.sessions, supportsMultipleProviderThreadsPerSession: false },
  },
  providerInstanceId = initialSelection.instanceId,
  restartRequired = true,
  driver: ProviderDriverKind = ProviderDriverKind.make("codex"),
): ProviderAdapter.ProviderAdapterV2["Service"] {
  return {
    instanceId: providerInstanceId,
    driver,
    getCapabilities: () => Effect.succeed(sessionCapabilities),
    planSelectionTransition: ({ current, target }) =>
      Ref.get(state).pipe(
        Effect.map((value) =>
          value.rejectSelection
            ? { type: "reject" as const, reason: "Synthetic selection refusal" }
            : value.handoffSelection
              ? { type: "create_with_handoff" as const }
              : !restartRequired || current.model === target.model
                ? { type: "apply_on_next_turn" as const }
                : { type: "restart_session" as const },
        ),
      ),
    openSession: Effect.fn("ThreadConfigurationTest.openSession")(function* (sessionInput) {
      const failThisOpen = yield* Ref.modify(state, (current) => {
        const shouldFail =
          sessionInput.modelSelection.model === replacementSelection.model &&
          !current.failedReplacementOpen;
        return [
          shouldFail,
          {
            ...current,
            failedReplacementOpen: current.failedReplacementOpen || shouldFail,
            opened: [
              ...current.opened,
              {
                model: sessionInput.modelSelection.model,
                closedBeforeOpen: current.closedSessionCount,
                cwd: sessionInput.runtimePolicy.cwd,
              },
            ],
          },
        ] as const;
      });
      if (failThisOpen) {
        return yield* new ProviderAdapter.ProviderAdapterOpenSessionError({
          driver,
          providerSessionId: sessionInput.providerSessionId,
          cause: "simulated replacement open failure",
        });
      }

      const gate = (yield* Ref.get(state)).openGate;
      if (gate !== undefined) {
        yield* Deferred.succeed(gate.entered, undefined);
        yield* Deferred.await(gate.release);
      }
      const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
      const now = yield* DateTime.now;
      const providerSession: OrchestrationV2ProviderSession = {
        id: sessionInput.providerSessionId,
        driver,
        providerInstanceId,
        status: "ready",
        cwd: sessionInput.runtimePolicy.cwd ?? "/fallback",
        model: sessionInput.modelSelection.model,
        capabilities: sessionCapabilities,
        createdAt: now,
        updatedAt: now,
        lastError: null,
      };
      yield* Effect.addFinalizer(() =>
        Ref.update(state, (current) => ({
          ...current,
          closedSessionCount: current.closedSessionCount + 1,
        })),
      );

      const publishTerminal = Effect.fn("ThreadConfigurationTest.publishTerminal")(function* (
        active: ActiveTurn,
        status: "completed" | "interrupted",
      ) {
        const occurredAt = yield* DateTime.now;
        yield* Queue.offer(events, {
          type: "provider_turn.updated",
          driver,
          providerTurn: {
            id: active.providerTurnId,
            providerThreadId: active.input.providerThread.id,
            nodeId: active.input.rootNodeId,
            runAttemptId: active.input.attemptId,
            nativeTurnRef: {
              driver,
              nativeId: `native:${active.providerTurnId}`,
              strength: "strong",
            },
            ordinal: active.input.providerTurnOrdinal,
            status,
            startedAt: occurredAt,
            completedAt: occurredAt,
          },
        });
        yield* Queue.offer(events, {
          type: "turn.terminal",
          driver,
          providerThreadId: active.input.providerThread.id,
          providerTurnId: active.providerTurnId,
          runOrdinal: active.input.runOrdinal,
          status,
          failure: null,
          threadDisposition: "reusable",
        });
      });

      const runtimeGeneration =
        sessionInput.runtimeLifecycle === undefined
          ? `synthetic:${sessionInput.providerSessionId}`
          : yield* sessionInput.runtimeLifecycle.reserve(sessionInput.threadId);
      if (sessionInput.runtimeLifecycle !== undefined)
        yield* sessionInput.runtimeLifecycle.admit({
          runtimeGeneration,
          threadId: sessionInput.threadId,
        });
      const bindThread = Effect.fn("ThreadConfigurationTest.bindThread")(function* (
        providerThread: OrchestrationV2ProviderThread,
      ) {
        const requested = ProviderAdapter.requestedRuntimeIdentity(
          sessionInput.modelSelection,
          driver,
        );
        const observed = ProviderAdapter.unobservedRuntimeIdentity();
        return sessionInput.runtimeLifecycle === undefined
          ? { ...providerThread, runtimeIdentity: { runtimeGeneration, requested, observed } }
          : yield* sessionInput.runtimeLifecycle.bind({
              providerThread,
              runtimeGeneration,
              requested,
              observed,
            });
      });
      return {
        instanceId: providerInstanceId,
        driver,
        providerSessionId: sessionInput.providerSessionId,
        providerSession,
        events: Stream.fromQueue(events),
        ensureThread: Effect.fn("ThreadConfigurationTest.ensureThread")(function* (threadInput) {
          const createdAt = yield* DateTime.now;
          const id =
            threadInput.existingProviderThread?.id ??
            ProviderThreadId.make(`provider-thread:${threadInput.threadId}`);
          return yield* bindThread({
            id,
            driver,
            providerInstanceId,
            providerSessionId: sessionInput.providerSessionId,
            appThreadId: threadInput.threadId,
            ownerNodeId: null,
            nativeThreadRef: {
              driver,
              nativeId: `native-thread:${id}`,
              strength: "strong",
            },
            nativeConversationHeadRef: null,
            status: "idle",
            firstRunOrdinal: null,
            lastRunOrdinal: null,
            handoffIds: [],
            forkedFrom: null,
            createdAt,
            updatedAt: createdAt,
          } satisfies OrchestrationV2ProviderThread);
        }),
        retireThreadForTransfer: Effect.fn("ThreadConfigurationTest.retireThreadForTransfer")(
          function* ({ providerThread, allowProducerRetirement }) {
            const binding = ProviderAdapter.runtimeBinding(providerThread, runtimeGeneration);
            if (
              binding === undefined ||
              providerThread.runtimeIdentity?.runtimeGeneration !== runtimeGeneration
            )
              return yield* new ProviderAdapter.ProviderRuntimeBindingError({
                driver,
                detail: "Synthetic writer binding changed.",
              });
            return {
              binding,
              evidence:
                allowProducerRetirement &&
                !sessionCapabilities.sessions.supportsMultipleProviderThreadsPerSession
                  ? ("producer_exited" as const)
                  : ("thread_closed" as const),
            };
          },
        ),
        resumeThread: ({ providerThread, modelSelection }) =>
          Ref.update(state, (current) => ({
            ...current,
            resumed: [
              ...(current.resumed ?? []),
              { sessionId: sessionInput.providerSessionId, selection: modelSelection },
            ],
          })).pipe(
            Effect.andThen(
              bindThread({
                ...providerThread,
                providerInstanceId,
                providerSessionId: sessionInput.providerSessionId,
              }),
            ),
          ),
        startTurn: Effect.fn("ThreadConfigurationTest.startTurn")(function* (input) {
          yield* Ref.update(state, (current) => ({
            ...current,
            started: [
              ...current.started,
              {
                model: input.modelSelection.model,
                selection: input.modelSelection,
                cwd: input.runtimePolicy.cwd,
                attemptId: input.attemptId,
                sessionId: sessionInput.providerSessionId,
                instanceId: providerInstanceId,
                input,
              },
            ],
          }));
          const active = {
            events,
            input,
            providerTurnId: ProviderTurnId.make(`provider-turn:${input.attemptId}`),
          } satisfies ActiveTurn;
          if (input.modelSelection.model === initialSelection.model) {
            const occurredAt = yield* DateTime.now;
            yield* Ref.update(state, (current) => ({ ...current, activeTurn: active }));
            yield* Queue.offer(events, {
              type: "provider_turn.updated",
              driver,
              providerTurn: {
                id: active.providerTurnId,
                providerThreadId: input.providerThread.id,
                nodeId: input.rootNodeId,
                runAttemptId: input.attemptId,
                nativeTurnRef: {
                  driver,
                  nativeId: `native:${active.providerTurnId}`,
                  strength: "strong",
                },
                ordinal: input.providerTurnOrdinal,
                status: "running",
                startedAt: occurredAt,
                completedAt: null,
              },
            });
            return;
          }
          yield* publishTerminal(active, "completed");
        }),
        steerTurn: (input) =>
          Ref.update(state, (current) => ({
            ...current,
            steered: [...(current.steered ?? []), input],
          })),
        interruptTurn: Effect.fn("ThreadConfigurationTest.interruptTurn")(function* (input) {
          yield* Ref.update(state, (current) => ({
            ...current,
            interrupted: [...(current.interrupted ?? []), input],
          }));
          if ((yield* Ref.get(state)).failInterrupt)
            return yield* new ProviderAdapter.ProviderAdapterInterruptError({
              driver,
              providerThreadId: input.providerThread.id,
              providerTurnId: input.providerTurnId,
              cause: "Synthetic interrupt failure",
            });
          const active = (yield* Ref.get(state)).activeTurn;
          if (active !== null) {
            const updatedAt = yield* DateTime.now;
            yield* Queue.offer(events, {
              type: "provider_thread.updated",
              driver,
              providerThread: {
                ...active.input.providerThread,
                status: "idle",
                updatedAt,
              },
            });
            yield* publishTerminal(active, "interrupted");
            yield* Ref.update(state, (current) => ({ ...current, activeTurn: null }));
          }
        }),
        respondToRuntimeRequest: () => Effect.void,
        readThreadSnapshot: () => Effect.die("unused readThreadSnapshot"),
        rollbackThread: () => Effect.die("unused rollbackThread"),
        forkThread: () => Effect.die("unused forkThread"),
      };
    }),
  };
}

export const finishActive = Effect.fn("ThreadConfigurationTest.finishActive")(function* (
  state: Ref.Ref<RestartAdapterState>,
) {
  const active = (yield* Ref.get(state)).activeTurn;
  if (active === null) return yield* Effect.die("No held turn");
  const now = yield* DateTime.now;
  yield* Queue.offer(active.events, {
    type: "turn_item.updated",
    driver,
    turnItem: {
      id: TurnItemId.make("continuation:" + active.input.attemptId),
      threadId: active.input.threadId,
      runId: active.input.runId,
      nodeId: active.input.rootNodeId,
      providerThreadId: active.input.providerThread.id,
      providerTurnId: active.providerTurnId,
      nativeItemRef: null,
      parentItemId: null,
      ordinal: 100,
      status: "completed",
      title: null,
      startedAt: now,
      completedAt: now,
      updatedAt: now,
      messageId: MessageId.make("continuation:" + active.input.attemptId),
      type: "assistant_message",
      streaming: false,
      text: "continued-after-self-configuration",
    },
  });
  yield* Queue.offer(active.events, {
    type: "provider_turn.updated",
    driver,
    providerTurn: {
      id: active.providerTurnId,
      providerThreadId: active.input.providerThread.id,
      nodeId: active.input.rootNodeId,
      runAttemptId: active.input.attemptId,
      nativeTurnRef: { driver, nativeId: "native:" + active.providerTurnId, strength: "strong" },
      ordinal: active.input.providerTurnOrdinal,
      status: "completed",
      startedAt: now,
      completedAt: now,
    },
  });
  yield* Queue.offer(active.events, {
    type: "turn.terminal",
    driver,
    providerThreadId: active.input.providerThread.id,
    providerTurnId: active.providerTurnId,
    runOrdinal: active.input.runOrdinal,
    status: "completed",
    failure: null,
    threadDisposition: "reusable",
  });
});

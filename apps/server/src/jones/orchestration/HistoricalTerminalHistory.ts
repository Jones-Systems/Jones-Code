import {
  type CommandId,
  type EventId,
  OrchestrationV2DomainEvent,
  OrchestrationV2ProviderThreadJson,
  OrchestrationV2ExecutionNodeJson,
  OrchestrationV2RunAttemptJson,
  type ProviderInstanceId,
  type ProviderSessionId,
  type RunId,
  type ThreadId,
} from "@t3tools/contracts";
import type { ProviderAdapterV2Event } from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderEventOrigin from "@t3tools/provider-core/server/jones/ProviderEventOrigin";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as EffectOutbox from "../../orchestration-v2/EffectOutbox.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import {
  RecordedRunJson,
  type RecordedStoredLifecycleEvent,
} from "../../orchestration-v2/RecordedTypes.ts";

import type { Reason } from "./SupersededTerminalTrace.ts";

export interface HistoricalTerminalInput {
  readonly providerSessionId: ProviderSessionId;
  readonly providerInstanceId: ProviderInstanceId;
  readonly threadId: ThreadId;
  readonly runId?: RunId;
  readonly event: Extract<ProviderAdapterV2Event, { type: "provider_turn.updated" }>;
  readonly terminal: OrchestrationV2DomainEvent;
}

export const isHistoricalTerminalRejection = (
  selected: "ordinary" | OrchestrationV2DomainEvent | HistoricalTerminalRejection,
): selected is HistoricalTerminalRejection =>
  selected !== "ordinary" && (selected.type === "stale" || selected.type === "deferred");

type HistoricalTerminalRejection = {
  readonly type: "stale" | "deferred";
  readonly reason: Reason;
};
const stale = (reason: Reason): HistoricalTerminalRejection => ({ type: "stale", reason });

// Only an exact persisted historical terminal can replace an ordinary ingestion error.
// Cancellation and unavailable/refused history preserve the original failure channel.
export function retainHistoricalTerminalOnError<A, E, R, E2, R2>(
  effect: Effect.Effect<ReadonlyArray<A>, E, R>,
  historicalWrite:
    | Effect.Effect<
        {
          readonly handled: boolean;
          readonly persisted?: boolean;
          readonly storedEvents: ReadonlyArray<A>;
        },
        E2,
        R2
      >
    | undefined,
): Effect.Effect<ReadonlyArray<A>, E, R | R2> {
  if (historicalWrite === undefined) return effect;
  return effect.pipe(
    Effect.catchCauseIf(
      (cause) => !Cause.hasInterruptsOnly(cause),
      (cause) =>
        historicalWrite.pipe(
          Effect.catchCause((fallback) =>
            Cause.hasInterruptsOnly(fallback)
              ? Effect.failCause(
                  Cause.fromReasons<never>(fallback.reasons.filter(Cause.isInterruptReason)),
                )
              : Effect.failCause(cause),
          ),
          Effect.flatMap((result) =>
            result.persisted === true || (result.handled && result.storedEvents.length > 0)
              ? Effect.succeed(result.storedEvents)
              : Effect.failCause(cause),
          ),
        ),
    ),
  );
}

type Locator = {
  readonly effectId: string;
  readonly commandId: CommandId;
  readonly successorEventId: EventId;
};
const decodeNode = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2ExecutionNodeJson),
);
const decodeRun = Schema.decodeUnknownEffect(Schema.fromJsonString(RecordedRunJson));
const decodeAttempt = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2RunAttemptJson),
);
const decodeThread = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2ProviderThreadJson),
);
const isTerminal = (status: string) =>
  ["completed", "failed", "interrupted", "cancelled"].includes(status);

export const isHistoricalTerminalCandidate = (
  event: ProviderAdapterV2Event,
): event is Extract<ProviderAdapterV2Event, { type: "provider_turn.updated" }> =>
  event.type === "provider_turn.updated" &&
  event.runtimeEvidence !== undefined &&
  isTerminal(event.providerTurn.status);

export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const outbox = yield* EffectOutbox.EffectOutboxV2;
  const events = yield* EventStore.EventStoreV2;
  // This process-local locator is installed only after commit. Captured producers
  // cannot survive process loss; all authority is re-read from persisted rows.
  const locators = new Map<ThreadId, Locator>();
  // Selection precedes the fallible append. Missing readback after that point
  // stays unresolved; it cannot authorize another historical commit.
  const retained = new WeakMap<HistoricalTerminalInput, OrchestrationV2DomainEvent>();
  const encode = Schema.encodeEffect(Schema.fromJsonString(OrchestrationV2DomainEvent));
  const readCommitted = Effect.fn("HistoricalTerminalHistory.readCommitted")(function* (
    input: HistoricalTerminalInput,
  ) {
    const expected = retained.get(input);
    if (expected === undefined) return undefined;
    const stored = yield* events.read({ eventId: expected.id, limit: 1 }).pipe(Stream.runHead);
    if (Option.isNone(stored) || stored.value.event.type !== "provider-turn.updated")
      return "unavailable" as const;
    return (yield* encode(stored.value.event)) === (yield* encode(expected))
      ? stored.value
      : ("unavailable" as const);
  });
  const rememberCommitted = (
    effects: ReadonlyArray<EffectOutbox.PendingOrchestrationEffectV2>,
    stored: ReadonlyArray<RecordedStoredLifecycleEvent>,
  ) => {
    for (const effect of effects) {
      if (effect.request.type !== "provider-turn.restart") continue;
      const request = effect.request;
      const successors = stored.filter(
        (item) =>
          item.commandId === effect.commandId &&
          item.event.threadId === effect.threadId &&
          item.event.type === "run-attempt.created" &&
          item.event.payload.runId === request.runId &&
          item.event.payload.reason === "steering_restart",
      );
      if (successors.length !== 1) continue;
      locators.delete(effect.threadId);
      locators.set(effect.threadId, {
        effectId: effect.id,
        commandId: effect.commandId,
        successorEventId: successors[0]!.event.id,
      });
      // Eviction denies the exception; it never falls back to stale-event admission.
      if (locators.size > 4096) locators.delete(locators.keys().next().value!);
    }
  };

  // The caller owns the same SQL transaction as the eventual single-event append.
  const select = Effect.fn("HistoricalTerminalHistory.select")(function* (
    input: HistoricalTerminalInput,
  ) {
    const { event, terminal } = input;
    const turn = event.providerTurn;
    if (
      !isHistoricalTerminalCandidate(event) ||
      event.runtimeEvidence === undefined ||
      terminal.type !== "provider-turn.updated"
    )
      return "ordinary" as const;
    const control = yield* projections.getProviderControlContext(input.threadId, {
      providerThreadId: turn.providerThreadId,
      providerTurnId: turn.id,
      ...(turn.runAttemptId === null ? {} : { attemptId: turn.runAttemptId }),
    });
    if (control.attempt?.status !== "superseded") return "ordinary" as const;
    const locator = locators.get(input.threadId);
    const origin = ProviderEventOrigin.readProviderEventOrigin(event);
    const binding = origin?.turn?.binding;
    const capture = event.runtimeEvidence;
    if (
      origin?.turn === undefined ||
      binding === undefined ||
      binding.runtimeGeneration === undefined ||
      origin.producer.runtimeGeneration === undefined
    )
      return stale("origin");
    if (
      input.runId !== origin.turn.runId ||
      input.runId !== control.attempt.runId ||
      input.threadId !== binding.threadId ||
      (event.threadId !== undefined && event.threadId !== input.threadId) ||
      input.providerSessionId !== binding.providerSessionId ||
      input.providerInstanceId !== binding.providerInstanceId ||
      turn.id !== origin.turn.providerTurnId ||
      turn.runAttemptId !== origin.turn.attemptId ||
      turn.nodeId !== control.attempt.rootNodeId ||
      turn.providerThreadId !== binding.providerThreadId ||
      control.attempt.providerInstanceId !== binding.providerInstanceId ||
      control.attempt.providerThreadId !== binding.providerThreadId ||
      control.attempt.providerTurnId !== turn.id
    )
      return stale("identity");
    if (
      capture.threadId !== binding.threadId ||
      capture.providerThreadId !== binding.providerThreadId ||
      capture.providerSessionId !== binding.providerSessionId ||
      capture.providerInstanceId !== binding.providerInstanceId ||
      capture.driver !== binding.driver ||
      capture.nativeThreadId !== binding.nativeThreadId ||
      capture.runtimeGeneration !== binding.runtimeGeneration
    )
      return stale("capture");
    if (
      terminal.threadId !== input.threadId ||
      terminal.runId !== input.runId ||
      terminal.nodeId !== turn.nodeId ||
      terminal.driver !== event.driver ||
      terminal.providerInstanceId !== input.providerInstanceId ||
      control.providerTurn === undefined ||
      control.providerTurn.status !== "running" ||
      control.providerTurn.nodeId !== turn.nodeId ||
      control.providerTurn.runAttemptId !== turn.runAttemptId ||
      control.providerTurn.nativeTurnRef === null ||
      turn.nativeTurnRef === null ||
      control.providerTurn.nativeTurnRef.driver !== turn.nativeTurnRef.driver ||
      control.providerTurn.nativeTurnRef.nativeId !== turn.nativeTurnRef.nativeId ||
      turn.nativeTurnRef.driver !== binding.driver ||
      turn.completedAt === null
    )
      return stale("projection_turn");
    const producer = yield* ProviderEventOrigin.revalidateProviderEventOrigin(event, {
      driver: binding.driver,
      instanceId: input.providerInstanceId,
      providerSessionId: input.providerSessionId,
      eventOriginMode: "captured",
    }).pipe(Effect.result);
    if (producer._tag === "Failure") return stale("origin");
    if (locator === undefined) return { type: "deferred", reason: "locator" } as const;
    const effect = yield* outbox.get(locator.effectId);
    if (
      Option.isNone(effect) ||
      effect.value.commandId !== locator.commandId ||
      effect.value.threadId !== input.threadId ||
      effect.value.request.type !== "provider-turn.restart"
    )
      return stale("effect");
    const request = effect.value.request;
    if (
      request.runId !== input.runId ||
      request.interruptedAttemptId !== turn.runAttemptId ||
      request.providerTurnId !== turn.id ||
      request.providerThreadId !== binding.providerThreadId ||
      request.providerSessionId !== binding.providerSessionId
    )
      return stale("effect");
    const committed = Array.from(
      yield* events.readByCommandId({ commandId: locator.commandId }).pipe(Stream.runCollect),
    );
    const created = committed.find((stored) => stored.event.id === locator.successorEventId)?.event;
    if (
      created?.type !== "run-attempt.created" ||
      created.threadId !== input.threadId ||
      created.payload.runId !== input.runId ||
      created.payload.reason !== "steering_restart" ||
      created.payload.attemptOrdinal !== control.attempt.attemptOrdinal + 1 ||
      created.payload.providerTurnId !== null ||
      created.payload.status !== "pending"
    )
      return stale("successor");
    const successor = created.payload;
    const recordedRun = committed.find(
      (stored) => stored.event.type === "run.updated" && stored.event.payload.id === input.runId,
    )?.event;
    const retired = committed.find(
      (stored) =>
        stored.event.type === "run-attempt.updated" &&
        stored.event.payload.id === turn.runAttemptId,
    )?.event;
    const root = committed.find(
      (stored) =>
        stored.event.type === "node.updated" && stored.event.payload.id === successor.rootNodeId,
    )?.event;
    if (
      recordedRun?.type !== "run.updated" ||
      recordedRun.payload.activeAttemptId !== successor.id ||
      recordedRun.payload.rootNodeId !== successor.rootNodeId ||
      recordedRun.payload.status !== "starting" ||
      retired?.type !== "run-attempt.updated" ||
      retired.payload.status !== "superseded" ||
      retired.payload.providerTurnId !== turn.id ||
      retired.payload.rootNodeId !== turn.nodeId ||
      retired.payload.runId !== input.runId ||
      retired.payload.providerThreadId !== binding.providerThreadId ||
      retired.payload.providerInstanceId !== binding.providerInstanceId ||
      retired.payload.attemptOrdinal !== control.attempt.attemptOrdinal ||
      root?.type !== "node.updated" ||
      root.payload.runId !== input.runId ||
      root.payload.providerThreadId !== successor.providerThreadId ||
      root.payload.status !== "pending"
    )
      return stale("successor");
    const runRows = yield* sql<{
      payload_json: string;
    }>`SELECT payload_json FROM orchestration_v2_projection_runs WHERE run_id = ${input.runId} AND thread_id = ${input.threadId}`;
    const attemptRows = yield* sql<{
      payload_json: string;
    }>`SELECT payload_json FROM orchestration_v2_projection_run_attempts WHERE attempt_id = ${successor.id} AND thread_id = ${input.threadId}`;
    const nodeRows = yield* sql<{
      payload_json: string;
    }>`SELECT payload_json FROM orchestration_v2_projection_nodes WHERE node_id = ${successor.rootNodeId} AND thread_id = ${input.threadId}`;
    if (runRows[0] === undefined || attemptRows[0] === undefined || nodeRows[0] === undefined)
      return stale("successor");
    const currentRoot = yield* decodeNode(nodeRows[0].payload_json);
    const currentRun = yield* decodeRun(runRows[0].payload_json);
    const currentAttempt = yield* decodeAttempt(attemptRows[0].payload_json);
    if (
      currentRoot.runId !== input.runId ||
      currentRoot.rootNodeId !== successor.rootNodeId ||
      currentRoot.providerThreadId !== successor.providerThreadId ||
      currentRoot.providerTurnId !== null ||
      currentRoot.kind !== "root_turn" ||
      currentRoot.parentNodeId !== null ||
      (currentRoot.status !== "pending" && currentRoot.status !== "failed") ||
      currentRun.activeAttemptId !== successor.id ||
      currentRun.rootNodeId !== successor.rootNodeId ||
      currentRun.providerThreadId !== successor.providerThreadId ||
      currentRun.providerInstanceId !== successor.providerInstanceId ||
      (currentRun.status !== "starting" && currentRun.status !== "failed") ||
      currentAttempt.runId !== input.runId ||
      currentAttempt.rootNodeId !== successor.rootNodeId ||
      currentAttempt.attemptOrdinal !== successor.attemptOrdinal ||
      currentAttempt.reason !== "steering_restart" ||
      currentAttempt.providerInstanceId !== successor.providerInstanceId ||
      currentAttempt.providerThreadId !== successor.providerThreadId ||
      currentAttempt.providerTurnId !== null ||
      currentAttempt.providerSettlement !== null ||
      (currentAttempt.status !== "pending" && currentAttempt.status !== "failed")
    )
      return stale("successor");
    const oldThread = control.providerThread;
    if (
      oldThread === undefined ||
      oldThread.appThreadId !== binding.threadId ||
      oldThread.driver !== binding.driver ||
      oldThread.nativeThreadRef?.nativeId !== binding.nativeThreadId ||
      oldThread.nativeThreadRef.driver !== binding.driver ||
      oldThread.runtimeIdentity?.runtimeGeneration !== binding.runtimeGeneration ||
      (capture.evidenceRevision !== undefined &&
        (oldThread.runtimeIdentity?.evidenceRevision === undefined ||
          oldThread.runtimeIdentity.evidenceRevision < capture.evidenceRevision))
    )
      return stale("replacement");
    if (request.sessionTransition?.type === "replace") {
      const replacement = committed.find(
        (stored) =>
          stored.event.type === "provider-thread.updated" &&
          stored.event.payload.id === successor.providerThreadId,
      )?.event;
      if (
        replacement?.type !== "provider-thread.updated" ||
        successor.providerThreadId !== oldThread.id ||
        oldThread.providerSessionId !== request.sessionTransition.replacementProviderSessionId ||
        oldThread.providerInstanceId !== successor.providerInstanceId ||
        replacement.payload.providerSessionId !== oldThread.providerSessionId ||
        replacement.payload.providerInstanceId !== oldThread.providerInstanceId ||
        replacement.payload.runtimeIdentity?.runtimeGeneration !== binding.runtimeGeneration ||
        replacement.payload.nativeThreadRef?.nativeId !== binding.nativeThreadId
      )
        return stale("replacement");
    } else {
      if (
        oldThread.providerSessionId !== binding.providerSessionId ||
        oldThread.providerInstanceId !== binding.providerInstanceId
      )
        return stale("replacement");
      if (successor.providerThreadId !== oldThread.id) {
        const replacement = committed.find(
          (stored) =>
            stored.event.type === "provider-thread.updated" &&
            stored.event.payload.id === successor.providerThreadId,
        )?.event;
        const rows = yield* sql<{
          payload_json: string;
        }>`SELECT payload_json FROM orchestration_v2_projection_provider_threads WHERE provider_thread_id = ${successor.providerThreadId} AND thread_id = ${input.threadId}`;
        if (
          request.sessionTransition?.type !== "detach" ||
          replacement?.type !== "provider-thread.updated" ||
          rows[0] === undefined
        )
          return stale("replacement");
        const current = yield* decodeThread(rows[0].payload_json);
        if (
          current.providerSessionId !== replacement.payload.providerSessionId ||
          current.providerInstanceId !== successor.providerInstanceId ||
          current.runtimeIdentity?.runtimeGeneration !==
            replacement.payload.runtimeIdentity?.runtimeGeneration ||
          current.nativeThreadRef?.nativeId !== replacement.payload.nativeThreadRef?.nativeId
        )
          return stale("replacement");
      }
    }
    const finalOrigin = yield* origin.producer.revalidateCurrent.pipe(Effect.result);
    if (finalOrigin._tag === "Failure") return stale("origin");
    const selected = {
      ...terminal,
      payload: {
        ...control.providerTurn,
        status: turn.status,
        completedAt: turn.completedAt,
        ...(turn.turnTokenUsage === undefined ? {} : { turnTokenUsage: turn.turnTokenUsage }),
      },
    };
    retained.set(input, selected);
    return selected;
  });
  return { rememberCommitted, select, readCommitted };
});

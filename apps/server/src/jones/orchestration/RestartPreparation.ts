import {
  EventId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ProviderThread,
  type ProviderRuntimeBinding,
} from "@t3tools/contracts";
import { runtimeBinding } from "@t3tools/provider-core/server/ProviderAdapter";
import type {
  CapturedRuntimeStop,
  ProviderAdapterV2Error,
} from "@t3tools/provider-core/server/ProviderAdapter";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import type * as EffectOutbox from "../../orchestration-v2/EffectOutbox.ts";
import type { ProviderTurnControlError } from "../../orchestration-v2/ProviderTurnControlService.ts";
import type { ProviderTurnStartError } from "../../orchestration-v2/ProviderTurnStartService.ts";
import type { ProviderSessionManagerV2Error } from "../../orchestration-v2/ProviderSessionManager.ts";
import { findSteerFailure } from "./LateSteer.ts";

export const RESTART_TERMINAL_TIMEOUT_MS = 30_000;

export class RestartPreparationError extends Schema.TaggedError<RestartPreparationError>()(
  "RestartPreparationError",
  {
    threadId: ThreadId,
    stage: Schema.Literals(["interrupt", "terminal", "detach", "admission"]),
    cause: Schema.Defect(),
  },
) {
  override get message() {
    return `Provider restart preparation failed during ${this.stage}; the restart will not be replayed.`;
  }
}

export class RestartThreadHeld extends Schema.TaggedError<RestartThreadHeld>()(
  "RestartThreadHeld",
  { threadId: ThreadId },
) {
  override get message() {
    return "The previous provider runtime has an unresolved restart. Stop that exact runtime before starting more work.";
  }
}

export function findRestartPreparationFailure(
  value: unknown,
): RestartPreparationError | RestartThreadHeld | undefined {
  const pending = [value];
  const seen = new Set<unknown>();
  for (let remaining = 64; pending.length > 0 && remaining > 0; remaining -= 1) {
    const current = pending.pop();
    if (Schema.is(RestartPreparationError)(current) || Schema.is(RestartThreadHeld)(current))
      return current;
    if (typeof current !== "object" || current === null || seen.has(current)) continue;
    seen.add(current);
    if (Cause.isCause(current)) {
      for (const reason of current.reasons) {
        if (reason._tag === "Fail") pending.push(reason.error);
        if (reason._tag === "Die") pending.push(reason.defect);
      }
    }
    if ("cause" in current) pending.push(current.cause);
  }
}

type RestartEffect = EffectOutbox.OrchestrationEffectV2 & {
  readonly request: Extract<
    EffectOutbox.OrchestrationEffectV2["request"],
    { type: "provider-turn.restart" }
  >;
};
type OldTarget = Pick<RestartEffect, "threadId"> &
  Pick<RestartEffect["request"], "providerThreadId" | "providerTurnId" | "interruptedAttemptId">;
export type RestartPreparationWriteError =
  | ProjectionStore.ProjectionStoreV2Error
  | EventSink.EventSinkV2Error;

export const awaitRestartTerminal = (input: {
  readonly target: OldTarget;
  readonly projections: ProjectionStore.ProjectionStoreV2["Service"];
  readonly sink: EventSink.EventSinkV2["Service"];
}) =>
  Effect.gen(function* () {
    const { target, projections, sink } = input;
    const read = projections
      .getProviderControlContext(target.threadId, {
        providerThreadId: target.providerThreadId,
        providerTurnId: target.providerTurnId,
        attemptId: target.interruptedAttemptId,
      })
      .pipe(
        Effect.map(
          ({ providerTurn, attempt }) =>
            providerTurn?.id === target.providerTurnId &&
            providerTurn.runAttemptId === target.interruptedAttemptId &&
            providerTurn.status !== "running" &&
            attempt?.id === target.interruptedAttemptId &&
            attempt.status !== "running",
        ),
      );
    // The cursor precedes the first read; EventSink subscribes before replaying it.
    // The second read also covers a terminal committed while observation starts.
    const afterSequence = yield* sink.latestSequence({ threadId: target.threadId });
    if (yield* read) return;
    yield* Effect.scoped(
      Effect.gen(function* () {
        const observation = yield* sink
          .stream({ threadId: target.threadId, afterSequence, eventType: "provider-turn.updated" })
          .pipe(
            Stream.filter(
              (stored) =>
                stored.event.type === "provider-turn.updated" &&
                stored.event.payload.id === target.providerTurnId,
            ),
            Stream.mapEffect(() => read),
            Stream.filter(Boolean),
            Stream.runHead,
            Effect.flatMap((terminal) => (Option.isSome(terminal) ? Effect.void : Effect.never)),
            Effect.forkScoped,
          );
        if (yield* read) return;
        yield* Fiber.join(observation);
      }),
    );
  }).pipe(
    Effect.timeoutOrElse({
      duration: Duration.millis(RESTART_TERMINAL_TIMEOUT_MS),
      orElse: () =>
        Effect.fail(
          new RestartPreparationError({
            threadId: input.target.threadId,
            stage: "terminal",
            cause: "The captured old provider turn has no persisted terminal evidence.",
          }),
        ),
    }),
  );

export class RestartPreparation extends Context.Service<
  RestartPreparation,
  {
    readonly run: (input: {
      readonly effect: RestartEffect;
      readonly interrupt: Effect.Effect<void, ProviderTurnControlError | RestartPreparationError>;
      readonly detach: Effect.Effect<void, ProviderSessionManagerV2Error | RestartPreparationError>;
      readonly start: Effect.Effect<void, ProviderTurnStartError>;
      readonly captureStop?: (
        providerThread: OrchestrationV2ProviderThread,
      ) => Effect.Effect<
        CapturedRuntimeStop | null,
        ProviderAdapterV2Error | ProviderSessionManagerV2Error
      >;
    }) => Effect.Effect<
      void,
      | RestartPreparationError
      | RestartThreadHeld
      | ProviderTurnStartError
      | ProjectionStore.ProjectionStoreV2Error
      | EventStore.EventStoreV2Error
    >;
    readonly assertThreadOpen: (
      threadId: ThreadId,
    ) => Effect.Effect<void, RestartThreadHeld | ProjectionStore.ProjectionStoreV2Error>;
    readonly captureHeldRuntimeStop: (
      threadId: ThreadId,
    ) => Effect.Effect<CapturedRuntimeStop | null>;
    readonly resolveAfterTeardown: (binding: ProviderRuntimeBinding) => Effect.Effect<void>;
    readonly recordSteerFailure: (
      effect: EffectOutbox.OrchestrationEffectV2,
      cause: unknown,
    ) => Effect.Effect<void, RestartPreparationWriteError>;
  }
>()("t3/jones/orchestration/RestartPreparation") {}

const make = Effect.gen(function* () {
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const sink = yield* EventSink.EventSinkV2;
  const events = yield* EventStore.EventStoreV2;
  const holds = new Map<
    string,
    {
      target: OldTarget;
      binding: ProviderRuntimeBinding | undefined;
      capture: CapturedRuntimeStop | null;
      stage: "interrupt" | "detach";
    }
  >();
  const assertThreadOpen = (threadId: ThreadId) =>
    Effect.gen(function* () {
      for (const [id, hold] of holds) {
        if (hold.target.threadId !== threadId) continue;
        if (hold.stage === "interrupt") {
          const current = yield* projections.getProviderControlContext(threadId, {
            providerThreadId: hold.target.providerThreadId,
            providerTurnId: hold.target.providerTurnId,
            attemptId: hold.target.interruptedAttemptId,
          });
          if (
            current.providerTurn?.id === hold.target.providerTurnId &&
            current.providerTurn.runAttemptId === hold.target.interruptedAttemptId &&
            current.providerTurn.status !== "running"
          ) {
            holds.delete(id);
            continue;
          }
        }
        return yield* new RestartThreadHeld({ threadId });
      }
    });
  return RestartPreparation.of({
    assertThreadOpen,
    captureHeldRuntimeStop: (threadId) =>
      Effect.gen(function* () {
        const targets = Array.from(holds.values()).filter(
          (hold) => hold.target.threadId === threadId,
        );
        const capture = targets.length === 1 ? targets[0]!.capture : null;
        return capture !== null && (yield* capture.isCurrent) ? capture : null;
      }),
    resolveAfterTeardown: (binding) =>
      Effect.sync(() => {
        for (const [id, hold] of holds) {
          if (
            hold.binding !== undefined &&
            Object.entries(hold.binding).every(
              ([key, value]) => binding[key as keyof ProviderRuntimeBinding] === value,
            )
          )
            holds.delete(id);
        }
      }),
    run: ({ effect, interrupt, detach, start, captureStop }) =>
      Effect.gen(function* () {
        const { request, threadId } = effect;
        const committed = yield* events
          .readByCommandId({ commandId: effect.commandId })
          .pipe(Stream.runCollect);
        const runEvent = committed.find(
          ({ event }) => event.type === "run.updated" && event.payload.id === request.runId,
        )?.event;
        const attemptEvent = committed.find(
          ({ event }) =>
            event.type === "run-attempt.created" && event.payload.runId === request.runId,
        )?.event;
        const nodeEvent = committed.find(
          ({ event }) =>
            runEvent?.type === "run.updated" &&
            event.type === "node.updated" &&
            event.payload.runId === request.runId &&
            event.payload.id === runEvent.payload.rootNodeId &&
            event.payload.parentNodeId === null,
        )?.event;
        if (
          runEvent?.type !== "run.updated" ||
          attemptEvent?.type !== "run-attempt.created" ||
          nodeEvent?.type !== "node.updated" ||
          runEvent.threadId !== threadId ||
          runEvent.payload.activeAttemptId !== attemptEvent.payload.id ||
          attemptEvent.payload.id === request.interruptedAttemptId ||
          runEvent.payload.rootNodeId !== nodeEvent.payload.id ||
          attemptEvent.payload.rootNodeId !== nodeEvent.payload.id
        ) {
          return yield* new RestartPreparationError({
            threadId,
            stage: "admission",
            cause: "The recorded restart successor is unavailable.",
          });
        }
        const run = runEvent.payload;
        const attempt = attemptEvent.payload;
        const node = nodeEvent.payload;
        const current = yield* projections.getThreadRecords(threadId, ["runs"], {
          runIds: [run.id],
        });
        if (
          !current.runs.some(
            (row) =>
              row.id === run.id && row.status === "starting" && row.activeAttemptId === attempt.id,
          )
        )
          return;
        const old = yield* projections.getProviderControlContext(threadId, {
          providerThreadId: request.providerThreadId,
          providerTurnId: request.providerTurnId,
          attemptId: request.interruptedAttemptId,
        });
        const generation = old.providerThread?.runtimeIdentity?.runtimeGeneration;
        const oldThread =
          old.providerThread !== undefined && old.attempt !== undefined
            ? {
                ...old.providerThread,
                providerSessionId: request.providerSessionId,
                providerInstanceId: old.attempt.providerInstanceId,
              }
            : undefined;
        const binding =
          oldThread !== undefined && generation !== undefined
            ? runtimeBinding(oldThread, generation)
            : undefined;
        const candidate =
          captureStop !== undefined && oldThread !== undefined
            ? yield* captureStop(oldThread).pipe(
                Effect.catchCause((cause) =>
                  Cause.hasInterruptsOnly(cause)
                    ? Effect.failCause(
                        Cause.fromReasons<never>(cause.reasons.filter(Cause.isInterruptReason)),
                      )
                    : Effect.succeed(null),
                ),
              )
            : null;
        const capture =
          candidate !== null &&
          binding !== undefined &&
          Object.entries(binding).every(
            ([key, value]) => candidate.binding[key as keyof ProviderRuntimeBinding] === value,
          )
            ? candidate
            : null;
        let stage: "interrupt" | "detach" | "admission" = "admission";
        const preparation = Effect.gen(function* () {
          yield* assertThreadOpen(threadId);
          stage = "interrupt";
          yield* interrupt;
          stage = "detach";
          yield* detach;
        });
        const result = yield* Effect.uninterruptibleMask((restore) =>
          Effect.gen(function* () {
            const result = yield* Effect.exit(restore(preparation));
            if (Exit.isSuccess(result)) return result;
            // Hold before publishing failure: queued work may react to that commit.
            if (stage !== "admission")
              holds.set(effect.id, { target: { threadId, ...request }, binding, capture, stage });
            const now = yield* DateTime.now;
            const eventBase = {
              threadId,
              runId: run.id,
              nodeId: node.id,
              providerInstanceId: run.providerInstanceId,
              occurredAt: now,
            };
            const failure = {
              class: "provider_error" as const,
              message: `Restart preparation failed during ${stage}. The replacement was not started. Stop the previous runtime before trying again.`,
              code: null,
              retryable: false,
            };
            const failureEvents: ReadonlyArray<OrchestrationV2DomainEvent> = [
              {
                ...eventBase,
                id: EventId.make(`${effect.id}:preparation:item`),
                type: "turn-item.updated",
                payload: {
                  id: TurnItemId.make(`${effect.id}:preparation`),
                  threadId,
                  runId: run.id,
                  nodeId: node.id,
                  providerThreadId: attempt.providerThreadId,
                  providerTurnId: null,
                  nativeItemRef: null,
                  parentItemId: null,
                  ordinal: 0,
                  type: "error",
                  title: "Provider restart could not finish",
                  failure,
                  status: "failed",
                  startedAt: now,
                  completedAt: now,
                  updatedAt: now,
                },
              },
              {
                ...eventBase,
                id: EventId.make(`${effect.id}:preparation:run`),
                type: "run.updated",
                payload: { ...run, status: "failed", completedAt: now },
              },
              {
                ...eventBase,
                id: EventId.make(`${effect.id}:preparation:attempt`),
                type: "run-attempt.updated",
                payload: { ...attempt, status: "failed", completedAt: now },
              },
              {
                ...eventBase,
                id: EventId.make(`${effect.id}:preparation:node`),
                type: "node.updated",
                payload: { ...node, status: "failed", completedAt: now },
              },
            ];
            yield* sink
              .writeIfRunCurrent({
                threadId,
                runId: run.id,
                activeAttemptId: attempt.id,
                expectedStatus: "starting",
                events: failureEvents,
              })
              .pipe(
                Effect.catchCause((cause) =>
                  Effect.fail(new RestartPreparationError({ threadId, stage, cause })),
                ),
              );
            return result;
          }),
        );
        if (Exit.isFailure(result))
          return yield* new RestartPreparationError({ threadId, stage, cause: result.cause });
        yield* start;
      }),
    recordSteerFailure: (effect, cause) =>
      Effect.gen(function* () {
        if (
          effect.request.type !== "provider-turn.steer" &&
          effect.request.type !== "provider-turn.steer-batch"
        )
          return;
        const failure = findSteerFailure(cause);
        if (failure === undefined) return;
        const context = yield* projections.getProviderControlContext(
          effect.threadId,
          effect.request,
        );
        if (context.run === undefined || context.providerTurn === undefined) return;
        const now = yield* DateTime.now;
        const id = TurnItemId.make(`${effect.id}:control-failure`);
        const existing = yield* projections.getTurnItem({ threadId: effect.threadId, itemId: id });
        if (existing !== null) return;
        const unknown = failure._tag === "SteerOutcomeUnknown";
        yield* sink.write({
          events: [
            {
              id: EventId.make(`${effect.id}:control-failure`),
              type: "turn-item.updated",
              threadId: effect.threadId,
              runId: context.run.id,
              nodeId: context.providerTurn.nodeId,
              occurredAt: now,
              payload: {
                id,
                threadId: effect.threadId,
                runId: context.run.id,
                nodeId: context.providerTurn.nodeId,
                providerThreadId: effect.request.providerThreadId,
                providerTurnId: effect.request.providerTurnId,
                nativeItemRef: null,
                parentItemId: null,
                ordinal: 0,
                type: "error",
                title: "Steering message not confirmed",
                failure: {
                  class: "provider_error",
                  message: unknown
                    ? "The provider did not acknowledge this steering message in time. Delivery is unknown; it will not be sent again. New work on this provider account is held until the request is reconciled or its runtime is stopped."
                    : "This steering message was not sent because provider control admission was unavailable. It will not be retried automatically.",
                  code: null,
                  retryable: false,
                },
                status: "failed",
                startedAt: now,
                completedAt: now,
                updatedAt: now,
              },
            },
          ],
        });
      }),
  });
});

export const layer = Layer.effect(RestartPreparation, make);

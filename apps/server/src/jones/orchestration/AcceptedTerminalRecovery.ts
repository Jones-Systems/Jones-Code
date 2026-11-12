import {
  CommandId,
  OrchestrationV2DomainEvent,
  TurnItemId,
  type NodeId,
  type OrchestrationV2TurnItem,
  type ThreadId,
  type RunAttemptId,
  type RunId,
} from "@t3tools/contracts";
import { makeProviderFailureTurnItem } from "@t3tools/provider-core/server/failure";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { findSqliteReaderAdmissionFailure } from "@t3tools/shared/jones/sqliteReaders/admissionFailure";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlError from "effect/sql/SqlError";
import * as EventSink from "../../orchestration-v2/EventSink.ts";

type Terminal = Extract<ProviderAdapter.ProviderAdapterV2Event, { readonly type: "turn.terminal" }>;
type CurrentWrite = Parameters<EventSink.EventSinkV2["Service"]["writeIfRunCurrent"]>[0];
type UnconditionalWrite = Parameters<EventSink.EventSinkV2["Service"]["writeWithEffects"]>[0];
type Publication =
  | { readonly type: "current"; readonly input: CurrentWrite }
  | { readonly type: "unconditional"; readonly input: UnconditionalWrite };
type Result = Effect.Success<ReturnType<EventSink.EventSinkV2["Service"]["writeIfRunCurrent"]>>;
const isLockTimeout = Schema.is(SqlError.LockTimeoutError);
const encodeTerminalEvent = Schema.encodeEffect(Schema.fromJsonString(OrchestrationV2DomainEvent));

export const isAdmissionUnavailable = (cause: unknown): boolean =>
  findSqliteReaderAdmissionFailure(cause) !== undefined;

export function isTransientLocalStorageFailure(value: unknown): boolean {
  if (isAdmissionUnavailable(value)) return true;
  const pending = [{ value, depth: 0 }];
  const seen = new Set<unknown>();
  for (let index = 0; index < pending.length && index < 128; index++) {
    const entry = pending[index]!;
    if (
      entry.depth > 16 ||
      typeof entry.value !== "object" ||
      entry.value === null ||
      seen.has(entry.value)
    )
      continue;
    seen.add(entry.value);
    const next = (value: unknown) => {
      if (pending.length < 128) pending.push({ value, depth: entry.depth + 1 });
    };
    try {
      const current = entry.value as Record<string, unknown>;
      if (Cause.isCause(current)) {
        for (const reason of current.reasons) next(reason);
      } else if (Cause.isReason(current)) {
        if (Cause.isFailReason(current)) next(current.error);
        else if (Cause.isDieReason(current)) next(current.defect);
      } else {
        // Native extended SQLite codes retain their primary code in the low byte.
        if (isLockTimeout(current)) return true;
        const code = current.errcode;
        if (
          typeof code === "number" &&
          Number.isInteger(code) &&
          code >= 0 &&
          [5, 6].includes(code & 0xff)
        )
          return true;
        next(current.cause);
        next(current.reason);
      }
    } catch {
      // Opaque wrappers and message text are never retry authority.
    }
  }
  return false;
}

// Per-event read bodies only: a caller must not wrap a bundle containing writes.
export const retryAdmissionUnavailable = <A, E, R>(
  read: Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R> => read.pipe(Effect.retry({ while: isAdmissionUnavailable, times: 2 }));

export const recoverAcceptedTerminal = Effect.fn("AcceptedTerminalRecovery.recover")(function* <
  A,
  E,
  R,
>(
  work: Effect.Effect<A, E, R>,
  identity: { readonly runId: RunId; readonly attemptId: RunAttemptId },
  initialCause: Cause.Cause<E>,
): Effect.fn.Return<A, E, R> {
  if (!isTransientLocalStorageFailure(initialCause)) return yield* Effect.failCause(initialCause);
  let nextWarningAt = yield* DateTime.now;
  return yield* work.pipe(
    Effect.catchCause((cause) =>
      Effect.gen(function* () {
        const now = yield* DateTime.now;
        if (
          isTransientLocalStorageFailure(cause) &&
          DateTime.isGreaterThanOrEqualTo(now, nextWarningAt)
        ) {
          yield* Effect.logWarning(
            "orchestration V2 accepted terminal finalization retry",
            identity,
          );
          nextWarningAt = DateTime.add(now, { minutes: 10 });
        }
        return yield* Effect.fail(cause);
      }),
    ),
    Effect.retry({
      while: isTransientLocalStorageFailure,
      schedule: Schedule.exponential("1 second").pipe(
        Schedule.modifyDelay(({ duration }) =>
          Effect.succeed(Duration.min(duration, Duration.seconds(30))),
        ),
      ),
    }),
    Effect.catch((cause) => Effect.failCause(cause)),
  );
});

export function finalizationFailureItems(input: {
  readonly terminal: Terminal;
  readonly acceptedNativeTerminal?: Terminal;
  readonly acceptedNativeCompletedAt?: DateTime.Utc;
  readonly failureItemPersisted: boolean;
  readonly threadId: ThreadId;
  readonly runId: RunId;
  readonly nodeId: NodeId;
  readonly attemptId: RunAttemptId;
  readonly completedAt: DateTime.Utc;
}): ReadonlyArray<OrchestrationV2TurnItem> {
  const items: Array<OrchestrationV2TurnItem> = [];
  const makeItem = (
    terminal: Extract<Terminal, { readonly status: "failed" }>,
    occurredAt: DateTime.Utc,
  ) =>
    makeProviderFailureTurnItem({
      driver: terminal.driver,
      threadId: input.threadId,
      runId: input.runId,
      nodeId: input.nodeId,
      providerThreadId: terminal.providerThreadId,
      providerTurnId: terminal.providerTurnId,
      itemOrdinal: terminal.failureItemOrdinal,
      failure: terminal.failure,
      ...(terminal.retry === undefined ? {} : { retry: terminal.retry }),
      ...(terminal.retryStartedAt === undefined ? {} : { retryStartedAt: terminal.retryStartedAt }),
      occurredAt,
    });
  const native = input.acceptedNativeTerminal;
  if (native?.status === "failed") {
    items.push(
      makeItem(
        native,
        native.providerTurn?.completedAt ?? input.acceptedNativeCompletedAt ?? input.completedAt,
      ),
    );
  }
  if (input.terminal.status === "failed" && !input.failureItemPersisted) {
    const item = makeItem(input.terminal, input.completedAt);
    items.push(
      native === undefined
        ? item
        : {
            ...item,
            id: TurnItemId.make(`turn-item:terminal-finalization:${input.attemptId}`),
          },
    );
  }
  return items;
}

// Retain successful normalization before publication: after an uncertain commit,
// a fresh pending-input read could be empty and otherwise hide the original batch.
export function makeTerminalNormalizer() {
  const retained = new WeakMap<Terminal, ReadonlyArray<OrchestrationV2DomainEvent>>();
  return Effect.fnUntraced(function* <E, R>(
    terminal: Terminal,
    normalize: Effect.Effect<ReadonlyArray<OrchestrationV2DomainEvent>, E, R>,
  ): Effect.fn.Return<ReadonlyArray<OrchestrationV2DomainEvent>, E, R> {
    const current = retained.get(terminal);
    if (current !== undefined) return current;
    const events = yield* normalize;
    retained.set(terminal, events);
    return events;
  });
}

// One instance per consumer/ingestor. Weak keys retain the original control-ack object
// only as long as its owner retains it; successful publication never runs twice.
export const makeTerminalPublisher = Effect.gen(function* () {
  const sink = yield* EventSink.EventSinkV2;
  const retained = new WeakMap<
    Terminal,
    Map<
      string,
      {
        readonly publication: Publication;
        readonly commandId: CommandId;
        uncertain: boolean;
        result?: Result;
      }
    >
  >();
  return Effect.fnUntraced(function* (
    terminal: Terminal,
    stage: string,
    publication: Publication,
  ): Effect.fn.Return<Result, EventSink.EventSinkV2Error> {
    let stages = retained.get(terminal);
    if (stages === undefined) {
      stages = new Map();
      retained.set(terminal, stages);
    }
    let entry = stages.get(stage);
    if (entry === undefined) {
      const first = publication.input.events[0];
      if (first === undefined) return { committed: true, storedEvents: [] } satisfies Result;
      entry = {
        publication,
        commandId: CommandId.make(`command:accepted-terminal:${first.id}`),
        uncertain: false,
      };
      stages.set(stage, entry);
    }
    if (entry.result !== undefined) return entry.result;
    if (entry.uncertain) {
      const stored = yield* sink
        .readByCommandId({ commandId: entry.commandId })
        .pipe(Stream.runCollect);
      if (stored.length > 0) {
        // Read back the original transaction, not merely a newer terminal-looking row.
        // A filtered cancellation may be absent, but every stored event must be exact.
        const ids = new Set(stored.map((record) => record.event.id));
        for (const event of entry.publication.input.events) {
          const mayFilter =
            entry.publication.input.guardPendingUserInputCancellations === true &&
            (event.type === "runtime-request.updated" ||
              event.type === "node.updated" ||
              event.type === "turn-item.updated") &&
            event.payload.status === "cancelled";
          if (!mayFilter && !ids.has(event.id))
            return yield* new EventSink.EventSinkWriteError({ eventCount: stored.length });
        }
        for (const record of stored) {
          const expected = entry.publication.input.events.find(
            (event) => event.id === record.event.id,
          );
          if (expected === undefined || record.commandId !== entry.commandId)
            return yield* new EventSink.EventSinkWriteError({ eventCount: stored.length });
          let normalizedExpected = expected;
          if (expected.type === "turn-item.updated" && record.event.type === "turn-item.updated") {
            normalizedExpected = {
              ...expected,
              payload: { ...expected.payload, ordinal: record.event.payload.ordinal },
            };
          }
          if (
            expected.type === "provider-thread.updated" &&
            record.event.type === "provider-thread.updated" &&
            expected.payload.runtimeIdentity === undefined
          ) {
            normalizedExpected = {
              ...expected,
              payload: {
                ...expected.payload,
                runtimeIdentity: record.event.payload.runtimeIdentity,
              },
            };
          }
          const actualJson = yield* encodeTerminalEvent(record.event).pipe(
            Effect.mapError(
              (cause) => new EventSink.EventSinkWriteError({ eventCount: stored.length, cause }),
            ),
          );
          const expectedJson = yield* encodeTerminalEvent(normalizedExpected).pipe(
            Effect.mapError(
              (cause) => new EventSink.EventSinkWriteError({ eventCount: stored.length, cause }),
            ),
          );
          if (actualJson !== expectedJson)
            return yield* new EventSink.EventSinkWriteError({ eventCount: stored.length });
        }
        entry.result = { committed: true, storedEvents: stored };
        return entry.result;
      }
      // The serialized writer transaction is absent. Only now may it be issued again.
      entry.uncertain = false;
    }
    const request = entry.publication;
    const write =
      request.type === "current"
        ? sink.writeIfRunCurrent({ ...request.input, commandId: entry.commandId })
        : sink
            .writeWithEffects({ ...request.input, commandId: entry.commandId })
            .pipe(Effect.map((storedEvents) => ({ committed: true, storedEvents })));
    entry.uncertain = true;
    const result = yield* write;
    entry.uncertain = false;
    entry.result = result;
    return result;
  });
});

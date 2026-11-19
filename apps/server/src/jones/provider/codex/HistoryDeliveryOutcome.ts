import type { OrchestrationV2ContextHandoff, OrchestrationV2StoredEvent } from "@t3tools/contracts";
import * as CodexErrors from "effect-codex-app-server/errors";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlError from "effect/sql/SqlError";
import type { EventSinkV2Shape } from "../../../orchestration-v2/EventSink.ts";
import { readEventLogPrunedThrough } from "../../persistence/EventLogPruneBoundary.ts";
import type { CodexCapacityContinuationBinding } from "./CodexCapacityContinuation.ts";

/** Evidence from the current call only; never evidence about a persisted pending marker. */
export class HistoryNotApplied extends Schema.TaggedError<HistoryNotApplied>()(
  "HistoryNotApplied",
  { cause: Schema.Defect() },
) {
  override get message(): string {
    return "Historical context was not applied by this attempt.";
  }
}

export class PromptNotApplied extends Schema.TaggedError<PromptNotApplied>()("PromptNotApplied", {
  cause: Schema.Defect(),
}) {
  override get message(): string {
    return "The provider did not consume this attempt's prompt.";
  }
}

const isCodexRequestError = Schema.is(CodexErrors.CodexAppServerRequestError);
const isHistoryNotApplied = Schema.is(HistoryNotApplied);
const isPromptNotApplied = Schema.is(PromptNotApplied);

export const historyInjectionRejection = (error: unknown) => {
  if (!isCodexRequestError(error) || error.method !== "thread/inject_items") return "ambiguous";
  if (error.operation === "encode-payload") return "not_applied";
  if (error.operation !== "receive-response") return "ambiguous";
  if (error.code === -32601) return "unsupported";
  return error.code === -32600 || error.code === -32602 ? "not_applied" : "ambiguous";
};

export const historyWasNotApplied = (error: unknown): boolean => {
  const seen = new Set<unknown>();
  let current = error;
  while (typeof current === "object" && current !== null && !seen.has(current)) {
    seen.add(current);
    if (isHistoryNotApplied(current)) return true;
    current = "cause" in current ? current.cause : undefined;
  }
  return false;
};

/** Only current-attempt proof authorizes undoing an unsent inline marker. */
export const promptWasNotApplied = (error: unknown): boolean => {
  const seen = new Set<unknown>();
  let current = error;
  while (typeof current === "object" && current !== null && !seen.has(current)) {
    seen.add(current);
    if (isPromptNotApplied(current) || Predicate.isTagged(current, "CodexStartupNotDispatched"))
      return true;
    if (
      isCodexRequestError(current) &&
      current.method === "turn/start" &&
      (current.operation === "encode-payload" ||
        (current.operation === "receive-response" &&
          (current.code === -32600 || current.code === -32601 || current.code === -32602)))
    )
      return true;
    current = "cause" in current ? current.cause : undefined;
  }
  return false;
};

/** A capability for the original prompt's inline ledger, captured by its producer waiter. */
export const CodexInlineHandoffRejection = Context.Reference<
  | ((
      binding: CodexCapacityContinuationBinding,
      proofIsCurrent: () => boolean,
    ) => Effect.Effect<"restored" | "withheld" | "failed">)
  | undefined
>("jones/CodexInlineHandoffRejection", { defaultValue: () => undefined });

class InlineRestorationWithheld extends Schema.TaggedError<
  InlineRestorationWithheld,
  { readonly brand: unique symbol }
>()("InlineRestorationWithheld", {}) {}

class InlineRestorationSqlFailure extends Schema.TaggedError<
  InlineRestorationSqlFailure,
  { readonly brand: unique symbol }
>()("InlineRestorationSqlFailure", { cause: Schema.Defect() }) {}

const isInlineRestorationWithheld = Schema.is(InlineRestorationWithheld);
const isInlineRestorationSqlFailure = Schema.is(InlineRestorationSqlFailure);

const hasSqlCause = (error: unknown): boolean => {
  // EventSink and EventStore wrap statement errors with their operation.
  for (let depth = 0; depth < 8; depth++) {
    if (SqlError.isSqlError(error)) return true;
    if (!Predicate.hasProperty(error, "cause")) return false;
    error = error.cause;
  }
  return false;
};

/** Run inside the same EventSink writer batch as the restoration. */
const ownsPendingHistoryDelivery = Effect.fn("HistoryDeliveryOutcome.ownsPendingDelivery")(
  function* (marker: OrchestrationV2StoredEvent) {
    const { event, sequence } = marker;
    if (event.type !== "context-handoff.updated" || event.payload.delivery?.status !== "pending")
      return false;
    // EventSink supplies the writer client and transaction to the batch.
    const database = yield* Effect.serviceOption(SqlClient.SqlClient);
    if (Option.isNone(database)) return false;
    const sql = database.value;
    if (Option.isNone(yield* Effect.serviceOption(sql.transactionService))) return false;
    if (sequence <= (yield* readEventLogPrunedThrough(sql))) return false;
    // The event sequence distinguishes otherwise identical writes at the same time.
    const rows = yield* sql<{ readonly sequence: number }>`
      SELECT marker.sequence
      FROM orchestration_events AS marker
      JOIN orchestration_v2_projection_context_handoffs AS handoff
        ON handoff.context_handoff_id = ${event.payload.id} AND handoff.thread_id = ${event.threadId}
      WHERE marker.sequence = ${sequence} AND marker.event_id = ${event.id}
        AND handoff.payload_json = marker.payload_json
        AND NOT EXISTS (
          SELECT 1 FROM orchestration_events AS newer
          WHERE newer.aggregate_kind = 'thread' AND newer.stream_id = ${event.threadId}
            AND newer.application_event_version = 2 AND newer.sequence > ${sequence}
            AND newer.event_type = 'context-handoff.updated'
            AND json_extract(newer.payload_json, '$.id') = ${event.payload.id}
        )
    `;
    return rows.length === 1;
  },
);

/** Compare and restore only pending writes committed by this delivery invocation. */
export function makeCodexHistoryDeliveryPersistence<WriteError>(
  write: (
    handoff: OrchestrationV2ContextHandoff,
  ) => Effect.Effect<ReadonlyArray<OrchestrationV2StoredEvent>, WriteError>,
  withProviderBatch: EventSinkV2Shape["withProviderBatch"],
) {
  const pending = new Map<
    OrchestrationV2ContextHandoff["id"],
    OrchestrationV2StoredEvent | undefined
  >();
  const restore = (handoff: OrchestrationV2ContextHandoff) =>
    Effect.suspend(() => {
      const marker = pending.get(handoff.id);
      if (marker === undefined) return Effect.void;
      return withProviderBatch(
        Effect.gen(function* () {
          if (yield* ownsPendingHistoryDelivery(marker)) yield* write(handoff);
        }),
      );
    });
  const restoreRejectedInline = Effect.fn("HistoryDeliveryOutcome.restoreRejectedInline")(
    function* (
      handoffs: ReadonlyArray<OrchestrationV2ContextHandoff>,
      proofIsCurrent: () => boolean,
    ) {
      const originals = new Map(handoffs.map((handoff) => [handoff.id, handoff]));
      const withheld = new InlineRestorationWithheld();
      const requireProof = Effect.suspend(() => (proofIsCurrent() ? Effect.void : withheld));
      let committed = false;
      const attempt = withProviderBatch(
        Effect.gen(function* () {
          yield* requireProof;
          if (pending.size === 0 || originals.size !== handoffs.length) return yield* withheld;
          for (const [id, marker] of pending) {
            const original = originals.get(id);
            if (
              marker === undefined ||
              original === undefined ||
              !(yield* ownsPendingHistoryDelivery(marker))
            )
              return yield* withheld;
            yield* requireProof;
            if ((yield* write(original)).length !== 1) return yield* withheld;
          }
          const database = yield* Effect.serviceOption(SqlClient.SqlClient);
          if (Option.isNone(database)) return yield* withheld;
          yield* database.value.afterCommit(
            Effect.sync(() => {
              committed = true;
            }),
          );
          // This can still roll back the batch, including earlier savepoints.
          // EventSink can await its publication lane and SQLite COMMIT after this
          // check. Revocation then still holds live admission, yet the restored
          // row can survive a restart that loses the in-memory hold.
          yield* requireProof;
        }).pipe(
          Effect.mapError((cause) =>
            hasSqlCause(cause) ? new InlineRestorationSqlFailure({ cause }) : cause,
          ),
        ),
      );
      const restoreAttempt = Effect.suspend(() => (proofIsCurrent() ? attempt : withheld));
      const result = yield* Effect.exit(
        restoreAttempt.pipe(
          Effect.catchCause((cause) => {
            const [failure] = cause.reasons;
            // Retry one SQL-only batch failure after confirmed rollback. A
            // COMMIT/rollback error or mixed cause is outside that evidence.
            return cause.reasons.length === 1 &&
              failure?._tag === "Fail" &&
              isInlineRestorationSqlFailure(failure.error)
              ? restoreAttempt
              : Effect.failCause(cause);
          }),
        ),
      );
      if (Exit.isSuccess(result)) {
        if (!committed || !proofIsCurrent()) return "withheld" as const;
        pending.clear();
        return "restored" as const;
      }
      const [failure] = result.cause.reasons;
      if (
        result.cause.reasons.length === 1 &&
        failure?._tag === "Fail" &&
        isInlineRestorationWithheld(failure.error)
      )
        return "withheld" as const;
      if (Cause.hasInterruptsOnly(result.cause)) return yield* Effect.interrupt;
      yield* Effect.logWarning(
        "Failed to restore rejected Codex inline history; admission is held",
      );
      return "failed" as const;
    },
  );
  return {
    persist: (handoff: OrchestrationV2ContextHandoff): ReturnType<typeof restore> =>
      write(handoff).pipe(
        Effect.map(([stored]) => {
          if (handoff.delivery?.status === "pending") pending.set(handoff.id, stored);
        }),
      ),
    restore,
    restoreRejectedInline,
  };
}

/** One delivery invocation owns only the records it attempted to mark pending. */
export const makeHistoryDeliveryOutcome = <T, E>(
  restoreRecord: (record: T) => Effect.Effect<void, E>,
) => {
  const attempted = new Set<T>();
  let mayHaveApplied = false;
  let restorationStarted = false;
  const unsent = Effect.suspend(() => {
    if (attempted.size === 0 || mayHaveApplied || restorationStarted) return Effect.void;
    restorationStarted = true;
    return Effect.forEach(attempted, restoreRecord, { discard: true });
  });
  return {
    pending: <A, E2>(record: T, effect: Effect.Effect<A, E2>) =>
      Effect.sync(() => {
        attempted.add(record);
      }).pipe(Effect.andThen(effect)),
    inject: <E2>(effect: Effect.Effect<boolean, E2>) =>
      Effect.sync(() => {
        mayHaveApplied = true;
      }).pipe(
        Effect.andThen(effect),
        Effect.tap((accepted) =>
          Effect.sync(() => {
            mayHaveApplied = accepted;
          }),
        ),
        Effect.tapError((error) =>
          Effect.sync(() => {
            if (historyWasNotApplied(error)) mayHaveApplied = false;
          }),
        ),
      ),
    unsent,
    protect: <A, E2>(effect: Effect.Effect<A, E2>) =>
      effect.pipe(
        Effect.onExit((exit) =>
          Exit.isFailure(exit)
            ? unsent.pipe(
                Effect.catchCause(() =>
                  Effect.logWarning(
                    "Failed to restore unsent context handoffs; delivery remains unresolved",
                  ),
                ),
              )
            : Effect.void,
        ),
      ),
  };
};

import type {
  MessageId,
  OrchestrationV2ContextHandoff,
  OrchestrationV2ProviderTurn,
  OrchestrationV2StoredEvent,
  ProviderRuntimeBinding,
  RunAttemptId,
  RunId,
} from "@t3tools/contracts";
import type { IdAllocatorV2 } from "@t3tools/provider-core/server/IdAllocator";
import type { ProviderAdapterV2TurnInput } from "@t3tools/provider-core/server/ProviderAdapter";
import * as CodexErrors from "effect-codex-app-server/errors";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlError from "effect/sql/SqlError";
import type { EventSinkV2Shape } from "../../../orchestration-v2/EventSink.ts";
import { readEventLogPrunedThrough } from "../../persistence/EventLogPruneBoundary.ts";
import type { CodexStartTerminal } from "./CodexLateStartRecovery.ts";

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

/** Wire rejection can release live admission but cannot undo a durable pending marker. */
export const promptWasNotApplied = (
  error: unknown,
  { beforeDispatchOnly = false } = {},
): boolean => {
  const seen = new Set<unknown>();
  let current = error;
  while (typeof current === "object" && current !== null && !seen.has(current)) {
    seen.add(current);
    if (
      isPromptNotApplied(current) ||
      (current instanceof Error && Predicate.isTagged(current, "CodexStartupNotDispatched"))
    )
      return true;
    // The cause of a provider response can contain arbitrary native JSON.
    if (current instanceof Error && isCodexRequestError(current))
      return (
        current.method === "turn/start" &&
        (current.operation === "encode-payload" ||
          (!beforeDispatchOnly &&
            current.operation === "receive-response" &&
            (current.code === -32600 || current.code === -32601 || current.code === -32602)))
      );
    current = "cause" in current ? current.cause : undefined;
  }
  return false;
};

interface LateInlineReceipt {
  readonly binding: ProviderRuntimeBinding;
  readonly runId: RunId;
  readonly runOrdinal: number;
  readonly messageId: MessageId;
  readonly turn: OrchestrationV2ProviderTurn & { readonly runAttemptId: RunAttemptId };
  readonly isCurrent: () => boolean;
}

interface InlineHistoryStart {
  readonly acknowledged: boolean;
  readonly closeUnsent: () => boolean;
  readonly canDispatch: () => boolean;
  readonly tryDispatch: () => boolean;
  readonly markAcknowledged: () => void;
  readonly reconcileLate: (
    terminal: Deferred.Deferred<CodexStartTerminal>,
    binding: ProviderRuntimeBinding,
    input: ProviderAdapterV2TurnInput,
    isCurrent: (terminal: CodexStartTerminal) => boolean,
    release: (terminal: CodexStartTerminal) => void,
  ) => Effect.Effect<void>;
}

/** The original prompted start owns the inline handoffs it marked pending. */
export const CodexInlineHistoryStart = Context.Reference<InlineHistoryStart | undefined>(
  "jones/CodexInlineHistoryStart",
  { defaultValue: () => undefined },
);

class InlineRestorationSqlFailure extends Schema.TaggedError<
  InlineRestorationSqlFailure,
  { readonly brand: unique symbol }
>()("InlineRestorationSqlFailure", { cause: Schema.Defect() }) {}

const isInlineRestorationSqlFailure = Schema.is(InlineRestorationSqlFailure);

class LateInlineReceiptChanged extends Schema.TaggedError<
  LateInlineReceiptChanged,
  { readonly brand: unique symbol }
>()("LateInlineReceiptChanged", {}) {}

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
        AND marker.stream_id = ${event.threadId} AND marker.event_type = 'context-handoff.updated'
        AND marker.aggregate_kind = 'thread' AND marker.application_event_version = 2
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

// The provider turn records native input acceptance independently of the app run's outcome.
const ownsLateInlineTurn = Effect.fn("HistoryDeliveryOutcome.ownsLateInlineTurn")(function* (
  receipt: LateInlineReceipt,
) {
  const { binding, runId, runOrdinal, messageId, turn } = receipt;
  const database = yield* Effect.serviceOption(SqlClient.SqlClient);
  if (Option.isNone(database)) return false;
  const sql = database.value;
  if (Option.isNone(yield* Effect.serviceOption(sql.transactionService))) return false;
  const rows = yield* sql<{ readonly owned: number }>`
    SELECT 1 AS owned
    FROM orchestration_v2_projection_provider_threads AS provider
    JOIN orchestration_v2_projection_threads AS thread ON thread.thread_id = provider.thread_id
    JOIN orchestration_v2_projection_runs AS run ON run.run_id = ${runId}
    JOIN orchestration_v2_projection_run_attempts AS attempt ON attempt.attempt_id = ${turn.runAttemptId}
    WHERE provider.provider_thread_id = ${binding.providerThreadId}
      AND provider.thread_id = ${binding.threadId}
      AND provider.provider = 'codex'
      AND provider.provider_session_id = ${binding.providerSessionId}
      AND provider.last_run_ordinal = ${runOrdinal}
      AND json_extract(provider.payload_json, '$.providerInstanceId') = ${binding.providerInstanceId}
      AND json_extract(provider.payload_json, '$.nativeThreadRef.driver') = 'codex'
      AND json_extract(provider.payload_json, '$.nativeThreadRef.strength') = 'strong'
      AND json_extract(provider.payload_json, '$.nativeThreadRef.nativeId') = ${binding.nativeThreadId}
      AND json_extract(provider.payload_json, '$.runtimeIdentity.runtimeGeneration') = ${binding.runtimeGeneration}
      AND thread.active_provider_thread_id = provider.provider_thread_id AND thread.deleted_at IS NULL
      AND run.thread_id = provider.thread_id AND run.provider_thread_id = provider.provider_thread_id
      AND run.ordinal = ${runOrdinal} AND run.status IN ('running', 'waiting', 'failed', 'interrupted', 'completed', 'cancelled')
      AND json_extract(run.payload_json, '$.activeAttemptId') = attempt.attempt_id
      AND json_extract(run.payload_json, '$.userMessageId') = ${messageId}
      AND attempt.run_id = run.run_id AND attempt.thread_id = run.thread_id
      AND attempt.provider_thread_id = provider.provider_thread_id AND attempt.root_node_id = ${turn.nodeId}
      AND attempt.provider_turn_id IS NULL
      AND attempt.status IN ('running', 'failed', 'interrupted', 'completed', 'cancelled')
      AND json_extract(attempt.payload_json, '$.providerInstanceId') = ${binding.providerInstanceId}
      AND json_extract(attempt.payload_json, '$.nativeThreadId') = ${binding.nativeThreadId}
      AND NOT EXISTS (SELECT 1 FROM orchestration_v2_projection_provider_turns WHERE provider_turn_id = ${turn.id})
      AND NOT EXISTS (
        SELECT 1 FROM orchestration_v2_projection_provider_turns
        WHERE provider_thread_id = provider.provider_thread_id AND ordinal >= ${turn.ordinal}
      )
  `;
  return rows.length === 1;
});

/** Update only pending writes committed by this delivery invocation. */
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
  const restore = Effect.fn("HistoryDeliveryOutcome.restoreUnsent")(function* (
    handoff: OrchestrationV2ContextHandoff,
  ) {
    const marker = pending.get(handoff.id);
    if (marker === undefined) return;
    const database = yield* Effect.serviceOption(SqlClient.SqlClient);
    const inTransaction =
      Option.isSome(database) &&
      Option.isSome(yield* Effect.serviceOption(database.value.transactionService));
    const attempt = withProviderBatch(
      Effect.gen(function* () {
        if (yield* ownsPendingHistoryDelivery(marker)) yield* write(handoff);
      }).pipe(
        Effect.mapError((cause) =>
          hasSqlCause(cause) ? new InlineRestorationSqlFailure({ cause }) : cause,
        ),
      ),
    );
    // A nested provider batch cannot establish rollback of its enclosing transaction.
    if (inTransaction) return yield* attempt;
    return yield* attempt.pipe(
      Effect.catchCause((cause) => {
        const [failure] = cause.reasons;
        // Retry a statement failure only after rollback, and compare ownership again.
        return cause.reasons.length === 1 &&
          failure?._tag === "Fail" &&
          isInlineRestorationSqlFailure(failure.error)
          ? attempt
          : Effect.failCause(cause);
      }),
    );
  });
  return {
    persist: (handoff: OrchestrationV2ContextHandoff): ReturnType<typeof restore> =>
      handoff.delivery?.status === "inline"
        ? withProviderBatch(
            Effect.gen(function* () {
              const marker = pending.get(handoff.id);
              if (marker !== undefined && (yield* ownsPendingHistoryDelivery(marker)))
                yield* write(handoff);
            }),
          )
        : write(handoff).pipe(
            Effect.map(([stored]) => {
              if (handoff.delivery?.status === "pending") pending.set(handoff.id, stored);
            }),
          ),
    restore,
    trackInlineStart(
      sink: EventSinkV2Shape["write"],
      idAllocator: IdAllocatorV2["Service"],
    ): InlineHistoryStart {
      let status: "unsent" | "closed" | "dispatched" | "acknowledged" = "unsent";
      const ownsDelivery = Effect.fn("HistoryDeliveryOutcome.ownsLateInlineDelivery")(function* (
        receipt: LateInlineReceipt,
      ) {
        if (!receipt.isCurrent() || pending.size === 0) return false;
        if (!(yield* ownsLateInlineTurn(receipt))) return false;
        const { binding, runId } = receipt;
        for (const marker of pending.values()) {
          if (marker === undefined) return false;
          const { event } = marker;
          if (
            event.type !== "context-handoff.updated" ||
            event.threadId !== binding.threadId ||
            event.runId !== runId ||
            event.providerInstanceId !== binding.providerInstanceId ||
            event.payload.status !== "ready" ||
            event.payload.toProviderThreadId !== binding.providerThreadId ||
            event.payload.delivery?.nativeThreadId !== binding.nativeThreadId ||
            !(yield* ownsPendingHistoryDelivery(marker))
          )
            return false;
        }
        return receipt.isCurrent();
      });
      const recordLateReceipt = Effect.fn("HistoryDeliveryOutcome.recordLateInlineReceipt")(
        function* (receipt: LateInlineReceipt) {
          if (status !== "acknowledged" || !receipt.isCurrent()) return false;
          // The matching ACK and terminal describe the original input. Live
          // exclusivity can still change while SQLite commits or publishes.
          // Keep the original pending marker through every crash cut. Only
          // independent native evidence could settle delivery for reconstruction.
          if (!(yield* withProviderBatch(ownsDelivery(receipt))) || !receipt.isCurrent())
            return false;
          return yield* withProviderBatch(
            Effect.gen(function* () {
              // The ownership read completed its writer/publication interval.
              // Recheck the exact marker and attempt in the receipt's own write.
              if (!(yield* ownsDelivery(receipt))) return false;
              const { binding, runId, turn } = receipt;
              const occurredAt = yield* DateTime.now;
              const id = yield* idAllocator.allocate.event({ threadId: binding.threadId });
              if (!receipt.isCurrent()) return false;
              const written = yield* sink({
                events: [
                  {
                    id,
                    type: "provider-turn.updated",
                    threadId: binding.threadId,
                    runId,
                    nodeId: turn.nodeId,
                    driver: binding.driver,
                    providerInstanceId: binding.providerInstanceId,
                    occurredAt,
                    payload: turn,
                  },
                ],
              });
              if (written.length !== 1 || !receipt.isCurrent())
                return yield* new LateInlineReceiptChanged();
              return true;
            }),
          ).pipe(Effect.catchTags({ LateInlineReceiptChanged: () => Effect.succeed(false) }));
        },
      );
      return {
        get acknowledged() {
          return status === "acknowledged";
        },
        closeUnsent() {
          if (status !== "unsent") return false;
          status = "closed";
          return true;
        },
        canDispatch: () => status === "unsent",
        tryDispatch() {
          if (status !== "unsent") return false;
          status = "dispatched";
          return true;
        },
        markAcknowledged() {
          if (status === "dispatched") status = "acknowledged";
        },
        reconcileLate: (terminal, binding, input, isCurrent, release) =>
          Deferred.await(terminal).pipe(
            Effect.flatMap((observed) =>
              recordLateReceipt({
                binding,
                runId: input.runId,
                runOrdinal: input.runOrdinal,
                messageId: input.message.messageId,
                turn: {
                  id: idAllocator.derive.providerTurn({
                    driver: binding.driver,
                    nativeTurnId: observed.nativeTurnId,
                  }),
                  providerThreadId: binding.providerThreadId,
                  nodeId: input.rootNodeId,
                  runAttemptId: input.attemptId,
                  nativeTurnRef: {
                    driver: binding.driver,
                    nativeId: observed.nativeTurnId,
                    strength: "strong",
                  },
                  ordinal: input.providerTurnOrdinal,
                  status: observed.status,
                  startedAt: observed.startedAt,
                  completedAt: observed.completedAt,
                },
                isCurrent: () => isCurrent(observed),
              }).pipe(
                Effect.tap((committed) =>
                  Effect.sync(() => {
                    if (committed) release(observed);
                  }),
                ),
              ),
            ),
            Effect.catchCause(() =>
              Effect.logWarning("Failed to reconcile late Codex inline history", {
                runId: input.runId,
              }),
            ),
          ),
      };
    },
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

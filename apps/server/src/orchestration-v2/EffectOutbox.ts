import type { NativeCreationExecutionReferenceV2 } from "../jones/nativeCreation/NativeCreationExecutionTypes.ts";
import { confirmedNativeCompletion } from "../jones/nativeCreation/NativeEffectCompletion.ts";
import { CommandId, ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import { forkParked } from "../serverActivation.ts";

import {
  enqueue as enqueueEffects,
  EffectOutboxError,
  NativeOrchestrationEffectPayloadV2,
  OrchestrationEffectRequestV2,
  type PendingOrchestrationEffectV2,
} from "./EffectOutboxEnqueue.ts";

export {
  EffectOutboxError,
  NativeOrchestrationEffectPayloadV2,
  OrchestrationEffectRequestV2,
  type PendingOrchestrationEffectV2,
} from "./EffectOutboxEnqueue.ts";

export const REPLAY_SAFE_EFFECT_TYPES_AFTER_PROCESS_LOSS = [
  "provider-runtime.continue",
  "provider-session.detach",
  "provider-thread.rollback",
  "checkpoint.capture",
  "terminal.cleanup",
  "preview.cleanup",
  "attachment.cleanup",
  "thread-title.generate",
  "delegated-tasks.stop",
] as const satisfies ReadonlyArray<OrchestrationEffectRequestV2["type"]>;

export const PROCESS_BOUND_EFFECT_TYPES = [
  "provider-turn.start",
  "provider-turn.interrupt",
  "provider-turn.steer",
  "provider-turn.steer-batch",
  "provider-turn.restart",
  "runtime-request.respond",
] as const satisfies ReadonlyArray<OrchestrationEffectRequestV2["type"]>;

export const OrchestrationEffectStatusV2 = Schema.Literals([
  "pending",
  "running",
  "succeeded",
  "failed",
  "cancelled",
]);
export type OrchestrationEffectStatusV2 = typeof OrchestrationEffectStatusV2.Type;

/**
 * Command retries are deduplicated by their receipts first, and a run's effects
 * are re-enqueued only around one server restart, so a week is far longer than
 * either and leaves recent history for debugging. Failed rows are kept: storage
 * cleanup keeps a deleted thread's worktree while any of its effects failed.
 */
export const SETTLED_EFFECT_RETENTION = Duration.days(7);
const PRUNE_BATCH_SIZE = 500;

export interface OrchestrationEffectV2 {
  readonly id: string;
  readonly commandId: CommandId;
  readonly threadId: ThreadId;
  readonly nativeCreationExecutionReference?: NativeCreationExecutionReferenceV2;
  readonly request: OrchestrationEffectRequestV2;
  readonly status: OrchestrationEffectStatusV2;
  readonly attemptCount: number;
  readonly availableAt: string;
  readonly leaseOwner: string | null;
  readonly leaseExpiresAt: string | null;
  readonly createdAt: string;
  readonly updatedAt: string;
  readonly completedAt: string | null;
  readonly lastError: string | null;
}

const isEffectOutboxError = Schema.is(EffectOutboxError);

export interface EffectOutboxV2Shape {
  readonly awaitAvailable: Effect.Effect<void>;
  readonly notifyAvailable: (count?: number) => Effect.Effect<void>;
  /** Persist rows only. Notify workers after the surrounding transaction commits. */
  readonly enqueue: (
    effects: ReadonlyArray<PendingOrchestrationEffectV2>,
  ) => Effect.Effect<void, EffectOutboxError>;
  readonly awaitCompletion: (
    effectIds: ReadonlyArray<string>,
  ) => Effect.Effect<void, EffectOutboxError>;
  readonly get: (
    effectId: string,
  ) => Effect.Effect<Option.Option<OrchestrationEffectV2>, EffectOutboxError>;
  readonly listByCommandId: (
    commandId: CommandId,
  ) => Effect.Effect<ReadonlyArray<OrchestrationEffectV2>, EffectOutboxError>;
  readonly listByThreadId: (
    threadId: ThreadId,
  ) => Effect.Effect<ReadonlyArray<OrchestrationEffectV2>, EffectOutboxError>;
  readonly cancelUnsettled: (input: {
    readonly threadId: ThreadId;
    readonly effectTypes: ReadonlyArray<OrchestrationEffectRequestV2["type"]>;
    readonly reason: string;
  }) => Effect.Effect<ReadonlyArray<string>, EffectOutboxError>;
  readonly signalCancellations: (effectIds: ReadonlyArray<string>) => Effect.Effect<void>;
  readonly awaitCancellation: (effectId: string) => Effect.Effect<void>;
  readonly clearCancellation: (effectId: string) => Effect.Effect<void>;
  /** Resolves once the effect is neither pending nor running, or right away when it does not exist. */
  readonly awaitSettled: (effectId: string) => Effect.Effect<void, EffectOutboxError>;
  readonly reconcileAfterProcessLoss: Effect.Effect<
    { readonly requeued: number; readonly cancelled: number },
    EffectOutboxError
  >;
  readonly claimNext: (input: {
    readonly workerId: string;
    readonly leaseDurationMs: number;
    readonly excludeRestartContinuations?: boolean;
  }) => Effect.Effect<Option.Option<OrchestrationEffectV2>, EffectOutboxError>;
  readonly nextClaimableAt: Effect.Effect<Option.Option<DateTime.Utc>, EffectOutboxError>;
  readonly succeed: (input: {
    readonly effectId: string;
    readonly workerId: string;
  }) => Effect.Effect<boolean, EffectOutboxError>;
  readonly retry: (input: {
    readonly effectId: string;
    readonly workerId: string;
    readonly error: string;
    readonly delayMs: number;
  }) => Effect.Effect<boolean, EffectOutboxError>;
  readonly fail: (input: {
    readonly effectId: string;
    readonly workerId: string;
    readonly error: string;
  }) => Effect.Effect<boolean, EffectOutboxError>;
  readonly pruneSettled: Effect.Effect<number, EffectOutboxError>;
}

export class EffectOutboxV2 extends Context.Service<EffectOutboxV2, EffectOutboxV2Shape>()(
  "t3/orchestration-v2/EffectOutbox/EffectOutboxV2",
) {}

type EffectRow = {
  readonly effect_id: string;
  readonly command_id: string;
  readonly thread_id: string;
  readonly effect_type: string;
  readonly payload_json: string;
  readonly status: string;
  readonly attempt_count: number;
  readonly available_at: string;
  readonly lease_owner: string | null;
  readonly lease_expires_at: string | null;
  readonly created_at: string;
  readonly updated_at: string;
  readonly completed_at: string | null;
  readonly last_error: string | null;
};

const decodeRequest = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationEffectRequestV2),
);

const decodeNativePayload = Schema.decodeUnknownEffect(NativeOrchestrationEffectPayloadV2, {
  onExcessProperty: "error",
});
// An invalid native envelope must never fall back to ordinary provider execution.
export const decodeOrchestrationEffectPayloadV2 = (payload: string) =>
  Effect.gen(function* () {
    const raw = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown))(payload);
    if (
      typeof raw === "object" &&
      raw !== null &&
      (Object.hasOwn(raw, "request") || Object.hasOwn(raw, "nativeCreationExecutionReference"))
    )
      return yield* decodeNativePayload(raw);
    return { request: yield* decodeRequest(payload) };
  });

const rowToEffect = (row: EffectRow) =>
  decodeOrchestrationEffectPayloadV2(row.payload_json).pipe(
    Effect.map((payload): OrchestrationEffectV2 => ({
      id: row.effect_id,
      commandId: CommandId.make(row.command_id),
      threadId: ThreadId.make(row.thread_id),
      ...payload,
      status: row.status as OrchestrationEffectStatusV2,
      attemptCount: row.attempt_count,
      availableAt: row.available_at,
      leaseOwner: row.lease_owner,
      leaseExpiresAt: row.lease_expires_at,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
      completedAt: row.completed_at,
      lastError: row.last_error,
    })),
  );

export const layer: Layer.Layer<EffectOutboxV2, never, SqlClient.SqlClient> = Layer.effect(
  EffectOutboxV2,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const available = yield* Queue.dropping<void>(64);
    const completionChanges = yield* PubSub.sliding<void>(64);
    const cancellationSignals = new Map<string, Deferred.Deferred<void>>();
    const settledSignals = new Map<string, Deferred.Deferred<void>>();
    const signalSettled = (effectIds: ReadonlyArray<string>) =>
      Effect.forEach(
        effectIds,
        (effectId) => {
          const signal = settledSignals.get(effectId);
          if (signal === undefined) return Effect.void;
          settledSignals.delete(effectId);
          return Deferred.succeed(signal, undefined);
        },
        { discard: true },
      );
    // Neither native envelope key is present. json_type is SQL NULL only for a
    // missing key, so an explicit JSON null still takes the strict native path.
    // This predicate reads no Jones table, so it works on upstream-only schema.
    const ordinaryPayload = sql`json_type(payload_json, '$.request') IS NULL
      AND json_type(payload_json, '$.nativeCreationExecutionReference') IS NULL`;
    const notifyAvailable = (count = 1) =>
      Queue.offerAll(
        available,
        Array.from({ length: Math.min(64, Math.max(0, Math.floor(count))) }, () => undefined),
      ).pipe(Effect.asVoid);
    // An earlier effect waiting out a retry backoff still blocks later ones, so
    // a turn cannot start while a failed rollback is about to restore files.
    const claimableCandidatePredicate = (
      availableBefore?: string,
      excludeRestartContinuations = false,
    ) =>
      sql`
        ${
          availableBefore === undefined
            ? sql`1 = 1`
            : sql`candidate.available_at <= ${availableBefore}`
        }
        AND candidate.status = 'pending'
        AND NOT (candidate.effect_type = 'provider-runtime.continue'
          AND COALESCE(json_extract(candidate.payload_json, '$.preparedForRestart'), 0) = 1)
        AND (
          candidate.effect_type NOT IN ('provider-turn.steer', 'provider-turn.steer-batch')
          OR candidate.command_id NOT GLOB 'command:queue-tool-boundary:*'
          OR EXISTS (
            SELECT 1 FROM orchestration_command_receipts AS candidate_receipt
            WHERE candidate_receipt.command_id = candidate.command_id
              AND candidate_receipt.aggregate_kind = 'thread'
              AND candidate_receipt.aggregate_id = candidate.thread_id
              AND candidate_receipt.status = 'accepted'
              AND candidate_receipt.command_type IN ('queued-message.promote-to-steer', 'queued-message.promote-batch-to-steer')
              AND typeof(candidate_receipt.result_sequence) = 'integer'
              AND candidate_receipt.result_sequence > 0
              AND NOT EXISTS (
                SELECT 1 FROM orchestration_v2_effect_outbox AS predecessor
                LEFT JOIN orchestration_command_receipts AS predecessor_receipt
                  ON predecessor_receipt.command_id = predecessor.command_id
                WHERE predecessor.thread_id = candidate.thread_id
                  AND predecessor.status IN ('pending', 'running', 'failed', 'cancelled')
                  AND predecessor.effect_type IN ('provider-turn.steer', 'provider-turn.steer-batch')
                  AND predecessor.command_id GLOB 'command:queue-tool-boundary:*'
                  AND predecessor.effect_id != candidate.effect_id
                  AND json_extract(predecessor.payload_json, '$.providerSessionId') = json_extract(candidate.payload_json, '$.providerSessionId')
                  AND json_extract(predecessor.payload_json, '$.providerThreadId') = json_extract(candidate.payload_json, '$.providerThreadId')
                  AND json_extract(predecessor.payload_json, '$.providerTurnId') = json_extract(candidate.payload_json, '$.providerTurnId')
                  AND (
                    predecessor_receipt.command_id IS NULL
                    OR predecessor_receipt.aggregate_kind IS NOT 'thread'
                    OR predecessor_receipt.aggregate_id IS NOT predecessor.thread_id
                    OR predecessor_receipt.status IS NOT 'accepted'
                    OR predecessor_receipt.command_type NOT IN ('queued-message.promote-to-steer', 'queued-message.promote-batch-to-steer')
                    OR typeof(predecessor_receipt.result_sequence) != 'integer'
                    OR predecessor_receipt.result_sequence <= 0
                    OR predecessor_receipt.result_sequence < candidate_receipt.result_sequence
                    OR (predecessor_receipt.result_sequence = candidate_receipt.result_sequence
                      AND predecessor.command_id != candidate.command_id)
                  )
              )
          )
        )
        AND NOT EXISTS (
          SELECT 1
          FROM orchestration_v2_effect_outbox AS active
          WHERE active.thread_id = candidate.thread_id
            AND (
              active.status = 'running'
              OR (
                active.status = 'pending'
                AND active.rowid < candidate.rowid
                AND NOT (active.effect_type = 'provider-runtime.continue'
                  AND COALESCE(json_extract(active.payload_json, '$.preparedForRestart'), 0) = 1)
                AND NOT (active.effect_type IN ('provider-turn.steer', 'provider-turn.steer-batch')
                  AND active.command_id GLOB 'command:queue-tool-boundary:*')
                AND ${
                  excludeRestartContinuations
                    ? sql`active.effect_type != 'provider-runtime.continue'`
                    : sql`1 = 1`
                }
              )
            )
            AND (
              (
                candidate.effect_type = 'thread-title.generate'
                AND active.effect_type = 'thread-title.generate'
              )
              OR
              (
                candidate.effect_type != 'thread-title.generate'
                AND active.effect_type != 'thread-title.generate'
              )
            )
        )
      `;

    const cancellationSignal = (effectId: string) => {
      const existing = cancellationSignals.get(effectId);
      if (existing !== undefined) return existing;
      const created = Deferred.makeUnsafe<void>();
      cancellationSignals.set(effectId, created);
      return created;
    };

    const decodeRows = (operation: string, rows: ReadonlyArray<EffectRow>) =>
      Effect.forEach(rows, rowToEffect).pipe(
        Effect.mapError((cause) => new EffectOutboxError({ operation, cause })),
      );

    const service: EffectOutboxV2Shape = {
      enqueue: (effects) =>
        enqueueEffects(effects).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
      get: (effectId) =>
        sql<EffectRow>`
          SELECT *
          FROM orchestration_v2_effect_outbox
          WHERE effect_id = ${effectId}
          LIMIT 1
        `.pipe(
          Effect.flatMap((rows) => {
            const row = rows[0];
            return row === undefined
              ? Effect.succeed(Option.none())
              : rowToEffect(row).pipe(Effect.map(Option.some));
          }),
          Effect.mapError((cause) => new EffectOutboxError({ operation: "get", effectId, cause })),
        ),
      awaitCompletion: (effectIds) =>
        Effect.scoped(
          Effect.gen(function* () {
            if (effectIds.length === 0) return;
            const ids = Array.from(new Set(effectIds));
            const subscription = yield* PubSub.subscribe(completionChanges);
            while (true) {
              const rows =
                yield* sql<EffectRow>`SELECT * FROM orchestration_v2_effect_outbox WHERE effect_id IN ${sql.in(ids)}`;
              const effects = yield* decodeRows("await-completion", rows);
              if (effects.length !== ids.length)
                return yield* new EffectOutboxError({
                  operation: "await-completion",
                  cause: "An exact cleanup effect is missing; its completion is unknown.",
                });
              const failed = effects.find(
                (effect) => effect.status === "failed" || effect.status === "cancelled",
              );
              if (failed !== undefined)
                return yield* new EffectOutboxError({
                  operation: "await-completion",
                  effectId: failed.id,
                  cause: failed.lastError ?? `Cleanup ${failed.status}.`,
                });
              if (effects.every((effect) => effect.status === "succeeded")) return;
              yield* PubSub.take(subscription);
            }
          }),
        ).pipe(
          Effect.mapError((cause) =>
            isEffectOutboxError(cause)
              ? cause
              : new EffectOutboxError({ operation: "await-completion", cause }),
          ),
        ),
      awaitAvailable: Queue.take(available),
      notifyAvailable,
      listByCommandId: (commandId) =>
        sql<EffectRow>`
          SELECT *
          FROM orchestration_v2_effect_outbox
          WHERE command_id = ${commandId}
          ORDER BY created_at ASC, effect_id ASC
        `.pipe(
          Effect.flatMap((rows) => decodeRows("list", rows)),
          Effect.mapError((cause) =>
            isEffectOutboxError(cause)
              ? cause
              : new EffectOutboxError({ operation: "list", cause }),
          ),
        ),
      listByThreadId: (threadId) =>
        sql<EffectRow>`SELECT * FROM orchestration_v2_effect_outbox WHERE thread_id = ${threadId}
          ORDER BY created_at ASC, effect_id ASC`.pipe(
          Effect.flatMap((rows) => decodeRows("list-thread", rows)),
          Effect.mapError((cause) =>
            isEffectOutboxError(cause)
              ? cause
              : new EffectOutboxError({ operation: "list-thread", cause }),
          ),
        ),
      cancelUnsettled: ({ threadId, effectTypes, reason }) =>
        Effect.gen(function* () {
          if (effectTypes.length === 0) return [];
          const now = DateTime.formatIso(yield* DateTime.now);
          const rows = yield* sql<{ readonly effect_id: string }>`
            UPDATE orchestration_v2_effect_outbox
            SET
              status = 'cancelled',
              lease_owner = NULL,
              lease_expires_at = NULL,
              completed_at = ${now},
              updated_at = ${now},
              last_error = ${reason}
            WHERE thread_id = ${threadId}
              AND status IN ('pending', 'running')
              AND effect_type IN ${sql.in(effectTypes)}
            RETURNING effect_id
          `;
          if (rows.length > 0) yield* PubSub.publish(completionChanges, undefined);
          return rows.map(({ effect_id }) => effect_id);
        }).pipe(
          Effect.mapError(
            (cause) => new EffectOutboxError({ operation: "cancel-unsettled", cause }),
          ),
        ),
      signalCancellations: (effectIds) =>
        Effect.gen(function* () {
          yield* Effect.forEach(
            effectIds,
            (effectId) => {
              const signal = cancellationSignals.get(effectId);
              return signal === undefined ? Effect.void : Deferred.succeed(signal, undefined);
            },
            { discard: true },
          );
          yield* signalSettled(effectIds);
          if (effectIds.length > 0) yield* notifyAvailable(effectIds.length);
        }),
      awaitCancellation: (effectId) => Deferred.await(cancellationSignal(effectId)),
      clearCancellation: (effectId) =>
        Effect.sync(() => {
          cancellationSignals.delete(effectId);
        }),
      awaitSettled: (effectId) =>
        Effect.gen(function* () {
          // Registered before the read, so a settle right after it still resolves.
          const signal = settledSignals.get(effectId) ?? Deferred.makeUnsafe<void>();
          settledSignals.set(effectId, signal);
          const effect = yield* service.get(effectId);
          if (Option.exists(effect, ({ status }) => status === "pending" || status === "running")) {
            return yield* Deferred.await(signal);
          }
          yield* signalSettled([effectId]);
        }),
      reconcileAfterProcessLoss: Effect.gen(function* () {
        const now = DateTime.formatIso(yield* DateTime.now);
        const cancelledRows = yield* sql<{ readonly effect_id: string }>`
          UPDATE orchestration_v2_effect_outbox
          SET
            status = 'cancelled',
            lease_owner = NULL,
            lease_expires_at = NULL,
            completed_at = ${now},
            updated_at = ${now},
            last_error = 'Cancelled because the server process ended before the effect completed.'
          WHERE status IN ('pending', 'running')
            AND effect_type IN ${sql.in(PROCESS_BOUND_EFFECT_TYPES)}
          RETURNING effect_id
        `;
        const requeuedRows = yield* sql<{ readonly effect_id: string }>`
          UPDATE orchestration_v2_effect_outbox
          SET
            status = 'pending',
            lease_owner = NULL,
            lease_expires_at = NULL,
            available_at = ${now},
            updated_at = ${now},
            last_error = 'Requeued after the previous server process ended.'
          WHERE status = 'running'
            AND effect_type IN ${sql.in(REPLAY_SAFE_EFFECT_TYPES_AFTER_PROCESS_LOSS)}
          RETURNING effect_id
        `;
        const preparedRows = yield* sql<{ readonly effect_id: string }>`
          UPDATE orchestration_v2_effect_outbox SET
            payload_json = json_set(payload_json, '$.preparedForRestart', json('false')),
            available_at = ${now}, updated_at = ${now}
          WHERE status = 'pending' AND effect_type = 'provider-runtime.continue'
            AND json_extract(payload_json, '$.preparedForRestart') = 1 RETURNING effect_id`;
        if (requeuedRows.length + preparedRows.length > 0)
          yield* notifyAvailable(requeuedRows.length + preparedRows.length);
        if (cancelledRows.length > 0) yield* PubSub.publish(completionChanges, undefined);
        return {
          requeued: requeuedRows.length + preparedRows.length,
          cancelled: cancelledRows.length,
        };
      }).pipe(
        Effect.mapError(
          (cause) => new EffectOutboxError({ operation: "reconcile-process-loss", cause }),
        ),
      ),
      claimNext: ({ workerId, leaseDurationMs, excludeRestartContinuations = false }) =>
        Effect.gen(function* () {
          const now = yield* DateTime.now;
          const nowIso = DateTime.formatIso(now);
          const leaseExpiresAt = DateTime.formatIso(
            DateTime.add(now, { milliseconds: Math.max(1, leaseDurationMs) }),
          );
          // Empty safety claims are expected while the daemon is idle. Tracing
          // this query records the full SQL text on every poll and can dominate
          // the local trace without adding actionable information.
          const claimStatement = sql<EffectRow>`
            UPDATE orchestration_v2_effect_outbox
            SET
              status = 'running',
              attempt_count = attempt_count + 1,
              lease_owner = ${workerId},
              lease_expires_at = ${leaseExpiresAt},
              updated_at = ${nowIso},
              last_error = NULL
            WHERE effect_id = (
              SELECT candidate.effect_id
              FROM orchestration_v2_effect_outbox AS candidate
              WHERE ${claimableCandidatePredicate(nowIso, excludeRestartContinuations)}
                AND ${excludeRestartContinuations ? sql`candidate.effect_type != 'provider-runtime.continue'` : sql`1 = 1`}
              ORDER BY candidate.available_at ASC, candidate.created_at ASC, candidate.effect_id ASC
              LIMIT 1
            )
            RETURNING *
          `;
          const rows = yield* claimStatement.pipe(Effect.withTracerEnabled(false));
          const row = rows[0];
          if (row === undefined) return Option.none();
          cancellationSignals.set(row.effect_id, Deferred.makeUnsafe<void>());
          return Option.some(yield* rowToEffect(row));
        }).pipe(Effect.mapError((cause) => new EffectOutboxError({ operation: "claim", cause }))),
      nextClaimableAt: Effect.gen(function* () {
        const rows = yield* sql<{ readonly available_at: string | null }>`
          SELECT MIN(candidate.available_at) AS available_at
          FROM orchestration_v2_effect_outbox AS candidate
          WHERE ${claimableCandidatePredicate()}
        `.pipe(Effect.withTracerEnabled(false));
        const availableAt = rows[0]?.available_at;
        if (availableAt === undefined || availableAt === null) return Option.none();
        const parsed = DateTime.make(availableAt);
        if (Option.isNone(parsed)) {
          return yield* new EffectOutboxError({
            operation: "next-claimable",
            cause: `Invalid available_at timestamp: ${availableAt}`,
          });
        }
        return parsed;
      }).pipe(
        Effect.mapError((cause) =>
          isEffectOutboxError(cause)
            ? cause
            : new EffectOutboxError({ operation: "next-claimable", cause }),
        ),
      ),
      succeed: ({ effectId, workerId }) =>
        Effect.gen(function* () {
          const now = DateTime.formatIso(yield* DateTime.now);
          const settle = (guard: typeof ordinaryPayload) => sql<{ readonly effect_id: string }>`
            UPDATE orchestration_v2_effect_outbox
            SET
              status = 'succeeded',
              lease_owner = NULL,
              lease_expires_at = NULL,
              completed_at = ${now},
              updated_at = ${now},
              last_error = NULL
            WHERE effect_id = ${effectId}
              AND status = 'running'
              AND lease_owner = ${workerId}
              AND ${guard}
            RETURNING effect_id
          `;
          let rows = yield* settle(ordinaryPayload);
          if (rows.length === 0) {
            const current = yield* sql<{ readonly payload_json: string }>`
              SELECT payload_json FROM orchestration_v2_effect_outbox
              WHERE effect_id = ${effectId} AND status = 'running' AND lease_owner = ${workerId}
            `;
            // A native envelope must pass the strict decode and its proof. The
            // payload match binds that proof to the row that was decoded.
            if (
              current.length === 1 &&
              "nativeCreationExecutionReference" in
                (yield* decodeOrchestrationEffectPayloadV2(current[0]!.payload_json))
            )
              rows = yield* settle(
                sql`payload_json = ${current[0]!.payload_json} AND ${confirmedNativeCompletion(sql, { workerId, now })}`,
              );
          }
          if (rows.length === 1) {
            cancellationSignals.delete(effectId);
            yield* signalSettled([effectId]);
            yield* notifyAvailable();
          }
          return rows.length === 1;
        }).pipe(
          Effect.mapError(
            (cause) => new EffectOutboxError({ operation: "succeed", effectId, cause }),
          ),
        ),
      retry: ({ effectId, workerId, error, delayMs }) =>
        Effect.gen(function* () {
          const now = yield* DateTime.now;
          const nowIso = DateTime.formatIso(now);
          const availableAt = DateTime.formatIso(
            DateTime.add(now, { milliseconds: Math.max(0, delayMs) }),
          );
          const rows = yield* sql<{ readonly effect_id: string }>`
            UPDATE orchestration_v2_effect_outbox
            SET
              status = CASE WHEN json_type(payload_json, '$.nativeCreationExecutionReference') IS NULL THEN 'pending' ELSE 'failed' END,
              available_at = ${availableAt},
              lease_owner = NULL,
              lease_expires_at = NULL,
              updated_at = ${nowIso},
              last_error = ${error}
            WHERE effect_id = ${effectId}
              AND status = 'running'
              AND lease_owner = ${workerId}
            RETURNING effect_id
          `;
          if (rows.length === 1) {
            cancellationSignals.delete(effectId);
            yield* PubSub.publish(completionChanges, undefined);
            yield* notifyAvailable();
          }
          return rows.length === 1;
        }).pipe(
          Effect.mapError(
            (cause) => new EffectOutboxError({ operation: "retry", effectId, cause }),
          ),
        ),
      fail: ({ effectId, workerId, error }) =>
        Effect.gen(function* () {
          const now = DateTime.formatIso(yield* DateTime.now);
          const rows = yield* sql<{ readonly effect_id: string }>`
            UPDATE orchestration_v2_effect_outbox
            SET
              status = 'failed',
              lease_owner = NULL,
              lease_expires_at = NULL,
              completed_at = ${now},
              updated_at = ${now},
              last_error = ${error}
            WHERE effect_id = ${effectId}
              AND status = 'running'
              AND lease_owner = ${workerId}
            RETURNING effect_id
          `;
          if (rows.length === 1) {
            cancellationSignals.delete(effectId);
            yield* signalSettled([effectId]);
            yield* notifyAvailable();
          }
          return rows.length === 1;
        }).pipe(
          Effect.mapError((cause) => new EffectOutboxError({ operation: "fail", effectId, cause })),
        ),
      pruneSettled: Effect.gen(function* () {
        const cutoff = DateTime.formatIso(
          DateTime.subtractDuration(yield* DateTime.now, SETTLED_EFFECT_RETENTION),
        );
        // Each batch is its own statement, so a large backlog never holds the
        // write lock for long and writers can commit between batches.
        const deleteBatch = sql<{ readonly effect_id: string }>`
          DELETE FROM orchestration_v2_effect_outbox
          WHERE rowid IN (
            SELECT rowid
            FROM orchestration_v2_effect_outbox
            WHERE status IN ('succeeded', 'cancelled')
              AND completed_at < ${cutoff}
            LIMIT ${PRUNE_BATCH_SIZE}
          )
          RETURNING effect_id
        `;
        let pruned = 0;
        while (true) {
          const deleted = (yield* deleteBatch).length;
          pruned += deleted;
          if (deleted < PRUNE_BATCH_SIZE) return pruned;
          yield* Effect.yieldNow;
        }
      }).pipe(
        Effect.mapError((cause) => new EffectOutboxError({ operation: "prune-settled", cause })),
      ),
    };

    return service;
  }),
);

export const layerPruneWorker = Layer.effectDiscard(
  Effect.gen(function* () {
    const outbox = yield* EffectOutboxV2;
    yield* forkParked(
      outbox.pruneSettled.pipe(
        Effect.catch((cause) => Effect.logWarning("Failed to prune settled effects", { cause })),
        Effect.repeat(Schedule.spaced(Duration.hours(1))),
      ),
    );
  }),
);

import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import * as TerminalManager from "../../terminal/Manager.ts";
import { makeJonesTablePresence } from "../persistence/JonesTablePresence.ts";
import * as Storage from "./Storage.ts";
import * as S from "./StorageSchema.ts";

export class LegacyTerminalCleanupWorkerError extends Schema.TaggedError<LegacyTerminalCleanupWorkerError>()(
  "LegacyTerminalCleanupWorkerError",
  {
    operation: Schema.Literals(["entry", "list", "claim", "close", "finish", "reconcile"]),
    reason: Schema.Literals([
      "ambient_transaction",
      "client_mismatch",
      "invalid_limit",
      "storage_failure",
      "claim_unconfirmed",
      "owner_unavailable",
      "close_unconfirmed",
      "completion_unconfirmed",
      "attempt_changed",
    ]),
    effectId: Schema.optional(Schema.NonEmptyString),
    cause: Schema.optional(Schema.Defect()),
  },
) {}

export interface TerminalCleanupDrainResult {
  readonly selected: number;
  readonly claimed: number;
  readonly closed: number;
  readonly contended: number;
  readonly unresolved: number;
}

export class TerminalCleanupWorker extends Context.Service<
  TerminalCleanupWorker,
  {
    readonly runOnce: (
      limit?: number,
    ) => Effect.Effect<TerminalCleanupDrainResult, LegacyTerminalCleanupWorkerError>;
    readonly drain: (
      limit?: number,
    ) => Effect.Effect<TerminalCleanupDrainResult, LegacyTerminalCleanupWorkerError>;
    readonly wake: Effect.Effect<void>;
    readonly awaitWork: Effect.Effect<void>;
  }
>()("t3/jones/legacyBootstrap/TerminalCleanupWorker") {}

const decodePending = Schema.decodeUnknownEffect(
  Schema.Array(Schema.Struct({ effect_id: Schema.NonEmptyString })),
);
const decodeCount = Schema.decodeUnknownEffect(
  Schema.Array(Schema.Struct({ count: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)) })),
);
const isWorkerError = Schema.is(LegacyTerminalCleanupWorkerError);
const isControlError = Schema.is(TerminalManager.LegacyTerminalControlError);

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const terminals = yield* TerminalManager.TerminalManager;
  const random = yield* SecureRandom.SecureRandom;
  const tables = makeJonesTablePresence(sql);
  const storage = Storage.make(sql, tables);
  // A buffered token survives wake-before-wait and wake-during-drain. Extra wakes coalesce
  // only when a token is already retained; runOnce/drain never consume it.
  const available = yield* Queue.sliding<void>(1);
  const normalize =
    (operation: LegacyTerminalCleanupWorkerError["operation"], effectId?: string) =>
    (cause: unknown) =>
      isWorkerError(cause)
        ? cause
        : new LegacyTerminalCleanupWorkerError({
            operation,
            reason: "storage_failure",
            ...(effectId === undefined ? {} : { effectId }),
            cause,
          });
  const entry = Effect.gen(function* () {
    const current = yield* Effect.serviceOption(SqlClient.SqlClient);
    if (Option.isSome(current) && current.value !== sql)
      return yield* new LegacyTerminalCleanupWorkerError({
        operation: "entry",
        reason: "client_mismatch",
      });
    if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
      return yield* new LegacyTerminalCleanupWorkerError({
        operation: "entry",
        reason: "ambient_transaction",
      });
  });

  const finish = Effect.fn("LegacyTerminalCleanupWorker.finish")(
    function* (started: S.TerminalCleanup, outcome: NonNullable<S.TerminalCleanup["outcome"]>) {
      if (started.state !== "started" || started.attemptId === null)
        return yield* new LegacyTerminalCleanupWorkerError({
          operation: "finish",
          reason: "attempt_changed",
          effectId: started.effectId,
        });
      const attemptId = started.attemptId;
      const completed = yield* sql.withTransaction(
        Effect.gen(function* () {
          const writer = yield* storage.forTransaction;
          return yield* writer.finishTerminalCleanup({
            effectId: started.effectId,
            expectedRevision: started.revision,
            attemptId,
            outcome,
          });
        }),
      );
      if (
        completed === undefined ||
        completed.attemptId !== started.attemptId ||
        completed.revision !== started.revision + 1 ||
        completed.outcome !== outcome ||
        completed.state !== (outcome === "closed" ? "succeeded" : "unknown")
      )
        return yield* new LegacyTerminalCleanupWorkerError({
          operation: "finish",
          reason: "attempt_changed",
          effectId: started.effectId,
        });
      return completed;
    },
    (effect, started) => effect.pipe(Effect.mapError(normalize("finish", started.effectId))),
  );

  const reconcile = Effect.fn("LegacyTerminalCleanupWorker.reconcile")(
    function* (effectId: string, attemptId: string, startedRevision: number) {
      // Reconcile persistence once, never the physical close. A surviving started row is
      // fenced as unknown using only this attempt/revision; another owner is never adopted.
      return yield* sql.withTransaction(
        Effect.gen(function* () {
          const current = yield* storage.readTerminalCleanup(effectId);
          if (current === undefined || current.state === "pending") return current;
          if (current.attemptId !== attemptId)
            return yield* new LegacyTerminalCleanupWorkerError({
              operation: "reconcile",
              reason: "attempt_changed",
              effectId,
            });
          if (current.state === "started" && current.revision === startedRevision) {
            const writer = yield* storage.forTransaction;
            const unknown = yield* writer.finishTerminalCleanup({
              effectId,
              attemptId,
              expectedRevision: startedRevision,
              outcome: "unconfirmed",
            });
            if (unknown === undefined)
              return yield* new LegacyTerminalCleanupWorkerError({
                operation: "reconcile",
                reason: "attempt_changed",
                effectId,
              });
            return unknown;
          }
          if (
            current.revision !== startedRevision + 1 ||
            (current.state !== "succeeded" && current.state !== "unknown")
          )
            return yield* new LegacyTerminalCleanupWorkerError({
              operation: "reconcile",
              reason: "attempt_changed",
              effectId,
            });
          return current;
        }),
      );
    },
    (effect, effectId) => effect.pipe(Effect.mapError(normalize("reconcile", effectId))),
  );

  const execute = Effect.fn("LegacyTerminalCleanupWorker.execute")(function* (
    pending: S.TerminalCleanup,
  ) {
    const attemptId = yield* random.uuidV4();
    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const claim = yield* sql
          .withTransaction(
            Effect.gen(function* () {
              const writer = yield* storage.forTransaction;
              return yield* writer.claimTerminalCleanup({
                effectId: pending.effectId,
                expectedRevision: pending.revision,
                attemptId,
              });
            }),
          )
          .pipe(Effect.exit);
        if (Exit.isFailure(claim)) {
          yield* reconcile(pending.effectId, attemptId, pending.revision + 1);
          return yield* new LegacyTerminalCleanupWorkerError({
            operation: "claim",
            reason: "claim_unconfirmed",
            effectId: pending.effectId,
            cause: claim.cause,
          });
        }
        const started = claim.value;
        if (started === undefined) return false;
        if (
          started.state !== "started" ||
          started.attemptId !== attemptId ||
          started.revision !== pending.revision + 1
        ) {
          yield* reconcile(pending.effectId, attemptId, pending.revision + 1);
          return yield* new LegacyTerminalCleanupWorkerError({
            operation: "claim",
            reason: "attempt_changed",
            effectId: pending.effectId,
          });
        }

        // withTransaction has returned: no SQL transaction/writer escapes into physical work.
        const closed = yield* restore(
          Effect.suspend(() =>
            terminals.close(
              {
                threadId: started.threadId,
                terminalId: started.control.terminalId,
                deleteHistory: true,
              },
              { legacyOwnedControl: started.control },
            ),
          ),
        ).pipe(Effect.exit);
        const failure = Exit.isFailure(closed)
          ? Option.getOrUndefined(Cause.findErrorOption(closed.cause))
          : undefined;
        const outcome = Exit.isSuccess(closed)
          ? "closed"
          : Cause.hasInterrupts(closed.cause)
            ? "interrupted"
            : isControlError(failure)
              ? "owner-unavailable"
              : "unconfirmed";
        const completion = yield* finish(started, outcome).pipe(Effect.exit);
        if (Exit.isFailure(completion)) {
          const retained = yield* reconcile(started.effectId, attemptId, started.revision);
          // An exact durable success can settle a lost commit reply, never a second close.
          if (
            Exit.isSuccess(closed) &&
            retained?.state === "succeeded" &&
            retained.outcome === "closed"
          )
            return true;
          if (Exit.isFailure(closed) && Cause.hasInterrupts(closed.cause))
            return yield* Effect.failCause(
              Cause.map(closed.cause, normalize("close", started.effectId)),
            );
          return yield* new LegacyTerminalCleanupWorkerError({
            operation: "finish",
            reason: "completion_unconfirmed",
            effectId: started.effectId,
            cause: completion.cause,
          });
        }
        if (Exit.isFailure(closed)) {
          if (Cause.hasInterrupts(closed.cause))
            return yield* Effect.failCause(
              Cause.map(closed.cause, normalize("close", started.effectId)),
            );
          return yield* new LegacyTerminalCleanupWorkerError({
            operation: "close",
            reason: isControlError(failure) ? "owner_unavailable" : "close_unconfirmed",
            effectId: started.effectId,
            cause: closed.cause,
          });
        }
        return true;
      }),
    );
  });

  const runBatch = Effect.fn("LegacyTerminalCleanupWorker.runBatch")(
    function* (limit: number) {
      if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1000)
        return yield* new LegacyTerminalCleanupWorkerError({
          operation: "entry",
          reason: "invalid_limit",
        });
      if (!(yield* tables("jones_legacy_bootstrap_terminal_cleanup")))
        return { selected: 0, claimed: 0, closed: 0, contended: 0, unresolved: 0 };
      // Storage's reconciliation listing includes started/unknown and has no cursor. Select
      // pending IDs only, so a full page of unresolved rows cannot starve executable work.
      const rows = yield* decodePending(
        yield* sql`SELECT effect_id FROM jones_legacy_bootstrap_terminal_cleanup WHERE state = 'pending' ORDER BY effect_id LIMIT ${limit}`,
      );
      let closed = 0;
      let contended = 0;
      for (const row of rows) {
        const pending = yield* storage.readTerminalCleanup(row.effect_id);
        if (pending === undefined || pending.state !== "pending") {
          contended++;
          continue;
        }
        if (yield* execute(pending)) closed++;
        else contended++;
      }
      const counts = yield* decodeCount(
        yield* sql`SELECT COUNT(*) AS count FROM jones_legacy_bootstrap_terminal_cleanup WHERE state IN ('started','unknown')`,
      );
      return {
        selected: rows.length,
        claimed: closed,
        closed,
        contended,
        unresolved: counts[0]!.count,
      };
    },
    Effect.mapError(normalize("list")),
  );

  const runOnce = Effect.fn("LegacyTerminalCleanupWorker.runOnce")(function* (limit = 100) {
    yield* entry;
    return yield* runBatch(limit).pipe(Effect.provideService(SqlClient.SqlClient, sql));
  });
  const drain = Effect.fn("LegacyTerminalCleanupWorker.drain")(function* (limit = 100) {
    yield* entry;
    let total = { selected: 0, claimed: 0, closed: 0, contended: 0, unresolved: 0 };
    while (true) {
      const result = yield* runBatch(limit).pipe(Effect.provideService(SqlClient.SqlClient, sql));
      total = {
        selected: total.selected + result.selected,
        claimed: total.claimed + result.claimed,
        closed: total.closed + result.closed,
        contended: total.contended + result.contended,
        unresolved: result.unresolved,
      };
      if (result.selected === 0) return total;
    }
  });
  return TerminalCleanupWorker.of({
    runOnce,
    drain,
    wake: Queue.offer(available, undefined).pipe(Effect.asVoid),
    awaitWork: Queue.take(available),
  });
});

export const layer = Layer.effect(TerminalCleanupWorker, make);

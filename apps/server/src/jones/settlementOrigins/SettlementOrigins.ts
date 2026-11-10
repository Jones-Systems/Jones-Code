import {
  SettlementProjectRemaining,
  type CommandId,
  type SettlementOrigin,
  type SettlementOriginQuery,
  type SettlementOriginsRequest,
  type SettlementOriginsResult,
  type ThreadId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";
import { forkParked } from "../../serverActivation.ts";
import { makeJonesTablePresence } from "../persistence/JonesTablePresence.ts";

export class SettlementOriginsReadError extends Schema.TaggedError<SettlementOriginsReadError>()(
  "SettlementOriginsReadError",
  { cause: Schema.Defect() },
) {
  override get message(): string {
    return "Failed to read settlement origins.";
  }
}

export class SettlementOriginsRetentionError extends Schema.TaggedError<SettlementOriginsRetentionError>()(
  "SettlementOriginsRetentionError",
  { cause: Schema.Defect() },
) {
  override get message(): string {
    return "Failed to prune expired settlement origin rows.";
  }
}

/** Rows older than this are deleted from both origin tables. */
export const SETTLEMENT_ORIGIN_RETENTION = Duration.days(30);
/**
 * A missing marker proves `other` only inside this window, so a pruned client marker reads
 * `unknown`, never `other`. One day under retention covers the marker-to-event lag.
 */
const MARKER_TRUST_WINDOW = Duration.days(29);
const PRUNE_BATCH_SIZE = 200;
const PRUNE_MAX_BATCHES = 25;
const PRUNE_INTERVAL = Duration.hours(6);

export class SettlementOrigins extends Context.Service<
  SettlementOrigins,
  {
    readonly recordClientSettle: (input: {
      readonly commandId: CommandId;
      readonly threadId: ThreadId;
    }) => Effect.Effect<void>;
    readonly resolve: (
      input: SettlementOriginsRequest,
    ) => Effect.Effect<SettlementOriginsResult, SettlementOriginsReadError>;
  }
>()("t3/jones/settlementOrigins/SettlementOrigins") {}

/**
 * Origin bookkeeping never blocks or fails the command it describes, so failures
 * and defects are logged and absorbed. Interruption is not a failure of this
 * work: it must reach the caller, or a cancelled dispatch would carry on.
 */
const absorbFailures =
  (message: string) =>
  <A, E, R>(self: Effect.Effect<A, E, R>): Effect.Effect<A | void, never, R> =>
    self.pipe(
      Effect.catchCauseIf(
        (cause) => !Cause.hasInterrupts(cause),
        (cause) => Effect.logWarning(message, { cause }),
      ),
      // A mixed failure-and-interruption cause stays interrupted; it never becomes a typed error.
      Effect.orDie,
    );

const decodeProjectRemaining = Schema.decodeUnknownOption(
  Schema.fromJsonString(SettlementProjectRemaining),
);

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const tables = makeJonesTablePresence(sql);
  const tablePresent = tables("jones_client_settlement_commands");
  const threadExists = (threadId: ThreadId) =>
    sql`SELECT 1 FROM orchestration_v2_projection_threads
      WHERE thread_id = ${threadId} AND deleted_at IS NULL`.pipe(
      Effect.map((rows) => rows.length === 1),
    );

  const recordClientSettle: SettlementOrigins["Service"]["recordClientSettle"] = (input) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          if (!(yield* tablePresent) || !(yield* threadExists(input.threadId))) return;
          // A socket retry of an already accepted agent command cannot change its origin.
          const receipts = yield* sql`SELECT 1 FROM orchestration_command_receipts
          WHERE command_id = ${input.commandId}`;
          if (receipts.length !== 0) return;
          const now = yield* DateTime.now;
          yield* sql`INSERT INTO jones_client_settlement_commands(command_id, thread_id, recorded_at)
          VALUES (${input.commandId}, ${input.threadId}, ${DateTime.formatIso(now)})
          ON CONFLICT(command_id) DO NOTHING`;
        }),
      )
      .pipe(absorbFailures("Could not record client settlement origin; continuing dispatch."));

  const resolveOne = Effect.fnUntraced(function* (
    query: SettlementOriginQuery,
  ): Effect.fn.Return<
    { origin: SettlementOrigin; projectRemaining?: readonly string[] },
    SqlError
  > {
    if (!(yield* threadExists(query.threadId))) return { origin: "unknown" };
    const trustedSinceMs =
      DateTime.toEpochMillis(yield* DateTime.now) - Duration.toMillis(MARKER_TRUST_WINDOW);
    let afterSequence = 0;
    // Walk forward on the existing thread/sequence index. A recent-event window
    // could mistake a later manual no-op for the transition that set this epoch.
    while (true) {
      const rows = yield* sql<{
        readonly sequence: number;
        readonly command_id: string | null;
        readonly occurred_at: string;
        readonly settled_at: string | null;
        readonly self_command_id: string | null;
      }>`SELECT sequence, command_id, occurred_at, json_extract(payload_json, '$.settledAt') AS settled_at, json_extract(payload_json, '$.selfSettlement.commandId') AS self_command_id
        FROM orchestration_events INDEXED BY idx_orchestration_events_agent_stream_sequence
        WHERE application_event_version = 2 AND aggregate_kind = 'thread'
          AND stream_id = ${query.threadId} AND event_type = 'thread.settled'
          AND sequence > ${afterSequence}
        ORDER BY sequence ASC LIMIT 64`;
      for (const row of rows) {
        const epoch = typeof row.settled_at === "string" ? Date.parse(row.settled_at) : NaN;
        if (!Number.isFinite(epoch)) return { origin: "unknown" };
        if (epoch !== query.settledAtMs) continue;
        if (row.command_id === null) return { origin: "unknown" };
        const markers = yield* sql<{ readonly thread_id: string }>`
          SELECT thread_id FROM jones_client_settlement_commands
          WHERE command_id = ${row.command_id}`;
        const marker = markers[0];
        // No marker means `other` only while a marker would still be retained, and
        // only when the event history that names the settlement is still readable.
        const origin =
          marker?.thread_id === query.threadId
            ? "client"
            : marker === undefined && !(Date.parse(row.occurred_at) >= trustedSinceMs)
              ? "unknown"
              : "other";
        if (
          row.self_command_id !== null &&
          row.command_id === `${row.self_command_id}:settle` &&
          (yield* tables("jones_self_settlement_project_items"))
        ) {
          const saved = yield* sql<{
            readonly items_json: string;
          }>`SELECT items_json FROM jones_self_settlement_project_items
            WHERE command_id = ${row.self_command_id} AND thread_id = ${query.threadId}`;
          if (saved[0] !== undefined) {
            const decoded = decodeProjectRemaining(saved[0].items_json);
            if (decoded._tag === "Some") return { origin, projectRemaining: decoded.value };
          }
        }
        return { origin };
      }
      if (rows.length < 64) return { origin: "unknown" };
      afterSequence = rows[rows.length - 1]!.sequence;
    }
  });

  const resolve: SettlementOrigins["Service"]["resolve"] = Effect.fn("SettlementOrigins.resolve")(
    function* (input) {
      if (!(yield* tablePresent)) {
        return { items: input.items.map((item) => ({ ...item, origin: "unknown" as const })) };
      }
      const items = yield* Effect.forEach(input.items, (item) =>
        resolveOne(item).pipe(Effect.map((resolved) => ({ ...item, ...resolved }))),
      );
      return { items };
    },
    Effect.mapError((cause) => new SettlementOriginsReadError({ cause })),
  );

  return SettlementOrigins.of({ recordClientSettle, resolve });
});

export const layer = Layer.effect(SettlementOrigins, make);

export interface SettlementOriginRetention {
  /** Deletes rows older than the retention window, a bounded number per call. */
  readonly pruneExpired: Effect.Effect<
    { readonly clientCommands: number; readonly projectItems: number },
    SettlementOriginsRetentionError
  >;
}

/**
 * Retention is separate from the service so the HTTP and WebSocket consumers of
 * `SettlementOrigins` see only recording and reading.
 */
export const makeRetention = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const tables = makeJonesTablePresence(sql);
  const pruneTable = Effect.fn("SettlementOrigins.pruneTable")(function* (
    table: "jones_client_settlement_commands" | "jones_self_settlement_project_items",
    cutoff: string,
  ) {
    if (!(yield* tables(table))) return 0;
    let deleted = 0;
    // Small batches keep each write short; the next pass continues where this one stopped.
    for (let batch = 0; batch < PRUNE_MAX_BATCHES; batch++) {
      const rows = yield* sql<{ readonly command_id: string }>`
        DELETE FROM ${sql(table)} WHERE command_id IN (
          SELECT command_id FROM ${sql(table)} WHERE recorded_at < ${cutoff}
          ORDER BY recorded_at LIMIT ${PRUNE_BATCH_SIZE}
        ) RETURNING command_id`;
      deleted += rows.length;
      if (rows.length < PRUNE_BATCH_SIZE) break;
      yield* Effect.yieldNow;
    }
    return deleted;
  });

  const pruneExpired: SettlementOriginRetention["pruneExpired"] = Effect.gen(function* () {
    const cutoff = DateTime.formatIso(
      DateTime.subtractDuration(yield* DateTime.now, SETTLEMENT_ORIGIN_RETENTION),
    );
    return {
      clientCommands: yield* pruneTable("jones_client_settlement_commands", cutoff),
      projectItems: yield* pruneTable("jones_self_settlement_project_items", cutoff),
    };
  }).pipe(Effect.mapError((cause) => new SettlementOriginsRetentionError({ cause })));

  return { pruneExpired } satisfies SettlementOriginRetention;
});

/**
 * Sweeps expired origin rows once the server is active and every six hours after.
 * Compose it once for the server lifetime: the service layer is also built per
 * connection, where a sweep of its own would be wasted. Retention is housekeeping,
 * so it never blocks startup and its failures are only logged.
 */
export const retentionLayer = Layer.effectDiscard(
  Effect.gen(function* () {
    const retention = yield* makeRetention;
    yield* forkParked(
      retention.pruneExpired.pipe(
        Effect.tap((pruned) =>
          pruned.clientCommands + pruned.projectItems > 0
            ? Effect.logInfo("Pruned expired settlement origin rows", pruned)
            : Effect.void,
        ),
        absorbFailures("Could not prune expired settlement origin rows."),
        Effect.repeat(Schedule.spaced(PRUNE_INTERVAL)),
      ),
    );
  }),
);

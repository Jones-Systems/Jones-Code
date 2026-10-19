import {
  type CommandId,
  type SettlementOrigin,
  type SettlementOriginQuery,
  type SettlementOriginsRequest,
  type SettlementOriginsResult,
  type ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";
import { makeJonesTablePresence } from "../persistence/JonesTablePresence.ts";

export class SettlementOriginsReadError extends Schema.TaggedError<SettlementOriginsReadError>()(
  "SettlementOriginsReadError",
  { cause: Schema.Defect() },
) {
  override get message(): string {
    return "Failed to read settlement origins.";
  }
}

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

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const tablePresent = makeJonesTablePresence(sql)("jones_client_settlement_commands");
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
      .pipe(
        Effect.catchCause(() =>
          Effect.logWarning("Could not record client settlement origin; continuing dispatch."),
        ),
      );

  const resolveOne = Effect.fnUntraced(function* (
    query: SettlementOriginQuery,
  ): Effect.fn.Return<SettlementOrigin, SqlError> {
    if (!(yield* threadExists(query.threadId))) return "unknown";
    let afterSequence = 0;
    // Walk forward on the existing thread/sequence index. A recent-event window
    // could mistake a later manual no-op for the transition that set this epoch.
    while (true) {
      const rows = yield* sql<{
        readonly sequence: number;
        readonly command_id: string | null;
        readonly settled_at: string | null;
      }>`SELECT sequence, command_id, json_extract(payload_json, '$.settledAt') AS settled_at
        FROM orchestration_events INDEXED BY idx_orchestration_events_agent_stream_sequence
        WHERE application_event_version = 2 AND aggregate_kind = 'thread'
          AND stream_id = ${query.threadId} AND event_type = 'thread.settled'
          AND sequence > ${afterSequence}
        ORDER BY sequence ASC LIMIT 64`;
      for (const row of rows) {
        const epoch = typeof row.settled_at === "string" ? Date.parse(row.settled_at) : NaN;
        if (!Number.isFinite(epoch)) return "unknown";
        if (epoch !== query.settledAtMs) continue;
        if (row.command_id === null) return "unknown";
        const markers = yield* sql<{ readonly thread_id: string }>`
          SELECT thread_id FROM jones_client_settlement_commands
          WHERE command_id = ${row.command_id}`;
        return markers[0]?.thread_id === query.threadId ? "client" : "other";
      }
      if (rows.length < 64) return "unknown";
      afterSequence = rows[rows.length - 1]!.sequence;
    }
  });

  const resolve: SettlementOrigins["Service"]["resolve"] = (input) =>
    Effect.gen(function* () {
      if (!(yield* tablePresent)) {
        return { items: input.items.map((item) => ({ ...item, origin: "unknown" as const })) };
      }
      const items = yield* Effect.forEach(input.items, (item) =>
        resolveOne(item).pipe(Effect.map((origin) => ({ ...item, origin }))),
      );
      return { items };
    }).pipe(Effect.mapError((cause) => new SettlementOriginsReadError({ cause })));

  return SettlementOrigins.of({ recordClientSettle, resolve });
});

export const layer = Layer.effect(SettlementOrigins, make);

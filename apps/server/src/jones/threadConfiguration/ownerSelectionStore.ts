import { CommandId, ModelSelection, NonNegativeInt, type ThreadId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/sql/SqlClient";
import type { JonesTablePresence } from "../persistence/JonesTablePresence.ts";
import { hasSelectionEvent, pendingOwnerSelection } from "./ownerSelection.ts";

interface Row {
  readonly command_id: string;
  readonly result_sequence: number;
  readonly model_selection_json: string;
  readonly recorded_at: string;
}
const encodeSelection = Schema.encodeSync(Schema.fromJsonString(ModelSelection));
const decode = Schema.decodeUnknownOption(
  Schema.Struct({
    command_id: CommandId,
    result_sequence: NonNegativeInt,
    model_selection_json: Schema.fromJsonString(ModelSelection),
    recorded_at: Schema.DateTimeUtcFromString,
  }),
);

export const makeOwnerSelectionStore = (sql: SqlClient.SqlClient, presence: JonesTablePresence) => {
  const present = presence("jones_owner_selections");
  const read = Effect.fn("OwnerSelectionStore.read")(function* (threadId: ThreadId) {
    if (!(yield* present)) return null;
    const rows =
      yield* sql<Row>`SELECT command_id, result_sequence, model_selection_json, recorded_at
      FROM jones_owner_selections WHERE thread_id = ${threadId}`;
    const row = rows[0];
    if (row === undefined) return null;
    const decoded = decode(row);
    return Option.isNone(decoded)
      ? null
      : {
          commandId: decoded.value.command_id,
          resultSequence: decoded.value.result_sequence,
          modelSelection: decoded.value.model_selection_json,
        };
  });
  const pending = Effect.fn("OwnerSelectionStore.pending")(function* (
    threadId: ThreadId,
    saved: ModelSelection,
  ) {
    return pendingOwnerSelection(yield* read(threadId), saved);
  });
  // Called only for a fresh receipt, inside the event/receipt/outbox transaction.
  const commit = Effect.fn("OwnerSelectionStore.commit")(function* (input: {
    readonly threadId: ThreadId;
    readonly commandId: CommandId;
    readonly sequence: number;
    readonly events: ReadonlyArray<{
      readonly type: string;
      readonly threadId?: ThreadId | undefined;
      readonly payload: unknown;
    }>;
    readonly record?: ModelSelection | undefined;
    readonly recordedAt: DateTime.Utc;
  }) {
    if (input.record === undefined && !hasSelectionEvent(input.events, input.threadId)) return;
    if (!(yield* present)) return;
    if (input.record !== undefined) {
      yield* sql`INSERT INTO jones_owner_selections
        (thread_id, command_id, result_sequence, model_selection_json, recorded_at)
        VALUES (${input.threadId}, ${input.commandId}, ${input.sequence},
          ${encodeSelection(input.record)}, ${DateTime.formatIso(input.recordedAt)})
        ON CONFLICT(thread_id) DO UPDATE SET command_id = excluded.command_id,
          result_sequence = excluded.result_sequence,
          model_selection_json = excluded.model_selection_json, recorded_at = excluded.recorded_at
        WHERE excluded.result_sequence > jones_owner_selections.result_sequence`;
    } else {
      yield* sql`DELETE FROM jones_owner_selections
        WHERE thread_id = ${input.threadId} AND result_sequence < ${input.sequence}`;
    }
  });
  return { read, pending, commit };
};

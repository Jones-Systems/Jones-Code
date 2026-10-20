import { type CommandId, type ThreadId, SettlementProjectRemaining } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/sql/SqlClient";
import type { JonesTablePresence } from "../persistence/JonesTablePresence.ts";

class SelfSettlementProjectItemsError extends Schema.TaggedError<SelfSettlementProjectItemsError>()(
  "SelfSettlementProjectItemsError",
  {
    reason: Schema.Literals(["acceptance_mismatch", "storage_unavailable"]),
  },
) {}

export interface SelfSettlementProjectRemaining {
  readonly commandId: CommandId;
  readonly threadId: ThreadId;
  readonly items: readonly string[];
}

const decodeItems = Schema.decodeEffect(SettlementProjectRemaining);
const encodeItems = Schema.encodeSync(Schema.fromJsonString(SettlementProjectRemaining));

// EventSink calls this inside its acceptance transaction, before publishing committed events.
export const recordSelfSettlementProjectRemaining = (
  sql: SqlClient.SqlClient,
  tablePresent: JonesTablePresence,
  input: {
    readonly commandId: CommandId;
    readonly threadId: ThreadId;
    readonly commandType: string;
    readonly acceptedAt: DateTime.Utc;
  },
  context: SelfSettlementProjectRemaining,
) =>
  Effect.gen(function* () {
    if (
      input.commandId !== context.commandId ||
      input.threadId !== context.threadId ||
      input.commandType !== "thread.metadata.update"
    )
      return yield* new SelfSettlementProjectItemsError({ reason: "acceptance_mismatch" });
    const items = yield* decodeItems(context.items);
    if (items.length === 0) return;
    if (!(yield* tablePresent("jones_self_settlement_project_items")))
      return yield* new SelfSettlementProjectItemsError({ reason: "storage_unavailable" });
    yield* sql`INSERT INTO jones_self_settlement_project_items (command_id, thread_id, items_json, recorded_at)
    VALUES (${context.commandId}, ${context.threadId}, ${encodeItems(items)}, ${DateTime.formatIso(input.acceptedAt)})`;
  });

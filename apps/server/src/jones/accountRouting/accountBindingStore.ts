import type { ModelSelection, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import type * as SqlClient from "effect/sql/SqlClient";

import type { JonesTablePresence } from "../persistence/JonesTablePresence.ts";
import type { AccountBinding } from "./accountBinding.ts";

class AccountBindingMismatch extends Data.TaggedError("AccountBindingMismatch")<{
  readonly message: string;
}> {}

export const makeAccountBindingStore = (sql: SqlClient.SqlClient, tables: JonesTablePresence) => {
  const available = tables("jones_auto_account_bindings");
  const read = Effect.fn("AccountBindingStore.read")(function* (
    threadId: ThreadId,
    instanceId: ProviderInstanceId,
  ) {
    if (!(yield* available)) return undefined;
    const rows = yield* sql<{ readonly account_digest: string }>`SELECT account_digest
      FROM jones_auto_account_bindings WHERE thread_id = ${threadId}
      AND provider_instance_id = ${instanceId}`;
    return rows[0]?.account_digest;
  });

  // Called inside the event, receipt and outbox transaction for the fresh creation.
  const commit = Effect.fn("AccountBindingStore.commit")(function* (
    binding: AccountBinding,
    creations: ReadonlyArray<{
      readonly threadId: ThreadId;
      readonly payload: { readonly modelSelection: ModelSelection };
    }>,
  ) {
    const creation = creations[0];
    if (
      creations.length !== 1 ||
      creation === undefined ||
      creation.payload.modelSelection.instanceId !== binding.instanceId
    ) {
      return yield* new AccountBindingMismatch({
        message: "Auto account binding does not match the created thread.",
      });
    }
    yield* sql`INSERT INTO jones_auto_account_bindings
      (thread_id, provider_instance_id, account_digest)
      VALUES (${creation.threadId}, ${binding.instanceId}, ${binding.digest})`;
  });
  return { read, commit };
};

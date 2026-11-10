import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import { nativeProcessCustodyDefinitions } from "../../nativeCreation/ProcessCustodySchema.ts";

// Fresh Jones-owned storage only; the transaction and ledger are owned by runJonesMigrations.
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  for (const definition of Object.values(nativeProcessCustodyDefinitions))
    yield* sql.unsafe(definition);
});

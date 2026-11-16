import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

// Settlement origin rows expire by age (SettlementOrigins.pruneExpired). Both
// tables are keyed by command, so the age sweep needs its own index on the
// Jones tables created by migrations 111 and 114. No upstream object changes.
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE INDEX jones_client_settlement_commands_by_recorded_at
    ON jones_client_settlement_commands(recorded_at)`;
  yield* sql`CREATE INDEX jones_self_settlement_project_items_by_recorded_at
    ON jones_self_settlement_project_items(recorded_at)`;
});

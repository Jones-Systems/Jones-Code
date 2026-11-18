import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`ALTER TABLE jones_personal_workstreams ADD COLUMN name_icon_project_key TEXT`;
  yield* sql`ALTER TABLE jones_personal_workstreams ADD COLUMN name_icon_updated_at TEXT`;
});

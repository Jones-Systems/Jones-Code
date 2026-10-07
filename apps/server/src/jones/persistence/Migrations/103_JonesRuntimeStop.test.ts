import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { SqlitePersistenceMemory } from "../../../persistence/Layers/Sqlite.ts";
it.effect(
  "runtime stop migration owns new tables without altering foreign rows and retained identities cannot be replaced",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TABLE foreign_runtime_history (value TEXT NOT NULL)`;
      yield* sql`INSERT INTO foreign_runtime_history VALUES ('untouched')`;
      yield* sql`INSERT INTO orchestration_command_receipts(command_id,aggregate_kind,aggregate_id,command_type,accepted_at,result_sequence,status,error) VALUES ('fixture-stop','thread','fixture-thread','provider-session.detach','2026-10-07T12:00:00Z',1,'accepted',NULL)`;
      yield* sql`INSERT INTO jones_runtime_stop_intents VALUES ('fixture-stop','fixture-thread','{}')`;
      yield* sql`INSERT INTO jones_runtime_stop_fences VALUES ('fixture-stop','fixture-thread','fixture-run','fixture-provider-thread','fixture-generation')`;
      yield* sql`INSERT INTO jones_runtime_stop_observations VALUES ('fixture-stop','started','unknown')`;
      for (const statement of [
        sql`UPDATE jones_runtime_stop_intents SET identity_json='{"changed":true}'`,
        sql`DELETE FROM jones_runtime_stop_fences`,
        sql`INSERT OR REPLACE INTO jones_runtime_stop_intents VALUES ('fixture-stop','replacement-thread','{}')`,
        sql`INSERT OR REPLACE INTO jones_runtime_stop_observations VALUES ('fixture-stop','started','stopped')`,
      ])
        assert.strictEqual((yield* statement.pipe(Effect.result))._tag, "Failure");
      assert.deepEqual(yield* sql`SELECT * FROM foreign_runtime_history`, [{ value: "untouched" }]);
      assert.deepEqual(yield* sql`SELECT phase,result FROM jones_runtime_stop_observations`, [
        { phase: "started", result: "unknown" },
      ]);
    }).pipe(Effect.provide(SqlitePersistenceMemory)),
);

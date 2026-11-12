import { expect, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import migration from "./126_JonesWorkstreamsSourceEnrollments.ts";

it.effect(
  "native ledger owns only Jones objects and is sparse, immutable and replacement safe",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TABLE auth_sessions(session_id TEXT PRIMARY KEY)`;
      yield* sql`INSERT INTO auth_sessions VALUES ('upstream-sentinel')`;
      yield* migration;
      expect(yield* sql`SELECT * FROM jones_workstreams_source_enrollments`).toHaveLength(0);
      expect(
        yield* sql`PRAGMA foreign_key_list(jones_workstreams_source_enrollments)`,
      ).toHaveLength(0);
      const envelope = JSON.stringify({
        v: 1,
        alg: "A256GCM",
        key_id: "synthetic",
        nonce: "a".repeat(16),
        ct: "",
        tag: "a".repeat(24),
      });
      yield* sql`INSERT INTO jones_workstreams_source_enrollments VALUES ('registry','owner','enrollment','session',${"a".repeat(64)},${envelope})`;
      for (const effect of [
        sql`DELETE FROM jones_workstreams_source_enrollments`,
        sql`UPDATE jones_workstreams_source_enrollments SET request_sha256 = ${"b".repeat(64)}`,
        sql`INSERT OR REPLACE INTO jones_workstreams_source_enrollments VALUES ('another','owner','enrollment','session',${"b".repeat(64)},${envelope})`,
        sql`INSERT INTO jones_workstreams_source_enrollments VALUES ('registry','owner','new-enrollment','session',${"b".repeat(64)},${envelope})`,
        sql`INSERT INTO jones_workstreams_source_enrollments VALUES ('registry','owner','bad','bad',${"a".repeat(64)},'{}')`,
      ])
        expect((yield* Effect.result(effect))._tag).toBe("Failure");
      expect(yield* sql`SELECT session_id FROM auth_sessions`).toEqual([
        { session_id: "upstream-sentinel" },
      ]);
      expect(yield* sql`SELECT enrollment_id FROM jones_workstreams_source_enrollments`).toEqual([
        { enrollment_id: "enrollment" },
      ]);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

import { expect, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import migration from "./128_JonesWorkstreamsPeerSessionPins.ts";
it.effect(
  "peer pins are sparse Jones-only rows, preserve issuance identity and enforce monotonic terminal transitions",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TABLE auth_sessions(session_id TEXT PRIMARY KEY, payload TEXT)`;
      yield* sql`INSERT INTO auth_sessions VALUES ('native-session','unchanged-upstream')`;
      yield* migration;
      expect(yield* sql`SELECT * FROM jones_workstreams_peer_session_pins`).toHaveLength(0);
      expect(yield* sql`PRAGMA foreign_key_list(jones_workstreams_peer_session_pins)`).toHaveLength(
        0,
      );
      const envelope = (ct: string) =>
        JSON.stringify({
          v: 1,
          alg: "A256GCM",
          key_id: "synthetic",
          nonce: "a".repeat(16),
          ct,
          tag: "a".repeat(24),
        });
      const first = envelope("YQ=="),
        second = envelope("Yg=="),
        third = envelope("Yw==");
      yield* sql`INSERT INTO jones_workstreams_peer_session_pins VALUES
      ('registry','owner','pin','native-session',1,'prepared','expiry','setup',${"a".repeat(64)},${first})`;
      for (const effect of [
        sql`DELETE FROM jones_workstreams_peer_session_pins`,
        sql`UPDATE jones_workstreams_peer_session_pins SET state='bound', revision=1, envelope=${second}`,
        sql`UPDATE jones_workstreams_peer_session_pins SET state='bound', revision=2, envelope=${second}, session_id='replacement'`,
        sql`UPDATE jones_workstreams_peer_session_pins SET state='bound', revision=2, envelope=${second}, owner_id='foreign-owner'`,
        sql`UPDATE jones_workstreams_peer_session_pins SET state='bound', revision=2, envelope=${second}, expires_at='extended'`,
        sql`INSERT OR REPLACE INTO jones_workstreams_peer_session_pins VALUES
        ('registry','owner','pin','native-session',1,'prepared','expiry','setup',${"a".repeat(64)},${first})`,
      ])
        expect((yield* Effect.result(effect))._tag).toBe("Failure");
      yield* sql`UPDATE jones_workstreams_peer_session_pins SET state='bound',revision=2,envelope=${second} WHERE pin_id='pin' AND revision=1`;
      expect(yield* sql`SELECT state, revision FROM jones_workstreams_peer_session_pins`).toEqual([
        { state: "bound", revision: 2 },
      ]);
      yield* sql`UPDATE jones_workstreams_peer_session_pins SET state='revoked',revision=3,envelope=${third} WHERE pin_id='pin' AND revision=2`;
      expect(
        (yield* Effect.result(
          sql`UPDATE jones_workstreams_peer_session_pins SET state='bound',revision=4,envelope=${second}`,
        ))._tag,
      ).toBe("Failure");
      expect(yield* sql`SELECT * FROM auth_sessions`).toEqual([
        { session_id: "native-session", payload: "unchanged-upstream" },
      ]);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

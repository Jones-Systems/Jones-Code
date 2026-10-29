import { expect, it } from "@effect/vitest";
import * as NodeSqlite from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import migration from "./130_JonesWorkstreamsForwardReceipts.ts";
const envelope = JSON.stringify({
  v: 1,
  alg: "A256GCM",
  key_id: "synthetic",
  nonce: "a".repeat(16),
  ct: "YQ==",
  tag: "a".repeat(22) + "==",
});
const row = {
  registry_instance_id: "11111111-1111-4111-8111-111111111111",
  owner_id: "owner",
  source_environment_id: "source",
  forward_id: "forward-00000000001",
  request_sha256: "a".repeat(64),
  identity_sha256: "b".repeat(64),
  wire_sha256: "c".repeat(64),
  state: "accepted",
  request_envelope: envelope,
  result_envelope: null,
};
it.effect(
  "Jones-only STRICT parent ledger preserves upstream schema and unresolved identity indefinitely",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TABLE auth_sessions(id TEXT PRIMARY KEY,payload TEXT)`;
      yield* sql`INSERT INTO auth_sessions VALUES ('upstream','unchanged')`;
      const before = yield* sql`SELECT sql FROM sqlite_master WHERE name='auth_sessions'`;
      yield* migration;
      expect(yield* sql`PRAGMA foreign_key_list(jones_workstreams_forward_receipts)`).toHaveLength(
        0,
      );
      expect(
        (yield* sql`PRAGMA table_list`).find(
          (r) => r.name === "jones_workstreams_forward_receipts",
        )!.strict,
      ).toBe(1);
      yield* sql`INSERT INTO jones_workstreams_forward_receipts ${sql.insert(row)}`;
      for (const effect of [
        sql`INSERT OR REPLACE INTO jones_workstreams_forward_receipts ${sql.insert(row)}`,
        sql`UPDATE jones_workstreams_forward_receipts SET request_sha256=${"d".repeat(64)}`,
        sql`UPDATE jones_workstreams_forward_receipts SET request_envelope='{}'`,
        sql`DELETE FROM jones_workstreams_forward_receipts`,
      ])
        expect((yield* Effect.result(effect))._tag).toBe("Failure");
      expect(yield* sql`SELECT sql FROM sqlite_master WHERE name='auth_sessions'`).toEqual(before);
      expect(yield* sql`SELECT * FROM auth_sessions`).toEqual([
        { id: "upstream", payload: "unchanged" },
      ]);
      expect(yield* sql`SELECT * FROM jones_workstreams_forward_receipts`).toEqual([row]);
    }).pipe(Effect.provide(NodeSqlite.layer({ filename: ":memory:" }))),
);
it.effect(
  "only one accepted-to-completed transition is allowed; outer failure rolls back both",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* migration;
      yield* sql`INSERT INTO jones_workstreams_forward_receipts ${sql.insert(row)}`;
      expect(
        (yield* Effect.result(
          sql.withTransaction(
            Effect.gen(function* () {
              yield* sql`UPDATE jones_workstreams_forward_receipts SET state='completed',result_envelope=${envelope}`;
              return yield* Effect.fail("synthetic rollback");
            }),
          ),
        ))._tag,
      ).toBe("Failure");
      expect(yield* sql`SELECT state FROM jones_workstreams_forward_receipts`).toEqual([
        { state: "accepted" },
      ]);
      yield* sql`UPDATE jones_workstreams_forward_receipts SET state='completed',result_envelope=${envelope}`;
      for (const effect of [
        sql`UPDATE jones_workstreams_forward_receipts SET state='accepted',result_envelope=NULL`,
        sql`UPDATE jones_workstreams_forward_receipts SET result_envelope=${envelope}`,
        sql`INSERT OR REPLACE INTO jones_workstreams_forward_receipts ${sql.insert(row)}`,
      ])
        expect((yield* Effect.result(effect))._tag).toBe("Failure");
    }).pipe(Effect.provide(NodeSqlite.layer({ filename: ":memory:" }))),
);
it.effect("rejects malformed digests, plaintext envelopes and incomplete completed rows", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* migration;
    for (const patch of [
      { request_sha256: "A".repeat(64) },
      { identity_sha256: "bad" },
      { wire_sha256: "f".repeat(63) },
      { request_envelope: "plaintext" },
      { state: "completed" },
      { result_envelope: envelope },
    ]) {
      expect(
        (yield* Effect.result(
          sql`INSERT INTO jones_workstreams_forward_receipts ${sql.insert({ ...row, ...patch })}`,
        ))._tag,
      ).toBe("Failure");
    }
  }).pipe(Effect.provide(NodeSqlite.layer({ filename: ":memory:" }))),
);

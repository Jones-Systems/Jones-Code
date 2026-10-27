// @effect-diagnostics nodeBuiltinImport:off -- Framework-boundary restart tests own and remove their synthetic SQLite directory with Node filesystem APIs.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { expect, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import migration from "./129_JonesWorkstreamsMemberBindings.ts";

const registry = "12345678-1234-1234-1234-123456789abc";
const envelope = (ct: string) =>
  JSON.stringify({
    v: 1,
    alg: "A256GCM",
    key_id: "synthetic",
    nonce: "a".repeat(16),
    ct,
    tag: "a".repeat(22) + "==",
  });
const first = envelope("YQ=="),
  second = envelope("Yg==");
const home = {
  registry_instance_id: registry,
  owner_id: "owner",
  bind_id: "bind",
  request_sha256: "a".repeat(64),
  home_pin_id: "home-pin",
  home_prepared_revision: 1,
  home_setup_request_id: "home-setup",
  home_setup_request_sha256: "b".repeat(64),
  request_envelope: first,
  result_envelope: second,
};
const member = {
  registry_instance_id: registry,
  owner_id: "owner",
  enrollment_id: "enrollment",
  session_id: "provider-session",
  setup_request_id: "member-setup",
  bind_id: "bind",
  request_sha256: "a".repeat(64),
  intent_sha256: "c".repeat(64),
  home_environment_id: "home",
  home_generation: 7,
  revision: 1,
  state: "prepared",
  envelope: first,
};

it.effect(
  "creates only two STRICT sparse encrypted ledgers with no upstream references or payload columns",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TABLE auth_sessions(session_id TEXT PRIMARY KEY, payload TEXT)`;
      yield* sql`INSERT INTO auth_sessions VALUES ('upstream-session','untouched')`;
      yield* migration;
      expect(
        (yield* sql`PRAGMA table_list`)
          .filter(
            (r) =>
              r.name === "jones_workstreams_home_bind_receipts" ||
              r.name === "jones_workstreams_member_enrollments",
          )
          .map((r) => r.strict),
      ).toEqual([1, 1]);
      for (const table of [
        "jones_workstreams_home_bind_receipts",
        "jones_workstreams_member_enrollments",
      ]) {
        expect(yield* sql.unsafe(`PRAGMA foreign_key_list(${table})`)).toHaveLength(0);
        expect(yield* sql.unsafe(`SELECT * FROM ${table}`)).toHaveLength(0);
        const columns = (yield* sql.unsafe<{ name: string }>(`PRAGMA table_info(${table})`)).map(
          (c) => c.name,
        );
        expect(columns).not.toContain("key_scope");
        expect(columns).not.toContain("grant_id");
        expect(columns).not.toContain("grant_version");
        expect(columns).not.toContain("request_body");
      }
      yield* sql`INSERT INTO jones_workstreams_home_bind_receipts ${sql.insert(home)}`;
      yield* sql`INSERT INTO jones_workstreams_member_enrollments ${sql.insert(member)}`;
      expect(yield* sql`SELECT * FROM auth_sessions`).toEqual([
        { session_id: "upstream-session", payload: "untouched" },
      ]);
      const indexes = yield* sql`PRAGMA index_list(jones_workstreams_member_enrollments)`;
      expect(indexes.filter((i) => i.unique === 1)).toHaveLength(5);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

it.effect(
  "reserves home bind and prepared setup identity even for REPLACE, and makes receipts immutable",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* migration;
      yield* sql`INSERT INTO jones_workstreams_home_bind_receipts ${sql.insert(home)}`;
      for (const effect of [
        sql`INSERT INTO jones_workstreams_home_bind_receipts ${sql.insert(home)}`,
        sql`INSERT OR REPLACE INTO jones_workstreams_home_bind_receipts ${sql.insert(home)}`,
        sql`INSERT OR REPLACE INTO jones_workstreams_home_bind_receipts ${sql.insert({ ...home, bind_id: "replacement" })}`,
        sql`INSERT INTO jones_workstreams_home_bind_receipts ${sql.insert({ ...home, bind_id: "different", home_pin_id: "different-pin" })}`,
        sql`INSERT INTO jones_workstreams_home_bind_receipts ${sql.insert({ ...home, bind_id: "different", home_setup_request_id: "different-setup" })}`,
        sql`UPDATE jones_workstreams_home_bind_receipts SET request_sha256=${"d".repeat(64)}`,
        sql`UPDATE jones_workstreams_home_bind_receipts SET request_envelope=${second}`,
        sql`UPDATE jones_workstreams_home_bind_receipts SET owner_id='different'`,
        sql`DELETE FROM jones_workstreams_home_bind_receipts`,
      ])
        expect((yield* Effect.result(effect))._tag).toBe("Failure");
      // Idempotency is lookup-first in H; duplicate SQL insertion must never replace history.
      expect(
        yield* sql`SELECT * FROM jones_workstreams_home_bind_receipts WHERE
      registry_instance_id=${registry} AND owner_id='owner' AND bind_id='bind'`,
      ).toEqual([home]);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

it.effect(
  "rejects invalid identity, digest, bounded version, envelope shape and direct bound insertion",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* migration;
      for (const patch of [
        { registry_instance_id: "registry" },
        { owner_id: "" },
        { bind_id: "bad/id" },
        { request_sha256: "A".repeat(64) },
        { home_setup_request_sha256: "bad" },
        { home_prepared_revision: 0 },
        { home_prepared_revision: 2 },
        { home_pin_id: "" },
        { home_setup_request_id: "" },
      ])
        expect(
          (yield* Effect.result(
            sql`INSERT INTO jones_workstreams_home_bind_receipts ${sql.insert({ ...home, ...patch })}`,
          ))._tag,
        ).toBe("Failure");
      const invalid = [
        "not-json",
        "null",
        "{}",
        JSON.stringify({ v: 1, alg: "A256GCM" }),
        JSON.stringify({
          v: 2,
          alg: "A256GCM",
          key_id: "key",
          nonce: "a".repeat(16),
          ct: "",
          tag: "a".repeat(24),
        }),
        JSON.stringify({
          v: 1,
          alg: "plain",
          key_id: "key",
          nonce: "a".repeat(16),
          ct: "",
          tag: "a".repeat(24),
        }),
        JSON.stringify({
          v: 1,
          alg: "A256GCM",
          key_id: "",
          nonce: "a".repeat(16),
          ct: "",
          tag: "a".repeat(24),
        }),
        JSON.stringify({
          v: 1,
          alg: "A256GCM",
          key_id: "key",
          nonce: "short",
          ct: "",
          tag: "a".repeat(24),
        }),
        JSON.stringify({
          v: 1,
          alg: "A256GCM",
          key_id: "key",
          nonce: "a".repeat(16),
          ct: 1,
          tag: "a".repeat(24),
        }),
        JSON.stringify({
          v: 1,
          alg: "A256GCM",
          key_id: "key",
          nonce: "a".repeat(16),
          ct: "",
          tag: "short",
        }),
      ];
      for (const value of invalid) {
        for (const patch of [{ request_envelope: value }, { result_envelope: value }])
          expect(
            (yield* Effect.result(
              sql`INSERT INTO jones_workstreams_home_bind_receipts ${sql.insert({ ...home, ...patch })}`,
            ))._tag,
          ).toBe("Failure");
        expect(
          (yield* Effect.result(
            sql`INSERT INTO jones_workstreams_member_enrollments ${sql.insert({ ...member, envelope: value })}`,
          ))._tag,
        ).toBe("Failure");
      }
      for (const patch of [
        { enrollment_id: "" },
        { session_id: "" },
        { setup_request_id: "" },
        { intent_sha256: "bad" },
        { request_sha256: "z".repeat(64) },
        { home_environment_id: "" },
        { home_generation: 0 },
        { home_generation: Number.MAX_SAFE_INTEGER + 1 },
        { revision: 0 },
        { revision: 2 },
        { state: "bound", revision: 2 },
        { state: "revoked" },
      ])
        expect(
          (yield* Effect.result(
            sql`INSERT INTO jones_workstreams_member_enrollments ${sql.insert({ ...member, ...patch })}`,
          ))._tag,
        ).toBe("Failure");
      expect(yield* sql`SELECT * FROM jones_workstreams_home_bind_receipts`).toHaveLength(0);
      expect(yield* sql`SELECT * FROM jones_workstreams_member_enrollments`).toHaveLength(0);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

it.effect(
  "enforces unique enrollment/session/setup and one immutable-selector revision successor",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* migration;
      yield* sql`INSERT INTO jones_workstreams_member_enrollments ${sql.insert(member)}`;
      for (const patch of [
        {},
        { enrollment_id: "other", bind_id: "other", setup_request_id: "other" },
        { enrollment_id: "other", bind_id: "other", session_id: "other" },
        { enrollment_id: "other", session_id: "other", setup_request_id: "other" },
      ])
        expect(
          (yield* Effect.result(
            sql`INSERT OR REPLACE INTO jones_workstreams_member_enrollments ${sql.insert({ ...member, ...patch })}`,
          ))._tag,
        ).toBe("Failure");
      for (const patch of [
        { registry_instance_id: "22345678-1234-1234-1234-123456789abc" },
        { owner_id: "other" },
        { enrollment_id: "other" },
        { session_id: "other" },
        { setup_request_id: "other" },
        { bind_id: "other" },
        { request_sha256: "d".repeat(64) },
        { intent_sha256: "d".repeat(64) },
        { home_environment_id: "other" },
        { home_generation: 8 },
        { revision: 1 },
        { revision: 3 },
        { state: "prepared" },
        { state: "revoked" },
        { envelope: first },
        { envelope: "{}" },
      ])
        expect(
          (yield* Effect.result(
            sql`UPDATE jones_workstreams_member_enrollments SET ${sql.update({
              state: "bound",
              revision: 2,
              envelope: second,
              ...patch,
            })}`,
          ))._tag,
        ).toBe("Failure");
      expect(
        (yield* Effect.result(sql`DELETE FROM jones_workstreams_member_enrollments`))._tag,
      ).toBe("Failure");
      const stale = yield* sql`UPDATE jones_workstreams_member_enrollments SET
      state='bound',revision=2,envelope=${second} WHERE enrollment_id='enrollment' AND revision=0 RETURNING enrollment_id`;
      expect(stale).toHaveLength(0); // M must check RETURNING cardinality; zero rows is a CAS conflict.
      const changed = yield* sql`UPDATE jones_workstreams_member_enrollments SET
      state='bound',revision=2,envelope=${second} WHERE enrollment_id='enrollment' AND revision=1 RETURNING enrollment_id`;
      expect(changed).toEqual([{ enrollment_id: "enrollment" }]);
      expect(yield* sql`SELECT * FROM jones_workstreams_member_enrollments`).toEqual([
        {
          ...member,
          revision: 2,
          state: "bound",
          envelope: second,
        },
      ]);
      for (const effect of [
        sql`UPDATE jones_workstreams_member_enrollments SET state='prepared',revision=3,envelope=${first}`,
        sql`UPDATE jones_workstreams_member_enrollments SET state='bound',revision=3,envelope=${first}`,
        sql`INSERT OR REPLACE INTO jones_workstreams_member_enrollments ${sql.insert(member)}`,
        sql`DELETE FROM jones_workstreams_member_enrollments`,
      ])
        expect((yield* Effect.result(effect))._tag).toBe("Failure");
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

it.effect(
  "rolls back all H receipt and M transition writes when their encompassing transaction fails",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* migration;
      // Synthetic Jones-owned stand-ins expose transaction rollback without inventing H/M stores.
      yield* sql`CREATE TABLE jones_test_transaction_facts (id TEXT PRIMARY KEY, version INTEGER) STRICT`;
      yield* sql`INSERT INTO jones_test_transaction_facts VALUES ('home-binding',0),('member-pin',1)`;
      yield* sql`INSERT INTO jones_workstreams_member_enrollments ${sql.insert(member)}`;
      const homeAttempt = sql.withTransaction(
        Effect.gen(function* () {
          yield* sql`UPDATE jones_test_transaction_facts SET version=1 WHERE id='home-binding'`;
          yield* sql`INSERT INTO jones_workstreams_home_bind_receipts ${sql.insert(home)}`;
          yield* sql`INSERT INTO jones_workstreams_home_bind_receipts ${sql.insert(home)}`;
        }),
      );
      expect((yield* Effect.result(homeAttempt))._tag).toBe("Failure");
      expect(yield* sql`SELECT * FROM jones_workstreams_home_bind_receipts`).toHaveLength(0);
      const memberAttempt = sql.withTransaction(
        Effect.gen(function* () {
          yield* sql`UPDATE jones_test_transaction_facts SET version=2 WHERE id='member-pin'`;
          yield* sql`UPDATE jones_workstreams_member_enrollments SET state='bound',revision=2,envelope=${second} WHERE enrollment_id='enrollment' AND revision=1`;
          yield* sql`DELETE FROM jones_workstreams_member_enrollments`;
        }),
      );
      expect((yield* Effect.result(memberAttempt))._tag).toBe("Failure");
      expect(
        yield* sql`SELECT revision,state,envelope FROM jones_workstreams_member_enrollments`,
      ).toEqual([
        {
          revision: 1,
          state: "prepared",
          envelope: first,
        },
      ]);
      expect(yield* sql`SELECT * FROM jones_test_transaction_facts ORDER BY id`).toEqual([
        { id: "home-binding", version: 0 },
        { id: "member-pin", version: 1 },
      ]);
      // A migration failure is also transactional and can be retried after rollback.
      expect((yield* Effect.result(sql.withTransaction(migration)))._tag).toBe("Failure");
      expect(yield* sql`SELECT * FROM jones_workstreams_member_enrollments`).toEqual([member]);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

it.effect(
  "preserves exact encrypted evidence and terminal triggers after closing and reopening SQLite",
  () =>
    Effect.acquireUseRelease(
      Effect.promise(() => NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "nw-j3f-ledger-"))),
      (directory) =>
        Effect.gen(function* () {
          const filename = NodePath.join(directory, "synthetic.sqlite");
          yield* Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient;
            yield* migration;
            yield* sql.withTransaction(
              Effect.gen(function* () {
                yield* sql`INSERT INTO jones_workstreams_home_bind_receipts ${sql.insert(home)}`;
                yield* sql`INSERT INTO jones_workstreams_member_enrollments ${sql.insert(member)}`;
              }),
            );
          }).pipe(Effect.provide(NodeSqliteClient.layer({ filename })), Effect.scoped);
          yield* Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient;
            expect(yield* sql`SELECT * FROM jones_workstreams_home_bind_receipts`).toEqual([home]);
            expect(yield* sql`SELECT * FROM jones_workstreams_member_enrollments`).toEqual([
              member,
            ]);
            yield* sql`UPDATE jones_workstreams_member_enrollments SET state='bound',revision=2,envelope=${second} WHERE enrollment_id='enrollment' AND revision=1`;
          }).pipe(Effect.provide(NodeSqliteClient.layer({ filename })), Effect.scoped);
          yield* Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient;
            expect(
              yield* sql`SELECT state,revision,envelope FROM jones_workstreams_member_enrollments`,
            ).toEqual([
              {
                state: "bound",
                revision: 2,
                envelope: second,
              },
            ]);
            expect(
              (yield* Effect.result(
                sql`INSERT OR REPLACE INTO jones_workstreams_home_bind_receipts ${sql.insert(home)}`,
              ))._tag,
            ).toBe("Failure");
            expect(
              (yield* Effect.result(
                sql`UPDATE jones_workstreams_member_enrollments SET state='bound',revision=3,envelope=${first}`,
              ))._tag,
            ).toBe("Failure");
          }).pipe(Effect.provide(NodeSqliteClient.layer({ filename })), Effect.scoped);
        }),
      (directory) => Effect.promise(() => NodeFSP.rm(directory, { recursive: true, force: true })),
    ),
);

it.effect("rolls back partial migration DDL before a clean retry", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TABLE jones_workstreams_member_enrollments (sentinel TEXT) STRICT`;
    const failed = yield* Effect.result(sql.withTransaction(migration));
    expect(failed._tag).toBe("Failure");
    expect(
      yield* sql`SELECT name FROM sqlite_schema WHERE name='jones_workstreams_home_bind_receipts'`,
    ).toHaveLength(0);
    expect(
      yield* sql`SELECT name FROM sqlite_schema WHERE type='trigger' AND name LIKE 'jones_workstreams_home_bind_receipts_%'`,
    ).toHaveLength(0);
    yield* sql`DROP TABLE jones_workstreams_member_enrollments`;
    yield* sql.withTransaction(migration);
    expect(yield* sql`SELECT * FROM jones_workstreams_home_bind_receipts`).toHaveLength(0);
    expect(yield* sql`SELECT * FROM jones_workstreams_member_enrollments`).toHaveLength(0);
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

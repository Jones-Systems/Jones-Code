// @effect-diagnostics nodeBuiltinImport:off -- Synthetic file-backed SQLite/WAL evidence stays in the task cache for root delivery.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Database from "./sql.ts";
import * as Secrets from "../../../../auth/ServerSecretStore.ts";
import * as SqlClient from "effect/sql/SqlClient";
import migration from "../../../persistence/Migrations/126_JonesWorkstreamsSourceEnrollments.ts";
import * as Records from "./sourceEnrollmentStore.ts";
import { seed, scope, testLayer, secrets, keyBytes } from "./sql.testkit.ts";

const layer = Records.layer.pipe(Layer.provideMerge(testLayer()));
it.effect(
  "encrypted issuance proof roundtrips and rejects identity collisions without mutation",
  () =>
    Effect.gen(function* () {
      yield* seed;
      yield* migration;
      const records = yield* Records.SourceEnrollmentStore,
        sql = yield* SqlClient.SqlClient;
      const record = {
        ...scope,
        enrollment_id: "enrollment",
        session_id: "session",
        request_sha256: "a".repeat(64),
        proof: { protected: "SOURCE-PROOF-SENTINEL" },
      };
      expect(yield* records.read({ session_id: "session" })).toBeNull();
      yield* records.insert(record);
      expect(yield* records.read({ enrollment_id: "enrollment" })).toEqual(record);
      expect(yield* records.read({ session_id: "session" })).toEqual(record);
      expect(
        JSON.stringify(yield* sql`SELECT * FROM jones_workstreams_source_enrollments`),
      ).not.toContain("SOURCE-PROOF-SENTINEL");
      expect((yield* Effect.result(records.insert({ ...record, proof: {} })))._tag).toBe("Failure");
      expect(
        (yield* Effect.result(records.insert({ ...record, enrollment_id: "other" })))._tag,
      ).toBe("Failure");
      expect(yield* records.read({ session_id: "session" })).toEqual(record);
    }).pipe(Effect.provide(layer)),
);

it.effect("owner-bound encryption refuses cross-owner envelope substitution", () =>
  Effect.gen(function* () {
    yield* seed;
    yield* migration;
    const records = yield* Records.SourceEnrollmentStore;
    const original = {
      ...scope,
      enrollment_id: "first",
      session_id: "session1",
      request_sha256: "a".repeat(64),
      proof: { private: "proof" },
    };
    yield* records.insert(original);
    const sql = yield* SqlClient.SqlClient;
    const row = (yield* sql<{
      envelope: string;
    }>`SELECT envelope FROM jones_workstreams_source_enrollments`)[0]!;
    yield* sql`INSERT INTO jones_workstreams_source_enrollments VALUES (${scope.registry_instance_id},'different-owner','second','session2',${"b".repeat(64)},${row.envelope})`;
    expect((yield* Effect.result(records.read({ session_id: "session2" })))._tag).toBe("Failure");
  }).pipe(Effect.provide(layer)),
);

it.effect("encrypted native proof sentinel is absent from file-backed SQLite and WAL", () =>
  Effect.gen(function* () {
    const parent = process.env.NW_CALLER_PREPARATION_EVIDENCE_DIR ?? NodeOS.tmpdir();
    NodeFS.mkdirSync(parent, { recursive: true });
    const directory = NodeFS.mkdtempSync(NodePath.join(parent, "source-enrollment-"));
    const filename = directory + "/proof.sqlite";
    yield* Effect.gen(function* () {
      yield* seed;
      yield* migration;
      const records = yield* Records.SourceEnrollmentStore,
        sql = yield* SqlClient.SqlClient;
      yield* sql`PRAGMA journal_mode = WAL`;
      yield* records.insert({
        ...scope,
        enrollment_id: "file-proof",
        session_id: "file-session",
        request_sha256: "a".repeat(64),
        proof: { protected: "NW-SOURCE-ENCRYPTION-SENTINEL" },
      });
      for (const file of [filename, filename + "-wal"]) {
        expect(NodeFS.existsSync(file)).toBe(true);
        expect(
          NodeFS.readFileSync(file).includes(Buffer.from("NW-SOURCE-ENCRYPTION-SENTINEL")),
        ).toBe(false);
      }
    }).pipe(Effect.provide(Records.layer.pipe(Layer.provideMerge(testLayer({ filename })))));
    yield* Effect.gen(function* () {
      const records = yield* Records.SourceEnrollmentStore;
      yield* records.withPreparedRead({ session_id: "file-session" }, (read) =>
        Effect.gen(function* () {
          expect((yield* read({ session_id: "file-session" }))?.proof).toEqual({
            protected: "NW-SOURCE-ENCRYPTION-SENTINEL",
          });
        }),
      );
    }).pipe(Effect.provide(Records.layer.pipe(Layer.provideMerge(testLayer({ filename })))));
  }),
);

const fixture = {
  ...scope,
  enrollment_id: "prepared-enrollment",
  session_id: "prepared-session",
  request_sha256: "a".repeat(64),
  proof: { protected: "original" },
};
const envelopeContext = {
  ...scope,
  purpose: "native_source_enrollment",
  record_id: fixture.enrollment_id,
  revision: 1,
};
const observedSecrets = (get: Secrets.ServerSecretStore["Service"]["get"]) =>
  Secrets.ServerSecretStore.of({
    get,
    set: () => Effect.die("Unexpected secret write"),
    create: () => Effect.die("Unexpected secret creation"),
    getOrCreateRandom: () => Effect.die("Unexpected automatic key creation"),
    remove: () => Effect.die("Unexpected key removal"),
  });

it.effect(
  "blocked real key acquisition releases selection before an unrelated same-client writer",
  () =>
    Effect.gen(function* () {
      const waiting = yield* Deferred.make<void>();
      const resume = yield* Deferred.make<void>();
      const values = secrets();
      let block = false;
      let calls = 0;
      let appSql: SqlClient.SqlClient | undefined;
      const secretStore = observedSecrets((name) =>
        Effect.gen(function* () {
          calls++;
          if (appSql)
            expect(Option.isNone(yield* Effect.serviceOption(appSql.transactionService))).toBe(
              true,
            );
          if (block) {
            yield* Deferred.succeed(waiting, undefined);
            yield* Deferred.await(resume);
          }
          return Option.fromNullishOr(values.get(name));
        }),
      );
      yield* Effect.gen(function* () {
        yield* seed;
        yield* migration;
        const records = yield* Records.SourceEnrollmentStore;
        const sql = yield* SqlClient.SqlClient;
        yield* records.insert(fixture);
        appSql = sql;
        yield* sql`CREATE TABLE prepared_reader_probe(value TEXT NOT NULL)`;
        block = true;
        const before = calls;
        const prepared = yield* records
          .withPreparedRead({ session_id: fixture.session_id }, (read) =>
            sql.withTransaction(
              Effect.gen(function* () {
                expect(Option.isSome(yield* Effect.serviceOption(sql.transactionService))).toBe(
                  true,
                );
                expect(yield* read({ session_id: fixture.session_id })).toEqual(fixture);
                expect(calls).toBe(before + 1);
              }),
            ),
          )
          .pipe(Effect.forkChild);
        yield* Deferred.await(waiting);
        yield* sql.withTransaction(
          sql`INSERT INTO prepared_reader_probe VALUES ('committed-during-key-wait')`,
        );
        expect(yield* sql<{ value: string }>`SELECT value FROM prepared_reader_probe`).toEqual([
          { value: "committed-during-key-wait" },
        ]);
        expect(
          yield* sql<{
            count: number;
          }>`SELECT COUNT(*) AS count FROM jones_workstreams_source_enrollments`,
        ).toEqual([{ count: 1 }]);
        yield* Deferred.succeed(resume, undefined);
        yield* Fiber.join(prepared);
      }).pipe(
        Effect.provide(Records.layer.pipe(Layer.provideMerge(testLayer({ secrets: secretStore })))),
      );
    }),
);

it.effect("ambient preparation refuses before SQL selection or secret access", () =>
  Effect.gen(function* () {
    let accesses = 0;
    const secretStore = observedSecrets(() => {
      accesses++;
      return Effect.die("Ambient secret access");
    });
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const records = yield* Records.SourceEnrollmentStore;
      // The table intentionally does not exist: selecting first would fail with SqlError.
      const result = yield* sql.withTransaction(
        Effect.result(
          records.withPreparedRead({ session_id: "absent" }, () =>
            Effect.die("Unexpected callback"),
          ),
        ),
      );
      expect(result).toMatchObject({
        _tag: "Failure",
        failure: { _tag: "WorkstreamsStoreError", reason: "fenced" },
      });
      expect(accesses).toBe(0);
    }).pipe(
      Effect.provide(Records.layer.pipe(Layer.provideMerge(testLayer({ secrets: secretStore })))),
    );
  }),
);

it.effect(
  "wrong SQL, another selector and escaped or copied readers cannot acquire or revive access",
  () =>
    Effect.gen(function* () {
      yield* seed;
      yield* migration;
      const records = yield* Records.SourceEnrollmentStore;
      const sql = yield* SqlClient.SqlClient;
      yield* records.insert(fixture);
      const escaped = yield* records.withPreparedRead({ session_id: fixture.session_id }, (read) =>
        Effect.gen(function* () {
          expect(yield* read({ session_id: fixture.session_id })).toEqual(fixture);
          expect(
            yield* Effect.result(read({ enrollment_id: fixture.enrollment_id })),
          ).toMatchObject({
            _tag: "Failure",
            failure: { reason: "fenced" },
          });
          yield* Effect.gen(function* () {
            const other = yield* SqlClient.SqlClient;
            expect(other).not.toBe(sql);
            expect(yield* Effect.result(read({ session_id: fixture.session_id }))).toMatchObject({
              _tag: "Failure",
              failure: { reason: "fenced" },
            });
            expect(
              yield* Effect.result(
                records.withPreparedRead({ session_id: fixture.session_id }, () =>
                  Effect.succeed(null),
                ),
              ),
            ).toMatchObject({
              _tag: "Failure",
              failure: { reason: "fenced" },
            });
          }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" })));
          return { read, copied: (...args: Parameters<typeof read>) => read(...args) };
        }),
      );
      for (const read of [escaped.read, escaped.copied])
        expect(yield* Effect.result(read({ session_id: fixture.session_id }))).toMatchObject({
          _tag: "Failure",
          failure: { reason: "fenced" },
        });
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "current same-key ciphertext is freshly authenticated; new keys never refill the cache",
  () =>
    Effect.gen(function* () {
      const values = secrets();
      values.set(
        "workstreams-content-key-second",
        Uint8Array.from(keyBytes, (n) => 255 - n),
      );
      let reads = 0;
      const secretStore = observedSecrets((name) =>
        Effect.sync(() => {
          reads++;
          return Option.fromNullishOr(values.get(name));
        }),
      );
      yield* Effect.gen(function* () {
        yield* seed;
        yield* migration;
        const records = yield* Records.SourceEnrollmentStore;
        const db = yield* Database.NativeSql;
        const sql = yield* SqlClient.SqlClient;
        yield* records.insert(fixture);
        const changed = { protected: "changed-current-proof" };
        const sameKey = yield* db.sealJson(envelopeContext, changed);
        yield* sql`UPDATE jones_workstreams_keys SET state = 'retired' WHERE key_id = 'test-home'`;
        yield* sql`INSERT INTO jones_workstreams_keys VALUES ('second',${scope.registry_instance_id},'home','active','2026-01-01')`;
        yield* sql`UPDATE jones_workstreams_registries SET active_key_id = 'second', registry_version = registry_version + 1`;
        const newKey = yield* db.sealJson(envelopeContext, { protected: "new-key" });
        // Corruption/race injection only: production issuance rows remain immutable.
        yield* sql`DROP TRIGGER jones_workstreams_source_enrollments_no_update`;
        // Historical test-home is now inactive, but the row still references that raw key.
        const before = reads;
        yield* records.withPreparedRead({ session_id: fixture.session_id }, (read) =>
          sql.withTransaction(
            Effect.gen(function* () {
              yield* sql`UPDATE jones_workstreams_source_enrollments SET envelope = ${sameKey} WHERE session_id = ${fixture.session_id}`;
              expect(yield* read({ session_id: fixture.session_id })).toEqual({
                ...fixture,
                proof: changed,
              });
              yield* sql`UPDATE jones_workstreams_source_enrollments SET envelope = ${newKey} WHERE session_id = ${fixture.session_id}`;
              expect(yield* Effect.result(read({ session_id: fixture.session_id }))).toMatchObject({
                _tag: "Failure",
                failure: { _tag: "KeyUnavailable", reason: "missing" },
              });
              expect(reads).toBe(before + 1);
              yield* sql`UPDATE jones_workstreams_source_enrollments SET envelope = ${sameKey}, enrollment_id = 'aad-substitution' WHERE session_id = ${fixture.session_id}`;
              expect(yield* Effect.result(read({ session_id: fixture.session_id }))).toMatchObject({
                _tag: "Failure",
                failure: { _tag: "EnvelopeError", reason: "authentication_failed" },
              });
            }),
          ),
        );
      }).pipe(
        Effect.provide(Records.layer.pipe(Layer.provideMerge(testLayer({ secrets: secretStore })))),
      );
    }),
);

it.effect("an advisory miss cannot hide a concurrent row or acquire its unprepared key", () =>
  Effect.gen(function* () {
    yield* seed;
    yield* migration;
    const records = yield* Records.SourceEnrollmentStore;
    const db = yield* Database.NativeSql;
    const sql = yield* SqlClient.SqlClient;
    const envelope = yield* db.sealJson(envelopeContext, fixture.proof);
    yield* records.withPreparedRead({ session_id: fixture.session_id }, (read) =>
      sql.withTransaction(
        Effect.gen(function* () {
          yield* sql`INSERT INTO jones_workstreams_source_enrollments ${sql.insert({ registry_instance_id: fixture.registry_instance_id, owner_id: fixture.owner_id, enrollment_id: fixture.enrollment_id, session_id: fixture.session_id, request_sha256: fixture.request_sha256, envelope })}`;
          expect(yield* Effect.result(read({ session_id: fixture.session_id }))).toMatchObject({
            _tag: "Failure",
            failure: { _tag: "KeyUnavailable", reason: "missing" },
          });
        }),
      ),
    );
  }).pipe(Effect.provide(layer)),
);

it.effect(
  "a missing original key remains missing without retry and outer abort rolls back an observed inner mutation",
  () =>
    Effect.gen(function* () {
      const values = secrets();
      yield* Effect.gen(function* () {
        yield* seed;
        yield* migration;
        const records = yield* Records.SourceEnrollmentStore;
        const sql = yield* SqlClient.SqlClient;
        yield* records.insert(fixture);
        values.delete("workstreams-content-key-test-home");
        yield* records.withPreparedRead({ session_id: fixture.session_id }, (read) =>
          Effect.gen(function* () {
            values.set("workstreams-content-key-test-home", keyBytes);
            expect(yield* Effect.result(read({ session_id: fixture.session_id }))).toMatchObject({
              _tag: "Failure",
              failure: { _tag: "KeyUnavailable", reason: "missing" },
            });
          }),
        );
        yield* sql`CREATE TABLE prepared_reader_probe(value TEXT NOT NULL)`;
        let observed = false;
        const result = yield* Effect.result(
          records.withPreparedRead({ session_id: fixture.session_id }, (read) =>
            sql.withTransaction(
              Effect.gen(function* () {
                expect(yield* read({ session_id: fixture.session_id })).toEqual(fixture);
                yield* sql`INSERT INTO prepared_reader_probe VALUES ('observed-inner-mutation')`;
                expect(
                  yield* sql<{ value: string }>`SELECT value FROM prepared_reader_probe`,
                ).toEqual([{ value: "observed-inner-mutation" }]);
                observed = true;
                return yield* new Database.StoreError({ reason: "conflict" });
              }),
            ),
          ),
        );
        expect(observed).toBe(true);
        expect(yield* sql`SELECT value FROM prepared_reader_probe`).toEqual([]);
        expect(result).toMatchObject({ _tag: "Failure", failure: { reason: "conflict" } });
        expect(yield* records.read({ session_id: fixture.session_id })).toEqual(fixture);
      }).pipe(Effect.provide(Records.layer.pipe(Layer.provideMerge(testLayer({ keys: values })))));
    }),
);

it.effect("cancellation rolls back the owned writer before invalidating the escaped reader", () =>
  Effect.gen(function* () {
    yield* seed;
    yield* migration;
    const records = yield* Records.SourceEnrollmentStore;
    const sql = yield* SqlClient.SqlClient;
    yield* records.insert(fixture);
    yield* sql`CREATE TABLE prepared_reader_probe(value TEXT NOT NULL)`;
    const entered = yield* Deferred.make<void>();
    const never = yield* Deferred.make<void>();
    let escaped: Records.SourceEnrollmentStore["Service"]["read"] | undefined;
    const writer = yield* records
      .withPreparedRead({ session_id: fixture.session_id }, (read) => {
        escaped = read;
        return sql.withTransaction(
          Effect.gen(function* () {
            expect(yield* read({ session_id: fixture.session_id })).toEqual(fixture);
            yield* sql`INSERT INTO prepared_reader_probe VALUES ('visible-before-interrupt')`;
            expect(yield* sql<{ value: string }>`SELECT value FROM prepared_reader_probe`).toEqual([
              { value: "visible-before-interrupt" },
            ]);
            yield* Deferred.succeed(entered, undefined);
            yield* Deferred.await(never);
          }),
        );
      })
      .pipe(Effect.forkChild);
    yield* Deferred.await(entered);
    yield* Fiber.interrupt(writer);
    expect(yield* sql`SELECT value FROM prepared_reader_probe`).toEqual([]);
    if (!escaped) return yield* Effect.die("Prepared callback was not entered");
    expect(yield* Effect.result(escaped({ session_id: fixture.session_id }))).toMatchObject({
      _tag: "Failure",
      failure: { reason: "fenced" },
    });
    yield* sql.withTransaction(sql`INSERT INTO prepared_reader_probe VALUES ('writer-released')`);
    expect(yield* sql<{ value: string }>`SELECT value FROM prepared_reader_probe`).toEqual([
      { value: "writer-released" },
    ]);
  }).pipe(Effect.provide(layer)),
);

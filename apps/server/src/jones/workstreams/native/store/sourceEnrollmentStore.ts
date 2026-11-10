import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Envelope from "../crypto/envelope.ts";
import * as Keys from "../crypto/keyProvider.ts";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";
import * as Database from "./sql.ts";

export interface SourceEnrollmentRecord extends Database.OwnerScope {
  readonly enrollment_id: string;
  readonly session_id: string;
  readonly request_sha256: string;
  readonly proof: unknown;
}
type Failure = SqlError | Database.StoreError | Database.CryptoError;
type Selector = { readonly enrollment_id: string } | { readonly session_id: string };
type Read = (selector: Selector) => Effect.Effect<SourceEnrollmentRecord | null, Failure>;
export class SourceEnrollmentStore extends Context.Service<
  SourceEnrollmentStore,
  {
    readonly read: Read;
    readonly withPreparedRead: <A, E, R>(
      selector: Selector,
      use: (read: Read) => Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | Failure, R>;
    readonly insert: (record: SourceEnrollmentRecord) => Effect.Effect<void, Failure>;
  }
>()("t3/jones/workstreams/native/store/sourceEnrollmentStore") {}
const context = (
  record: Omit<SourceEnrollmentRecord, "proof" | "session_id" | "request_sha256">,
) => ({
  registry_instance_id: record.registry_instance_id,
  owner_id: record.owner_id,
  purpose: "native_source_enrollment",
  record_id: record.enrollment_id,
  revision: 1,
});
const select = (sql: SqlClient.SqlClient, selector: Selector) =>
  sql<
    Omit<SourceEnrollmentRecord, "proof"> & { envelope: string }
  >`SELECT * FROM jones_workstreams_source_enrollments WHERE ${"session_id" in selector ? sql`session_id = ${selector.session_id}` : sql`enrollment_id = ${selector.enrollment_id}`}`;
const makeRead = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient,
    db = yield* Database.NativeSql;
  return Effect.fn("SourceEnrollmentStore.read")(function* (selector: Selector) {
    const row = (yield* select(sql, selector))[0];
    if (!row) return null;
    const proof = yield* db.openJson(context(row), row.envelope);
    return {
      registry_instance_id: row.registry_instance_id,
      owner_id: row.owner_id,
      enrollment_id: row.enrollment_id,
      session_id: row.session_id,
      request_sha256: row.request_sha256,
      proof,
    };
  }) satisfies Read;
});
const decodeKeyId = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.Struct({ key_id: Schema.String })),
);
const selectorId = (selector: Selector) =>
  JSON.stringify(
    "session_id" in selector
      ? ["session_id", selector.session_id]
      : ["enrollment_id", selector.enrollment_id],
  );
const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient,
    db = yield* Database.NativeSql,
    keys = yield* Effect.serviceOption(Keys.WorkstreamsKeyProvider),
    read = yield* makeRead;
  const withPreparedRead: SourceEnrollmentStore["Service"]["withPreparedRead"] = Effect.fn(
    "SourceEnrollmentStore.withPreparedRead",
  )(function* <A, E, R>(selector: Selector, use: (read: Read) => Effect.Effect<A, E, R>) {
    // Refuse the actual client transaction before selection or key acquisition.
    if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
      return yield* new Database.StoreError({ reason: "fenced" });
    const selectedClient = yield* Effect.serviceOption(SqlClient.SqlClient);
    if (Option.isSome(selectedClient) && selectedClient.value !== sql)
      return yield* new Database.StoreError({ reason: "fenced" });
    if (Option.isNone(keys)) return yield* new Keys.KeyUnavailable({ reason: "store_unavailable" });
    const selectedId = selectorId(selector);
    const material = new Map<string, Result.Result<Uint8Array, Keys.KeyUnavailable>>();
    let live = true;
    const check = Effect.fn("SourceEnrollmentStore.checkPreparedRead")(function* () {
      const current = yield* Effect.serviceOption(SqlClient.SqlClient);
      if (!live || Option.isNone(current) || current.value !== sql)
        return yield* new Database.StoreError({ reason: "fenced" });
    });
    const unavailable = () => new Keys.KeyUnavailable({ reason: "missing" });
    const cached = Layer.succeed(Keys.WorkstreamsKeyProvider, {
      activeKeyId: () => Effect.fail(unavailable()),
      indexKey: () => Effect.fail(unavailable()),
      get: (id) =>
        Effect.suspend(() => {
          const result = live ? material.get(id) : undefined;
          if (!result) return Effect.fail(unavailable());
          return Result.isFailure(result)
            ? Effect.fail(result.failure)
            : Effect.succeed(Uint8Array.from(result.success));
        }),
    });
    const graph = Layer.fresh(Database.layer).pipe(
      Layer.provide(Layer.fresh(Envelope.layer).pipe(Layer.provide(cached))),
      Layer.provideMerge(Layer.succeed(SqlClient.SqlClient, sql)),
    );
    return yield* Effect.gen(function* () {
      // The advisory statement releases its connection before any secret wait.
      const row = (yield* select(sql, selector))[0];
      if (row) {
        const envelope = yield* decodeKeyId(row.envelope).pipe(
          Effect.mapError(() => new Envelope.EnvelopeError({ reason: "malformed" })),
        );
        // Historical envelopes name their raw key; active-key selection is irrelevant here.
        material.set(
          envelope.key_id,
          yield* keys.value.get(envelope.key_id).pipe(
            Effect.map((bytes) => Uint8Array.from(bytes)),
            Effect.result,
          ),
        );
      }
      const freshRead = yield* makeRead.pipe(Effect.provide(graph));
      const preparedRead: Read = Effect.fn("SourceEnrollmentStore.preparedRead")(function* (next) {
        yield* check();
        if (selectorId(next) !== selectedId || Reflect.ownKeys(next).length !== 1)
          return yield* new Database.StoreError({ reason: "fenced" });
        // Reselect even after an advisory miss: concurrent insertion cannot become absence.
        const value = yield* freshRead(next);
        yield* check();
        return value;
      });
      return yield* use(preparedRead).pipe(
        Effect.awaitAllChildren,
        Effect.provideService(SqlClient.SqlClient, sql),
      );
    }).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          live = false;
          for (const value of material.values()) if (Result.isSuccess(value)) value.success.fill(0);
          material.clear();
        }),
      ),
    );
  });
  const insert = Effect.fn("SourceEnrollmentStore.insert")((record: SourceEnrollmentRecord) =>
    db.transaction(
      record,
      Effect.gen(function* () {
        if (
          !record.enrollment_id ||
          !record.session_id ||
          !/^[a-f0-9]{64}$/.test(record.request_sha256)
        )
          return yield* new Database.StoreError({ reason: "invalid_payload" });
        const byId = yield* read({ enrollment_id: record.enrollment_id });
        const bySession = yield* read({ session_id: record.session_id });
        if (byId || bySession)
          return yield* new Database.StoreError({ reason: "idempotency_conflict" });
        const envelope = yield* db.sealJson(context(record), record.proof);
        yield* sql`INSERT INTO jones_workstreams_source_enrollments ${sql.insert({
          registry_instance_id: record.registry_instance_id,
          owner_id: record.owner_id,
          enrollment_id: record.enrollment_id,
          session_id: record.session_id,
          request_sha256: record.request_sha256,
          envelope,
        })}`;
      }),
    ),
  );
  return SourceEnrollmentStore.of({ read, insert, withPreparedRead });
});
/** @public Canonical service construction. */
export const layer = Layer.effect(SourceEnrollmentStore, make);

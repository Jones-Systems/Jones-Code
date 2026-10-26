import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
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
export class SourceEnrollmentStore extends Context.Service<
  SourceEnrollmentStore,
  {
    readonly read: (
      selector: { readonly enrollment_id: string } | { readonly session_id: string },
    ) => Effect.Effect<SourceEnrollmentRecord | null, Failure>;
    readonly insert: (record: SourceEnrollmentRecord) => Effect.Effect<void, Failure>;
  }
>()("t3/jones/workstreams/native/store/sourceEnrollmentStore") {}
const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient,
    db = yield* Database.NativeSql;
  const context = (
    record: Omit<SourceEnrollmentRecord, "proof" | "session_id" | "request_sha256">,
  ) => ({
    registry_instance_id: record.registry_instance_id,
    owner_id: record.owner_id,
    purpose: "native_source_enrollment",
    record_id: record.enrollment_id,
    revision: 1,
  });
  const read: SourceEnrollmentStore["Service"]["read"] = Effect.fn("SourceEnrollmentStore.read")(
    function* (selector) {
      const rows = yield* sql<
        Omit<SourceEnrollmentRecord, "proof"> & { envelope: string }
      >`SELECT * FROM jones_workstreams_source_enrollments WHERE ${"session_id" in selector ? sql`session_id = ${selector.session_id}` : sql`enrollment_id = ${selector.enrollment_id}`}`;
      const row = rows[0];
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
    },
  );
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
  return SourceEnrollmentStore.of({ read, insert });
});
/** @public Canonical service construction. */
export const layer = Layer.effect(SourceEnrollmentStore, make);

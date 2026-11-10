import * as F from "@t3tools/contracts/jones/workstreamsHomeBind";
import { canonicalWorkstreamsSnapshotJson as canonical } from "@t3tools/contracts/jones/workstreamsSnapshot";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";
import { sha256EnrollmentBytes as digest } from "../../enrollment/request.ts";
import * as Database from "./sql.ts";
import { decodeSetupRequest } from "./peerSessionPinStore.ts";

export interface HomeBindRecord {
  readonly request_body: string;
  readonly home_setup_request_body: string;
  readonly result: F.WorkstreamsHomeBindCommittedV2;
}
type Failure = Database.StoreError | Database.CryptoError | SqlError;
export class HomeBindReceiptStore extends Context.Service<
  HomeBindReceiptStore,
  {
    readonly read: (
      scope: Database.OwnerScope,
      bindId: string,
    ) => Effect.Effect<HomeBindRecord | null, Failure>;
    readonly insert: (record: HomeBindRecord) => Effect.Effect<void, Failure>;
  }
>()("t3/jones/workstreams/native/store/homeBindReceiptStore") {}
interface Row extends Database.OwnerScope {
  readonly bind_id: string;
  readonly request_sha256: string;
  readonly home_pin_id: string;
  readonly home_prepared_revision: number;
  readonly home_setup_request_id: string;
  readonly home_setup_request_sha256: string;
  readonly request_envelope: string;
  readonly result_envelope: string;
}
const decodeKeyHeader = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.Struct({ key_id: Schema.String })),
);
const decodeResult = Schema.decodeUnknownSync(F.WorkstreamsHomeBindCommittedV2);
const invalid = () => new Database.StoreError({ reason: "invalid_payload" });
const validate = Effect.fn("HomeBindReceiptStore.validate")(function* (value: unknown) {
  const record = yield* Effect.try({
    try: () => {
      if (
        typeof value !== "object" ||
        value === null ||
        Array.isArray(value) ||
        Object.keys(value).sort().join(",") !== "home_setup_request_body,request_body,result"
      )
        throw invalid();
      const r = value as HomeBindRecord;
      const b = F.decodeWorkstreamsHomeBindBody(r.request_body);
      const result = decodeResult(r.result);
      const setup = decodeSetupRequest(r.home_setup_request_body);
      if (
        result.request_body !== r.request_body ||
        result.receipt.request_sha256 !== digest(r.request_body) ||
        setup.pin.pin_id !== b.home_setup.pin_id ||
        setup.pin.revision !== b.home_setup.prepared_revision ||
        setup.pin.setup_request_id !== b.home_setup.setup_request_id ||
        setup.pin.setup_request_sha256 !== b.home_setup.setup_request_sha256 ||
        canonical(setup.pin.expected_home) !== canonical(b.expected_home) ||
        canonical(setup.pin.source_authority) !== canonical(b.source.authority) ||
        canonical(setup.pin.receiver_authority) !== canonical(b.expected_home_authority) ||
        !setup.pin.roles.includes("member_home") ||
        setup.pin.source_enrollment_id !== b.source.enrollment.enrollment_id
      )
        throw invalid();
      return { record: { ...r, result }, body: b };
    },
    catch: invalid,
  });
  return record;
});
const aad = (scope: Database.OwnerScope, id: string, purpose: string) => ({
  registry_instance_id: scope.registry_instance_id,
  owner_id: scope.owner_id,
  key_scope: "home" as const,
  purpose,
  record_id: id,
  revision: 1,
});
const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient,
    db = yield* Database.NativeSql;
  const read = Effect.fn("HomeBindReceiptStore.read")(function* (
    scope: Database.OwnerScope,
    id: string,
  ) {
    return yield* db.transaction(
      scope,
      Effect.gen(function* () {
        const rows =
          yield* sql<Row>`SELECT * FROM jones_workstreams_home_bind_receipts WHERE registry_instance_id=${scope.registry_instance_id} AND owner_id=${scope.owner_id} AND bind_id=${id}`;
        if (!rows[0]) return null;
        const row = rows[0];
        for (const text of [row.request_envelope, row.result_envelope]) {
          const header = yield* decodeKeyHeader(text).pipe(Effect.mapError(invalid));
          const keyRows =
            yield* sql`SELECT key_id FROM jones_workstreams_keys WHERE key_id=${header.key_id} AND registry_instance_id=${scope.registry_instance_id} AND scope='home' AND state IN ('active','decrypt_only')`;
          if (keyRows.length !== 1) return yield* invalid();
        }
        const request = yield* db.openJson(
          aad(scope, id, "home_bind_request"),
          row.request_envelope,
        );
        if (
          typeof request !== "object" ||
          request === null ||
          Array.isArray(request) ||
          Object.keys(request).sort().join(",") !== "home_setup_request_body,request_body"
        )
          return yield* invalid();
        const result = yield* db.openJson(aad(scope, id, "home_bind_result"), row.result_envelope);
        const { record, body } = yield* validate({ ...request, result });
        if (
          row.registry_instance_id !== scope.registry_instance_id ||
          row.owner_id !== scope.owner_id ||
          canonical(body.expected_home.registry_instance_id) !==
            canonical(scope.registry_instance_id) ||
          body.expected_home.owner_id !== scope.owner_id ||
          row.bind_id !== id ||
          body.bind_id !== id ||
          row.request_sha256 !== digest(record.request_body) ||
          row.home_pin_id !== body.home_setup.pin_id ||
          row.home_prepared_revision !== body.home_setup.prepared_revision ||
          row.home_setup_request_id !== body.home_setup.setup_request_id ||
          row.home_setup_request_sha256 !== digest(record.home_setup_request_body)
        )
          return yield* invalid();
        return record;
      }),
    );
  });
  const insert = Effect.fn("HomeBindReceiptStore.insert")(function* (input: HomeBindRecord) {
    const { record, body } = yield* validate(input);
    const scope = {
      registry_instance_id: body.expected_home.registry_instance_id,
      owner_id: body.expected_home.owner_id,
    };
    return yield* db.transaction(
      scope,
      Effect.gen(function* () {
        const old = yield* read(scope, body.bind_id);
        if (old) {
          if (canonical(old) !== canonical(record))
            return yield* new Database.StoreError({ reason: "idempotency_conflict" });
          return;
        }
        const request_envelope = yield* db.sealJson(aad(scope, body.bind_id, "home_bind_request"), {
          request_body: record.request_body,
          home_setup_request_body: record.home_setup_request_body,
        });
        const result_envelope = yield* db.sealJson(
          aad(scope, body.bind_id, "home_bind_result"),
          record.result,
        );
        yield* sql`INSERT INTO jones_workstreams_home_bind_receipts ${sql.insert({
          ...scope,
          bind_id: body.bind_id,
          request_sha256: digest(record.request_body),
          home_pin_id: body.home_setup.pin_id,
          home_prepared_revision: body.home_setup.prepared_revision,
          home_setup_request_id: body.home_setup.setup_request_id,
          home_setup_request_sha256: digest(record.home_setup_request_body),
          request_envelope,
          result_envelope,
        })}`;
        const current = yield* read(scope, body.bind_id);
        if (!current || canonical(current) !== canonical(record)) return yield* invalid();
      }),
    );
  });
  return HomeBindReceiptStore.of({ read, insert });
});
/** Home-scope encrypted immutable ledger. This store does not authenticate a receipt's producer. */
export const layer = Layer.effect(HomeBindReceiptStore, make);

import { WorkstreamsPeerSessionPinV1 } from "@t3tools/contracts/jones/workstreamsPeerAuthority";
import { canonicalWorkstreamsSnapshotJson as canonical } from "@t3tools/contracts/jones/workstreamsSnapshot";
import { sha256EnrollmentBytes } from "../../enrollment/request.ts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";
import * as Database from "./sql.ts";

export interface PeerPinSetupRequest {
  readonly schema: "jones-workstreams-peer-pin-setup/1";
  readonly assertion: Omit<WorkstreamsPeerSessionPinV1, "setup_request_sha256">;
  readonly expected: {
    readonly registry_version: number;
    readonly grant_id: string;
    readonly grant_version: number;
  };
}
export interface PeerPinRecord {
  readonly pin: WorkstreamsPeerSessionPinV1;
  // Exact original bytes survive bound/revoked projections and process restart.
  readonly request_body: string;
}
type Failure = SqlError | Database.StoreError | Database.CryptoError;
export class PeerSessionPinStore extends Context.Service<
  PeerSessionPinStore,
  {
    readonly read: (
      selector:
        | { readonly pin_id: string }
        | { readonly session_id: string }
        | { readonly setup_request_id: string },
    ) => Effect.Effect<PeerPinRecord | null, Failure>;
    readonly insert: (record: PeerPinRecord) => Effect.Effect<void, Failure>;
    readonly transition: (
      pin: WorkstreamsPeerSessionPinV1,
      expectedRevision: number,
    ) => Effect.Effect<void, Failure>;
  }
>()("t3/jones/workstreams/native/store/peerSessionPinStore") {}

const decodePin = Schema.decodeUnknownEffect(WorkstreamsPeerSessionPinV1);
const decodePinSync = Schema.decodeUnknownSync(WorkstreamsPeerSessionPinV1);
const invalid = () => new Database.StoreError({ reason: "invalid_payload" });
const keysAre = (v: unknown, keys: readonly string[]): v is Record<string, unknown> =>
  typeof v === "object" &&
  v !== null &&
  !Array.isArray(v) &&
  Object.keys(v).length === keys.length &&
  keys.every((k) => Object.hasOwn(v, k));
/** Decode exact setup bytes; the digest field is derived, never part of its own preimage. */
export const decodeSetupRequest = (
  body: string,
): { request: PeerPinSetupRequest; pin: WorkstreamsPeerSessionPinV1 } => {
  if (Buffer.byteLength(body, "utf8") > 65536) throw invalid();
  const v: unknown = JSON.parse(body);
  if (
    !keysAre(v, ["schema", "assertion", "expected"]) ||
    v.schema !== "jones-workstreams-peer-pin-setup/1" ||
    !keysAre(v.expected, ["registry_version", "grant_id", "grant_version"]) ||
    !Number.isSafeInteger(v.expected.registry_version) ||
    Number(v.expected.registry_version) < 0 ||
    !Number.isSafeInteger(v.expected.grant_version) ||
    Number(v.expected.grant_version) < 1 ||
    typeof v.expected.grant_id !== "string" ||
    !v.expected.grant_id ||
    typeof v.assertion !== "object" ||
    v.assertion === null ||
    Array.isArray(v.assertion) ||
    Object.hasOwn(v.assertion, "setup_request_sha256")
  )
    throw invalid();
  const pin = decodePinSync(
    { ...v.assertion, setup_request_sha256: sha256EnrollmentBytes(body) },
    { onExcessProperty: "error" },
  );
  if (
    pin.revision !== 1 ||
    pin.state !== "prepared" ||
    pin.bind_receipt !== null ||
    pin.source_binding_version !== null ||
    pin.singleton_binding_version !== null
  )
    throw invalid();
  return { request: v as unknown as PeerPinSetupRequest, pin };
};
const same = (a: unknown, b: unknown) => canonical(a) === canonical(b);
const immutable = (pin: WorkstreamsPeerSessionPinV1) => {
  const {
    revision: _revision,
    state: _state,
    source_binding_version: _source,
    singleton_binding_version: _singleton,
    bind_receipt: _receipt,
    ...rest
  } = pin;
  return rest;
};
const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient,
    db = yield* Database.NativeSql;
  const aad = (p: WorkstreamsPeerSessionPinV1) => ({
    ...p.expected_home,
    key_scope: "local" as const,
    purpose: "peer_session_pin",
    record_id: p.pin_id,
    revision: p.revision,
  });
  const validate = Effect.fn("PeerSessionPinStore.validate")(function* (record: PeerPinRecord) {
    const pin = yield* decodePin(record.pin, { onExcessProperty: "error" }).pipe(
      Effect.mapError(invalid),
    );
    const original = yield* Effect.try({
      try: () => decodeSetupRequest(record.request_body).pin,
      catch: invalid,
    });
    if (!same(immutable(pin), immutable(original))) return yield* invalid();
    return pin;
  });
  const read: PeerSessionPinStore["Service"]["read"] = Effect.fn("PeerSessionPinStore.read")(
    function* (selector) {
      const rows = yield* sql<{
        registry_instance_id: string;
        owner_id: string;
        pin_id: string;
        session_id: string;
        revision: number;
        state: string;
        expires_at: string;
        setup_request_id: string;
        setup_request_sha256: string;
        envelope: string;
      }>`SELECT * FROM jones_workstreams_peer_session_pins WHERE ${"pin_id" in selector ? sql`pin_id = ${selector.pin_id}` : "session_id" in selector ? sql`session_id = ${selector.session_id}` : sql`setup_request_id = ${selector.setup_request_id}`}`;
      const row = rows[0];
      if (!row) return null;
      const value = yield* db.openJson(
        { ...row, key_scope: "local", purpose: "peer_session_pin", record_id: row.pin_id },
        row.envelope,
      );
      if (!keysAre(value, ["pin", "request_body"]) || typeof value.request_body !== "string")
        return yield* invalid();
      const record = {
        pin: value.pin as WorkstreamsPeerSessionPinV1,
        request_body: value.request_body,
      };
      const p = yield* validate(record);
      if (
        p.pin_id !== row.pin_id ||
        p.session.session_id !== row.session_id ||
        p.expected_home.registry_instance_id !== row.registry_instance_id ||
        p.expected_home.owner_id !== row.owner_id ||
        p.revision !== row.revision ||
        p.state !== row.state ||
        p.expires_at !== row.expires_at ||
        p.setup_request_id !== row.setup_request_id ||
        p.setup_request_sha256 !== row.setup_request_sha256
      )
        return yield* invalid();
      return record;
    },
  );
  const insert = Effect.fn("PeerSessionPinStore.insert")((record: PeerPinRecord) =>
    db.transaction(
      record.pin.expected_home,
      Effect.gen(function* () {
        const p = yield* validate(record);
        if (p.state !== "prepared" || p.revision !== 1) return yield* invalid();
        if (
          (yield* read({ pin_id: p.pin_id })) ||
          (yield* read({ session_id: p.session.session_id })) ||
          (yield* read({ setup_request_id: p.setup_request_id }))
        )
          return yield* new Database.StoreError({ reason: "idempotency_conflict" });
        const envelope = yield* db.sealJson(aad(p), record);
        yield* sql`INSERT INTO jones_workstreams_peer_session_pins ${sql.insert({ registry_instance_id: p.expected_home.registry_instance_id, owner_id: p.expected_home.owner_id, pin_id: p.pin_id, session_id: p.session.session_id, revision: p.revision, state: p.state, expires_at: p.expires_at, setup_request_id: p.setup_request_id, setup_request_sha256: p.setup_request_sha256, envelope })}`;
      }),
    ),
  );
  const transition = Effect.fn("PeerSessionPinStore.transition")(
    (pin: WorkstreamsPeerSessionPinV1, expectedRevision: number) =>
      db.transaction(
        pin.expected_home,
        Effect.gen(function* () {
          const current = yield* read({ pin_id: pin.pin_id });
          if (
            !current ||
            current.pin.revision !== expectedRevision ||
            pin.revision !== expectedRevision + 1 ||
            !same(immutable(current.pin), immutable(pin)) ||
            !(
              (current.pin.state === "prepared" &&
                (pin.state === "bound" || pin.state === "revoked")) ||
              (current.pin.state === "bound" && pin.state === "revoked")
            ) ||
            (pin.state === "revoked" &&
              (!same(pin.bind_receipt, current.pin.bind_receipt) ||
                pin.source_binding_version !== current.pin.source_binding_version ||
                pin.singleton_binding_version !== current.pin.singleton_binding_version))
          )
            return yield* new Database.StoreError({ reason: "conflict" });
          const record = { pin, request_body: current.request_body };
          yield* validate(record);
          const envelope = yield* db.sealJson(aad(pin), record);
          const rows =
            yield* sql`UPDATE jones_workstreams_peer_session_pins SET revision=${pin.revision},state=${pin.state},envelope=${envelope} WHERE pin_id=${pin.pin_id} AND revision=${expectedRevision} RETURNING pin_id`;
          if (rows.length !== 1) return yield* new Database.StoreError({ reason: "conflict" });
        }),
      ),
  );
  return PeerSessionPinStore.of({ read, insert, transition });
});
/** @public Canonical service construction. */
export const layer = Layer.effect(PeerSessionPinStore, make);

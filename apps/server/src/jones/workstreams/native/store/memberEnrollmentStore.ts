import * as Wire from "@t3tools/contracts/jones/workstreamsMemberEnrollment";
import { decodeWorkstreamsHomeBindBody } from "@t3tools/contracts/jones/workstreamsHomeBind";
import { canonicalWorkstreamsSnapshotJson as canonical } from "@t3tools/contracts/jones/workstreamsSnapshot";
import { sha256EnrollmentBytes as sha } from "../../enrollment/request.ts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";
import * as Database from "./sql.ts";

export type MemberEnrollmentSelector =
  | { readonly enrollment_id: string }
  | { readonly session_id: string }
  | { readonly setup_request_id: string }
  | { readonly registry_instance_id: string; readonly owner_id: string; readonly bind_id: string };
type Failure = SqlError | Database.StoreError | Database.CryptoError;
export class MemberEnrollmentStore extends Context.Service<
  MemberEnrollmentStore,
  {
    readonly read: (
      selector: MemberEnrollmentSelector,
    ) => Effect.Effect<Wire.WorkstreamsMemberEnrollmentV1 | null, Failure>;
    readonly insert: (
      record: Wire.WorkstreamsMemberEnrollmentPreparedV1,
    ) => Effect.Effect<void, Failure>;
    readonly accept: (
      record: Wire.WorkstreamsMemberEnrollmentBoundV1,
      expectedRevision: 1,
    ) => Effect.Effect<void, Failure>;
  }
>()("t3/jones/workstreams/native/store/memberEnrollmentStore") {}

const invalid = () => new Database.StoreError({ reason: "invalid_payload" });
const decode = Schema.decodeUnknownSync(Wire.WorkstreamsMemberEnrollmentV1);
/** Retained evidence validation does not authenticate the remote producer. */
export const validateMemberEnrollment = (input: unknown): Wire.WorkstreamsMemberEnrollmentV1 => {
  const record = decode(input, { onExcessProperty: "error" });
  const i = record.intent;
  if (
    sha(i.bind_request_body) !== i.bind_request_sha256 ||
    sha(i.member_setup_request_body) !== i.member_setup_request_sha256 ||
    sha(i.home_setup.setup_request_body) !== i.home_setup.pin.setup_request_sha256 ||
    sha(Wire.canonicalWorkstreamsMemberEnrollmentIntent(i)) !== record.intent_sha256
  )
    throw invalid();
  return record;
};
const selectors = (r: Wire.WorkstreamsMemberEnrollmentV1) => {
  const b = decodeWorkstreamsHomeBindBody(r.intent.bind_request_body);
  return {
    registry_instance_id: b.expected_home.registry_instance_id,
    owner_id: b.expected_home.owner_id,
    enrollment_id: b.source.enrollment.enrollment_id,
    session_id: b.source.enrollment.session.session_id,
    setup_request_id: b.member_setup.setup_request_id,
    bind_id: b.bind_id,
    request_sha256: r.intent.bind_request_sha256,
    intent_sha256: r.intent_sha256,
    home_environment_id: b.expected_home.home_environment_id,
    home_generation: b.expected_home.server_generation,
    revision: r.revision,
    state: r.state,
  };
};
type Row = ReturnType<typeof selectors> & { readonly envelope: string };
const aad = (r: Pick<Row, "registry_instance_id" | "owner_id" | "enrollment_id" | "revision">) => ({
  registry_instance_id: r.registry_instance_id,
  owner_id: r.owner_id,
  key_scope: "local" as const,
  purpose: "native_member_enrollment",
  record_id: r.enrollment_id,
  revision: r.revision,
});
const decodeEnvelopeKey = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.Struct({ key_id: Schema.String })),
);
const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient,
    db = yield* Database.NativeSql;
  const validate = (input: unknown) =>
    Effect.try({ try: () => validateMemberEnrollment(input), catch: invalid });
  const read: MemberEnrollmentStore["Service"]["read"] = Effect.fn("MemberEnrollmentStore.read")(
    function* (s) {
      const rows = yield* sql<Row>`SELECT * FROM jones_workstreams_member_enrollments WHERE ${
        "enrollment_id" in s
          ? sql`enrollment_id=${s.enrollment_id}`
          : "session_id" in s
            ? sql`session_id=${s.session_id}`
            : "setup_request_id" in s
              ? sql`setup_request_id=${s.setup_request_id}`
              : sql`registry_instance_id=${s.registry_instance_id} AND owner_id=${s.owner_id} AND bind_id=${s.bind_id}`
      }`;
      if (rows.length === 0) return null;
      if (rows.length !== 1) return yield* invalid();
      const row = rows[0]!;
      const metadata = yield* decodeEnvelopeKey(row.envelope).pipe(Effect.mapError(invalid));
      const localKey = yield* sql`SELECT key_id FROM jones_workstreams_keys
      WHERE registry_instance_id=${row.registry_instance_id} AND key_id=${metadata.key_id} AND scope='local'`;
      if (localKey.length !== 1) return yield* invalid();
      const record = yield* validate(yield* db.openJson(aad(row), row.envelope));
      const expected = selectors(record);
      if (
        Object.entries(expected).some(([key, value]) => row[key as keyof Row] !== value) ||
        Object.entries(s).some(([key, value]) => expected[key as keyof typeof expected] !== value)
      )
        return yield* invalid();
      return record;
    },
  );
  const insert = Effect.fn("MemberEnrollmentStore.insert")(function* (
    input: Wire.WorkstreamsMemberEnrollmentPreparedV1,
  ) {
    const r = yield* validate(input),
      s = selectors(r);
    if (r.state !== "prepared") return yield* invalid();
    yield* db.transaction(
      s,
      Effect.gen(function* () {
        for (const selector of [
          { enrollment_id: s.enrollment_id },
          { session_id: s.session_id },
          { setup_request_id: s.setup_request_id },
          {
            registry_instance_id: s.registry_instance_id,
            owner_id: s.owner_id,
            bind_id: s.bind_id,
          },
        ])
          if (yield* read(selector))
            return yield* new Database.StoreError({ reason: "idempotency_conflict" });
        const envelope = yield* db.sealJson(aad(s), r);
        yield* sql`INSERT INTO jones_workstreams_member_enrollments ${sql.insert({ ...s, envelope })}`;
      }),
    );
  });
  const accept = Effect.fn("MemberEnrollmentStore.accept")(function* (
    input: Wire.WorkstreamsMemberEnrollmentBoundV1,
    revision: 1,
  ) {
    const r = yield* validate(input),
      s = selectors(r);
    yield* db.transaction(
      s,
      Effect.gen(function* () {
        const before = yield* read({ enrollment_id: s.enrollment_id });
        if (
          !before ||
          revision !== 1 ||
          before.revision !== revision ||
          !Wire.isWorkstreamsMemberEnrollmentAcceptance(before, r) ||
          canonical(selectors(before)) !== canonical({ ...s, state: "prepared", revision: 1 })
        )
          return yield* new Database.StoreError({ reason: "conflict" });
        const envelope = yield* db.sealJson(aad(s), r);
        const updated = yield* sql`UPDATE jones_workstreams_member_enrollments
        SET state=${r.state},revision=${r.revision},envelope=${envelope}
        WHERE registry_instance_id=${s.registry_instance_id} AND owner_id=${s.owner_id}
          AND enrollment_id=${s.enrollment_id} AND session_id=${s.session_id}
          AND setup_request_id=${s.setup_request_id} AND bind_id=${s.bind_id}
          AND intent_sha256=${s.intent_sha256} AND request_sha256=${s.request_sha256}
          AND revision=${revision} AND state='prepared' RETURNING enrollment_id`;
        if (updated.length !== 1) return yield* new Database.StoreError({ reason: "conflict" });
      }),
    );
  });
  return MemberEnrollmentStore.of({ read, insert, accept });
});
export const layer = Layer.effect(MemberEnrollmentStore, make);

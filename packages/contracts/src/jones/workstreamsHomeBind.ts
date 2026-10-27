import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { AuthSessionId } from "../baseSchemas.ts";
import {
  closedWorkstreamsNative as closed,
  WorkstreamsNativeDigest as Digest,
  WorkstreamsNativeHomeTarget,
  WorkstreamsNativeId as Id,
  WorkstreamsNativeOrigin,
  WorkstreamsNativeSequence as Sequence,
  WorkstreamsNativeTimestamp,
  WorkstreamsNativeVersion as Version,
} from "./workstreamsNative.ts";
import { WorkstreamsNativeContext } from "./workstreamsNativeProvider.ts";
import {
  WorkstreamsPeerSessionPinV1,
  WorkstreamsSourceAuthority,
} from "./workstreamsPeerAuthority.ts";
import { canonicalWorkstreamsSnapshotJson as canonical } from "./workstreamsSnapshot.ts";

export const WORKSTREAMS_HOME_BIND_CONTRACT = "workstreams-native-home-bind/2.0.0";

// This is the original source issuance wire, composed from its existing contract primitives.
// It is intent only: decoding it neither issues nor reserves an authentication session.
const UtcMillis = WorkstreamsNativeTimestamp.check(
  Schema.isPattern(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/),
  Schema.makeFilter((value) => {
    const parsed = DateTime.make(value);
    return Option.isSome(parsed) && DateTime.formatIso(parsed.value) === value;
  }),
);
const RegistryOrigin = Schema.String.check(
  Schema.isMaxLength(2048),
  Schema.makeFilter((value) => {
    try {
      const url = new URL(value);
      return (
        (url.protocol === "https:" ||
          (url.protocol === "http:" &&
            (url.hostname === "127.0.0.1" || url.hostname === "[::1]"))) &&
        url.origin === value &&
        url.username === "" &&
        url.password === ""
      );
    } catch {
      return false;
    }
  }),
);
export const WorkstreamsSourceIssuanceRequestV1 = closed(
  Schema.Struct({
    schema: Schema.Literal("jones-code.workstreams-native-enrollment/v1"),
    enrollment_id: WorkstreamsNativeContext.fields.enrollment_id,
    registry_origin: RegistryOrigin,
    context: WorkstreamsNativeContext,
    session: closed(
      Schema.Struct({ session_id: AuthSessionId, issued_at: UtcMillis, expires_at: UtcMillis }),
    ),
  }),
).check(
  Schema.makeFilter(
    (r) =>
      r.enrollment_id === r.context.enrollment_id &&
      Date.parse(r.session.expires_at) > Date.parse(r.session.issued_at) &&
      Date.parse(r.session.expires_at) - Date.parse(r.session.issued_at) <=
        30 * 24 * 60 * 60 * 1000,
  ),
);
export type WorkstreamsSourceIssuanceRequestV1 = typeof WorkstreamsSourceIssuanceRequestV1.Type;

const { setup_request_sha256: _digest, ...assertionFields } = WorkstreamsPeerSessionPinV1.fields;
export const WorkstreamsPreparedPinAssertionV1 = closed(Schema.Struct(assertionFields)).check(
  Schema.makeFilter(
    (p) =>
      p.revision === 1 &&
      p.state === "prepared" &&
      p.bind_receipt === null &&
      p.source_binding_version === null &&
      p.singleton_binding_version === null &&
      Date.parse(p.session.issued_at) < Date.parse(p.expires_at) &&
      Date.parse(p.expires_at) <= Date.parse(p.session.expires_at),
  ),
);
export type WorkstreamsPreparedPinAssertionV1 = typeof WorkstreamsPreparedPinAssertionV1.Type;
// Historical grant preconditions occur only inside these exact retained setup bytes.
export const WorkstreamsPeerPinSetupRequestV1 = closed(
  Schema.Struct({
    schema: Schema.Literal("jones-workstreams-peer-pin-setup/1"),
    assertion: WorkstreamsPreparedPinAssertionV1,
    expected: closed(
      Schema.Struct({ registry_version: Sequence, grant_id: Id, grant_version: Version }),
    ),
  }),
);
export type WorkstreamsPeerPinSetupRequestV1 = typeof WorkstreamsPeerPinSetupRequestV1.Type;

const decodeIssuance = Schema.decodeUnknownSync(WorkstreamsSourceIssuanceRequestV1);
const decodeSetup = Schema.decodeUnknownSync(WorkstreamsPeerPinSetupRequestV1);
const retainedJson = (validate: (value: unknown) => unknown, maxBytes: number) =>
  Schema.String.check(
    Schema.makeFilter((body) => {
      if (new TextEncoder().encode(body).length > maxBytes) return false;
      try {
        validate(JSON.parse(body));
        return true;
      } catch {
        return false;
      }
    }),
  );
// Original issuance and setup bytes need not be normalized during recovery.
export const WorkstreamsSourceIssuanceBodyV1 = retainedJson(decodeIssuance, 65_536);
export const WorkstreamsPeerPinSetupBodyV1 = retainedJson(decodeSetup, 65_536);

export const WorkstreamsHomeBindSourcePreimage = closed(
  Schema.Struct({
    binding_version: Version,
    authority_namespace: WorkstreamsSourceAuthority.fields.authority_namespace,
    store_generation: Version,
    state: Schema.Literals(["active", "revoked"]),
  }),
);
export type WorkstreamsHomeBindSourcePreimage = typeof WorkstreamsHomeBindSourcePreimage.Type;
export const WorkstreamsHomeBindBodyV2 = closed(
  Schema.Struct({
    bind_id: Id,
    expected_home: WorkstreamsNativeHomeTarget,
    expected_canonical_origin: WorkstreamsNativeOrigin,
    expected_home_authority: WorkstreamsSourceAuthority,
    source: closed(
      Schema.Struct({
        authority: WorkstreamsSourceAuthority,
        enrollment: WorkstreamsSourceIssuanceRequestV1,
      }),
    ),
    home_setup: closed(
      Schema.Struct({
        pin_id: Id,
        prepared_revision: Version,
        setup_request_id: Id,
        setup_request_sha256: Digest,
      }),
    ),
    member_setup: closed(
      Schema.Struct({
        pin_id: Id,
        prepared_revision: Version,
        setup_request_id: Id,
        outbound_link_generation: Schema.String.check(
          Schema.isMinLength(1),
          Schema.isMaxLength(1024),
          Schema.makeFilter((value) => value.trim() === value),
        ),
        expected_singleton_version: Schema.NullOr(Version),
        singleton_binding_version: Version,
      }),
    ),
    expected: closed(
      Schema.Struct({
        principal_id: Id,
        registry_version: Sequence,
        grant_id: Id,
        grant_version: Version,
        source_binding: Schema.NullOr(WorkstreamsHomeBindSourcePreimage),
      }),
    ),
    rebind: Schema.Boolean,
  }),
).check(
  Schema.makeFilter((r) => {
    const c = r.source.enrollment.context;
    return (
      r.expected_home.home_environment_id === r.expected_home_authority.environment_id &&
      r.expected_home.owner_id === c.owner_id &&
      r.expected.principal_id === c.principal_id &&
      r.source.authority.environment_id === c.source_instance_id &&
      r.source.authority.authority_namespace === c.authority_namespace &&
      r.source.authority.store_generation === c.store_generation &&
      r.source.enrollment.registry_origin === r.expected_canonical_origin &&
      r.source.authority.environment_id !== r.expected_home.home_environment_id &&
      r.home_setup.prepared_revision === 1 &&
      r.member_setup.prepared_revision === 1 &&
      r.home_setup.pin_id !== r.member_setup.pin_id &&
      r.home_setup.setup_request_id !== r.member_setup.setup_request_id &&
      r.member_setup.singleton_binding_version ===
        (r.member_setup.expected_singleton_version ?? 0) + 1
    );
  }),
);
export type WorkstreamsHomeBindBodyV2 = typeof WorkstreamsHomeBindBodyV2.Type;
const decodeBody = Schema.decodeUnknownSync(WorkstreamsHomeBindBodyV2);
/** @public Canonical UTF-8 JSON preimage; does not compute a digest or a commit witness. */
export const canonicalWorkstreamsHomeBindBody = (input: WorkstreamsHomeBindBodyV2): string =>
  canonical(decodeBody(input));
export const WorkstreamsHomeBindRequestBodyV2 = retainedJson(
  (value) => decodeBody(value),
  262_144,
).check(
  Schema.makeFilter((body) => {
    try {
      return canonical(decodeBody(JSON.parse(body))) === body;
    } catch {
      return false;
    }
  }),
);
const decodeCanonicalBody = Schema.decodeSync(WorkstreamsHomeBindRequestBodyV2);
/** @public Validates canonical bytes and returns their closed intent, without authenticating it. */
export const decodeWorkstreamsHomeBindBody = (body: string): WorkstreamsHomeBindBodyV2 => {
  decodeCanonicalBody(body);
  return decodeBody(JSON.parse(body));
};

const contract = Schema.Literal(WORKSTREAMS_HOME_BIND_CONTRACT);
export const WorkstreamsHomeBindRequestV2 = closed(
  Schema.Struct({ contract, request_body: WorkstreamsHomeBindRequestBodyV2 }),
);
export type WorkstreamsHomeBindRequestV2 = typeof WorkstreamsHomeBindRequestV2.Type;
export const WorkstreamsHomeBindReceiptV2 = closed(
  Schema.Struct({
    principal_id: Id,
    binding_version: Version,
    registry_version: Sequence,
    request_sha256: Digest,
  }),
);
export type WorkstreamsHomeBindReceiptV2 = typeof WorkstreamsHomeBindReceiptV2.Type;
export const WorkstreamsHomeBindCommittedV2 = closed(
  Schema.Struct({
    contract,
    kind: Schema.Literal("committed"),
    request_body: WorkstreamsHomeBindRequestBodyV2,
    receipt: WorkstreamsHomeBindReceiptV2,
    home_pin_bound_revision: Version,
  }),
).check(
  Schema.makeFilter((r) => {
    const b = decodeWorkstreamsHomeBindBody(r.request_body);
    // Validate the one-append shape; only H can supply actual post-write facts and the digest.
    return (
      r.receipt.principal_id === b.expected.principal_id &&
      r.home_pin_bound_revision === b.home_setup.prepared_revision + 1 &&
      r.receipt.registry_version === b.expected.registry_version + 1 &&
      r.receipt.binding_version === (b.expected.source_binding?.binding_version ?? 0) + 1
    );
  }),
);
export type WorkstreamsHomeBindCommittedV2 = typeof WorkstreamsHomeBindCommittedV2.Type;
export const WorkstreamsHomeBindReadbackRequestV2 = closed(
  Schema.Struct({
    contract,
    expected_home: WorkstreamsNativeHomeTarget,
    bind_id: Id,
    request_sha256: Digest,
  }),
);
export type WorkstreamsHomeBindReadbackRequestV2 = typeof WorkstreamsHomeBindReadbackRequestV2.Type;
const readbackFields = { contract, request: WorkstreamsHomeBindReadbackRequestV2 };
// These are wire observations, not verified readback objects. Only the genuine adapter can
// authenticate them. Empty lookup, key loss, restore uncertainty and in-flight work stay unknown.
export const WorkstreamsHomeBindReadbackV2 = Schema.Union([
  closed(
    Schema.Struct({
      ...readbackFields,
      kind: Schema.Literal("committed"),
      result: WorkstreamsHomeBindCommittedV2,
    }),
  ).check(
    Schema.makeFilter((r) => {
      const b = decodeWorkstreamsHomeBindBody(r.result.request_body);
      return (
        canonical(r.request.expected_home) === canonical(b.expected_home) &&
        r.request.bind_id === b.bind_id &&
        r.request.request_sha256 === r.result.receipt.request_sha256
      );
    }),
  ),
  closed(
    Schema.Struct({
      ...readbackFields,
      kind: Schema.Literal("conflict"),
      retained_request_sha256: Digest,
    }),
  ),
  closed(
    Schema.Struct({
      ...readbackFields,
      kind: Schema.Literal("proven_absent"),
      coverage: closed(
        Schema.Struct({
          home_authority: WorkstreamsSourceAuthority,
          registry_version: Sequence,
          receipts: Schema.Literal("complete"),
          dispatch: Schema.Literal("definitive_no_effect"),
        }),
      ),
    }),
  ).check(
    Schema.makeFilter(
      (r) =>
        r.coverage.home_authority.environment_id === r.request.expected_home.home_environment_id,
    ),
  ),
  closed(
    Schema.Struct({
      ...readbackFields,
      kind: Schema.Literal("unknown"),
      reason: Schema.Literals([
        "key_unavailable",
        "restore_uncertain",
        "generation_mismatch",
        "unsupported_capability",
        "authentication_failed",
        "home_unreachable",
        "in_flight",
        "coverage_incomplete",
      ]),
    }),
  ),
]);
export type WorkstreamsHomeBindReadbackV2 = typeof WorkstreamsHomeBindReadbackV2.Type;

export const WorkstreamsHomeBindSetupReadbackRequestV2 = closed(
  Schema.Struct({
    contract,
    expected_home: WorkstreamsNativeHomeTarget,
    home_setup_request_id: Id,
  }),
);
export type WorkstreamsHomeBindSetupReadbackRequestV2 =
  typeof WorkstreamsHomeBindSetupReadbackRequestV2.Type;
export const WorkstreamsHomeBindSetupReadbackV2 = closed(
  Schema.Struct({
    contract,
    kind: Schema.Literal("prepared"),
    request: WorkstreamsHomeBindSetupReadbackRequestV2,
    canonical_origin: WorkstreamsNativeOrigin,
    home_authority: WorkstreamsSourceAuthority,
    setup_request_body: WorkstreamsPeerPinSetupBodyV1,
    pin: WorkstreamsPeerSessionPinV1,
  }),
).check(
  Schema.makeFilter((r) => {
    const s = decodeSetup(JSON.parse(r.setup_request_body));
    const { setup_request_sha256: _sha, ...assertion } = r.pin;
    return (
      canonical(s.assertion) === canonical(assertion) &&
      r.request.home_setup_request_id === r.pin.setup_request_id &&
      canonical(r.request.expected_home) === canonical(r.pin.expected_home) &&
      canonical(r.home_authority) === canonical(r.pin.receiver_authority) &&
      r.home_authority.environment_id === r.request.expected_home.home_environment_id &&
      r.pin.roles.includes("member_home") &&
      r.pin.peer_environment_id === r.pin.source_authority.environment_id
    );
  }),
);
export type WorkstreamsHomeBindSetupReadbackV2 = typeof WorkstreamsHomeBindSetupReadbackV2.Type;

import * as Schema from "effect/Schema";
import {
  closedWorkstreamsNative as closed,
  WorkstreamsNativeDigest as Digest,
  WorkstreamsNativeVersion as Version,
} from "./workstreamsNative.ts";
import { canonicalWorkstreamsSnapshotJson as canonical } from "./workstreamsSnapshot.ts";
import {
  WorkstreamsHomeBindCommittedV2,
  WorkstreamsHomeBindRequestBodyV2,
  WorkstreamsHomeBindSetupReadbackV2,
  WorkstreamsPeerPinSetupBodyV1,
  WorkstreamsPeerPinSetupRequestV1,
  WorkstreamsSourceIssuanceBodyV1,
  WorkstreamsSourceIssuanceRequestV1,
  decodeWorkstreamsHomeBindBody,
} from "./workstreamsHomeBind.ts";

export const WORKSTREAMS_MEMBER_ENROLLMENT_SCHEMA = "jones-workstreams-member-enrollment/1";
const decodeIssuance = Schema.decodeUnknownSync(WorkstreamsSourceIssuanceRequestV1);
const decodeSetup = Schema.decodeUnknownSync(WorkstreamsPeerPinSetupRequestV1);
const intentFields = {
  issuance_body: WorkstreamsSourceIssuanceBodyV1,
  bind_request_body: WorkstreamsHomeBindRequestBodyV2,
  bind_request_sha256: Digest,
  home_setup: WorkstreamsHomeBindSetupReadbackV2,
  member_setup_request_body: WorkstreamsPeerPinSetupBodyV1,
  member_setup_request_sha256: Digest,
};
const Intent = closed(Schema.Struct(intentFields)).check(
  Schema.makeFilter((r) => {
    const b = decodeWorkstreamsHomeBindBody(r.bind_request_body);
    const issuance = decodeIssuance(JSON.parse(r.issuance_body));
    const setup = decodeSetup(JSON.parse(r.member_setup_request_body));
    const h = r.home_setup.pin;
    const m = setup.assertion;
    return (
      canonical(issuance) === canonical(b.source.enrollment) &&
      canonical(r.home_setup.request.expected_home) === canonical(b.expected_home) &&
      canonical(r.home_setup.home_authority) === canonical(b.expected_home_authority) &&
      r.home_setup.canonical_origin === b.expected_canonical_origin &&
      h.pin_id === b.home_setup.pin_id &&
      h.revision === b.home_setup.prepared_revision &&
      h.setup_request_id === b.home_setup.setup_request_id &&
      h.setup_request_sha256 === b.home_setup.setup_request_sha256 &&
      canonical(h.source_authority) === canonical(b.source.authority) &&
      h.source_enrollment_id === issuance.enrollment_id &&
      canonical(decodeSetup(JSON.parse(r.home_setup.setup_request_body)).expected) ===
        canonical({
          registry_version: b.expected.registry_version,
          grant_id: b.expected.grant_id,
          grant_version: b.expected.grant_version,
        }) &&
      canonical(setup.expected) ===
        canonical({
          registry_version: b.expected.registry_version,
          grant_id: b.expected.grant_id,
          grant_version: b.expected.grant_version,
        }) &&
      m.pin_id === b.member_setup.pin_id &&
      m.revision === b.member_setup.prepared_revision &&
      m.setup_request_id === b.member_setup.setup_request_id &&
      canonical(m.expected_home) === canonical(b.expected_home) &&
      canonical(m.receiver_authority) === canonical(b.source.authority) &&
      canonical(m.source_authority) === canonical(b.source.authority) &&
      m.peer_environment_id === b.expected_home.home_environment_id &&
      m.roles.includes("home_source") &&
      m.source_enrollment_id === issuance.enrollment_id &&
      m.session.session_id !== issuance.session.session_id &&
      h.session.session_id !== issuance.session.session_id &&
      m.session.session_id !== h.session.session_id
    );
  }),
);
export const WorkstreamsMemberEnrollmentIntentV1 = Intent;
export type WorkstreamsMemberEnrollmentIntentV1 = typeof WorkstreamsMemberEnrollmentIntentV1.Type;
const fields = {
  schema: Schema.Literal(WORKSTREAMS_MEMBER_ENROLLMENT_SCHEMA),
  intent: Intent,
  intent_sha256: Digest,
};
// A prepared bootstrap journal cannot be mistaken for accepted member authority.
export const WorkstreamsMemberEnrollmentPreparedV1 = closed(
  Schema.Struct({
    ...fields,
    state: Schema.Literal("prepared"),
    revision: Schema.Literal(1),
    home_result: Schema.Null,
    local_binding: Schema.Null,
  }),
);
export type WorkstreamsMemberEnrollmentPreparedV1 =
  typeof WorkstreamsMemberEnrollmentPreparedV1.Type;
export const WorkstreamsMemberEnrollmentBoundV1 = closed(
  Schema.Struct({
    ...fields,
    state: Schema.Literal("bound"),
    revision: Schema.Literal(2),
    home_result: WorkstreamsHomeBindCommittedV2,
    local_binding: closed(
      Schema.Struct({
        pin_bound_revision: Version,
        singleton_binding_version: Version,
      }),
    ),
  }),
).check(
  Schema.makeFilter((r) => {
    const b = decodeWorkstreamsHomeBindBody(r.intent.bind_request_body);
    return (
      r.home_result.request_body === r.intent.bind_request_body &&
      r.home_result.receipt.request_sha256 === r.intent.bind_request_sha256 &&
      r.local_binding.pin_bound_revision === b.member_setup.prepared_revision + 1 &&
      r.local_binding.singleton_binding_version === b.member_setup.singleton_binding_version
    );
  }),
);
export type WorkstreamsMemberEnrollmentBoundV1 = typeof WorkstreamsMemberEnrollmentBoundV1.Type;
// These schemas represent retained evidence, never a module-private verified authority token.
export const WorkstreamsMemberEnrollmentV1 = Schema.Union([
  WorkstreamsMemberEnrollmentPreparedV1,
  WorkstreamsMemberEnrollmentBoundV1,
]);
export type WorkstreamsMemberEnrollmentV1 = typeof WorkstreamsMemberEnrollmentV1.Type;

const decodeIntent = Schema.decodeSync(Intent);
const decodeRecord = Schema.decodeSync(WorkstreamsMemberEnrollmentV1);
/** @public Digest preimage for immutable retained bytes and destination; the store hashes it. */
export const canonicalWorkstreamsMemberEnrollmentIntent = (
  intent: WorkstreamsMemberEnrollmentIntentV1,
): string => canonical(decodeIntent(intent));
/** @public Deterministic encrypted-record encoding; retained request strings remain byte-exact. */
export const canonicalWorkstreamsMemberEnrollment = (
  record: WorkstreamsMemberEnrollmentV1,
): string => canonical(decodeRecord(record));
/** @public Guarded projection check only; stores must also hash, authenticate and CAS their row. */
export const isWorkstreamsMemberEnrollmentAcceptance = (
  before: WorkstreamsMemberEnrollmentV1,
  after: WorkstreamsMemberEnrollmentV1,
): boolean => {
  const a = decodeRecord(before);
  const b = decodeRecord(after);
  return (
    a.state === "prepared" &&
    b.state === "bound" &&
    b.revision === a.revision + 1 &&
    a.intent_sha256 === b.intent_sha256 &&
    canonicalWorkstreamsMemberEnrollmentIntent(a.intent) ===
      canonicalWorkstreamsMemberEnrollmentIntent(b.intent)
  );
};

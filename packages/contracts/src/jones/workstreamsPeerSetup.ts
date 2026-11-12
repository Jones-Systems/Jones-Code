import * as Schema from "effect/Schema";
import { WorkstreamsNativeBuild } from "./workstreamsNativeProvider.ts";
import {
  closedWorkstreamsNative as closed,
  WorkstreamsNativeHomeTarget as Home,
  WorkstreamsNativeId as Id,
  WorkstreamsNativeDigest as Digest,
  WorkstreamsNativeVersion as Version,
  WorkstreamsNativeOrigin as Origin,
} from "./workstreamsNative.ts";
import {
  WorkstreamsPeerSessionPinV1 as Pin,
  WorkstreamsSourceAuthority as Authority,
} from "./workstreamsPeerAuthority.ts";
import * as F from "./workstreamsHomeBind.ts";
import { WorkstreamsMemberEnrollmentIntentV1 as Intent } from "./workstreamsMemberEnrollment.ts";
import { canonicalWorkstreamsSnapshotJson as canonical } from "./workstreamsSnapshot.ts";

const decodePinSetup = Schema.decodeUnknownSync(F.WorkstreamsPeerPinSetupRequestV1);
export const WORKSTREAMS_PEER_SETUP_CONTRACT = "workstreams-native-peer-setup/1.0.0";
const contract = Schema.Literal(WORKSTREAMS_PEER_SETUP_CONTRACT);
export const WorkstreamsSetupOwnerDescriptor = closed(
  Schema.Struct({
    kind: Schema.Literals(["standalone", "desktop"]),
    authority: Authority,
    build: WorkstreamsNativeBuild,
    runtime_version: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(128)),
    qualification_sha256: Digest,
  }),
);
export type WorkstreamsSetupOwnerDescriptor = typeof WorkstreamsSetupOwnerDescriptor.Type;
const Generation = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(1024),
  Schema.makeFilter((v) => v.trim() === v),
);
export const WorkstreamsPeerSetupAddress = closed(
  Schema.Struct({
    setup_id: Id,
    expected_home: Home,
    source_environment_id: Id,
  }),
);
export type WorkstreamsPeerSetupAddress = typeof WorkstreamsPeerSetupAddress.Type;
export const WorkstreamsPeerSetupObservation = closed(
  Schema.Struct({
    owner: WorkstreamsSetupOwnerDescriptor,
    session: Pin.fields.session,
    outbound_link_generation: Generation,
    singleton_version: Schema.NullOr(Version),
  }),
);
export type WorkstreamsPeerSetupObservation = typeof WorkstreamsPeerSetupObservation.Type;
const ContextRequest = closed(
  Schema.Struct({
    contract,
    operation: Schema.Literal("context"),
    ...WorkstreamsPeerSetupAddress.fields,
    receiver_environment_id: Id,
  }),
);
export const WorkstreamsPeerSetupIdentity = closed(
  Schema.Struct({
    ...WorkstreamsPeerSetupAddress.fields,
    request_sha256: Digest,
  }),
);
export type WorkstreamsPeerSetupIdentity = typeof WorkstreamsPeerSetupIdentity.Type;
const IntentRead = closed(
  Schema.Struct({
    contract,
    operation: Schema.Literal("source_intent"),
    ...WorkstreamsPeerSetupIdentity.fields,
    receiver_environment_id: Id,
  }),
);
export const WorkstreamsPeerSetupReadRequest = Schema.Union([ContextRequest, IntentRead]);
export type WorkstreamsPeerSetupReadRequest = typeof WorkstreamsPeerSetupReadRequest.Type;
const HomeContext = closed(
  Schema.Struct({
    canonical_origin: Origin,
    expected: F.WorkstreamsHomeBindBodyV2.fields.expected,
  }),
);
export const WorkstreamsPeerSetupContext = closed(
  Schema.Struct({
    contract,
    operation: Schema.Literal("context"),
    request: ContextRequest,
    local: WorkstreamsPeerSetupObservation,
    reverse: Schema.NullOr(WorkstreamsPeerSetupObservation),
    home: Schema.NullOr(HomeContext),
  }),
);
export type WorkstreamsPeerSetupContext = typeof WorkstreamsPeerSetupContext.Type;
// Retained bytes are intent. They are never a receipt, invocation or portable owner token.
export const WorkstreamsPeerSetupRequest = closed(
  Schema.Struct({
    contract,
    ...WorkstreamsPeerSetupAddress.fields,
    home_observation: WorkstreamsPeerSetupObservation,
    member_observation: WorkstreamsPeerSetupObservation,
    intent: Intent,
  }),
).check(
  Schema.makeFilter((r) => {
    const b = F.decodeWorkstreamsHomeBindBody(r.intent.bind_request_body);
    const h = decodePinSetup(JSON.parse(r.intent.home_setup.setup_request_body)).assertion;
    const m = decodePinSetup(JSON.parse(r.intent.member_setup_request_body)).assertion;
    return (
      canonical(b.expected_home) === canonical(r.expected_home) &&
      b.source.authority.environment_id === r.source_environment_id &&
      canonical(r.home_observation.owner.authority) === canonical(b.expected_home_authority) &&
      canonical(r.member_observation.owner.authority) === canonical(b.source.authority) &&
      canonical(r.member_observation.owner.build) ===
        canonical(b.source.enrollment.context.build) &&
      h.owner_qualification_digest === r.home_observation.owner.qualification_sha256 &&
      m.owner_qualification_digest === r.member_observation.owner.qualification_sha256 &&
      canonical(h.session) === canonical(r.home_observation.session) &&
      canonical(m.session) === canonical(r.member_observation.session) &&
      b.member_setup.outbound_link_generation === r.member_observation.outbound_link_generation &&
      b.member_setup.expected_singleton_version === r.member_observation.singleton_version
    );
  }),
);
export type WorkstreamsPeerSetupRequest = typeof WorkstreamsPeerSetupRequest.Type;
const decode = Schema.decodeUnknownSync(WorkstreamsPeerSetupRequest);
export const WorkstreamsPeerSetupRequestBody = Schema.String.check(
  Schema.makeFilter((body) => {
    if (new TextEncoder().encode(body).byteLength > 786432) return false;
    try {
      decode(JSON.parse(body));
      return true;
    } catch {
      return false;
    }
  }),
);
const decodeRequestBody = Schema.decodeSync(WorkstreamsPeerSetupRequestBody);
export const decodeWorkstreamsPeerSetupRequest = (body: string): WorkstreamsPeerSetupRequest =>
  decode(JSON.parse(decodeRequestBody(body)));
export const WorkstreamsPeerSetupReadResult = Schema.Union([
  WorkstreamsPeerSetupContext,
  closed(
    Schema.Struct({
      contract,
      operation: Schema.Literal("source_intent"),
      request: IntentRead,
      request_body: WorkstreamsPeerSetupRequestBody,
    }),
  ),
]);
export type WorkstreamsPeerSetupReadResult = typeof WorkstreamsPeerSetupReadResult.Type;
export const WorkstreamsPeerSetupApplyRequest = closed(
  Schema.Struct({
    contract,
    operation: Schema.Literals(["prepare_home", "accept_member", "resume_member"]),
    ...WorkstreamsPeerSetupIdentity.fields,
    receiver_environment_id: Id,
  }),
);
export type WorkstreamsPeerSetupApplyRequest = typeof WorkstreamsPeerSetupApplyRequest.Type;
export const WorkstreamsPeerSetupResult = closed(
  Schema.Struct({
    contract,
    ...WorkstreamsPeerSetupIdentity.fields,
    state: Schema.Literals(["retained", "prepared", "bound", "unknown"]),
    enrollment_id: Id,
    bind_id: Id,
  }),
);
export type WorkstreamsPeerSetupResult = typeof WorkstreamsPeerSetupResult.Type;
export const WorkstreamsPeerSetupPrepare = closed(
  Schema.Struct({
    ...WorkstreamsPeerSetupAddress.fields,
    lifetime_days: Schema.Int.check(
      Schema.isGreaterThanOrEqualTo(1),
      Schema.isLessThanOrEqualTo(30),
    ),
  }),
);
export type WorkstreamsPeerSetupPrepare = typeof WorkstreamsPeerSetupPrepare.Type;
export const WorkstreamsPeerSetupOwnerRequest = Schema.Union([
  closed(
    Schema.Struct({ operation: Schema.Literal("prepare"), ...WorkstreamsPeerSetupPrepare.fields }),
  ),
  closed(
    Schema.Struct({
      operation: Schema.Literals(["apply", "status", "resume"]),
      ...WorkstreamsPeerSetupIdentity.fields,
    }),
  ),
]);
export type WorkstreamsPeerSetupOwnerRequest = typeof WorkstreamsPeerSetupOwnerRequest.Type;

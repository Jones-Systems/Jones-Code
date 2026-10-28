import {
  WorkstreamsForwardIdentity,
  WorkstreamsForwardCommandId,
} from "./workstreamsForwarding.ts";
import * as Schema from "effect/Schema";
import {
  closedWorkstreamsNative as closed,
  WorkstreamsNativeCommandId,
  WorkstreamsNativeDigest,
  WorkstreamsNativeHomeTarget,
  WorkstreamsNativeId,
  WorkstreamsNativeVersion,
  WorkstreamsNativeTimestamp,
} from "./workstreamsNative.ts";
import {
  WorkstreamsSourceAuthority,
  WorkstreamsTransportCeiling,
} from "./workstreamsPeerAuthority.ts";

export const WORKSTREAMS_SOURCE_READMISSION_CONTRACT = "workstreams-source-readmission/1.0.0";
export const WorkstreamsSourceReadmissionRequest = closed(
  Schema.Struct({
    contract: Schema.Literal(WORKSTREAMS_SOURCE_READMISSION_CONTRACT),
    command_id: WorkstreamsNativeCommandId,
    request_sha256: WorkstreamsNativeDigest,
  }),
);
export type WorkstreamsSourceReadmissionRequest = typeof WorkstreamsSourceReadmissionRequest.Type;
const fields = {
  contract: Schema.Literal(WORKSTREAMS_SOURCE_READMISSION_CONTRACT),
  request: WorkstreamsSourceReadmissionRequest,
};
const rank = { "approval-required": 0, "auto-accept-edits": 1, auto: 2, "full-access": 3 };
export const WorkstreamsSourceReadmissionResponse = Schema.Union([
  closed(
    Schema.Struct({
      ...fields,
      state: Schema.Literal("authorized"),
      expected_home: WorkstreamsNativeHomeTarget,
      source_authority: WorkstreamsSourceAuthority,
      source_enrollment_id: WorkstreamsNativeId,
      singleton_binding_version: WorkstreamsNativeVersion,
      outbound_link_generation: WorkstreamsNativeVersion,
      principal_id: WorkstreamsNativeId,
      source_binding_version: WorkstreamsNativeVersion,
      home_authorization_revision: WorkstreamsNativeVersion,
      recorded_caller_sha256: WorkstreamsNativeDigest,
      original_ceiling: WorkstreamsTransportCeiling,
      current_ceiling: WorkstreamsTransportCeiling,
      observed_at: WorkstreamsNativeTimestamp,
    }),
  ).check(
    Schema.makeFilter(
      (r) =>
        rank[r.current_ceiling.runtime_mode] <= rank[r.original_ceiling.runtime_mode] &&
        (r.original_ceiling.interaction_mode !== "plan" ||
          r.current_ceiling.interaction_mode === "plan") &&
        (r.original_ceiling.access !== "read-only" || r.current_ceiling.access === "read-only") &&
        (r.current_ceiling.access === "read-only" ||
          (r.original_ceiling.access !== "read-only" &&
            rank[r.current_ceiling.access] <= rank[r.original_ceiling.access])) &&
        r.current_ceiling.scopes.every((s) => r.original_ceiling.scopes.includes(s)) &&
        r.current_ceiling.permissions.every((s) => r.original_ceiling.permissions.includes(s)),
    ),
  ),
  closed(
    Schema.Struct({
      ...fields,
      state: Schema.Literal("denied"),
      reason: Schema.Literals([
        "not_found",
        "not_granted",
        "caller_changed",
        "source_changed",
        "home_changed",
        "request_conflict",
        "historical_evidence_missing",
      ]),
    }),
  ),
  closed(
    Schema.Struct({
      ...fields,
      state: Schema.Literal("unavailable"),
      reason: Schema.Literals([
        "authority_unavailable",
        "home_unreachable",
        "key_unavailable",
        "provider_unconfigured",
      ]),
    }),
  ),
]);
export type WorkstreamsSourceReadmissionResponse = typeof WorkstreamsSourceReadmissionResponse.Type;
// A distinct retained origin, never a fabricated home-local RecordedCallerV1.
export const WorkstreamsPeerEffectRecoveryV1 = closed(
  Schema.Struct({
    schema: Schema.Literal("jones-workstreams-peer-effect-recovery/1"),
    kind: Schema.Literal("source-recorded-forward"),
    incoming_peer_pin_id: WorkstreamsNativeId,
    incoming_peer_pin_revision: WorkstreamsNativeVersion,
    source_environment_id: WorkstreamsNativeId,
    source_operation_id: WorkstreamsNativeId,
    command_id: WorkstreamsNativeCommandId,
    request_sha256: WorkstreamsNativeDigest,
    expected_home: WorkstreamsNativeHomeTarget,
    original_ceiling: WorkstreamsTransportCeiling,
  }),
);
export type WorkstreamsPeerEffectRecoveryV1 = typeof WorkstreamsPeerEffectRecoveryV1.Type;

// V2 names the source-owned accepted record. The home cannot supply caller JSON or
// assert current home authorization through this lookup-only protocol.
export const WORKSTREAMS_SOURCE_READMISSION_CONTRACT_V2 = "workstreams-source-readmission/2.0.0";
export const WorkstreamsSourceReadmissionRequestV2 = closed(
  Schema.Struct({
    contract: Schema.Literal(WORKSTREAMS_SOURCE_READMISSION_CONTRACT_V2),
    accepted_forward: WorkstreamsForwardIdentity,
    command_id: WorkstreamsForwardCommandId,
    command_request_sha256: WorkstreamsNativeDigest,
  }),
).check(Schema.makeFilter((r) => r.accepted_forward.command_ids.includes(r.command_id)));
export type WorkstreamsSourceReadmissionRequestV2 =
  typeof WorkstreamsSourceReadmissionRequestV2.Type;
const v2Fields = {
  contract: Schema.Literal(WORKSTREAMS_SOURCE_READMISSION_CONTRACT_V2),
  request: WorkstreamsSourceReadmissionRequestV2,
};
const ceilingNarrowed = (
  original: WorkstreamsTransportCeiling,
  current: WorkstreamsTransportCeiling,
) =>
  rank[current.runtime_mode] <= rank[original.runtime_mode] &&
  (original.interaction_mode !== "plan" || current.interaction_mode === "plan") &&
  (original.access !== "read-only" || current.access === "read-only") &&
  (current.access === "read-only" ||
    (original.access !== "read-only" && rank[current.access] <= rank[original.access])) &&
  current.scopes.every((s) => original.scopes.includes(s)) &&
  current.permissions.every((p) => original.permissions.includes(p));
export const WorkstreamsSourceReadmissionResponseV2 = Schema.Union([
  closed(
    Schema.Struct({
      ...v2Fields,
      state: Schema.Literal("authorized"),
      original_ceiling: WorkstreamsTransportCeiling,
      current_ceiling: WorkstreamsTransportCeiling,
      observed_at: WorkstreamsNativeTimestamp,
    }),
  ).check(Schema.makeFilter((r) => ceilingNarrowed(r.original_ceiling, r.current_ceiling))),
  closed(
    Schema.Struct({
      ...v2Fields,
      state: Schema.Literal("denied"),
      reason: Schema.Literals([
        "not_found",
        "not_granted",
        "caller_changed",
        "source_changed",
        "home_changed",
        "request_conflict",
        "historical_evidence_missing",
      ]),
    }),
  ),
  closed(
    Schema.Struct({
      ...v2Fields,
      state: Schema.Literal("unavailable"),
      reason: Schema.Literals([
        "authority_unavailable",
        "home_unreachable",
        "key_unavailable",
        "provider_unconfigured",
        "unsupported_protocol",
      ]),
    }),
  ),
]);
export type WorkstreamsSourceReadmissionResponseV2 =
  typeof WorkstreamsSourceReadmissionResponseV2.Type;
export const WorkstreamsPeerEffectRecoveryV2 = closed(
  Schema.Struct({
    schema: Schema.Literal("jones-workstreams-peer-effect-recovery/2"),
    kind: Schema.Literal("source-recorded-forward"),
    incoming_peer_pin_id: WorkstreamsNativeId,
    incoming_peer_pin_revision: WorkstreamsNativeVersion,
    source_readmission: WorkstreamsSourceReadmissionRequestV2,
    original_ceiling: WorkstreamsTransportCeiling,
  }),
);
export type WorkstreamsPeerEffectRecoveryV2 = typeof WorkstreamsPeerEffectRecoveryV2.Type;

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

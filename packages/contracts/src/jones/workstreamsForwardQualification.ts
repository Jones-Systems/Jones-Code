import * as Schema from "effect/Schema";
import { WorkstreamsForwardIdentity } from "./workstreamsForwarding.ts";
import {
  closedWorkstreamsNative as closed,
  WorkstreamsNativeDigest,
  WorkstreamsNativeId,
  WorkstreamsNativeTimestamp,
  WorkstreamsNativeVersion,
} from "./workstreamsNative.ts";
import { WorkstreamsTransportCeiling } from "./workstreamsPeerAuthority.ts";

export const WORKSTREAMS_SOURCE_READMISSION_CONTRACT_V3 = "workstreams-source-readmission/3.0.0";

const PinId = WorkstreamsNativeId.check(
  Schema.isPattern(/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$(?![\s\S])/),
);
const Digest = WorkstreamsNativeDigest.check(Schema.isMaxLength(64));
const Timestamp = WorkstreamsNativeTimestamp.check(Schema.isPattern(/Z$(?![\s\S])/));

/**
 * Parent-only lookup of the complete source-retained forward. It does not attest
 * home-constructed child bytes; exact raw command authority remains explicit V2.
 * outbound_link_generation is the opaque member → home generation, never the
 * opposite home → source generation or numeric server_generation.
 */
export const WorkstreamsSourceParentReadmissionRequestV3 = closed(
  Schema.Struct({
    contract: Schema.Literal(WORKSTREAMS_SOURCE_READMISSION_CONTRACT_V3),
    accepted_forward: WorkstreamsForwardIdentity,
    receiving_home_pin_id: PinId,
    receiving_home_pin_revision: WorkstreamsNativeVersion,
  }),
);
export type WorkstreamsSourceParentReadmissionRequestV3 =
  typeof WorkstreamsSourceParentReadmissionRequestV3.Type;

const fields = {
  contract: Schema.Literal(WORKSTREAMS_SOURCE_READMISSION_CONTRACT_V3),
  request: WorkstreamsSourceParentReadmissionRequestV3,
};
const rank = { "approval-required": 0, "auto-accept-edits": 1, auto: 2, "full-access": 3 };

/**
 * Echoes the complete request. forward_request_sha256 hashes the entire retained
 * serialized forwarding envelope, including supplied evidence, independently of
 * accepted_forward.request_sha256 (the raw inner body). The consumer must compare
 * the echo and retained bytes; decoding alone proves neither authenticity nor
 * digest equality. Authorization is a fresh observation, not a remote revocation lock.
 */
export const WorkstreamsSourceParentReadmissionResponseV3 = Schema.Union([
  closed(
    Schema.Struct({
      ...fields,
      state: Schema.Literal("authorized"),
      forward_request_sha256: Digest,
      original_ceiling: WorkstreamsTransportCeiling,
      current_ceiling: WorkstreamsTransportCeiling,
      observed_at: Timestamp,
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
        r.current_ceiling.permissions.every((p) => r.original_ceiling.permissions.includes(p)),
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
        "unsupported_protocol",
      ]),
    }),
  ),
]);
export type WorkstreamsSourceParentReadmissionResponseV3 =
  typeof WorkstreamsSourceParentReadmissionResponseV3.Type;

import * as Schema from "effect/Schema";
import { WorkstreamsSourceReadmissionRequestV2 } from "./workstreamsForwardReadmission.ts";
import {
  closedWorkstreamsNative as closed,
  WorkstreamsNativeId,
  WorkstreamsNativeVersion,
} from "./workstreamsNative.ts";
import {
  WorkstreamsNativeAttestationRequest,
  WorkstreamsNativeAttestationResponseV2,
  WorkstreamsNativeSettlementLookupResponseV2,
  WorkstreamsNativeSettlementRequest,
  WorkstreamsNativeSettlementResponseV2,
} from "./workstreamsNativeProvider.ts";
import { canonicalWorkstreamsSnapshotJson as canonical } from "./workstreamsSnapshot.ts";

export const WORKSTREAMS_SOURCE_RETAINED_EFFECT_CONTRACT_V1 =
  "workstreams-source-retained-effect/1.0.0";

const fields = {
  contract: Schema.Literal(WORKSTREAMS_SOURCE_RETAINED_EFFECT_CONTRACT_V1),
  source_readmission: WorkstreamsSourceReadmissionRequestV2,
  receiving_home_pin_id: WorkstreamsNativeId.check(
    Schema.isPattern(/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$(?![\s\S])/),
  ),
  receiving_home_pin_revision: WorkstreamsNativeVersion,
};
const Attest = closed(
  Schema.Struct({
    ...fields,
    kind: Schema.Literal("attest"),
    request: WorkstreamsNativeAttestationRequest,
  }),
);
const SettleLookup = closed(
  Schema.Struct({
    ...fields,
    kind: Schema.Literal("settle_lookup"),
    request: WorkstreamsNativeSettlementRequest,
  }),
);
const Settle = closed(
  Schema.Struct({
    ...fields,
    kind: Schema.Literal("settle"),
    request: WorkstreamsNativeSettlementRequest,
  }),
);

/**
 * Fixed source-provider carrier for exact accepted raw commands. V2 is reused
 * unchanged; the source must validate the retained command bytes and provider
 * request before effects. High-level children and raw supplied evidence are
 * unsupported. Parent V3 qualification cannot substitute for this authority.
 */
export const WorkstreamsRetainedSourceEffectRequestV1 = Schema.Union([
  Attest,
  SettleLookup,
  Settle,
]);
export type WorkstreamsRetainedSourceEffectRequestV1 =
  typeof WorkstreamsRetainedSourceEffectRequestV1.Type;

const resultFields = {
  contract: Schema.Literal(WORKSTREAMS_SOURCE_RETAINED_EFFECT_CONTRACT_V1),
  state: Schema.Literal("result"),
};

/**
 * Echoes the full carrier and preserves the matching provider-V2 response.
 * Request-bearing provider responses must also echo the exact provider request.
 * The consumer still compares this carrier against its sent request; a decoded
 * record alone establishes no retained-record association or dispatch permission.
 */
export const WorkstreamsRetainedSourceEffectResultV1 = Schema.Union([
  closed(
    Schema.Struct({
      ...resultFields,
      request: Attest,
      result: WorkstreamsNativeAttestationResponseV2,
    }),
  ),
  closed(
    Schema.Struct({
      ...resultFields,
      request: SettleLookup,
      result: WorkstreamsNativeSettlementLookupResponseV2,
    }),
  ),
  closed(
    Schema.Struct({
      ...resultFields,
      request: Settle,
      result: WorkstreamsNativeSettlementResponseV2,
    }),
  ),
  closed(
    Schema.Struct({
      contract: Schema.Literal(WORKSTREAMS_SOURCE_RETAINED_EFFECT_CONTRACT_V1),
      request: WorkstreamsRetainedSourceEffectRequestV1,
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
      contract: Schema.Literal(WORKSTREAMS_SOURCE_RETAINED_EFFECT_CONTRACT_V1),
      request: WorkstreamsRetainedSourceEffectRequestV1,
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
]).check(
  Schema.makeFilter(
    (r) =>
      r.state !== "result" ||
      !("request" in r.result) ||
      canonical(r.request.request) === canonical(r.result.request),
  ),
);
export type WorkstreamsRetainedSourceEffectResultV1 =
  typeof WorkstreamsRetainedSourceEffectResultV1.Type;

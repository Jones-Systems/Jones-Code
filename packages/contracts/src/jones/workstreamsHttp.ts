import * as Schema from "effect/Schema";

export const WORKSTREAMS_HTTP_RESPONSE_CONTRACT_HEADER = "x-t3-workstreams-response-contract";
export const WORKSTREAMS_HTTP_RESPONSE_CONTRACT = "workstreams-http-response/1.0.0";
export const WorkstreamsHttpResponseContract = Schema.Literal(WORKSTREAMS_HTTP_RESPONSE_CONTRACT);

/**
 * Absent opt-in retains 200 for every receipt and workstream_command_not_current for stale refusals.
 * The supported opt-in permits 202 for pending/unresolved and version_conflict for version refusals.
 * Unsupported, empty, repeated/comma-joined or malformed values fail with an empty 400 before gateway work.
 * Receipt bodies and request identity are unchanged; current clients must also accept legacy responses.
 */
export const WorkstreamsHttpResponseContractHeaders = Schema.Struct({
  [WORKSTREAMS_HTTP_RESPONSE_CONTRACT_HEADER]: Schema.optionalKey(WorkstreamsHttpResponseContract),
});

export const workstreamsHttpResponseHeaders = {
  [WORKSTREAMS_HTTP_RESPONSE_CONTRACT_HEADER]: WORKSTREAMS_HTTP_RESPONSE_CONTRACT,
} as const;

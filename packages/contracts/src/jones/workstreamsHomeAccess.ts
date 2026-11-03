import * as Schema from "effect/Schema";
import {
  WorkstreamCapabilities,
  WorkstreamReadContext,
  WORKSTREAM_CONTRACT_MANIFEST_SHA256,
} from "../workstreams.ts";
import {
  closedWorkstreamsNative as closed,
  WorkstreamsNativeHomeTarget,
  WorkstreamsNativeRegistryContext,
  WorkstreamsNativeId,
  WorkstreamsNativeVersion,
  WorkstreamsNativeTimestamp,
} from "./workstreamsNative.ts";

export const WORKSTREAMS_HOME_ACCESS_CONTRACT = "workstreams-native-home-access/1.0.0";
export const WorkstreamsHomeAccessRequest = closed(
  Schema.Struct({
    contract: Schema.Literal(WORKSTREAMS_HOME_ACCESS_CONTRACT),
    expected_home: WorkstreamsNativeHomeTarget,
  }),
);
export type WorkstreamsHomeAccessRequest = typeof WorkstreamsHomeAccessRequest.Type;
const capabilities = closed(
  Schema.Struct({
    ...WorkstreamCapabilities.fields,
    context: closed(WorkstreamReadContext),
  }),
);
// This response is an observation for the current request, never a reusable authorization proof.
export const WorkstreamsHomeAccessResult = closed(
  Schema.Struct({
    contract: Schema.Literal(WORKSTREAMS_HOME_ACCESS_CONTRACT),
    home: WorkstreamsNativeRegistryContext,
    principal_id: WorkstreamsNativeId,
    authorization_revision: WorkstreamsNativeVersion,
    observed_at: WorkstreamsNativeTimestamp,
    capabilities,
  }),
).check(
  Schema.makeFilter(
    (r) =>
      r.capabilities.manifest_sha256 === WORKSTREAM_CONTRACT_MANIFEST_SHA256 &&
      r.capabilities.permissions.includes("workstreams:read") &&
      r.capabilities.context.owner_id === r.home.owner_id &&
      r.capabilities.context.server_generation === r.home.server_generation &&
      r.capabilities.context.registry_version === r.home.registry_version,
  ),
);
export type WorkstreamsHomeAccessResult = typeof WorkstreamsHomeAccessResult.Type;

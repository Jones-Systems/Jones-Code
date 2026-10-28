import { expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import {
  WorkstreamsSourceReadmissionRequest,
  WorkstreamsSourceReadmissionResponse,
  WorkstreamsPeerEffectRecoveryV1,
  WORKSTREAMS_SOURCE_READMISSION_CONTRACT as contract,
} from "./workstreamsForwardReadmission.ts";
const request = { contract, command_id: "command-123456789", request_sha256: "a".repeat(64) };
const home = {
  registry_instance_id: "12345678-1234-1234-1234-123456789abc",
  owner_id: "owner",
  home_environment_id: "home",
  server_generation: 1,
};
const original = {
  access: "full-access",
  runtime_mode: "auto",
  interaction_mode: "default",
  scopes: ["orchestration:read", "orchestration:operate"],
  permissions: ["workstreams:read", "workstreams:write"],
};
const narrow = {
  access: "read-only",
  runtime_mode: "approval-required",
  interaction_mode: "plan",
  scopes: ["orchestration:read"],
  permissions: ["workstreams:read"],
};
const authorized = {
  contract,
  request,
  state: "authorized",
  expected_home: home,
  source_authority: {
    environment_id: "source",
    authority_namespace: "source-store",
    store_generation: 4,
  },
  source_enrollment_id: "enrollment",
  singleton_binding_version: 2,
  outbound_link_generation: 3,
  principal_id: "principal",
  source_binding_version: 2,
  home_authorization_revision: 5,
  recorded_caller_sha256: "b".repeat(64),
  original_ceiling: original,
  current_ceiling: narrow,
  observed_at: "2026-10-26T12:00:00Z",
};
it("negotiates a distinct read-only authorization lookup and retains source identity and original ceiling", () => {
  expect(decodeRequest(request)).toEqual(request);
  const decode = decodeResponse;
  expect(decode(authorized)).toEqual(authorized);
  for (const result of [
    { contract, request, state: "denied", reason: "caller_changed" },
    { contract, request, state: "unavailable", reason: "home_unreachable" },
  ])
    expect(decode(result)).toEqual(result);
  const proof = {
    schema: "jones-workstreams-peer-effect-recovery/1",
    kind: "source-recorded-forward",
    incoming_peer_pin_id: "pin",
    incoming_peer_pin_revision: 2,
    source_environment_id: "source",
    source_operation_id: "operation",
    command_id: request.command_id,
    request_sha256: request.request_sha256,
    expected_home: home,
    original_ceiling: original,
  };
  expect(decodeRecovery(proof)).toEqual(proof);
});
it("rejects broadened recovery ceilings, dispatch arguments and false absence outcomes", () => {
  const decode = decodeResponse;
  for (const result of [
    { ...authorized, current_ceiling: { ...original, runtime_mode: "full-access" } },
    { ...authorized, original_ceiling: narrow, current_ceiling: original },
    { ...authorized, current_ceiling: { ...narrow, scopes: ["settings:write"] } },
    { contract, request, state: "proven_absent" },
    { ...authorized, dispatch: true },
  ])
    expect(() => decode(result)).toThrow();
  for (const invalid of [
    { ...request, command_id: "short" },
    { ...request, dispatch: true },
    { ...request, request_sha256: "abc" },
    { ...request, expected_home: home },
    { ...request, contract: "workstreams-native/1.0.0" },
  ])
    expect(() => decodeRequest(invalid)).toThrow();
});

const decodeRequest = Schema.decodeUnknownSync(WorkstreamsSourceReadmissionRequest);
const decodeResponse = Schema.decodeUnknownSync(WorkstreamsSourceReadmissionResponse);
const decodeRecovery = Schema.decodeUnknownSync(WorkstreamsPeerEffectRecoveryV1);

import {
  WORKSTREAMS_SOURCE_READMISSION_CONTRACT_V2 as contractV2,
  WorkstreamsSourceReadmissionRequestV2,
  WorkstreamsSourceReadmissionResponseV2,
  WorkstreamsPeerEffectRecoveryV2,
} from "./workstreamsForwardReadmission.ts";
const acceptedForward = {
  forward_id: "forward-original-0001",
  source_operation_id: "operation-original-0001",
  parent_operation_id: null,
  command_ids: [request.command_id],
  request_sha256: request.request_sha256,
  recorded_caller_sha256: authorized.recorded_caller_sha256,
  expected_home: home,
  canonical_origin: "https://home.invalid",
  source_authority: authorized.source_authority,
  source_enrollment_id: "enrollment",
  source_enrollment_session_id: "enrollment-session",
  principal_id: "principal",
  singleton_binding_version: 2,
  source_binding_version: 2,
  member_pin_id: "member-pin",
  member_pin_bound_revision: 2,
  outbound_link_generation: "opaque:generation/β",
};
const requestV2 = {
  contract: contractV2,
  accepted_forward: acceptedForward,
  command_id: request.command_id,
  command_request_sha256: request.request_sha256,
};
const decodeV2 = Schema.decodeUnknownSync(WorkstreamsSourceReadmissionResponseV2);
const decodeRequestV2 = Schema.decodeUnknownSync(WorkstreamsSourceReadmissionRequestV2);
const decodeRecoveryV2 = Schema.decodeUnknownSync(WorkstreamsPeerEffectRecoveryV2);
it("readmission V2 retains source-selected forward identity and string generation without a current home grant", () => {
  expect(decodeRequestV2(requestV2)).toEqual(requestV2);
  const response = {
    contract: contractV2,
    request: requestV2,
    state: "authorized",
    original_ceiling: original,
    current_ceiling: narrow,
    observed_at: authorized.observed_at,
  };
  expect(decodeV2(response)).toEqual(response);
  expect(response).not.toHaveProperty("home_authorization_revision");
  const recovery = {
    schema: "jones-workstreams-peer-effect-recovery/2",
    kind: "source-recorded-forward",
    incoming_peer_pin_id: "home-pin",
    incoming_peer_pin_revision: 3,
    source_readmission: requestV2,
    original_ceiling: original,
  };
  expect(decodeRecoveryV2(recovery)).toEqual(recovery);
  expect(decodeResponse(authorized)).toEqual(authorized);
  expect(() => decodeResponse(response)).toThrow();
  for (const invalid of [
    { ...response, home_authorization_revision: 100 },
    { ...response, current_ceiling: { ...original, runtime_mode: "full-access" } },
    { ...response, original_ceiling: narrow, current_ceiling: original },
    { ...response, state: "proven_absent" },
  ])
    expect(() => decodeV2(invalid)).toThrow();
});
it("readmission V2 cannot select original evidence using home-supplied caller JSON or replacement IDs", () => {
  for (const invalid of [
    { ...requestV2, caller: { kind: "agent" } },
    { ...requestV2, command_id: "command-replacement-0001" },
    { ...requestV2, dispatch: true },
    { ...requestV2, contract },
    { ...requestV2, accepted_forward: { ...acceptedForward, outbound_link_generation: 4 } },
    { ...requestV2, accepted_forward: { ...acceptedForward, forward_id: "a".repeat(16) + "\n" } },
  ])
    expect(() => decodeRequestV2(invalid)).toThrow();
  expect(
    decodeV2({
      contract: contractV2,
      request: requestV2,
      state: "unavailable",
      reason: "unsupported_protocol",
    }),
  ).toMatchObject({ state: "unavailable" });
});

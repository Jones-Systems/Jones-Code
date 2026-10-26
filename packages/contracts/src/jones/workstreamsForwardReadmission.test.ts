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

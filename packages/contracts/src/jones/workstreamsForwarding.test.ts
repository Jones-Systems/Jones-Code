import { expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import * as Forward from "./workstreamsForwarding.ts";
import * as Native from "./workstreamsNative.ts";
import { WorkstreamOperationResult } from "./workstreamOperations.ts";

const home = {
  registry_instance_id: "12345678-1234-1234-1234-123456789abc",
  owner_id: "owner",
  home_environment_id: "home",
  server_generation: 1,
};
const identity = {
  forward_id: "forward-original-0001",
  source_operation_id: "operation-original-0001",
  parent_operation_id: "operation-parent-0001",
  command_ids: ["command-original-0001"],
  request_sha256: "a".repeat(64),
  recorded_caller_sha256: "b".repeat(64),
  expected_home: home,
  canonical_origin: "https://home.invalid",
  source_authority: { environment_id: "source", authority_namespace: "store", store_generation: 1 },
  source_enrollment_id: "enrollment",
  source_enrollment_session_id: "enrollment-session",
  principal_id: "principal",
  singleton_binding_version: 2,
  source_binding_version: 3,
  member_pin_id: "member-pin",
  member_pin_bound_revision: 4,
  outbound_link_generation: "opaque:link/β",
};
const relay = {
  source_environment_id: "source",
  origin: { kind: "app" },
  linked_origin_environment_id: null,
  hop: 1,
};
const command = {
  command_id: identity.command_ids[0],
  expected_server_generation: 1,
  expected_registry_version: 0,
  action: {
    operation: "create_workstream",
    name: "Synthetic",
    lifecycle: "active",
    progress: { state: "unknown" },
    sort_order: 0,
  },
};
const raw = JSON.stringify(command, null, 2) + "\n";
const request = {
  contract: Forward.WORKSTREAMS_NATIVE_FORWARDING_CONTRACT,
  kind: "command",
  identity,
  request: { expected_home: home, relay, body: raw, body_sha256: identity.request_sha256 },
};
const decode = Schema.decodeUnknownSync(Forward.WorkstreamsForwardRequest);
const decodeHomeCommand = Schema.decodeUnknownSync(Native.WorkstreamsNativeHomeCommand);
const decodeResult = Schema.decodeUnknownSync(Forward.WorkstreamsForwardResult);
const decodeReadback = Schema.decodeUnknownSync(Forward.WorkstreamsForwardReadbackResult);
const isCommandId = Schema.is(Forward.WorkstreamsForwardCommandId);
const decodePublicResult = Schema.decodeUnknownSync(WorkstreamOperationResult);

it("retains exact raw bytes and opaque string generation while composing frozen home command", () => {
  expect(decode(request)).toEqual(request);
  expect(Forward.decodeWorkstreamsForwardBody(JSON.stringify(request))).toEqual(request);
  expect(decodeHomeCommand(request.request)).toEqual(request.request);
  for (const invalid of [
    { ...request, contract: undefined },
    { ...request, contract: "workstreams-native-forwarding/2.0.0" },
    { ...request, dispatch: true },
    { ...request, identity: { ...identity, outbound_link_generation: 1 } },
    { ...request, identity: { ...identity, caller_json: {} } },
    { ...request, identity: { ...identity, command_ids: ["command-another-0001"] } },
    {
      ...request,
      request: { ...request.request, expected_home: { ...home, server_generation: 2 } },
    },
    { ...request, request: { ...request.request, relay: { ...relay, hop: 2 } } },
    {
      ...request,
      request: { ...request.request, relay: { ...relay, source_environment_id: "foreign" } },
    },
  ])
    expect(() => decode(invalid)).toThrow();
  for (const id of [
    "short",
    "a".repeat(129),
    "a".repeat(16) + "\n",
    "a".repeat(16) + "\r",
    "-".repeat(16),
  ])
    expect(isCommandId(id)).toBe(false);
  for (const id of ["a".repeat(16), "a".repeat(128), "valid:command-00001"])
    expect(isCommandId(id)).toBe(true);
});
it("negotiates operation and appearance without creating another command protocol", () => {
  const operation = {
    contract: "workstream-operations/1.0.0",
    operation_id: identity.source_operation_id,
    origin: { kind: "app" },
    kind: "create_workstream",
    registry: { registry_id: home.registry_instance_id, owner_id: home.owner_id },
    workstream: {
      name: "Synthetic",
      lifecycle: "active",
      progress: { state: "unknown" },
      sort_order: 0,
    },
  };
  const op = {
    contract: request.contract,
    kind: "operation",
    identity,
    request: {
      expected_home: home,
      relay,
      request: operation,
      request_sha256: identity.request_sha256,
    },
    request_body: JSON.stringify(operation, null, 2) + "\n",
  };
  expect(decode(op)).toEqual(op);
  expect(() =>
    decode({
      ...op,
      request_body: JSON.stringify({ ...operation, operation_id: "operation-other-0001" }),
    }),
  ).toThrow();
  const appearance = {
    command_id: identity.command_ids[0],
    workstream_id: "ws",
    expected_server_generation: 1,
    expected_version: 0,
    border_color: "#AABBCC",
  };
  const app = {
    contract: request.contract,
    kind: "appearance",
    identity,
    request: {
      expected_home: home,
      relay,
      request: appearance,
      request_body: JSON.stringify(appearance),
      request_sha256: identity.request_sha256,
    },
  };
  expect(decode(app)).toEqual(app);
  expect(() =>
    decode({ ...app, request: { ...app.request, request: { ...appearance, extra: true } } }),
  ).toThrow();
});
it("keeps typed home provenance separate from the unchanged public operation result", () => {
  const result = {
    contract: "workstream-operations/1.0.0",
    operation_id: identity.source_operation_id,
    origin: { kind: "app" },
    kind: "create_workstream",
    request_sha256: identity.request_sha256,
    accepted_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
    observed: null,
    children: [],
    result: null,
    poll_after_ms: null,
    detail: null,
    state: "unavailable",
    reason: "not_configured",
  };
  const native = {
    contract: request.contract,
    identity,
    kind: "operation",
    result,
    home: {
      registry: { ...home, canonical_origin: identity.canonical_origin, registry_version: 8 },
      principal_id: identity.principal_id,
      authorization_revision: 9,
      source_binding_version: 3,
      incoming_peer_pin_id: "home-pin",
      incoming_peer_pin_revision: 7,
    },
  };
  expect(decodePublicResult(result)).toEqual(result);
  expect(decodeResult(native)).toEqual(native);
  expect(() => decodePublicResult({ ...result, home: native.home })).toThrow();
  expect(() =>
    decodeResult({ ...native, home: { ...native.home, authorization_revision: 0 } }),
  ).toThrow();
  expect(() =>
    decodeResult({ ...native, home: { ...native.home, principal_id: "other" } }),
  ).toThrow();
  expect(
    decodeReadback({ contract: request.contract, identity, state: "present", result: native }),
  ).toMatchObject({ state: "present" });
  expect(() =>
    decodeReadback({
      contract: request.contract,
      identity: { ...identity, forward_id: "forward-replacement-0001" },
      state: "present",
      result: native,
    }),
  ).toThrow();
  expect(
    decodeReadback({ contract: request.contract, identity, state: "unknown", reason: "not_found" }),
  ).toMatchObject({ state: "unknown" });
  expect(() =>
    decodeReadback({ contract: request.contract, identity, state: "proven_absent" }),
  ).toThrow();
});

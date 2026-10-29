// @effect-diagnostics nodeBuiltinImport:off -- Hashes synthetic retained wire bytes in contract tests.
import * as NodeCrypto from "node:crypto";
import { expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import {
  WORKSTREAMS_SOURCE_READMISSION_CONTRACT_V3 as contract,
  WorkstreamsSourceParentReadmissionRequestV3,
  WorkstreamsSourceParentReadmissionResponseV3,
} from "./workstreamsForwardQualification.ts";
import {
  WORKSTREAMS_SOURCE_READMISSION_CONTRACT_V2 as contractV2,
  WorkstreamsSourceReadmissionRequest,
  WorkstreamsSourceReadmissionRequestV2,
  WorkstreamsSourceReadmissionResponseV2,
} from "./workstreamsForwardReadmission.ts";
import {
  WORKSTREAMS_NATIVE_FORWARDING_CONTRACT,
  WorkstreamsForwardIdentity,
  WorkstreamsForwardRequest,
} from "./workstreamsForwarding.ts";
import { WorkstreamsTransportCeiling } from "./workstreamsPeerAuthority.ts";

const identity = {
  forward_id: "jwf-forward-original-0001",
  source_operation_id: "operation-original-0001",
  parent_operation_id: "operation-parent-0001",
  command_ids: ["command-original-0001"],
  request_sha256: "a".repeat(64),
  recorded_caller_sha256: "b".repeat(64),
  expected_home: {
    registry_instance_id: "12345678-1234-1234-1234-123456789abc",
    owner_id: "owner",
    home_environment_id: "home",
    server_generation: 7,
  },
  canonical_origin: "https://home.invalid",
  source_authority: { environment_id: "source", authority_namespace: "store", store_generation: 3 },
  source_enrollment_id: "enrollment",
  source_enrollment_session_id: "enrollment-session",
  principal_id: "principal",
  singleton_binding_version: 2,
  source_binding_version: 3,
  member_pin_id: "member-pin",
  member_pin_bound_revision: 4,
  outbound_link_generation: "member→home:opaque/β",
} satisfies WorkstreamsForwardIdentity;
const request = {
  contract,
  accepted_forward: identity,
  receiving_home_pin_id: "home-pin",
  receiving_home_pin_revision: 9,
} satisfies WorkstreamsSourceParentReadmissionRequestV3;
const original = {
  access: "full-access",
  runtime_mode: "auto",
  interaction_mode: "default",
  scopes: ["orchestration:read", "orchestration:operate"],
  permissions: ["workstreams:read", "workstreams:write"],
} satisfies WorkstreamsTransportCeiling;
const current = {
  access: "read-only",
  runtime_mode: "approval-required",
  interaction_mode: "plan",
  scopes: ["orchestration:read"],
  permissions: ["workstreams:read"],
} satisfies WorkstreamsTransportCeiling;
const authorized = {
  contract,
  request,
  state: "authorized",
  forward_request_sha256: "c".repeat(64),
  original_ceiling: original,
  current_ceiling: current,
  observed_at: "2026-10-28T12:00:00Z",
} satisfies WorkstreamsSourceParentReadmissionResponseV3;
const decodeRequest = Schema.decodeUnknownSync(WorkstreamsSourceParentReadmissionRequestV3);
const decodeResponse = Schema.decodeUnknownSync(WorkstreamsSourceParentReadmissionResponseV3);
const decodeRawReadmissionRequest = Schema.decodeUnknownSync(WorkstreamsSourceReadmissionRequestV2);
const decodeLegacyReadmissionRequest = Schema.decodeUnknownSync(
  WorkstreamsSourceReadmissionRequest,
);
const decodeRawReadmissionResponse = Schema.decodeUnknownSync(
  WorkstreamsSourceReadmissionResponseV2,
);
const hash = (body: string) => NodeCrypto.createHash("sha256").update(body).digest("hex");

it("echoes the complete frozen parent identity and distinct receiving-home pin without child authority", () => {
  expect(decodeRequest(request)).toEqual(request);
  expect(decodeResponse(authorized)).toEqual(authorized);
  expect(authorized.request.accepted_forward.member_pin_id).not.toBe(request.receiving_home_pin_id);
  expect(authorized.forward_request_sha256).not.toBe(identity.request_sha256);
  for (const key of Object.keys(identity)) {
    const reduced = { ...identity } as Record<string, unknown>;
    delete reduced[key];
    expect(() => decodeRequest({ ...request, accepted_forward: reduced })).toThrow();
  }
  for (const extra of [
    { command_id: identity.command_ids[0] },
    { command_request_sha256: identity.request_sha256 },
    { home_authorization_revision: 10 },
    { child_request_sha256: "d".repeat(64) },
  ]) {
    expect(() => decodeRequest({ ...request, ...extra })).toThrow();
    expect(() => decodeResponse({ ...authorized, ...extra })).toThrow();
  }
});

it("retains opaque directional generation strings without numeric conversion or opposite-side aliases", () => {
  for (const generation of ["opaque:member/home/β", "0007", "x".repeat(512)]) {
    const value = {
      ...request,
      accepted_forward: { ...identity, outbound_link_generation: generation },
    };
    expect(decodeRequest(value)).toEqual(value);
  }
  for (const generation of [7, "", "x".repeat(513)])
    expect(() =>
      decodeRequest({
        ...request,
        accepted_forward: { ...identity, outbound_link_generation: generation },
      }),
    ).toThrow();
  expect(() => decodeRequest({ ...request, source_link_generation: "home→source/γ" })).toThrow();
});

it("requires explicit V3 and closed bounded pin, digest, timestamp and nested identity fields", () => {
  for (const invalid of [
    { ...request, contract: undefined },
    { ...request, contract: contractV2 },
    { ...request, contract: "workstreams-source-readmission/3.0.0\n" },
    { ...request, contract: "workstreams-source-readmission/4.0.0" },
    { ...request, receiving_home_pin_id: undefined },
    { ...request, receiving_home_pin_id: "" },
    { ...request, receiving_home_pin_id: "x".repeat(129) },
    { ...request, receiving_home_pin_id: "home-pin\n" },
    { ...request, receiving_home_pin_revision: undefined },
    { ...request, receiving_home_pin_revision: 0 },
    { ...request, receiving_home_pin_revision: 1.5 },
    { ...request, receiving_home_pin_revision: Number.MAX_SAFE_INTEGER + 1 },
    { ...request, caller: {} },
    { ...request, accepted_forward: { ...identity, caller: {} } },
    {
      ...request,
      accepted_forward: {
        ...identity,
        expected_home: { ...identity.expected_home, url: "https://x.invalid" },
      },
    },
  ])
    expect(() => decodeRequest(invalid)).toThrow();
  for (const invalid of [
    { ...authorized, contract: contractV2 },
    { ...authorized, request: { ...request, receiving_home_pin_revision: 0 } },
    { ...authorized, forward_request_sha256: undefined },
    { ...authorized, forward_request_sha256: "A".repeat(64) },
    { ...authorized, forward_request_sha256: "c".repeat(64) + "\n" },
    { ...authorized, forward_request_sha256: "c".repeat(63) },
    { ...authorized, observed_at: "2026-02-29T00:00:00Z" },
    { ...authorized, observed_at: authorized.observed_at + "\n" },
    { ...authorized, observed_at: undefined },
    { ...authorized, current_ceiling: { ...current, grant: true } },
  ])
    expect(() => decodeResponse(invalid)).toThrow();
});

it("accepts only the selected denial and unavailability reasons and refuses authority-bearing extras", () => {
  for (const reason of [
    "not_found",
    "not_granted",
    "caller_changed",
    "source_changed",
    "home_changed",
    "request_conflict",
    "historical_evidence_missing",
  ]) {
    const result = { contract, request, state: "denied", reason };
    expect(decodeResponse(result)).toEqual(result);
    expect(() => decodeResponse({ ...result, forward_request_sha256: "c".repeat(64) })).toThrow();
  }
  for (const reason of [
    "authority_unavailable",
    "home_unreachable",
    "key_unavailable",
    "provider_unconfigured",
    "unsupported_protocol",
  ]) {
    const result = { contract, request, state: "unavailable", reason };
    expect(decodeResponse(result)).toEqual(result);
    expect(() => decodeResponse({ ...result, dispatch: true })).toThrow();
  }
  for (const invalid of [
    { contract, request, state: "denied", reason: "unsupported_protocol" },
    { contract, request, state: "unavailable", reason: "not_found" },
    { contract, request, state: "denied", reason: undefined },
    { contract, request, state: "proven_absent", reason: "not_found" },
    { ...authorized, reason: "not_found" },
  ])
    expect(() => decodeResponse(invalid)).toThrow();
});

it("permits equal or narrowed ceilings and refuses expansion in every authority dimension", () => {
  expect(decodeResponse({ ...authorized, current_ceiling: original })).toMatchObject({
    current_ceiling: original,
  });
  for (const invalid of [
    { ...authorized, current_ceiling: { ...original, runtime_mode: "full-access" } },
    { ...authorized, original_ceiling: current, current_ceiling: original },
    {
      ...authorized,
      original_ceiling: { ...original, access: "auto" },
      current_ceiling: original,
    },
    {
      ...authorized,
      original_ceiling: { ...original, interaction_mode: "plan" },
      current_ceiling: original,
    },
    { ...authorized, current_ceiling: { ...current, scopes: ["settings:write"] } },
    {
      ...authorized,
      original_ceiling: { ...original, permissions: ["workstreams:read"] },
      current_ceiling: original,
    },
  ])
    expect(() => decodeResponse(invalid)).toThrow();
});

it("carries the full serialized envelope digest including evidence separately from the raw parent digest", () => {
  const operation = {
    contract: "workstream-operations/1.0.0",
    operation_id: identity.source_operation_id,
    origin: { kind: "app" },
    kind: "create_workstream",
    registry: { registry_id: identity.expected_home.registry_instance_id, owner_id: "owner" },
    workstream: {
      name: "Synthetic",
      lifecycle: "active",
      progress: { state: "unknown" },
      sort_order: 0,
    },
  };
  const raw = JSON.stringify(operation, null, 2) + "\n";
  const parentIdentity = { ...identity, request_sha256: hash(raw) };
  const evidence = {
    protocol: "workstreams-t3-provider/2.0.0",
    state: "attested",
    request: {
      owner_id: "owner",
      principal_id: "principal",
      identity: {
        provider: "t3",
        source_instance_id: "source",
        resource_kind: "thread",
        id_kind: "internal",
        native_id: "thread",
        account_provenance: { kind: "not_account_scoped" },
      },
      expected_authority_namespace: "store",
      expected_store_generation: 3,
    },
    attestation: {
      provider: "t3",
      source_instance_id: "source",
      authority_namespace: "store",
      native_id: "thread",
      store_generation: 3,
      evidence_sha256: "d".repeat(64),
    },
    attested_at: "2026-10-28T12:00:00Z",
    expires_at: "2026-10-28T12:05:00Z",
  };
  const envelope = {
    contract: WORKSTREAMS_NATIVE_FORWARDING_CONTRACT,
    kind: "operation",
    identity: parentIdentity,
    request: {
      expected_home: identity.expected_home,
      relay: {
        source_environment_id: "source",
        origin: { kind: "app" },
        linked_origin_environment_id: null,
        hop: 1,
      },
      request: operation,
      request_sha256: hash(raw),
      evidence,
    },
    request_body: raw,
  };
  const changedEvidence = {
    ...envelope,
    request: {
      ...envelope.request,
      evidence: {
        ...evidence,
        attestation: { ...evidence.attestation, evidence_sha256: "e".repeat(64) },
      },
    },
  };
  const decodeForward = Schema.decodeUnknownSync(WorkstreamsForwardRequest);
  expect(decodeForward(envelope).identity).toEqual(decodeForward(changedEvidence).identity);
  const encoded = JSON.stringify(envelope);
  const digest = hash(encoded);
  expect(digest).not.toBe(hash(JSON.stringify(changedEvidence)));
  expect(digest).not.toBe(hash(JSON.stringify(envelope, null, 2)));
  expect(digest).not.toBe(parentIdentity.request_sha256);
  const response = {
    ...authorized,
    request: { ...request, accepted_forward: parentIdentity },
    forward_request_sha256: digest,
  };
  expect(decodeResponse(response)).toEqual(response);
  // A codec preserves the claim; the authenticated runtime must verify bytes and echo.
  expect(decodeResponse({ ...response, forward_request_sha256: "f".repeat(64) })).toMatchObject({
    forward_request_sha256: "f".repeat(64),
  });
});

it("keeps V1 and exact-command V2 readable while refusing aliases or fallback from parent V3", () => {
  const raw = {
    contract: contractV2,
    accepted_forward: identity,
    command_id: identity.command_ids[0],
    command_request_sha256: identity.request_sha256,
  };
  expect(decodeRawReadmissionRequest(raw)).toEqual(raw);
  const legacy = {
    contract: "workstreams-source-readmission/1.0.0",
    command_id: identity.command_ids[0],
    request_sha256: identity.request_sha256,
  };
  expect(decodeLegacyReadmissionRequest(legacy)).toEqual(legacy);
  expect(() => decodeRequest(raw)).toThrow();
  expect(() => decodeRawReadmissionRequest(request)).toThrow();
  expect(() => decodeRawReadmissionResponse(authorized)).toThrow();
  expect(() => decodeRequest({ ...request, contract: contractV2 })).toThrow();
});

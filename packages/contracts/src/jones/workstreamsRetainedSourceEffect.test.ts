import { expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import {
  WORKSTREAMS_SOURCE_RETAINED_EFFECT_CONTRACT_V1 as contract,
  WorkstreamsRetainedSourceEffectRequestV1,
  WorkstreamsRetainedSourceEffectResultV1,
} from "./workstreamsRetainedSourceEffect.ts";
import {
  WORKSTREAMS_SOURCE_READMISSION_CONTRACT_V2,
  WorkstreamsSourceReadmissionRequestV2,
} from "./workstreamsForwardReadmission.ts";
import {
  WORKSTREAMS_SOURCE_READMISSION_CONTRACT_V3,
  WorkstreamsSourceParentReadmissionRequestV3,
} from "./workstreamsForwardQualification.ts";
import {
  WORKSTREAMS_T3_PROVIDER_V2_PROTOCOL as protocol,
  WorkstreamsNativeAttestationRequest,
  WorkstreamsNativeAttestationResponseV2,
  WorkstreamsNativeSettlementRequest,
  WorkstreamsNativeSettlementResponseV2,
} from "./workstreamsNativeProvider.ts";

const sourceReadmission = {
  contract: WORKSTREAMS_SOURCE_READMISSION_CONTRACT_V2,
  accepted_forward: {
    forward_id: "jwf-forward-original-0001",
    source_operation_id: "operation-original-0001",
    parent_operation_id: null,
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
    source_authority: {
      environment_id: "source",
      authority_namespace: "store",
      store_generation: 3,
    },
    source_enrollment_id: "enrollment",
    source_enrollment_session_id: "enrollment-session",
    principal_id: "principal",
    singleton_binding_version: 2,
    source_binding_version: 3,
    member_pin_id: "member-pin",
    member_pin_bound_revision: 4,
    outbound_link_generation: "member→home:opaque/β",
  },
  command_id: "command-original-0001",
  command_request_sha256: "a".repeat(64),
} satisfies WorkstreamsSourceReadmissionRequestV2;
const nativeIdentity = {
  provider: "t3",
  source_instance_id: "source",
  resource_kind: "thread",
  id_kind: "internal",
  native_id: "thread",
  account_provenance: { kind: "not_account_scoped" },
} as const;
const attestationRequest = {
  owner_id: "owner",
  principal_id: "principal",
  identity: nativeIdentity,
  expected_authority_namespace: "store",
  expected_store_generation: 3,
} satisfies WorkstreamsNativeAttestationRequest;
const settlementRequest = {
  ...attestationRequest,
  command_id: sourceReadmission.command_id,
  native_reference_id: "reference",
  native_action: "settle",
  request_sha256: sourceReadmission.command_request_sha256,
  reservation_sha256: "c".repeat(64),
} satisfies WorkstreamsNativeSettlementRequest;
const common = {
  contract,
  source_readmission: sourceReadmission,
  receiving_home_pin_id: "home-pin",
  receiving_home_pin_revision: 9,
} as const;
const attest = {
  ...common,
  kind: "attest",
  request: attestationRequest,
} satisfies WorkstreamsRetainedSourceEffectRequestV1;
const lookup = {
  ...common,
  kind: "settle_lookup",
  request: settlementRequest,
} satisfies WorkstreamsRetainedSourceEffectRequestV1;
const settle = {
  ...common,
  kind: "settle",
  request: settlementRequest,
} satisfies WorkstreamsRetainedSourceEffectRequestV1;
const attested = {
  protocol,
  state: "attested",
  request: attestationRequest,
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
} satisfies WorkstreamsNativeAttestationResponseV2;
const committed = {
  protocol,
  state: "terminal",
  request: settlementRequest,
  result: {
    native_outcome: "committed",
    native_evidence: {
      owner_id: settlementRequest.owner_id,
      principal_id: settlementRequest.principal_id,
      command_id: settlementRequest.command_id,
      native_reference_id: settlementRequest.native_reference_id,
      native_action: settlementRequest.native_action,
      request_sha256: settlementRequest.request_sha256,
      reservation_sha256: settlementRequest.reservation_sha256,
      result_sha256: "e".repeat(64),
    },
  },
  native_receipt: {
    commandId: "native-command",
    aggregateKind: "thread",
    aggregateId: "thread",
    acceptedAt: "2026-10-28T12:00:00Z",
    resultSequence: 1,
    status: "accepted",
  },
  settlement_event: {
    eventId: "event",
    commandId: "native-command",
    aggregateKind: "thread",
    aggregateId: "thread",
    sequence: 1,
    type: "thread.settled",
    occurredAt: "2026-10-28T12:00:00Z",
  },
} satisfies WorkstreamsNativeSettlementResponseV2;
const decodeRequest = Schema.decodeUnknownSync(WorkstreamsRetainedSourceEffectRequestV1);
const decodeResult = Schema.decodeUnknownSync(WorkstreamsRetainedSourceEffectResultV1);
const decodeReadmissionV2 = Schema.decodeUnknownSync(WorkstreamsSourceReadmissionRequestV2);
const decodeAttestationRequest = Schema.decodeUnknownSync(WorkstreamsNativeAttestationRequest);
const decodeSettlementRequest = Schema.decodeUnknownSync(WorkstreamsNativeSettlementRequest);
const decodeAttestationResponse = Schema.decodeUnknownSync(WorkstreamsNativeAttestationResponseV2);
const decodeSettlementResponse = Schema.decodeUnknownSync(WorkstreamsNativeSettlementResponseV2);
const decodeParentReadmission = Schema.decodeUnknownSync(
  WorkstreamsSourceParentReadmissionRequestV3,
);
const resultFor = (request: WorkstreamsRetainedSourceEffectRequestV1, result: unknown) => ({
  contract,
  request,
  state: "result",
  result,
});

it("reuses frozen V2 source readmission and provider requests without widening or stripping their identity", () => {
  for (const request of [attest, lookup, settle]) {
    expect(decodeRequest(request)).toEqual(request);
    expect(decodeReadmissionV2(request.source_readmission)).toEqual(sourceReadmission);
  }
  expect(decodeAttestationRequest(attest.request)).toEqual(attestationRequest);
  expect(decodeSettlementRequest(settle.request)).toEqual(settlementRequest);
  expect(decodeRequest(attest).source_readmission.accepted_forward.outbound_link_generation).toBe(
    "member→home:opaque/β",
  );
});

it("echoes the full carrier and matching unchanged provider-V2 results for every operation", () => {
  for (const [request, result] of [
    [attest, attested],
    [attest, { protocol, state: "unknown", reason: "observation_unavailable" }],
    [attest, { protocol, state: "rejected", reason: "caller_mismatch" }],
    [lookup, committed],
    [
      lookup,
      { protocol, state: "unknown", request: settlementRequest, reason: "attempt_not_found" },
    ],
    [lookup, { protocol, state: "rejected", reason: "authority_fenced" }],
    [settle, committed],
    [
      settle,
      {
        ...committed,
        result: { ...committed.result, native_outcome: "failed" },
        native_receipt: null,
        settlement_event: null,
      },
    ],
    [
      settle,
      { protocol, state: "unknown", request: settlementRequest, reason: "dispatch_in_progress" },
    ],
    [settle, { protocol, state: "rejected", reason: "source_mismatch" }],
  ] as const) {
    const value = resultFor(request, result);
    expect(decodeResult(value)).toEqual(value);
  }
  expect(decodeAttestationResponse(attested)).toEqual(attested);
  expect(decodeSettlementResponse(committed)).toEqual(committed);
});

it("refuses mismatched provider variants, protocols and request-bearing provider echoes", () => {
  for (const invalid of [
    resultFor(attest, committed),
    resultFor(lookup, attested),
    resultFor(settle, attested),
    resultFor(attest, { protocol, state: "unknown", reason: "attempt_not_found" }),
    resultFor(settle, { protocol, state: "unknown", reason: "observation_unavailable" }),
    resultFor(attest, { ...attested, protocol: "workstreams-t3-provider/1.0.0" }),
    resultFor(settle, { ...committed, protocol: "workstreams-t3-provider/3.0.0" }),
    resultFor(attest, { ...attested, request: { ...attestationRequest, principal_id: "other" } }),
    resultFor(settle, {
      ...committed,
      request: { ...settlementRequest, reservation_sha256: "f".repeat(64) },
    }),
    resultFor(lookup, {
      ...committed,
      request: { ...settlementRequest, native_action: "unsettle" },
    }),
    resultFor(lookup, { protocol, state: "unknown", reason: "attempt_not_found" }),
    { ...resultFor(attest, attested), response: attested },
    { ...resultFor(settle, committed), result: undefined },
  ])
    expect(() => decodeResult(invalid)).toThrow();
});

it("closes all carrier variants against caller, routing, supplied evidence and high-level child authority", () => {
  const parent = {
    contract: WORKSTREAMS_SOURCE_READMISSION_CONTRACT_V3,
    accepted_forward: sourceReadmission.accepted_forward,
    receiving_home_pin_id: common.receiving_home_pin_id,
    receiving_home_pin_revision: common.receiving_home_pin_revision,
  } satisfies WorkstreamsSourceParentReadmissionRequestV3;
  expect(decodeParentReadmission(parent)).toEqual(parent);
  for (const request of [attest, lookup, settle]) {
    for (const extra of [
      { caller: {} },
      { tool: "arbitrary" },
      { url: "https://other.invalid" },
      { credential: "synthetic" },
      { destination: "other" },
      { evidence: attested },
      { source_link_generation: "home→source/γ" },
      { child_request_sha256: "f".repeat(64) },
    ])
      expect(() => decodeRequest({ ...request, ...extra })).toThrow();
    for (const invalid of [
      { ...request, contract: undefined },
      { ...request, contract: "workstreams-source-retained-effect/2.0.0" },
      { ...request, contract: contract + "\n" },
      { ...request, kind: "operation" },
      { ...request, kind: "verify_reference" },
      { ...request, source_readmission: parent },
      { ...request, source_readmission: { ...sourceReadmission, caller: {} } },
      {
        ...request,
        source_readmission: { ...sourceReadmission, command_id: "command-other-0001" },
      },
      {
        ...request,
        source_readmission: {
          ...sourceReadmission,
          contract: "workstreams-source-readmission/1.0.0",
        },
      },
      { ...request, receiving_home_pin_id: undefined },
      { ...request, receiving_home_pin_id: "home-pin\n" },
      { ...request, receiving_home_pin_id: "x".repeat(129) },
      { ...request, receiving_home_pin_revision: 0 },
      { ...request, receiving_home_pin_revision: 1.5 },
      { ...request, receiving_home_pin_revision: Number.MAX_SAFE_INTEGER + 1 },
      { ...request, request: { ...request.request, evidence: attested } },
      { ...request, request: { ...request.request, caller: {} } },
      { ...request, request: undefined },
    ])
      expect(() => decodeRequest(invalid)).toThrow();
  }
  expect(() => decodeRequest({ ...attest, request: settlementRequest })).toThrow();
  expect(() => decodeRequest({ ...settle, request: attestationRequest })).toThrow();
  // The source's accepted-record check still has to establish exact raw bytes.
  // V2 command_ids membership alone cannot attest home-generated high-level children.
});

it("preserves exact denial and unavailable outcomes without treating absence as dispatch authority", () => {
  for (const request of [attest, lookup, settle]) {
    for (const reason of [
      "not_found",
      "not_granted",
      "caller_changed",
      "source_changed",
      "home_changed",
      "request_conflict",
      "historical_evidence_missing",
    ]) {
      const value = { contract, request, state: "denied", reason };
      expect(decodeResult(value)).toEqual(value);
      expect(() => decodeResult({ ...value, result: committed })).toThrow();
    }
    for (const reason of [
      "authority_unavailable",
      "home_unreachable",
      "key_unavailable",
      "provider_unconfigured",
      "unsupported_protocol",
    ]) {
      const value = { contract, request, state: "unavailable", reason };
      expect(decodeResult(value)).toEqual(value);
      expect(() => decodeResult({ ...value, dispatch: true })).toThrow();
    }
    for (const invalid of [
      { contract, request, state: "denied", reason: "attempt_not_found" },
      { contract, request, state: "denied", reason: "unsupported_protocol" },
      { contract, request, state: "unavailable", reason: "not_found" },
      { contract, request, state: "proven_absent" },
      { contract, request, state: "denied" },
      { contract, request, state: "result", result: committed, reason: "not_found" },
    ])
      expect(() => decodeResult(invalid)).toThrow();
  }
});

it("refuses excess properties throughout result echoes and unchanged provider records", () => {
  for (const invalid of [
    { ...resultFor(attest, attested), contract: undefined },
    {
      ...resultFor(attest, attested),
      request: { ...attest, receiving_home_pin_revision: undefined },
    },
    resultFor(attest, { ...attested, extra: true }),
    resultFor(attest, { ...attested, attestation: { ...attested.attestation, caller: {} } }),
    resultFor(settle, {
      ...committed,
      native_receipt: { ...committed.native_receipt, caller: {} },
    }),
    resultFor(lookup, {
      ...committed,
      request: { ...settlementRequest, identity: { ...nativeIdentity, destination: "home" } },
    }),
    resultFor(settle, {
      ...committed,
      result: {
        ...committed.result,
        native_evidence: { ...committed.result.native_evidence, dispatch: true },
      },
    }),
  ])
    expect(() => decodeResult(invalid)).toThrow();
});

// @effect-diagnostics nodeBuiltinImport:off -- Synthetic fixture digests use synchronous Node SHA-256; production codecs compute no witnesses.
import * as NodeCrypto from "node:crypto";
import { expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import {
  WORKSTREAMS_HOME_BIND_CONTRACT as contract,
  WorkstreamsHomeBindBodyV2,
  WorkstreamsHomeBindRequestV2,
  WorkstreamsHomeBindCommittedV2,
  WorkstreamsHomeBindReadbackRequestV2,
  WorkstreamsHomeBindReadbackV2,
  WorkstreamsHomeBindSetupReadbackV2,
  WorkstreamsSourceIssuanceRequestV1,
  WorkstreamsPeerPinSetupRequestV1,
  canonicalWorkstreamsHomeBindBody,
  decodeWorkstreamsHomeBindBody,
} from "./workstreamsHomeBind.ts";
import { WorkstreamsNativeHomeBindResult } from "./workstreamsNative.ts";
const decodeUnknownSyncWorkstreamsHomeBindBodyV2 =
  Schema.decodeUnknownSync(WorkstreamsHomeBindBodyV2);
const decodeUnknownSyncWorkstreamsHomeBindRequestV2 = Schema.decodeUnknownSync(
  WorkstreamsHomeBindRequestV2,
);
const encodeSyncWorkstreamsHomeBindRequestV2 = Schema.encodeSync(WorkstreamsHomeBindRequestV2);
const decodeUnknownSyncWorkstreamsSourceIssuanceRequestV1 = Schema.decodeUnknownSync(
  WorkstreamsSourceIssuanceRequestV1,
);
const decodeUnknownSyncWorkstreamsHomeBindCommittedV2 = Schema.decodeUnknownSync(
  WorkstreamsHomeBindCommittedV2,
);
const encodeSyncWorkstreamsHomeBindCommittedV2 = Schema.encodeSync(WorkstreamsHomeBindCommittedV2);
const decodeUnknownSyncWorkstreamsNativeHomeBindResult = Schema.decodeUnknownSync(
  WorkstreamsNativeHomeBindResult,
);
const decodeUnknownSyncWorkstreamsHomeBindReadbackRequestV2 = Schema.decodeUnknownSync(
  WorkstreamsHomeBindReadbackRequestV2,
);
const decodeUnknownSyncWorkstreamsHomeBindReadbackV2 = Schema.decodeUnknownSync(
  WorkstreamsHomeBindReadbackV2,
);
const decodeUnknownSyncWorkstreamsHomeBindSetupReadbackV2 = Schema.decodeUnknownSync(
  WorkstreamsHomeBindSetupReadbackV2,
);
const encodeSyncWorkstreamsHomeBindSetupReadbackV2 = Schema.encodeSync(
  WorkstreamsHomeBindSetupReadbackV2,
);
const decodeUnknownSyncWorkstreamsPeerPinSetupRequestV1 = Schema.decodeUnknownSync(
  WorkstreamsPeerPinSetupRequestV1,
);
const home = {
  registry_instance_id: "12345678-1234-1234-1234-123456789abc",
  owner_id: "owner",
  home_environment_id: "home",
  server_generation: 7,
};
const homeAuthority = {
  environment_id: "home",
  authority_namespace: "home-store",
  store_generation: 3,
};
const source = {
  environment_id: "member",
  authority_namespace: "source-store",
  store_generation: 4,
};
const sha = (body: string) => NodeCrypto.createHash("sha256").update(body).digest("hex");
const issuance = {
  schema: "jones-code.workstreams-native-enrollment/v1",
  enrollment_id: "enrollment-1",
  registry_origin: "https://home.invalid",
  context: {
    owner_id: "owner",
    principal_id: "principal",
    source_instance_id: source.environment_id,
    authority_namespace: source.authority_namespace,
    store_generation: source.store_generation,
    enrollment_id: "enrollment-1",
    protocol: "workstreams-t3-provider/1.0.0",
    build: { repository: "Jones-Systems/Jones-Code", sha: "a".repeat(40), tree: "b".repeat(40) },
  },
  session: {
    session_id: "provider-session",
    issued_at: "2026-10-27T00:00:00.000Z",
    expires_at: "2026-10-28T00:00:00.000Z",
  },
};
const assertion = (atHome: boolean) => ({
  schema: "jones-workstreams-peer-session-pin/1",
  pin_id: atHome ? "home-pin" : "member-pin",
  revision: 1,
  receiver_authority: atHome ? homeAuthority : source,
  session: {
    session_id: atHome ? "home-link-session" : "source-link-session",
    subject: "native-peer",
    method: "bearer-access-token",
    issued_at: "2026-10-27T00:00:00Z",
    expires_at: "2026-10-28T00:00:00Z",
    peer_link: true,
    ceiling: {
      access: "read-only",
      runtime_mode: "approval-required",
      interaction_mode: "default",
      scopes: ["orchestration:read"],
      permissions: ["workstreams:read"],
    },
  },
  peer_environment_id: atHome ? "member" : "home",
  roles: [atHome ? "member_home" : "home_source"],
  expected_home: home,
  source_authority: source,
  source_enrollment_id: issuance.enrollment_id,
  source_binding_version: null,
  singleton_binding_version: null,
  state: "prepared",
  expires_at: "2026-10-27T12:00:00Z",
  setup_request_id: atHome ? "home-setup" : "member-setup",
  owner_qualification_digest: "c".repeat(64),
  bind_receipt: null,
});
const expected = { registry_version: 9, grant_id: "grant", grant_version: 2 };
const setup = (atHome: boolean) => ({
  schema: "jones-workstreams-peer-pin-setup/1",
  assertion: assertion(atHome),
  expected,
});
// Deliberately noncanonical original bytes must survive encrypted-record round trips.
const homeSetupBody = JSON.stringify(setup(true), null, 2) + "\n";
const bodyValue = {
  bind_id: "bind-1",
  expected_home: home,
  expected_canonical_origin: issuance.registry_origin,
  expected_home_authority: homeAuthority,
  source: { authority: source, enrollment: issuance },
  home_setup: {
    pin_id: "home-pin",
    prepared_revision: 1,
    setup_request_id: "home-setup",
    setup_request_sha256: sha(homeSetupBody),
  },
  member_setup: {
    pin_id: "member-pin",
    prepared_revision: 1,
    setup_request_id: "member-setup",
    outbound_link_generation: "peer-link-secret:opaque-generation-v1",
    expected_singleton_version: null,
    singleton_binding_version: 1,
  },
  expected: { principal_id: "principal", ...expected, source_binding: null },
  rebind: false,
};
const decodeBody = decodeUnknownSyncWorkstreamsHomeBindBodyV2;
const body = () => canonicalWorkstreamsHomeBindBody(decodeBody(bodyValue));
const committed = () => ({
  contract,
  kind: "committed",
  request_body: body(),
  receipt: {
    principal_id: "principal",
    binding_version: 1,
    registry_version: 10,
    request_sha256: sha(body()),
  },
  home_pin_bound_revision: 2,
});
const homeSetup = () => ({
  contract,
  kind: "prepared",
  request: { contract, expected_home: home, home_setup_request_id: "home-setup" },
  canonical_origin: issuance.registry_origin,
  home_authority: homeAuthority,
  setup_request_body: homeSetupBody,
  pin: { ...assertion(true), setup_request_sha256: sha(homeSetupBody) },
});

it("canonicalizes closed bind intent and retains exactly one canonical byte preimage", () => {
  const b = decodeBody(bodyValue);
  const text = body();
  expect(decodeWorkstreamsHomeBindBody(text)).toEqual(b);
  expect(canonicalWorkstreamsHomeBindBody({ ...b, bind_id: b.bind_id })).toBe(text);
  const request = { contract, request_body: text };
  const decoded = decodeUnknownSyncWorkstreamsHomeBindRequestV2(request);
  expect(encodeSyncWorkstreamsHomeBindRequestV2(decoded)).toEqual(request);
  for (const request_body of [
    JSON.stringify(bodyValue),
    text + "\n",
    text.replace('"bind-1"', '"bind-1","bind_id":"bind-1"'),
  ])
    expect(() =>
      decodeUnknownSyncWorkstreamsHomeBindRequestV2({ contract, request_body }),
    ).toThrow();
  expect(() => decodeWorkstreamsHomeBindBody(" ".repeat(262_145))).toThrow();
});

it("refuses malformed selectors, versions, source/home/principal mismatches and non-successor singleton intent", () => {
  for (const b of [
    { ...bodyValue, bind_id: "" },
    { ...bodyValue, bind_id: "bad/id" },
    { ...bodyValue, expected_home: { ...home, registry_instance_id: "not-a-uuid" } },
    { ...bodyValue, expected_home: { ...home, owner_id: "other" } },
    { ...bodyValue, expected_home: { ...home, server_generation: 0 } },
    { ...bodyValue, expected_home_authority: { ...homeAuthority, environment_id: "other" } },
    {
      ...bodyValue,
      source: { ...bodyValue.source, authority: { ...source, store_generation: 8 } },
    },
    {
      ...bodyValue,
      source: { ...bodyValue.source, authority: { ...source, authority_namespace: "other" } },
    },
    { ...bodyValue, expected_canonical_origin: "https://other.invalid" },
    { ...bodyValue, expected: { ...bodyValue.expected, principal_id: "other" } },
    {
      ...bodyValue,
      expected: { ...bodyValue.expected, registry_version: Number.MAX_SAFE_INTEGER + 1 },
    },
    { ...bodyValue, home_setup: { ...bodyValue.home_setup, setup_request_sha256: "A".repeat(64) } },
    { ...bodyValue, member_setup: { ...bodyValue.member_setup, outbound_link_generation: 0 } },
    { ...bodyValue, member_setup: { ...bodyValue.member_setup, outbound_link_generation: 5 } },
    { ...bodyValue, member_setup: { ...bodyValue.member_setup, outbound_link_generation: "" } },
    {
      ...bodyValue,
      member_setup: { ...bodyValue.member_setup, outbound_link_generation: " opaque " },
    },
    { ...bodyValue, member_setup: { ...bodyValue.member_setup, expected_singleton_version: 1 } },
    { ...bodyValue, member_setup: { ...bodyValue.member_setup, prepared_revision: 2 } },
  ])
    expect(() => decodeBody(b)).toThrow();
});

it("closes every bind and original issuance object without discarding bearer material or unknown fields", () => {
  const decodeIssuance = decodeUnknownSyncWorkstreamsSourceIssuanceRequestV1;
  expect(decodeIssuance(issuance)).toEqual(issuance);
  for (const enrollment of [
    { ...issuance, schema: "jones-code.workstreams-native-enrollment/v2" },
    { ...issuance, enrollment_id: "different" },
    {
      ...issuance,
      context: { ...issuance.context, build: { ...issuance.context.build, tree: "bad" } },
    },
    { ...issuance, session: { ...issuance.session, session_id: "" } },
    { ...issuance, session: { ...issuance.session, expires_at: issuance.session.issued_at } },
    { ...issuance, session: { ...issuance.session, expires_at: "2026-12-28T00:00:00.000Z" } },
    { ...issuance, session: { ...issuance.session, issued_at: "2026-02-30T00:00:00.000Z" } },
    { ...issuance, session: { ...issuance.session, bearer: "never-retain" } },
    {
      ...issuance,
      context: { ...issuance.context, build: { ...issuance.context.build, extra: true } },
    },
  ])
    expect(() => decodeIssuance(enrollment)).toThrow();

  const paths = [
    [],
    ["expected_home"],
    ["expected_home_authority"],
    ["source"],
    ["source", "authority"],
    ["source", "enrollment"],
    ["source", "enrollment", "context"],
    ["source", "enrollment", "context", "build"],
    ["source", "enrollment", "session"],
    ["home_setup"],
    ["member_setup"],
    ["expected"],
  ];
  for (const path of paths) {
    const value = structuredClone(bodyValue);
    let obj: Record<string, unknown> = value;
    for (const key of path) obj = obj[key] as Record<string, unknown>;
    obj.extra = "closed";
    expect(() => decodeBody(value)).toThrow();
  }
});

it("keeps committed facts separate from intent and preserves the old two-field result", () => {
  const result = committed();
  const decode = decodeUnknownSyncWorkstreamsHomeBindCommittedV2;
  expect(encodeSyncWorkstreamsHomeBindCommittedV2(decode(result))).toEqual(result);
  for (const value of [
    { ...result, contract: "workstreams-native-home-bind/3.0.0" },
    { ...result, home_pin_bound_revision: 1 },
    { ...result, receipt: { ...result.receipt, principal_id: "wrong" } },
    { ...result, receipt: { ...result.receipt, registry_version: 9 } },
    { ...result, receipt: { ...result.receipt, binding_version: 0 } },
    { ...result, receipt: { ...result.receipt, binding_version: 3 } },
    { ...result, receipt: { ...result.receipt, registry_version: 12 } },
    { ...result, receipt: { ...result.receipt, request_sha256: "bad" } },
    { ...result, receipt: { ...result.receipt, grant_version: 2 } },
  ])
    expect(() => decode(value)).toThrow();
  expect(
    decodeUnknownSyncWorkstreamsNativeHomeBindResult({
      principal_id: "principal",
      binding_version: 1,
    }),
  ).toEqual({ principal_id: "principal", binding_version: 1 });
  expect(() => decodeUnknownSyncWorkstreamsNativeHomeBindResult(result)).toThrow();
});

it("reconciles exact readback identity and never turns unknown coverage into absence", () => {
  const request = { contract, expected_home: home, bind_id: "bind-1", request_sha256: sha(body()) };
  expect(decodeUnknownSyncWorkstreamsHomeBindReadbackRequestV2(request)).toEqual(request);
  const decode = decodeUnknownSyncWorkstreamsHomeBindReadbackV2;
  const present = { contract, request, kind: "committed", result: committed() };
  expect(decode(present)).toEqual(present);
  for (const request of [
    { ...present.request, bind_id: "different" },
    { ...present.request, request_sha256: "d".repeat(64) },
    { ...present.request, expected_home: { ...home, server_generation: 8 } },
    { ...present.request, expected_home: { ...home, owner_id: "other" } },
  ])
    expect(() => decode({ ...present, request })).toThrow();
  const absent = {
    contract,
    request,
    kind: "proven_absent",
    coverage: {
      home_authority: homeAuthority,
      registry_version: 10,
      receipts: "complete",
      dispatch: "definitive_no_effect",
    },
  };
  expect(decode(absent)).toEqual(absent);
  expect(() =>
    decode({ ...absent, coverage: { ...absent.coverage, dispatch: "in_flight" } }),
  ).toThrow();
  expect(() => decode({ contract, request, kind: "proven_absent" })).toThrow();
  for (const reason of [
    "key_unavailable",
    "restore_uncertain",
    "generation_mismatch",
    "unsupported_capability",
    "authentication_failed",
    "home_unreachable",
    "in_flight",
    "coverage_incomplete",
  ])
    expect(decode({ contract, request, kind: "unknown", reason })).toEqual({
      contract,
      request,
      kind: "unknown",
      reason,
    });
  expect(
    decode({ contract, request, kind: "conflict", retained_request_sha256: "d".repeat(64) }).kind,
  ).toBe("conflict");
});

it("retains the exact qualified prepared setup bytes while checking home, pin and nested session identity", () => {
  const decode = decodeUnknownSyncWorkstreamsHomeBindSetupReadbackV2;
  const r = homeSetup();
  expect(encodeSyncWorkstreamsHomeBindSetupReadbackV2(decode(r))).toEqual(r);
  expect(decode(r).setup_request_body).toBe(homeSetupBody);
  for (const value of [
    { ...r, pin: { ...r.pin, state: "bound" } },
    { ...r, pin: { ...r.pin, revision: 2 } },
    { ...r, pin: { ...r.pin, session: { ...r.pin.session, session_id: "replacement" } } },
    { ...r, request: { ...r.request, home_setup_request_id: "other" } },
    { ...r, home_authority: { ...homeAuthority, store_generation: 9 } },
    { ...r, setup_request_body: JSON.stringify({ ...setup(true), extra: true }) },
    {
      ...r,
      setup_request_body: JSON.stringify({
        ...setup(true),
        expected: { ...expected, extra: true },
      }),
    },
  ])
    expect(() => decode(value)).toThrow();
  const decodeSetup = decodeUnknownSyncWorkstreamsPeerPinSetupRequestV1;
  expect(() =>
    decodeSetup({
      ...setup(true),
      assertion: { ...assertion(true), setup_request_sha256: "a".repeat(64) },
    }),
  ).toThrow();
  expect(() =>
    decodeSetup({
      ...setup(true),
      assertion: {
        ...assertion(true),
        session: {
          ...assertion(true).session,
          ceiling: { ...assertion(true).session.ceiling, bearer: "no" },
        },
      },
    }),
  ).toThrow();
});

// @effect-diagnostics nodeBuiltinImport:off -- Synthetic fixture digests use synchronous Node SHA-256; production codecs compute no witnesses.
import * as NodeCrypto from "node:crypto";
import { expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import {
  WORKSTREAMS_HOME_BIND_CONTRACT as contract,
  WorkstreamsHomeBindBodyV2,
  canonicalWorkstreamsHomeBindBody,
} from "./workstreamsHomeBind.ts";
import {
  WORKSTREAMS_MEMBER_ENROLLMENT_SCHEMA as schema,
  WorkstreamsMemberEnrollmentV1,
  WorkstreamsMemberEnrollmentIntentV1,
  WorkstreamsMemberEnrollmentBoundV1,
  canonicalWorkstreamsMemberEnrollmentIntent,
  canonicalWorkstreamsMemberEnrollment,
  isWorkstreamsMemberEnrollmentAcceptance,
} from "./workstreamsMemberEnrollment.ts";
const decodeUnknownSyncWorkstreamsHomeBindBodyV2 =
  Schema.decodeUnknownSync(WorkstreamsHomeBindBodyV2);
const decodeUnknownSyncWorkstreamsMemberEnrollmentIntentV1 = Schema.decodeUnknownSync(
  WorkstreamsMemberEnrollmentIntentV1,
);
const decodeUnknownSyncWorkstreamsMemberEnrollmentV1 = Schema.decodeUnknownSync(
  WorkstreamsMemberEnrollmentV1,
);
const decodeUnknownSyncWorkstreamsMemberEnrollmentBoundV1 = Schema.decodeUnknownSync(
  WorkstreamsMemberEnrollmentBoundV1,
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
const memberSetupBody = JSON.stringify(setup(false), null, 2) + "\n";
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

const intentValue = () => ({
  issuance_body: JSON.stringify(issuance, null, 2) + "\n",
  bind_request_body: body(),
  bind_request_sha256: sha(body()),
  home_setup: homeSetup(),
  member_setup_request_body: memberSetupBody,
  member_setup_request_sha256: sha(memberSetupBody),
});
const prepared = () => {
  const intent = intentValue();
  return {
    schema,
    state: "prepared",
    revision: 1,
    intent,
    intent_sha256: sha(
      canonicalWorkstreamsMemberEnrollmentIntent(
        decodeUnknownSyncWorkstreamsMemberEnrollmentIntentV1(intent),
      ),
    ),
    home_result: null,
    local_binding: null,
  };
};
const bound = () => ({
  ...prepared(),
  state: "bound",
  revision: 2,
  home_result: committed(),
  local_binding: { pin_bound_revision: 2, singleton_binding_version: 1 },
});
const decode = decodeUnknownSyncWorkstreamsMemberEnrollmentV1;

it("retains immutable issuance, setup, build, session and bind bytes across prepared-to-bound acceptance", () => {
  const a = decode(prepared());
  const b = decode(bound());
  expect(isWorkstreamsMemberEnrollmentAcceptance(a, b)).toBe(true);
  for (const value of [a, b]) {
    const text = canonicalWorkstreamsMemberEnrollment(value);
    expect(decode(JSON.parse(text))).toEqual(value);
    expect(canonicalWorkstreamsMemberEnrollment(decode(JSON.parse(text)))).toBe(text);
    expect(value.intent.issuance_body).toBe(intentValue().issuance_body);
    expect(value.intent.member_setup_request_body).toBe(memberSetupBody);
    expect(value.intent.home_setup.setup_request_body).toBe(homeSetupBody);
    expect(value.intent.bind_request_body).toBe(body());
  }
  expect(() => decodeUnknownSyncWorkstreamsMemberEnrollmentBoundV1(a)).toThrow();
  expect(isWorkstreamsMemberEnrollmentAcceptance(b, a)).toBe(false);
  expect(isWorkstreamsMemberEnrollmentAcceptance(b, b)).toBe(false);
});

it("rejects mismatched setup/source/home authority, enrollment and original sessions", () => {
  const a = prepared();
  const replaceMember = (update: Record<string, unknown>) => ({
    ...a,
    intent: {
      ...a.intent,
      member_setup_request_body: JSON.stringify({
        ...setup(false),
        assertion: { ...assertion(false), ...update },
      }),
    },
  });
  for (const value of [
    {
      ...a,
      intent: {
        ...a.intent,
        issuance_body: JSON.stringify({
          ...issuance,
          context: {
            ...issuance.context,
            build: { ...issuance.context.build, sha: "d".repeat(40) },
          },
        }),
      },
    },
    {
      ...a,
      intent: {
        ...a.intent,
        issuance_body: JSON.stringify({
          ...issuance,
          session: { ...issuance.session, session_id: "different" },
        }),
      },
    },
    replaceMember({ source_enrollment_id: "different" }),
    replaceMember({ receiver_authority: { ...source, store_generation: 9 } }),
    replaceMember({ expected_home: { ...home, server_generation: 8 } }),
    replaceMember({ source_authority: { ...source, authority_namespace: "different" } }),
    replaceMember({
      session: { ...assertion(false).session, session_id: issuance.session.session_id },
    }),
    replaceMember({ roles: ["member_home"] }),
    {
      ...a,
      intent: {
        ...a.intent,
        member_setup_request_body: JSON.stringify({
          ...setup(false),
          expected: { ...expected, grant_version: 9 },
        }),
      },
    },
    {
      ...a,
      intent: {
        ...a.intent,
        member_setup_request_body: JSON.stringify({
          ...setup(false),
          expected: { ...expected, registry_version: 10 },
        }),
      },
    },
    {
      ...a,
      intent: {
        ...a.intent,
        home_setup: { ...homeSetup(), canonical_origin: "https://other.invalid" },
      },
    },
    {
      ...a,
      intent: {
        ...a.intent,
        home_setup: {
          ...homeSetup(),
          request: { ...homeSetup().request, expected_home: { ...home, owner_id: "different" } },
        },
      },
    },
  ])
    expect(() => decode(value)).toThrow();
});

it("requires the complete immutable committed result and exact local postconditions only on bound records", () => {
  const a = prepared(),
    b = bound();
  for (const value of [
    { ...a, state: "bound" },
    { ...a, home_result: committed() },
    { ...b, revision: 1 },
    { ...b, home_result: null },
    { ...b, local_binding: null },
    { ...b, local_binding: { ...b.local_binding, pin_bound_revision: 1 } },
    { ...b, local_binding: { ...b.local_binding, singleton_binding_version: 2 } },
    {
      ...b,
      home_result: {
        ...b.home_result,
        receipt: { ...b.home_result.receipt, request_sha256: "d".repeat(64) },
      },
    },
    {
      ...b,
      home_result: {
        ...b.home_result,
        request_body: canonicalWorkstreamsHomeBindBody(
          decodeBody({ ...bodyValue, bind_id: "other" }),
        ),
      },
    },
    { ...b, intent: { ...b.intent, bind_request_sha256: "INVALID" } },
    { ...b, intent_sha256: "" },
    { ...b, schema: "jones-workstreams-member-enrollment/2" },
  ])
    expect(() => decode(value)).toThrow();
});

it("checks immutable bytes on acceptance even when semantically identical setup JSON is reserialized", () => {
  const a = decode(prepared());
  const b = decode(bound());
  const changed = decode({
    ...bound(),
    intent: {
      ...bound().intent,
      issuance_body: JSON.stringify(issuance),
    },
  });
  expect(isWorkstreamsMemberEnrollmentAcceptance(a, changed)).toBe(false);
  expect(isWorkstreamsMemberEnrollmentAcceptance(a, { ...b, intent_sha256: "d".repeat(64) })).toBe(
    false,
  );
});

it("closes proof and local-result objects and refuses a cached current grant outside retained request bytes", () => {
  const b = bound();
  for (const value of [
    { ...b, grant_version: 9 },
    { ...b, intent: { ...b.intent, current_home_grant: expected } },
    { ...b, local_binding: { ...b.local_binding, grant_version: 9 } },
    { ...b, home_result: { ...b.home_result, authority: true } },
    {
      ...b,
      home_result: { ...b.home_result, receipt: { ...b.home_result.receipt, grant_id: "grant" } },
    },
  ])
    expect(() => decode(value)).toThrow();
});

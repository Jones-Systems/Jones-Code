import { expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { WorkstreamsPeerSessionPinV1 } from "./workstreamsPeerAuthority.ts";
const ceiling = {
  access: "full-access",
  runtime_mode: "full-access",
  interaction_mode: "default",
  scopes: ["orchestration:read"],
  permissions: ["workstreams:read"],
};
const pin = {
  schema: "jones-workstreams-peer-session-pin/1",
  pin_id: "pin-1",
  revision: 1,
  receiver_authority: {
    environment_id: "home",
    authority_namespace: "home-store",
    store_generation: 1,
  },
  session: {
    session_id: "session-1",
    subject: "peer",
    method: "bearer-access-token",
    issued_at: "2026-10-26T12:00:00Z",
    expires_at: "2026-10-26T13:00:00Z",
    peer_link: true,
    ceiling,
  },
  peer_environment_id: "source",
  roles: ["member_home"],
  expected_home: {
    registry_instance_id: "12345678-1234-1234-1234-123456789abc",
    owner_id: "owner",
    home_environment_id: "home",
    server_generation: 1,
  },
  source_authority: {
    environment_id: "source",
    authority_namespace: "source-store",
    store_generation: 1,
  },
  source_enrollment_id: "enrollment",
  source_binding_version: null,
  singleton_binding_version: null,
  state: "prepared",
  expires_at: "2026-10-26T12:30:00Z",
  setup_request_id: "setup",
  setup_request_sha256: "a".repeat(64),
  owner_qualification_digest: "b".repeat(64),
  bind_receipt: null,
};
it("retains original setup/session pins across codec round trips without inventing a bound state", () => {
  const value = decodePin(pin);
  expect(value).toEqual(pin);
  expect(decodePin(encodePin(value))).toEqual(pin);
  const bound = {
    ...pin,
    state: "bound",
    source_binding_version: 3,
    singleton_binding_version: 2,
    bind_receipt: {
      principal_id: "principal",
      binding_version: 3,
      registry_version: 4,
      request_sha256: "c".repeat(64),
    },
  };
  expect(decodePin(bound)).toEqual(bound);
});
it("refuses unbound claims, expiry extension, duplicate roles and bearer-like excess data", () => {
  const decode = decodePin;
  for (const value of [
    { ...pin, state: "bound" },
    { ...pin, expires_at: "2026-10-26T14:00:00Z" },
    { ...pin, expires_at: pin.session.issued_at },
    { ...pin, roles: ["member_home", "member_home"] },
    { ...pin, session: { ...pin.session, peer_link: false } },
    { ...pin, session: { ...pin.session, bearer: "must-not-be-retained" } },
    { ...pin, session: { ...pin.session, ceiling: { ...ceiling, access: "read-only" } } },
    { ...pin, source_authority: { ...pin.source_authority, store_generation: 0 } },
  ])
    expect(() => decode(value)).toThrow();
});

const decodePin = Schema.decodeUnknownSync(WorkstreamsPeerSessionPinV1);
const encodePin = Schema.encodeSync(WorkstreamsPeerSessionPinV1);

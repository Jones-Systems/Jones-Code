import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import * as W from "./workstreamsPeerSetup.ts";
const base = {
  setup_id: "setup:test",
  source_environment_id: "member:test",
  expected_home: {
    registry_instance_id: "11111111-1111-4111-8111-111111111111",
    owner_id: "owner:test",
    home_environment_id: "home:test",
    server_generation: 1,
  },
};
describe("closed independently versioned peer setup contract", () => {
  const decode = Schema.decodeUnknownSync(W.WorkstreamsPeerSetupReadRequest);
  const context = {
    contract: W.WORKSTREAMS_PEER_SETUP_CONTRACT,
    ...base,
    operation: "context",
    receiver_environment_id: "home:test",
  };
  it("accepts only fixed read/apply operations and retains exact source bytes", () => {
    expect(decode(context)).toEqual(context);
    const apply = Schema.decodeUnknownSync(W.WorkstreamsPeerSetupApplyRequest);
    for (const operation of ["prepare_home", "accept_member", "resume_member"])
      expect(apply({ ...context, operation, request_sha256: "a".repeat(64) }).operation).toBe(
        operation,
      );
    for (const operation of ["receipt_import", "register_session", "call", "forward"])
      expect(() => apply({ ...context, operation, request_sha256: "a".repeat(64) })).toThrow();
  });
  it.each(["tool", "url", "callback", "receipt", "session", "origin", "route"])(
    "refuses extra %s arguments",
    (key) => {
      expect(() => decode({ ...context, [key]: "forged" })).toThrow();
    },
  );
  it("rejects unsupported version, malformed digest and receiver", () => {
    for (const r of [
      { ...context, contract: "workstreams-native-peer-setup/2.0.0" },
      { ...context, operation: "source_intent", request_sha256: "bad" },
      { ...context, receiver_environment_id: "" },
    ])
      expect(() => decode(r)).toThrow();
  });
});

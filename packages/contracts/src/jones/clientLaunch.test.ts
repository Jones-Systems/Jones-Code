import { describe, expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";
import { OrchestratorMcpClientRequestId, OrchestratorMcpFailure } from "../orchestratorMcp.ts";
import {
  ClientLaunchLookup,
  ClientLaunchFence,
  InitialMessageState,
  PreparationState,
} from "./clientLaunch.ts";

const absent = {
  clientRequestId: "request",
  state: "absent",
  basis: "none",
  threadId: null,
  requestDigest: null,
  claimed: false,
  threadDeleted: false,
  initialMessage: "unknown",
  runId: null,
  runStatus: null,
  preparation: "not_applicable",
  workspace: null,
  reason: null,
};
describe("client launch wire contracts", () => {
  it("round trips an absent lookup without claiming no effect", () => {
    const value = Schema.decodeUnknownSync(ClientLaunchLookup)(absent);
    expect(Schema.encodeSync(ClientLaunchLookup)(value)).toEqual(absent);
    expect(() => Schema.decodeUnknownSync(ClientLaunchFence)(absent)).toThrow();
    expect(
      Schema.decodeUnknownSync(ClientLaunchFence)({ ...absent, state: "no_effect", basis: "fence" })
        .state,
    ).toBe("no_effect");
  });
  it("bounds keys and closes state and failure literals", () => {
    const key = Schema.decodeUnknownSync(OrchestratorMcpClientRequestId);
    expect(key(" request ")).toBe("request");
    expect(key("x".repeat(256))).toHaveLength(256);
    for (const invalid of ["", " ", "x".repeat(257)]) expect(() => key(invalid)).toThrow();
    expect(() => Schema.decodeUnknownSync(InitialMessageState)("delivered")).toThrow();
    expect(() => Schema.decodeUnknownSync(PreparationState)("retryable")).toThrow();
    for (const code of ["request_conflict", "launch_fenced"]) {
      expect(
        Schema.decodeUnknownSync(OrchestratorMcpFailure)({
          _tag: "OrchestratorMcpFailure",
          code,
          message: code,
        }).code,
      ).toBe(code);
    }
    expect(() =>
      Schema.decodeUnknownSync(ClientLaunchLookup)({ ...absent, requestDigest: "bad" }),
    ).toThrow();
  });
});

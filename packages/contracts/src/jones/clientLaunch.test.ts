import { describe, expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";
import { OrchestratorMcpClientRequestId, OrchestratorMcpFailure } from "../orchestratorMcp.ts";
import {
  ClientLaunchLookup,
  ClientLaunchFence,
  InitialMessageState,
  PreparationState,
} from "./clientLaunch.ts";

const decodeClientLaunchLookup = Schema.decodeUnknownSync(ClientLaunchLookup);
const decodeClientLaunchFence = Schema.decodeUnknownSync(ClientLaunchFence);
const decodeOrchestratorMcpClientRequestId = Schema.decodeUnknownSync(
  OrchestratorMcpClientRequestId,
);
const decodeInitialMessageState = Schema.decodeUnknownSync(InitialMessageState);
const decodePreparationState = Schema.decodeUnknownSync(PreparationState);
const decodeOrchestratorMcpFailure = Schema.decodeUnknownSync(OrchestratorMcpFailure);
const encodeLookup = Schema.encodeSync(ClientLaunchLookup);

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
    const value = decodeClientLaunchLookup(absent);
    expect(encodeLookup(value)).toEqual(absent);
    expect(() => decodeClientLaunchFence(absent)).toThrow();
    expect(decodeClientLaunchFence({ ...absent, state: "no_effect", basis: "fence" }).state).toBe(
      "no_effect",
    );
  });
  it("bounds keys and closes state and failure literals", () => {
    const key = decodeOrchestratorMcpClientRequestId;
    expect(key(" request ")).toBe("request");
    expect(key("x".repeat(256))).toHaveLength(256);
    for (const invalid of ["", " ", "x".repeat(257)]) expect(() => key(invalid)).toThrow();
    expect(() => decodeInitialMessageState("delivered")).toThrow();
    expect(() => decodePreparationState("retryable")).toThrow();
    for (const code of ["request_conflict", "launch_fenced"]) {
      expect(
        decodeOrchestratorMcpFailure({
          _tag: "OrchestratorMcpFailure",
          code,
          message: code,
        }).code,
      ).toBe(code);
    }
    expect(() => decodeClientLaunchLookup({ ...absent, requestDigest: "bad" })).toThrow();
  });
});

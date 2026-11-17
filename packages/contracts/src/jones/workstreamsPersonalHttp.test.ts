import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { ExecutionEnvironmentCapabilities } from "../environment.ts";
import fixture from "./fixtures/personal-workstreams-http-v1.json" with { type: "json" };
import {
  PersonalWorkstreamHttpAssignOutcome,
  PersonalWorkstreamHttpAssignRequest,
  PersonalWorkstreamHttpAssignResult,
  PersonalWorkstreamsHttpList,
} from "./workstreamsPersonalHttp.ts";

const decodeRequest = Schema.decodeUnknownSync(PersonalWorkstreamHttpAssignRequest);
const decodeResult = Schema.decodeUnknownSync(PersonalWorkstreamHttpAssignResult);
const decodeList = Schema.decodeUnknownSync(PersonalWorkstreamsHttpList);
const decodeCapabilities = Schema.decodeUnknownSync(ExecutionEnvironmentCapabilities);
const encodeList = Schema.encodeSync(PersonalWorkstreamsHttpList);
const encodeRequest = Schema.encodeSync(PersonalWorkstreamHttpAssignRequest);
const encodeResult = Schema.encodeSync(PersonalWorkstreamHttpAssignResult);

// The fixture is shared byte-for-byte with the external queue adapter's tests.
describe("personal Workstreams HTTP v1 shared fixture", () => {
  it("decodes and re-encodes every documented message unchanged", () => {
    expect(encodeList(decodeList(fixture.list))).toEqual(fixture.list);
    expect(encodeRequest(decodeRequest(fixture.assignRequest))).toEqual(fixture.assignRequest);
    for (const [outcome, result] of Object.entries(fixture.assignResults)) {
      const decoded = decodeResult(result);
      expect(decoded.outcome).toBe(outcome);
      expect(decoded.operationId).toBe(fixture.assignRequest.operationId);
      expect(encodeResult(decoded)).toEqual(result);
    }
  });

  it("carries one result for every outcome", () => {
    expect(Object.keys(fixture.assignResults).sort()).toEqual(
      [...PersonalWorkstreamHttpAssignOutcome.literals].sort(),
    );
    expect(fixture.assignResults.thread_not_found.assignment).toBeNull();
  });

  it("orders the listed definitions by order key then id", () => {
    const keys = fixture.list.workstreams.map((w) => [w.orderKey, w.id].join("\u0000"));
    expect(keys).toEqual([...keys].sort());
  });
});

describe("personal Workstreams assign request", () => {
  it("rejects excess properties at every level", () => {
    const base = fixture.assignRequest;
    expect(() => decodeRequest({ ...base, extra: true })).toThrow();
    expect(() => decodeRequest({ ...base, origin: { kind: "queue", extra: true } })).toThrow();
    expect(() =>
      decodeRequest({ ...base, expected: { kind: "unassigned", revision: 0 } }),
    ).toThrow();
  });

  it("accepts a revision expectation, an unassign target and an absent origin", () => {
    const { origin: _origin, ...base } = fixture.assignRequest;
    expect(
      decodeRequest({ ...base, workstreamId: null, expected: { kind: "revision", revision: 3 } }),
    ).toMatchObject({ workstreamId: null, expected: { kind: "revision", revision: 3 } });
  });

  it.each([
    ["short operation id", { operationId: "too-short" }],
    ["unknown schema", { schema: "jones.personal-workstream-assign/v2" }],
    ["unknown origin kind", { origin: { kind: "browser" } }],
    ["negative revision", { expected: { kind: "revision", revision: -1 } }],
    ["invalid workstream id", { workstreamId: "-leading-dash" }],
  ])("rejects %s", (_label, patch) => {
    expect(() => decodeRequest({ ...fixture.assignRequest, ...patch })).toThrow();
  });
});

describe("workstreamsPersonalHttp capability", () => {
  it("decodes the advertised version and later versions with unknown keys", () => {
    expect(decodeCapabilities(fixture.capability).workstreamsPersonalHttp).toEqual({ version: 1 });
    expect(
      decodeCapabilities({ workstreamsPersonalHttp: { version: 2, futureField: true } })
        .workstreamsPersonalHttp?.version,
    ).toBe(2);
    expect(decodeCapabilities({}).workstreamsPersonalHttp).toBeUndefined();
  });

  it("rejects a non-positive version", () => {
    expect(() => decodeCapabilities({ workstreamsPersonalHttp: { version: 0 } })).toThrow();
  });
});

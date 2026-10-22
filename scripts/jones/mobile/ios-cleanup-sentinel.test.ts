import { describe, expect, it } from "vite-plus/test";
import {
  assertCase,
  assertCleanupEvidence,
  caseArguments,
  qualification,
  SENTINEL_CASES,
  testStarted,
} from "./ios-cleanup-sentinel.ts";

describe("native sentinel qualification decisions", () => {
  it("requires all four independent cleanup observations", () => {
    const evidence = {
      createdUdidAbsent: true,
      groupsGone: true,
      scratchAbsent: true,
      portClosed: true,
    };
    expect(() => assertCleanupEvidence(evidence)).not.toThrow();
    for (const key of Object.keys(evidence))
      expect(() => assertCleanupEvidence({ ...evidence, [key]: false })).toThrow(
        "cleanup unresolved",
      );
  });
  it("requires the intended native failure, not an unrelated startup failure", () => {
    expect(caseArguments("failure")).toEqual(["--no-action", "true"]);
    expect(caseArguments("startup-timeout")).toEqual(["--fixture-readiness-timeout-ms", "1"]);
    const failure = {
      outcome: "fail",
      nativeClaim: false,
      retainScratch: false,
      failureCode: "negative-control-oracle",
    };
    expect(() => assertCase("failure", 1, failure)).not.toThrow();
    expect(() => assertCase("failure", 1, { ...failure, failureCode: "other" })).toThrow();
    expect(() => assertCase("failure", 0, failure)).toThrow();
    expect(() =>
      assertCase("startup-timeout", 1, { ...failure, failureCode: "startup-timeout" }),
    ).not.toThrow();
    expect(() =>
      assertCase("cancellation", 1, { ...failure, failureCode: "cancelled" }),
    ).not.toThrow();
    expect(() =>
      assertCase("success", 0, { outcome: "pass", nativeClaim: true, retainScratch: true }),
    ).toThrow("cleanup");
  });
  it("cannot qualify missing cases or unresolved sentinel cleanup", () => {
    const cases = SENTINEL_CASES.map((kind) => ({ case: kind, passed: true }));
    expect(qualification(cases, true)).toBe("established");
    expect(qualification(cases, false)).toBe("not-established");
    expect(qualification(cases.slice(1), true)).toBe("not-established");
    expect(
      qualification(
        cases.map((row, index) => ({ ...row, passed: index !== 2 })),
        true,
      ),
    ).toBe("not-established");
  });
  it("waits for a complete test-started event, not device creation or a partial write", () => {
    expect(testStarted('{"stage":"device-created"}\n')).toBe(false);
    expect(testStarted('{"stage":"test-started"}')).toBe(false);
    expect(testStarted('{"stage":"device-booted"}\n{"stage":"test-started"}\n{"sta')).toBe(true);
  });
});

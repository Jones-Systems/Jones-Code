import { expect, it } from "vite-plus/test";
import { decodeServicePreflightResult, qualifiedServicePreflightFailure } from "./servicePreflightResult.ts";

it("binds ordinary preflight to the supervising launcher without granting qualified capabilities", () => {
  const ordinary = { status: "ready", version: "1.1.0", launcherProtocol: 3 };
  expect(decodeServicePreflightResult(ordinary, 3)).toEqual(ordinary);
  expect(decodeServicePreflightResult(ordinary)).toBeUndefined();
  expect(decodeServicePreflightResult({ ...ordinary, startupGateProtocol: 1 }, 3)).toBeUndefined();
  expect(qualifiedServicePreflightFailure({ code: 0, stdout: JSON.stringify(ordinary), version: ordinary.version }))
    .toMatch(/startup-gate-unavailable/);
});

it("retains migration plans and refuses malformed gate or recovery capabilities", () => {
  const candidate = { status: "ready", version: "1.1.0", launcherProtocol: 4,
    startupGateProtocol: 1, qualifiedRecoveryProtocol: 1,
    migrationPlan: { pendingUpstream: [71], pendingJones: [107] } };
  expect(decodeServicePreflightResult(candidate)).toEqual(candidate);
  expect(decodeServicePreflightResult({ ...candidate, migrationPlan: {} })).toBeUndefined();
  expect(decodeServicePreflightResult({ ...candidate, startupGateProtocol: 2 })).toBeUndefined();
  expect(qualifiedServicePreflightFailure({ code: 0, stdout: JSON.stringify(candidate), version: "foreign" }))
    .toMatch(/startup-gate-unavailable/);
  expect(qualifiedServicePreflightFailure({ code: 0, stdout: JSON.stringify(candidate), version: candidate.version, requiredQualifiedRecoveryProtocol: 1 }))
    .toBeUndefined();
});

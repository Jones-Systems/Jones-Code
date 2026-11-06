import { assert, it } from "@effect/vitest";
import type { PullRequestCheck, PullRequestCheckObservation } from "@t3tools/contracts";
import {
  classifyCheckObservation,
  evaluateObservedChecks,
  observationInFlight,
} from "./checkObservation.ts";

const observation: PullRequestCheckObservation = {
  headSha: "head",
  enumerationComplete: true,
  workflowState: "complete",
  requiredSetKnown: false,
  requiredState: "unknown",
  requiredChecks: [],
};
const check = (
  name: string,
  status: PullRequestCheck["status"],
  required = false,
): PullRequestCheck => ({ name, status, required, description: null, url: null });
const initial = {
  headSha: "head",
  checks: [] as ReadonlyArray<PullRequestCheck>,
  observation,
  failedChecks: [],
  passed: false,
  passedChecks: [],
  owedChecks: [],
};

it("never treats skipped-only, neutral-only, active-workflow or unknown enumeration as completion", () => {
  for (const checks of [
    [check("a", "skipped"), check("b", "skipped"), check("c", "skipped")],
    [check("a", "neutral")],
  ])
    assert.isFalse(classifyCheckObservation("head", checks, observation).ready);
  for (const override of [
    { workflowState: "pending" as const },
    { enumerationComplete: false },
    { headSha: "older" },
  ])
    assert.isFalse(
      classifyCheckObservation("head", [check("advisory", "success")], {
        ...observation,
        ...override,
      }).ready,
    );
});

it("requires the complete bound gate even when every visible row is required and successful", () => {
  const bound = { ...observation, requiredSetKnown: true, requiredChecks: ["Check"] };
  for (const requiredState of ["unknown", "missing", "pending", "failure"] as const)
    assert.isFalse(
      classifyCheckObservation("head", [check("classifier", "success", true)], {
        ...bound,
        requiredState,
      }).ready,
    );
  assert.isTrue(
    classifyCheckObservation("head", [check("Check", "success", false)], {
      ...bound,
      requiredState: "success",
    }).ready,
  );
});

it("wakes once for a later successful name even if required is false", () => {
  const first = evaluateObservedChecks({ ...initial, checks: [check("advisory", "success")] });
  const later = {
    ...initial,
    ...first,
    checks: [check("advisory", "success"), check("Check", "success", false)],
  };
  const second = evaluateObservedChecks(later);
  assert.isDefined(first.pass);
  assert.isDefined(second.pass);
  assert.isUndefined(evaluateObservedChecks({ ...later, ...second }).pass);
});

it("wakes again after a positively observed rerun while preserving partial-read dedupe", () => {
  const green = { ...initial, checks: [check("test", "success")] };
  const first = evaluateObservedChecks(green);
  const rerun = evaluateObservedChecks({
    ...green,
    ...first,
    checks: [check("test", "pending")],
    observation: { ...observation, workflowState: "pending" },
  });
  assert.isFalse(rerun.passed);
  assert.isDefined(evaluateObservedChecks({ ...green, ...rerun }).pass);
});

it("owes a later completion after the bound gate passes with pending advisory checks", () => {
  const bound = {
    ...observation,
    requiredSetKnown: true,
    requiredState: "success" as const,
    requiredChecks: ["Check"],
  };
  const first = evaluateObservedChecks({
    ...initial,
    observation: { ...bound, workflowState: "pending" },
    checks: [check("Check", "success"), check("advisory", "pending")],
  });
  const settled = evaluateObservedChecks({
    ...initial,
    ...first,
    observation: bound,
    checks: [check("Check", "success"), check("advisory", "success")],
  });
  assert.isDefined(first.pass);
  assert.isDefined(settled.pass);
  assert.isFalse(observationInFlight({ ...bound, requiredState: "failure" }));
  assert.isTrue(observationInFlight({ ...bound, requiredState: "missing" }));
});

it("separates successful, skipped and neutral counts in observed-only completion", () => {
  const result = evaluateObservedChecks({
    ...initial,
    checks: [check("a", "success"), check("b", "skipped"), check("c", "neutral")],
  });
  assert.deepEqual(result.pass?.observed, { successful: 1, skipped: 1, neutral: 1 });
  assert.isFalse(result.pass?.required);
});

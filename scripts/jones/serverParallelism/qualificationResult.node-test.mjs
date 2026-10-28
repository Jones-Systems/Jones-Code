import { test } from "node:test";
import assert from "node:assert/strict";
import { qualificationDecision, requiredNativePredicates } from "./qualificationResult.mjs";

test("a failed resource sample after valid overlap fails the entry gate", () => {
  assert.equal(qualificationDecision({ productiveOverlap: true, resourcesComplete: false,
    childRssWithinBudget: "unknown" }).passed, false);
});
test("complete but over-budget samples fail and absent comparisons remain unknown", () => {
  assert.equal(qualificationDecision({ resourcesComplete: true, childRssWithinBudget: false }).outcome, "failed");
  assert.equal(qualificationDecision({ resourcesComplete: true, writerTarget: "unknown" }).outcome, "unknown");
  assert.equal(qualificationDecision({}).passed, false);
  assert.equal(qualificationDecision({ resourcesComplete: true, childRssWithinBudget: true }).passed, true);
});

test("missing required predicates cannot qualify even after useful overlap", () => {
  const decision = qualificationDecision({ productiveOverlap: true }, requiredNativePredicates);
  assert.equal(decision.passed, false);
  assert.ok(decision.unknown.includes("resourcesComplete"));
});

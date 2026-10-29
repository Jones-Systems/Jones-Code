import { nativeBlockTestProof } from "./native-block-proof-fixture.mjs";
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  qualificationDecision,
  requiredNativePredicates,
  nativeBlockMatrixDecision,
} from "./qualificationResult.mjs";

test("a failed resource sample after valid overlap fails the entry gate", () => {
  assert.equal(
    qualificationDecision({
      productiveOverlap: true,
      resourcesComplete: false,
      childRssWithinBudget: "unknown",
    }).passed,
    false,
  );
});
test("complete but over-budget samples fail and absent comparisons remain unknown", () => {
  assert.equal(
    qualificationDecision({ resourcesComplete: true, childRssWithinBudget: false }).outcome,
    "failed",
  );
  assert.equal(
    qualificationDecision({ resourcesComplete: true, writerTarget: "unknown" }).outcome,
    "unknown",
  );
  assert.equal(qualificationDecision({}).passed, false);
  assert.equal(
    qualificationDecision({ resourcesComplete: true, childRssWithinBudget: true }).passed,
    true,
  );
});

test("missing required predicates cannot qualify even after useful overlap", () => {
  const decision = qualificationDecision({ productiveOverlap: true }, requiredNativePredicates);
  assert.equal(decision.passed, false);
  assert.ok(decision.unknown.includes("resourcesComplete"));
});

test("native block fan-in rejects successful subprocesses, skips, missing cases and duplicate bindings", () => {
  const bindings = ["linux-x64", "macos-arm64"].flatMap((os) =>
    ["source", "bundle", "electron", "sea"].flatMap((form) =>
      ["laptop", "vps"].map((profile) => ({ os, form, profile, build: "a".repeat(64) })),
    ),
  );
  assert.equal(bindings.length, 16);
  assert.equal(nativeBlockMatrixDecision([], bindings).predicate, "unknown");
  assert.equal(
    nativeBlockMatrixDecision([{ code: 0, outcome: "passed" }], bindings).predicate,
    "unknown",
  );
  const skips = bindings.map((binding) => ({
    proof: binding,
    skipped: true,
    artifactsVerified: false,
  }));
  assert.equal(nativeBlockMatrixDecision(skips, bindings).predicate, "unknown");
  assert.equal(nativeBlockMatrixDecision(skips.slice(1), bindings).predicate, "unknown");
  assert.equal(nativeBlockMatrixDecision([...skips, skips[0]], bindings).predicate, "unknown");
  assert.equal(nativeBlockMatrixDecision(skips, [bindings[0], bindings[0]]).predicate, "unknown");
  assert.equal(nativeBlockMatrixDecision([], []).predicate, "unknown");
  const os = bindings.filter((binding) => binding.os === "linux-x64");
  assert.equal(nativeBlockMatrixDecision([], os).cases.length, 8);
  assert.equal(nativeBlockMatrixDecision(skips, bindings).cases.length, 16);
});

test("native block requires verified explicit receipts in every per-OS and full-matrix cell", () => {
  const expected = ["linux-x64", "macos-arm64"].flatMap((os) =>
    ["source", "bundle", "electron", "sea"].flatMap((form) =>
      ["laptop", "vps"].map((profile) => ({ os, form, profile, build: "a".repeat(64) })),
    ),
  );
  const receipts = expected.map(({ build: _build, ...binding }) => {
    const proof = nativeBlockTestProof(binding);
    if (proof.profile === "vps") proof.allocation.registeredChildren.push(44);
    if (proof.os === "macos-arm64") proof.witness.method = "macos-native-stack";
    return { proof, artifactsVerified: true, outcome: "passed", predicate: true };
  });
  assert.equal(nativeBlockMatrixDecision(receipts, expected).predicate, true);
  for (const os of ["linux-x64", "macos-arm64"]) {
    const group = receipts.filter((value) => value.proof.os === os);
    assert.equal(group.length, 8);
    assert.equal(
      nativeBlockMatrixDecision(
        group,
        expected.filter((value) => value.os === os),
      ).predicate,
      true,
    );
  }
  assert.equal(nativeBlockMatrixDecision([receipts[0]], [expected[0]]).predicate, "unknown");
  assert.equal(
    nativeBlockMatrixDecision(
      receipts.map((value, i) => (i ? value : { ...value, skipped: true })),
      expected,
    ).predicate,
    "unknown",
  );
  assert.equal(nativeBlockMatrixDecision(receipts.slice(1), expected).predicate, "unknown");
  assert.equal(
    nativeBlockMatrixDecision([...receipts, receipts[0]], expected).predicate,
    "unknown",
  );
  assert.equal(
    nativeBlockMatrixDecision(
      receipts.map((value, i) => (i ? value : { ...value, artifactsVerified: false })),
      expected,
    ).predicate,
    "unknown",
  );
  receipts[0].proof.replacementUseful = false;
  assert.equal(nativeBlockMatrixDecision(receipts, expected).predicate, false);
});

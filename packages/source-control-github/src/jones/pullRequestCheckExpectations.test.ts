import { assert, it } from "@effect/vitest";
import {
  expectedRequiredChecks,
  requiredCheckState,
  type ObservedCheckRun,
  type ObservedWorkflow,
} from "./pullRequestCheckExpectations.ts";

it("binds only the exact existing Jones-Code main policy", () => {
  const target = { host: "github.com", repository: "Jones-Systems/Jones-Code", baseBranch: "main" };
  assert.deepEqual(expectedRequiredChecks(target), ["Check"]);
  for (const variant of [
    { ...target, host: "elsewhere.test" },
    { ...target, repository: "acme/web" },
    { ...target, baseBranch: "release" },
  ])
    assert.isNull(expectedRequiredChecks(variant));
});

it("requires a successful Check associated with the exact current CI workflow", () => {
  const run: ObservedWorkflow = {
    id: 1,
    workflow_id: 1,
    check_suite_id: 10,
    run_number: 1,
    run_attempt: 1,
    head_sha: "sha",
    head_branch: "topic",
    path: ".github/workflows/ci.yml",
    name: "CI",
    event: "pull_request",
    status: "completed",
    conclusion: "success",
  };
  const check: ObservedCheckRun = {
    id: 1,
    name: "Check",
    head_sha: "sha",
    status: "completed",
    conclusion: "success",
    details_url: null,
    check_suite: { id: 10 },
    app: { id: 1, slug: "github-actions" },
  };
  const input = {
    expected: ["Check"],
    headSha: "sha",
    headBranch: "topic",
    runs: [run],
    checks: [check],
    complete: true,
  };
  assert.equal(requiredCheckState(input), "success");
  for (const conclusion of ["skipped", "neutral", "failure", "cancelled"])
    assert.equal(requiredCheckState({ ...input, checks: [{ ...check, conclusion }] }), "failure");
  assert.equal(requiredCheckState({ ...input, checks: [] }), "missing");
  assert.equal(
    requiredCheckState({ ...input, checks: [{ ...check, check_suite: { id: 99 } }] }),
    "missing",
  );
  assert.equal(
    requiredCheckState({ ...input, runs: [{ ...run, path: ".github/workflows/other.yml" }] }),
    "missing",
  );
  assert.equal(requiredCheckState({ ...input, complete: false }), "unknown");
  assert.equal(requiredCheckState({ ...input, expected: null }), "unknown");
});

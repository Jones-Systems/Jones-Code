import { assert, it } from "@effect/vitest";
import * as Schema from "effect/Schema";
import { PullRequestCheckObservation } from "./pullRequestCheckObservation.ts";
import { PullRequestChecks } from "../pullRequest.ts";
const decodeObservation = Schema.decodeSync(PullRequestCheckObservation);
const isObservation = Schema.is(PullRequestCheckObservation);
const decodeChecks = Schema.decodeSync(PullRequestChecks);

it("preserves unknown and partial evidence across the optional wire extension", () => {
  const value = {
    headSha: "head",
    enumerationComplete: false,
    workflowState: "unknown" as const,
    requiredSetKnown: false,
    requiredState: "unknown" as const,
    requiredChecks: [],
  };
  assert.deepEqual(decodeObservation(value), value);
  assert.deepEqual(decodeChecks({ state: "open", checks: [] }), { state: "open", checks: [] });
  assert.deepEqual(
    decodeChecks({ state: "open", checks: [], jonesCheckObservation: value }).jonesCheckObservation,
    value,
  );
  assert.isFalse(isObservation({ ...value, workflowState: "passed" }));
});

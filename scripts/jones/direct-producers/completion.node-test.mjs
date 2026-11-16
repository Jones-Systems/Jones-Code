import * as NodeAssert from "node:assert/strict";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
const { completeProducer } = await import("./completion.ts");
const worktree = NodePath.resolve("fixture");

for (const status of ["succeeded", "failed", "cancelled"]) {
  NodeTest.test(`known ${status} completion records one observation without content proof`, () => {
    const observations = [];
    const result = completeProducer(
      { producer: "fixture", worktree, status, roots: ["dist", "dist/client"] },
      {
        env: { T3CODE_THREAD_ID: "fixture-owner", T3CODE_RUN_ID: "fixture-run" },
        record: (observation) => {
          observations.push(observation);
          return { status: "recorded", reason: null, reference: "/fixture-observation" };
        },
      },
    );
    NodeAssert.equal(result.status, "recorded");
    NodeAssert.equal(observations.length, 1);
    const observation = observations[0];
    NodeAssert.equal(observation.status, status);
    NodeAssert.equal(observation.owner, "fixture-owner");
    NodeAssert.equal(observation.proofStatus, "deferred");
    NodeAssert.deepEqual(observation.roots, [
      NodePath.join(worktree, "dist"),
      NodePath.join(worktree, "dist/client"),
    ]);
    NodeAssert.deepEqual(observation.lineage, { threadId: "fixture-owner", runId: "fixture-run" });
    NodeAssert.deepEqual(observation.omittedEnrichment, ["allocation_disposition_not_observed"]);
    NodeAssert.ok(!("manifest" in observation));
    NodeAssert.ok(!("sha256" in observation));
  });
}

NodeTest.test("optional recording and diagnostic failures return unavailable without retry", () => {
  let attempts = 0;
  const result = completeProducer(
    { producer: "fixture", worktree, status: "succeeded", roots: [] },
    {
      record: () => {
        attempts++;
        throw new Error("fixture sink failed");
      },
      report: () => {
        throw new Error("fixture diagnostic failed");
      },
    },
  );
  NodeAssert.equal(attempts, 1);
  NodeAssert.deepEqual(result, { status: "unavailable", reason: "metadata_sink_unavailable" });
});

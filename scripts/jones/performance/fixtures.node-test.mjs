import * as NodeAssert from "node:assert/strict";
import * as NodeTest from "node:test";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";
import { fixtureCustodyReceipt, withClosedSyntheticFixture } from "./fixtures.mjs";
import { withPreparedHistoricalSources } from "./prepare-sources.mjs";

NodeTest.test(
  "current fixture default rejects an unbound candidate before creating roots",
  async () => {
    await NodeAssert.rejects(
      withClosedSyntheticFixture({ databaseSource: {}, profile: "health-offline-delete" }, () => {
        throw new Error("consumer must not run");
      }),
      { code: "invalid_source" },
    );
  },
);
NodeTest.test("unsupported fixture producer is refused", async () => {
  await NodeAssert.rejects(
    withClosedSyntheticFixture({ producer: "unknown" }, () => undefined),
    { code: "invalid_producer" },
  );
});
NodeTest.test("V2 custody projection preserves all V1 closure and manifest fields", () => {
  const custody = {
    schema: "jones-performance-fixture/v1",
    closure: { completed: true },
    manifest: [{ sha256: "synthetic" }],
    creationReceipt: { binding: { runId: "synthetic" } },
  };
  NodeAssert.equal(fixtureCustodyReceipt(custody), custody);
  NodeAssert.deepEqual(
    fixtureCustodyReceipt({
      ...custody,
      schema: "jones-performance-fixture/v2",
      producer: "current-v2",
      databaseSource: {},
      runtime: {},
    }),
    custody,
  );
  NodeAssert.throws(
    () =>
      fixtureCustodyReceipt({
        ...custody,
        schema: "jones-performance-fixture/v2",
        producer: "historical-v1",
      }),
    { code: "invalid_receipt" },
  );
});
NodeTest.test("historical preparation never clones without explicit opt-in", async () => {
  await NodeAssert.rejects(
    withPreparedHistoricalSources({}, () => undefined),
    { code: "unavailable" },
  );
});

for (const scenario of ["command-failure", "cancelled-before-start"]) {
  NodeTest.test(
    `historical preparer ${scenario} removes its exact owned scratch without cloning`,
    async () => {
      const worktree = NodePath.resolve(import.meta.dirname, "../../..");
      const parent = NodeFS.mkdtempSync(
        NodePath.join(NodePath.dirname(worktree), ".prepare-source-test-"),
      );
      const identity = NodeFS.lstatSync(parent);
      const cancellation = new AbortController();
      if (scenario === "cancelled-before-start") cancellation.abort();
      let knownCleanup = false;
      try {
        await NodeAssert.rejects(
          withPreparedHistoricalSources(
            {
              explicitHistoricalRequest: true,
              // Node rejects the first Git argv before fetch; no network or source checkout occurs.
              gitExecutable: process.execPath,
              parentPath: parent,
              signal: cancellation.signal,
              binding: {
                repository: "Jones-Systems/Jones-Code",
                sourceRevision: "9b34a930ecc20533f518db813b1005466f33b265",
                taskRef: "jones-salvage-preparer-failure-test",
                runId: NodeCrypto.randomUUID(),
              },
              policy: { homePath: parent, worktreePaths: [worktree], protectedPaths: [] },
            },
            () => {
              throw new Error("consumer must not run");
            },
          ),
          (error) => {
            NodeAssert.equal(error.evidence.cleanup.outcome, "complete");
            NodeAssert.equal(error.evidence.cleanup.absent, true);
            knownCleanup = true;
            return true;
          },
        );
        NodeAssert.deepEqual(NodeFS.readdirSync(parent), []);
      } finally {
        const current = NodeFS.lstatSync(parent);
        if (
          knownCleanup &&
          current.dev === identity.dev &&
          current.ino === identity.ino &&
          !current.isSymbolicLink()
        )
          NodeFS.rmSync(parent, { recursive: true });
        else console.error(`preparer test root retained: ${parent}`);
      }
    },
  );
}

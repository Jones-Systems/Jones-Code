import * as Assert from "node:assert/strict";
import * as Test from "node:test";
import * as FS from "node:fs";
import * as Path from "node:path";
import * as Crypto from "node:crypto";
import { fixtureCustodyReceipt, withClosedSyntheticFixture } from "./fixtures.mjs";
import { withPreparedHistoricalSources } from "./prepare-sources.mjs";

Test.test("current fixture default rejects an unbound candidate before creating roots", async () => {
  await Assert.rejects(withClosedSyntheticFixture({ databaseSource: {}, profile: "health-offline-delete" }, () => { throw new Error("consumer must not run"); }), { code: "invalid_source" });
});
Test.test("unsupported fixture producer is refused", async () => {
  await Assert.rejects(withClosedSyntheticFixture({ producer: "unknown" }, () => undefined), { code: "invalid_producer" });
});
Test.test("V2 custody projection preserves all V1 closure and manifest fields", () => {
  const custody = { schema: "jones-performance-fixture/v1", closure: { completed: true }, manifest: [{ sha256: "synthetic" }], creationReceipt: { binding: { runId: "synthetic" } } };
  Assert.equal(fixtureCustodyReceipt(custody), custody);
  Assert.deepEqual(fixtureCustodyReceipt({ ...custody, schema: "jones-performance-fixture/v2", producer: "current-v2", databaseSource: {}, runtime: {} }), custody);
  Assert.throws(() => fixtureCustodyReceipt({ ...custody, schema: "jones-performance-fixture/v2", producer: "historical-v1" }), { code: "invalid_receipt" });
});
Test.test("historical preparation never clones without explicit opt-in", async () => {
  await Assert.rejects(withPreparedHistoricalSources({}, () => undefined), { code: "unavailable" });
});

for (const scenario of ["command-failure", "cancelled-before-start"]) {
  Test.test(`historical preparer ${scenario} removes its exact owned scratch without cloning`, async () => {
    const worktree = Path.resolve(import.meta.dirname, "../../..");
    const parent = FS.mkdtempSync(Path.join(Path.dirname(worktree), ".prepare-source-test-"));
    const identity = FS.lstatSync(parent);
    const cancellation = new AbortController();
    if (scenario === "cancelled-before-start") cancellation.abort();
    let knownCleanup = false;
    try {
      await Assert.rejects(withPreparedHistoricalSources({
        explicitHistoricalRequest: true,
        // Node rejects the first Git argv before fetch; no network or source checkout occurs.
        gitExecutable: process.execPath, parentPath: parent, signal: cancellation.signal,
        binding: { repository: "Jones-Systems/Jones-Code", sourceRevision: "9b34a930ecc20533f518db813b1005466f33b265", taskRef: "jones-salvage-preparer-failure-test", runId: Crypto.randomUUID() },
        policy: { homePath: parent, worktreePaths: [worktree], protectedPaths: [] },
      }, () => { throw new Error("consumer must not run"); }), (error) => {
        Assert.equal(error.evidence.cleanup.outcome, "complete");
        Assert.equal(error.evidence.cleanup.absent, true);
        knownCleanup = true;
        return true;
      });
      Assert.deepEqual(FS.readdirSync(parent), []);
    } finally {
      const current = FS.lstatSync(parent);
      if (knownCleanup && current.dev === identity.dev && current.ino === identity.ino && !current.isSymbolicLink()) FS.rmSync(parent, { recursive: true });
      else console.error(`preparer test root retained: ${parent}`);
    }
  });
}

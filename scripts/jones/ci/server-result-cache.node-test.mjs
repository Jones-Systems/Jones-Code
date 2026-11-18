import * as NodeAssert from "node:assert/strict";
import * as NodeTest from "node:test";
import { caseResults, pilotCacheIdentity, verifyCases } from "./server-result-cache.mjs";

const root = "/workspace/apps/server";

NodeTest.test("image rollouts retain the pilot key but platform changes do not", () => {
  const runner = {
    GITHUB_RUN_ID: "123",
    GITHUB_SHA: "a".repeat(40),
    RUNNER_OS: "Linux",
    RUNNER_ARCH: "X64",
    ImageOS: "ubuntu24",
    ImageVersion: "20261116.133.1",
  };
  const cold = pilotCacheIdentity("4/8", runner);
  NodeAssert.deepEqual(
    cold,
    pilotCacheIdentity("4/8", { ...runner, ImageVersion: "20261111.127.1" }),
  );
  NodeAssert.notDeepEqual(cold, pilotCacheIdentity("4/8", { ...runner, RUNNER_ARCH: "ARM64" }));
  NodeAssert.notDeepEqual(cold, pilotCacheIdentity("4/8", { ...runner, ImageOS: "ubuntu22" }));
  NodeAssert.notDeepEqual(
    cold,
    pilotCacheIdentity("4/8", { ...runner, GITHUB_SHA: "b".repeat(40) }),
  );
});

function moduleReport(name, status = "passed", title = "reads") {
  return { name, assertionResults: [{ ancestorTitles: ["git"], title, status }] };
}

function report(...files) {
  return { success: true, numFailedTests: 0, numTotalTests: files.length, testResults: files };
}

NodeTest.test("case comparison ignores absolute roots, enumeration order and timing", () => {
  const baseline = caseResults(
    report(moduleReport(root + "/a.test.ts"), moduleReport(root + "/b.test.mjs", "pending")),
    root,
  );
  const candidate = caseResults(
    report(moduleReport("/warm/apps/server/b.test.mjs", "pending"), {
      ...moduleReport("/warm/apps/server/a.test.ts"),
      startTime: 1234,
    }),
    "/warm/apps/server",
  );
  verifyCases(baseline, candidate);
});

NodeTest.test(
  "missing cases, substituted files and changed outcomes invalidate the cache proof",
  () => {
    const expected = caseResults(report(moduleReport("a.test.ts")), root);
    NodeAssert.throws(() => verifyCases(expected, new Map()), /file counts/);
    for (const candidate of [
      moduleReport("b.test.ts"),
      moduleReport("a.test.ts", "pending"),
      moduleReport("a.test.ts", "passed", "writes"),
    ]) {
      NodeAssert.throws(
        () => verifyCases(expected, caseResults(report(candidate), root)),
        /cases differ/,
      );
    }
  },
);

NodeTest.test("only complete successful reports for modules inside the project qualify", () => {
  const file = moduleReport("a.test.ts");
  NodeAssert.throws(() => caseResults(report(file, file), root), /duplicate test module/);
  NodeAssert.throws(() => caseResults(report(moduleReport("../b.test.ts")), root), /Invalid/);
  NodeAssert.throws(() => caseResults({ ...report(file), success: false }, root), /passing/);
  NodeAssert.throws(
    () => caseResults({ ...report(file), numTotalTests: 2 }, root),
    /do not reconcile/,
  );
});

import * as NodeAssert from "node:assert/strict";
import * as NodeTest from "node:test";
import {
  caseResults,
  combineCaseReports,
  pilotCacheIdentity,
  rolloutCacheKeys,
  validateArchive,
  verifyCases,
} from "./server-result-cache.mjs";

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

const runner = {
  GITHUB_REPOSITORY: "Jones-Systems/Jones-Code",
  GITHUB_REF: "refs/pull/1145/merge",
  GITHUB_RUN_ID: "123",
  GITHUB_RUN_ATTEMPT: "1",
  GITHUB_SHA: "a".repeat(40),
  RUNNER_OS: "Linux",
  RUNNER_ARCH: "X64",
  ImageVersion: "20261116.133.1",
};
const toolchain = "a".repeat(64);

NodeTest.test("PR pushes share candidates, isolated by PR, shard and runner identity", () => {
  const first = rolloutCacheKeys("4/8", toolchain, runner);
  const next = rolloutCacheKeys("4/8", toolchain, {
    ...runner,
    GITHUB_RUN_ID: "124",
    GITHUB_SHA: "b".repeat(40),
  });
  NodeAssert.equal(first.prefix, next.prefix);
  NodeAssert.notEqual(first.key, next.key);
  for (const changed of [
    { GITHUB_REF: "refs/pull/1146/merge" },
    { RUNNER_ARCH: "ARM64" },
    { ImageVersion: "20261117.100.1" },
  ]) {
    NodeAssert.notEqual(
      first.prefix,
      rolloutCacheKeys("4/8", toolchain, { ...runner, ...changed }).prefix,
    );
  }
  NodeAssert.notEqual(first.prefix, rolloutCacheKeys("5/8", toolchain, runner).prefix);
  NodeAssert.notEqual(first.prefix, rolloutCacheKeys("4/8", "b".repeat(64), runner).prefix);
  NodeAssert.throws(
    () =>
      rolloutCacheKeys("4/8", toolchain, {
        ...runner,
        GITHUB_REF: "refs/heads/main",
      }),
    /identity/,
  );
});

NodeTest.test(
  "restored results require a receipt for the matched archive and per-file producers",
  () => {
    const { key, prefix } = rolloutCacheKeys("4/8", toolchain, runner);
    const record = {
      producer: { commit: runner.GITHUB_SHA, run: runner.GITHUB_RUN_ID, attempt: "1" },
      reportSha256: "b".repeat(64),
    };
    const archive = { schema: "jones.server-result-cache/v1", key, files: { "a.test.ts": record } };
    NodeAssert.deepEqual(validateArchive(archive, key, prefix).get("a.test.ts"), record);
    NodeAssert.throws(() => validateArchive(archive, key + "1", prefix), /matching receipt/);
    NodeAssert.throws(
      () => validateArchive(archive, key, prefix + "unrelated"),
      /matching receipt/,
    );
    NodeAssert.throws(
      () => validateArchive({ ...archive, files: { "a.test.ts": {} } }, key, prefix),
      /producer/,
    );
  },
);

NodeTest.test("combined shard cases retain counts, identity and outcome from each file", () => {
  const totals = {
    numTotalTestSuites: 1,
    numPassedTestSuites: 1,
    numFailedTestSuites: 0,
    numPendingTestSuites: 0,
    numPassedTests: 1,
    numPendingTests: 0,
    numTodoTests: 0,
  };
  const one = { ...report(moduleReport("a.test.ts")), ...totals };
  const two = {
    ...report(moduleReport("b.test.mjs", "pending")),
    ...totals,
    numPassedTests: 0,
    numPendingTests: 1,
  };
  const combined = combineCaseReports([one, two]);
  verifyCases(
    caseResults(report(...one.testResults, ...two.testResults), root),
    caseResults(combined, root),
  );
  NodeAssert.equal(combined.numPassedTests, 1);
  NodeAssert.equal(combined.numPendingTests, 1);
  NodeAssert.throws(() => combineCaseReports([report(moduleReport("a.test.ts"))]), /totals/);
});

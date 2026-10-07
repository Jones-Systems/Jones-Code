import * as NodeAssert from "node:assert/strict";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import { currentQualificationModules, currentQualificationUnavailable, currentQualificationMetadata, currentQualificationClosureKnown, validateCurrentQualificationRequest, runCurrentQualification } from "./current-qualification.mjs";

NodeTest.test("current V2 qualification keeps initialization and storage coverage distinct from historical fork007", () => {
  NodeAssert.equal(currentQualificationModules.length, 3);
  NodeAssert.ok(currentQualificationModules.includes("apps/server/src/persistence/initializeV2Database.test.ts"));
  NodeAssert.ok(currentQualificationModules.every((file) => !file.includes("007_Jones")));
});

NodeTest.test("qualifies an explicitly bound clean V2 candidate with synthetic databases", {
  skip: process.env.JONES_CURRENT_QUALIFICATION_REQUEST ? false : "explicit candidate/runtime binding required",
}, async (test) => {
  const path = process.env.JONES_CURRENT_QUALIFICATION_REQUEST;
  NodeAssert.ok(NodePath.isAbsolute(path));
  const info = NodeFS.lstatSync(path);
  NodeAssert.ok(info.isFile() && !info.isSymbolicLink() && info.size <= 16384);
  const request = JSON.parse(NodeFS.readFileSync(path, "utf8"));
  const result = await runCurrentQualification(request, test.signal);
  test.diagnostic(JSON.stringify(result));
  NodeAssert.equal(result.outcome, "passed");
});

NodeTest.test("qualification metadata records the unavailable native acceptance module", () => {
  const metadata = currentQualificationMetadata();
  NodeAssert.equal(metadata.outcome, "unavailable");
  NodeAssert.equal(metadata.reason, "explicit candidate/runtime binding required");
  NodeAssert.deepEqual(metadata.modules, currentQualificationModules);
  NodeAssert.ok(currentQualificationUnavailable.includes("StoreNativeAcceptance.integration.test.ts: unavailable until #148 source is bound"));
});

NodeTest.test("qualification refuses another checkout and unbounded request fields before source reads", () => {
  NodeAssert.throws(() => validateCurrentQualificationRequest({ candidate: { worktreePath: "/other/source" }, parentPath: "/explicit/scratch" }));
  NodeAssert.throws(() => validateCurrentQualificationRequest({ candidate: {}, parentPath: "/explicit/scratch", databasePath: "/live.sqlite" }));
});

NodeTest.test("qualification retains roots unless the runner acknowledges closure independently of its child", () => {
  for (const outcome of ["success", "failed", "cancelled", "timed_out", "output_limited", "spawn_refused"]) {
    const child = { closed: true, reaped: true, outcome };
    NodeAssert.equal(currentQualificationClosureKnown(child, { runnerClosed: true }, true), true);
    NodeAssert.equal(currentQualificationClosureKnown(child, { runnerClosed: false }, true), false);
    NodeAssert.equal(currentQualificationClosureKnown(child, { databaseClosed: true }, true), false);
    NodeAssert.equal(currentQualificationClosureKnown(child, { runnerClosed: true }, false), false);
    NodeAssert.equal(currentQualificationClosureKnown({ ...child, closed: false }, { runnerClosed: true }, true), false);
    NodeAssert.equal(currentQualificationClosureKnown({ ...child, reaped: false }, { runnerClosed: true }, true), false);
  }
  NodeAssert.equal(currentQualificationClosureKnown({ closed: true, reaped: true, outcome: "unknown" }, { runnerClosed: true }, true), false);
  NodeAssert.equal(currentQualificationClosureKnown(undefined, undefined, true), true);
  NodeAssert.equal(currentQualificationClosureKnown(undefined, undefined, false), false);
});

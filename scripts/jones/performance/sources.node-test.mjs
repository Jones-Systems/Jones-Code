import * as NodeAssert from "node:assert/strict";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import * as NodeURL from "node:url";

import {
  assertCurrentDatabaseSource,
  currentDatabaseSource,
  assertQualificationDatabaseSource,
  assertSyntheticDatabaseSource,
  qualificationDatabaseSource,
  qualificationSourcePins,
  sourceParentEnvironment,
  syntheticDatabaseSource,
  syntheticSourceParent,
  syntheticSourcePins,
} from "./sources.mjs";

const testDirectory = NodePath.dirname(NodeURL.fileURLToPath(import.meta.url));

function withScratch(body) {
  const scratch = NodeFS.mkdtempSync(NodePath.join(testDirectory, ".sources-test-"));
  try {
    return body(scratch, { [sourceParentEnvironment]: scratch });
  } finally {
    NodeFS.rmSync(scratch, { recursive: true, force: true });
  }
}

NodeTest.test("source parents require explicit canonical directories and refuse aliases", () => {
  NodeAssert.throws(() => syntheticSourceParent({}), { code: "invalid_source" });
  NodeAssert.throws(() => syntheticSourceParent({ [sourceParentEnvironment]: "." }), {
    code: "invalid_source",
  });
  withScratch((scratch, environment) => {
    NodeAssert.equal(syntheticSourceParent(environment), scratch);
    const alias = NodePath.join(scratch, "alias");
    NodeFS.symlinkSync(scratch, alias);
    NodeAssert.throws(() => syntheticSourceParent({ [sourceParentEnvironment]: alias }), {
      code: "invalid_source",
    });
    const file = NodePath.join(scratch, "file");
    NodeFS.writeFileSync(file, "synthetic");
    NodeAssert.throws(() => syntheticSourceParent({ [sourceParentEnvironment]: file }), {
      code: "invalid_source",
    });
    NodeAssert.equal(NodeFS.readFileSync(file, "utf8"), "synthetic");
  });
});

NodeTest.test("baseline sources cannot acquire qualification-only or unpinned revisions", () => {
  withScratch((scratch, environment) => {
    for (const pin of syntheticSourcePins) {
      const source = syntheticDatabaseSource(pin.sourceRevision, environment);
      NodeAssert.equal(source.repository, "Jones-Systems/Jones-Code");
      NodeAssert.equal(source.worktreePath, NodePath.join(scratch, pin.directory));
      NodeAssert.equal(Object.isFrozen(source), true);
    }
    for (const pin of qualificationSourcePins) {
      const source = qualificationDatabaseSource(pin.sourceRevision, environment);
      NodeAssert.equal(source.worktreePath, NodePath.join(scratch, pin.directory));
      if (!syntheticSourcePins.some((baseline) => baseline.sourceRevision === pin.sourceRevision)) {
        NodeAssert.throws(() => syntheticDatabaseSource(pin.sourceRevision, environment), {
          code: "invalid_source",
        });
      }
    }
    for (const construct of [syntheticDatabaseSource, qualificationDatabaseSource]) {
      NodeAssert.throws(() => construct("a".repeat(40), environment), { code: "invalid_source" });
      NodeAssert.throws(() => construct(syntheticSourcePins[0].sourceRevision, {}), {
        code: "invalid_source",
      });
    }
  });
});

NodeTest.test("forged repository, path, fields and checkout aliases never qualify", () => {
  withScratch((scratch, environment) => {
    const pin = syntheticSourcePins[0];
    const source = syntheticDatabaseSource(pin.sourceRevision, environment);
    const canary = NodePath.join(scratch, "canary");
    NodeFS.mkdirSync(canary);
    NodeFS.writeFileSync(NodePath.join(canary, "keep"), "unchanged");
    NodeFS.symlinkSync(canary, source.worktreePath);
    for (const check of [assertSyntheticDatabaseSource, assertQualificationDatabaseSource]) {
      for (const forged of [
        { ...source, repository: "another/repository" },
        { ...source, worktreePath: canary },
        { ...source, extra: true },
        source,
      ]) {
        NodeAssert.throws(() => check(forged, environment), { code: "invalid_source" });
      }
    }
    NodeAssert.equal(NodeFS.readFileSync(NodePath.join(canary, "keep"), "utf8"), "unchanged");
  });
});

NodeTest.test(
  "prepared historical sources match their exact commit, tree and frozen lock",
  { skip: process.env[sourceParentEnvironment] ? false : "historical source parent unbound" },
  () => {
    for (const pin of qualificationSourcePins) {
      const source = qualificationDatabaseSource(pin.sourceRevision);
      NodeAssert.deepEqual(assertQualificationDatabaseSource(source), source);
    }
  },
);

NodeTest.test("current candidate binds clean HEAD, tree and frozen lock; mismatches refuse", () => {
  const worktree = NodePath.resolve(testDirectory, "../../..");
  const candidate = currentDatabaseSource(worktree);
  NodeAssert.deepEqual(assertCurrentDatabaseSource(candidate), candidate);
  for (const forged of [
    { ...candidate, sourceRevision: "a".repeat(40) }, { ...candidate, tree: "b".repeat(40) },
    { ...candidate, lockSha256: "c".repeat(64) }, { ...candidate, repository: "other/repository" },
    { ...candidate, extra: true }, { ...candidate, worktreePath: NodePath.dirname(worktree) },
  ]) NodeAssert.throws(() => assertCurrentDatabaseSource(forged), { code: "invalid_source" });
});

import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import * as NodeTest from "node:test";
import * as NodeAssert from "node:assert/strict";
import { createManifest, validateManifest, NOT_COVERED, writeManifest } from "./manifest.mjs";

function candidate() {
  const hash = "a".repeat(64),
    head = "b".repeat(40);
  return createManifest({
    runId: "test-run",
    provenance: {
      build: Object.fromEntries(
        ["desktop", "server", "web", "boot"].map((name) => [name, { sha256: hash }]),
      ),
      harness: { head },
      source: { head, statusSha256: hash, patch: { sha256: hash }, untracked: { sha256: hash } },
    },
    scenario: { id: "sidebar-rename", sha256: hash },
    inner: {
      steps: [{ name: "Read backend", status: "passed" }],
      assertions: [{ name: "Rename persists", status: "passed" }],
    },
    status: "passed",
    cleanup: { outcome: "complete" },
  });
}
NodeTest.test("manifest round-trips and claims only asserted behavior", () => {
  const value = candidate();
  NodeAssert.deepEqual(validateManifest(JSON.parse(JSON.stringify(value))), value);
  NodeAssert.deepEqual(value.coverage.claims, ["Rename persists"]);
  NodeAssert.deepEqual(value.coverage.notCovered, NOT_COVERED);
  NodeAssert.equal(value.runtime, null);
  NodeAssert.match(value.coverage.comparison, /one candidate build/);
});
NodeTest.test("manifest rejects abbreviated identity and false passing evidence", () => {
  const value = candidate();
  value.source.head = "b".repeat(8);
  NodeAssert.throws(() => validateManifest(value), /full source/);
  const incomplete = candidate();
  incomplete.scenario.steps[0].status = "running";
  NodeAssert.throws(() => validateManifest(incomplete), /incomplete/);
  const capture = candidate();
  capture.captures.push({
    file: "../escape.png",
    sha256: "a".repeat(64),
    width: 100,
    height: 100,
    scale: 1,
    theme: "dark",
  });
  NodeAssert.throws(() => validateManifest(capture), /capture/);
});

NodeTest.test(
  "manifest publication rejects a sandbox-created symlink without changing its target",
  async () => {
    const parent = NodePath.resolve(
      NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)),
      "../../../../.t3/ui-evidence-test-scratch",
    );
    await NodeFSP.mkdir(parent, { recursive: true });
    const root = await NodeFSP.mkdtemp(NodePath.join(parent, "manifest-"));
    try {
      const artifacts = NodePath.join(root, "artifacts");
      await NodeFSP.mkdir(artifacts);
      const sentinel = NodePath.join(root, "sentinel");
      await NodeFSP.writeFile(sentinel, "preserved");
      await NodeFSP.symlink(sentinel, NodePath.join(artifacts, "manifest.json"));
      await NodeAssert.rejects(writeManifest(artifacts, candidate()), { code: "EEXIST" });
      NodeAssert.equal(await NodeFSP.readFile(sentinel, "utf8"), "preserved");
    } finally {
      await NodeFSP.rm(root, { recursive: true, force: true });
    }
  },
);

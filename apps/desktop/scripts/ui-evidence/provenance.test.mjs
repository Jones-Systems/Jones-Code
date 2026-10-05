import * as NodeTest from "node:test";
import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeUtil from "node:util";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import { collectProvenance } from "./provenance.mjs";
const exec = NodeUtil.promisify(NodeChildProcess.execFile);

NodeTest.test(
  "provenance distinguishes tracked and untracked changes, excludes secrets, and matches exact build receipt",
  async () => {
    const parent = NodePath.resolve(
      NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)),
      "../../../../.t3/ui-evidence-test-scratch",
    );
    await NodeFSP.mkdir(parent, { recursive: true });
    const root = await NodeFSP.mkdtemp(NodePath.join(parent, "provenance-"));
    try {
      const source = NodePath.join(root, "source");
      await NodeFSP.mkdir(source);
      const git = (...args) => exec("git", ["-C", source, ...args]);
      await git("init", "-b", "test-fixture");
      await NodeFSP.writeFile(NodePath.join(source, "tracked.txt"), "before\n");
      await git("add", "tracked.txt");
      await git(
        "-c",
        "user.name=Fixture",
        "-c",
        "user.email=fixture@example.invalid",
        "commit",
        "-m",
        "Fixture",
      );
      const buildPaths = {};
      for (const name of ["desktop", "server", "web"]) {
        buildPaths[name] = NodePath.join(root, name);
        await NodeFSP.mkdir(buildPaths[name]);
        await NodeFSP.writeFile(
          NodePath.join(buildPaths[name], name === "desktop" ? "boot.cjs" : "output.js"),
          "synthetic output",
        );
      }
      let n = 0;
      const collect = async (buildReceipt) => {
        const artifacts = NodePath.join(root, `artifacts-${n++}`);
        await NodeFSP.mkdir(artifacts);
        return collectProvenance({ source, harness: source, artifacts, buildPaths, buildReceipt });
      };
      const before = await collect();
      await NodeFSP.writeFile(NodePath.join(source, "tracked.txt"), "after\n");
      const tracked = await collect();
      NodeAssert.notEqual(tracked.source.patch.sha256, before.source.patch.sha256);
      await NodeFSP.writeFile(NodePath.join(source, "untracked.txt"), "first");
      await NodeFSP.writeFile(NodePath.join(source, ".env.local"), "SECRET_SENTINEL");
      const first = await collect();
      await NodeFSP.writeFile(NodePath.join(source, "untracked.txt"), "second");
      const second = await collect();
      NodeAssert.notEqual(first.source.untracked.sha256, second.source.untracked.sha256);
      NodeAssert.equal(second.source.untracked.count, 1);
      NodeAssert.equal(second.build.sourceCorrespondence.status, "unproved");
      const receipt = {
        schema: "jones-code-ui-evidence-build/v1",
        source: {
          head: second.source.head,
          statusSha256: second.source.statusSha256,
          patchSha256: second.source.patch.sha256,
          untrackedSha256: second.source.untracked.sha256,
        },
        build: Object.fromEntries(
          ["desktop", "server", "web", "boot"].map((name) => [name, second.build[name].sha256]),
        ),
        invocation: "synthetic fixture output",
        startedAt: "2026-09-30T00:00:00Z",
        finishedAt: "2026-09-30T00:00:01Z",
      };
      const receiptPath = NodePath.join(root, "receipt.json");
      await NodeFSP.writeFile(receiptPath, JSON.stringify(receipt));
      NodeAssert.equal(
        (await collect(receiptPath)).build.sourceCorrespondence.status,
        "receipt-matched",
      );
      receipt.build.boot = "0".repeat(64);
      await NodeFSP.writeFile(receiptPath, JSON.stringify(receipt));
      await NodeAssert.rejects(collect(receiptPath), /output mismatch/);
      NodeAssert.equal(
        (await NodeFSP.readFile(NodePath.join(root, "artifacts-3/source.patch"), "utf8")).includes(
          "SECRET_SENTINEL",
        ),
        false,
      );
    } finally {
      await NodeFSP.rm(root, { recursive: true, force: true });
    }
  },
);

import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";

const repository = NodePath.resolve(import.meta.dirname, "../../..");
NodeTest.test(
  "default entrypoint links synthetic configuration without starting an installer",
  () => {
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "jones-setup-entry-"));
    try {
      const worktree = NodePath.join(root, "worktree");
      const project = NodePath.join(root, "project");
      const bin = NodePath.join(root, "bin");
      NodeFS.mkdirSync(NodePath.join(worktree, "scripts", "jones"), { recursive: true });
      NodeFS.mkdirSync(project);
      NodeFS.mkdirSync(bin);
      NodeFS.writeFileSync(NodePath.join(project, ".env"), "SYNTHETIC_FIXTURE=1\n");
      NodeFS.writeFileSync(NodePath.join(bin, "vp"), "#!/bin/sh\nexit 73\n", { mode: 0o755 });
      NodeFS.copyFileSync(
        NodePath.join(repository, "scripts/setup-worktree.ts"),
        NodePath.join(worktree, "scripts/setup-worktree.ts"),
      );
      const implementation = NodePath.join(repository, "scripts/jones/worktree-setup/setup.ts");
      if (NodeFS.existsSync(implementation)) {
        NodeFS.mkdirSync(NodePath.join(worktree, "scripts/jones/worktree-setup"));
        NodeFS.copyFileSync(
          implementation,
          NodePath.join(worktree, "scripts/jones/worktree-setup/setup.ts"),
        );
      }
      const result = NodeChildProcess.spawnSync(
        process.execPath,
        [NodePath.join(worktree, "scripts/setup-worktree.ts")],
        {
          env: { PATH: bin, T3CODE_PROJECT_ROOT: project },
          encoding: "utf8",
        },
      );
      NodeAssert.equal(result.status, 0, result.stderr);
      NodeAssert.equal(
        NodeFS.readlinkSync(NodePath.join(worktree, ".env")),
        NodePath.join(project, ".env"),
      );
      NodeAssert.equal(NodeFS.existsSync(NodePath.join(worktree, "node_modules")), false);
    } finally {
      NodeFS.rmSync(root, { recursive: true, force: true });
    }
  },
);

import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import * as fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { test } from "node:test";

const repository = path.resolve(import.meta.dirname, "../../..");
test("default entrypoint links synthetic configuration without starting an installer", () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "jones-setup-entry-"));
  try {
    const worktree = path.join(root, "worktree");
    const project = path.join(root, "project");
    const bin = path.join(root, "bin");
    fs.mkdirSync(path.join(worktree, "scripts", "jones"), { recursive: true });
    fs.mkdirSync(project);
    fs.mkdirSync(bin);
    fs.writeFileSync(path.join(project, ".env"), "SYNTHETIC_FIXTURE=1\n");
    fs.writeFileSync(path.join(bin, "vp"), "#!/bin/sh\nexit 73\n", { mode: 0o755 });
    fs.copyFileSync(path.join(repository, "scripts/setup-worktree.ts"), path.join(worktree, "scripts/setup-worktree.ts"));
    const implementation = path.join(repository, "scripts/jones/worktree-setup/setup.ts");
    if (fs.existsSync(implementation)) {
      fs.mkdirSync(path.join(worktree, "scripts/jones/worktree-setup"));
      fs.copyFileSync(implementation, path.join(worktree, "scripts/jones/worktree-setup/setup.ts"));
    }
    const result = spawnSync(process.execPath, [path.join(worktree, "scripts/setup-worktree.ts")], {
      env: { PATH: bin, T3CODE_PROJECT_ROOT: project }, encoding: "utf8",
    });
    assert.equal(result.status, 0, result.stderr);
    assert.equal(fs.readlinkSync(path.join(worktree, ".env")), path.join(project, ".env"));
    assert.equal(fs.existsSync(path.join(worktree, "node_modules")), false);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

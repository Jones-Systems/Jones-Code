import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { homedir } from "node:os";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readSource, resolveSource, objectGit, SOURCE_LIMITS } from "./SourceBroker.ts";
import { createConversationDirectory, disposeConversationDirectory, resumeConversationDirectory } from "./ConversationDirectory.ts";

async function fixture(run: (root: string) => Promise<void>) {
  const parent = path.join(homedir(), "Library", "Caches");
  await fs.mkdir(parent, { recursive: true });
  const root = await fs.mkdtemp(path.join(parent, "jones-c9-fixture-"));
  try { await run(root); }
  finally { await fs.rm(root, { recursive: true, force: true }); assert.equal(await fs.stat(root).catch(() => null), null); }
}
const git = (cwd: string, ...args: string[]) => execFileSync("git", ["-c", "init.defaultBranch=main", "-c", "user.name=C9 Fixture", "-c", "user.email=c9@example.invalid", ...args], { cwd, encoding: "utf8", env: { ...process.env, GIT_CONFIG_GLOBAL: "/dev/null", GIT_CONFIG_NOSYSTEM: "1", GIT_TERMINAL_PROMPT: "0" }, maxBuffer: 1024 * 1024 }).trim();
async function treeDigest(root: string) {
  const hash = createHash("sha256");
  async function visit(directory: string) {
    for (const name of (await fs.readdir(directory)).sort()) {
      const file = path.join(directory, name); const stat = await fs.lstat(file);
      hash.update(path.relative(root, file));
      if (stat.isDirectory()) await visit(file); else hash.update(await fs.readFile(file));
    }
  }
  await visit(root); return hash.digest("hex");
}

export const sourceCases = {
  "source reads use the recorded objects, resist worktree/config traps, and leave refs/index/objects unchanged": () => fixture(async (root) => {
  const repo = path.join(root, "repo"); const state = path.join(root, "state");
  await fs.mkdir(repo); await fs.mkdir(state);
  git(repo, "init", "--template=");
  await fs.writeFile(path.join(repo, "AGENTS.md"), "Instructions at first commit\n");
  await fs.writeFile(path.join(repo, "file.txt"), "first source needle\n");
  await fs.writeFile(path.join(repo, ".gitattributes"), "*.txt diff=c9trap\n");
  git(repo, "add", "."); git(repo, "commit", "-qm", "first");
  const first = await resolveSource(repo, "main");
  await fs.writeFile(path.join(repo, "file.txt"), "second source\n");
  git(repo, "add", "."); git(repo, "commit", "-qm", "second");
  const second = await resolveSource(repo, "main");
  const trap = path.join(root, "trap.sh"); const touched = path.join(root, "trap-touched");
  await fs.writeFile(trap, `#!/bin/sh\ntouch '${touched}'\nexit 99\n`, { mode: 0o700 });
  git(repo, "config", "diff.external", trap); git(repo, "config", "diff.c9trap.textconv", trap);
  await fs.writeFile(path.join(repo, "file.txt"), "uncommitted decoy\n");
  const directory = await createConversationDirectory(state, [repo, first.commonDirectory]);
  assert.deepEqual(await fs.readdir(directory.path), []);
  const before = await treeDigest(path.join(repo, ".git"));
  assert.equal(await readSource(first, { operation: "read", path: "file.txt" }), "first source needle\n");
  assert.match(await readSource(first, { operation: "list" }), /AGENTS.md/);
  assert.match(await readSource(first, { operation: "search", query: "needle" }), /first source needle/);
  assert.match(await readSource(first, { operation: "log" }), /first/);
  assert.match(await readSource(second, { operation: "show" }), /second/);
  assert.match(await readSource(second, { operation: "diff", from: first.commit }), /second source/);
  assert.equal(await treeDigest(path.join(repo, ".git")), before);
  assert.equal(await fs.stat(touched).catch(() => null), null);
  assert.deepEqual(await fs.readdir(directory.path), []);
  await assert.rejects(readSource(first, { operation: "read", path: "../outside" }), /invalid_request/);
  await assert.rejects(readSource(first, { operation: "diff", from: "HEAD" }), /invalid_request/);
  assert.equal(await disposeConversationDirectory(directory), "disposed");
}),

  "source output and child lifetime are bounded, with no uncancelled background reader": () => fixture(async (root) => {
  const repo = path.join(root, "repo"); await fs.mkdir(repo); git(repo, "init", "--template=");
  await fs.writeFile(path.join(repo, "large"), "x".repeat(SOURCE_LIMITS.bytes + 1));
  git(repo, "add", "."); git(repo, "commit", "-qm", "large");
  const snapshot = await resolveSource(repo, "main");
  await assert.rejects(readSource(snapshot, { operation: "read", path: "large" }), /limit/);
  const bin = path.join(root, "bin"); await fs.mkdir(bin);
  const pidFile = path.join(root, "pid");
  await fs.writeFile(path.join(bin, "git"), `#!/bin/sh\necho $$ > '${pidFile}'\nexec /bin/sleep 10\n`, { mode: 0o700 });
  const oldPath = process.env.PATH;
  try {
    process.env.PATH = `${bin}:${oldPath}`;
    await assert.rejects(objectGit(["log"], { timeoutMs: 1000 }), /limit/);
    const pid = Number(await fs.readFile(pidFile, "utf8"));
    assert.throws(() => process.kill(pid, 0), { code: "ESRCH" });
  } finally { process.env.PATH = oldPath; }
}),

  "exact empty disposal and same-path resume preserve nonempty content and refuse checkout/temp placement": () => fixture(async (root) => {
  const state = path.join(root, "state"); const repo = path.join(root, "repo");
  await fs.mkdir(state); await fs.mkdir(repo); git(repo, "init", "--template=");
  const identity = await createConversationDirectory(state, [repo]);
  await fs.writeFile(path.join(identity.path, "owned-note"), "retain me");
  assert.equal(await disposeConversationDirectory(identity), "conversation_dir_nonempty");
  assert.equal(await fs.readFile(path.join(identity.path, "owned-note"), "utf8"), "retain me");
  await fs.unlink(path.join(identity.path, "owned-note"));
  assert.equal(await disposeConversationDirectory(identity), "disposed");
  const resumed = await resumeConversationDirectory(identity);
  assert.equal(resumed.path, identity.path); assert.deepEqual(await fs.readdir(resumed.path), []);
  assert.equal(await disposeConversationDirectory(resumed), "disposed");
  const inRepo = path.join(repo, "state"); await fs.mkdir(inRepo);
  await assert.rejects(createConversationDirectory(inRepo, []), /Git checkout/);
  await assert.rejects(createConversationDirectory(state, [root]), /excluded root/);
}),
};

import * as Crypto from "node:crypto";
import * as FS from "node:fs";
import * as Path from "node:path";
import { createOwnedRoot, disposeOwnedRoot } from "./guard.mjs";
import { runOwnedChild } from "./lifecycle.mjs";
import { qualificationSourcePins, qualificationDatabaseSource, assertQualificationDatabaseSource, sourceParentEnvironment } from "./sources.mjs";

export async function withPreparedHistoricalSources(options, use) {
  if (options?.explicitHistoricalRequest !== true || typeof use !== "function" || !Path.isAbsolute(options.gitExecutable ?? ""))
    throw Object.assign(new Error("historical source preparation needs explicit opt-in and an exact Git executable"), { code: "unavailable" });
  const backing = FS.statfsSync(options.parentPath);
  if ([0x01021994, 0x858458f6].includes(backing.type)) throw new Error("historical clones require disk-backed scratch");
  const policy = { ...options.policy, maxFiles: Math.min(options.policy?.maxFiles ?? 50000, 50000), maxFileBytes: Math.min(options.policy?.maxFileBytes ?? 128 * 1024 * 1024, 128 * 1024 * 1024), maxTotalBytes: Math.min(options.policy?.maxTotalBytes ?? 1024 * 1024 * 1024, 1024 * 1024 * 1024) };
  const owner = createOwnedRoot({ ...options, policy, childName: `historical-sources-${Crypto.randomUUID()}` });
  const root = owner.creationReceipt.canonicalRootPath;
  const children = [];
  let value, failure;
  const environment = { [sourceParentEnvironment]: root };
  const childEnvironment = { PATH: process.env.PATH ?? "/usr/bin:/bin", HOME: Path.join(root, "home"), TMPDIR: Path.join(root, "tmp"), LANG: "C.UTF-8", TZ: "UTC", GIT_CONFIG_NOSYSTEM: "1", GIT_CONFIG_GLOBAL: "/dev/null", GIT_TERMINAL_PROMPT: "0" };
  const run = async (args) => {
    const receipt = await runOwnedChild({ owner, executable: options.gitExecutable, args, env: childEnvironment, timeoutMs: 120000, terminateGraceMs: 2000, reapTimeoutMs: 5000, maxOutputBytes: 64 * 1024, signal: options.signal });
    children.push(receipt);
    if (!receipt.closed || !receipt.reaped || receipt.outcome !== "success") throw Object.assign(new Error("historical source Git operation failed"), { receipt });
  };
  try {
    FS.mkdirSync(childEnvironment.HOME, { mode: 0o700 });
    FS.mkdirSync(childEnvironment.TMPDIR, { mode: 0o700 });
    const sources = [];
    for (const pin of qualificationSourcePins) {
      options.signal?.throwIfAborted();
      const source = qualificationDatabaseSource(pin.sourceRevision, environment);
      await run(["init", "--quiet", source.worktreePath]);
      // Keep repository symlink entries as tracked text so custody never traverses aliases.
      await run(["-C", source.worktreePath, "config", "core.symlinks", "false"]);
      await run(["-C", source.worktreePath, "fetch", "--quiet", "--depth=1", "https://github.com/Jones-Systems/Jones-Code.git", pin.sourceRevision]);
      await run(["-C", source.worktreePath, "checkout", "--quiet", "-b", "performance-source", "FETCH_HEAD"]);
      sources.push(assertQualificationDatabaseSource(source, environment));
    }
    // Dependency installation is a separately requested step; cloning alone does not qualify execution.
    value = await use({ root, sources, environment, dependencies: "unprepared" });
  } catch (error) { failure = error instanceof Error ? error : new Error(String(error)); }
  const cleanup = disposeOwnedRoot(owner, { childReceipts: children });
  const evidencePath = cleanup.outcome === "complete" ? null : `${root}.json`;
  if (failure || cleanup.outcome !== "complete") {
    const error = failure ?? new Error("historical source cleanup retained");
    error.evidence = { creationReceipt: owner.creationReceipt, children, cleanup, evidencePath, dependencies: "unprepared", storageLimits: { files: policy.maxFiles, fileBytes: policy.maxFileBytes, totalBytes: policy.maxTotalBytes } };
    if (cleanup.outcome !== "complete") FS.writeFileSync(`${root}.json`, `${JSON.stringify(error.evidence)}\n`, { flag: "wx", mode: 0o600 });
    throw error;
  }
  return { value, cleanup, dependencies: "unprepared" };
}

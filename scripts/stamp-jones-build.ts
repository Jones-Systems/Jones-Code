// @effect-diagnostics nodeBuiltinImport:off -- Build-job source stamping and its isolated Git fixture use native Node adapters.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

/** Build-job-only stamping of the committed tree, independent of preview package version edits. */
const root = NodePath.resolve(NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)), "..");
const git = (args: string[]) =>
  NodeChildProcess.execFileSync("git", args, { cwd: root, encoding: "utf8" }).trim();
const sha = git(["rev-parse", "HEAD"]);
const tree = git(["rev-parse", "HEAD^{tree}"]);
if (!/^[a-f0-9]{40}$/.test(sha) || !/^[a-f0-9]{40}$/.test(tree))
  throw new Error("Invalid Jones build source.");
if (process.env.GITHUB_SHA && process.env.GITHUB_SHA !== sha)
  throw new Error("Checkout does not match workflow source.");
const identity = { repository: "Jones-Systems/Jones-Code", sha, tree };
for (const relative of ["apps/server/package.json", "apps/desktop/package.json"]) {
  const file = NodePath.join(root, relative);
  const manifest = JSON.parse(NodeFS.readFileSync(file, "utf8")) as Record<string, unknown>;
  manifest.jonesSource = identity;
  manifest.startupGateProtocol = 1;
  NodeFS.writeFileSync(file, JSON.stringify(manifest, null, 2) + "\n");
}
if (process.env.GITHUB_ENV) {
  NodeFS.appendFileSync(
    process.env.GITHUB_ENV,
    `JONES_SOURCE_SHA=${sha}\nJONES_SOURCE_TREE=${tree}\n`,
  );
}

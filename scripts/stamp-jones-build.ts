// @effect-diagnostics nodeBuiltinImport:off globalDate:off -- Build-job source stamping uses native Node adapters; Date validates supplied metadata, never the clock.
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
// Pull-request jobs may check out the head while GitHub retains the merge SHA in GITHUB_SHA.
const expectedSourceSha =
  process.env.JONES_EXPECTED_SOURCE_SHA ?? (process.env.GITHUB_SHA || undefined);
if (expectedSourceSha !== undefined && !/^[a-f0-9]{40}$/.test(expectedSourceSha))
  throw new Error("Invalid expected Jones build source.");
if (expectedSourceSha !== undefined && expectedSourceSha !== sha)
  throw new Error("Checkout does not match workflow source.");
const buildStartedAt = process.env.JONES_BUILD_STARTED_AT || undefined;
if (buildStartedAt !== undefined) {
  const match = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,3}))?Z$/.exec(buildStartedAt);
  const instant = Date.parse(buildStartedAt);
  if (
    !match ||
    !Number.isFinite(instant) ||
    new Date(instant).toISOString() !== `${match[1]}.${(match[2] ?? "").padEnd(3, "0")}Z`
  )
    throw new Error("Invalid Jones build start time.");
}
const identity = { repository: "Jones-Systems/Jones-Code", sha, tree };
for (const relative of ["apps/server/package.json", "apps/desktop/package.json"]) {
  const file = NodePath.join(root, relative);
  const manifest = JSON.parse(NodeFS.readFileSync(file, "utf8")) as Record<string, unknown>;
  manifest.jonesSource = identity;
  manifest.startupGateProtocol = 1;
  if (buildStartedAt === undefined) delete manifest.jonesBuildStartedAt;
  else manifest.jonesBuildStartedAt = buildStartedAt;
  NodeFS.writeFileSync(file, JSON.stringify(manifest, null, 2) + "\n");
}
if (process.env.GITHUB_ENV) {
  NodeFS.appendFileSync(
    process.env.GITHUB_ENV,
    `JONES_SOURCE_SHA=${sha}\nJONES_SOURCE_TREE=${tree}\n`,
  );
}

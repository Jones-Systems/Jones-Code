import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";

export const sourceParentEnvironment = "JONES_PERFORMANCE_SOURCE_PARENT";
const defaultParent = "/home/malcolmjones/Projects/Jones-Code-performance-worktrees-20261002";
export const syntheticSourcePins = Object.freeze([
  Object.freeze({
    directory: "baseline",
    sourceRevision: "e5a31aceec91484b64315c63dcce80f6e7581604",
    tree: "79792d4a66914f7750fd57268845b3aa0afd77aa",
  }),
  Object.freeze({
    directory: "live-baseline",
    sourceRevision: "414bb8da204c3275cd0b76b2ec4d74dfb09a97e4",
    tree: "57a0d6d07aa2f67976018d9e7a6cca2a925d5e1c",
  }),
]);
const lockSha256 = "34460ca4290c8132ae0f26ba0224476b22c39983e8bcd735e357f15989592fc4";

function refuse(message, cause) {
  throw Object.assign(new Error(message, { cause }), { code: "invalid_source" });
}

export function syntheticSourceParent(environment = process.env) {
  const parent = environment[sourceParentEnvironment] ?? defaultParent;
  try {
    if (
      typeof parent !== "string" ||
      !NodePath.isAbsolute(parent) ||
      NodePath.resolve(parent) !== parent ||
      NodeFS.realpathSync(parent) !== parent ||
      !NodeFS.statSync(parent).isDirectory()
    )
      refuse("source parent must be an existing canonical absolute directory");
    return parent;
  } catch (error) {
    if (error.code === "invalid_source") throw error;
    refuse("source parent identity could not be established", error);
  }
}

export function syntheticDatabaseSource(sourceRevision, environment = process.env) {
  const pin = syntheticSourcePins.find((entry) => entry.sourceRevision === sourceRevision);
  if (!pin) refuse("database source revision is not pinned");
  return Object.freeze({
    repository: "Jones-Systems/Jones-Code",
    sourceRevision,
    worktreePath: NodePath.join(syntheticSourceParent(environment), pin.directory),
  });
}

export function assertSyntheticDatabaseSource(source, environment = process.env) {
  try {
    const expected = syntheticDatabaseSource(source?.sourceRevision, environment);
    if (
      !source ||
      Object.keys(source).sort().join(",") !== "repository,sourceRevision,worktreePath" ||
      source.repository !== expected.repository ||
      source.worktreePath !== expected.worktreePath ||
      NodeFS.realpathSync(expected.worktreePath) !== expected.worktreePath
    )
      refuse("database source must match an exact root-bound baseline");
    const pin = syntheticSourcePins.find((entry) => entry.sourceRevision === source.sourceRevision);
    const git = (...args) =>
      NodeChildProcess.execFileSync(
        "git",
        ["-c", "core.fsmonitor=false", "-C", source.worktreePath, ...args],
        {
          encoding: "utf8",
          maxBuffer: 256 * 1024,
          env: {
            PATH: process.env.PATH ?? "/usr/bin:/bin",
            GIT_CONFIG_NOSYSTEM: "1",
            GIT_CONFIG_GLOBAL: "/dev/null",
            GIT_OPTIONAL_LOCKS: "0",
          },
        },
      ).trim();
    if (
      git("rev-parse", "--show-toplevel") !== source.worktreePath ||
      git("rev-parse", "HEAD") !== pin.sourceRevision ||
      git("rev-parse", "HEAD^{tree}") !== pin.tree ||
      git("status", "--porcelain", "--untracked-files=no") !== "" ||
      NodeCrypto.createHash("sha256")
        .update(NodeFS.readFileSync(NodePath.join(source.worktreePath, "pnpm-lock.yaml")))
        .digest("hex") !== lockSha256
    )
      refuse("database source commit, tree, clean tracked inputs or frozen lock differ");
    return expected;
  } catch (error) {
    if (error.code === "invalid_source") throw error;
    refuse("database source identity could not be established", error);
  }
}

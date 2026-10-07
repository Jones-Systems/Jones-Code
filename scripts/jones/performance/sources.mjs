import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";

export const sourceParentEnvironment = "JONES_PERFORMANCE_SOURCE_PARENT";
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
export const qualificationSourcePins = Object.freeze([
  ...syntheticSourcePins,
  Object.freeze({
    directory: "history",
    sourceRevision: "c4c68bb0b33eafb72545e6e23b0b7258e49bd613",
    tree: "47f2cef2ba09c3212103a0152daf200557a05115",
    lockSha256: "68549e7f8c7fb39bc313b1314374d3303c0461bfe9c1af0740a719b999df6cfd",
  }),
  Object.freeze({
    directory: "lease",
    sourceRevision: "da5f4aee0035beec471b38598eaa2857d1e5155c",
    tree: "e4cc6a712c634cfff25979dcbadb4b753d3f7309",
    lockSha256: "68549e7f8c7fb39bc313b1314374d3303c0461bfe9c1af0740a719b999df6cfd",
  }),
  Object.freeze({
    "directory": "lease-current",
    "sourceRevision": "7c86493f6eff9ba9b30d3ff20ae33e10cdfb4607",
    "tree": "338eb4a11f2fea6042b829944b7408bd8952123a",
    "lockSha256": "755533d1deccfb663c092f62c26d65eac057ff1e43f09c24659e0cfc870d7c7f"
}),
  Object.freeze({
    "directory": "v2-aggregate-77",
    "sourceRevision": "ae25e5d04bec70c2c0af51af70329a9e9086d15e",
    "tree": "88fef296fa78526a649ffb87d8efebbecc7d5a17",
    "lockSha256": "37a8109c36aa9e065cc9cd4dc1144a024b4d81883db11db4822decfb25c1257a"
}),
  Object.freeze({
    "directory": "v2-aggregate-91",
    "sourceRevision": "09ead6ea565ffce428e9cda1bd5696c69ce9e616",
    "tree": "fa551ba6ef57f64461964dce62c255817b412dd9",
    "lockSha256": "37a8109c36aa9e065cc9cd4dc1144a024b4d81883db11db4822decfb25c1257a"
}),
]);
const lockSha256 = "34460ca4290c8132ae0f26ba0224476b22c39983e8bcd735e357f15989592fc4";

function refuse(message, cause) {
  throw Object.assign(new Error(message, { cause }), { code: "invalid_source" });
}

export function syntheticSourceParent(environment = process.env) {
  const parent = environment[sourceParentEnvironment];
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

function databaseSource(sourceRevision, pins, environment) {
  const pin = pins.find((entry) => entry.sourceRevision === sourceRevision);
  if (!pin) refuse("database source revision is not pinned");
  return Object.freeze({
    repository: "Jones-Systems/Jones-Code",
    sourceRevision,
    worktreePath: NodePath.join(syntheticSourceParent(environment), pin.directory),
  });
}

function assertDatabaseSource(source, pins, environment) {
  try {
    const expected = databaseSource(source?.sourceRevision, pins, environment);
    if (
      !source ||
      Object.keys(source).sort().join(",") !== "repository,sourceRevision,worktreePath" ||
      source.repository !== expected.repository ||
      source.worktreePath !== expected.worktreePath ||
      NodeFS.realpathSync(expected.worktreePath) !== expected.worktreePath
    )
      refuse("database source must match an exact root-bound baseline");
    const pin = pins.find((entry) => entry.sourceRevision === source.sourceRevision);
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
        .digest("hex") !== (pin.lockSha256 ?? lockSha256)
    )
      refuse("database source commit, tree, clean tracked inputs or frozen lock differ");
    return expected;
  } catch (error) {
    if (error.code === "invalid_source") throw error;
    refuse("database source identity could not be established", error);
  }
}

export function syntheticDatabaseSource(sourceRevision, environment = process.env) {
  return databaseSource(sourceRevision, syntheticSourcePins, environment);
}

export function assertSyntheticDatabaseSource(source, environment = process.env) {
  return assertDatabaseSource(source, syntheticSourcePins, environment);
}

export function qualificationDatabaseSource(sourceRevision, environment = process.env) {
  return databaseSource(sourceRevision, qualificationSourcePins, environment);
}

export function assertQualificationDatabaseSource(source, environment = process.env) {
  return assertDatabaseSource(source, qualificationSourcePins, environment);
}

function currentGit(worktreePath, ...args) {
  return NodeChildProcess.execFileSync(
    "git",
    ["-c", "core.fsmonitor=false", "-C", worktreePath, ...args],
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
}

export function assertCurrentDatabaseSource(source) {
  try {
    if (
      !source ||
      Object.keys(source).sort().join(",") !==
        "lockSha256,repository,sourceRevision,tree,worktreePath" ||
      source.repository !== "Jones-Systems/Jones-Code" ||
      !/^[a-f0-9]{40}$/.test(source.sourceRevision) ||
      !/^[a-f0-9]{40}$/.test(source.tree) ||
      !/^[a-f0-9]{64}$/.test(source.lockSha256) ||
      !NodePath.isAbsolute(source.worktreePath) ||
      NodePath.resolve(source.worktreePath) !== source.worktreePath ||
      NodeFS.realpathSync(source.worktreePath) !== source.worktreePath
    )
      refuse("current database source must have exact candidate binding fields");
    if (
      currentGit(source.worktreePath, "rev-parse", "--show-toplevel") !== source.worktreePath ||
      currentGit(source.worktreePath, "rev-parse", "HEAD") !== source.sourceRevision ||
      currentGit(source.worktreePath, "rev-parse", "HEAD^{tree}") !== source.tree ||
      currentGit(source.worktreePath, "status", "--porcelain", "--untracked-files=all") !== "" ||
      NodeCrypto.createHash("sha256")
        .update(NodeFS.readFileSync(NodePath.join(source.worktreePath, "pnpm-lock.yaml")))
        .digest("hex") !== source.lockSha256
    )
      refuse("current database source commit, tree, lock or clean worktree differ");
    return Object.freeze({ ...source });
  } catch (error) {
    if (error.code === "invalid_source") throw error;
    refuse("current database source identity could not be established", error);
  }
}

export function currentDatabaseSource(worktreePath) {
  return assertCurrentDatabaseSource({
    repository: "Jones-Systems/Jones-Code",
    worktreePath,
    sourceRevision: currentGit(worktreePath, "rev-parse", "HEAD"),
    tree: currentGit(worktreePath, "rev-parse", "HEAD^{tree}"),
    lockSha256: NodeCrypto.createHash("sha256")
      .update(NodeFS.readFileSync(NodePath.join(worktreePath, "pnpm-lock.yaml")))
      .digest("hex"),
  });
}

// @effect-diagnostics nodeBuiltinImport:off -- physical ownership requires lstat device/inode and symlink witnesses.
import * as NodeFSP from "node:fs/promises";

import { GitCommandError, T3_PROJECT_FILE_NAME } from "@t3tools/contracts";
import { parseT3ProjectFile } from "@t3tools/shared/t3ProjectFile";
import { resolveProjectFileBackedSetting } from "@t3tools/shared/projectSettings";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Result from "effect/Result";
import type * as GitVcsDriver from "../../../vcs/GitVcsDriver.ts";

type ObservationPorts = {
  readonly execute: GitVcsDriver.GitVcsDriver["Service"]["execute"];
  readonly readConfigValue: GitVcsDriver.GitVcsDriver["Service"]["readConfigValue"];
  readonly resolveBranchBase: (
    cwd: string,
    refName: string,
  ) => Effect.Effect<{ baseRef: string; branchName: string } | null, GitCommandError>;
  readonly parseCheckoutProgress: (line: string) => {
    percent: number;
    completed: number;
    total: number;
  } | null;
};

export const makeObservedWorktreeCreation = Effect.fn("makeObservedWorktreeCreation")(function* (
  ports: ObservationPorts,
) {
  const path = yield* Path.Path;
  const fileSystem = yield* FileSystem.FileSystem;
  const { execute, readConfigValue, resolveBranchBase, parseCheckoutProgress } = ports;
  const executeGit = (
    operation: string,
    cwd: string,
    args: ReadonlyArray<string>,
    options: Omit<GitVcsDriver.ExecuteGitInput, "operation" | "cwd" | "args"> = {},
  ) => execute({ operation, cwd, args, ...options });
  const runGitStdoutWithOptions = (
    operation: string,
    cwd: string,
    args: ReadonlyArray<string>,
    options: Omit<GitVcsDriver.ExecuteGitInput, "operation" | "cwd" | "args"> = {},
  ) => executeGit(operation, cwd, args, options).pipe(Effect.map((result) => result.stdout));
  const gitCommandContext = (input: {
    operation: string;
    cwd: string;
    args: ReadonlyArray<string>;
  }) => ({ operation: input.operation, cwd: input.cwd, command: "git" });
  const readLegacyMaterial = Effect.fn("readLegacyMaterial")(function* (input: {
    readonly worktreePath: string;
    readonly identity: {
      readonly commonDirectory: string;
      readonly targetRef: string;
      readonly baseCommitOid: string;
    };
    readonly before: GitVcsDriver.LegacyWorktreeBeforeObservation;
    readonly ownedClaim?: GitVcsDriver.LegacyWorktreeMaterialClaim;
  }) {
    const { worktreePath, identity, before, ownedClaim } = input;
    const observationError = () =>
      new GitCommandError({
        ...gitCommandContext({
          operation: "GitVcsDriver.legacyMaterial",
          cwd: worktreePath,
          args: [],
        }),
        detail: "Exact legacy worktree ownership is unavailable or changed.",
      });
    const fsProbe = <A>(probe: () => Promise<A>) =>
      Effect.tryPromise({ try: probe, catch: observationError });
    const readOwnerGit = (cwd: string, args: ReadonlyArray<string>) =>
      runGitStdoutWithOptions("GitVcsDriver.legacyMaterial", cwd, args, {
        env: { GIT_OPTIONAL_LOCKS: "0" },
      });
    const readRegistration = () =>
      readOwnerGit(worktreePath, ["worktree", "list", "--porcelain", "-z"]);
    const physical = yield* fsProbe(async () => {
      const stat = await NodeFSP.lstat(worktreePath);
      const parentStat = await NodeFSP.lstat(before.parentPath);
      const parentRealPath = await NodeFSP.realpath(before.parentPath);
      if (
        !stat.isDirectory() ||
        stat.isSymbolicLink() ||
        !parentStat.isDirectory() ||
        parentStat.isSymbolicLink() ||
        String(parentStat.dev) !== before.parentDevice ||
        String(parentStat.ino) !== before.parentInode ||
        parentRealPath !== before.parentRealPath
      )
        throw new Error("Replaced target or parent");
      const realPath = await NodeFSP.realpath(worktreePath);
      if (realPath !== path.resolve(parentRealPath, path.relative(before.parentPath, worktreePath)))
        throw new Error("Replaced path ancestry");
      const dotGitPath = path.join(worktreePath, ".git");
      const dotGit = await NodeFSP.lstat(dotGitPath);
      if (!dotGit.isFile() || dotGit.isSymbolicLink() || dotGit.size > 4096)
        throw new Error("Unsafe gitdir link");
      const link = (await NodeFSP.readFile(dotGitPath, "utf8")).trim();
      if (!link.startsWith("gitdir: ")) throw new Error("Missing gitdir link");
      const gitDirectory = await NodeFSP.realpath(path.resolve(worktreePath, link.slice(8)));
      const commonDirectory = await NodeFSP.realpath(identity.commonDirectory);
      const relativeGitDirectory = path.relative(
        path.join(commonDirectory, "worktrees"),
        gitDirectory,
      );
      if (
        relativeGitDirectory === "" ||
        relativeGitDirectory.startsWith("..") ||
        path.isAbsolute(relativeGitDirectory)
      )
        throw new Error("Foreign gitdir");
      const gitDirectoryStat = await NodeFSP.lstat(gitDirectory);
      const commonDirectoryStat = await NodeFSP.lstat(commonDirectory);
      if (
        !gitDirectoryStat.isDirectory() ||
        gitDirectoryStat.isSymbolicLink() ||
        !commonDirectoryStat.isDirectory() ||
        commonDirectoryStat.isSymbolicLink() ||
        String(commonDirectoryStat.dev) !== before.commonDirectoryDevice ||
        String(commonDirectoryStat.ino) !== before.commonDirectoryInode
      )
        throw new Error("Replaced Git identity");
      return {
        path: worktreePath,
        realPath,
        device: String(stat.dev),
        inode: String(stat.ino),
        parentRealPath,
        gitDirectory,
        commonDirectory,
        parentDevice: String(parentStat.dev),
        parentInode: String(parentStat.ino),
        dotGitDevice: String(dotGit.dev),
        dotGitInode: String(dotGit.ino),
        gitDirectoryDevice: String(gitDirectoryStat.dev),
        gitDirectoryInode: String(gitDirectoryStat.ino),
        commonDirectoryDevice: String(commonDirectoryStat.dev),
        commonDirectoryInode: String(commonDirectoryStat.ino),
      };
    });
    const registration = (yield* readRegistration())
      .split("\0\0")
      .map((record) => record.split("\0"));
    const matching = registration.filter((record) => record.includes(`worktree ${worktreePath}`));
    const headRef = (yield* readOwnerGit(worktreePath, ["symbolic-ref", "HEAD"])).trim();
    const headOid = (yield* readOwnerGit(worktreePath, [
      "rev-parse",
      "--verify",
      "HEAD^{commit}",
    ])).trim();
    const gitDirectory = (yield* readOwnerGit(worktreePath, [
      "rev-parse",
      "--absolute-git-dir",
    ])).trim();
    const commonDirectory = (yield* readOwnerGit(worktreePath, [
      "rev-parse",
      "--path-format=absolute",
      "--git-common-dir",
    ])).trim();
    if (
      matching.length !== 1 ||
      !matching[0]!.includes(`branch ${identity.targetRef}`) ||
      !matching[0]!.includes(`HEAD ${identity.baseCommitOid}`) ||
      matching[0]!.some((field) => field.startsWith("prunable") || field.startsWith("locked")) ||
      headRef !== identity.targetRef ||
      headOid !== identity.baseCommitOid ||
      gitDirectory !== physical.gitDirectory ||
      commonDirectory !== physical.commonDirectory
    )
      return yield* observationError();
    const claim = { ...physical, registeredPath: worktreePath, headRef, headOid };
    if (
      ownedClaim !== undefined &&
      (claim.path !== ownedClaim.path ||
        claim.realPath !== ownedClaim.realPath ||
        claim.device !== ownedClaim.device ||
        claim.inode !== ownedClaim.inode ||
        claim.parentRealPath !== ownedClaim.parentRealPath ||
        claim.gitDirectory !== ownedClaim.gitDirectory ||
        claim.commonDirectory !== ownedClaim.commonDirectory ||
        claim.registeredPath !== ownedClaim.registeredPath ||
        claim.headRef !== ownedClaim.headRef ||
        claim.headOid !== ownedClaim.headOid ||
        claim.parentDevice !== ownedClaim.parentDevice ||
        claim.parentInode !== ownedClaim.parentInode ||
        claim.dotGitDevice !== ownedClaim.dotGitDevice ||
        claim.dotGitInode !== ownedClaim.dotGitInode ||
        claim.gitDirectoryDevice !== ownedClaim.gitDirectoryDevice ||
        claim.gitDirectoryInode !== ownedClaim.gitDirectoryInode ||
        claim.commonDirectoryDevice !== ownedClaim.commonDirectoryDevice ||
        claim.commonDirectoryInode !== ownedClaim.commonDirectoryInode)
    )
      return yield* observationError();
    return claim;
  });

  const create: GitVcsDriver.GitVcsDriver["Service"]["createWorktree"] = Effect.fn(
    "ObservedWorktreeCreation.create",
  )(function* (input, options) {
    const journal = options?.legacyPreparation;
    const worktreePath = input.path;
    if (journal === undefined || worktreePath == null || !path.isAbsolute(worktreePath))
      return yield* new GitCommandError({
        operation: "GitVcsDriver.createWorktree.preparationIdentity",
        cwd: input.cwd,
        command: "git worktree add",
        detail: "Observed creation requires its exact absolute worktree path and owner contract.",
      });
    const targetBranch = input.newRefName ?? input.refName;
    const progress = options?.progress;
    const onCheckoutProgress = progress?.onCheckoutProgress;
    const [configuredCheckoutWorkers, base] = yield* Effect.all(
      [
        readConfigValue(input.cwd, "checkout.workers"),
        input.newRefName && input.baseRefName
          ? resolveBranchBase(input.cwd, input.baseRefName)
          : Effect.succeed(null),
      ],
      { concurrency: "unbounded" },
    );
    const checkoutWorkers = configuredCheckoutWorkers ?? "0";
    const identity = {
      commonDirectory: (yield* runGitStdoutWithOptions(
        "GitVcsDriver.createWorktree.commonDirectory",
        input.cwd,
        ["rev-parse", "--path-format=absolute", "--git-common-dir"],
        { env: { GIT_OPTIONAL_LOCKS: "0" } },
      )).trim(),
      baseCommitOid: (yield* runGitStdoutWithOptions(
        "GitVcsDriver.createWorktree.baseCommit",
        input.cwd,
        ["rev-parse", "--verify", `${input.refName}^{commit}`],
        { env: { GIT_OPTIONAL_LOCKS: "0" } },
      )).trim(),
      targetRef: input.newRefName ? `refs/heads/${input.newRefName}` : input.refName,
      worktreePath,
    };
    if (
      !path.isAbsolute(identity.commonDirectory) ||
      !/^[a-f0-9]{40,64}$/u.test(identity.baseCommitOid)
    )
      return yield* new GitCommandError({
        operation: "GitVcsDriver.createWorktree.preparationIdentity",
        cwd: input.cwd,
        command: "git",
        detail: "Cannot bind the legacy worktree preparation identity.",
      });
    const observationError = () =>
      new GitCommandError({
        ...gitCommandContext({
          operation: "GitVcsDriver.createWorktree.ownership",
          cwd: input.cwd,
          args: [],
        }),
        detail: "Exact legacy worktree ownership is unavailable or changed.",
      });
    const fsProbe = <A>(probe: () => Promise<A>) =>
      Effect.tryPromise({ try: probe, catch: observationError });
    const readOwnerGit = (cwd: string, args: ReadonlyArray<string>) =>
      runGitStdoutWithOptions("GitVcsDriver.createWorktree.ownership", cwd, args, {
        env: { GIT_OPTIONAL_LOCKS: "0" },
      });
    const readRegistration = () =>
      readOwnerGit(input.cwd, ["worktree", "list", "--porcelain", "-z"]);
    const beforeAdd = Effect.gen(function* () {
      if (input.newRefName === null || input.newRefName === undefined)
        return yield* observationError();
      const absent = yield* fsProbe(async () => {
        try {
          await NodeFSP.lstat(worktreePath);
          return false;
        } catch (error) {
          if (
            typeof error === "object" &&
            error !== null &&
            "code" in error &&
            error.code === "ENOENT"
          )
            return true;
          throw error;
        }
      });
      if (!absent) return yield* observationError();
      const parent = yield* fsProbe(async () => {
        let parentPath = path.dirname(worktreePath);
        for (let depth = 0; depth < 64; depth++) {
          try {
            const stat = await NodeFSP.lstat(parentPath);
            if (!stat.isDirectory() || stat.isSymbolicLink()) throw new Error("Unsafe parent");
            return {
              parentPath,
              parentRealPath: await NodeFSP.realpath(parentPath),
              parentDevice: String(stat.dev),
              parentInode: String(stat.ino),
            };
          } catch (error) {
            if (
              !(
                typeof error === "object" &&
                error !== null &&
                "code" in error &&
                error.code === "ENOENT"
              )
            )
              throw error;
            const next = path.dirname(parentPath);
            if (next === parentPath) throw error;
            parentPath = next;
          }
        }
        throw new Error("No bounded existing parent");
      });
      const refs = (yield* readOwnerGit(input.cwd, [
        "for-each-ref",
        "--format=%(refname)",
        identity.targetRef,
      ])).trim();
      const registration = yield* readRegistration();
      const config = yield* executeGit(
        "GitVcsDriver.createWorktree.ownershipConfig",
        input.cwd,
        [
          "config",
          "--null",
          "--get-regexp",
          `^${`branch.${input.newRefName}.`.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&")}(t3-merge-base|gh-merge-base|t3-merge-base-ref)$`,
        ],
        { allowNonZeroExit: true, env: { GIT_OPTIONAL_LOCKS: "0" } },
      );
      if (
        refs !== "" ||
        registration.split("\0").includes(`worktree ${worktreePath}`) ||
        config.exitCode !== 1
      )
        return yield* observationError();
      const common = yield* fsProbe(async () => {
        const stat = await NodeFSP.lstat(identity.commonDirectory);
        if (!stat.isDirectory() || stat.isSymbolicLink())
          throw new Error("Unsafe common directory");
        return { commonDirectoryDevice: String(stat.dev), commonDirectoryInode: String(stat.ino) };
      });
      return {
        ...parent,
        ...common,
        targetRefAbsent: true as const,
        registrationAbsent: true as const,
      };
    });
    let before: GitVcsDriver.LegacyWorktreeBeforeObservation | undefined;
    let ownedClaim: GitVcsDriver.LegacyWorktreeMaterialClaim | undefined;
    const readMaterial = Effect.gen(function* () {
      if (before === undefined) return yield* observationError();
      return yield* readLegacyMaterial({
        worktreePath,
        identity,
        before,
        ...(ownedClaim === undefined ? {} : { ownedClaim }),
      });
    });
    const journalStep = Effect.fn("journalStep")(function* <A>(
      kind: GitVcsDriver.LegacyWorktreePreparationStep["kind"],
      cwd: string,
      stepArgs: ReadonlyArray<string>,
      effect: Effect.Effect<A, GitCommandError>,
    ): Effect.fn.Return<A, GitCommandError> {
      if (kind === "worktree.add") before = yield* beforeAdd;
      else yield* readMaterial;
      const step: GitVcsDriver.LegacyWorktreePreparationStep = {
        ...identity,
        kind,
        cwd,
        args: [...stepArgs],
        ...(before === undefined ? {} : { before }),
      };
      const mapJournalError = (cause: unknown) =>
        new GitCommandError({
          ...gitCommandContext({
            operation: "GitVcsDriver.createWorktree.preparationJournal",
            cwd,
            args: stepArgs,
          }),
          detail: "Legacy preparation journal readback failed.",
          cause,
        });
      // Fallible journal callbacks stay outside native best-effort catches.
      yield* journal.beforeEffect(step).pipe(Effect.mapError(mapJournalError));
      // These characters can never form a Git branch; refusal is an owner decision,
      // not an inference from a subprocess error or missing material afterward.
      if (kind === "worktree.add" && /[\x00-\x20\x7f~^:?*[\\]/u.test(targetBranch)) {
        if (journal.neverInvoked !== undefined)
          yield* journal
            .neverInvoked(step, "input_validation_failed")
            .pipe(Effect.mapError(mapJournalError));
        return yield* new GitCommandError({
          ...gitCommandContext({ operation: "GitVcsDriver.createWorktree", cwd, args: stepArgs }),
          detail: "Legacy worktree input validation refused before invocation.",
        });
      }
      // Entry is conservative: every error or interruption from this point remains
      // possibly executed, even if a later observation finds no material.
      const result = yield* effect.pipe(Effect.result);
      const material = yield* readMaterial.pipe(Effect.result);
      const claim = Result.isSuccess(material) ? material.success : undefined;
      yield* journal
        .afterEffect(
          step,
          Result.isSuccess(result) && claim !== undefined ? "settled_success" : "failed_or_unknown",
          claim,
        )
        .pipe(Effect.mapError(mapJournalError));
      if (Result.isFailure(material)) return yield* material.failure;
      ownedClaim = claim;
      return yield* Result.isSuccess(result)
        ? Effect.succeed(result.success)
        : Effect.fail(result.failure);
    });
    const addArgs = [
      "-c",
      `checkout.workers=${checkoutWorkers}`,
      "worktree",
      "add",
      "-b",
      targetBranch,
      "--",
      worktreePath,
      identity.baseCommitOid,
    ];
    yield* journalStep(
      "worktree.add",
      input.cwd,
      addArgs,
      executeGit("GitVcsDriver.createWorktree", input.cwd, addArgs, {
        writesSharedConfig: true,
        timeoutMs: null,
        appendTruncationMarker: true,
        keepLineCallbacksAfterTruncation: true,
        env: { LANGUAGE: "C", ...(onCheckoutProgress ? { GIT_PROGRESS_DELAY: "0" } : {}) },
        ...(onCheckoutProgress
          ? {
              progress: {
                onStderrLine: (line: string) => {
                  const parsed = parseCheckoutProgress(line);
                  return parsed ? onCheckoutProgress(parsed) : Effect.void;
                },
              },
            }
          : {}),
      }),
    );
    // The first-material observation above must settle before progress or later mutations.
    if (progress?.onWorktreeClaimed)
      yield* Effect.uninterruptible(progress.onWorktreeClaimed(worktreePath));
    const hasSubmodules = yield* fileSystem
      .exists(path.join(worktreePath, ".gitmodules"))
      .pipe(Effect.mapError(observationError));
    const submoduleMode = !hasSubmodules
      ? { value: "none" as const, source: "environment" as const }
      : resolveProjectFileBackedSetting(
          "worktreeSubmodules",
          options?.submodules ?? null,
          options?.submodules != null
            ? null
            : yield* fileSystem.readFileString(path.join(worktreePath, T3_PROJECT_FILE_NAME)).pipe(
                Effect.map(parseT3ProjectFile),
                Effect.orElseSucceed(() => null),
              ),
        );
    if (hasSubmodules && submoduleMode.value === "none" && progress?.onSubmodulesDisabled)
      yield* progress.onSubmodulesDisabled({
        source: submoduleMode.source === "t3.json" ? "t3.json" : "settings",
      });
    if (submoduleMode.value !== "none") {
      if (progress?.onSubmodulesStarted) yield* progress.onSubmodulesStarted();
      const args = [
        "submodule",
        "update",
        "--init",
        ...(submoduleMode.value === "recursive" ? ["--recursive"] : []),
      ];
      const onLine = progress?.onSubmoduleLine;
      // update --init owns registration and checkout under the same exact observed command.
      yield* journalStep(
        "worktree.submodules",
        worktreePath,
        args,
        executeGit("GitVcsDriver.createWorktree.updateSubmodules", worktreePath, args, {
          writesSharedConfig: true,
          timeoutMs: null,
          env: { LANGUAGE: "C" },
          ...(onLine
            ? {
                progress: { onStdoutLine: onLine, onStderrLine: onLine },
                appendTruncationMarker: true,
                keepLineCallbacksAfterTruncation: true,
              }
            : {}),
        }),
      );
      if (progress?.onSubmodulesFinished)
        yield* progress.onSubmodulesFinished({ ok: true, detail: null });
    }
    if (input.newRefName && base) {
      const configurations = [
        { key: `branch.${input.newRefName}.t3-merge-base`, value: base.branchName },
        ...(base.baseRef.startsWith("refs/remotes/")
          ? [{ key: `branch.${input.newRefName}.t3-merge-base-ref`, value: base.baseRef }]
          : []),
      ];
      for (const { key, value } of configurations) {
        const args = ["config", key, value];
        yield* journalStep(
          "worktree.base-config",
          input.cwd,
          args,
          executeGit("GitVcsDriver.createWorktree.configureBaseRef", input.cwd, args, {
            writesSharedConfig: true,
          }),
        );
      }
    }
    return { worktree: { path: worktreePath, refName: targetBranch } };
  });
  return create;
});

// @effect-diagnostics nodeBuiltinImport:off -- physical custody requires lstat device/inode and gitdir-link witnesses.
import * as NodeFSP from "node:fs/promises";

import { GitCommandError } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Path from "effect/Path";
import type * as Git from "../../../vcs/GitVcsDriver.ts";
import type {
  LegacyBranchRenameHooks,
  LegacyBranchRenameStep,
  LegacyWorktreeMaterialClaim,
} from "./WorktreeObservation.ts";

const oidPattern = /^(?:[a-f0-9]{40}|[a-f0-9]{64})$/u;
const physicalKeys = [
  "path",
  "realPath",
  "device",
  "inode",
  "parentRealPath",
  "parentDevice",
  "parentInode",
  "gitDirectory",
  "commonDirectory",
  "dotGitDevice",
  "dotGitInode",
  "gitDirectoryDevice",
  "gitDirectoryInode",
  "commonDirectoryDevice",
  "commonDirectoryInode",
] as const;

export const makeObservedBranchRename = Effect.fn("makeObservedBranchRename")(function* (ports: {
  readonly execute: Git.GitVcsDriver["Service"]["execute"];
  readonly rename: (
    cwd: string,
    args: ReadonlyArray<string>,
  ) => Effect.Effect<void, GitCommandError>;
}) {
  const path = yield* Path.Path;
  return Effect.fn("ObservedBranchRename.rename")(function* (input: {
    readonly cwd: string;
    readonly oldBranch: string;
    readonly targetBranch: string;
    readonly exactName: boolean;
    readonly hooks: LegacyBranchRenameHooks;
  }) {
    const { hooks, cwd } = input;
    const oldRef = `refs/heads/${input.oldBranch}`;
    const targetRef = `refs/heads/${input.targetBranch}`;
    const args = ["branch", "-m", "--", input.oldBranch, input.targetBranch];
    const denied = (cause?: unknown) =>
      new GitCommandError({
        operation: "GitVcsDriver.renameBranch.ownership",
        cwd,
        command: "git",
        detail: "Exact legacy branch rename ownership is unavailable or changed.",
        ...(cause === undefined ? {} : { cause }),
      });
    const journalError = (cause: unknown) =>
      new GitCommandError({
        operation: "GitVcsDriver.renameBranch.preparationJournal",
        cwd,
        command: "git branch -m",
        detail: "Legacy rename journal readback failed; retained intent must be reconciled.",
        cause,
      });
    const read = Effect.fn("ObservedBranchRename.read")(function* (
      readArgs: ReadonlyArray<string>,
    ) {
      const result = yield* ports.execute({
        operation: "GitVcsDriver.renameBranch.ownership",
        cwd,
        args: readArgs,
        env: { GIT_OPTIONAL_LOCKS: "0", GIT_NO_LAZY_FETCH: "1" },
      });
      if (
        result.exitCode !== 0 ||
        result.stdoutTruncated ||
        result.stderrTruncated ||
        result.stdout.includes("\ufffd")
      )
        return yield* denied();
      return result.stdout;
    });
    const line = Effect.fn("ObservedBranchRename.line")(function* (
      readArgs: ReadonlyArray<string>,
    ) {
      const stdout = yield* read(readArgs);
      if (
        !stdout.endsWith("\n") ||
        stdout.slice(0, -1).includes("\n") ||
        stdout.includes("\0") ||
        stdout.includes("\r")
      )
        return yield* denied();
      return stdout.slice(0, -1);
    });
    const physical = () =>
      Effect.tryPromise({
        try: async () => {
          const claim = hooks.claim;
          const before = hooks.before;
          if (
            cwd !== claim.path ||
            !path.isAbsolute(cwd) ||
            path.resolve(cwd) !== cwd ||
            !path.isAbsolute(before.parentPath)
          )
            throw new Error("Unbound path");
          const target = await NodeFSP.lstat(cwd);
          const parent = await NodeFSP.lstat(before.parentPath);
          const parentRealPath = await NodeFSP.realpath(before.parentPath);
          const realPath = await NodeFSP.realpath(cwd);
          const relativeWorktree = path.relative(before.parentPath, cwd);
          if (
            !target.isDirectory() ||
            target.isSymbolicLink() ||
            !parent.isDirectory() ||
            parent.isSymbolicLink() ||
            String(parent.dev) !== before.parentDevice ||
            String(parent.ino) !== before.parentInode ||
            parentRealPath !== before.parentRealPath ||
            relativeWorktree === "" ||
            relativeWorktree === ".." ||
            relativeWorktree.startsWith(`..${path.sep}`) ||
            path.isAbsolute(relativeWorktree) ||
            realPath !== path.resolve(parentRealPath, relativeWorktree)
          )
            throw new Error("Replaced worktree ancestry");
          const dotGit = await NodeFSP.lstat(path.join(cwd, ".git"));
          if (!dotGit.isFile() || dotGit.isSymbolicLink() || dotGit.size > 4096)
            throw new Error("Unsafe gitdir link");
          const link = await NodeFSP.readFile(path.join(cwd, ".git"), "utf8");
          if (!/^gitdir: [^\0\r\n]+\n$/u.test(link)) throw new Error("Invalid gitdir link");
          const gitDirectory = await NodeFSP.realpath(path.resolve(cwd, link.slice(8, -1)));
          const commonDirectory = await NodeFSP.realpath(claim.commonDirectory);
          const relative = path.relative(path.join(commonDirectory, "worktrees"), gitDirectory);
          if (
            relative === "" ||
            relative === ".." ||
            relative.startsWith(`..${path.sep}`) ||
            path.isAbsolute(relative)
          )
            throw new Error("Foreign or primary gitdir");
          const git = await NodeFSP.lstat(gitDirectory);
          const common = await NodeFSP.lstat(commonDirectory);
          if (
            !git.isDirectory() ||
            git.isSymbolicLink() ||
            !common.isDirectory() ||
            common.isSymbolicLink() ||
            String(common.dev) !== before.commonDirectoryDevice ||
            String(common.ino) !== before.commonDirectoryInode
          )
            throw new Error("Replaced Git identity");
          const actual = {
            path: cwd,
            realPath,
            device: String(target.dev),
            inode: String(target.ino),
            parentRealPath,
            parentDevice: String(parent.dev),
            parentInode: String(parent.ino),
            gitDirectory,
            commonDirectory,
            dotGitDevice: String(dotGit.dev),
            dotGitInode: String(dotGit.ino),
            gitDirectoryDevice: String(git.dev),
            gitDirectoryInode: String(git.ino),
            commonDirectoryDevice: String(common.dev),
            commonDirectoryInode: String(common.ino),
          };
          if (physicalKeys.some((key) => actual[key] !== claim[key]))
            throw new Error("Replaced material");
          return actual;
        },
        catch: denied,
      });
    const material = Effect.fn("ObservedBranchRename.material")(function* (headRef: string) {
      const actual = yield* physical();
      const registered = yield* read(["worktree", "list", "--porcelain", "-z"]);
      if (!registered.endsWith("\0\0")) return yield* denied();
      const records = registered
        .slice(0, -2)
        .split("\0\0")
        .map((record) => record.split("\0"));
      const matches = records.filter((record) => record.includes(`worktree ${cwd}`));
      const headOid = yield* line(["rev-parse", "--verify", "--end-of-options", "HEAD^{commit}"]);
      const observedRef = yield* line(["symbolic-ref", "HEAD"]);
      const gitDirectory = yield* line(["rev-parse", "--absolute-git-dir"]);
      const commonDirectory = yield* line([
        "rev-parse",
        "--path-format=absolute",
        "--git-common-dir",
      ]);
      if (
        matches.length !== 1 ||
        !matches[0]!.includes(`branch ${headRef}`) ||
        records.filter((record) => record.includes(`branch ${headRef}`)).length !== 1 ||
        !matches[0]!.includes(`HEAD ${hooks.claim.headOid}`) ||
        matches[0]!.some((field) => field.startsWith("prunable") || field.startsWith("locked")) ||
        observedRef !== headRef ||
        headOid !== hooks.claim.headOid ||
        gitDirectory !== actual.gitDirectory ||
        commonDirectory !== actual.commonDirectory ||
        hooks.claim.registeredPath !== cwd
      )
        return yield* denied();
      yield* physical();
      return {
        ...actual,
        registeredPath: cwd,
        headRef,
        headOid,
      } satisfies LegacyWorktreeMaterialClaim;
    });
    const refAbsent = Effect.fn("ObservedBranchRename.refAbsent")(function* (ref: string) {
      // Match the exact ref, not descendants returned by for-each-ref's prefix lookup.
      const refs = yield* read(["for-each-ref", "--format=%(refname)", ref]);
      if (refs !== "" && (!refs.endsWith("\n") || refs.split("\n").includes(ref)))
        return yield* denied();
    });
    if (
      !oidPattern.test(hooks.claim.headOid) ||
      /^0+$/u.test(hooks.claim.headOid) ||
      hooks.claim.headRef !== oldRef ||
      oldRef === targetRef
    )
      return yield* denied();
    yield* read(["check-ref-format", oldRef]);
    yield* read(["check-ref-format", targetRef]);
    const claim = yield* material(oldRef);
    yield* refAbsent(targetRef);
    const step: LegacyBranchRenameStep = {
      claim,
      oldRef,
      oldOid: claim.headOid,
      targetRef,
      exactName: input.exactName,
      args,
    };
    // Once intent is accepted, every exit (including interruption) attempts one conservative
    // completion. A lost completion remains an unresolved intent; this helper never retries Git.
    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        yield* restore(hooks.beforeEffect(step)).pipe(Effect.mapError(journalError));
        const result = yield* restore(
          Effect.gen(function* () {
            yield* material(oldRef);
            yield* refAbsent(targetRef);
            yield* ports.rename(cwd, args);
          }),
        ).pipe(Effect.exit);
        const observed = yield* material(targetRef).pipe(
          Effect.tap(() => refAbsent(oldRef)),
          Effect.exit,
        );
        const after = Exit.isSuccess(observed) ? observed.value : undefined;
        yield* hooks
          .afterEffect(
            step,
            Exit.isSuccess(result) && after !== undefined ? "settled_success" : "failed_or_unknown",
            after,
          )
          .pipe(Effect.mapError(journalError));
        if (Exit.isFailure(result)) return yield* Effect.failCause(result.cause);
        if (Exit.isFailure(observed)) return yield* Effect.failCause(observed.cause);
      }),
    );
  });
});

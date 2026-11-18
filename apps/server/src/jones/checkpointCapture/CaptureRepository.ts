import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import {
  VcsPrimaryCheckoutCheckpointError,
  VcsProcessExitError,
  VcsRepositoryDetectionError,
  type VcsError,
} from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import type * as VcsDriver from "../../vcs/VcsDriver.ts";
import { CHECKPOINT_CAPTURE_OPERATION } from "../../vcs/VcsProcess.ts";

interface ResolvedCaptureRepository {
  readonly gitCommonDir: string;
  readonly indexPath: string | undefined;
  readonly topLevel: string | undefined;
  readonly prefix: string;
  readonly headExists: boolean;
  readonly head: string | undefined;
  readonly resolveSubmodule: (cwd: string) => Effect.Effect<ResolvedCaptureRepository, VcsError>;
}

export const make = Effect.fn("CheckpointCapture.make")(function* (
  execute: VcsDriver.VcsDriver["Service"]["execute"],
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const hostPlatform = yield* HostProcess.Platform;
  // Preserve a path's whitespace and embedded newlines; Git adds one record terminator.
  const resolveCapturePath = Effect.fn("GitVcsDriver.resolveCapturePath")(function* (
    cwd: string,
    args: ReadonlyArray<string>,
    operation: string,
  ) {
    const result = yield* execute({ operation, cwd, args });
    if (result.exitCode !== 0) {
      return yield* VcsProcessExitError.fromProcessExit(
        { operation, command: "git", cwd, argumentCount: args.length + 2 },
        result,
        "command-failed",
      );
    }
    if (
      result.stdoutTruncated ||
      result.stderrTruncated ||
      result.stdoutInvalidUtf8 === true ||
      result.stderrInvalidUtf8 === true ||
      !result.stdout.endsWith("\n")
    ) {
      return yield* new VcsRepositoryDetectionError({
        operation,
        cwd,
        detail: "Git returned incomplete or invalid capture-path metadata.",
      });
    }
    const value = result.stdout.slice(
      0,
      hostPlatform === "win32" && result.stdout.endsWith("\r\n") ? -2 : -1,
    );
    if (value.length === 0 || value.includes("\0")) {
      return yield* new VcsRepositoryDetectionError({
        operation,
        cwd,
        detail: "Git returned an empty or invalid capture path.",
      });
    }
    if (path.isAbsolute(value)) return path.normalize(value);
    const physicalCwd = yield* fileSystem.realPath(cwd).pipe(
      Effect.mapError(
        (cause) =>
          new VcsRepositoryDetectionError({
            operation,
            cwd,
            detail: "Unable to resolve the physical capture working directory.",
            cause,
          }),
      ),
    );
    return path.resolve(physicalCwd, value);
  });

  const resolveGitCommonDir = (cwd: string) =>
    resolveCapturePath(
      cwd,
      ["rev-parse", "--git-common-dir"],
      "GitVcsDriver.checkpoints.resolveGitCommonDir",
    );
  const resolveGitDir = (cwd: string) =>
    resolveCapturePath(cwd, ["rev-parse", "--git-dir"], "GitVcsDriver.checkpoints.resolveGitDir");
  const captureObjectId = /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/i;
  const isCaptureObjectId = (value: string | undefined) =>
    value !== undefined && captureObjectId.test(value) && !/^0+$/.test(value);

  // Unlike general nested-repository detection, an unknown HEAD probe cannot authorize an unborn capture.
  const captureHasHead = Effect.fn("GitVcsDriver.captureHasHead")(function* (cwd: string) {
    const operation = "GitVcsDriver.checkpoints.hasHeadCommit";
    const args = ["rev-parse", "--verify", "--quiet", "HEAD"];
    const result = yield* execute({ operation, cwd, args, allowNonZeroExit: true });
    if (result.exitCode !== 0 && result.exitCode !== 1) {
      return yield* VcsProcessExitError.fromProcessExit(
        { operation, command: "git", cwd, argumentCount: args.length + 2 },
        result,
        "command-failed",
      );
    }
    const stdout =
      hostPlatform === "win32" && result.stdout.endsWith("\r\n")
        ? result.stdout.slice(0, -2)
        : result.stdout.endsWith("\n")
          ? result.stdout.slice(0, -1)
          : result.stdout;
    if (
      result.stdoutTruncated ||
      result.stderrTruncated ||
      result.stdoutInvalidUtf8 === true ||
      result.stderrInvalidUtf8 === true ||
      (result.exitCode === 0
        ? !result.stdout.endsWith("\n") || !isCaptureObjectId(stdout)
        : result.stdout !== "")
    ) {
      return yield* new VcsRepositoryDetectionError({
        operation,
        cwd,
        detail: "Git did not establish a valid HEAD or an unborn capture.",
      });
    }
    return result.exitCode === 0;
  });

  const isAbsoluteCapturePath = (value: string | undefined): value is string =>
    value !== undefined && value.length > 0 && path.isAbsolute(value) && !value.includes("\0");

  const refusePrimary = (cwd: string, gitCommonDir: string, gitDir: string) =>
    path.normalize(gitDir) === path.normalize(gitCommonDir)
      ? Effect.fail(
          new VcsPrimaryCheckoutCheckpointError({
            operation: CHECKPOINT_CAPTURE_OPERATION,
            cwd,
          }),
        )
      : Effect.void;

  // Only a successfully resolved parent can mint this continuation. A public capture
  // still refuses primary repositories, including a directly requested submodule.
  const authorizeSubmodule = Effect.fn("CheckpointCapture.authorizeSubmodule")(function* (
    parent: { readonly cwd: string; readonly gitDir: string },
    cwd: string,
    gitDir: string,
  ) {
    const operation = CHECKPOINT_CAPTURE_OPERATION;
    const physical = (value: string) => fileSystem.realPath(value);
    const valid = yield* Effect.gen(function* () {
      const parentTop = yield* resolveCapturePath(
        parent.cwd,
        ["rev-parse", "--show-toplevel"],
        operation,
      );
      const childTop = yield* resolveCapturePath(cwd, ["rev-parse", "--show-toplevel"], operation);
      const superproject = yield* resolveCapturePath(
        cwd,
        ["rev-parse", "--show-superproject-working-tree"],
        operation,
      );
      const parentRoot = yield* physical(parentTop);
      const childRoot = yield* physical(cwd);
      const currentParentGitDir = yield* resolveGitDir(parent.cwd);
      const modules = yield* physical(path.join(parent.gitDir, "modules"));
      const moduleGitDir = yield* physical(gitDir);
      const relative = path.relative(parentRoot, childRoot);
      const moduleRelative = path.relative(modules, moduleGitDir);
      const inside = (value: string) =>
        value !== "" &&
        !path.isAbsolute(value) &&
        value !== ".." &&
        !value.startsWith(`..${path.sep}`);
      if (
        !inside(relative) ||
        !inside(moduleRelative) ||
        (yield* physical(currentParentGitDir)) !== (yield* physical(parent.gitDir)) ||
        (yield* physical(childTop)) !== childRoot ||
        (yield* physical(superproject)) !== parentRoot
      )
        return false;
      // The physical backlink alone is insufficient: require the parent's actual
      // stage-zero gitlink, preserving literal paths and NUL record framing.
      const gitPath = relative.split(path.sep).join("/");
      const registration = yield* execute({
        operation,
        cwd: parentTop,
        args: ["ls-files", "--stage", "-z", "--", `:(top,literal)${gitPath}`],
      });
      const match = /^160000 ([0-9a-f]{40}|[0-9a-f]{64}) 0\t/.exec(registration.stdout);
      return (
        !registration.stdoutTruncated &&
        !registration.stderrTruncated &&
        registration.stdoutInvalidUtf8 !== true &&
        registration.stderrInvalidUtf8 !== true &&
        match !== null &&
        isCaptureObjectId(match[1]) &&
        registration.stdout.slice(match[0].length) === `${gitPath}\0`
      );
    }).pipe(
      Effect.mapError(
        (cause) =>
          new VcsRepositoryDetectionError({
            operation,
            cwd,
            detail: "Unable to establish the physical submodule registration.",
            cause,
          }),
      ),
    );
    if (!valid)
      return yield* new VcsRepositoryDetectionError({
        operation,
        cwd,
        detail: "The capture target is not a physically registered child submodule.",
      });
  });

  // Batch the receiving custody witness with upstream's subdirectory and HEAD optimizations.
  const resolveRepository = Effect.fn("GitVcsDriver.resolveCaptureRepository")(function* (
    cwd: string,
    parent?: { readonly cwd: string; readonly gitDir: string },
  ): Effect.fn.Return<ResolvedCaptureRepository, VcsError> {
    const result = yield* execute({
      operation: CHECKPOINT_CAPTURE_OPERATION,
      cwd,
      args: [
        "rev-parse",
        "--path-format=absolute",
        "--git-common-dir",
        "--git-dir",
        "--git-path",
        "index",
        "--show-toplevel",
        "--show-prefix",
        "--verify",
        "--quiet",
        "HEAD",
      ],
      allowNonZeroExit: true,
    });
    const lines = (
      hostPlatform === "win32" ? result.stdout.replace(/\r\n/g, "\n") : result.stdout
    ).split("\n");
    const intact =
      !result.stdoutTruncated &&
      !result.stderrTruncated &&
      result.stdoutInvalidUtf8 !== true &&
      result.stderrInvalidUtf8 !== true;
    const establishedHead =
      intact && result.exitCode === 0 && lines.at(-1) === "" && isCaptureObjectId(lines.at(-2));
    const [gitCommonDir, gitDir, indexPath, topLevel, prefix] = lines;
    if (
      intact &&
      lines.at(-1) === "" &&
      ((establishedHead && lines.length === 7) || (result.exitCode === 1 && lines.length === 6)) &&
      isAbsoluteCapturePath(gitCommonDir) &&
      isAbsoluteCapturePath(gitDir) &&
      isAbsoluteCapturePath(indexPath) &&
      isAbsoluteCapturePath(topLevel) &&
      prefix !== undefined &&
      !prefix.includes("\0") &&
      (prefix === "" || (!path.isAbsolute(prefix) && prefix.endsWith("/")))
    ) {
      const common = path.normalize(gitCommonDir);
      yield* parent === undefined
        ? refusePrimary(cwd, common, gitDir)
        : authorizeSubmodule(parent, cwd, gitDir);
      return {
        gitCommonDir: common,
        indexPath: path.normalize(indexPath),
        topLevel: path.normalize(topLevel),
        prefix,
        headExists: establishedHead,
        head: establishedHead ? lines.at(-2) : undefined,
        resolveSubmodule: (childCwd) => resolveRepository(childCwd, { cwd, gitDir }),
      };
    }
    // Old Git echoes --path-format; embedded newlines require independent path reads.
    // Establish actual primary/linked custody and HEAD before any optional optimization.
    const fallbackCommonDir = yield* resolveGitCommonDir(cwd);
    const fallbackGitDir = yield* resolveGitDir(cwd);
    const headExists = establishedHead || (yield* captureHasHead(cwd));
    yield* parent === undefined
      ? refusePrimary(cwd, fallbackCommonDir, fallbackGitDir)
      : authorizeSubmodule(parent, cwd, fallbackGitDir);
    const prefixResult = yield* execute({
      operation: CHECKPOINT_CAPTURE_OPERATION,
      cwd,
      args: ["rev-parse", "--show-prefix"],
    });
    if (
      prefixResult.stdoutTruncated ||
      prefixResult.stderrTruncated ||
      prefixResult.stdoutInvalidUtf8 === true ||
      prefixResult.stderrInvalidUtf8 === true ||
      !prefixResult.stdout.endsWith("\n") ||
      prefixResult.stdout.includes("\0")
    ) {
      return yield* new VcsRepositoryDetectionError({
        operation: CHECKPOINT_CAPTURE_OPERATION,
        cwd,
        detail: "Git returned incomplete or invalid capture-prefix metadata.",
      });
    }
    return {
      gitCommonDir: fallbackCommonDir,
      indexPath: undefined,
      topLevel: undefined,
      prefix: prefixResult.stdout.slice(
        0,
        hostPlatform === "win32" && prefixResult.stdout.endsWith("\r\n") ? -2 : -1,
      ),
      headExists,
      head: undefined,
      resolveSubmodule: (childCwd) => resolveRepository(childCwd, { cwd, gitDir: fallbackGitDir }),
    };
  });

  return {
    resolveRepository: (cwd: string) => resolveRepository(cwd),
    resolvePath: resolveCapturePath,
  };
});

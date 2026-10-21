import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import { expandHomePath } from "@t3tools/provider-core/server/pathExpansion";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { canonical, CheckoutError, type Target } from "./CheckoutProtocol.ts";

export interface TargetInput {
  readonly projectId: string;
  readonly threadId: string;
  readonly projectRoot: string;
  readonly worktreePath: string | null;
  readonly threadBranch: string | null;
  readonly projectRevision: string;
  readonly threadRevision: string;
}
export interface Directory {
  readonly realPath: string;
  readonly identity: Target["directoryIdentity"];
}
export interface GitCheckout {
  readonly root: string;
  readonly commonDirectory: string;
  readonly gitDirectory: string;
  readonly commonIdentity: Target["directoryIdentity"];
  readonly directoryIdentity: Target["directoryIdentity"];
  readonly branch: string | null;
  readonly registeredWorktrees: readonly string[];
}

// Observation only: no fallback to the server cwd and no workspace creation.
export class CheckoutTargetObservation extends Context.Service<
  CheckoutTargetObservation,
  {
    readonly directory: (path: string) => Effect.Effect<Directory, CheckoutError>;
    readonly git: (cwd: string) => Effect.Effect<GitCheckout | null, CheckoutError>;
  }
>()("t3/jones/ordinaryCheckout/CheckoutTarget/CheckoutTargetObservation") {}

export class CheckoutTarget extends Context.Service<
  CheckoutTarget,
  {
    readonly capture: (input: TargetInput) => Effect.Effect<Target, CheckoutError>;
    readonly revalidate: (target: Target) => Effect.Effect<void, CheckoutError>;
  }
>()("t3/jones/ordinaryCheckout/CheckoutTarget") {}

export const overlaps = (left: string, right: string, separator: string): boolean => {
  const prefix = (value: string) => (value.endsWith(separator) ? value : `${value}${separator}`);
  return left === right || left.startsWith(prefix(right)) || right.startsWith(prefix(left));
};

const make = Effect.gen(function* () {
  const observation = yield* CheckoutTargetObservation;
  const path = yield* Path.Path;
  const home = yield* HostProcess.HomeDirectory;
  const normalize = (value: string) => {
    const expanded = expandHomePath(value, home);
    return value.trim() !== "" && path.isAbsolute(expanded) ? path.normalize(expanded) : null;
  };
  const capture = Effect.fn("CheckoutTarget.capture")(function* (input: TargetInput) {
    const projectPath = normalize(input.projectRoot);
    const cwdPath = normalize(input.worktreePath ?? input.projectRoot);
    if (
      projectPath === null ||
      cwdPath === null ||
      !input.projectId ||
      !input.threadId ||
      !input.projectRevision ||
      !input.threadRevision
    )
      return yield* new CheckoutError({ reason: "identity" });
    const project = yield* observation.directory(projectPath);
    const cwd = yield* observation.directory(cwdPath);
    const projectGit = yield* observation.git(project.realPath);
    const git = yield* observation.git(cwd.realPath);
    const root = git === null ? cwd : yield* observation.directory(git.root);
    if (
      git !== null &&
      cwd.realPath !== root.realPath &&
      !cwd.realPath.startsWith(
        root.realPath.endsWith(path.sep) ? root.realPath : `${root.realPath}${path.sep}`,
      )
    )
      return yield* new CheckoutError({ reason: "identity" });
    if (input.worktreePath !== null && cwd.realPath !== project.realPath) {
      const insideProject = cwd.realPath.startsWith(
        project.realPath.endsWith(path.sep) ? project.realPath : `${project.realPath}${path.sep}`,
      );
      if (
        !insideProject &&
        (projectGit === null ||
          git === null ||
          projectGit.commonDirectory !== git.commonDirectory ||
          !projectGit.registeredWorktrees.includes(root.realPath))
      )
        return yield* new CheckoutError({ reason: "identity" });
    }
    if (git !== null && !git.registeredWorktrees.includes(root.realPath))
      return yield* new CheckoutError({ reason: "identity" });
    const requestedBranch =
      input.threadBranch === null
        ? null
        : input.threadBranch.startsWith("refs/")
          ? input.threadBranch
          : `refs/heads/${input.threadBranch}`;
    if (requestedBranch !== null && git?.branch !== requestedBranch)
      return yield* new CheckoutError({ reason: "stale" });
    return {
      ...input,
      version: 1 as const,
      canonicalProjectRoot: project.realPath,
      cwd: cwd.realPath,
      checkoutKey: root.realPath,
      directoryIdentity: root.identity,
      gitCommonDirectory: git?.commonDirectory ?? null,
      gitDirectory: git?.gitDirectory ?? null,
      gitCommonIdentity: git?.commonIdentity ?? null,
      gitDirectoryIdentity: git?.directoryIdentity ?? null,
      branch: git?.branch ?? null,
    };
  });
  return CheckoutTarget.of({
    capture,
    revalidate: (target) =>
      capture(target).pipe(
        Effect.flatMap((current) =>
          canonical(current) === canonical(target)
            ? Effect.void
            : Effect.fail(new CheckoutError({ reason: "stale" })),
        ),
      ),
  });
});
export const layer = Layer.effect(CheckoutTarget, make);

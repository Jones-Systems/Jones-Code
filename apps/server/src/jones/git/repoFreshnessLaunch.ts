import type { OrchestrationV2ThreadLaunchWorkspaceStrategy, ProjectId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import * as RepoFreshness from "./RepoFreshness.ts";

type Worktree = Extract<OrchestrationV2ThreadLaunchWorkspaceStrategy, { type: "worktree" }>;
type FreshnessMode = NonNullable<Worktree["baseFreshness"]>;

export type FreshWorktreeBase =
  | {
      readonly status: "selected";
      readonly startRef: string;
      readonly baseRefName: string;
      readonly detail: string;
    }
  | { readonly status: "local"; readonly detail: string }
  | { readonly status: "failed"; readonly detail: string };

function requestMatchesDefault(
  baseRef: string | undefined,
  remoteName: string,
  branchName: string,
): boolean {
  return (
    baseRef === undefined ||
    baseRef === branchName ||
    baseRef === "refs/heads/" + branchName ||
    baseRef === remoteName + "/" + branchName ||
    baseRef === "refs/remotes/" + remoteName + "/" + branchName
  );
}

export function chooseFreshWorktreeBase(
  mode: FreshnessMode,
  requestedBase: string | undefined,
  status: RepoFreshness.RepoFreshnessStatus,
): FreshWorktreeBase {
  const verification =
    mode === "fetch-now"
      ? status.latestAttempt?.status === "succeeded"
        ? status.latestAttempt.verification
        : null
      : status.latestVerified;
  if (verification === null) {
    return mode === "fetch-now"
      ? { status: "failed", detail: "The remote default branch could not be verified." }
      : { status: "local", detail: "No verified remote commit. Using the local base." };
  }

  if (!requestMatchesDefault(requestedBase, verification.remoteName, verification.branchName)) {
    return mode === "fetch-now"
      ? {
          status: "failed",
          detail: "The selected base no longer matches the remote default branch.",
        }
      : { status: "local", detail: "The selected branch uses its local base." };
  }

  const detail =
    mode === "fetch-now"
      ? "Fetched the remote default branch."
      : status.freshness === "stale"
        ? "Using the last verified commit. The remote check is stale."
        : "Using the last verified commit.";
  return {
    status: "selected",
    startRef: verification.commitSha,
    baseRefName: "refs/remotes/" + verification.remoteName + "/" + verification.branchName,
    detail,
  };
}

export const make = Effect.fn("RepoFreshnessLaunch.make")(function* () {
  const service = yield* Effect.serviceOption(RepoFreshness.RepoFreshness);
  return Effect.fn("RepoFreshnessLaunch.resolve")(function* (
    projectId: ProjectId,
    mode: FreshnessMode,
    requestedBase: string | undefined,
  ): Effect.fn.Return<FreshWorktreeBase> {
    if (Option.isNone(service)) {
      return mode === "fetch-now"
        ? { status: "failed", detail: "Repository freshness is unavailable on this server." }
        : { status: "local", detail: "Repository freshness is unavailable. Using the local base." };
    }
    const current =
      mode === "fetch-now"
        ? (yield* service.value.refresh(projectId)).status
        : yield* service.value.status(projectId);
    return chooseFreshWorktreeBase(mode, requestedBase, current);
  });
});

import { expect, it } from "@effect/vitest";
import { ProjectId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import { RepoFreshness, type RepoFreshnessStatus } from "./RepoFreshness.ts";
import { chooseFreshWorktreeBase, make } from "./repoFreshnessLaunch.ts";

const projectId = ProjectId.make("freshness-launch-project");
const verified = {
  branchName: "main",
  remoteName: "origin",
  commitSha: "a".repeat(40),
  checkedAtMs: 10,
};

const cachedStatus = {
  projectId,
  cadenceSeconds: 120,
  target: { status: "resolved", remoteName: "origin" },
  latestVerified: verified,
  latestAttempt: { status: "failed", checkedAtMs: 11 },
  freshness: "stale",
} satisfies RepoFreshnessStatus;

it("keeps a stale verified SHA available with a warning and preserves explicit feature bases", () => {
  expect(chooseFreshWorktreeBase("verified", "refs/heads/main", cachedStatus)).toEqual({
    status: "selected",
    startRef: verified.commitSha,
    baseRefName: "refs/remotes/origin/main",
    detail: "Using the last verified commit. The remote check is stale.",
  });
  expect(chooseFreshWorktreeBase("verified", "feature/parent", cachedStatus).status).toBe("local");
  expect(chooseFreshWorktreeBase("verified", "archive/main", cachedStatus).status).toBe("local");
  expect(
    chooseFreshWorktreeBase("verified", undefined, {
      ...cachedStatus,
      latestVerified: null,
    }).status,
  ).toBe("local");
});

it("uses the exact manual attempt, refuses a renamed selected branch, and rejects a failed check", () => {
  const live = { ...verified, branchName: "trunk", commitSha: "b".repeat(40), checkedAtMs: 11 };
  const manualStatus = {
    ...cachedStatus,
    latestAttempt: { status: "succeeded", verification: live },
    freshness: "fresh",
  } satisfies RepoFreshnessStatus;
  expect(chooseFreshWorktreeBase("fetch-now", undefined, manualStatus)).toMatchObject({
    status: "selected",
    startRef: live.commitSha,
    baseRefName: "refs/remotes/origin/trunk",
  });
  expect(chooseFreshWorktreeBase("fetch-now", "main", manualStatus).status).toBe("failed");
  expect(chooseFreshWorktreeBase("fetch-now", undefined, cachedStatus).status).toBe("failed");
});

it.effect("routes verified launch to status and only fetches on explicit request", () => {
  const requests: string[] = [];
  return Effect.gen(function* () {
    const select = yield* make();
    expect((yield* select(projectId, "verified", undefined)).status).toBe("selected");
    expect((yield* select(projectId, "fetch-now", undefined)).status).toBe("failed");
    expect(requests).toEqual(["status", "refresh"]);
  }).pipe(
    Effect.provideService(RepoFreshness, {
      status: (id) =>
        Effect.sync(() => {
          expect(id).toBe(projectId);
          requests.push("status");
          return cachedStatus;
        }),
      refresh: (id) =>
        Effect.sync(() => {
          expect(id).toBe(projectId);
          requests.push("refresh");
          return {
            status: cachedStatus,
            checkout: { status: "skipped", reason: "checkout-update-deferred" },
          };
        }),
    }),
  );
});

import { PullRequestOperationError } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { pullRequestReadPause } from "./PullRequestReadPauseBanner";

describe("pull request read pause metadata", () => {
  it("recognizes a typed rate limit and preserves its retry deadline", () => {
    const failure = new PullRequestOperationError({
      operation: "detail",
      detail: "Provider reads are paused.",
      reason: "rate-limited",
      retryAt: 60_000,
    });
    expect(pullRequestReadPause(failure)).toEqual({ retryAt: 60_000 });
  });

  it.each([
    null,
    new Error("rate-limited"),
    { reason: "rate-limited" },
    { _tag: "PullRequestOperationError", reason: "not-found" },
  ])("does not infer a pause from unrelated failures: %s", (failure) => {
    expect(pullRequestReadPause(failure)).toBeNull();
  });

  it.each([undefined, null, "60000", Infinity, NaN])(
    "does not expose an invalid retry deadline: %s",
    (retryAt) => {
      expect(
        pullRequestReadPause({
          _tag: "PullRequestOperationError",
          reason: "rate-limited",
          retryAt,
        }),
      ).toEqual({ retryAt: null });
    },
  );
});

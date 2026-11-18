import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import { ExecutionEnvironmentCapabilities } from "../environment.ts";
import {
  RepoFreshnessInput,
  RepoFreshnessRefreshResult,
  RepoFreshnessStatus,
} from "./repoFreshness.ts";

const verification = {
  remoteName: "origin",
  branchName: "release",
  commitSha: "ab".repeat(32),
  checkedAtMs: 100_000,
};
const snapshot = {
  projectId: "project-a",
  cadenceSeconds: 120,
  target: { status: "resolved", remoteName: "origin" },
  latestVerified: verification,
  latestAttempt: { status: "failed", checkedAtMs: 420_000 },
  freshness: "stale",
  sampledAtMs: 420_000,
};
const decodeStatus = Schema.decodeUnknownSync(RepoFreshnessStatus);
const decodeResult = Schema.decodeUnknownSync(RepoFreshnessRefreshResult);
const decodeInput = Schema.decodeUnknownSync(RepoFreshnessInput);
const decodeCapabilities = Schema.decodeUnknownSync(ExecutionEnvironmentCapabilities);

describe("repository freshness wire contract", () => {
  it("keeps an exact verified SHA when a later attempt fails", () => {
    expect(decodeStatus(snapshot)).toEqual(snapshot);
    expect(() =>
      decodeStatus({
        ...snapshot,
        latestVerified: { ...verification, commitSha: "abc123" },
      }),
    ).toThrow();
  });

  it("carries all checkout result statuses and a future skip reason", () => {
    for (const checkout of [
      { status: "updated" },
      { status: "already-current" },
      { status: "skipped" },
      { status: "skipped", reason: "checkout-update-deferred" },
      { status: "skipped", reason: "future-reason" },
    ]) {
      const response = { status: snapshot, checkout };
      expect(decodeResult(response)).toEqual(response);
    }
  });

  it("requires a project ID and decodes descriptors from older servers", () => {
    expect(() => decodeInput({ cwd: "/tmp/example" })).toThrow();
    expect(decodeCapabilities({}).repoFreshness).toBeUndefined();
    expect(decodeCapabilities({ repoFreshness: true }).repoFreshness).toBe(true);
  });
});

import { describe, expect, it } from "vite-plus/test";
import { ServerSelfUpdateError } from "@t3tools/contracts";
import type { JonesUpdatePolicy } from "@t3tools/contracts/jones/jonesUpdates";
import {
  AUTOMATIC_ACTIVATION_SPACING_MS,
  AutomaticActivationWithdrawn,
  QUIET_INPUT_IDLE_MS,
  automaticActivationDecision,
  findActivationWithdrawal,
  followerTargetRefusal,
  workBlockers,
  type HostWorkSnapshot,
} from "./followerPolicy.ts";

const idle: HostWorkSnapshot = {
  activeRuns: 0,
  unheldQueuedRuns: 0,
  processEffects: 0,
  backgroundTasks: 0,
  pendingRequests: [],
};
const leader = "b".repeat(40);

describe("automatic activation decision", () => {
  const quiet = {
    now: 10 * QUIET_INPUT_IDLE_MS,
    lastInputAt: 0,
    runningTerminals: 0,
    lastAutomaticActivationAt: null,
  };

  it("is ready only after the idle window, spacing and a clean work snapshot", () => {
    expect(automaticActivationDecision({ ...quiet, work: idle })).toEqual({
      ready: true,
      inputIdleMs: 10 * QUIET_INPUT_IDLE_MS,
      blockers: [],
    });
    expect(automaticActivationDecision(quiet)).toMatchObject({
      ready: false,
      reason: "Running work on this host has not been checked yet.",
    });
    expect(automaticActivationDecision({ ...quiet, work: "unavailable" })).toMatchObject({
      ready: false,
      blockers: [{ kind: "unknown", count: 1 }],
    });
  });

  it("names when recent input or a recent activation allows the next attempt", () => {
    const recentInput = automaticActivationDecision({
      ...quiet,
      lastInputAt: quiet.now - 60_000,
      work: idle,
    });
    expect(recentInput).toMatchObject({
      ready: false,
      inputIdleMs: 60_000,
      notBefore: quiet.now - 60_000 + QUIET_INPUT_IDLE_MS,
      reason: "Waiting until no client has had input for 30 minutes (29 minutes left).",
    });
    const recentActivation = automaticActivationDecision({
      ...quiet,
      lastAutomaticActivationAt: quiet.now - 1,
      work: idle,
    });
    expect(recentActivation).toMatchObject({
      ready: false,
      notBefore: quiet.now - 1 + AUTOMATIC_ACTIVATION_SPACING_MS,
      reason: "The previous automatic update was too recent.",
    });
  });

  it("holds for running work but not for restart-survivable questions or held queues", () => {
    const work: HostWorkSnapshot = {
      ...idle,
      activeRuns: 1,
      pendingRequests: [
        { kind: "user_input", capability: "message" },
        { kind: "command_execution_approval", capability: "live" },
        { kind: "file_change_approval", capability: "not_resumable" },
        { kind: "dynamic_tool_call", capability: "message" },
        { kind: "file_read_approval", capability: null },
      ],
    };
    expect(workBlockers(work, 2)).toEqual([
      { kind: "run", count: 1 },
      { kind: "terminal", count: 2 },
      { kind: "approval", count: 2 },
      { kind: "tool-call", count: 1 },
      { kind: "unknown", count: 1 },
    ]);
    expect(
      workBlockers(
        { ...idle, pendingRequests: [{ kind: "user_input", capability: "message" }] },
        0,
      ),
    ).toEqual([]);
    expect(automaticActivationDecision({ ...quiet, runningTerminals: 1, work })).toMatchObject({
      ready: false,
      reason: expect.stringContaining("terminal commands (1)"),
    });
  });
});

describe("follower target refusal", () => {
  it("lets a host without a policy choose its own builds", () => {
    expect(followerTargetRefusal(undefined, "c".repeat(40))).toBeUndefined();
  });

  it("accepts only the laptop's committed source, with the tree it reported", () => {
    const tree = "e".repeat(40);
    const policy = { role: "follower" as const, leaderSource: leader, leaderTree: tree };
    expect(followerTargetRefusal(policy, leader)).toBeUndefined();
    expect(followerTargetRefusal(policy, leader, tree)).toBeUndefined();
    expect(followerTargetRefusal(policy, leader, "9".repeat(40))).toContain("source tree");
    expect(followerTargetRefusal(policy, "c".repeat(40))).toContain("bbbbbbb");
    expect(followerTargetRefusal(policy, "a".repeat(40))).toContain("bbbbbbb");
    expect(followerTargetRefusal(policy, undefined)).toBeDefined();
  });

  it("holds every target until the laptop reports a build with its tree", () => {
    for (const policy of [
      { role: "follower" as const, leaderSource: null },
      { role: "follower" as const, leaderSource: leader },
    ]) {
      expect(followerTargetRefusal(policy, leader)).toContain("waits for the laptop");
      expect(followerTargetRefusal(policy, leader, "e".repeat(40))).toContain("predates following");
    }
  });
});

describe("activation withdrawal", () => {
  it("is found through the self-update error that carries it", () => {
    const withdrawn = new AutomaticActivationWithdrawn("input arrived");
    const error = new ServerSelfUpdateError({ reason: "withdrawn", cause: withdrawn });
    expect(findActivationWithdrawal(error)).toBe(withdrawn);
    expect(findActivationWithdrawal(new Error("other"))).toBeUndefined();
    expect(findActivationWithdrawal(undefined)).toBeUndefined();
  });
});

describe("reviewed follower targets", () => {
  const source = "b".repeat(40);
  const tree = "c".repeat(40);
  const reviewTarget = { source, tree, directiveId: "11111111-1111-4111-8111-111111111111" };
  const waiting = { role: "follower" as const, leaderSource: null };

  it("permits only the reviewed source and tree while preserving legacy follower restrictions", () => {
    expect(followerTargetRefusal(waiting, source, tree)).toBeDefined();
    const policy = { ...waiting, reviewTarget };
    expect(followerTargetRefusal(policy, source)).toBeUndefined();
    expect(followerTargetRefusal(policy, source, tree)).toBeUndefined();
    expect(followerTargetRefusal(policy, source, "d".repeat(40))).toBeDefined();
    expect(followerTargetRefusal(policy, "e".repeat(40), tree)).toBeDefined();
    expect(followerTargetRefusal(policy, undefined)).toBeDefined();
    expect(followerTargetRefusal(policy, undefined, tree)).toBeDefined();

    const following = { ...policy, leaderSource: "e".repeat(40), leaderTree: "f".repeat(40) };
    expect(followerTargetRefusal(following, following.leaderSource)).toBeUndefined();
    expect(
      followerTargetRefusal(following, following.leaderSource, following.leaderTree),
    ).toBeUndefined();
    expect(followerTargetRefusal(following, following.leaderSource, tree)).toContain("source tree");
    expect(followerTargetRefusal(following, "a".repeat(40))).toBeDefined();
  });

  it("never treats an absent review target and an undefined requested target as a review", () => {
    for (const policy of [
      waiting,
      { role: "follower" as const, leaderSource: leader, leaderTree: tree },
    ]) {
      for (const candidate of [policy, { ...policy, reviewTarget: undefined }]) {
        const legacy = candidate as JonesUpdatePolicy;
        expect(followerTargetRefusal(legacy, undefined)).toBeDefined();
        expect(followerTargetRefusal(legacy, undefined, tree)).toBeDefined();
        expect(followerTargetRefusal(legacy, "d".repeat(40))).toBeDefined();
      }
    }
  });

  it("requires a validated source, tree and directive identity before granting a review override", () => {
    for (const unvalidated of [
      { source, directiveId: reviewTarget.directiveId },
      { ...reviewTarget, tree: undefined },
      { ...reviewTarget, tree: "c".repeat(39) },
      { source, tree },
      { ...reviewTarget, directiveId: "not-a-directive" },
      { ...reviewTarget, source: "b".repeat(39) },
      { ...reviewTarget, source: "B".repeat(40) },
      { ...reviewTarget, source: "" },
      { tree, directiveId: reviewTarget.directiveId },
    ]) {
      const policy = { ...waiting, reviewTarget: unvalidated } as unknown as JonesUpdatePolicy;
      const target = "source" in unvalidated ? unvalidated.source : undefined;
      expect(followerTargetRefusal(policy, target)).toBeDefined();
      expect(followerTargetRefusal(policy, target, tree)).toBeDefined();
    }
  });
});

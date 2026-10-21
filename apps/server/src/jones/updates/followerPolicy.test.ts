import { describe, expect, it } from "vite-plus/test";
import { ServerSelfUpdateError } from "@t3tools/contracts";
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

import { describe, expect, it } from "vite-plus/test";
import {
  classifyThreadActivity,
  isOperatingThread,
  type ThreadActivityInput,
} from "./threadActivity.ts";
import type { OrchestrationSessionStatus } from "./orchestrationNative.ts";

const input = (status: OrchestrationSessionStatus | null): ThreadActivityInput => ({
  session: status === null ? null : { status },
  interactionMode: "default",
  latestTurn: null,
  hasPendingApprovals: false,
  hasPendingUserInput: false,
  hasActionableProposedPlan: false,
});

describe("native thread activity", () => {
  it("counts startup and execution, and excludes parked session states", () => {
    for (const status of ["starting", "running"] as const)
      expect(classifyThreadActivity(input(status)).operating).toBe(true);
    for (const status of [null, "idle", "ready", "interrupted", "stopped", "error"] as const)
      expect(classifyThreadActivity(input(status)).operating).toBe(false);
  });
  it("retains foreground waits while independent native background remains positive", () => {
    for (const field of ["hasPendingApprovals", "hasPendingUserInput"] as const) {
      const waiting = { ...input("running"), [field]: true };
      expect(classifyThreadActivity(waiting).operating).toBe(false);
      for (const backgroundLiveness of ["working", "monitoring"] as const) {
        const classified = classifyThreadActivity({ ...waiting, backgroundLiveness });
        expect(classified.operating).toBe(true);
        expect(classified.foreground).toMatch(/^waiting_/);
        expect(classified.background).toBe(backgroundLiveness);
      }
    }
  });
  it("does not let a retained plan suppress execution", () => {
    expect(
      classifyThreadActivity({
        ...input("running"),
        interactionMode: "plan",
        hasActionableProposedPlan: true,
      }).operating,
    ).toBe(true);
    const waiting = {
      ...input("ready"),
      interactionMode: "plan" as const,
      hasActionableProposedPlan: true,
      latestTurn: {
        startedAt: "2026-10-02T00:00:00Z",
        completedAt: "2026-10-02T00:01:00Z",
      },
    };
    expect(classifyThreadActivity(waiting).foreground).toBe("waiting_plan");
    expect(classifyThreadActivity(waiting).operating).toBe(false);
    expect(classifyThreadActivity({ ...waiting, backgroundLiveness: "monitoring" }).operating).toBe(
      true,
    );
  });
  it("excludes archived threads regardless of foreground or background", () => {
    expect(
      isOperatingThread({
        ...input("running"),
        backgroundLiveness: "working",
        archivedAt: "2026-10-02T00:00:00Z",
      }),
    ).toBe(false);
    expect(isOperatingThread({ ...input("running"), archivedAt: null })).toBe(true);
  });
});

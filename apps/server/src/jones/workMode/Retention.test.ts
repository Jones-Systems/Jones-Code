import { describe, expect, it } from "@effect/vitest";
import { CommandId, ProviderInstanceId, ProviderSessionId, ThreadId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { workModeFixture } from "./Fixtures.testkit.ts";
import { workModeRetainsSession } from "./Retention.ts";

const fixture = workModeFixture();
const input = {
  projection: fixture,
  providerSessionId: fixture.providerSessions[0]!.id,
  providerInstanceId: fixture.thread.providerInstanceId,
  nowMs: 30 * 60_000,
};

describe("Work mode session retention", () => {
  it("retains a completed attached context before dispatch is due and beyond nine intervals", () => {
    for (const nowMs of [0, 30 * 60_000, 55 * 60_000, 9 * 55 * 60_000])
      expect(workModeRetainsSession({ ...input, nowMs })).toBe(true);
    expect(workModeRetainsSession({ ...input, nowMs: Number.NaN })).toBe(false);
  });

  it("checks snooze against the real clock, including snoozes ending before dispatch is due", () => {
    const projection = {
      ...fixture,
      thread: { ...fixture.thread, snoozedUntil: DateTime.makeUnsafe(31 * 60_000) },
    };
    expect(workModeRetainsSession({ ...input, projection })).toBe(false);
    expect(workModeRetainsSession({ ...input, projection, nowMs: 31 * 60_000 })).toBe(true);
  });

  it("requires the same native provider session and instance", () => {
    expect(
      workModeRetainsSession({ ...input, providerSessionId: ProviderSessionId.make("other") }),
    ).toBe(false);
    expect(
      workModeRetainsSession({ ...input, providerInstanceId: ProviderInstanceId.make("other") }),
    ).toBe(false);
    expect(
      workModeRetainsSession({
        ...input,
        projection: {
          ...fixture,
          providerThreads: [{ ...fixture.providerThreads[0]!, nativeThreadRef: null }],
        },
      }),
    ).toBe(false);
  });

  it("excludes archived, deleted, settled, blocked, subagent and fork threads", () => {
    const at = DateTime.makeUnsafe(0);
    for (const patch of [
      { archivedAt: at },
      { deletedAt: at },
      { settledAt: at },
      { settledOverride: "settled" as const },
      { threadMessagesBlocked: true },
      {
        selfSettlement: {
          mcpCredentialId: "credential",
          commandId: CommandId.make("settle"),
          runId: fixture.runs[0]!.id,
          providerSessionId: input.providerSessionId,
          providerInstanceId: input.providerInstanceId,
        },
      },
      {
        forkedFrom: {
          type: "run" as const,
          threadId: fixture.thread.id,
          runId: fixture.runs[0]!.id,
        },
      },
      ...(["subagent", "fork"] as const).map((relationshipToParent) => ({
        lineage: {
          ...fixture.thread.lineage,
          parentThreadId: ThreadId.make("parent"),
          relationshipToParent,
        },
      })),
    ]) {
      expect(
        workModeRetainsSession({
          ...input,
          projection: { ...fixture, thread: { ...fixture.thread, ...patch } },
        }),
      ).toBe(false);
    }
  });

  it("excludes stopped or failed sessions, unfinished runs and background-held contexts", () => {
    for (const status of ["stopped", "error"] as const)
      expect(
        workModeRetainsSession({
          ...input,
          projection: {
            ...fixture,
            providerSessions: [{ ...fixture.providerSessions[0]!, status }],
          },
        }),
      ).toBe(false);
    expect(
      workModeRetainsSession({
        ...input,
        projection: {
          ...fixture,
          providerSessions: [{ ...fixture.providerSessions[0]!, lastError: "runtime failed" }],
        },
      }),
    ).toBe(false);
    for (const status of ["waiting", "running", "cancelled", "failed", "interrupted"] as const)
      expect(
        workModeRetainsSession({
          ...input,
          projection: { ...fixture, runs: [{ ...fixture.runs[0]!, status }] },
        }),
      ).toBe(false);
    expect(
      workModeRetainsSession({
        ...input,
        projection: {
          ...fixture,
          providerThreads: [
            {
              ...fixture.providerThreads[0]!,
              pendingBackgroundTasks: [{ taskId: "held", kind: "monitor" }],
            },
          ],
        },
      }),
    ).toBe(false);
    expect(
      workModeRetainsSession({ ...input, projection: { ...fixture, providerTurns: [] } }),
    ).toBe(false);
  });
});

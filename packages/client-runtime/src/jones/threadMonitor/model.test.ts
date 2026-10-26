import {
  EnvironmentId,
  RunId,
  RuntimeRequestId,
  ThreadId,
  type OrchestrationV2ThreadShell,
} from "@t3tools/contracts";
import { THREAD_MONITOR_GUARDED_SETTLE_CAPABILITY } from "@t3tools/contracts/jones/guardedThreadSettlement";
import * as DateTime from "effect/DateTime";
import { describe, expect, it } from "vite-plus/test";
import { v2ThreadShell } from "../../state/orchestrationV2TestFixtures.ts";
import {
  buildMonitorCohort,
  isRecentMonitorCompletion,
  type MonitorEnvironmentInput,
} from "./model.ts";
const now = Date.parse("2026-10-26T07:00:00.000Z");
const at = (offset = 0) => DateTime.makeUnsafe(now + offset);
const shell = (patch: Partial<OrchestrationV2ThreadShell> = {}): OrchestrationV2ThreadShell => ({
  ...v2ThreadShell,
  updatedAt: at(),
  ...patch,
});
const environment = (
  threads: ReadonlyArray<OrchestrationV2ThreadShell>,
): MonitorEnvironmentInput => ({
  environment: {
    publisherGeneration: "g",
    environmentId: EnvironmentId.make("e"),
    environmentEpoch: 0,
    label: "Test",
    transport: "connected",
    freshness: "live",
    reason: null,
  },
  threads,
  projectLabels: new Map(),
  canOperate: true,
  canQueue: true,
  guardedSettleCapability: THREAD_MONITOR_GUARDED_SETTLE_CAPABILITY,
});
const completed = shell({
  status: "completed",
  latestRunId: RunId.make("run"),
  latestRunCompletedAt: at(-1000),
});

describe("source-grounded monitor cohorts", () => {
  it("keeps guest review in Needs You after completion expires without inventing a provider decision", () => {
    const guest = { ...completed, pendingGuestMessageCount: 1, snoozedUntil: at(7_200_000) };
    for (const clock of [now + 3_600_001, now]) {
      const cohort = buildMonitorCohort([environment([guest])], clock);
      expect(cohort.counts).toEqual({ needsYou: 1, working: 0, waiting: 0, done: 0 });
      expect(cohort.nextDoneExpiryMs).toBeNull();
      expect(cohort.rows).toHaveLength(1);
      expect(cohort.rows[0]).toMatchObject({
        group: "needsYou",
        status: "input",
        statusLabel: "Guest message awaiting review",
        pendingChildAttention: null,
        actions: {
          settle: { available: false, reason: "Review pending guest messages in Jones Code first" },
          respondToApproval: { available: false },
          respondToUserInput: { available: false },
        },
      });
      expect(guest.pendingRuntimeRequest).toBeNull();
    }
    const resolved = { ...guest, pendingGuestMessageCount: 0, snoozedUntil: undefined };
    const legacy = { ...completed, pendingGuestMessageCount: undefined };
    for (const thread of [resolved, legacy]) {
      expect(
        buildMonitorCohort([environment([thread])], now).rows[0]?.actions.settle.available,
      ).toBe(true);
      expect(buildMonitorCohort([environment([thread])], now + 3_600_001).rows).toHaveLength(0);
    }
  });

  it("preserves actual provider request precedence when guest review is also pending", () => {
    const thread = {
      ...completed,
      pendingGuestMessageCount: 1,
      pendingRuntimeRequest: {
        id: RuntimeRequestId.make("real-approval"),
        kind: "permission" as const,
        createdAt: at(),
      },
    };
    expect(buildMonitorCohort([environment([thread])], now).rows[0]).toMatchObject({
      group: "needsYou",
      status: "approval",
      statusLabel: "Pending Approval",
      actions: { settle: { available: false } },
    });
  });
  it("uses actual terminal timestamps and the exact half-open last-hour window", () => {
    expect(isRecentMonitorCompletion(DateTime.formatIso(at(-3_599_999)), now)).toBe(true);
    expect(isRecentMonitorCompletion(DateTime.formatIso(at(-3_600_000)), now)).toBe(false);
    expect(isRecentMonitorCompletion(DateTime.formatIso(at(1)), now)).toBe(false);
    expect(isRecentMonitorCompletion(null, now)).toBe(false);
    expect(isRecentMonitorCompletion("invalid", now)).toBe(false);
    const input = environment([completed]);
    const first = buildMonitorCohort([input], now);
    expect(first.counts.done).toBe(1);
    expect(first.nextDoneExpiryMs).toBe(now + 3_599_000);
    expect(buildMonitorCohort([input], now + 3_599_000).counts.done).toBe(0);
    expect(
      buildMonitorCohort([environment([{ ...completed, latestRunCompletedAt: undefined }])], now)
        .rows[0]?.completionTimeUnavailable,
    ).toBe(true);
    expect(
      buildMonitorCohort([environment([{ ...completed, latestRunCompletedAt: at(1) }])], now).counts
        .done,
    ).toBe(0);
    expect(
      buildMonitorCohort([environment([{ ...completed, lastVisitedAt: at() }])], now).counts.done,
    ).toBe(1);
  });
  it("does not call checkpoint, active goals or completion-holding work Done", () => {
    for (const patch of [
      { status: "waiting" as const },
      { goal: { objective: "Finish", status: "active" as const } },
      {
        pendingBackgroundTasks: [
          { kind: "monitor" as const, taskId: "watch", awaited: true as const },
        ],
      },
      { blockedOnThreadWait: true },
      { activeRunId: RunId.make("active") },
    ])
      expect(buildMonitorCohort([environment([{ ...completed, ...patch }])], now).counts.done).toBe(
        0,
      );
    expect(
      buildMonitorCohort([environment([{ ...completed, status: "waiting" }])], now).rows[0]?.status,
    ).toBe("checkpoint");
  });
  it("keeps unawaited monitoring complete while refusing to settle its background work", () => {
    const thread = {
      ...completed,
      pendingBackgroundTasks: [{ kind: "monitor" as const, taskId: "watch" }],
    };
    const cohort = buildMonitorCohort([environment([thread])], now);
    expect(cohort.counts.done).toBe(1);
    expect(cohort.rows[0]?.actions.settle).toEqual({
      available: false,
      reason: "Thread has background work",
    });
  });
  it("retains hidden child attention, request precedence, and scoped duplicate IDs", () => {
    const child = shell({
      id: ThreadId.make("child"),
      lineage: {
        rootThreadId: completed.id,
        parentThreadId: completed.id,
        relationshipToParent: "subagent",
      },
      status: "running",
      pendingRuntimeRequest: {
        id: RuntimeRequestId.make("request"),
        kind: "permission",
        createdAt: at(),
      },
    });
    const first = environment([completed, child]);
    const second = environment([completed]);
    const result = buildMonitorCohort(
      [
        first,
        {
          ...second,
          environment: { ...second.environment, environmentId: EnvironmentId.make("other") },
        },
      ],
      now,
    );
    expect(result.rows).toHaveLength(2);
    expect(result.counts.needsYou).toBe(1);
    expect(result.counts.done).toBe(1);
    expect(result.rows.find((row) => row.environmentId === "e")?.pendingChildAttention).toBe(
      "approval",
    );
    const pending = {
      ...completed,
      pendingRuntimeRequest: {
        id: RuntimeRequestId.make("input"),
        kind: "user_input" as const,
        blocking: false,
        createdAt: at(),
      },
    };
    expect(buildMonitorCohort([environment([pending])], now).rows[0]?.status).toBe("input");
  });
  it("filters placement and snooze without erasing a hand-raised request", () => {
    for (const patch of [
      { deletedAt: at() },
      { archivedAt: at() },
      { settledAt: at() },
      { snoozedUntil: at(10000) },
    ]) {
      expect(
        buildMonitorCohort([environment([{ ...completed, ...patch }])], now).rows,
      ).toHaveLength(0);
    }
    const requesting = {
      ...completed,
      snoozedUntil: at(10000),
      pendingRuntimeRequest: {
        id: RuntimeRequestId.make("a"),
        kind: "permission" as const,
        createdAt: at(),
      },
    };
    expect(buildMonitorCohort([environment([requesting])], now).counts.needsYou).toBe(1);
  });
  it("distinguishes cached freshness from connection health and disables unsafe actions", () => {
    const input = environment([completed]);
    const cached = buildMonitorCohort(
      [{ ...input, environment: { ...input.environment, freshness: "cached" } }],
      now,
    ).rows[0]!;
    expect(cached.actions.reply.available).toBe(false);
    expect(cached.actions.settle.available).toBe(false);
    expect(cached.status).toBe("completed");
    const { guardedSettleCapability: _capability, ...legacyInput } = input;
    expect(buildMonitorCohort([legacyInput], now).rows[0]?.actions.settle.available).toBe(false);
    expect(buildMonitorCohort([input], now).rows[0]?.actions.settle.available).toBe(true);
  });
  it("keeps stable source chronology and truthful failed/limited recovery", () => {
    const failed = shell({
      id: ThreadId.make("failure"),
      status: "failed",
      lastErrorClass: "usage_limit",
    });
    expect(buildMonitorCohort([environment([failed])], now).rows[0]).toMatchObject({
      group: "needsYou",
      status: "limited",
    });
    const recovering = {
      ...failed,
      limitRecovery: { runId: RunId.make("r"), resetAt: "2026-10-26T08:00:00Z", autoResume: true },
    };
    expect(buildMonitorCohort([environment([recovering])], now).rows[0]?.group).toBe("waiting");
    const older = { ...completed, id: ThreadId.make("older"), updatedAt: at(-2000) };
    expect(
      buildMonitorCohort([environment([older, completed])], now).rows.map((row) => row.threadId),
    ).toEqual([completed.id, older.id]);
  });
});

import { describe, expect, it } from "@effect/vitest";
import { ThreadId } from "@t3tools/contracts";
import { WORK_MODE_INTERVAL_MS, WORK_MODE_SENTINEL } from "@t3tools/shared/jones/workMode";
import * as DateTime from "effect/DateTime";
import { threadShellFromProjection } from "../../orchestration-v2/ProjectionStore.ts";
import { workModeFixture } from "./Fixtures.testkit.ts";
import { workModeCandidate, workModeCommand, workModeContext } from "./Policy.ts";

describe("Work Mode eligibility", () => {
  it("becomes due at 55 minutes and preserves exact server provenance", () => {
    const projection = workModeFixture();
    const shell = threadShellFromProjection(projection);
    expect(workModeCandidate(shell, WORK_MODE_INTERVAL_MS - 1)).toBeNull();
    const candidate = workModeCandidate(shell, WORK_MODE_INTERVAL_MS)!;
    const command = workModeCommand(candidate, shell.modelSelection);
    expect(command).toMatchObject({
      text: WORK_MODE_SENTINEL,
      attachments: [],
      createdBy: "system",
      creationSource: "server",
      dispatchMode: { type: "start_immediately" },
      modelSelection: shell.modelSelection,
    });
    expect(workModeCommand(candidate)).toEqual(workModeCommand(candidate));
    expect(workModeContext(projection, WORK_MODE_INTERVAL_MS)?.id).toBe(
      shell.activeProviderThreadId,
    );
  });

  it("excludes forks and subagents even when their provider context completed", () => {
    const shell = threadShellFromProjection(workModeFixture());
    for (const relationshipToParent of ["fork", "subagent"] as const) {
      expect(
        workModeCandidate(
          {
            ...shell,
            lineage: {
              ...shell.lineage,
              parentThreadId: ThreadId.make("parent"),
              relationshipToParent,
            },
          },
          WORK_MODE_INTERVAL_MS,
        ),
      ).toBeNull();
    }
    expect(
      workModeCandidate(
        { ...shell, lineage: { ...shell.lineage, relationshipToParent: "subagent" } },
        WORK_MODE_INTERVAL_MS,
      ),
    ).toBeNull();
  });

  it("skips every blocked, inactive, or uncompleted shell state", () => {
    const shell = threadShellFromProjection(workModeFixture());
    const at = DateTime.makeUnsafe(0);
    const variants = [
      { archivedAt: at },
      { deletedAt: at },
      { settledAt: at },
      { settledOverride: "settled" as const },
      { threadMessagesBlocked: true },
      { activeProviderThreadId: null },
      { latestRunCompletedAt: null },
      { activeRunId: shell.latestRunId },
      { activityRunStatus: "waiting" as const },
      { snoozedUntil: DateTime.makeUnsafe(WORK_MODE_INTERVAL_MS + 1) },
      { pendingBackgroundTasks: [{ taskId: "held", kind: "monitor" as const }] },
      { lastErrorClass: "usage_limit" as const },
    ];
    for (const variant of variants)
      expect(workModeCandidate({ ...shell, ...variant }, WORK_MODE_INTERVAL_MS)).toBeNull();
    for (const status of [
      "idle",
      "queued",
      "preparing",
      "starting",
      "running",
      "waiting",
      "failed",
      "cancelled",
      "interrupted",
      "rolled_back",
    ] as const)
      expect(workModeCandidate({ ...shell, status }, WORK_MODE_INTERVAL_MS)).toBeNull();
  });

  it("regular activity resets the interval; cosmetic thread updates do not", () => {
    const shell = threadShellFromProjection(workModeFixture());
    const activityAt = DateTime.makeUnsafe(WORK_MODE_INTERVAL_MS - 1);
    expect(
      workModeCandidate({ ...shell, latestUserMessageAt: activityAt }, WORK_MODE_INTERVAL_MS),
    ).toBeNull();
    expect(
      workModeCandidate(
        { ...shell, updatedAt: activityAt, lastVisitedAt: activityAt },
        WORK_MODE_INTERVAL_MS,
      ),
    ).not.toBeNull();
    expect(
      workModeCandidate({ ...shell, latestRunCompletedAt: activityAt }, WORK_MODE_INTERVAL_MS),
    ).toBeNull();
  });

  it("rejects missing, cold, failed or busy provider context and hidden queued work", () => {
    const projection = workModeFixture();
    expect(workModeContext({ ...projection, providerTurns: [] }, WORK_MODE_INTERVAL_MS)).toBeNull();
    expect(
      workModeContext({ ...projection, providerSessions: [] }, WORK_MODE_INTERVAL_MS),
    ).toBeNull();
    for (const status of ["not_loaded", "active", "archived", "closed", "error"] as const)
      expect(
        workModeContext(
          { ...projection, providerThreads: [{ ...projection.providerThreads[0]!, status }] },
          WORK_MODE_INTERVAL_MS,
        ),
      ).toBeNull();
    for (const status of [
      "queued",
      "preparing",
      "starting",
      "running",
      "waiting",
      "failed",
      "cancelled",
      "interrupted",
    ] as const)
      expect(
        workModeContext(
          { ...projection, runs: [{ ...projection.runs[0]!, status }] },
          WORK_MODE_INTERVAL_MS,
        ),
      ).toBeNull();
    expect(
      workModeContext(
        {
          ...projection,
          providerThreads: [{ ...projection.providerThreads[0]!, nativeThreadRef: null }],
        },
        WORK_MODE_INTERVAL_MS,
      ),
    ).toBeNull();
    expect(
      workModeContext(
        {
          ...projection,
          messages: [{ ...projection.messages[0]!, updatedAt: DateTime.makeUnsafe(1) }],
        },
        WORK_MODE_INTERVAL_MS,
      ),
    ).toBeNull();
  });
});

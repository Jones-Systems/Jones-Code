import { describe, expect, it } from "@effect/vitest";
import {
  MessageId,
  ProviderThreadId,
  ThreadId,
  type OrchestrationV2ProviderGoalStatus,
} from "@t3tools/contracts";
import { WORK_MODE_INTERVAL_MS, WORK_MODE_SENTINEL } from "@t3tools/shared/jones/workMode";
import * as DateTime from "effect/DateTime";
import { threadShellFromProjection } from "../../orchestration-v2/ProjectionStore.ts";
import { workModeFixture } from "./Fixtures.testkit.ts";
import { workModeCandidate, workModeCommand, workModeContext, workModeTiming } from "./Policy.ts";

describe("Work Mode eligibility", () => {
  it("becomes due at 55 minutes and preserves exact server provenance", () => {
    const projection = workModeFixture();
    const shell = threadShellFromProjection(projection);
    expect(workModeContext(projection, WORK_MODE_INTERVAL_MS - 1)).toBeNull();
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

  it("shell changes identify generations without setting the model activity clock", () => {
    const shell = threadShellFromProjection(workModeFixture());
    const activityAt = DateTime.makeUnsafe(WORK_MODE_INTERVAL_MS - 1);
    expect(
      workModeCandidate({ ...shell, latestUserMessageAt: activityAt }, WORK_MODE_INTERVAL_MS),
    ).not.toBeNull();
    expect(
      workModeCandidate(
        { ...shell, updatedAt: activityAt, lastVisitedAt: activityAt },
        WORK_MODE_INTERVAL_MS,
      ),
    ).not.toBeNull();
    expect(
      workModeCandidate({ ...shell, latestRunCompletedAt: activityAt }, WORK_MODE_INTERVAL_MS),
    ).not.toBeNull();
  });

  it("never refreshes a thread whose provider goal could resume work", () => {
    const projection = workModeFixture();
    const shell = threadShellFromProjection(projection);
    const owner = projection.providerThreads[0]!;
    const withGoal = (status: OrchestrationV2ProviderGoalStatus) => {
      const goal = { objective: "Finish the migration", status };
      return {
        shell: { ...shell, goal },
        projection: { ...projection, providerThreads: [{ ...owner, goal }] },
      };
    };
    for (const status of ["active", "blocked", "usage_limited", "budget_limited"] as const) {
      const variant = withGoal(status);
      expect(workModeCandidate(variant.shell, WORK_MODE_INTERVAL_MS)).toBeNull();
      expect(workModeContext(variant.projection, WORK_MODE_INTERVAL_MS)).toBeNull();
    }
    for (const status of ["paused", "complete"] as const) {
      const variant = withGoal(status);
      expect(workModeCandidate(variant.shell, WORK_MODE_INTERVAL_MS)).not.toBeNull();
      expect(workModeContext(variant.projection, WORK_MODE_INTERVAL_MS)?.id).toBe(owner.id);
    }
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
    ).not.toBeNull();
  });
});

describe("Work cache activity", () => {
  const minute = 60_000;

  it("does not start a first refresh beyond 90 minutes, including after a recent app-only reply", () => {
    const projection = workModeFixture();
    expect(workModeContext(projection, 90 * minute)).not.toBeNull();
    expect(workModeContext(projection, 90 * minute + 1)).toBeNull();
    const recent = DateTime.makeUnsafe(60 * minute);
    expect(
      workModeContext(
        {
          ...projection,
          runs: projection.runs.map((run) => ({ ...run, completedAt: recent })),
          messages: projection.messages.map((message) => ({ ...message, updatedAt: recent })),
        },
        120 * minute,
      ),
    ).toBeNull();
  });

  it("uses the last processed model input despite later run completion or message edits", () => {
    const projection = workModeFixture();
    const later = DateTime.makeUnsafe(40 * minute);
    const held = {
      ...projection,
      runs: projection.runs.map((run) => ({ ...run, completedAt: later })),
      messages: projection.messages.map((message) => ({ ...message, updatedAt: later })),
    };
    expect(workModeContext(held, 55 * minute - 1)).toBeNull();
    expect(workModeContext(held, 55 * minute)).not.toBeNull();
  });

  it.each([undefined, { usedTokens: 200, maxTokens: 1000, updatedAt: "1970-01-01T00:40:00Z" }])(
    "does not infer model input from missing or compaction-only usage: %j",
    (tokenUsage) => {
      const projection = workModeFixture();
      expect(
        workModeContext(
          {
            ...projection,
            providerTurns: projection.providerTurns.map((turn) => ({ ...turn, tokenUsage })),
          },
          55 * minute,
        ),
      ).toBeNull();
    },
  );

  it("ignores another provider conversation's recent model calls", () => {
    const projection = workModeFixture();
    expect(
      workModeContext(
        {
          ...projection,
          providerTurns: [
            ...projection.providerTurns,
            {
              ...projection.providerTurns[0]!,
              providerThreadId: ProviderThreadId.make("other-conversation"),
              tokenUsage: { usedTokens: 1, inputTokens: 1, updatedAt: "1970-01-01T00:40:00Z" },
            },
          ],
        },
        55 * minute,
      ),
    ).not.toBeNull();
  });

  it("requires native root-run inference with a usable timestamp", () => {
    const projection = workModeFixture();
    const turn = projection.providerTurns[0]!;
    for (const patch of [
      { nativeTurnRef: null },
      { runAttemptId: null },
      { tokenUsage: { usedTokens: 1, inputTokens: 1, updatedAt: "invalid" } },
    ])
      expect(
        workModeTiming(
          { ...projection, providerTurns: [{ ...turn, ...patch }] },
          turn.providerThreadId,
        ),
      ).toBeNull();
    expect(
      workModeContext(
        {
          ...projection,
          providerTurns: [
            {
              ...turn,
              tokenUsage: {
                usedTokens: 1,
                cachedInputTokens: 1,
                updatedAt: "1970-01-01T01:00:00Z",
              },
            },
          ],
        },
        55 * minute,
      ),
    ).toBeNull();
  });

  it("keeps an established successful refresh sequence on the 55-minute rule beyond initial age", () => {
    const projection = workModeFixture();
    const message = {
      ...projection.messages[0]!,
      id: MessageId.make("work-mode:established"),
      text: WORK_MODE_SENTINEL,
      createdBy: "system" as const,
      creationSource: "server" as const,
    };
    const established = {
      ...projection,
      runs: projection.runs.map((run) => ({ ...run, userMessageId: message.id })),
      messages: [message],
    };
    expect(workModeContext(established, 3 * 60 * minute)).not.toBeNull();
    expect(
      workModeContext(
        {
          ...established,
          messages: [{ ...message, createdBy: "user", creationSource: "web" }],
        },
        3 * 60 * minute,
      ),
    ).toBeNull();
  });
});

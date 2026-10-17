import { describe, expect, it } from "vite-plus/test";
import { EnvironmentId, ProjectId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import { makeThreadFixture } from "../../test-fixtures";
import {
  filterSidebarV2VisibleThreads,
  resolveThreadLastVisitedAt,
} from "../../components/Sidebar.logic";
import { filterSidebarEnvironmentThreads } from "../environmentFilters/model";
import {
  classifyThreadStatus,
  nextThreadStatusFilter,
  nextThreadStatusWakeAt,
  selectThreadStatus,
  type StatusThread,
  type ThreadStatusFilter,
} from "./model";

const completed = "2026-10-17T12:00:00.000Z";
const visited = "2026-10-17T11:00:00.000Z";
const latestRun = {
  runId: "run" as never,
  status: "completed" as const,
  assistantMessageId: null,
  requestedAt: visited,
  startedAt: visited,
  completedAt: completed,
};
const base: StatusThread = {
  hasPendingApprovals: false,
  hasPendingUserInput: false,
  hasActionableProposedPlan: false,
  interactionMode: "default",
  latestRun: null,
  runtime: null,
};
const runtime = (status: NonNullable<StatusThread["runtime"]>["status"]) => ({
  status,
  providerName: "Codex",
  providerInstanceId: ProviderInstanceId.make("codex"),
  activeRunId: null,
  lastError: null,
  updatedAt: visited,
});

describe("sidebar status filter", () => {
  it("schedules the earliest scoped wake even when its status is filtered out", () => {
    const now = Date.parse(visited);
    const roster = [
      { status: "done", snoozedUntil: completed },
      { status: "working", snoozedUntil: "2026-10-17T13:00:00.000Z" },
      { status: null, snoozedUntil: visited },
      { status: null, snoozedUntil: "invalid" },
      { status: null, snoozedUntil: null },
    ];
    expect(nextThreadStatusWakeAt(roster, now)).toBe(Date.parse(completed));
    expect(nextThreadStatusWakeAt(roster, Date.parse(completed))).toBe(
      Date.parse("2026-10-17T13:00:00.000Z"),
    );
    expect(nextThreadStatusWakeAt(roster, Date.parse("2026-10-17T13:00:00.000Z"))).toBeNull();
    expect(nextThreadStatusWakeAt([], now)).toBeNull();
  });

  it("cycles every state, including empty states, back to all", () => {
    let filter: ThreadStatusFilter = "all";
    const seen = [];
    for (let i = 0; i < 4; i++) {
      filter = nextThreadStatusFilter(filter);
      seen.push(filter);
      expect(selectThreadStatus([], filter, () => null)).toEqual({
        counts: { working: 0, input: 0, done: 0 },
        threads: [],
      });
    }
    expect(seen).toEqual(["working", "input", "done", "all"]);
  });

  it.each(["current", "legacy"] as const)(
    "preserves request precedence and connecting work in %s",
    (layout) => {
      for (const status of ["preparing", "queued", "starting", "running", "waiting"] as const) {
        const working = { ...base, runtime: runtime(status), latestRun, lastVisitedAt: visited };
        expect(classifyThreadStatus(working, layout)).toBe("working");
        expect(classifyThreadStatus({ ...working, hasPendingApprovals: true }, layout)).toBe(
          "input",
        );
        expect(classifyThreadStatus({ ...working, hasPendingUserInput: true }, layout)).toBe(
          "input",
        );
        expect(
          classifyThreadStatus({ ...working, pendingSubagentRequest: "approval" }, layout),
        ).toBe("input");
        expect(
          classifyThreadStatus({ ...working, pendingSubagentRequest: "user_input" }, layout),
        ).toBe("input");
      }
      expect(
        classifyThreadStatus(
          { ...base, interactionMode: "plan", latestRun, hasActionableProposedPlan: true },
          layout,
        ),
      ).toBe("input");
    },
  );

  it("keeps Waiting, failures, and acknowledged or never-visited completions out of Done", () => {
    for (const status of ["idle", "failed"] as const) {
      expect(
        classifyThreadStatus(
          { ...base, runtime: runtime(status), latestRun, lastVisitedAt: visited },
          "current",
        ),
      ).toBeNull();
    }
    for (const layout of ["current", "legacy"] as const) {
      expect(classifyThreadStatus({ ...base, latestRun }, layout)).toBeNull();
      expect(
        classifyThreadStatus({ ...base, latestRun, lastVisitedAt: completed }, layout),
      ).toBeNull();
      expect(classifyThreadStatus({ ...base, latestRun, lastVisitedAt: visited }, layout)).toBe(
        "done",
      );
      expect(classifyThreadStatus({ ...base, latestRun, lastVisitedAt: "invalid" }, layout)).toBe(
        "done",
      );
    }
    expect(
      classifyThreadStatus({ ...base, latestRun, lastVisitedAt: visited }, "current", true),
    ).toBeNull();
  });

  it("honors server mark-unread over local visit and local fallback on older servers", () => {
    for (const [server, local, expected] of [
      [visited, completed, "done"],
      [completed, visited, null],
      [null, visited, null],
      [undefined, visited, "done"],
    ] as const) {
      expect(
        classifyThreadStatus(
          { ...base, latestRun, lastVisitedAt: resolveThreadLastVisitedAt(server, local) },
          "current",
        ),
      ).toBe(expected);
    }
  });

  it("counts the scoped roster before status filtering and updates to zero after a status changes", () => {
    const environmentId = EnvironmentId.make("included");
    const projectId = ProjectId.make("included");
    const working = makeThreadFixture({ environmentId, projectId, runtime: runtime("running") });
    const input = makeThreadFixture({ environmentId, projectId, hasPendingUserInput: true });
    const done = makeThreadFixture({
      environmentId,
      projectId,
      latestRun,
      lastVisitedAt: visited,
      runtime: null,
    });
    const others = [
      makeThreadFixture({ ...working, archivedAt: completed }),
      makeThreadFixture({
        ...working,
        lineage: {
          rootThreadId: working.id,
          parentThreadId: ThreadId.make("parent"),
          relationshipToParent: "subagent",
        },
      }),
      makeThreadFixture({ ...working, projectId: ProjectId.make("other") }),
      makeThreadFixture({ ...working, environmentId: EnvironmentId.make("excluded") }),
    ];
    const scoped = filterSidebarV2VisibleThreads(
      filterSidebarEnvironmentThreads(
        [working, input, done, ...others],
        new Set([EnvironmentId.make("excluded")]),
      ),
      new Set([`${environmentId}:${projectId}`]),
    );
    for (const filter of ["all", "working", "input", "done"] as const) {
      const selected = selectThreadStatus(scoped, filter, (thread) =>
        classifyThreadStatus(thread, "current"),
      );
      expect(selected.counts).toEqual({ working: 1, input: 1, done: 1 });
      expect(selected.threads).toEqual(
        filter === "all"
          ? [working, input, done]
          : filter === "working"
            ? [working]
            : filter === "input"
              ? [input]
              : [done],
      );
    }
    expect(
      selectThreadStatus([input, { ...done, lastVisitedAt: completed }], "working", (thread) =>
        classifyThreadStatus(thread, "current"),
      ),
    ).toEqual({ counts: { working: 0, input: 1, done: 0 }, threads: [] });
  });
});

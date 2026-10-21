import { describe, expect, it } from "vite-plus/test";

import {
  buildDraftActionMenuItems,
  buildThreadActionMenuItems,
  type ThreadActionMenuState,
} from "./threadActionMenu.logic";

const baseState: ThreadActionMenuState = {
  canOperate: true,
  branch: null,
  projectFilter: null,
  isPinned: false,
  isSettled: false,
  autoSettleEnabled: true,
  isSnoozed: false,
  canSnoozeNow: true,
  isRegeneratingTitle: false,
  isRunning: false,
  supports: {
    settlement: true,
    autoSettleOptOut: true,
    snooze: true,
    pinning: true,
    titleRegeneration: true,
  },
  snoozePresets: [
    { id: "hour", label: "In 1 hour", whenLabel: "3:00 PM", snoozedUntil: "2026-08-07T15:00:00Z" },
  ],
};

function ids(state: ThreadActionMenuState): string[] {
  return buildThreadActionMenuItems(state).map((item) => item.id);
}

function allIds(state: ThreadActionMenuState): string[] {
  const flatten = (items: ReturnType<typeof buildThreadActionMenuItems>): string[] =>
    items.flatMap((item) => [item.id, ...(item.children ? flatten(item.children) : [])]);
  return flatten(buildThreadActionMenuItems(state));
}

describe("buildThreadActionMenuItems", () => {
  it.each(["checking", "check_failed", "signed_out", "scope_missing"] as const)(
    "explains disabled environment access (%s) without changing authorized menus",
    (environmentAccessStatus) => {
      const denied = buildThreadActionMenuItems({
        ...baseState,
        canOperate: false,
        environmentAccessStatus,
      });
      expect(denied[0]).toMatchObject({ id: "environment-access-status", disabled: true });
      expect(denied[0]?.label).toBeTruthy();
      expect(denied.find((item) => item.id === "rename")?.disabled).toBe(true);
      expect(denied.find((item) => item.id === "copy")?.disabled).not.toBe(true);
      expect(buildThreadActionMenuItems({ ...baseState, environmentAccessStatus })).toEqual(
        buildThreadActionMenuItems(baseState),
      );
    },
  );
  it.each([false, true])(
    "puts assignment directly above Snooze/Wake with read-only gating (snoozed: %s)",
    (isSnoozed) => {
      const assignmentItems = [
        { id: "assign-workstream:group:alpha" as const, label: "alpha" },
        { id: "assign-workstream:remove" as const, label: "Unassigned (remove primary)" },
      ];
      for (const canOperate of [true, false]) {
        const items = buildThreadActionMenuItems({
          ...baseState,
          isSnoozed,
          canOperate,
          assignmentItems,
        });
        const snooze = items.findIndex((item) => item.id === (isSnoozed ? "unsnooze" : "snooze"));
        expect(items[snooze - 1]).toMatchObject({ id: "assign-thread", label: "Assign thread" });
        expect(items[snooze - 1]?.children).toEqual(
          canOperate
            ? assignmentItems
            : assignmentItems.map((item) => ({ ...item, disabled: true })),
        );
      }
    },
  );

  it("places Stop after snooze and keeps it reachable without session details", () => {
    const items = buildThreadActionMenuItems(baseState);
    expect(items[items.findIndex((item) => item.id === "snooze") + 1]).toMatchObject({
      id: "stop-thread",
      label: "Stop thread",
      icon: "square",
      disabled: false,
    });
    expect(
      buildThreadActionMenuItems(baseState).find((item) => item.id === "stop-thread"),
    ).toMatchObject({ disabled: false });
    expect(
      buildThreadActionMenuItems({
        ...baseState,
        isPinned: true,
        isSettled: true,
      }).find((item) => item.id === "stop-thread"),
    ).toMatchObject({ disabled: false });
  });
  it.each([false, true])(
    "disables both lifecycle directions without permission (reversed: %s)",
    (reversed) => {
      const items = buildThreadActionMenuItems({
        ...baseState,
        canOperate: false,
        isPinned: reversed,
        isSettled: reversed,
        isSnoozed: reversed,
      });
      const expected = reversed
        ? [
            "unpin",
            "unsettle",
            "regenerate-title",
            "unsnooze",
            "stop-thread",
            "rename",
            "auto-settle",
            "archive",
            "delete",
          ]
        : [
            "pin",
            "settle",
            "regenerate-title",
            "snooze",
            "stop-thread",
            "rename",
            "auto-settle",
            "archive",
            "delete",
          ];
      expect(items.filter((item) => item.disabled).map((item) => item.id)).toEqual(expected);
      expect(
        items.find((item) => item.id === "snooze")?.children?.every((child) => child.disabled) ??
          true,
      ).toBe(true);
    },
  );

  it("preserves local actions and restores mutations after a grant", () => {
    const denied = buildThreadActionMenuItems({ ...baseState, canOperate: false, branch: "main" });
    expect(denied.filter((item) => !item.disabled).map((item) => item.id)).toEqual([
      "mark-unread",
      "new-thread-on-branch",
      "copy",
      "project-settings",
    ]);
    const allowed = buildThreadActionMenuItems({
      ...baseState,
      canOperate: true,
    });
    expect(allowed.every((item) => !item.disabled)).toBe(true);
  });

  it("hides capability-gated items while keeping Stop available", () => {
    expect(
      ids({
        ...baseState,
        supports: {
          settlement: false,
          autoSettleOptOut: false,
          snooze: false,
          pinning: false,
          titleRegeneration: false,
        },
      }),
    ).toEqual([
      "mark-unread",
      "stop-thread",
      "rename",
      "copy",
      "project-settings",
      "archive",
      "delete",
    ]);
  });

  it.each([
    {
      titleRegeneration: true,
      assignment: true,
      snooze: true,
      branch: "feat/menu",
      activity: ["regenerate-title", "assign-thread", "snooze", "stop-thread"],
    },
    {
      titleRegeneration: false,
      assignment: true,
      snooze: true,
      branch: null,
      activity: ["assign-thread", "snooze", "stop-thread"],
    },
    {
      titleRegeneration: false,
      assignment: false,
      snooze: true,
      branch: null,
      activity: ["snooze", "stop-thread"],
    },
    {
      titleRegeneration: false,
      assignment: false,
      snooze: false,
      branch: null,
      activity: ["stop-thread"],
    },
  ])("groups actions with optional members: $activity", (scenario) => {
    const items = buildThreadActionMenuItems({
      ...baseState,
      branch: scenario.branch,
      assignmentItems: scenario.assignment ? [] : undefined,
      projectFilter: { label: "Beta Project", isActive: false },
      supports: {
        ...baseState.supports,
        titleRegeneration: scenario.titleRegeneration,
        snooze: scenario.snooze,
      },
    });
    const sections: string[][] = [];
    for (const item of items) {
      if (item.separatorBefore || sections.length === 0) sections.push([]);
      sections.at(-1)?.push(item.id);
    }
    expect(items[0]?.separatorBefore).not.toBe(true);
    expect(sections).toEqual([
      ["mark-unread", "pin", "settle"],
      scenario.activity,
      [
        ...(scenario.branch ? ["new-thread-on-branch"] : []),
        "rename",
        "filter-by-project",
        "auto-settle",
      ],
      ["copy", "project-settings"],
      ["archive", "delete"],
    ]);
  });

  it("offers project filtering only for surfaces with a scoped thread list", () => {
    expect(ids(baseState)).not.toContain("filter-by-project");
    expect(
      buildThreadActionMenuItems({
        ...baseState,
        projectFilter: { label: "Beta Project", isActive: false },
      }).find((item) => item.id === "filter-by-project"),
    ).toMatchObject({ label: "Filter by Beta Project", icon: "folder-tree" });
  });

  it("offers the way back to all projects once the list is scoped", () => {
    const items = buildThreadActionMenuItems({
      ...baseState,
      projectFilter: { label: "Beta Project", isActive: true },
    });
    const filterIndex = items.findIndex((candidate) => candidate.id === "filter-by-project");
    expect(items[filterIndex]).toMatchObject({ label: "Show all projects", icon: "folder-tree" });
    expect(items[filterIndex - 1]?.id).toBe("rename");
    expect(items[filterIndex + 1]?.id).toBe("auto-settle");
  });

  it("includes branch items only for threads with a branch", () => {
    const withBranch = allIds({ ...baseState, branch: "feat/menu" });
    expect(withBranch).toContain("new-thread-on-branch");
    expect(withBranch).toContain("copy-branch");
    expect(allIds(baseState)).not.toContain("new-thread-on-branch");
    expect(allIds(baseState)).not.toContain("copy-branch");
  });

  it("flips lifecycle labels with thread state", () => {
    expect(ids({ ...baseState, isPinned: true, isSettled: true, isSnoozed: true })).toEqual(
      expect.arrayContaining(["unpin", "unsettle", "unsnooze"]),
    );
    expect(ids(baseState)).toEqual(expect.arrayContaining(["pin", "settle", "snooze"]));
  });

  it("offers auto-settle as a submenu with the current option checked", () => {
    const find = (state: ThreadActionMenuState) =>
      buildThreadActionMenuItems(state).find((item) => item.id === "auto-settle");
    const on = find(baseState);
    expect(on?.label).toBe("Auto-settle behavior");
    expect(on?.children?.map((child) => [child.id, child.checked])).toEqual([
      ["auto-settle:enabled", true],
      ["auto-settle:disabled", false],
    ]);
    const off = find({ ...baseState, autoSettleEnabled: false });
    expect(off?.children?.map((child) => child.checked)).toEqual([false, true]);
    const items = buildThreadActionMenuItems(baseState);
    expect(items[items.findIndex((item) => item.id === "rename") + 1]?.id).toBe("auto-settle");
    expect(
      ids({ ...baseState, supports: { ...baseState.supports, autoSettleOptOut: false } }),
    ).not.toContain("auto-settle");
  });

  it("disables snooze when the thread cannot snooze, keeping presets visible", () => {
    const snooze = buildThreadActionMenuItems({ ...baseState, canSnoozeNow: false }).find(
      (item) => item.id === "snooze",
    );
    expect(snooze?.disabled).toBe(true);
    expect(snooze?.children?.map((child) => child.id)).toEqual(["snooze:hour", "snooze:custom"]);
  });

  it("disables title regeneration while one is in flight", () => {
    const item = buildThreadActionMenuItems({ ...baseState, isRegeneratingTitle: true }).find(
      (candidate) => candidate.id === "regenerate-title",
    );
    expect(item).toMatchObject({ label: "Regenerating…", disabled: true });
  });

  it("marks delete as destructive and keeps it last", () => {
    const items = buildThreadActionMenuItems({ ...baseState, branch: "main" });
    expect(items.at(-1)).toMatchObject({ id: "delete", destructive: true });
  });
  it("offers archive as a non-destructive action right before delete", () => {
    const items = buildThreadActionMenuItems(baseState);
    const archiveItem = items.at(-2);
    expect(archiveItem?.id).toBe("archive");
    expect(archiveItem?.icon).toBe("archive");
    expect(archiveItem?.separatorBefore).toBe(true);
    expect(archiveItem?.destructive).toBeFalsy();
    expect(items.at(-1)?.id).toBe("delete");
  });

  it("keeps archive available even when the environment lacks every other capability", () => {
    expect(
      ids({
        ...baseState,
        supports: {
          settlement: false,
          autoSettleOptOut: false,
          snooze: false,
          pinning: false,
          titleRegeneration: false,
        },
      }),
    ).toContain("archive");
  });

  it("disables archive while the thread is running", () => {
    const archiveItem = buildThreadActionMenuItems({ ...baseState, isRunning: true }).find(
      (item) => item.id === "archive",
    );
    expect(archiveItem?.disabled).toBe(true);
  });
});

describe("buildDraftActionMenuItems", () => {
  it("offers only the copy values the draft has", () => {
    const items = buildDraftActionMenuItems({ hasPath: false, hasBranch: true, hasProject: true });
    expect(items[0]).toMatchObject({ id: "copy", disabled: false });
    expect(items[0]?.children?.map((item) => item.id)).toEqual(["copy-branch"]);

    const noCopy = buildDraftActionMenuItems({
      hasPath: false,
      hasBranch: false,
      hasProject: true,
    });
    expect(noCopy[0]).toMatchObject({ id: "copy", disabled: true, children: [] });
  });

  it("drops project settings without a project and keeps discard last", () => {
    const items = buildDraftActionMenuItems({ hasPath: true, hasBranch: false, hasProject: false });
    expect(items.map((item) => item.id)).toEqual(["copy", "discard"]);
    expect(items.at(-1)).toMatchObject({ label: "Discard draft", destructive: true });
  });
});

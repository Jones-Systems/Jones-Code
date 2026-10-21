import { describe, expect, it } from "vite-plus/test";

import { buildThreadActionMenuItems, type ThreadActionMenuState } from "./threadActionMenu.logic";

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

  it("disables archive while the thread is running", () => {
    const archiveItem = buildThreadActionMenuItems({ ...baseState, isRunning: true }).find(
      (item) => item.id === "archive",
    );
    expect(archiveItem?.disabled).toBe(true);
  });
});

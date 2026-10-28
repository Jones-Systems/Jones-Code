import {
  CommandId,
  EnvironmentAuthorizationError,
  EnvironmentId,
  ThreadId,
  type SettlementRestoreInspection,
} from "@t3tools/contracts";
import { AsyncResult } from "effect/reactivity";
import { afterEach, beforeEach, describe, expect, it, vi, type MockInstance } from "vite-plus/test";

import {
  buildThreadActionMenuItems,
  type ThreadActionMenuState,
} from "../../components/threadActionMenu.logic";
import { SETTLEMENT_RESTORE_MENU_ID } from "./menu";
import { settlementRestoreTransport } from "./settlementRestore";
import { undoSettleFromMenu, withThreadSettlementRestore } from "./threadMenu";

const access = vi.hoisted(() => ({ restore: true, operate: true }));

vi.mock("./settlementRestore", async (original) => ({
  ...(await original<typeof import("./settlementRestore")>()),
  readEnvironmentSupportsSettlementRestore: () => access.restore,
}));
vi.mock("../../state/session", async (original) => ({
  ...(await original<typeof import("../../state/session")>()),
  readEnvironmentScope: () => access.operate,
}));

const environmentId = EnvironmentId.make("sidebar-env");
const threadRef = { environmentId, threadId: ThreadId.make("settled-thread") };
const settleCommandId = CommandId.make("settle-command");

// The sidebar's menu: workstream assignment, project filter and access status included.
const sidebarMenu = (isSettled: boolean, canOperate = true) =>
  buildThreadActionMenuItems({
    environmentAccessStatus: canOperate ? "granted" : "scope_missing",
    assignmentItems: [{ id: "assign-workstream:none" as never, label: "No workstream" }],
    canOperate,
    branch: "main",
    projectFilter: { label: "Project", isActive: false },
    isPinned: false,
    isSettled,
    autoSettleEnabled: true,
    isSnoozed: false,
    canSnoozeNow: !isSettled,
    isRegeneratingTitle: false,
    isRunning: false,
    supports: {
      settlement: true,
      autoSettleOptOut: true,
      snooze: true,
      pinning: true,
      titleRegeneration: true,
    },
    snoozePresets: [],
  } satisfies ThreadActionMenuState);

const restorable: SettlementRestoreInspection = {
  threadId: threadRef.threadId,
  status: "restorable",
  record: {
    settleCommandId,
    settledSequence: 2,
    settledAt: "2026-10-20T10:00:00.000Z",
    capturedAt: "2026-10-20T10:00:00.000Z",
    prior: {
      settledOverride: null,
      unsettledAt: null,
      pinnedAt: "2026-10-01T00:00:00.000Z",
      pinOrderKey: "a1",
      activeOrderKey: null,
    },
    snoozedUntilAtSettle: null,
    clearedWatchCount: 0,
    restore: null,
    notRestored: [],
  },
};

type Transport = typeof settlementRestoreTransport;
let dispatch: MockInstance<Transport["dispatch"]>;
let inspect: MockInstance<Transport["inspect"]>;
const reportFailure = vi.fn<(error: unknown) => void>();

beforeEach(() => {
  access.restore = true;
  access.operate = true;
  reportFailure.mockReset();
  dispatch = vi
    .spyOn(settlementRestoreTransport, "dispatch")
    .mockResolvedValue(AsyncResult.success({ sequence: 3 }));
  inspect = vi.spyOn(settlementRestoreTransport, "inspect");
});
afterEach(() => {
  vi.restoreAllMocks();
});

describe("sidebar thread menu Undo settle", () => {
  it("sits after Un-settle and restores the server's current settle after a reload", async () => {
    const items = withThreadSettlementRestore(sidebarMenu(true), {
      threadRef,
      isSettled: true,
      canOperate: true,
    });
    const ids = items.map((item) => item.id);
    expect(ids[ids.indexOf("unsettle") + 1]).toBe(SETTLEMENT_RESTORE_MENU_ID);
    expect(items.find((item) => item.id === SETTLEMENT_RESTORE_MENU_ID)?.disabled).toBe(false);

    // No notice and no remembered settle: the server's record names the settle to undo.
    inspect.mockResolvedValueOnce(AsyncResult.success(restorable));
    await undoSettleFromMenu(threadRef, reportFailure);

    expect(inspect).toHaveBeenCalledExactlyOnceWith(environmentId, {
      threadId: threadRef.threadId,
    });
    expect(dispatch).toHaveBeenCalledExactlyOnceWith(environmentId, {
      commandId: expect.any(String),
      threadId: threadRef.threadId,
      settleCommandId,
    });
    expect(reportFailure).not.toHaveBeenCalled();
  });

  it("reports a thread with no settle to undo instead of sending a restore", async () => {
    inspect.mockResolvedValueOnce(
      AsyncResult.success({ threadId: threadRef.threadId, status: "none", record: null }),
    );
    await undoSettleFromMenu(threadRef, reportFailure);

    expect(dispatch).not.toHaveBeenCalled();
    expect(reportFailure).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({ message: expect.stringMatching(/no settle to undo/) }),
    );
  });

  it("is disabled without operate access, and refuses before asking the server", async () => {
    const items = withThreadSettlementRestore(sidebarMenu(true, false), {
      threadRef,
      isSettled: true,
      canOperate: false,
    });
    expect(items.find((item) => item.id === SETTLEMENT_RESTORE_MENU_ID)?.disabled).toBe(true);

    access.operate = false;
    await undoSettleFromMenu(threadRef, reportFailure);

    expect(inspect).not.toHaveBeenCalled();
    expect(dispatch).not.toHaveBeenCalled();
    expect(reportFailure).toHaveBeenCalledExactlyOnceWith(
      expect.any(EnvironmentAuthorizationError),
    );
  });

  it("is absent for threads that are not settled and on servers without restore", () => {
    expect(
      withThreadSettlementRestore(sidebarMenu(false), {
        threadRef,
        isSettled: false,
        canOperate: true,
      }).map((item) => item.id),
    ).not.toContain(SETTLEMENT_RESTORE_MENU_ID);

    access.restore = false;
    const items = sidebarMenu(true);
    expect(
      withThreadSettlementRestore(items, { threadRef, isSettled: true, canOperate: true }),
    ).toBe(items);
  });
});

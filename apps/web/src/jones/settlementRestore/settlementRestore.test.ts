import {
  CommandId,
  EnvironmentId,
  ThreadId,
  type SettlementRestoreInspection,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import { AsyncResult } from "effect/reactivity";
import { afterEach, beforeEach, describe, expect, it, vi, type MockInstance } from "vite-plus/test";

import { toastManager } from "../../components/ui/toast";
import { undoLatestThreadAction, useThreadUndoNotice } from "../../hooks/showThreadUndoNotice";
import { useThreadActions } from "../../hooks/useThreadActions";
import { threadEnvironment } from "../../state/threads";
import { withSettlementRestoreMenuItem, SETTLEMENT_RESTORE_MENU_ID } from "./menu";
import { restoreCurrentSettlement, settlementRestoreTransport } from "./settlementRestore";

const commands = vi.hoisted(() => ({
  pin: vi.fn(),
  settle: vi.fn(),
  unsettle: vi.fn(),
  snooze: vi.fn(),
}));
const capability = vi.hoisted(() => ({ restore: true }));
const shells = vi.hoisted(() => new Map<string, Record<string, unknown>>());

vi.mock("./settlementRestore", async (original) => ({
  ...(await original<typeof import("./settlementRestore")>()),
  readEnvironmentSupportsSettlementRestore: () => capability.restore,
}));
vi.mock("../../state/use-atom-query-runner", () => ({ useAtomQueryRunner: () => vi.fn() }));
vi.mock("../../state/session", async (original) => ({
  ...(await original<typeof import("../../state/session")>()),
  readEnvironmentScope: () => true,
}));
vi.mock("react", async (original) => ({
  ...(await original<typeof import("react")>()),
  useCallback: (callback: unknown) => callback,
  useMemo: (create: () => unknown) => create(),
  useRef: (value: unknown) => ({ current: value }),
}));
vi.mock("@tanstack/react-router", () => ({
  useRouter: () => ({ navigate: vi.fn(), state: { matches: [{ params: {} }] } }),
}));
vi.mock("../../hooks/useSettings", () => ({ useClientSettings: () => false }));
vi.mock("../../hooks/useHandleNewThread", () => ({ useNewThreadHandler: () => vi.fn() }));
vi.mock("../../composerDraftStore", () => ({ useComposerDraftStore: () => vi.fn() }));
vi.mock("../../terminalUiStateStore", () => ({ useTerminalUiStateStore: () => vi.fn() }));
vi.mock("../../uiStateStore", () => ({ useUiStateStore: () => vi.fn() }));
vi.mock("../../state/entities", async (original) => ({
  ...(await original<typeof import("../../state/entities")>()),
  readEnvironmentSupportsPinning: () => true,
  readEnvironmentSupportsPinReorder: () => true,
  readEnvironmentSupportsSettlement: () => true,
  readEnvironmentSupportsSnooze: () => true,
  readThreadShell: (ref: { threadId: string }) => shells.get(ref.threadId) ?? null,
}));
vi.mock("../../state/use-atom-command", () => ({
  useAtomCommand: (command: unknown) => {
    switch (command) {
      case threadEnvironment.pin:
        return commands.pin;
      case threadEnvironment.settle:
        return commands.settle;
      case threadEnvironment.unsettle:
        return commands.unsettle;
      case threadEnvironment.snooze:
        return commands.snooze;
      default:
        return vi.fn();
    }
  },
}));

const environmentId = EnvironmentId.make("restore-env");
const target = (threadId: string) => ({ environmentId, threadId: ThreadId.make(threadId) });

function addShell(threadId: string, fields: Record<string, unknown> = {}) {
  shells.set(threadId, {
    id: threadId,
    title: threadId,
    projectId: "project",
    environmentId,
    runtime: null,
    settledOverride: null,
    settledAt: null,
    pinnedAt: "2026-10-01T00:00:00.000Z",
    pinOrderKey: "a1",
    snoozedUntil: "2030-01-01T09:00:00.000Z",
    ...fields,
  });
}

function settleIdOf(threadId: string) {
  const call = commands.settle.mock.calls.find(([value]) => value.input.threadId === threadId);
  return call?.[0].input.commandId as CommandId;
}

const restorable = (threadId: string, settleCommandId: CommandId): SettlementRestoreInspection => ({
  threadId: ThreadId.make(threadId),
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
});

type Transport = typeof settlementRestoreTransport;
let dispatch: MockInstance<Transport["dispatch"]>;
let inspect: MockInstance<Transport["inspect"]>;

beforeEach(() => {
  vi.useFakeTimers();
  capability.restore = true;
  shells.clear();
  for (const command of Object.values(commands)) {
    command.mockReset().mockResolvedValue(AsyncResult.success({ sequence: 1 }));
  }
  dispatch = vi
    .spyOn(settlementRestoreTransport, "dispatch")
    .mockResolvedValue(AsyncResult.success({ sequence: 2 }));
  inspect = vi.spyOn(settlementRestoreTransport, "inspect");
});
afterEach(() => {
  vi.runAllTimers();
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe("settle notice Undo on a server with native restore", () => {
  it("sends one restore bound to the settle the notice shows, and no legacy commands", async () => {
    addShell("one");
    await useThreadActions().settleThread(target("one"));
    const settleCommandId = settleIdOf("one");
    expect(settleCommandId).toEqual(expect.any(String));

    expect(undoLatestThreadAction()).toBe(true);
    await vi.waitFor(() => expect(dispatch).toHaveBeenCalledOnce());
    expect(dispatch).toHaveBeenCalledWith(environmentId, {
      commandId: expect.any(String),
      threadId: "one",
      settleCommandId,
    });
    expect(dispatch.mock.calls[0]![1].commandId).not.toBe(settleCommandId);
    expect(commands.unsettle).not.toHaveBeenCalled();
    expect(commands.pin).not.toHaveBeenCalled();
    expect(commands.snooze).not.toHaveBeenCalled();
    expect(useThreadUndoNotice.getState().notice).toBeNull();
  });

  it("restores each thread of a grouped notice by its own settle", async () => {
    addShell("one");
    addShell("two");
    const actions = useThreadActions();
    await actions.settleThread(target("one"));
    await actions.settleThread(target("two"));
    expect(useThreadUndoNotice.getState().notice).toMatchObject({ action: "Settled", count: 2 });

    await useThreadUndoNotice.getState().notice!.undo();
    expect(dispatch.mock.calls.map(([, command]) => command)).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ threadId: "one", settleCommandId: settleIdOf("one") }),
        expect.objectContaining({ threadId: "two", settleCommandId: settleIdOf("two") }),
      ]),
    );
    expect(dispatch).toHaveBeenCalledTimes(2);
  });

  it("shows why a refused restore did not apply, without falling back", async () => {
    const add = vi.spyOn(toastManager, "add").mockReturnValue("toast");
    addShell("one");
    dispatch.mockResolvedValueOnce(
      AsyncResult.failure(
        Cause.fail({ detail: "Settlement restore rejected: superseded: changed." }),
      ),
    );
    await useThreadActions().settleThread(target("one"));
    await useThreadUndoNotice.getState().notice!.undo();
    expect(add).toHaveBeenCalledWith(
      expect.objectContaining({
        type: "error",
        title: "Failed to undo settle",
        description: expect.stringMatching(/changed after it was settled/),
      }),
    );
    expect(commands.unsettle).not.toHaveBeenCalled();
  });

  it("treats Undo of a settle that changed nothing as done", async () => {
    const add = vi.spyOn(toastManager, "add").mockReturnValue("toast");
    addShell("one", {
      settledOverride: "settled",
      settledAt: "2026-10-19T00:00:00.000Z",
      pinnedAt: null,
    });
    dispatch.mockResolvedValueOnce(
      AsyncResult.failure(
        Cause.fail({ detail: "Settlement restore rejected: stale_settle_command: newer." }),
      ),
    );
    await useThreadActions().settleThread(target("one"));
    await useThreadUndoNotice.getState().notice!.undo();
    expect(add).not.toHaveBeenCalled();
  });
});

describe("older servers", () => {
  it("keep the legacy Undo and never see a restore", async () => {
    capability.restore = false;
    addShell("one");
    await useThreadActions().settleThread(target("one"));
    await useThreadUndoNotice.getState().notice!.undo();
    expect(commands.unsettle).toHaveBeenCalledOnce();
    expect(commands.pin).toHaveBeenCalledOnce();
    expect(dispatch).not.toHaveBeenCalled();
  });

  it("get no Undo settle menu item", () => {
    const items = [
      { id: "unsettle", label: "Un-settle thread" },
      { id: "rename", label: "Rename" },
    ];
    expect(withSettlementRestoreMenuItem(items, { available: false, canOperate: true })).toBe(
      items,
    );
    expect(
      withSettlementRestoreMenuItem(items, { available: true, canOperate: false }).map((item) => [
        item.id,
        item.disabled,
      ]),
    ).toEqual([
      ["unsettle", undefined],
      [SETTLEMENT_RESTORE_MENU_ID, true],
      ["rename", undefined],
    ]);
  });
});

describe("thread menu Undo settle", () => {
  it("restores the server's current settle after the notice expired", async () => {
    addShell("one");
    await useThreadActions().settleThread(target("one"));
    vi.advanceTimersByTime(5_000);
    expect(useThreadUndoNotice.getState().notice).toBeNull();
    const settleCommandId = settleIdOf("one");
    inspect.mockResolvedValueOnce(AsyncResult.success(restorable("one", settleCommandId)));

    expect(await restoreCurrentSettlement(target("one"))).toMatchObject({ _tag: "Success" });
    expect(inspect).toHaveBeenCalledExactlyOnceWith(environmentId, { threadId: "one" });
    expect(dispatch).toHaveBeenCalledExactlyOnceWith(environmentId, {
      commandId: expect.any(String),
      threadId: "one",
      settleCommandId,
    });
  });

  it("replaces a still-visible notice, so Cmd+Z cannot send a second restore", async () => {
    addShell("one");
    await useThreadActions().settleThread(target("one"));
    inspect.mockResolvedValueOnce(AsyncResult.success(restorable("one", settleIdOf("one"))));
    await restoreCurrentSettlement(target("one"));
    expect(useThreadUndoNotice.getState().notice).toBeNull();
    expect(undoLatestThreadAction()).toBe(false);
    expect(dispatch).toHaveBeenCalledOnce();
  });

  it("explains a thread with no settle record, such as one settled before the update", async () => {
    inspect.mockResolvedValueOnce(
      AsyncResult.success({ threadId: ThreadId.make("old"), status: "none", record: null }),
    );
    const result = await restoreCurrentSettlement(target("old"));
    expect(result._tag).toBe("Failure");
    expect(Cause.squash((result as { cause: Cause.Cause<unknown> }).cause)).toMatchObject({
      message: expect.stringMatching(/no settle to undo/),
    });
    expect(dispatch).not.toHaveBeenCalled();
  });
});

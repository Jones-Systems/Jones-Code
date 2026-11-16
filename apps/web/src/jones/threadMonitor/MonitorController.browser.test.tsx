import { CommandId, type EnvironmentId } from "@t3tools/contracts";
import { THREAD_MONITOR_GUARDED_SETTLE_CAPABILITY } from "@t3tools/contracts/jones/guardedThreadSettlement";
import type { MonitorEnvironmentInput } from "@t3tools/client-runtime/jones/thread-monitor";
import { buildMonitorCohort } from "@t3tools/client-runtime/jones/thread-monitor";
import {
  DESKTOP_THREAD_MONITOR_PROTOCOL,
  decodeThreadMonitorFrame,
  type ThreadMonitorPeerIntent,
  type ThreadMonitorPublisherFrame,
} from "@t3tools/contracts/jones/desktopThreadMonitor";
import { AsyncResult } from "effect/reactivity";
import * as DateTime from "effect/DateTime";
import { describe, expect, it, vi } from "vite-plus/test";
import { act, render, stubGlobal } from "../../../test/render";
import { makeThreadProjectionFixture } from "../../test-fixtures";
import type { AppRouter } from "../../router";

const ports = vi.hoisted(() => ({
  inputs: [] as ReadonlyArray<MonitorEnvironmentInput>,
  run: vi.fn<(...args: unknown[]) => Promise<unknown>>(),
  listeners: new Set<() => void>(),
}));
vi.mock("@effect/atom-react", async () => {
  const { useSyncExternalStore } = await import("react");
  return {
    useAtomValue: () =>
      useSyncExternalStore(
        (listener) => {
          ports.listeners.add(listener);
          return () => {
            ports.listeners.delete(listener);
          };
        },
        () => ports.inputs,
      ),
  };
});
vi.mock("../../rpc/atomRegistry", () => ({ appAtomRegistry: { get: () => ports.inputs } }));
vi.mock("./inputs", () => ({
  monitorInputsAtom: "inputs",
  monitorInputFor: (inputs: ReadonlyArray<MonitorEnvironmentInput>, id: EnvironmentId) =>
    inputs.find((input) => input.environment.environmentId === id),
}));
vi.mock("./store", async (original) => {
  const module = await original<typeof import("./store")>();
  return { ...module, monitorStore: new module.MonitorStore("generation") };
});
vi.mock("../../state/session", () => ({ readEnvironmentScope: () => true }));
vi.mock("../../state/server", () => ({ environmentServerConfigsAtom: "configs" }));
vi.mock("../../state/shell", () => ({ environmentSnapshotAtom: () => "snapshot" }));
vi.mock("../../state/threads", () => ({
  threadEnvironment: { settle: "settle", startTurn: "start" },
  environmentThreadDetails: { stateAtom: () => "detail" },
}));
vi.mock("../../state/vcs", () => ({ vcsEnvironment: { readRefs: "refs" } }));
vi.mock("../settlementRestore/settlementRestore", () => ({
  restoreObservedSettlement: vi.fn(),
  settlementRestoreTransport: { inspect: vi.fn() },
  readEnvironmentSupportsSettlementRestore: () => true,
}));
vi.mock("@t3tools/client-runtime/state/runtime", async (original) => ({
  ...(await original<typeof import("@t3tools/client-runtime/state/runtime")>()),
  runAtomCommand: (...args: unknown[]) => ports.run(...args),
}));
import { restoreObservedSettlement } from "../settlementRestore/settlementRestore";
import { MonitorController } from "./MonitorController";
import { MonitorUI } from "./MonitorUI";
import { MonitorPeerStore } from "./peerStore";
import { monitorStore, refOf } from "./store";
import { testEnvironment } from "./fixtures.test-support";
import "./monitor.css";

describe("authenticated publisher replay into recreated peers", () => {
  it("reconciles the original rowless Restore after TWO peer recreations through fullReplay", async () => {
    const now = Date.parse("2026-10-28T12:00:00Z");
    using _now = vi.spyOn(Date, "now").mockReturnValue(now);
    ports.run.mockResolvedValue(AsyncResult.success(undefined));
    vi.mocked(restoreObservedSettlement)
      .mockRejectedValueOnce(new Error("Synthetic lost backend delivery"))
      .mockResolvedValue(AsyncResult.success(undefined));
    const thread = {
      ...makeThreadProjectionFixture().thread,
      latestRunId: null,
      activeRunId: null,
      status: "idle" as const,
      pendingRuntimeRequest: null,
      latestVisibleMessage: null,
      latestUserMessageAt: null,
      hasActionableProposedPlan: false,
      itemCount: 0,
      visibleItemCount: 0,
    };
    ports.inputs = [
      {
        environment: testEnvironment,
        threads: [thread],
        projectLabels: new Map(),
        canOperate: true,
        canQueue: true,
        guardedSettleCapability: THREAD_MONITOR_GUARDED_SETTLE_CAPABILITY,
      },
    ];
    monitorStore.register(testEnvironment.environmentId);
    const row = buildMonitorCohort(ports.inputs, now).rows.find((r) => r.threadId === thread.id)!;
    expect(row.actions.settle.available).toBe(true);
    const intents: ThreadMonitorPeerIntent[] = [];
    const frames: ThreadMonitorPublisherFrame[] = [];
    let listener: ((intent: ThreadMonitorPeerIntent) => void) | undefined;
    let peer: MonitorPeerStore | null = null;
    const dispatch = async (intent: ThreadMonitorPeerIntent) => {
      intents.push(intent);
      listener?.(intent);
    };
    const bridge = {
      publish: async (frame: ThreadMonitorPublisherFrame) => {
        const decoded = decodeThreadMonitorFrame({
          schemaVersion: DESKTOP_THREAD_MONITOR_PROTOCOL,
          payload: frame,
        });
        frames.push(frame);
        peer?.receive(decoded.payload as ThreadMonitorPublisherFrame);
      },
      onIntent: (next: typeof listener) => {
        listener = next;
        return () => {
          listener = undefined;
        };
      },
      onState: () => () => undefined,
      getState: async () => ({
        supported: true,
        enabled: true,
        visible: true,
        mainVisible: false,
        mainFocused: false,
        expanded: true,
        reason: null,
      }),
      invalidateEnvironment: async (frame: object) => ({
        ...frame,
        type: "environmentInvalidated",
      }),
    };
    using _bridge = stubGlobal("desktopBridge", { threadMonitor: bridge });
    const router = { navigate: vi.fn() } as unknown as AppRouter;
    await using _publisher = await render(<MonitorController router={router} />, { router: false });
    await expect.poll(() => frames.some((f) => f.type === "viewState")).toBe(true);
    const recreate = async () => {
      const next = new MonitorPeerStore(dispatch);
      peer = next;
      next.receive({ type: "reset", publisherGeneration: "generation" });
      await dispatch({ type: "ready" });
      await expect.poll(() => next.countsReady && next.viewState !== null).toBe(true);
      return next;
    };
    const initial = await recreate();
    const settle = {
      ...refOf(row),
      type: "settle" as const,
      actionId: "settle",
      commandId: CommandId.make("original-settle"),
      expectedUpdatedAt: row.updatedAt,
    };
    await act(() => initial.action(settle));
    await expect.poll(() => monitorStore.results.get(settle.actionId)?.status).toBe("accepted");
    ports.inputs = ports.inputs.map((input) => ({
      ...input,
      threads: [{ ...thread, settledAt: DateTime.makeUnsafe("2026-10-26T08:00:00.000Z") }],
    }));
    await act(() => {
      for (const listener of ports.listeners) listener();
    });
    const first = await recreate();
    expect(first.retainedRows.size).toBe(0);
    const restore = {
      ...refOf(row),
      type: "restore" as const,
      actionId: "original-restore",
      commandId: CommandId.make("restore-command"),
      settleCommandId: settle.commandId,
    };
    await act(() => first.action(restore));
    await expect.poll(() => monitorStore.results.get(restore.actionId)?.status).toBe("unknown");
    const second = await recreate();
    await expect.poll(() => second.actions.get(restore.actionId)?.type).toBe("restore");
    expect(second.actions.get(restore.actionId)).toEqual(restore);
    expect(second.retainedRows.size).toBe(0);
    expect(second.selection).toBeNull();
    const container = document.createElement("div");
    container.style.cssText = "position:relative;width:384px;height:704px";
    await using screen = await render(<MonitorUI store={second} />, { container, router: false });
    await screen.getByRole("button", { name: /^Open thread monitor:/ }).click();
    const reconcile = screen.getByRole("button", { name: "Reconcile", exact: true });
    await expect.element(reconcile).toBeVisible();
    ports.inputs = ports.inputs.map((input) => ({
      ...input,
      environment: { ...testEnvironment, transport: "disconnected", freshness: "cached" },
    }));
    await act(() => {
      for (const listener of ports.listeners) listener();
    });
    await expect.element(reconcile).toHaveAttribute("aria-disabled", "true");
    await reconcile.click({ force: true });
    expect(intents.filter((i) => i.type === "reconcile")).toHaveLength(0);
    ports.inputs = ports.inputs.map((input) => ({ ...input, environment: testEnvironment }));
    await act(() => {
      for (const listener of ports.listeners) listener();
    });
    await expect.element(reconcile).toHaveAttribute("aria-disabled", "false");
    await reconcile.click();
    await expect.poll(() => second.results.get(restore.actionId)?.status).toBe("accepted");
    expect(intents.filter((i) => i.type === "reconcile")).toEqual([
      expect.objectContaining({
        ...refOf(row),
        originalActionId: restore.actionId,
        originalCommandId: restore.commandId,
      }),
    ]);
    expect(intents.filter((i) => i.type === "restore")).toEqual([restore]);
    expect(intents.filter((i) => i.type === "selectDetail")).toEqual([]);
    expect(restoreObservedSettlement).toHaveBeenCalledTimes(2);
    expect(vi.mocked(restoreObservedSettlement).mock.calls).toEqual(
      Array.from({ length: 2 }, () => [
        { environmentId: row.environmentId, threadId: row.threadId },
        settle.commandId,
        { settleWasNoop: false },
      ]),
    );
    expect(ports.run.mock.calls.map((call) => call[1])).toEqual(["settle"]);
    expect(second.selection).toBeNull();
    const third = await recreate();
    await expect.poll(() => third.actions.get(restore.actionId)?.type).toBe("restore");
    expect(third.results.get(restore.actionId)?.status).toBe("accepted");
    expect(restoreObservedSettlement).toHaveBeenCalledTimes(2);
    frames.length = 0;
    await dispatch({ type: "requestSnapshot" });
    await expect
      .poll(
        () =>
          frames.some(
            (f) =>
              f.type === "actionResult" &&
              f.actionId === restore.actionId &&
              f.restoreIdentity === undefined,
          ) && frames.at(-1)?.type === "viewState",
      )
      .toBe(true);
    expect(
      frames.filter((f) => f.type === "actionResult").every((f) => !("restoreIdentity" in f)),
    ).toBe(true);
  });
});

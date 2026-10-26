import { CommandId, type EnvironmentId, MessageId } from "@t3tools/contracts";
import { describe, expect, it, vi, beforeEach } from "vite-plus/test";
import { AsyncResult } from "effect/reactivity";
import { makeThreadProjectionFixture } from "../../test-fixtures";
import type { OrchestrationV2ThreadShell } from "@t3tools/contracts";
const v2ThreadShell: OrchestrationV2ThreadShell = {
  ...makeThreadProjectionFixture().thread,
  latestRunId: null,
  activeRunId: null,
  status: "idle",
  pendingRuntimeRequest: null,
  latestVisibleMessage: null,
  latestUserMessageAt: null,
  hasActionableProposedPlan: false,
  itemCount: 0,
  visibleItemCount: 0,
};
import type { MonitorEnvironmentInput } from "@t3tools/client-runtime/jones/thread-monitor";
import type { ThreadMonitorPublisherFrame } from "@t3tools/contracts/jones/desktopThreadMonitor";
import type { AppRouter } from "../../router";
const ports = vi.hoisted(() => ({
  values: new Map<unknown, unknown>(),
  scope: true,
  run: vi.fn<(...args: unknown[]) => Promise<unknown>>(),
}));
vi.mock("../../rpc/atomRegistry", () => ({
  appAtomRegistry: { get: (key: unknown) => ports.values.get(key) },
}));
vi.mock("./inputs", () => ({
  monitorInputsAtom: "inputs",
  monitorInputFor: (inputs: ReadonlyArray<MonitorEnvironmentInput>, id: EnvironmentId) =>
    inputs.find((input) => input.environment.environmentId === id),
}));
vi.mock("../../state/session", () => ({ readEnvironmentScope: () => ports.scope }));
vi.mock("../../state/server", () => ({ environmentServerConfigsAtom: "configs" }));
vi.mock("../../state/shell", () => ({ environmentSnapshotAtom: () => "snapshot" }));
vi.mock("../../state/threads", () => ({
  threadEnvironment: {
    startTurn: "start",
    settle: "settle",
    respondToApproval: "approval",
    respondToUserInput: "input",
    setInteractionMode: "interaction",
  },
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
import { MonitorActionController } from "./actions";
import { MonitorStore, refOf, emptyCounts } from "./store";
import { testEnvironment, testRow } from "./fixtures.test-support";
function setup(active = false) {
  const store = new MonitorStore("generation");
  store.register(testEnvironment.environmentId);
  const row = testRow();
  store.setCohort([testEnvironment], [row], { ...emptyCounts(), waiting: 1 });
  const thread = {
    ...v2ThreadShell,
    id: row.threadId,
    status: active ? ("running" as const) : ("idle" as const),
  };
  const input: MonitorEnvironmentInput = {
    environment: testEnvironment,
    threads: [thread],
    projectLabels: new Map(),
    canOperate: true,
    canQueue: true,
  };
  ports.values.set("inputs", [input]);
  ports.values.set(
    "configs",
    new Map([
      [
        row.environmentId,
        {
          providers: [
            {
              instanceId: thread.providerInstanceId,
              enabled: true,
              installed: true,
              status: "ready",
              models: [{ slug: thread.modelSelection.model }],
              showInteractionModeToggle: true,
            },
          ],
        },
      ],
    ]),
  );
  const frames: ThreadMonitorPublisherFrame[] = [];
  const controller = new MonitorActionController(
    { navigate: vi.fn() } as unknown as AppRouter,
    async (frame) => {
      frames.push(frame);
    },
    store,
  );
  const action = {
    ...refOf(row),
    type: "send" as const,
    actionId: "send",
    commandId: CommandId.make("stable-command"),
    messageId: MessageId.make("stable-message"),
    draftRevision: 1,
    text: "actual draft",
  };
  return { store, controller, frames, action, input };
}
beforeEach(() => {
  ports.values.clear();
  ports.scope = true;
  ports.run.mockReset();
  ports.run.mockResolvedValue(AsyncResult.success(undefined));
});
describe("monitor uses the existing runtime command path", () => {
  it.each([false, true])(
    "dispatches safe queue mode with exact saved context when active=%s",
    async (active) => {
      const { controller, action, frames } = setup(active);
      await controller.dispatch(action);
      expect(ports.run).toHaveBeenCalledTimes(1);
      expect(ports.run.mock.calls[0]?.[1]).toBe("start");
      expect(ports.run.mock.calls[0]?.[2]).toMatchObject({
        environmentId: testEnvironment.environmentId,
        input: {
          commandId: "stable-command",
          threadId: "thread",
          message: { messageId: "stable-message", text: "actual draft", attachments: [] },
          dispatchMode: "queue",
          modelSelection: v2ThreadShell.modelSelection,
          runtimeMode: v2ThreadShell.runtimeMode,
          interactionMode: v2ThreadShell.interactionMode,
        },
      });
      expect(frames.at(-1)).toMatchObject({
        type: "actionResult",
        status: "accepted",
        commandId: "stable-command",
      });
      await controller.dispatch(action);
      expect(ports.run).toHaveBeenCalledTimes(1);
    },
  );
  it("checks current permission and fresh scope immediately before mutation", async () => {
    const { controller, action, frames } = setup();
    ports.scope = false;
    await controller.dispatch(action);
    expect(ports.run).not.toHaveBeenCalled();
    expect(frames.at(-1)).toMatchObject({ status: "unavailable" });
  });
  it("does not deliver a late accepted outcome to a removed or replacement epoch", async () => {
    const { controller, action, store, frames } = setup();
    let finish: ((result: unknown) => void) | undefined;
    let signalStarted: (() => void) | undefined;
    const started = new Promise<void>((resolve) => {
      signalStarted = resolve;
    });
    ports.run.mockImplementation(
      () =>
        new Promise((resolve) => {
          finish = resolve;
          signalStarted!();
        }),
    );
    const pending = controller.dispatch(action);
    await started;
    await store.retire(action.environmentId);
    store.observeCatalog(new Set());
    store.register(action.environmentId);
    finish!(AsyncResult.success(undefined));
    await pending;
    expect(
      frames.filter((frame) => frame.type === "actionResult" && frame.status === "accepted"),
    ).toHaveLength(0);
    expect(store.results.size).toBe(0);
  });
});

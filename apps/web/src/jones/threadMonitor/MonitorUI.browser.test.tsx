import { CommandId, RuntimeRequestId, ThreadId } from "@t3tools/contracts";
import { monitorThreadKey } from "@t3tools/client-runtime/jones/thread-monitor";
import type {
  ThreadMonitorPeerIntent,
  ThreadMonitorAction,
  ThreadMonitorNativeState,
} from "@t3tools/contracts/jones/desktopThreadMonitor";
import { describe, expect, it, vi } from "vite-plus/test";
import { userEvent } from "vite-plus/test/browser";
import { act, render, stubGlobal } from "../../../test/render";
import { MonitorUI } from "./MonitorUI";
import { MonitorToggle } from "./MonitorToggle";
import { readMonitorNativeState, setMonitorNativeState } from "./nativeState";
import { MonitorPeerStore } from "./peerStore";
import { monitorSnapshot } from "./snapshots";
import { refOf } from "./store";
import { testEnvironment, testRow } from "./fixtures.test-support";
import { emptyMonitorViewState } from "./viewState";
import "./monitor.css";
const NOW = Date.parse("2026-10-26T08:00:00.000Z");
function setup() {
  const intents: ThreadMonitorPeerIntent[] = [];
  const store = new MonitorPeerStore(async (intent) => {
    intents.push(intent);
  });
  store.receive({ type: "reset", publisherGeneration: "generation" });
  const rows = [
    testRow({
      group: "needsYou",
      status: "input",
      threadId: ThreadId.make("needs"),
      title: "Question thread",
    }),
    testRow({
      group: "working",
      status: "working",
      threadId: ThreadId.make("working"),
      title: "Working thread",
    }),
    testRow({ title: "Waiting thread" }),
    testRow({
      group: "done",
      status: "completed",
      threadId: ThreadId.make("done"),
      title: "Done thread",
      completedAt: new Date(NOW - 60_000).toISOString(),
    }),
  ];
  for (const frame of monitorSnapshot({
    publisherGeneration: "generation",
    revision: 1,
    environments: [testEnvironment],
    rows,
    counts: { needsYou: 1, working: 1, waiting: 1, done: 1 },
  }))
    store.receive(frame);
  const container = document.createElement("div");
  container.style.cssText = "position:relative;width:384px;height:704px";
  return { store, intents, rows, container };
}
async function expand(container: HTMLElement) {
  const notch = container.querySelector<HTMLButtonElement>(".notch")!;
  await userEvent.click(notch);
}
describe("real monitor renderer interactions", () => {
  it("reconciles an unknown rowless Restore after connection recovery without replaying a stale row", async () => {
    const { store: oldPeer, rows, container } = setup();
    const row = rows[2]!;
    const receipt = {
      ...refOf(row),
      type: "actionResult" as const,
      actionId: "settled-waiting",
      commandId: CommandId.make("original-settle"),
      settleCommandId: CommandId.make("original-settle"),
      status: "accepted" as const,
      reason: null,
    };
    oldPeer.receive(receipt);
    const intents: ThreadMonitorPeerIntent[] = [];
    const store = new MonitorPeerStore(async (intent) => {
      intents.push(intent);
      if (intent.type === "restore") throw new Error("Synthetic lost delivery");
    });
    store.receive({ type: "reset", publisherGeneration: "generation" });
    const replayView = (revision: number, connected: boolean) => {
      for (const frame of monitorSnapshot({
        publisherGeneration: "generation",
        revision,
        environments: [
          {
            ...testEnvironment,
            transport: connected ? "connected" : "disconnected",
            freshness: connected ? "live" : "cached",
          },
        ],
        rows: [],
        counts: { needsYou: 0, working: 0, waiting: 0, done: 0 },
      }))
        store.receive(frame);
    };
    replayView(1, false);
    for (const result of oldPeer.results.values()) store.receive(result);
    expect(store.retainedRows.size).toBe(0);
    await using screen = await render(<MonitorUI store={store} />, { container, router: false });
    await expand(container);
    const restore = screen.getByRole("button", { name: "Restore thread", exact: true });
    await expect.element(restore).toBeVisible();
    await expect.element(restore).toHaveAttribute("aria-disabled", "true");
    await restore.click({ force: true });
    expect(intents.filter((intent) => intent.type === "restore")).toHaveLength(0);
    await act(() => replayView(2, true));
    await expect.element(restore).toHaveAttribute("aria-disabled", "false");
    expect(container.querySelectorAll("[data-monitor-thread-id]")).toHaveLength(0);
    await restore.click();
    expect(intents.filter((intent) => intent.type === "restore")).toEqual([
      expect.objectContaining({
        ...refOf(row),
        type: "restore",
        settleCommandId: receipt.commandId,
      }),
    ]);
    const originalRestore = intents.find(
      (intent): intent is Extract<ThreadMonitorAction, { type: "restore" }> =>
        intent.type === "restore",
    )!;
    await expect.poll(() => store.results.get(originalRestore.actionId)?.status).toBe("unknown");
    await act(() => replayView(3, false));
    const reconcile = screen.getByRole("button", { name: "Reconcile", exact: true });
    await expect.element(reconcile).toBeVisible();
    await expect.element(reconcile).toHaveAttribute("aria-disabled", "true");
    await reconcile.click({ force: true });
    expect(intents.filter((intent) => intent.type === "reconcile")).toHaveLength(0);
    await expect.element(restore).toHaveAttribute("aria-disabled", "true");
    await restore.click({ force: true });
    expect(intents.filter((intent) => intent.type === "restore")).toHaveLength(1);
    await act(() => replayView(4, true));
    await expect.element(reconcile).toHaveAttribute("aria-disabled", "false");
    expect(store.retainedRows.size).toBe(0);
    await reconcile.click();
    const reconciliations = intents.filter((intent) => intent.type === "reconcile");
    expect(reconciliations).toEqual([
      expect.objectContaining({
        ...refOf(row),
        type: "reconcile",
        originalActionId: originalRestore.actionId,
        originalCommandId: originalRestore.commandId,
      }),
    ]);
    expect(store.selection).toBeNull();
    expect(intents.filter((intent) => intent.type === "restore")).toHaveLength(1);
    await act(() => {
      store.receive({ ...testEnvironment, type: "invalidateEnvironment", requestId: "retired" });
      store.receive(receipt);
      store.receive({
        ...refOf(originalRestore),
        type: "actionResult",
        actionId: originalRestore.actionId,
        commandId: originalRestore.commandId,
        status: "unknown",
        reason: "Late unknown result from retired lifetime",
      });
    });
    await expect.element(reconcile).not.toBeInTheDocument();
    await expect.element(restore).not.toBeInTheDocument();
    expect(store.results.size).toBe(0);
  });
  it("keeps the actual aggregate label and number text inside each tile at 320px and 200% text", async () => {
    using _now = vi.spyOn(Date, "now").mockReturnValue(NOW);
    const { store, container } = setup();
    const previousSize = document.documentElement.style.fontSize;
    await using _screen = await render(<MonitorUI store={store} />, { container, router: false });
    try {
      await expand(container);
      await act(() => container.querySelector<HTMLButtonElement>(".section-toggle")!.focus());
      for (const width of [320, 384]) {
        container.style.width = width + "px";
        for (const size of ["100%", "200%"]) {
          document.documentElement.style.fontSize = size;
          const tiles = [...container.querySelectorAll<HTMLElement>(".count-strip > span")];
          expect(tiles).toHaveLength(4);
          for (const tile of tiles) {
            const bounds = tile.getBoundingClientRect();
            for (const text of tile.querySelectorAll(".count-name, strong")) {
              const range = document.createRange();
              range.selectNodeContents(text);
              const rects = [...range.getClientRects()];
              expect(rects.length).toBeGreaterThan(0);
              for (const rect of rects) {
                expect(rect.left).toBeGreaterThanOrEqual(bounds.left - 1);
                expect(rect.right).toBeLessThanOrEqual(bounds.right + 1);
              }
            }
          }
        }
      }
    } finally {
      document.documentElement.style.fontSize = previousSize;
    }
  });

  it("retains the peer expansion and editor contract when Hide is requested with a focused draft", async () => {
    using _now = vi.spyOn(Date, "now").mockReturnValue(NOW);
    const { store, intents, rows, container } = setup();
    await using screen = await render(<MonitorUI store={store} />, { container, router: false });
    await expand(container);
    await act(() =>
      container.querySelector<HTMLButtonElement>('[data-monitor-thread-id="thread"]')!.focus(),
    );
    const editor = container.querySelector<HTMLTextAreaElement>("[data-monitor-reply] textarea")!;
    await userEvent.fill(editor, "draft through native hide");
    editor.setSelectionRange(2, 5);
    expect(document.activeElement).toBe(editor);
    await screen.getByRole("button", { name: "Hide thread monitor", exact: true }).click();
    expect(intents.filter((intent) => intent.type === "hide")).toHaveLength(1);
    expect(store.expanded).toBe(true);
    expect(container.querySelector("[data-thread-monitor]")?.getAttribute("data-expanded")).toBe(
      "true",
    );
    expect(container.querySelector("[data-monitor-reply] textarea")).toBe(editor);
    expect(editor.value).toBe("draft through native hide");
    expect(editor.selectionStart).toBe(2);
    expect(editor.selectionEnd).toBe(5);
    expect(store.draft(rows[2]!).text).toBe(editor.value);
  });

  it("replays focus and caret once, then preserves local editing when the completion expires", async () => {
    let now = NOW;
    using _now = vi.spyOn(Date, "now").mockImplementation(() => now);
    const { store, intents, rows, container } = setup();
    await using _screen = await render(<MonitorUI store={store} />, { container, router: false });
    const row = rows[3]!;
    const saved = {
      ...emptyMonitorViewState("generation"),
      revision: (store.viewState?.revision ?? 0) + 1,
      collapsed: { needsYou: true, working: false, waiting: false, done: false },
      editor: { ref: refOf(row), quick: true, focused: true, selectionStart: 2, selectionEnd: 5 },
    };
    await act(() => {
      store.edit({ ...store.draft(row), revision: 1, text: "restored reply" });
      store.receive({ type: "viewState", state: saved });
    });
    const editor = container.querySelector<HTMLTextAreaElement>("[data-monitor-reply] textarea")!;
    await expect.poll(() => document.activeElement).toBe(editor);
    expect(editor.selectionStart).toBe(2);
    expect(editor.selectionEnd).toBe(5);
    expect(
      container
        .querySelector('[data-monitor-section="needsYou"] .section-toggle')
        ?.getAttribute("aria-expanded"),
    ).toBe("false");
    const applyVersion = store.viewStateApplyVersion;
    const captures = () => intents.filter((intent) => intent.type === "setViewState").length;
    const capturesBeforeTyping = captures();
    await userEvent.fill(editor, "typing after replay");
    editor.setSelectionRange(4, 7);
    await act(() =>
      editor.dispatchEvent(new KeyboardEvent("keyup", { key: "ArrowRight", bubbles: true })),
    );
    expect(captures()).toBeGreaterThan(capturesBeforeTyping);
    const local = store.viewState!;
    const capturesBeforeEcho = captures();
    await act(() => store.receive({ type: "viewState", state: local }));
    expect(captures()).toBe(capturesBeforeEcho);
    await act(() => {
      now += 3_600_000;
      for (const frame of monitorSnapshot({
        publisherGeneration: "generation",
        revision: 2,
        environments: [testEnvironment],
        rows: rows.slice(0, 3),
        counts: { needsYou: 1, working: 1, waiting: 1, done: 0 },
      }))
        store.receive(frame);
    });
    expect(store.viewStateApplyVersion).toBe(applyVersion);
    expect(container.querySelector("[data-monitor-reply] textarea")).toBe(editor);
    expect(document.activeElement).toBe(editor);
    expect(editor.value).toBe("typing after replay");
    expect(editor.selectionStart).toBe(4);
    expect(editor.selectionEnd).toBe(7);
    expect(container.querySelector(".retained-editor")?.textContent).toContain(
      "outside the last hour",
    );
    expect(
      container.querySelector('[data-monitor-section="done"] .section-count')?.textContent,
    ).toBe("0");
    expect(
      container.querySelector('[data-monitor-section="done"] [data-monitor-thread-id="done"]'),
    ).toBeNull();
  });
  it("keeps four counts while all sections collapse and requests 50 more without a local eviction", async () => {
    using _now = vi.spyOn(Date, "now").mockReturnValue(NOW);
    const { store, intents, rows, container } = setup();
    await using _screen = await render(<MonitorUI store={store} />, { container, router: false });
    await expand(container);
    for (const button of container.querySelectorAll<HTMLButtonElement>(".section-toggle"))
      await userEvent.click(button);
    expect(
      [...container.querySelectorAll(".section-toggle")].every(
        (button) => button.getAttribute("aria-expanded") === "false",
      ),
    ).toBe(true);
    expect(container.querySelector("[data-monitor-counts]")?.textContent).toContain(
      "Needs You1Working1Waiting1Done1",
    );
    await act(() => {
      for (const frame of monitorSnapshot({
        publisherGeneration: "generation",
        revision: 2,
        environments: [testEnvironment],
        rows,
        counts: { needsYou: 51, working: 1, waiting: 1, done: 1 },
      }))
        store.receive(frame);
    });
    await userEvent.click(
      container.querySelector<HTMLButtonElement>(
        '[data-monitor-section="needsYou"] .section-toggle',
      )!,
    );
    await userEvent.click(
      container.querySelector<HTMLButtonElement>('[data-monitor-section="needsYou"] .show-more')!,
    );
    expect(intents).toContainEqual({ type: "requestMore", group: "needsYou" });
    expect(store.rows.size).toBe(4);
  });
  it("keeps the actual editor node, selection and newer draft through regroup, detail and acceptance", async () => {
    using _now = vi.spyOn(Date, "now").mockReturnValue(NOW);
    const { store, intents, rows, container } = setup();
    await using screen = await render(<MonitorUI store={store} />, { container, router: false });
    await expand(container);
    const open = container.querySelector<HTMLButtonElement>('[data-monitor-thread-id="thread"]')!;
    await act(() => open.focus());
    const editor = container.querySelector<HTMLTextAreaElement>("[data-monitor-reply] textarea")!;
    await userEvent.fill(editor, "first message");
    await userEvent.keyboard("{Enter}");
    const sent = intents.find(
      (intent): intent is Extract<ThreadMonitorAction, { type: "send" }> => intent.type === "send",
    )!;
    expect(sent.text).toBe("first message");
    expect(sent.messageId).toBeTruthy();
    expect(sent.commandId).toBeTruthy();
    await userEvent.fill(editor, "typed while awaiting acceptance");
    editor.setSelectionRange(5, 9);
    await act(() => {
      const moved = rows.map((row) =>
        row.threadId === "thread"
          ? { ...row, group: "working" as const, status: "working" as const }
          : row,
      );
      for (const frame of monitorSnapshot({
        publisherGeneration: "generation",
        revision: 2,
        environments: [testEnvironment],
        rows: moved,
        counts: { needsYou: 1, working: 2, waiting: 0, done: 1 },
      }))
        store.receive(frame);
      store.receive({
        ...refOf(sent),
        type: "actionResult",
        actionId: sent.actionId,
        commandId: sent.commandId,
        status: "accepted",
        reason: null,
        submittedDraftRevision: sent.draftRevision,
      });
    });
    expect(container.querySelector("[data-monitor-reply] textarea")).toBe(editor);
    expect(document.activeElement).toBe(editor);
    expect(editor.value).toBe("typed while awaiting acceptance");
    expect(editor.selectionStart).toBe(5);
    expect(editor.selectionEnd).toBe(9);
    const section = container.querySelector<HTMLButtonElement>(
      '[data-monitor-section="working"] .section-toggle',
    )!;
    await userEvent.click(section);
    expect(container.querySelector("[data-monitor-reply] textarea")).toBe(editor);
    expect(editor.value).toBe("typed while awaiting acceptance");
    expect(editor.selectionStart).toBe(5);
    expect(editor.selectionEnd).toBe(9);
    await userEvent.click(section);
    await userEvent.click(
      container.querySelector<HTMLButtonElement>('[data-monitor-thread-id="thread"]')!,
    );
    expect(container.querySelector("[data-monitor-reply] textarea")).toBe(editor);
    expect(editor.value).toBe("typed while awaiting acceptance");
    await screen.getByRole("button", { name: "Threads", exact: true }).click();
    expect(store.draft(rows[2]!).text).toBe("typed while awaiting acceptance");
  });
  it("does not send during IME composition or Shift+Enter, then sends one exact identity", async () => {
    using _now = vi.spyOn(Date, "now").mockReturnValue(NOW);
    const { store, intents, container } = setup();
    await using _screen = await render(<MonitorUI store={store} />, { container, router: false });
    await expand(container);
    await act(() =>
      container.querySelector<HTMLButtonElement>('[data-monitor-thread-id="done"]')!.focus(),
    );
    const editor = container.querySelector<HTMLTextAreaElement>("[data-monitor-reply] textarea")!;
    await userEvent.fill(editor, "composed reply");
    await act(() => {
      editor.dispatchEvent(new CompositionEvent("compositionstart", { bubbles: true }));
      editor.dispatchEvent(
        new KeyboardEvent("keydown", { key: "Enter", bubbles: true, isComposing: true }),
      );
      editor.dispatchEvent(new CompositionEvent("compositionend", { bubbles: true }));
      editor.dispatchEvent(
        new KeyboardEvent("keydown", { key: "Enter", shiftKey: true, bubbles: true }),
      );
    });
    expect(intents.filter((intent) => intent.type === "send")).toHaveLength(0);
    await userEvent.click(
      container.querySelector<HTMLButtonElement>("[data-monitor-reply] .send")!,
    );
    const sends = intents.filter((intent) => intent.type === "send");
    expect(sends).toHaveLength(1);
    expect(sends[0]).toMatchObject({ type: "send", threadId: "done", text: "composed reply" });
  });
  it("submits standard single-choice input rows immediately after the final answer", async () => {
    using _now = vi.spyOn(Date, "now").mockReturnValue(NOW);
    const { store, intents, rows, container } = setup();
    await using screen = await render(<MonitorUI store={store} />, { container, router: false });
    await expand(container);
    await act(() => {
      store.select(rows[0]!);
      store.receive({
        type: "detail",
        detail: {
          ...refOf(rows[0]!),
          selectionId: store.selection!.selectionId,
          revision: 1,
          entries: [],
          plan: null,
          unavailableReason: null,
          request: {
            kind: "user_input",
            requestId: RuntimeRequestId.make("quick-input"),
            availability: { available: true, reason: null },
            questions: ["one", "two"].map((id) => ({
              id,
              header: id,
              question: `Confirm ${id}`,
              allowCustomAnswer: true,
              multiSelect: false,
              options: [{ label: `Done ${id}`, value: id }],
            })),
          },
        },
      });
    });
    await screen.getByRole("textbox", { name: "Custom answer: one" }).fill("Verified workspace B");
    await screen.getByRole("radio", { name: "Done one" }).click();
    expect(intents.filter((intent) => intent.type === "respondToUserInput")).toHaveLength(0);
    await screen.getByRole("radio", { name: "Done two" }).click();
    expect(intents.filter((intent) => intent.type === "respondToUserInput")).toMatchObject([
      {
        requestId: "quick-input",
        answers: { one: "one\nNote: Verified workspace B", two: "two" },
        draftRevision: 3,
      },
    ]);
  });

  it("renders actual offered decisions/questions and purges visible private content on retirement", async () => {
    using _now = vi.spyOn(Date, "now").mockReturnValue(NOW);
    const { store, intents, rows, container } = setup();
    await using screen = await render(<MonitorUI store={store} />, { container, router: false });
    await expand(container);
    await act(() => {
      store.select(rows[0]!);
      store.receive({
        type: "detail",
        detail: {
          ...refOf(rows[0]!),
          selectionId: store.selection!.selectionId,
          revision: 1,
          entries: [
            { id: "private", role: "assistant", text: "private recent text", excerpt: false },
          ],
          request: {
            kind: "approval",
            requestId: RuntimeRequestId.make("actual-approval"),
            prompt: "Actual permission request",
            options: [
              { decision: "accept", label: "Offered once", warning: "Actual warning" },
              { decision: "decline", label: "Offered deny" },
              { decision: "acceptAlways", label: "Always" },
            ],
            availability: { available: true, reason: null },
          },
          plan: null,
          unavailableReason: null,
        },
      });
    });
    await screen.getByRole("button", { name: /Approve once/ }).click();
    expect(intents.find((intent) => intent.type === "respondToApproval")).toMatchObject({
      type: "respondToApproval",
      requestId: "actual-approval",
      decision: "accept",
      threadId: "needs",
    });
    expect(container.textContent).not.toContain("Always");
    await act(() =>
      store.receive({ ...testEnvironment, type: "invalidateEnvironment", requestId: "retired" }),
    );
    expect(container.textContent).not.toContain("private recent text");
    expect(
      container.querySelector<HTMLTextAreaElement>("[data-monitor-reply] textarea")?.value,
    ).toBe("");
    expect(store.selection).toBeNull();
    expect(store.rows.has(monitorThreadKey(rows[0]!))).toBe(false);
  });
});

describe("persistent monitor toolbar recovery", () => {
  const healthy: ThreadMonitorNativeState = {
    supported: true,
    enabled: true,
    visible: true,
    mainVisible: false,
    mainFocused: false,
    expanded: false,
    reason: null,
  };
  function preserveNativeState(): Disposable {
    const previous = readMonitorNativeState();
    return { [Symbol.dispose]: () => setMonitorNativeState(previous) };
  }

  it.each([
    "Thread monitor renderer stopped. Toggle the monitor to recover.",
    "Thread monitor assets could not load. Toggle the monitor to retry.",
  ])("retries an enabled but failed peer through the real toolbar: %s", async (reason) => {
    using _state = preserveNativeState();
    let finishRetry!: (state: ThreadMonitorNativeState) => void;
    const retry = new Promise<ThreadMonitorNativeState>((resolve) => {
      finishRetry = resolve;
    });
    const setEnabled = vi
      .fn<(enabled: boolean) => Promise<ThreadMonitorNativeState>>()
      .mockImplementationOnce(() => retry)
      .mockImplementation(async (enabled) => ({ ...healthy, enabled, visible: enabled }));
    using _bridge = stubGlobal("desktopBridge", { threadMonitor: { setEnabled } });
    await act(() => setMonitorNativeState({ ...healthy, visible: false, reason }));
    await using screen = await render(<MonitorToggle />, { router: false });
    const button = screen.container.querySelector<HTMLButtonElement>(
      "[data-thread-monitor-toggle]",
    )!;
    expect(button.disabled).toBe(false);
    expect(button.getAttribute("aria-label")).toBe("Retry thread monitor");
    expect(button.getAttribute("aria-description")).toBe(reason);
    expect(button.getAttribute("aria-pressed")).toBe("false");
    await userEvent.click(button);
    expect(setEnabled.mock.calls).toEqual([[true]]);
    expect(button.disabled).toBe(true);
    await act(() => button.click());
    expect(setEnabled).toHaveBeenCalledTimes(1);
    await act(() => finishRetry(healthy));
    expect(button.disabled).toBe(false);
    expect(button.getAttribute("aria-label")).toBe("Hide thread monitor");
    expect(button.getAttribute("aria-pressed")).toBe("true");
    expect(readMonitorNativeState()).toEqual(healthy);
    await userEvent.click(button);
    expect(setEnabled.mock.calls).toEqual([[true], [false]]);
    await expect.poll(() => readMonitorNativeState()?.enabled).toBe(false);
    expect(button.getAttribute("aria-label")).toBe("Show thread monitor");
  });

  it("keeps an unsupported Wayland backend disabled rather than offering peer recovery", async () => {
    using _state = preserveNativeState();
    const setEnabled = vi.fn();
    using _bridge = stubGlobal("desktopBridge", { threadMonitor: { setEnabled } });
    const reason = "Thread monitor needs an X11 desktop. Native Wayland is unavailable.";
    await act(() =>
      setMonitorNativeState({ ...healthy, supported: false, visible: false, reason }),
    );
    await using screen = await render(<MonitorToggle />, { router: false });
    const button = screen.container.querySelector<HTMLButtonElement>(
      "[data-thread-monitor-toggle]",
    )!;
    expect(button.disabled).toBe(true);
    expect(button.getAttribute("aria-description")).toBe(reason);
    await act(() => button.click());
    expect(setEnabled).not.toHaveBeenCalled();
  });
});

import { act, render as renderTest, type Screen } from "../../../test/render";
import "~/index.css";
import { page, userEvent } from "vite-plus/test/browser";
import { StrictMode } from "react";
import { describe, expect, it, vi } from "vite-plus/test";
import {
  EnvironmentId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationV2ThreadShell,
} from "@t3tools/contracts";
import { RegistryContext } from "@effect/atom-react";
import {
  createSettlementNotificationController,
  type SettlementEntry,
  type SettlementArrival,
  type SettlementOriginDetails,
} from "@t3tools/client-runtime/jones/settlement-notifications";
import { Atom } from "effect/reactivity";
import * as AtomRegistry from "effect/reactivity/AtomRegistry";
import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";
import type { EnvironmentShellState } from "@t3tools/client-runtime/state/shell";
import { createLocalStorageSettlementStore, SETTLEMENT_STORAGE_PREFIX } from "./localStorageStore";

const state = vi.hoisted(() => ({
  controller: null as ReturnType<typeof createSettlementNotificationController> | null,
  navigate: vi.fn(),
  environmentIds: [] as EnvironmentId[],
  shellAtom: null as Atom.Atom<EnvironmentShellState> | null,
  resolveOrigins:
    vi.fn<
      (arrivals: readonly SettlementArrival[]) => Promise<readonly SettlementOriginDetails[] | null>
    >(),
}));
vi.mock(import("./controller"), () => ({
  getSettlementNotificationController: () => state.controller!,
}));
vi.mock(
  import("../../state/environments"),
  () => ({ useEnvironmentIds: () => state.environmentIds }) as never,
);
vi.mock(
  import("../../state/shell"),
  () =>
    ({
      environmentShell: { stateValueAtom: () => state.shellAtom! },
    }) as never,
);
vi.mock(import("./resolveOrigins"), () => ({
  resolveSettlementOrigins: (_environmentId, arrivals) => state.resolveOrigins(arrivals),
}));
vi.mock(import("../../hooks/useThreadRouteTarget"), () => ({
  useThreadRouteTarget: () => null,
  useThreadRouteRef: () => null,
}));
vi.mock("@tanstack/react-router", async (original) => ({
  ...(await original<typeof import("@tanstack/react-router")>()),
  useNavigate: () => state.navigate,
}));

import { SettlementNotificationCoordinator } from "./SettlementNotificationCoordinator";
import { toastManager, ToastProvider } from "../../components/ui/toast";

const env = EnvironmentId.make("settlement-test-environment");

function thread(id: string, settledAtMs: number | null = null): OrchestrationV2ThreadShell {
  const threadId = ThreadId.make(id);
  const providerInstanceId = ProviderInstanceId.make("codex");
  return {
    id: threadId,
    projectId: ProjectId.make("settlement-test-project"),
    title: id,
    providerInstanceId,
    modelSelection: { instanceId: providerInstanceId, model: "gpt-5.4" },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { rootThreadId: threadId, parentThreadId: null, relationshipToParent: null },
    forkedFrom: null,
    createdBy: "user",
    creationSource: "web",
    latestRunId: null,
    activeRunId: null,
    status: "idle",
    pendingRuntimeRequest: null,
    latestVisibleMessage: null,
    latestUserMessageAt: null,
    hasActionableProposedPlan: false,
    itemCount: 0,
    visibleItemCount: 0,
    createdAt: DateTime.makeUnsafe(100),
    updatedAt: DateTime.makeUnsafe(settledAtMs ?? 100),
    archivedAt: null,
    settledOverride: settledAtMs === null ? null : "settled",
    settledAt: settledAtMs === null ? null : DateTime.makeUnsafe(settledAtMs),
    lastVisitedAt: null,
    deletedAt: null,
  };
}

function shell(
  sequence: number,
  threads: readonly OrchestrationV2ThreadShell[],
): EnvironmentShellState {
  return {
    status: "live",
    error: Option.none(),
    snapshot: Option.some({
      schemaVersion: 1,
      snapshotSequence: sequence,
      projects: [],
      threads,
      archivedThreads: [],
    }),
  };
}

class MemoryStorage implements Storage {
  private values = new Map<string, string>();
  get length() {
    return this.values.size;
  }
  clear() {
    this.values.clear();
  }
  getItem(key: string) {
    return this.values.get(key) ?? null;
  }
  key(index: number) {
    return [...this.values.keys()][index] ?? null;
  }
  removeItem(key: string) {
    this.values.delete(key);
  }
  setItem(key: string, value: string) {
    this.values.set(key, value);
  }
}
const entry = (id: string, environment = "env-a"): SettlementEntry => ({
  id: `${environment}:${id}:${id.charCodeAt(0)}`,
  environmentId: EnvironmentId.make(environment),
  threadId: ThreadId.make(id),
  title: `Thread ${id}`,
  settledAtMs: id.charCodeAt(0),
  detectedAtMs: 1,
  threadGone: false,
});
const inbox = (entries: SettlementEntry[], acknowledged: string[] = []) =>
  JSON.stringify({ version: 1, entries, acknowledged });
let storage: MemoryStorage;
let events: EventTarget;
let root: Screen;
let container: HTMLDivElement;
let mounted: boolean;
let registry: AtomRegistry.AtomRegistry;

async function render() {
  await act(async () => {
    await root.rerender(
      <StrictMode>
        <RegistryContext.Provider value={registry}>
          <ToastProvider>
            <SettlementNotificationCoordinator />
          </ToastProvider>
        </RegistryContext.Provider>
      </StrictMode>,
    );
  });
  mounted = true;
}
async function publish(entries: SettlementEntry[], acknowledged: string[] = []) {
  const otherTab = createLocalStorageSettlementStore(() => storage, events as unknown as Window);
  await act(async () => {
    await otherTab.saveInbox(inbox(entries, acknowledged));
    events.dispatchEvent(
      new StorageEvent("storage", { key: `${SETTLEMENT_STORAGE_PREFIX}entry.external` }),
    );
  });
}
function button(label: string) {
  const result = [...document.querySelectorAll<HTMLElement>("button, [role=button]")].find(
    (node) => node.getAttribute("aria-label") === label || node.textContent === label,
  );
  if (!result) throw new Error(`Missing button: ${label}`);
  return result;
}
const notices = () => document.querySelectorAll('[data-slot="toast-close"]');

async function setupTest(): Promise<AsyncDisposable> {
  let initialized = false;
  let ownedContainer: HTMLDivElement | undefined;
  async function cleanup() {
    const screen = initialized ? root : undefined;
    try {
      if (mounted) await act(async () => screen?.unmount());
      registry.dispose();
    } finally {
      try {
        await screen?.[Symbol.asyncDispose]();
      } finally {
        ownedContainer?.remove();
        vi.restoreAllMocks();
        vi.unstubAllGlobals();
      }
    }
  }
  try {
    registry = AtomRegistry.make();
    storage = new MemoryStorage();
    events = new EventTarget();
    state.navigate.mockReset();
    state.environmentIds = [];
    state.shellAtom = null;
    state.resolveOrigins.mockReset();
    const store = createLocalStorageSettlementStore(() => storage, events as unknown as Window);
    await store.saveInbox(inbox([entry("a")]));
    state.controller = createSettlementNotificationController({ store });
    ownedContainer = container = document.createElement("div");
    document.body.append(container);
    root = await renderTest(null, { container: container, router: false });
    initialized = true;
    mounted = false;

    return { [Symbol.asyncDispose]: cleanup };
  } catch (error) {
    await cleanup();
    throw error;
  }
}

describe("persistent settlement notification", () => {
  it("removes saved child notices and only shows parent and fork settlements after reload", async () => {
    await using _fixture = await setupTest();
    storage.clear();
    const store = createLocalStorageSettlementStore(() => storage, events as unknown as Window);
    const parent = thread("parent", 200);
    const children = ["mcp-child", "provider-child", "nested-child"].map((id, index) => ({
      ...thread(id, 300 + index),
      createdBy: "agent" as const,
      creationSource: index === 1 ? ("provider" as const) : ("mcp" as const),
      lineage: {
        rootThreadId: parent.id,
        parentThreadId: index === 2 ? ThreadId.make("mcp-child") : parent.id,
        relationshipToParent: "subagent" as const,
      },
    }));
    const saved = [parent, ...children].map((row) => ({
      ...entry(row.id, env),
      title: row.title,
    }));
    await store.saveInbox(inbox(saved));
    state.controller = createSettlementNotificationController({ store });
    state.environmentIds = [env];
    const shellAtom = Atom.make(shell(1, [parent, ...children]));
    state.shellAtom = shellAtom;
    state.resolveOrigins.mockImplementation(async (arrivals) =>
      arrivals.map(() => ({ origin: "other" as const })),
    );
    await render();
    await act(async () => {
      await state.controller!.flush();
    });
    expect(state.controller.getSnapshot().visible.map((row) => row.threadId)).toEqual([parent.id]);
    expect(button("parent")).toBeTruthy();
    for (const child of children) expect(document.body.textContent).not.toContain(child.title);

    const fork = {
      ...thread("fork", 400),
      createdAt: DateTime.makeUnsafe(400),
      lineage: { ...children[0]!.lineage, relationshipToParent: "fork" as const },
    };
    await act(async () => {
      registry.set(shellAtom, shell(2, [parent, ...children, fork]));
    });
    await act(async () => {
      await state.controller!.flush();
    });
    await act(async () => root.unmount());
    mounted = false;
    state.controller = createSettlementNotificationController({ store });
    root = await renderTest(null, { container: container, router: false });
    await render();
    await act(async () => {
      await state.controller!.flush();
    });
    await act(async () => button("Show settled threads").click());
    expect(button("parent")).toBeTruthy();
    await act(async () => button("fork").click());
    expect(state.navigate).toHaveBeenLastCalledWith({
      to: "/$environmentId/$threadId",
      params: { environmentId: env, threadId: fork.id },
    });
    for (const child of children) expect(document.body.textContent).not.toContain(child.title);
    const persisted = JSON.parse((await store.loadInbox())!);
    expect(persisted.acknowledged).toEqual(
      saved
        .filter((row) => row.threadId !== parent.id)
        .map((row) => row.id)
        .sort(),
    );
    expect(state.controller.getSnapshot().visible.map((row) => row.threadId)).toEqual([
      fork.id,
      parent.id,
    ]);
  });

  it.each([375, 1280])("keeps 45 settled threads in one scroll area at %ipx", async (width) => {
    await using _fixture = await setupTest();
    await page.viewport(width, 800);
    const entries = Array.from({ length: 45 }, (_, index) => ({
      ...entry(`thread-${index}`),
      title: index === 0 ? "x".repeat(200) : `Settled thread ${index}`,
    }));
    storage.clear();
    await publish(entries);
    await render();
    await act(async () => button("Show settled threads").click());
    const first = button(entries[0]!.title);
    const list = first.closest("ul")!;
    const content = list.parentElement!;
    const elements = [content, ...content.querySelectorAll<HTMLElement>("*")];
    const scrollers = elements.filter(
      (node) =>
        /auto|scroll/.test(getComputedStyle(node).overflowY) &&
        node.scrollHeight > node.clientHeight + 1,
    );
    expect(scrollers).toHaveLength(1);
    for (const node of elements.filter((node) => node.clientWidth > 0)) {
      expect(node.scrollWidth).toBeLessThanOrEqual(node.clientWidth + 1);
    }
    const scroller = scrollers[0]!;
    scroller.scrollTop = scroller.scrollHeight;
    const last = list.lastElementChild!.querySelector("button")!;
    const lastEntry = entries.find((entry) => entry.title === last.textContent)!;
    expect(last.getBoundingClientRect().bottom).toBeLessThanOrEqual(
      scroller.getBoundingClientRect().bottom + 1,
    );
    expect(last.getBoundingClientRect().top).toBeGreaterThanOrEqual(
      scroller.getBoundingClientRect().top - 1,
    );
    await act(async () => last.click());
    expect(state.navigate).toHaveBeenLastCalledWith({
      to: "/$environmentId/$threadId",
      params: { environmentId: lastEntry.environmentId, threadId: lastEntry.threadId },
    });
    expect(notices()).toHaveLength(1);
    await act(async () => button("Dismiss notification").click());
    await expect.poll(() => state.controller!.getSnapshot().visible).toHaveLength(0);
  });

  it.each([375, 1280])("wraps a single unbroken settled title at %ipx", async (width) => {
    await using _fixture = await setupTest();
    await page.viewport(width, 800);
    const longEntry = {
      ...entry("long"),
      title: "x".repeat(200),
      projectRemaining: Array.from({ length: 64 }, (_, index) => `Remaining work ${index}`),
    };
    storage.clear();
    await publish([longEntry]);
    await render();
    const title = button(longEntry.title);
    const description = title.closest('[data-slot="toast-description"]')!;
    const list = title.closest("ul")!;
    expect(list.scrollHeight).toBeGreaterThan(list.clientHeight);
    expect(list.textContent).toContain("Remaining work 63");
    for (const node of [description, ...description.querySelectorAll<HTMLElement>("*")]) {
      if (node.clientWidth > 0 && node.getAttribute("data-slot") !== "settlement-remaining-item") {
        expect(node.scrollWidth).toBeLessThanOrEqual(node.clientWidth + 1);
      }
    }
    expect(title.getBoundingClientRect().height).toBeGreaterThan(
      Number.parseFloat(getComputedStyle(title).lineHeight),
    );
  });

  it("keeps row collapse through navigation, aggregate updates and panel reopening", async () => {
    await using _fixture = await setupTest();
    storage.clear();
    await publish([{ ...entry("a"), projectRemaining: ["First task"] }]);
    await render();
    const row = () => button("Thread a").closest<HTMLElement>('[data-slot="settlement-row"]')!;
    await act(async () => button("Thread a").click());
    expect(row().textContent).toContain("First task");
    const selection = window.getSelection()!;
    const range = document.createRange();
    range.selectNodeContents(row().querySelector('[data-slot="settlement-remaining-item"]')!);
    selection.removeAllRanges();
    selection.addRange(range);
    await act(async () => row().click());
    expect(row().textContent).toContain("First task");
    selection.removeAllRanges();
    await act(async () => row().click());
    expect(row().textContent).not.toContain("First task");
    await publish([{ ...entry("b"), projectRemaining: ["Second task"] }]);
    await act(async () => button("Show settled threads").click());
    expect(row().textContent).not.toContain("First task");
    expect(document.body.textContent).toContain("Second task");
    await act(async () => button("Hide settled threads").click());
    await act(async () => button("Show settled threads").click());
    expect(row().textContent).not.toContain("First task");
    button("Show remaining work for Thread a").focus();
    await userEvent.keyboard("{Enter}");
    expect(row().textContent).toContain("First task");
    await userEvent.keyboard(" ");
    expect(row().textContent).not.toContain("First task");
    await publish([entry("c")]);
    expect(row().textContent).not.toContain("First task");
    expect(
      button("Thread c").closest('[data-slot="settlement-row"]')!.querySelector("[aria-expanded]"),
    ).toBeNull();
  });

  it("retains a row collapsed in the aggregate when reopened without an inbox update", async () => {
    await using _fixture = await setupTest();
    storage.clear();
    await publish([
      { ...entry("a"), projectRemaining: ["First task"] },
      { ...entry("b"), projectRemaining: ["Second task"] },
    ]);
    await render();
    await act(async () => button("Show settled threads").click());
    const row = () => button("Thread a").closest<HTMLElement>('[data-slot="settlement-row"]')!;
    await act(async () => row().click());
    expect(row().textContent).not.toContain("First task");
    expect(document.body.textContent).toContain("Second task");
    await act(async () => button("Hide settled threads").click());
    await act(async () => button("Show settled threads").click());
    expect(row().textContent).not.toContain("First task");
    expect(button("Show remaining work for Thread a").getAttribute("aria-expanded")).toBe("false");
    expect(document.body.textContent).toContain("Second task");
  });

  it.each([375, 1280])(
    "fits aggregate width and truncates full remaining text at %ipx",
    async (width) => {
      await using _fixture = await setupTest();
      await page.viewport(width, 800);
      storage.clear();
      await publish([entry("a"), entry("b")]);
      await render();
      const toast = () =>
        document.querySelector<HTMLElement>('[data-slot="toast-close"]')!.parentElement!
          .parentElement!;
      const initialWidth = toast().getBoundingClientRect().width;
      await act(async () => button("Show settled threads").click());
      expect(toast().getBoundingClientRect().width).toBeCloseTo(initialWidth, 0);
      await publish([
        { ...entry("medium"), title: "Medium length settled thread title with several words here" },
      ]);
      const mediumWidth = toast().getBoundingClientRect().width;
      if (width === 1280) {
        expect(mediumWidth).toBeGreaterThan(initialWidth);
        expect(mediumWidth).toBeLessThan(720);
      }
      const note = "Full remaining work detail ".repeat(60);
      await publish([{ ...entry("long"), title: "x".repeat(200), projectRemaining: [note] }]);
      const span = document.querySelector<HTMLElement>('[data-slot="settlement-remaining-item"]')!;
      expect(span.textContent).toBe(note);
      await userEvent.hover(span);
      await expect
        .poll(() => document.querySelector('[data-slot="tooltip-popup"]')?.textContent)
        .toBe(note);
      await userEvent.unhover(span);
      expect(span.scrollWidth).toBeGreaterThan(span.clientWidth);
      expect(span.getBoundingClientRect().height).toBeCloseTo(
        Number.parseFloat(getComputedStyle(span).lineHeight),
        0,
      );
      expect(toast().getBoundingClientRect().width).toBeCloseTo(width === 1280 ? 720 : 343, 0);
      await act(async () => button("Hide settled threads").click());
      expect(toast().getBoundingClientRect().width).toBeCloseTo(initialWidth, 0);
    },
  );

  it("keeps one toast through StrictMode, title navigation and an expanded aggregate update", async () => {
    await using _fixture = await setupTest();
    const add = vi.spyOn(toastManager, "add");
    await render();
    expect(notices()).toHaveLength(1);
    await act(async () => button("Thread a").click());
    expect(state.navigate).toHaveBeenCalledWith({
      to: "/$environmentId/$threadId",
      params: { environmentId: "env-a", threadId: "a" },
    });
    expect(notices()).toHaveLength(1);
    await publish([entry("b", "env-b")]);
    await act(async () => button("Show settled threads").click());
    expect(button("Thread a")).toBeTruthy();
    expect(button("Thread b")).toBeTruthy();
    await publish([entry("c")]);
    expect(button("Hide settled threads").getAttribute("aria-expanded")).toBe("true");
    expect(button("Thread c")).toBeTruthy();
    expect(notices()).toHaveLength(1);
    expect(add).toHaveBeenCalledTimes(1);
    await act(async () => button("Thread b").click());
    expect(state.navigate).toHaveBeenLastCalledWith({
      to: "/$environmentId/$threadId",
      params: { environmentId: "env-b", threadId: "b" },
    });
  });

  it("keeps the notification after Escape inside it and focus changes", async () => {
    await using _fixture = await setupTest();
    await render();
    await act(async () => {
      const title = button("Thread a");
      title.focus();
      title.dispatchEvent(
        new KeyboardEvent("keydown", { key: "Escape", bubbles: true, cancelable: true }),
      );
      window.dispatchEvent(new Event("blur"));
      window.dispatchEvent(new Event("focus"));
    });
    expect(notices()).toHaveLength(1);
    expect(state.controller!.getSnapshot().visible).toHaveLength(1);
  });

  it("dismisses only shown entries, persists the acknowledgement, and retains an unseen arrival", async () => {
    await using _fixture = await setupTest();
    await render();
    const other = createLocalStorageSettlementStore(() => storage, events as unknown as Window);
    await other.saveInbox(inbox([entry("b")]));
    await act(async () => button("Dismiss notification").click());
    const saved = JSON.parse((await other.loadInbox())!);
    expect(saved.acknowledged).toContain(entry("a").id);
    expect(saved.acknowledged).not.toContain(entry("b").id);
    await act(async () => root.unmount());
    mounted = false;
    state.controller = createSettlementNotificationController({ store: other });
    root = await renderTest(null, { container: container, router: false });
    await render();
    await expect.poll(() => document.body.textContent).toContain("Thread b");
    expect(document.body.textContent).not.toContain("Thread a");
  });

  it("recreates externally removed toasts without acknowledging entries", async () => {
    await using _fixture = await setupTest();
    const add = vi.spyOn(toastManager, "add");
    await render();
    const id = add.mock.results[0]!.value as string;
    await act(async () => toastManager.close(id));
    await expect.poll(() => add.mock.calls.length).toBe(2);
    expect(state.controller!.getSnapshot().visible).toHaveLength(1);
    expect(button("Thread a")).toBeTruthy();
  });

  it("mirrors another tab's acknowledgement and restores undismissed entries after remount", async () => {
    await using _fixture = await setupTest();
    await render();
    await act(async () => root.unmount());
    mounted = false;
    root = await renderTest(null, { container: container, router: false });
    await render();
    expect(button("Thread a")).toBeTruthy();
    await publish([], [entry("a").id]);
    await expect.poll(() => notices().length).toBe(0);
    expect(state.controller!.getSnapshot().visible).toHaveLength(0);
  });

  it("filters manual UI settlement while retaining an agent notice through navigation, remount and explicit X", async () => {
    await using _fixture = await setupTest();
    storage.clear();
    const store = createLocalStorageSettlementStore(() => storage, events as unknown as Window);
    state.controller = createSettlementNotificationController({ store });
    state.environmentIds = [env];
    const shellAtom = Atom.make(shell(1, [thread("manual"), thread("agent")]));
    state.shellAtom = shellAtom;
    state.resolveOrigins.mockImplementation(async (arrivals) =>
      arrivals.map((arrival) =>
        arrival.threadId === "manual"
          ? { origin: "client" }
          : {
              origin: "other",
              projectRemaining: ["Review <script> literally", "Deliver the separate project slice"],
            },
      ),
    );
    await render();
    await act(async () => {
      await state.controller!.flush();
    });
    expect(state.controller.getSnapshot().visible).toEqual([]);
    expect(state.resolveOrigins).not.toHaveBeenCalled();
    await act(async () => {
      registry.set(shellAtom, shell(2, [thread("manual", 200), thread("agent", 300)]));
    });
    await act(async () => {
      await state.controller!.flush();
    });
    await expect.poll(() => notices().length).toBe(1);
    expect(state.resolveOrigins).toHaveBeenCalledTimes(1);
    expect(state.controller.getSnapshot().visible.map((value) => value.threadId)).toEqual([
      "agent",
    ]);
    expect(document.body.textContent).not.toContain("manual");
    await act(async () => button("agent").click());
    expect(state.navigate).toHaveBeenCalledWith({
      to: "/$environmentId/$threadId",
      params: { environmentId: env, threadId: "agent" },
    });
    expect(notices()).toHaveLength(1);
    expect(document.body.textContent).toContain("Deliver the separate project slice");
    await act(async () => root.unmount());
    mounted = false;
    state.controller = createSettlementNotificationController({ store });
    root = await renderTest(null, { container: container, router: false });
    await render();
    await expect.poll(() => notices().length).toBe(1);
    expect(button("agent")).toBeTruthy();
    expect(document.body.textContent).toContain("Review <script> literally");
    expect(document.body.textContent).toContain("Deliver the separate project slice");
    expect(button("agent").closest("ul")!.querySelector("script")).toBeNull();
    expect(document.body.textContent).not.toContain("manual");
    await act(async () => button("Dismiss notification").click());
    await act(async () => {
      await state.controller!.flush();
    });
    expect(state.controller.getSnapshot().visible).toEqual([]);
    const saved = JSON.parse((await store.loadInbox())!);
    expect(saved.entries.map((value: SettlementEntry) => value.threadId)).toEqual(["agent"]);
    expect(saved.acknowledged).toEqual([`${env}:agent:300`]);
  });
});

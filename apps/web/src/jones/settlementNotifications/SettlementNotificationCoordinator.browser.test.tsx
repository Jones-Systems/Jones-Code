import { act, StrictMode } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { RegistryContext } from "@effect/atom-react";
import {
  createSettlementNotificationController,
  type SettlementEntry,
  type SettlementOriginResolver,
} from "@t3tools/client-runtime/jones/settlement-notifications";
import { Atom } from "effect/reactivity";
import * as AtomRegistry from "effect/reactivity/AtomRegistry";
import type { EnvironmentShellState } from "@t3tools/client-runtime/state/shell";
import {
  env,
  shell,
  thread,
} from "../../../../../packages/client-runtime/src/jones/settlementNotifications/testFixtures";
import { createLocalStorageSettlementStore, SETTLEMENT_STORAGE_PREFIX } from "./localStorageStore";

const state = vi.hoisted(() => ({
  controller: null as ReturnType<typeof createSettlementNotificationController> | null,
  navigate: vi.fn(),
  environmentIds: [] as EnvironmentId[],
  shellAtom: null as Atom.Atom<EnvironmentShellState> | null,
  resolveOrigins: vi.fn<SettlementOriginResolver>(),
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
let root: Root;
let container: HTMLDivElement;
let mounted: boolean;
let registry: AtomRegistry.AtomRegistry;

async function render() {
  await act(async () => {
    root.render(
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

beforeEach(async () => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
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
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
  mounted = false;
});
afterEach(async () => {
  if (mounted) await act(async () => root.unmount());
  registry.dispose();
  container.remove();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe("persistent settlement notification", () => {
  it("keeps one toast through StrictMode, title navigation and an expanded aggregate update", async () => {
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
    root = createRoot(container);
    await render();
    await expect.poll(() => document.body.textContent).toContain("Thread b");
    expect(document.body.textContent).not.toContain("Thread a");
  });

  it("recreates externally removed toasts without acknowledging entries", async () => {
    const add = vi.spyOn(toastManager, "add");
    await render();
    const id = add.mock.results[0]!.value as string;
    await act(async () => toastManager.close(id));
    await expect.poll(() => add.mock.calls.length).toBe(2);
    expect(state.controller!.getSnapshot().visible).toHaveLength(1);
    expect(button("Thread a")).toBeTruthy();
  });

  it("mirrors another tab's acknowledgement and restores undismissed entries after remount", async () => {
    await render();
    await act(async () => root.unmount());
    mounted = false;
    root = createRoot(container);
    await render();
    expect(button("Thread a")).toBeTruthy();
    await publish([], [entry("a").id]);
    await expect.poll(() => notices().length).toBe(0);
    expect(state.controller!.getSnapshot().visible).toHaveLength(0);
  });

  it("filters manual UI settlement while retaining an agent notice through navigation, remount and explicit X", async () => {
    storage.clear();
    const store = createLocalStorageSettlementStore(() => storage, events as unknown as Window);
    state.controller = createSettlementNotificationController({ store });
    state.environmentIds = [env];
    const shellAtom = Atom.make(shell(1, [thread("manual"), thread("agent")]));
    state.shellAtom = shellAtom;
    state.resolveOrigins.mockImplementation(async (arrivals) =>
      arrivals.map((arrival) => (arrival.threadId === "manual" ? "client" : "other")),
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
    await act(async () => root.unmount());
    mounted = false;
    state.controller = createSettlementNotificationController({ store });
    root = createRoot(container);
    await render();
    await expect.poll(() => notices().length).toBe(1);
    expect(button("agent")).toBeTruthy();
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

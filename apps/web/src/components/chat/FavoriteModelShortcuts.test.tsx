// @vitest-environment jsdom
import { act, type ComponentProps } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import { ProviderDriverKind, ProviderInstanceId } from "@t3tools/contracts";
import { DEFAULT_UNIFIED_SETTINGS } from "@t3tools/contracts/settings";
import { deriveProviderInstanceEntries } from "../../providerInstances";
import { FavoriteModelShortcuts } from "./FavoriteModelShortcuts";

const client = vi.hoisted(() => ({ favorites: [] as { provider: string; model: string }[] }));
vi.mock("~/hooks/useSettings", () => ({
  useClientSettings: (select: (settings: typeof client) => unknown) => select(client),
}));

const instanceId = ProviderInstanceId.make("codex_work");
const driver = ProviderDriverKind.make("codex");
const entries = deriveProviderInstanceEntries([
  {
    instanceId,
    driver,
    displayName: "Work",
    enabled: true,
    installed: true,
    status: "ready",
    version: null,
    auth: { status: "authenticated" },
    checkedAt: "2026-09-29T00:00:00.000Z",
    models: [
      { slug: "first", name: "First model", isCustom: false, capabilities: {} },
      { slug: "second", name: "Second model", isCustom: false, capabilities: {} },
      { slug: "third", name: "Third model", isCustom: false, capabilities: {} },
    ],
    slashCommands: [],
    skills: [],
  },
]);

let container: HTMLDivElement;
let root: Root;
let resize: () => void;
let rows: number;
let disconnect: ReturnType<typeof vi.fn>;
let onSelect = vi.fn<(instanceId: ProviderInstanceId, model: string) => void>();

function render(overrides: Partial<ComponentProps<typeof FavoriteModelShortcuts>> = {}) {
  act(() =>
    root.render(
      <FavoriteModelShortcuts
        instanceEntries={entries}
        settings={DEFAULT_UNIFIED_SETTINGS}
        activeInstanceId={instanceId}
        model="first"
        selectedModels={null}
        lockedProvider={null}
        lockedContinuationGroupKey={null}
        disabled={false}
        getModelDisabledReason={() => null}
        onSelect={onSelect}
        {...overrides}
      />,
    ),
  );
}

beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  rows = 1;
  disconnect = vi.fn();
  onSelect = vi.fn();
  client.favorites = ["first", "second", "third"].map((model) => ({ provider: instanceId, model }));
  vi.stubGlobal(
    "ResizeObserver",
    class {
      constructor(callback: () => void) {
        resize = callback;
      }
      observe() {}
      disconnect = disconnect;
    },
  );
  vi.spyOn(HTMLElement.prototype, "offsetHeight", "get").mockImplementation(() => rows * 32);
  vi.spyOn(HTMLElement.prototype, "offsetTop", "get").mockImplementation(function (
    this: HTMLElement,
  ) {
    const index = Array.from(this.parentElement?.children ?? []).indexOf(this);
    return Math.min(index, rows - 1) * 32;
  });
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
});

afterEach(() => {
  act(() => root.unmount());
  container.remove();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe("favorite model shortcuts", () => {
  it("switches the exact provider instance and updates the selected model", () => {
    render();
    const buttons = container.querySelectorAll("button");
    expect(buttons[0]?.getAttribute("aria-pressed")).toBe("true");
    act(() => buttons[1]?.click());
    expect(onSelect).toHaveBeenCalledWith(instanceId, "second");
    render({ model: "second" });
    expect(buttons[0]?.getAttribute("aria-pressed")).toBe("false");
    expect(buttons[1]?.getAttribute("aria-pressed")).toBe("true");
  });

  it.each([
    { disabled: true },
    { lockedProvider: ProviderDriverKind.make("claudeAgent") },
    { lockedProvider: driver, lockedContinuationGroupKey: "other-account" },
    { getModelDisabledReason: () => "Start a new thread." },
    { instanceEntries: entries.map((entry) => ({ ...entry, enabled: false })) },
  ])("prevents selection when restricted: %j", (overrides) => {
    render(overrides);
    const buttons = Array.from(container.querySelectorAll("button"));
    expect(buttons.every((button) => button.disabled)).toBe(true);
    act(() => buttons.forEach((button) => button.click()));
    expect(onSelect).not.toHaveBeenCalled();
  });

  it("renders no group without favorites or when stored favorites no longer resolve", () => {
    client.favorites = [];
    render();
    expect(container.childElementCount).toBe(0);
    client.favorites = [{ provider: "removed-instance", model: "first" }];
    render();
    expect(container.childElementCount).toBe(0);
    client.favorites = [{ provider: instanceId, model: "removed-model" }];
    render();
    expect(container.childElementCount).toBe(0);
  });

  it("keeps two rows visible, hides three rows without oscillation, and restores on resize", () => {
    rows = 2;
    render();
    const group = container.querySelector<HTMLElement>('[role="group"]')!;
    expect(group.style.visibility).toBe("visible");
    expect(group.parentElement?.style.height).toBe("64px");
    rows = 3;
    act(() => resize());
    expect(group.style.visibility).toBe("hidden");
    expect(group.hasAttribute("inert")).toBe(true);
    expect(group.parentElement?.style.height).toBe("0px");
    act(() => resize());
    expect(group.style.visibility).toBe("hidden");
    expect(group.children).toHaveLength(3);
    rows = 1;
    act(() => resize());
    expect(group.style.visibility).toBe("visible");
    expect(group.hasAttribute("inert")).toBe(false);
    expect(group.parentElement?.style.height).toBe("32px");
  });

  it("observes favorites added after mount and disconnects observation when removed", () => {
    client.favorites = [];
    render();
    client.favorites = [{ provider: instanceId, model: "first" }];
    render();
    expect(container.querySelector<HTMLElement>('[role="group"]')?.style.visibility).toBe(
      "visible",
    );
    client.favorites = [];
    render();
    expect(disconnect).toHaveBeenCalledOnce();
    expect(container.childElementCount).toBe(0);
  });
});

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
  onSelect = vi.fn();
  client.favorites = ["first", "second", "third"].map((model) => ({ provider: instanceId, model }));
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

describe("provider account shortcuts", () => {
  it("keeps an enabled provider account reachable without favorite models", () => {
    client.favorites = [];
    render();
    const button = container.querySelector<HTMLButtonElement>(
      'button[aria-label="Work: First model"]',
    );
    expect(button).not.toBeNull();
    act(() => button?.click());
    expect(onSelect).toHaveBeenCalledWith(instanceId, "first");
  });
  it("shows one cell per account rather than one per favorite model", () => {
    render();
    expect(container.querySelectorAll("button")).toHaveLength(1);
    expect(container.querySelector("button")?.getAttribute("aria-pressed")).toBe("true");
    expect(container.querySelector("button")?.textContent).toBe("WO");
  });

  it.each([
    { disabled: true },
    { lockedProvider: ProviderDriverKind.make("claudeAgent") },
    { lockedProvider: driver, lockedContinuationGroupKey: "other-account" },
    { getModelDisabledReason: () => "Start a new thread." },
    { instanceEntries: entries.map((entry) => ({ ...entry, status: "error" as const })) },
  ])("prevents selection when restricted: %j", (overrides) => {
    render(overrides);
    const button = container.querySelector<HTMLButtonElement>("button")!;
    expect(button.disabled).toBe(true);
    act(() => button.click());
    expect(onSelect).not.toHaveBeenCalled();
  });

  it("omits settings-disabled accounts and retains unavailable accounts with an explanation", () => {
    const disabled = { ...entries[0]!, enabled: false };
    const unavailable = {
      ...entries[0]!,
      instanceId: ProviderInstanceId.make("codex_offline"),
      isAvailable: false,
    };
    render({ instanceEntries: [disabled, unavailable] });
    const buttons = container.querySelectorAll<HTMLButtonElement>("button");
    expect(buttons).toHaveLength(1);
    expect(buttons[0]?.disabled).toBe(true);
    expect(buttons[0]?.title).toContain("Work");
    expect(buttons[0]?.title).toContain("No models are available");
  });

  it("keeps nine accounts and hides the entire group above the cap", () => {
    const many = Array.from({ length: 10 }, (_, index) => ({
      ...entries[0]!,
      instanceId: ProviderInstanceId.make(`codex_${index}`),
      displayName: `Account ${index}`,
    }));
    render({ instanceEntries: many.slice(0, 9) });
    expect(container.querySelectorAll("button")).toHaveLength(9);
    render({ instanceEntries: many });
    expect(container.childElementCount).toBe(0);
  });

  it("keeps hidden measurements inert and refuses clicks until restored", () => {
    render({ visible: false });
    const group = container.querySelector<HTMLElement>('[role="group"]')!;
    const button = container.querySelector<HTMLButtonElement>("button")!;
    expect(group.hasAttribute("inert")).toBe(true);
    expect(button.tabIndex).toBe(-1);
    act(() => button.click());
    expect(onSelect).not.toHaveBeenCalled();
    render({ visible: true });
    act(() => button.click());
    expect(onSelect).toHaveBeenCalledWith(instanceId, "first");
  });

  it("hides measurements and selection in multiple-model mode", () => {
    render({ selectedModels: [{ instanceId, model: "first" }] });
    expect(container.querySelector<HTMLElement>('[role="group"]')?.hasAttribute("inert")).toBe(
      true,
    );
    act(() => container.querySelector<HTMLButtonElement>("button")?.click());
    expect(onSelect).not.toHaveBeenCalled();
  });

  it("uses the existing exact-instance draft memory and updates account selection on rerender", () => {
    const otherId = ProviderInstanceId.make("codex_personal");
    const other = { ...entries[0]!, instanceId: otherId, displayName: "Personal" };
    render({
      instanceEntries: [...entries, other],
      rememberedSelections: { [otherId]: { instanceId: otherId, model: "second" } },
    });
    const button = container.querySelector<HTMLButtonElement>(
      'button[aria-label="Personal: Second model"]',
    )!;
    act(() => button.click());
    expect(onSelect).toHaveBeenCalledWith(otherId, "second");
    render({ instanceEntries: [...entries, other], activeInstanceId: otherId, model: "second" });
    expect(button.getAttribute("aria-pressed")).toBe("true");
  });

  it("keeps Antigravity selection on the session account without continuation metadata", () => {
    const antigravity = ProviderDriverKind.make("antigravity");
    const activeId = ProviderInstanceId.make("antigravity_work");
    const siblingId = ProviderInstanceId.make("antigravity_personal");
    const accountEntries = deriveProviderInstanceEntries([
      { ...entries[0]!.snapshot, driver: antigravity, instanceId: activeId, displayName: "Work" },
      {
        ...entries[0]!.snapshot,
        driver: antigravity,
        instanceId: siblingId,
        displayName: "Personal",
      },
    ]);
    const overrides = {
      instanceEntries: accountEntries,
      activeInstanceId: activeId,
      lockedProvider: antigravity,
      lockedInstanceId: activeId,
      lockedContinuationGroupKey: null,
    };
    render(overrides);
    const active = container.querySelector<HTMLButtonElement>(
      'button[aria-label="Work: First model"]',
    )!;
    const sibling = container.querySelector<HTMLButtonElement>(
      'button[aria-label="Personal: First model"]',
    )!;
    expect(active.disabled).toBe(false);
    expect(sibling.disabled).toBe(true);
    act(() => sibling.click());
    expect(onSelect).not.toHaveBeenCalled();
    act(() => active.click());
    expect(onSelect).toHaveBeenCalledWith(activeId, "first");
  });

  it("uses the session account lock even when draft selection points at a sibling", () => {
    const antigravity = ProviderDriverKind.make("antigravity");
    const sessionId = ProviderInstanceId.make("antigravity_work");
    const siblingId = ProviderInstanceId.make("antigravity_personal");
    render({
      instanceEntries: deriveProviderInstanceEntries([
        {
          ...entries[0]!.snapshot,
          driver: antigravity,
          instanceId: sessionId,
          displayName: "Work",
        },
        {
          ...entries[0]!.snapshot,
          driver: antigravity,
          instanceId: siblingId,
          displayName: "Personal",
        },
      ]),
      activeInstanceId: siblingId,
      lockedProvider: antigravity,
      lockedInstanceId: sessionId,
    });
    const session = container.querySelector<HTMLButtonElement>(
      'button[aria-label="Work: First model"]',
    )!;
    const sibling = container.querySelector<HTMLButtonElement>(
      'button[aria-label="Personal: First model"]',
    )!;
    expect(session.disabled).toBe(false);
    expect(sibling.disabled).toBe(true);
    act(() => sibling.click());
    expect(onSelect).not.toHaveBeenCalled();
    act(() => session.click());
    expect(onSelect).toHaveBeenCalledWith(sessionId, "first");
  });

  it("allows Antigravity accounts in the shared continuation group", () => {
    const antigravity = ProviderDriverKind.make("antigravity");
    const activeId = ProviderInstanceId.make("antigravity_work");
    const siblingId = ProviderInstanceId.make("antigravity_personal");
    const accountEntries = deriveProviderInstanceEntries([
      { ...entries[0]!.snapshot, driver: antigravity, instanceId: activeId, displayName: "Work" },
      {
        ...entries[0]!.snapshot,
        driver: antigravity,
        instanceId: siblingId,
        displayName: "Personal",
      },
    ]).map((entry) => ({ ...entry, continuationGroupKey: "shared-google-profile" }));
    render({
      instanceEntries: accountEntries,
      activeInstanceId: activeId,
      lockedProvider: antigravity,
      lockedInstanceId: activeId,
      lockedContinuationGroupKey: "shared-google-profile",
    });
    const sibling = container.querySelector<HTMLButtonElement>(
      'button[aria-label="Personal: First model"]',
    )!;
    expect(sibling.disabled).toBe(false);
    act(() => sibling.click());
    expect(onSelect).toHaveBeenCalledWith(siblingId, "first");
  });
});

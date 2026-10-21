import { act, render as renderTest, type Screen } from "../../../test/render";
import { type ComponentProps } from "react";
import { describe, expect, it, vi } from "vite-plus/test";
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
let root: Screen;
let onSelect = vi.fn<(instanceId: ProviderInstanceId, model: string) => void>();

async function render(overrides: Partial<ComponentProps<typeof FavoriteModelShortcuts>> = {}) {
  await act(async () =>
    await root.rerender(
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

async function setupTest(): Promise<AsyncDisposable> {
  let initialized = false;
  let ownedContainer: HTMLDivElement | undefined;
  async function cleanup() {
    const screen = initialized ? root : undefined;
    try {
  await act(() => screen?.unmount());
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
  onSelect = vi.fn();
  client.favorites = ["first", "second", "third"].map((model) => ({ provider: instanceId, model }));
  ownedContainer = container = document.createElement("div");
  document.body.append(container);
  root = await renderTest(null, { container: container, router: false });
  initialized = true;

    return { [Symbol.asyncDispose]: cleanup };
  } catch (error) {
    await cleanup();
    throw error;
  }
}



describe("provider account shortcuts", () => {
  it("keeps an enabled provider account reachable without favorite models", async () => {
    await using _fixture = await setupTest();
    client.favorites = [];
    await render();
    const button = container.querySelector<HTMLButtonElement>(
      'button[aria-label="Work: First model"]',
    );
    expect(button).not.toBeNull();
    await act(() => button?.click());
    expect(onSelect).toHaveBeenCalledWith(instanceId, "first");
  });
  it("shows one cell per account rather than one per favorite model", async () => {
    await using _fixture = await setupTest();
    await render();
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
  ])("prevents selection when restricted: %j", async (overrides) => {
    await using _fixture = await setupTest();
    await render(overrides);
    const button = container.querySelector<HTMLButtonElement>("button")!;
    expect(button.disabled).toBe(true);
    await act(() => button.click());
    expect(onSelect).not.toHaveBeenCalled();
  });

  it("omits settings-disabled accounts and retains unavailable accounts with an explanation", async () => {
    await using _fixture = await setupTest();
    const disabled = { ...entries[0]!, enabled: false };
    const unavailable = {
      ...entries[0]!,
      instanceId: ProviderInstanceId.make("codex_offline"),
      isAvailable: false,
    };
    await render({ instanceEntries: [disabled, unavailable] });
    const buttons = container.querySelectorAll<HTMLButtonElement>("button");
    expect(buttons).toHaveLength(1);
    expect(buttons[0]?.disabled).toBe(true);
    expect(buttons[0]?.title).toContain("Work");
    expect(buttons[0]?.title).toContain("No models are available");
  });

  it("keeps nine accounts and hides the entire group above the cap", async () => {
    await using _fixture = await setupTest();
    const many = Array.from({ length: 10 }, (_, index) => ({
      ...entries[0]!,
      instanceId: ProviderInstanceId.make(`codex_${index}`),
      displayName: `Account ${index}`,
    }));
    await render({ instanceEntries: many.slice(0, 9) });
    expect(container.querySelectorAll("button")).toHaveLength(9);
    await render({ instanceEntries: many });
    expect(container.childElementCount).toBe(0);
  });

  it("keeps hidden measurements inert and refuses clicks until restored", async () => {
    await using _fixture = await setupTest();
    await render({ visible: false });
    const group = container.querySelector<HTMLElement>('[role="group"]')!;
    const button = container.querySelector<HTMLButtonElement>("button")!;
    expect(group.hasAttribute("inert")).toBe(true);
    expect(button.tabIndex).toBe(-1);
    await act(() => button.click());
    expect(onSelect).not.toHaveBeenCalled();
    await render({ visible: true });
    await act(() => button.click());
    expect(onSelect).toHaveBeenCalledWith(instanceId, "first");
  });

  it("hides measurements and selection in multiple-model mode", async () => {
    await using _fixture = await setupTest();
    await render({ selectedModels: [{ instanceId, model: "first" }] });
    expect(container.querySelector<HTMLElement>('[role="group"]')?.hasAttribute("inert")).toBe(
      true,
    );
    await act(() => container.querySelector<HTMLButtonElement>("button")?.click());
    expect(onSelect).not.toHaveBeenCalled();
  });

  it("uses the existing exact-instance draft memory and updates account selection on rerender", async () => {
    await using _fixture = await setupTest();
    const otherId = ProviderInstanceId.make("codex_personal");
    const other = { ...entries[0]!, instanceId: otherId, displayName: "Personal" };
    await render({
      instanceEntries: [...entries, other],
      rememberedSelections: { [otherId]: { instanceId: otherId, model: "second" } },
    });
    const button = container.querySelector<HTMLButtonElement>(
      'button[aria-label="Personal: Second model"]',
    )!;
    await act(() => button.click());
    expect(onSelect).toHaveBeenCalledWith(otherId, "second");
    await render({ instanceEntries: [...entries, other], activeInstanceId: otherId, model: "second" });
    expect(button.getAttribute("aria-pressed")).toBe("true");
  });

  it("keeps Antigravity selection on the session account without continuation metadata", async () => {
    await using _fixture = await setupTest();
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
    await render(overrides);
    const active = container.querySelector<HTMLButtonElement>(
      'button[aria-label="Work: First model"]',
    )!;
    const sibling = container.querySelector<HTMLButtonElement>(
      'button[aria-label="Personal: First model"]',
    )!;
    expect(active.disabled).toBe(false);
    expect(sibling.disabled).toBe(true);
    await act(() => sibling.click());
    expect(onSelect).not.toHaveBeenCalled();
    await act(() => active.click());
    expect(onSelect).toHaveBeenCalledWith(activeId, "first");
  });

  it("uses the session account lock even when draft selection points at a sibling", async () => {
    await using _fixture = await setupTest();
    const antigravity = ProviderDriverKind.make("antigravity");
    const sessionId = ProviderInstanceId.make("antigravity_work");
    const siblingId = ProviderInstanceId.make("antigravity_personal");
    await render({
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
    await act(() => sibling.click());
    expect(onSelect).not.toHaveBeenCalled();
    await act(() => session.click());
    expect(onSelect).toHaveBeenCalledWith(sessionId, "first");
  });

  it("allows Antigravity accounts in the shared continuation group", async () => {
    await using _fixture = await setupTest();
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
    await render({
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
    await act(() => sibling.click());
    expect(onSelect).toHaveBeenCalledWith(siblingId, "first");
  });
});

import "~/index.css";
import { act, render } from "../../../test/render";
import { type ComponentProps } from "react";
import { describe, expect, it } from "vite-plus/test";
import { userEvent } from "vite-plus/test/browser";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderOptionDescriptor,
} from "@t3tools/contracts";
import { ComposerQuickControls } from "./ComposerQuickControls";
import { DraftId, useComposerDraftStore } from "../../composerDraftStore";
import { describeComposerEffort } from "../../components/chat/ComposerModelEffort";
import type { TraitsPersistence, TraitsSelectionInput } from "../../components/chat/TraitsPicker";

const instanceId = ProviderInstanceId.make("quick-controls-test");
const draftId = DraftId.make("quick-controls-test");
const effort: ProviderOptionDescriptor = {
  id: "reasoningEffort",
  type: "select",
  label: "Reasoning effort",
  options: [
    { id: "medium", label: "Medium" },
    { id: "high", label: "High", isDefault: true },
    { id: "xhigh", label: "Extra high" },
  ],
};
const fast: ProviderOptionDescriptor = { id: "fastMode", type: "boolean", label: "Fast mode" };
const tier: ProviderOptionDescriptor = {
  id: "serviceTier",
  type: "select",
  label: "Service tier",
  options: [
    { id: "default", label: "Standard", isDefault: true },
    { id: "priority", label: "Fast" },
  ],
};
const extendedTier: ProviderOptionDescriptor = {
  ...tier,
  options: [...tier.options, { id: "ultrafast", label: "Ultrafast" }],
};
const extendedEffort: ProviderOptionDescriptor = {
  ...effort,
  options: [{ id: "low", label: "Low" }, ...effort.options, { id: "max", label: "Max" }],
};
function Fixture({
  descriptors = [effort, fast],
  badgeEffortDisplay,
  hidden = false,
  ...overrides
}: {
  descriptors?: ProviderOptionDescriptor[];
  badgeEffortDisplay?: "label" | "bars";
} & Partial<Omit<ComponentProps<typeof ComposerQuickControls>, keyof TraitsPersistence>>) {
  const selection = useComposerDraftStore(
    (store) => store.stickyModelSelectionByProvider[instanceId],
  );
  const traits: TraitsSelectionInput = {
    provider: ProviderDriverKind.make("codex"),
    instanceId,
    draftId,
    model: "test-model",
    models: [
      {
        slug: "test-model",
        name: "Test",
        isCustom: false,
        capabilities: { optionDescriptors: descriptors },
      },
    ],
    modelOptions: selection?.options,
    ...overrides,
  };
  return (
    <div
      data-chat-composer-controls
      style={{
        display: "flex",
        gap: 4,
        width: 500,
        position: "relative",
        justifyContent: badgeEffortDisplay ? "flex-end" : undefined,
      }}
    >
      {badgeEffortDisplay ? null : (
        <span style={{ width: 250, flexShrink: 0 }}>Model · High · Full access</span>
      )}
      <ComposerQuickControls {...traits} hidden={hidden} />
      {badgeEffortDisplay ? (
        <div className="flex items-center gap-1.5 text-sm" data-chat-provider-model-picker>
          <span data-chat-provider-model-picker-label>Test model</span>
          {describeComposerEffort(traits, badgeEffortDisplay, "sm").suffix}
        </div>
      ) : null}
    </div>
  );
}
function reset() {
  useComposerDraftStore.setState({
    draftsByThreadKey: {},
    stickyModelSelectionByProvider: {},
    stickyActiveProvider: null,
  });
}

async function clickAt(button: HTMLButtonElement, timeStamp: number) {
  await act(() => {
    const event = new MouseEvent("click", { bubbles: true, detail: 1 });
    Object.defineProperty(event, "timeStamp", { value: timeStamp });
    button.dispatchEvent(event);
  });
}

function savedOptions() {
  return useComposerDraftStore.getState().stickyModelSelectionByProvider[instanceId]?.options;
}

describe("composer quick controls", () => {
  it.each(["label", "bars"] as const)(
    "keeps the click targets in place as the %s model badge changes",
    async (badgeEffortDisplay) => {
      reset();
      await using screen = await render(
        <Fixture
          descriptors={[extendedEffort, extendedTier]}
          badgeEffortDisplay={badgeEffortDisplay}
        />,
        { router: false },
      );
      const fast = screen.getByRole("button", { name: "Fast mode", exact: true });
      const medium = screen.getByRole("button", { name: "Medium reasoning", exact: true });
      const extra = screen.getByRole("button", { name: "Extra high reasoning", exact: true });
      await expect.element(extra).toBeEnabled();
      await userEvent.dblClick(fast);
      await expect
        .element(screen.getByRole("button", { name: "Ultrafast mode" }))
        .toHaveAttribute("aria-pressed", "true");
      await userEvent.dblClick(medium);
      await expect
        .element(screen.getByRole("button", { name: "Low reasoning" }))
        .toHaveAttribute("aria-pressed", "true");
      await userEvent.dblClick(extra);
      await expect
        .element(screen.getByRole("button", { name: "Max reasoning" }))
        .toHaveAttribute("aria-pressed", "true");
    },
  );

  it.each([fast, tier])(
    "toggles $id both ways and preserves reasoning, other options and the draft selection",
    async (speed) => {
      reset();
      await using screen = await render(<Fixture descriptors={[effort, speed]} />, {
        router: false,
      });
      const fastButton = screen.container.querySelector<HTMLButtonElement>(
        "[data-composer-quick-fast]",
      )!;
      const extra = screen.container.querySelector<HTMLButtonElement>(
        '[aria-label="Extra high reasoning"]',
      )!;
      await expect.poll(() => extra.disabled).toBe(false);
      expect(
        screen.container
          .querySelector('[aria-label="High reasoning"]')
          ?.getAttribute("aria-pressed"),
      ).toBe("true");
      await act(() => fastButton.click());
      expect(fastButton.getAttribute("aria-pressed")).toBe("true");
      expect(fastButton.querySelector("svg")?.classList.contains("fill-current")).toBe(true);
      await act(() => extra.click());
      expect(extra.getAttribute("aria-pressed")).toBe("true");
      expect(
        useComposerDraftStore.getState().stickyModelSelectionByProvider[instanceId]?.options,
      ).toContainEqual({ id: speed.id, value: speed.id === "fastMode" ? true : "priority" });
      await act(() => fastButton.click());
      expect(fastButton.getAttribute("aria-pressed")).toBe("false");
      const state = useComposerDraftStore.getState();
      const selection = state.stickyModelSelectionByProvider[instanceId];
      expect(selection?.options).toContainEqual({ id: "reasoningEffort", value: "xhigh" });
      expect(selection?.options).toContainEqual({
        id: speed.id,
        value: speed.id === "fastMode" ? false : "default",
      });
      expect(
        Object.values(state.draftsByThreadKey)[0]?.modelSelectionByProvider[instanceId],
      ).toEqual(selection);
    },
  );

  it("enters Ultrafast on the second click at 250 ms and exits with one click", async () => {
    reset();
    await using screen = await render(<Fixture descriptors={[effort, extendedTier]} />, {
      router: false,
    });
    const bolt = screen.container.querySelector<HTMLButtonElement>("[data-composer-quick-fast]")!;
    await clickAt(bolt, 1000);
    expect(savedOptions()).toContainEqual({ id: "serviceTier", value: "priority" });
    await clickAt(bolt, 1250);
    expect(savedOptions()).toContainEqual({ id: "serviceTier", value: "ultrafast" });
    expect(bolt.getAttribute("aria-label")).toBe("Ultrafast mode");
    expect(bolt.getAttribute("aria-pressed")).toBe("true");
    await clickAt(bolt, 1300);
    expect(savedOptions()).toContainEqual({ id: "serviceTier", value: "ultrafast" });
    await clickAt(bolt, 1551);
    expect(savedOptions()).toContainEqual({ id: "serviceTier", value: "default" });
    expect(bolt.getAttribute("aria-pressed")).toBe("false");
    await clickAt(bolt, 1802);
    await clickAt(bolt, 2053);
    expect(savedOptions()).toContainEqual({ id: "serviceTier", value: "default" });
  });

  it.each([
    { name: "Medium", value: "medium", alternate: "Low", alternateValue: "low" },
    { name: "Extra high", value: "xhigh", alternate: "Max", alternateValue: "max" },
  ])(
    "doubles $name into $alternate and returns with a single activation",
    async ({ name, value, alternate, alternateValue }) => {
      reset();
      await using screen = await render(<Fixture descriptors={[extendedEffort]} />, {
        router: false,
      });
      const button = screen.container.querySelector<HTMLButtonElement>(
        `[aria-label="${name} reasoning"]`,
      )!;
      await expect.poll(() => button.disabled).toBe(false);
      await clickAt(button, 1000);
      expect(savedOptions()).toContainEqual({ id: "reasoningEffort", value });
      await clickAt(button, 1250);
      expect(savedOptions()).toContainEqual({ id: "reasoningEffort", value: alternateValue });
      expect(button.getAttribute("aria-label")).toBe(`${alternate} reasoning`);
      expect(button.getAttribute("aria-pressed")).toBe("true");
      await act(() => button.click());
      expect(savedOptions()).toContainEqual({ id: "reasoningEffort", value });
    },
  );

  it("keeps rapid clicks on different controls and unsupported alternate choices as single clicks", async () => {
    reset();
    await using screen = await render(<Fixture descriptors={[extendedEffort, tier]} />, {
      router: false,
    });
    const bolt = screen.container.querySelector<HTMLButtonElement>("[data-composer-quick-fast]")!;
    const medium = screen.container.querySelector<HTMLButtonElement>(
      '[aria-label="Medium reasoning"]',
    )!;
    const extra = screen.container.querySelector<HTMLButtonElement>(
      '[aria-label="Extra high reasoning"]',
    )!;
    await expect.poll(() => extra.disabled).toBe(false);
    await clickAt(medium, 1000);
    await clickAt(extra, 1050);
    expect(savedOptions()).toContainEqual({ id: "reasoningEffort", value: "xhigh" });
    await clickAt(medium, 1100);
    expect(savedOptions()).toContainEqual({ id: "reasoningEffort", value: "medium" });
    await clickAt(bolt, 1150);
    await clickAt(bolt, 1200);
    expect(savedOptions()).toContainEqual({ id: "serviceTier", value: "default" });
    await screen.rerender(<Fixture descriptors={[effort, tier]} />);
    await clickAt(extra, 1250);
    await clickAt(extra, 1300);
    expect(savedOptions()).toContainEqual({ id: "reasoningEffort", value: "xhigh" });
  });

  it("keeps reasoning until 4px clearance remains, hides the whole group and restores it without changing selection", async () => {
    reset();
    await using screen = await render(<Fixture />, { router: false });
    const host = screen.container.querySelector<HTMLElement>("[data-chat-composer-controls]")!;
    const group = screen.container.querySelector<HTMLElement>("[data-composer-quick-effort]")!;
    const fastButton = screen.container.querySelector<HTMLButtonElement>(
      "[data-composer-quick-fast]",
    )!;
    await expect.poll(() => group.getAttribute("aria-hidden")).toBe(null);
    const edge =
      fastButton.getBoundingClientRect().right -
      host.getBoundingClientRect().left +
      8 +
      group.getBoundingClientRect().width;
    host.style.width = `${edge + 6}px`;
    await expect.poll(() => group.getAttribute("aria-hidden")).toBe(null);
    host.style.width = `${edge + 3}px`;
    await expect.poll(() => group.getAttribute("aria-hidden")).toBe("true");
    expect(group.inert).toBe(true);
    expect(fastButton.disabled).toBe(false);
    await act(() => fastButton.click());
    expect(fastButton.getAttribute("aria-pressed")).toBe("true");
    host.style.width = `${edge + 6}px`;
    await expect.poll(() => group.getAttribute("aria-hidden")).toBe(null);
    expect(group.querySelector('[aria-label="High reasoning"]')?.getAttribute("aria-pressed")).toBe(
      "true",
    );
  });

  it("uses an agent-declared reasoning descriptor rather than its first select", async () => {
    reset();
    await using screen = await render(
      <Fixture
        descriptors={[
          { ...effort, id: "mode", label: "Mode", options: [{ id: "code", label: "Code" }] },
          { ...effort, id: "thought_level", role: "reasoning" },
        ]}
      />,
      { router: false },
    );
    const medium = screen.container.querySelector<HTMLButtonElement>(
      '[aria-label="Medium reasoning"]',
    )!;
    await expect.poll(() => medium.disabled).toBe(false);
    await act(() => medium.click());
    expect(
      useComposerDraftStore.getState().stickyModelSelectionByProvider[instanceId]?.options,
    ).toEqual([{ id: "thought_level", value: "medium" }]);
  });

  it("shows only supported efforts, omits unsupported Fast and refuses hidden activation", async () => {
    reset();
    await using screen = await render(<Fixture descriptors={[effort]} />, { router: false });
    expect(screen.container.querySelector("[data-composer-quick-fast]")).toBe(null);
    await screen.rerender(<Fixture hidden />);
    const high = screen.container.querySelector<HTMLButtonElement>(
      '[aria-label="High reasoning"]',
    )!;
    expect(high.disabled).toBe(true);
    high.disabled = false;
    await act(() => high.click());
    expect(
      useComposerDraftStore.getState().stickyModelSelectionByProvider[instanceId],
    ).toBeUndefined();
  });
});

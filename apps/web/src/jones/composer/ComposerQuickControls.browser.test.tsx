import { act, render } from "../../../test/render";
import { type ComponentProps } from "react";
import { describe, expect, it } from "vite-plus/test";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderOptionDescriptor,
} from "@t3tools/contracts";
import { ComposerQuickControls } from "./ComposerQuickControls";
import { DraftId, useComposerDraftStore } from "../../composerDraftStore";

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
function Fixture({
  descriptors = [effort, fast],
  ...overrides
}: {
  descriptors?: ProviderOptionDescriptor[];
} & Partial<ComponentProps<typeof ComposerQuickControls>>) {
  const selection = useComposerDraftStore(
    (store) => store.stickyModelSelectionByProvider[instanceId],
  );
  return (
    <div
      data-chat-composer-controls
      style={{ display: "flex", gap: 4, width: 500, position: "relative" }}
    >
      <span style={{ width: 250, flexShrink: 0 }}>Model · High · Full access</span>
      <ComposerQuickControls
        provider={ProviderDriverKind.make("codex")}
        instanceId={instanceId}
        draftId={draftId}
        model="test-model"
        models={[
          {
            slug: "test-model",
            name: "Test",
            isCustom: false,
            capabilities: { optionDescriptors: descriptors },
          },
        ]}
        modelOptions={selection?.options}
        planModeEnabled
        {...overrides}
      />
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

describe("composer quick controls", () => {
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

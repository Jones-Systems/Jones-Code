// @vitest-environment jsdom
import { act, type ComponentProps } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderOptionDescriptor,
} from "@t3tools/contracts";
import { DraftId, useComposerDraftStore } from "../../composerDraftStore";
import { ReasoningEffortShortcuts } from "./ReasoningEffortShortcuts";

const instanceId = ProviderInstanceId.make("reasoning-test");
const draftId = DraftId.make("reasoning-test-draft");
const effort: ProviderOptionDescriptor = {
  id: "reasoningEffort",
  label: "Reasoning effort",
  type: "select",
  options: [
    { id: "low", label: "Quick" },
    { id: "high", label: "Deep", isDefault: true },
    { id: "max", label: "Maximum" },
  ],
};
const fast: ProviderOptionDescriptor = { id: "fastMode", label: "Fast", type: "boolean" };
let container: HTMLDivElement;
let root: Root;
let resize: () => void;
let rows: number;
let disconnect: ReturnType<typeof vi.fn>;
let onPromptChange = vi.fn<(prompt: string) => void>();

function render(
  overrides: Partial<ComponentProps<typeof ReasoningEffortShortcuts>> = {},
  descriptors = [effort, fast],
) {
  act(() =>
    root.render(
      <ReasoningEffortShortcuts
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
        modelOptions={[{ id: "fastMode", value: true }]}
        prompt=""
        onPromptChange={onPromptChange}
        planModeEnabled={true}
        {...overrides}
      />,
    ),
  );
}

function click(label: string) {
  const button = Array.from(container.querySelectorAll("button")).find(
    (button) => button.textContent === label,
  );
  expect(button).toBeDefined();
  act(() => button?.click());
}

beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  rows = 1;
  disconnect = vi.fn();
  onPromptChange = vi.fn();
  useComposerDraftStore.setState({
    draftsByThreadKey: {},
    stickyModelSelectionByProvider: {},
    stickyActiveProvider: null,
  });
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
  vi.spyOn(HTMLElement.prototype, "offsetHeight", "get").mockImplementation(() => rows * 28);
  vi.spyOn(HTMLElement.prototype, "offsetTop", "get").mockImplementation(function (
    this: HTMLElement,
  ) {
    return Math.min(Array.from(this.parentElement?.children ?? []).indexOf(this), rows - 1) * 28;
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

describe("reasoning effort shortcuts", () => {
  it("uses model labels and defaults, preserving other traits in the draft and sticky selection", () => {
    render();
    expect(
      Array.from(container.querySelectorAll("button"), (button) => button.textContent),
    ).toEqual(["Quick", "Deep", "Maximum"]);
    expect(container.querySelector('[aria-pressed="true"]')?.textContent).toBe("Deep");
    click("Maximum");
    const state = useComposerDraftStore.getState();
    const selection = state.stickyModelSelectionByProvider[instanceId];
    expect(selection).toEqual({
      instanceId,
      model: "test-model",
      options: [
        { id: "reasoningEffort", value: "max" },
        { id: "fastMode", value: true },
      ],
    });
    expect(Object.values(state.draftsByThreadKey)[0]?.modelSelectionByProvider[instanceId]).toEqual(
      selection,
    );
    render({ modelOptions: selection?.options });
    expect(container.querySelector('[aria-pressed="true"]')?.textContent).toBe("Maximum");
  });

  it.each(["effort", "variant", "reasoning"])(
    "uses the %s descriptor without exposing unrelated selectors",
    (id) => {
      render({}, [
        { ...effort, id },
        { id: "agent", label: "Agent", type: "select", options: [{ id: "build", label: "Build" }] },
      ]);
      click("Quick");
      expect(
        useComposerDraftStore.getState().stickyModelSelectionByProvider[instanceId]?.options,
      ).toContainEqual({ id, value: "low" });
      expect(container.textContent).not.toContain("Build");
    },
  );

  it("hides unsupported models and unavailable OpenCode metadata", () => {
    render({}, [fast]);
    expect(container.querySelector("button")).toBeNull();
    render({
      provider: ProviderDriverKind.make("opencode"),
      models: [],
      modelOptions: [{ id: "variant", value: "max" }],
    });
    expect(container.querySelector("button")).toBeNull();
  });

  const claude: ProviderOptionDescriptor = {
    ...effort,
    id: "effort",
    type: "select",
    options: [
      { id: "high", label: "High" },
      { id: "ultrathink", label: "Ultrathink" },
    ],
    promptInjectedValues: ["ultrathink"],
  };

  it("inserts and removes the Claude prefix without persisting prompt-injected values", () => {
    render({ provider: ProviderDriverKind.make("claudeAgent"), prompt: "Explain" }, [claude]);
    click("Ultrathink");
    expect(onPromptChange).toHaveBeenLastCalledWith("Ultrathink:\nExplain");
    expect(
      useComposerDraftStore.getState().stickyModelSelectionByProvider[instanceId],
    ).toBeUndefined();
    render({ prompt: "Ultrathink:\nExplain" }, [claude]);
    expect(container.querySelector('[aria-pressed="true"]')?.textContent).toBe("Ultrathink");
    click("High");
    expect(onPromptChange).toHaveBeenLastCalledWith("Explain");
  });

  it("locks effort when ultrathink remains in the prompt body and respects injection restrictions", () => {
    render({ prompt: "Please ultrathink about this" }, [claude]);
    expect(Array.from(container.querySelectorAll("button"), (button) => button.disabled)).toEqual([
      true,
      true,
    ]);
    click("High");
    expect(onPromptChange).not.toHaveBeenCalled();
    render({ allowPromptInjectedEffort: false }, [claude]);
    click("Ultrathink");
    expect(onPromptChange).not.toHaveBeenCalled();
  });

  it("hides the entire group at three rows and restores it at two without remounting", () => {
    render();
    const group = container.querySelector('[role="group"]');
    expect(group?.getAttribute("aria-hidden")).toBe("false");
    rows = 3;
    act(() => resize());
    expect(group?.getAttribute("aria-hidden")).toBe("true");
    expect(group?.hasAttribute("inert")).toBe(true);
    rows = 2;
    act(() => resize());
    expect(group?.getAttribute("aria-hidden")).toBe("false");
    act(() => root.unmount());
    expect(disconnect).toHaveBeenCalled();
    root = createRoot(container);
  });
});

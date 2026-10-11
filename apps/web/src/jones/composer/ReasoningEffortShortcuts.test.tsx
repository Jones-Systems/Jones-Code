// @vitest-environment jsdom
import { act, createRef, type ComponentProps } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderOptionDescriptor,
} from "@t3tools/contracts";
import { DraftId, useComposerDraftStore } from "../../composerDraftStore";
import { Menu, MenuPopup, MenuTrigger } from "../../components/ui/menu";
import { ReasoningEffortShortcuts } from "./ReasoningEffortShortcuts";
import { TraitsMenuContent, useTraitsSelection } from "../../components/chat/TraitsPicker";
import { getComposerEffectiveTraitsOptions } from "../../components/chat/composerProviderState";

const instanceId = ProviderInstanceId.make("reasoning-test");
const draftId = DraftId.make("reasoning-test-draft");
const effort: Extract<ProviderOptionDescriptor, { type: "select" }> = {
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
const claude: Extract<ProviderOptionDescriptor, { type: "select" }> = {
  ...effort,
  id: "effort",
  options: [
    { id: "high", label: "High", isDefault: true },
    { id: "ultrathink", label: "Ultrathink" },
  ],
  promptInjectedValues: ["ultrathink"],
};
let container: HTMLDivElement;
let root: Root;
let onPromptChange = vi.fn<(prompt: string) => void>();

function render(
  overrides: Partial<ComponentProps<typeof ReasoningEffortShortcuts>> = {},
  descriptors: ReadonlyArray<ProviderOptionDescriptor> = [effort, fast],
  withMenu = false,
) {
  const props = {
    provider: ProviderDriverKind.make("opencode"),
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
    modelOptions: [{ id: "fastMode", value: true }],
    prompt: "",
    onPromptChange,
    planModeEnabled: true,
    visible: true,
    ...overrides,
  };
  act(() =>
    root.render(
      <>
        <ReasoningEffortShortcuts {...props} />
        {withMenu ? (
          <Menu open>
            <MenuTrigger>Traits</MenuTrigger>
            <MenuPopup>
              <TraitsMenuContent {...props} />
            </MenuPopup>
          </Menu>
        ) : null}
      </>,
    ),
  );
}

function buttons() {
  return Array.from(container.querySelectorAll<HTMLButtonElement>('[role="group"] button'));
}

function click(label: string) {
  const button = buttons().find((button) => button.textContent === label);
  expect(button).toBeDefined();
  act(() => button?.click());
}

function selectedLabel() {
  return container.querySelector('[aria-pressed="true"]')?.textContent;
}

function currentOptions(id = instanceId) {
  return useComposerDraftStore.getState().stickyModelSelectionByProvider[id]?.options;
}

beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  onPromptChange = vi.fn();
  useComposerDraftStore.setState({
    draftsByThreadKey: {},
    stickyModelSelectionByProvider: {},
    stickyActiveProvider: null,
  });
  vi.stubGlobal(
    "ResizeObserver",
    class {
      observe() {}
      unobserve() {}
      disconnect() {}
    },
  );
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
  it("uses current model defaults and preserves other saved options in draft and sticky selection", () => {
    render({
      modelOptions: [
        { id: "fastMode", value: true },
        { id: "futureTrait", value: "saved" },
      ],
    });
    expect(buttons().map((button) => button.textContent)).toEqual(["Low", "High", "Max"]);
    expect(selectedLabel()).toBe("High");
    click("Max");
    const state = useComposerDraftStore.getState();
    const selection = state.stickyModelSelectionByProvider[instanceId];
    expect(selection).toEqual({
      instanceId,
      model: "test-model",
      options: [
        { id: "reasoningEffort", value: "max" },
        { id: "fastMode", value: true },
        { id: "futureTrait", value: "saved" },
      ],
    });
    expect(Object.values(state.draftsByThreadKey)[0]?.modelSelectionByProvider[instanceId]).toEqual(
      selection,
    );
    render({ modelOptions: selection?.options });
    expect(selectedLabel()).toBe("Max");
    expect(buttons()[2]?.title).toBe("Maximum");
  });

  it.each(["effort", "variant", "reasoning"])(
    "selects the %s descriptor without exposing unrelated selectors",
    (id) => {
      render({}, [
        { ...effort, id },
        { id: "agent", label: "Agent", type: "select", options: [{ id: "build", label: "Build" }] },
      ]);
      click("Low");
      expect(currentOptions()).toContainEqual({ id, value: "low" });
      expect(container.textContent).not.toContain("Build");
    },
  );

  it("selects every actual effort choice and keeps unknown metadata labels in full", () => {
    const options = [
      { id: "none", label: "None" },
      { id: "minimal", label: "Minimal" },
      { id: "low", label: "Low" },
      { id: "medium", label: "Medium", isDefault: true },
      { id: "high", label: "High" },
      { id: "xhigh", label: "Extra High" },
      { id: "max", label: "Maximum" },
      { id: "ultrathink", label: "Ultrathink" },
      { id: "adaptive", label: "Adaptive model supplied effort" },
    ];
    const descriptor = { ...effort, options };
    const labels = [
      "Off",
      "Min",
      "Low",
      "Med",
      "High",
      "XH",
      "Max",
      "Ultra",
      "Adaptive model supplied effort",
    ];
    render({}, [descriptor]);
    expect(buttons().map((button) => button.textContent)).toEqual(labels);
    for (const [index, option] of options.entries()) {
      click(labels[index]!);
      expect(currentOptions()).toEqual([
        { id: "reasoningEffort", value: option.id },
        { id: "fastMode", value: true },
      ]);
      render({ modelOptions: currentOptions() }, [descriptor]);
      expect(selectedLabel()).toBe(labels[index]);
    }
  });

  it("inherits the same configured Codex default as the menu without persisting it until selected", () => {
    const resolved = getComposerEffectiveTraitsOptions({
      provider: ProviderDriverKind.make("codex"),
      instanceId,
      model: "gpt-6.1-sol",
      models: familyModels,
      modelOptions: undefined,
      defaultModelSelection: {
        instanceId,
        model: "gpt-6.1-sol",
        options: [{ id: "reasoningEffort", value: "medium" }],
      },
      defaultDriverKind: ProviderDriverKind.make("codex"),
      planModeEnabled: true,
    });
    render({
      ...resolved,
      provider: ProviderDriverKind.make("codex"),
      model: "gpt-6.1-sol",
      models: familyModels,
    });
    expect(selectedLabel()).toBe("Medium");
    expect(currentOptions()).toBeUndefined();
    act(() => familyButton("GPT-6.1 Sol, High reasoning").click());
    expect(currentOptions()).toContainEqual({ id: "reasoningEffort", value: "high" });
  });

  it("follows the selected instance and model catalog without offering prior model choices", () => {
    const otherInstance = ProviderInstanceId.make("other-reasoning-instance");
    render();
    click("Low");
    render({
      instanceId: otherInstance,
      model: "other-model",
      modelOptions: undefined,
      models: [
        {
          slug: "other-model",
          name: "Other",
          isCustom: false,
          capabilities: {
            optionDescriptors: [
              { ...effort, options: [{ id: "xhigh", label: "Extra high", isDefault: true }] },
            ],
          },
        },
      ],
    });
    expect(buttons().map((button) => button.textContent)).toEqual(["XH"]);
    click("XH");
    expect(currentOptions()).toContainEqual({ id: "reasoningEffort", value: "low" });
    expect(useComposerDraftStore.getState().stickyModelSelectionByProvider[otherInstance]).toEqual({
      instanceId: otherInstance,
      model: "other-model",
      options: [{ id: "reasoningEffort", value: "xhigh" }],
    });
    render({ modelOptions: currentOptions() });
    expect(selectedLabel()).toBe("Low");
  });

  it("hides unsupported models and unavailable OpenCode metadata", () => {
    render({}, [fast]);
    expect(buttons()).toEqual([]);
    render({ model: "missing-model" });
    expect(buttons()).toEqual([]);
    render({
      provider: ProviderDriverKind.make("opencode"),
      models: [],
      modelOptions: [{ id: "variant", value: "max" }],
    });
    expect(buttons()).toEqual([]);
    expect(currentOptions()).toBeUndefined();
  });

  it("inserts and removes the prompt-injected prefix in the descriptor fallback without persisting prompt-injected values", () => {
    render({ prompt: "Explain" }, [claude]);
    click("Ultra");
    expect(onPromptChange).toHaveBeenLastCalledWith("Ultrathink:\nExplain");
    expect(currentOptions()).toBeUndefined();
    render({ prompt: "Ultrathink:\nExplain" }, [claude]);
    expect(selectedLabel()).toBe("Ultra");
    click("High");
    expect(onPromptChange).toHaveBeenLastCalledWith("Explain");
    expect(currentOptions()).toContainEqual({ id: "effort", value: "high" });
  });

  it("locks effort when ultrathink remains in the prompt body", () => {
    render({ prompt: "Please ultrathink about this" }, [claude]);
    expect(buttons().every((button) => button.disabled)).toBe(true);
    click("High");
    click("Ultra");
    expect(onPromptChange).not.toHaveBeenCalled();
    expect(currentOptions()).toBeUndefined();
  });

  it("guards prompt injection in the shared handler even when invoked outside a disabled control", () => {
    function AttemptInjection() {
      const traits = useTraitsSelection({
        provider: ProviderDriverKind.make("claudeAgent"),
        instanceId,
        draftId,
        model: "test-model",
        models: [
          {
            slug: "test-model",
            name: "Test",
            isCustom: false,
            capabilities: { optionDescriptors: [claude] },
          },
        ],
        prompt: "Explain",
        onPromptChange,
        planModeEnabled: true,
        allowPromptInjectedEffort: false,
      });
      return (
        <button onClick={() => traits.handleSelectChange(claude, "ultrathink")}>
          Attempt injection
        </button>
      );
    }
    act(() => root.render(<AttemptInjection />));
    act(() => container.querySelector("button")?.click());
    expect(onPromptChange).not.toHaveBeenCalled();
    expect(currentOptions()).toBeUndefined();
    render({ allowPromptInjectedEffort: false }, [claude]);
    expect(buttons().find((button) => button.textContent === "Ultra")?.disabled).toBe(true);
  });

  it("retains the measurable group while hidden and blocks activation until the parent restores it", () => {
    const groupRef = createRef<HTMLDivElement>();
    render({ visible: false, groupRef });
    const group = groupRef.current;
    expect(group).toBe(container.querySelector('[role="group"]'));
    expect(group?.style.visibility).toBe("hidden");
    expect(group?.hasAttribute("inert")).toBe(true);
    expect(buttons().every((button) => button.disabled && button.tabIndex === -1)).toBe(true);
    const button = buttons()[0]!;
    button.disabled = false;
    act(() => button.click());
    expect(currentOptions()).toBeUndefined();
    render({ visible: true, groupRef });
    expect(groupRef.current).toBe(group);
    expect(group?.style.visibility).toBe("visible");
    expect(group?.hasAttribute("inert")).toBe(false);
    click("Low");
    expect(currentOptions()).toContainEqual({ id: "reasoningEffort", value: "low" });
  });

  it("keeps actual menu and shortcut selections in parity through both directions", async () => {
    render({}, [effort, fast], true);
    click("Low");
    render({ modelOptions: currentOptions() }, [effort, fast], true);
    const lowItem = Array.from(
      document.querySelectorAll<HTMLElement>('[role="menuitemradio"]'),
    ).find((item) => item.textContent?.startsWith("Quick"));
    expect(lowItem?.getAttribute("aria-checked")).toBe("true");
    const maxItem = Array.from(
      document.querySelectorAll<HTMLElement>('[role="menuitemradio"]'),
    ).find((item) => item.textContent?.startsWith("Maximum"));
    expect(maxItem).toBeDefined();
    await act(async () => maxItem?.click());
    expect(currentOptions()).toEqual([
      { id: "reasoningEffort", value: "max" },
      { id: "fastMode", value: true },
    ]);
    render({ modelOptions: currentOptions() }, [effort, fast], true);
    expect(selectedLabel()).toBe("Max");
    const selectedItem = Array.from(
      document.querySelectorAll<HTMLElement>('[role="menuitemradio"]'),
    ).find(
      (item) =>
        item.getAttribute("aria-checked") === "true" && item.textContent?.startsWith("Maximum"),
    );
    expect(selectedItem).toBeDefined();
  });
});

const familyEffort = {
  ...effort,
  options: [
    { id: "medium", label: "Medium" },
    { id: "high", label: "High", isDefault: true },
    { id: "xhigh", label: "Extra High" },
  ],
};
const familyModels = [
  {
    slug: "gpt-6.1-sol",
    name: "GPT-6.1 Sol",
    isCustom: false,
    capabilities: { optionDescriptors: [familyEffort, fast] },
  },
  {
    slug: "gpt-6-astra",
    name: "GPT-6 Astra",
    isCustom: false,
    capabilities: { optionDescriptors: [familyEffort, fast] },
  },
];

function familyButton(name: string) {
  const button = buttons().find((candidate) => candidate.getAttribute("aria-label") === name);
  expect(button).toBeDefined();
  return button!;
}

it("selects a model and effort together on the current account, retaining same-model traits", () => {
  const onProviderModelSelect = vi.fn();
  const props = {
    provider: ProviderDriverKind.make("codex"),
    models: familyModels,
    model: "gpt-6.1-sol",
    onProviderModelSelect,
  };
  render(props);
  expect(buttons().map((button) => button.textContent)).toEqual([
    "Medium",
    "High",
    "Extra High",
    "Medium",
    "High",
    "Extra High",
  ]);
  expect(familyButton("GPT-6.1 Sol, High reasoning").getAttribute("aria-pressed")).toBe("true");
  expect(familyButton("GPT-6 Astra, High reasoning").getAttribute("aria-pressed")).toBe("false");
  act(() => familyButton("GPT-6.1 Sol, Medium reasoning").click());
  expect(currentOptions()).toEqual([
    { id: "reasoningEffort", value: "medium" },
    { id: "fastMode", value: true },
  ]);
  expect(onProviderModelSelect).not.toHaveBeenCalled();
  act(() => familyButton("GPT-6 Astra, Extra High reasoning").click());
  expect(onProviderModelSelect).toHaveBeenCalledExactlyOnceWith(instanceId, "gpt-6-astra", {
    effort: { id: "reasoningEffort", value: "xhigh" },
  });
  expect(currentOptions()).toContainEqual({ id: "reasoningEffort", value: "medium" });
  render({
    ...props,
    model: "gpt-6-astra",
    modelOptions: [{ id: "reasoningEffort", value: "xhigh" }],
  });
  expect(familyButton("GPT-6 Astra, Extra High reasoning").getAttribute("aria-pressed")).toBe(
    "true",
  );
  expect(familyButton("GPT-6.1 Sol, Extra High reasoning").getAttribute("aria-pressed")).toBe(
    "false",
  );
});

it("keeps missing families and unsupported efforts disabled, and enforces model locks and rail hiding", () => {
  const onProviderModelSelect = vi.fn();
  const props = {
    provider: ProviderDriverKind.make("codex"),
    model: "missing",
    models: [familyModels[0]!],
    onProviderModelSelect,
  };
  render(props);
  expect(familyButton("GPT-6 Astra, Medium reasoning").disabled).toBe(true);
  expect(familyButton("GPT-6 Astra, Medium reasoning").title).toContain("isn't available");
  expect(familyButton("GPT-6.1 Sol, Medium reasoning").disabled).toBe(false);
  render({ ...props, getModelDisabledReason: () => "Session model is locked" });
  expect(buttons().every((button) => button.disabled)).toBe(true);
  act(() => familyButton("GPT-6.1 Sol, Medium reasoning").click());
  expect(onProviderModelSelect).not.toHaveBeenCalled();
  render({ ...props, visible: false });
  const button = familyButton("GPT-6.1 Sol, Medium reasoning");
  button.disabled = false;
  act(() => button.click());
  expect(onProviderModelSelect).not.toHaveBeenCalled();
});

it("preserves Claude body locks and strips the prefix on same- and cross-model effort choices", () => {
  const descriptor = { ...familyEffort, id: "effort", promptInjectedValues: ["ultrathink"] };
  const models = ["opus", "sonnet"].map((family) => ({
    slug: `claude-${family}-5-5`,
    name: `Claude ${family}`,
    isCustom: false,
    capabilities: { optionDescriptors: [descriptor] },
  }));
  const onProviderModelSelect = vi.fn();
  const props = {
    provider: ProviderDriverKind.make("claudeAgent"),
    model: "claude-opus-5-5",
    models,
    onProviderModelSelect,
  };
  render({ ...props, prompt: "Ultrathink:\nExplain" });
  expect(selectedLabel()).toBeUndefined();
  act(() => familyButton("Claude opus, Medium reasoning").click());
  expect(onPromptChange).toHaveBeenLastCalledWith("Explain");
  expect(currentOptions()).toContainEqual({ id: "effort", value: "medium" });
  act(() => familyButton("Claude sonnet, Extra High reasoning").click());
  expect(onProviderModelSelect).toHaveBeenCalledExactlyOnceWith(instanceId, "claude-sonnet-5-5", {
    effort: { id: "effort", value: "xhigh" },
  });
  expect(onPromptChange).toHaveBeenLastCalledWith("Explain");
  render({ ...props, prompt: "Ultrathink:\nPlease ultrathink further" });
  expect(buttons().every((button) => button.disabled)).toBe(true);
  act(() => familyButton("Claude sonnet, High reasoning").click());
  expect(onProviderModelSelect).toHaveBeenCalledTimes(1);
});

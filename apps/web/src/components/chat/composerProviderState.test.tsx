import { describe, expect, it } from "vite-plus/test";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderOptionDescriptor,
  type ProviderOptionSelection,
  type ServerProviderModel,
} from "@t3tools/contracts";
import {
  applyConfiguredReasoningEffortDefault,
  getProviderOptionDescriptors,
} from "@t3tools/shared/model";
import { buildTraitsOptionSelections } from "./TraitsPicker";
import { getProviderModelCapabilities } from "../../providerModels";
import {
  getComposerPromptInjectionState,
  getComposerProviderState,
  getComposerReasoningStep,
  getComposerEffectiveTraitsOptions,
  renderProviderTraitsMenuContent,
  renderProviderTraitsPicker,
  withImplicitFastModeDefault,
} from "./composerProviderState";

const PROVIDER: ProviderDriverKind = ProviderDriverKind.make("codex");
const MODEL = "test-model";

function selectDescriptor(
  id: string,
  options: ReadonlyArray<{ id: string; label: string; isDefault?: boolean }>,
  promptInjectedValues?: ReadonlyArray<string>,
): Extract<ProviderOptionDescriptor, { type: "select" }> {
  const defaultId = options.find((option) => option.isDefault)?.id;
  return {
    id,
    label: id,
    type: "select",
    options: [...options],
    ...(defaultId ? { currentValue: defaultId } : {}),
    ...(promptInjectedValues && promptInjectedValues.length > 0
      ? { promptInjectedValues: [...promptInjectedValues] }
      : {}),
  };
}

function booleanDescriptor(
  id: string,
  currentValue?: boolean,
): Extract<ProviderOptionDescriptor, { type: "boolean" }> {
  return {
    id,
    label: id,
    type: "boolean",
    ...(typeof currentValue === "boolean" ? { currentValue } : {}),
  };
}

function modelWith(
  descriptors: ReadonlyArray<ProviderOptionDescriptor>,
): ReadonlyArray<ServerProviderModel> {
  return [
    { slug: MODEL, name: MODEL, isCustom: false, capabilities: { optionDescriptors: descriptors } },
  ];
}

function selections(
  ...entries: Array<[string, string | boolean]>
): ReadonlyArray<ProviderOptionSelection> {
  return entries.map(([id, value]) => ({ id, value }));
}

const ULTRATHINK_FRAME_CLASSES = {
  composerFrameClassName: "ultrathink-frame",
  composerSurfaceClassName: "shadow-[0_0_0_1px_rgba(255,255,255,0.07)_inset]",
  modelPickerIconClassName: "ultrathink-chroma",
} as const;

describe("getComposerProviderState", () => {
  it("derives a stable prompt injection state for ordinary prompt edits", () => {
    expect(getComposerPromptInjectionState("Investigate this failure")).toBe("none");
    expect(getComposerPromptInjectionState("Ultrathink:\nInvestigate this failure")).toBe(
      "ultrathink",
    );
  });

  it("displays inherited reasoning effort from the live model default without persisting it", () => {
    const modelOptions = selections(["fastMode", true], ["serviceTier", "priority"]);
    for (const defaultEffort of ["medium", "high"]) {
      const state = getComposerProviderState({
        provider: PROVIDER,
        model: MODEL,
        models: modelWith([
          selectDescriptor("reasoningEffort", [
            { id: "low", label: "Low" },
            { id: defaultEffort, label: defaultEffort, isDefault: true },
          ]),
          booleanDescriptor("fastMode"),
          selectDescriptor("serviceTier", [{ id: "priority", label: "Priority" }]),
        ]),
        modelOptions,
        planModeEnabled: false,
      });
      expect(state.promptEffort).toBe(defaultEffort);
      expect(state.modelOptionsForDispatch).toEqual(modelOptions);
      expect(modelOptions).toEqual(selections(["fastMode", true], ["serviceTier", "priority"]));
    }
  });

  it("honors an explicit supported reasoning effort for the current model", () => {
    const state = getComposerProviderState({
      provider: PROVIDER,
      model: MODEL,
      models: modelWith([
        selectDescriptor("reasoningEffort", [
          { id: "low", label: "Low" },
          { id: "medium", label: "Medium", isDefault: true },
        ]),
      ]),
      modelOptions: selections(["reasoningEffort", "low"]),
      planModeEnabled: false,
    });
    expect(state.promptEffort).toBe("low");
    expect(state.modelOptionsForDispatch).toEqual(selections(["reasoningEffort", "low"]));
  });

  it("follows the current configured effort across Codex accounts without dispatching it", () => {
    const models = modelWith([
      selectDescriptor("reasoningEffort", [
        { id: "low", label: "Low" },
        { id: "medium", label: "Medium", isDefault: true },
        { id: "high", label: "High" },
      ]),
      selectDescriptor("serviceTier", [
        { id: "default", label: "Standard", isDefault: true },
        { id: "priority", label: "Fast" },
      ]),
    ]);
    const options = selections(["serviceTier", "priority"]);
    for (const effort of ["high", "low"]) {
      const input = {
        provider: PROVIDER,
        instanceId: ProviderInstanceId.make("codex_personal"),
        model: MODEL,
        models,
        modelOptions: options,
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex_work"),
          model: MODEL,
          options: selections(["reasoningEffort", effort]),
        },
        defaultDriverKind: PROVIDER,
        planModeEnabled: false,
      };
      expect(getComposerProviderState(input).promptEffort).toBe(effort);
      expect(getComposerProviderState(input).modelOptionsForDispatch).toEqual(options);
      expect(
        getComposerProviderState({
          ...input,
          modelOptions: selections(["reasoningEffort", "medium"]),
        }).promptEffort,
      ).toBe("medium");
    }
  });

  it("uses descriptor defaults for display without dispatching them as overrides", () => {
    const state = getComposerProviderState({
      provider: PROVIDER,
      model: MODEL,
      models: modelWith([
        selectDescriptor("effort", [
          { id: "low", label: "Low" },
          { id: "high", label: "High", isDefault: true },
        ]),
      ]),
      modelOptions: undefined,
      planModeEnabled: true,
    });

    expect(state).toEqual({
      provider: PROVIDER,
      promptEffort: "high",
      modelOptionsForDispatch: undefined,
    });
  });

  it("lets selections override defaults and propagates them through dispatch", () => {
    const state = getComposerProviderState({
      provider: PROVIDER,
      model: MODEL,
      models: modelWith([
        selectDescriptor("effort", [
          { id: "low", label: "Low" },
          { id: "high", label: "High", isDefault: true },
        ]),
        booleanDescriptor("fastMode"),
      ]),
      modelOptions: selections(["effort", "low"], ["fastMode", true]),
      planModeEnabled: true,
    });

    expect(state).toEqual({
      provider: PROVIDER,
      promptEffort: "low",
      modelOptionsForDispatch: selections(["effort", "low"], ["fastMode", true]),
    });
  });

  it("preserves selections that match defaults so deepMerge can overwrite prior state", () => {
    const state = getComposerProviderState({
      provider: PROVIDER,
      model: MODEL,
      models: modelWith([
        selectDescriptor("effort", [{ id: "high", label: "High", isDefault: true }]),
        booleanDescriptor("fastMode"),
      ]),
      modelOptions: selections(["effort", "high"], ["fastMode", false]),
      planModeEnabled: true,
    });

    expect(state.modelOptionsForDispatch).toEqual(
      selections(["effort", "high"], ["fastMode", false]),
    );
  });

  it("drops selections for descriptors the model does not declare", () => {
    const state = getComposerProviderState({
      provider: PROVIDER,
      model: MODEL,
      models: modelWith([booleanDescriptor("thinking")]),
      modelOptions: selections(["effort", "max"], ["thinking", false]),
      planModeEnabled: true,
    });

    expect(state).toEqual({
      provider: PROVIDER,
      promptEffort: null,
      modelOptionsForDispatch: selections(["thinking", false]),
    });
  });

  it("derives promptEffort from the first select descriptor and preserves all others for dispatch", () => {
    const state = getComposerProviderState({
      provider: PROVIDER,
      model: MODEL,
      models: modelWith([
        selectDescriptor("effort", [{ id: "high", label: "High", isDefault: true }]),
        selectDescriptor("contextWindow", [
          { id: "200k", label: "200k", isDefault: true },
          { id: "1m", label: "1M" },
        ]),
        selectDescriptor("agent", [
          { id: "build", label: "Build", isDefault: true },
          { id: "plan", label: "Plan" },
        ]),
      ]),
      modelOptions: selections(["agent", "plan"]),
      planModeEnabled: true,
    });

    expect(state.promptEffort).toBe("high");
    expect(state.modelOptionsForDispatch).toEqual(selections(["agent", "plan"]));
  });

  it("drops the plan agent from dispatch when legacy plan mode is disabled", () => {
    const state = getComposerProviderState({
      provider: PROVIDER,
      model: MODEL,
      models: modelWith([
        selectDescriptor("agent", [
          { id: "build", label: "Build", isDefault: true },
          { id: "plan", label: "Plan" },
        ]),
      ]),
      modelOptions: selections(["agent", "plan"]),
      planModeEnabled: false,
    });

    expect(state.modelOptionsForDispatch).toEqual(selections(["agent", "build"]));
  });

  it("drops the agent descriptor entirely when plan is the only option and plan mode is disabled", () => {
    const state = getComposerProviderState({
      provider: PROVIDER,
      model: MODEL,
      models: modelWith([
        selectDescriptor("agent", [{ id: "plan", label: "Plan", isDefault: true }]),
      ]),
      modelOptions: selections(["agent", "plan"]),
      planModeEnabled: false,
    });

    expect(state).toEqual({
      provider: PROVIDER,
      promptEffort: null,
      modelOptionsForDispatch: undefined,
    });
  });

  it("falls back to a surviving agent when plan was the descriptor default and plan mode is disabled", () => {
    const state = getComposerProviderState({
      provider: PROVIDER,
      model: MODEL,
      models: modelWith([
        selectDescriptor("agent", [
          { id: "plan", label: "Plan", isDefault: true },
          { id: "research", label: "Research" },
        ]),
      ]),
      modelOptions: undefined,
      planModeEnabled: false,
    });

    expect(state.modelOptionsForDispatch).toBeUndefined();
  });

  it("returns undefined dispatch options when the model declares no descriptors", () => {
    const state = getComposerProviderState({
      provider: PROVIDER,
      model: MODEL,
      models: modelWith([]),
      modelOptions: selections(["anything", "value"]),
      planModeEnabled: true,
    });

    expect(state).toEqual({
      provider: PROVIDER,
      promptEffort: null,
      modelOptionsForDispatch: undefined,
    });
  });

  it("preserves explicit options when the selected model is absent from the catalog", () => {
    const state = getComposerProviderState({
      provider: ProviderDriverKind.make("opencode"),
      model: "opencode/kimi-k3",
      models: [
        {
          slug: "opencode/big-pickle",
          name: "Big Pickle",
          isCustom: false,
          capabilities: {},
        },
      ],
      modelOptions: selections(["variant", "max"], ["agent", "build"]),
      planModeEnabled: false,
    });

    expect(state.modelOptionsForDispatch).toEqual(
      selections(["variant", "max"], ["agent", "build"]),
    );
  });

  it.each(["codex", "claudeAgent", "cursor", "grok"])(
    "does not preserve unknown options for a missing %s model",
    (provider) => {
      const state = getComposerProviderState({
        provider: ProviderDriverKind.make(provider),
        model: "missing-model",
        models: modelWith([]),
        modelOptions: selections(["unknown", "value"]),
        planModeEnabled: true,
      });

      expect(state.modelOptionsForDispatch).toBeUndefined();
    },
  );

  it("preserves explicit options while the catalog is empty", () => {
    const state = getComposerProviderState({
      provider: ProviderDriverKind.make("opencode"),
      model: "opencode/kimi-k3",
      models: [],
      modelOptions: selections(["variant", "max"], ["agent", "build"]),
      planModeEnabled: false,
    });

    expect(state.modelOptionsForDispatch).toEqual(
      selections(["variant", "max"], ["agent", "build"]),
    );
  });

  it("validates options for a known model selected through a legacy alias", () => {
    const state = getComposerProviderState({
      provider: ProviderDriverKind.make("claudeAgent"),
      model: "legacy-test-model",
      models: [
        {
          slug: "test-model",
          name: "Test Model",
          aliases: ["legacy-test-model"],
          isCustom: false,
          capabilities: {
            optionDescriptors: [
              selectDescriptor("effort", [
                { id: "low", label: "Low" },
                { id: "high", label: "High", isDefault: true },
              ]),
            ],
          },
        },
      ],
      modelOptions: selections(["effort", "low"], ["unknown", "value"]),
      planModeEnabled: false,
    });

    expect(state.modelOptionsForDispatch).toEqual(selections(["effort", "low"]));
  });

  it("still drops the plan agent when an absent model has a saved plan selection", () => {
    const state = getComposerProviderState({
      provider: ProviderDriverKind.make("opencode"),
      model: "opencode/kimi-k3",
      models: [],
      modelOptions: selections(["variant", "max"], ["agent", "plan"]),
      planModeEnabled: false,
    });

    expect(state.modelOptionsForDispatch).toEqual(selections(["variant", "max"]));
  });

  it("adds ultrathink class names when the prompt triggers a promptInjectedValues descriptor", () => {
    const state = getComposerProviderState({
      provider: PROVIDER,
      model: MODEL,
      models: modelWith([
        selectDescriptor(
          "effort",
          [
            { id: "medium", label: "Medium" },
            { id: "high", label: "High", isDefault: true },
            { id: "ultrathink", label: "Ultrathink" },
          ],
          ["ultrathink"],
        ),
      ]),
      promptInjectionState: getComposerPromptInjectionState(
        "Ultrathink:\nInvestigate this failure",
      ),
      modelOptions: selections(["effort", "medium"]),
      planModeEnabled: true,
    });

    expect(state).toEqual({
      provider: PROVIDER,
      promptEffort: "medium",
      modelOptionsForDispatch: selections(["effort", "medium"]),
      ...ULTRATHINK_FRAME_CLASSES,
    });
  });

  it("does not add ultrathink class names when the descriptor has no promptInjectedValues", () => {
    const state = getComposerProviderState({
      provider: PROVIDER,
      model: MODEL,
      models: modelWith([
        selectDescriptor("effort", [{ id: "high", label: "High", isDefault: true }]),
      ]),
      promptInjectionState: getComposerPromptInjectionState(
        "Ultrathink:\nInvestigate this failure",
      ),
      modelOptions: undefined,
      planModeEnabled: true,
    });

    expect(state).not.toHaveProperty("composerFrameClassName");
    expect(state).not.toHaveProperty("composerSurfaceClassName");
    expect(state).not.toHaveProperty("modelPickerIconClassName");
  });

  it("defaults fastMode to false when the provider reports true but the user has not selected it", () => {
    const state = getComposerProviderState({
      provider: ProviderDriverKind.make("cursor"),
      model: MODEL,
      models: modelWith([booleanDescriptor("fastMode", true)]),
      modelOptions: undefined,
      planModeEnabled: true,
    });

    expect(state.modelOptionsForDispatch).toEqual(selections(["fastMode", false]));
  });

  it("keeps explicit fastMode true when the user selected Fast", () => {
    const state = getComposerProviderState({
      provider: ProviderDriverKind.make("cursor"),
      model: MODEL,
      models: modelWith([booleanDescriptor("fastMode", true)]),
      modelOptions: selections(["fastMode", true]),
      planModeEnabled: true,
    });

    expect(state.modelOptionsForDispatch).toEqual(selections(["fastMode", true]));
  });

  it("keeps explicit fastMode false when the user selected Normal", () => {
    const state = getComposerProviderState({
      provider: ProviderDriverKind.make("cursor"),
      model: MODEL,
      models: modelWith([booleanDescriptor("fastMode", true)]),
      modelOptions: selections(["fastMode", false]),
      planModeEnabled: true,
    });

    expect(state.modelOptionsForDispatch).toEqual(selections(["fastMode", false]));
  });
});

describe("withImplicitFastModeDefault", () => {
  it("injects fastMode false only when the model exposes fastMode and no selection exists", () => {
    expect(
      withImplicitFastModeDefault(
        {
          optionDescriptors: [booleanDescriptor("fastMode", true)],
        },
        undefined,
      ),
    ).toEqual(selections(["fastMode", false]));

    expect(
      withImplicitFastModeDefault(
        {
          optionDescriptors: [booleanDescriptor("fastMode", true)],
        },
        selections(["fastMode", true]),
      ),
    ).toEqual(selections(["fastMode", true]));
  });

  it("does not add fastMode when the model does not expose it", () => {
    expect(
      withImplicitFastModeDefault(
        {
          optionDescriptors: [booleanDescriptor("thinking", true)],
        },
        undefined,
      ),
    ).toBeUndefined();
  });
});

describe("trait controls fastMode display", () => {
  it("uses effective display defaults without dropping unknown stored keys or inheriting stale model metadata", () => {
    const models = modelWith([
      selectDescriptor("reasoningEffort", [
        { id: "low", label: "Low", isDefault: true },
        { id: "high", label: "High" },
      ]),
      booleanDescriptor("fastMode", true),
    ]);
    const options = selections(["futureTrait", "saved"]);
    const input = {
      provider: PROVIDER,
      instanceId: ProviderInstanceId.make("codex_personal"),
      model: MODEL,
      models,
      modelOptions: options,
      defaultModelSelection: {
        instanceId: ProviderInstanceId.make("codex_work"),
        model: MODEL,
        options: selections(["reasoningEffort", "high"]),
      },
      defaultDriverKind: PROVIDER,
      planModeEnabled: true,
    };
    const effective = getComposerEffectiveTraitsOptions(input);
    expect(effective.modelOptions).toEqual(
      selections(["futureTrait", "saved"], ["fastMode", false]),
    );
    const descriptors = getProviderOptionDescriptors({
      caps: effective.displayCapabilities,
      selections: effective.modelOptions,
    });
    expect(
      descriptors.find((descriptor) => descriptor.id === "reasoningEffort")?.currentValue,
    ).toBe("high");
    expect(
      buildTraitsOptionSelections(descriptors, effective.modelOptions, {
        id: "reasoningEffort",
        value: "low",
      }),
    ).toEqual(
      selections(["reasoningEffort", "low"], ["fastMode", false], ["futureTrait", "saved"]),
    );
    expect(options).toEqual(selections(["futureTrait", "saved"]));
    expect(
      getComposerEffectiveTraitsOptions({ ...input, model: "missing-model" }).displayCapabilities
        .optionDescriptors,
    ).toEqual([]);
  });

  it("resolves traits fastMode to Normal when the provider defaults to true without a user selection", () => {
    const models = modelWith([booleanDescriptor("fastMode", true)]);
    const provider = ProviderDriverKind.make("cursor");
    const caps = getProviderModelCapabilities(models, MODEL, provider);
    const resolved = withImplicitFastModeDefault(caps, undefined);
    const descriptors = getProviderOptionDescriptors({ caps, selections: resolved });
    const fastMode = descriptors.find((descriptor) => descriptor.id === "fastMode");

    expect(fastMode?.type).toBe("boolean");
    if (fastMode?.type === "boolean") {
      expect(fastMode.currentValue).toBe(false);
    }
  });
});

describe("traits option persistence", () => {
  it("preserves unknown saved options when an effort selection changes", () => {
    const descriptors = getProviderOptionDescriptors({
      caps: {
        optionDescriptors: [
          selectDescriptor("reasoningEffort", [
            { id: "low", label: "Low" },
            { id: "high", label: "High", isDefault: true },
          ]),
          booleanDescriptor("fastMode", false),
        ],
      },
      selections: selections(["fastMode", true], ["futureTrait", "preserved"]),
    });
    expect(
      buildTraitsOptionSelections(
        descriptors,
        selections(["fastMode", true], ["futureTrait", "preserved"]),
        { id: "reasoningEffort", value: "low" },
      ),
    ).toEqual(
      selections(["reasoningEffort", "low"], ["fastMode", true], ["futureTrait", "preserved"]),
    );
  });

  it("keeps configured effort inherited after changing an unrelated trait", () => {
    const caps = modelWith([
      selectDescriptor("reasoningEffort", [
        { id: "low", label: "Low", isDefault: true },
        { id: "high", label: "High" },
      ]),
      booleanDescriptor("thinking", false),
    ])[0]!.capabilities;
    const modelSelection = { instanceId: ProviderInstanceId.make("codex"), model: MODEL };
    const displayCaps = applyConfiguredReasoningEffortDefault({
      modelSelection,
      driverKind: PROVIDER,
      capabilities: caps ?? undefined,
      defaultModelSelection: {
        ...modelSelection,
        options: selections(["reasoningEffort", "high"]),
      },
      defaultDriverKind: PROVIDER,
    })!;
    const descriptors = getProviderOptionDescriptors({ caps: displayCaps, selections: undefined });
    expect(
      buildTraitsOptionSelections(descriptors, undefined, { id: "thinking", value: true }),
    ).toEqual(selections(["thinking", true]));
    expect(
      buildTraitsOptionSelections(descriptors, undefined, { id: "reasoningEffort", value: "low" }),
    ).toEqual(selections(["reasoningEffort", "low"]));
    expect(
      buildTraitsOptionSelections(descriptors, selections(["reasoningEffort", "high"]), {
        id: "thinking",
        value: true,
      }),
    ).toEqual(selections(["reasoningEffort", "high"], ["thinking", true]));
  });
});

describe("provider traits render guards", () => {
  it("returns null when no thread target is provided", () => {
    const models = modelWith([
      selectDescriptor("effort", [{ id: "high", label: "High", isDefault: true }]),
    ]);
    const args = {
      provider: PROVIDER,
      model: MODEL,
      models,
      modelOptions: undefined,
      prompt: "",
      onPromptChange: () => {},
      planModeEnabled: true,
    };

    expect(renderProviderTraitsPicker(args)).toBeNull();
    expect(renderProviderTraitsMenuContent(args)).toBeNull();
  });
});

describe("getComposerReasoningStep", () => {
  const LEVELS = [
    { id: "low", label: "Low" },
    { id: "medium", label: "Medium", isDefault: true },
    { id: "high", label: "High" },
  ];
  const INSTANCE = ProviderInstanceId.make("codex");
  const CLAUDE = {
    provider: ProviderDriverKind.make("claudeAgent"),
    instanceId: ProviderInstanceId.make("claudeAgent"),
    models: modelWith([
      selectDescriptor(
        "effort",
        [
          { id: "medium", label: "Medium" },
          { id: "high", label: "High", isDefault: true },
          { id: "ultrathink", label: "Ultrathink" },
        ],
        ["ultrathink"],
      ),
    ]),
  };

  function step(input: Partial<Parameters<typeof getComposerReasoningStep>[0]>) {
    return getComposerReasoningStep({
      provider: PROVIDER,
      instanceId: INSTANCE,
      model: MODEL,
      models: modelWith([selectDescriptor("reasoningEffort", LEVELS)]),
      modelOptions: undefined,
      prompt: "Fix the bug",
      cursor: 0,
      planModeEnabled: true,
      direction: 1,
      ...input,
    });
  }

  it.each([
    { direction: 1, expected: "low" },
    { direction: -1, expected: "medium" },
  ] as const)(
    "steps $direction from the displayed project default and persists the explicit choice",
    ({ direction, expected }) => {
      const modelOptions = selections(["futureTrait", "preserved"]);
      const input = {
        provider: PROVIDER,
        instanceId: INSTANCE,
        model: MODEL,
        models: modelWith([selectDescriptor("reasoningEffort", LEVELS)]),
        modelOptions,
        defaultModelSelection: {
          instanceId: INSTANCE,
          model: MODEL,
          options: selections(["reasoningEffort", "high"]),
        },
        defaultDriverKind: PROVIDER,
        planModeEnabled: true,
      };
      expect(getComposerProviderState(input).promptEffort).toBe("high");
      expect(getComposerProviderState(input).modelOptionsForDispatch).toBeUndefined();
      expect(getComposerEffectiveTraitsOptions(input).modelOptions).toEqual(modelOptions);
      const result = step({ ...input, direction });
      expect(result).toEqual({
        prompt: "Fix the bug",
        cursor: 0,
        modelOptions: selections(["reasoningEffort", expected], ["futureTrait", "preserved"]),
      });
      expect(
        getComposerProviderState({ ...input, modelOptions: result?.modelOptions }).promptEffort,
      ).toBe(expected);
      expect(
        getComposerProviderState({ ...input, modelOptions: result?.modelOptions })
          .modelOptionsForDispatch,
      ).toEqual(selections(["reasoningEffort", expected]));
      expect(modelOptions).toEqual(selections(["futureTrait", "preserved"]));
    },
  );

  it("walks the catalog order, wrapping at both ends and keeping the other traits", () => {
    const models = modelWith([
      selectDescriptor("contextWindow", [
        { id: "200k", label: "200k", isDefault: true },
        { id: "1m", label: "1M" },
      ]),
      selectDescriptor("reasoningEffort", LEVELS),
    ]);
    for (const [from, direction, to] of [
      ["low", -1, "high"],
      ["low", 1, "medium"],
      ["high", 1, "low"],
      ["high", -1, "medium"],
    ] as const) {
      expect(
        step({
          models,
          modelOptions: selections(["contextWindow", "1m"], ["reasoningEffort", from]),
          direction,
        }),
        `${from} by ${direction}`,
      ).toStrictEqual({
        prompt: "Fix the bug",
        cursor: 0,
        modelOptions: selections(["contextWindow", "1m"], ["reasoningEffort", to]),
      });
    }
  });

  it("adds and strips the Ultrathink prefix while the caret stays on the same character", () => {
    for (const { prompt, modelOptions, direction, expected } of [
      {
        prompt: "  Fix the bug \n",
        modelOptions: selections(["effort", "high"]),
        direction: 1,
        expected: { prompt: "Ultrathink:\nFix the bug", cursor: 20 },
      },
      {
        // The prefix, not the draft's Medium, is the current level.
        prompt: "Ultrathink:\n  Fix the bug",
        modelOptions: selections(["effort", "medium"]),
        direction: -1,
        expected: {
          prompt: "Fix the bug",
          cursor: 8,
          modelOptions: selections(["effort", "high"]),
        },
      },
    ] as const) {
      const result = step({
        ...CLAUDE,
        prompt,
        cursor: prompt.indexOf("bug"),
        modelOptions,
        direction,
      });
      expect(result).toStrictEqual(expected);
      expect(result?.prompt.slice(result.cursor)).toMatch(/^bug/);
    }
    // A caret in leading whitespace must not land inside the new prefix.
    expect(
      step({ ...CLAUDE, prompt: "   Fix", modelOptions: selections(["effort", "high"]) })?.cursor,
    ).toBe("Ultrathink:\n".length);
  });

  it("skips Ultrathink for slash commands and does nothing when the prompt locks it", () => {
    expect(
      step({ ...CLAUDE, prompt: "/review", modelOptions: selections(["effort", "high"]) }),
    ).toStrictEqual({
      prompt: "/review",
      cursor: 0,
      modelOptions: selections(["effort", "medium"]),
    });
    for (const [prompt, direction] of [
      ["Please ultrathink harder", 1],
      ["Ultrathink:\nPlease ultrathink harder", -1],
    ] as const) {
      expect(
        step({ ...CLAUDE, prompt, modelOptions: selections(["effort", "high"]), direction }),
        prompt,
      ).toBeNull();
    }
  });

  it("keeps a provider-default Fast mode off unless the user chose it", () => {
    const cursor = {
      provider: ProviderDriverKind.make("cursor"),
      instanceId: ProviderInstanceId.make("cursor"),
      models: modelWith([
        selectDescriptor("reasoning", LEVELS),
        booleanDescriptor("fastMode", true),
      ]),
    };

    expect(step({ ...cursor, modelOptions: undefined })?.modelOptions).toStrictEqual(
      selections(["reasoning", "high"], ["fastMode", false]),
    );
    expect(
      step({ ...cursor, modelOptions: selections(["fastMode", true]) })?.modelOptions,
    ).toStrictEqual(selections(["reasoning", "high"], ["fastMode", true]));
  });

  it("steps from the provider-reported level for the same instance and model", () => {
    const reported = {
      instanceId: INSTANCE,
      model: MODEL,
      options: selections(["reasoningEffort", "high"]),
    };
    for (const { label, modelOptions, reportedModelSelection, to } of [
      { label: "reported", modelOptions: undefined, reportedModelSelection: reported, to: "low" },
      {
        label: "other instance",
        modelOptions: undefined,
        reportedModelSelection: { ...reported, instanceId: ProviderInstanceId.make("codex-work") },
        to: "high",
      },
      {
        label: "other model",
        modelOptions: undefined,
        reportedModelSelection: { ...reported, model: "other-model" },
        to: "high",
      },
      {
        label: "explicit draft",
        modelOptions: selections(["reasoningEffort", "low"]),
        reportedModelSelection: reported,
        to: "medium",
      },
      {
        // OpenCode reports "default" when no variant was chosen.
        label: "reported value outside the catalog",
        modelOptions: undefined,
        reportedModelSelection: {
          ...reported,
          options: selections(["reasoningEffort", "default"]),
        },
        to: "high",
      },
    ]) {
      expect(step({ modelOptions, reportedModelSelection })?.modelOptions, label).toStrictEqual(
        selections(["reasoningEffort", to]),
      );
    }
  });
});

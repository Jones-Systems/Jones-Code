import { describe, expect, it } from "vite-plus/test";
import { ProviderDriverKind, ProviderInstanceId, type ModelCapabilities } from "@t3tools/contracts";

import {
  applyClaudePromptEffortPrefix,
  applyConfiguredReasoningEffortDefault,
  buildExplicitProviderOptionSelectionsFromDescriptors,
  buildProviderOptionSelectionsFromDescriptors,
  createModelCapabilities,
  createModelSelection,
  getConfiguredReasoningEffort,
  formatCodexModelName,
  formatModelSlugName,
  getModelSelectionBooleanOptionValue,
  getModelSelectionStringOptionValue,
  getProviderOptionDescriptors,
  readCustomModelEntries,
  toCustomModelSetting,
  getProviderOptionBooleanSelectionValue,
  getProviderOptionStringSelectionValue,
  normalizeCustomModelSlug,
  normalizeModelSlug,
  modelSelectionsEqual,
} from "./model.ts";

it("keeps the Codex catalog display formatting", () => {
  expect(formatCodexModelName("gpt-5.3-codex-spark")).toBe("GPT-5.3-Codex-Spark");
  expect(formatCodexModelName("GPT Test")).toBe("GPT Test");
});

it.each([
  ["gpt-5.4", "GPT-5.4"],
  ["claude-opus-4-6", "Claude Opus 4.6"],
  ["claude-sonnet-4-20250514", "Claude Sonnet 4 20250514"],
  ["claude-opus-4-6[1m]", "Claude Opus 4.6[1m]"],
  ["openai/gpt-5.4-mini", "openai/GPT-5.4-Mini"],
  ["gemini-2.5-pro-preview-06-05", "Gemini 2.5 Pro Preview 06 05"],
  ["custom/model-v2", "custom/model-v2"],
  ["gpt-proxy", "gpt-proxy"],
  ["My Custom Model", "My Custom Model"],
])("formats a known model ID without losing its qualifiers: %s", (slug, expected) => {
  expect(formatModelSlugName(slug)).toBe(expected);
});

const codexCaps: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [
    {
      id: "reasoningEffort",
      label: "Reasoning",
      type: "select",
      options: [
        { id: "xhigh", label: "Extra High" },
        { id: "high", label: "High", isDefault: true },
      ],
      currentValue: "high",
    },
    {
      id: "fastMode",
      label: "Fast Mode",
      type: "boolean",
    },
  ],
});

describe("model slug normalization", () => {
  it("preserves exact custom slugs instead of expanding provider aliases", () => {
    // Claude aliases now resolve through the model catalog (#9084), so the
    // provider alias table passes unknown slugs through unchanged.
    const claude = ProviderDriverKind.make("claudeAgent");

    expect(normalizeModelSlug("opus", claude)).toBe("opus");
    expect(normalizeCustomModelSlug(" opus ")).toBe("opus");
  });
});

const claudeCaps: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [
    {
      id: "effort",
      label: "Reasoning",
      type: "select",
      options: [
        { id: "medium", label: "Medium" },
        { id: "high", label: "High", isDefault: true },
        { id: "ultrathink", label: "Ultrathink" },
      ],
      currentValue: "high",
      promptInjectedValues: ["ultrathink"],
    },
    {
      id: "contextWindow",
      label: "Context Window",
      type: "select",
      options: [
        { id: "200k", label: "200k" },
        { id: "1m", label: "1M", isDefault: true },
      ],
      currentValue: "1m",
    },
  ],
});

describe("descriptor helpers", () => {
  it("applies selection values to capability descriptors", () => {
    expect(
      getProviderOptionDescriptors({
        caps: claudeCaps,
        selections: [
          { id: "effort", value: "medium" },
          { id: "contextWindow", value: "200k" },
        ],
      }),
    ).toEqual([
      {
        id: "effort",
        label: "Reasoning",
        type: "select",
        options: [
          { id: "medium", label: "Medium" },
          { id: "high", label: "High", isDefault: true },
          { id: "ultrathink", label: "Ultrathink" },
        ],
        currentValue: "medium",
        promptInjectedValues: ["ultrathink"],
      },
      {
        id: "contextWindow",
        label: "Context Window",
        type: "select",
        options: [
          { id: "200k", label: "200k" },
          { id: "1m", label: "1M", isDefault: true },
        ],
        currentValue: "200k",
      },
    ]);
  });

  it("builds wire-format option selections from descriptors", () => {
    const descriptors = getProviderOptionDescriptors({
      caps: codexCaps,
      selections: [
        { id: "reasoningEffort", value: "high" },
        { id: "fastMode", value: true },
      ],
    });

    expect(buildProviderOptionSelectionsFromDescriptors(descriptors)).toEqual([
      { id: "reasoningEffort", value: "high" },
      { id: "fastMode", value: true },
    ]);
  });

  it("builds dispatch options only from explicit selections", () => {
    const descriptors = getProviderOptionDescriptors({
      caps: codexCaps,
      selections: [{ id: "fastMode", value: true }],
    });

    expect(buildExplicitProviderOptionSelectionsFromDescriptors(descriptors, undefined)).toBe(
      undefined,
    );
    expect(
      buildExplicitProviderOptionSelectionsFromDescriptors(descriptors, [
        { id: "fastMode", value: true },
      ]),
    ).toEqual([{ id: "fastMode", value: true }]);
  });

  it("stores option selection arrays in model selections", () => {
    expect(
      createModelSelection(ProviderInstanceId.make("codex"), "gpt-5.4", [
        { id: "reasoningEffort", value: "high" },
        { id: "fastMode", value: true },
      ]),
    ).toEqual({
      instanceId: "codex",
      model: "gpt-5.4",
      options: [
        { id: "reasoningEffort", value: "high" },
        { id: "fastMode", value: true },
      ],
    });
  });

  it("reads typed option selection values", () => {
    const selection = createModelSelection(ProviderInstanceId.make("codex"), "gpt-5.4", [
      { id: "reasoningEffort", value: "high" },
      { id: "fastMode", value: true },
    ]);

    expect(getProviderOptionStringSelectionValue(selection.options, "reasoningEffort")).toBe(
      "high",
    );
    expect(getProviderOptionStringSelectionValue(selection.options, "fastMode")).toBeUndefined();
    expect(getProviderOptionBooleanSelectionValue(selection.options, "fastMode")).toBe(true);
    expect(
      getProviderOptionBooleanSelectionValue(selection.options, "reasoningEffort"),
    ).toBeUndefined();
    expect(getModelSelectionStringOptionValue(selection, "reasoningEffort")).toBe("high");
    expect(getModelSelectionBooleanOptionValue(selection, "fastMode")).toBe(true);
  });

  it("compares complete model selections independent of option ordering", () => {
    const left = createModelSelection(ProviderInstanceId.make("codex"), "gpt-5.4", [
      { id: "reasoningEffort", value: "high" },
      { id: "fastMode", value: true },
    ]);
    const reordered = createModelSelection(ProviderInstanceId.make("codex"), "gpt-5.4", [
      { id: "fastMode", value: true },
      { id: "reasoningEffort", value: "high" },
    ]);

    expect(modelSelectionsEqual(left, reordered)).toBe(true);
    expect(
      modelSelectionsEqual(left, {
        ...reordered,
        options: [
          { id: "fastMode", value: true },
          { id: "reasoningEffort", value: "medium" },
        ],
      }),
    ).toBe(false);
    expect(modelSelectionsEqual(left, { ...reordered, model: "gpt-5.5" })).toBe(false);
  });
});

describe("configured reasoning effort", () => {
  const input = {
    modelSelection: createModelSelection(ProviderInstanceId.make("codex-work"), "gpt-5.4"),
    driverKind: ProviderDriverKind.make("codex"),
    capabilities: codexCaps,
    defaultModelSelection: createModelSelection(
      ProviderInstanceId.make("codex-personal"),
      "gpt-5.4",
      [{ id: "reasoningEffort", value: "xhigh" }],
    ),
    defaultDriverKind: ProviderDriverKind.make("codex"),
  };

  it("inherits the current configured effort across Codex accounts", () => {
    expect(getConfiguredReasoningEffort(input)).toBe("xhigh");
    expect(
      getConfiguredReasoningEffort({
        ...input,
        modelSelection: createModelSelection(ProviderInstanceId.make("codex-work"), " 5.4 "),
        defaultModelSelection: { ...input.defaultModelSelection, model: "openai.gpt-5.4" },
      }),
    ).toBe("xhigh");
    expect(
      getConfiguredReasoningEffort({
        ...input,
        modelSelection: { ...input.modelSelection, model: "openai.gpt-5.3" },
        defaultModelSelection: { ...input.defaultModelSelection, model: "gpt-5.3-codex" },
      }),
    ).toBe("xhigh");
  });

  it("preserves explicit effort while allowing unrelated options", () => {
    for (const value of ["high", "unsupported", true]) {
      const explicit = {
        ...input,
        modelSelection: {
          ...input.modelSelection,
          options: [{ id: "reasoningEffort", value }],
        },
      };
      expect(getConfiguredReasoningEffort(explicit)).toBeUndefined();
      expect(applyConfiguredReasoningEffortDefault(explicit)).toBe(codexCaps);
    }
    expect(
      getConfiguredReasoningEffort({
        ...input,
        modelSelection: { ...input.modelSelection, options: [{ id: "fastMode", value: true }] },
      }),
    ).toBe("xhigh");
  });

  it("does not inherit for other models or provider drivers", () => {
    expect(
      getConfiguredReasoningEffort({
        ...input,
        modelSelection: { ...input.modelSelection, model: "gpt-5.3-codex" },
      }),
    ).toBeUndefined();
    expect(
      getConfiguredReasoningEffort({ ...input, driverKind: ProviderDriverKind.make("claude") }),
    ).toBeUndefined();
    expect(
      getConfiguredReasoningEffort({
        ...input,
        defaultDriverKind: ProviderDriverKind.make("claude"),
      }),
    ).toBeUndefined();
    expect(
      getConfiguredReasoningEffort({ ...input, defaultDriverKind: undefined }),
    ).toBeUndefined();
  });

  it("leaves catalog fallback when the configured effort or capabilities are unavailable", () => {
    for (const value of ["unsupported", true]) {
      expect(
        getConfiguredReasoningEffort({
          ...input,
          defaultModelSelection: {
            ...input.defaultModelSelection,
            options: [{ id: "reasoningEffort", value }],
          },
        }),
      ).toBeUndefined();
    }
    expect(
      getConfiguredReasoningEffort({ ...input, defaultModelSelection: undefined }),
    ).toBeUndefined();
    expect(
      getConfiguredReasoningEffort({
        ...input,
        defaultModelSelection: createModelSelection(
          input.defaultModelSelection.instanceId,
          input.defaultModelSelection.model,
        ),
      }),
    ).toBeUndefined();
    expect(getConfiguredReasoningEffort({ ...input, capabilities: undefined })).toBeUndefined();
    expect(
      getConfiguredReasoningEffort({ ...input, capabilities: { optionDescriptors: [] } }),
    ).toBeUndefined();
    expect(
      getConfiguredReasoningEffort({
        ...input,
        capabilities: {
          optionDescriptors: [{ id: "reasoningEffort", label: "Reasoning", type: "boolean" }],
        },
      }),
    ).toBeUndefined();
    expect(
      getConfiguredReasoningEffort({
        ...input,
        capabilities: {
          optionDescriptors: [
            { id: "reasoningEffort", label: "Reasoning", type: "select", options: [] },
          ],
        },
      }),
    ).toBeUndefined();
    expect(
      applyConfiguredReasoningEffortDefault({ ...input, defaultModelSelection: undefined }),
    ).toBe(codexCaps);
    expect(
      applyConfiguredReasoningEffortDefault({ ...input, capabilities: undefined }),
    ).toBeUndefined();
  });

  it("updates the displayed effort while preserving catalog defaults and explicit dispatch options", () => {
    const inherited = applyConfiguredReasoningEffortDefault(input);
    expect(inherited?.optionDescriptors).toEqual([
      {
        id: "reasoningEffort",
        label: "Reasoning",
        type: "select",
        options: [
          { id: "xhigh", label: "Extra High" },
          { id: "high", label: "High", isDefault: true },
        ],
        currentValue: "xhigh",
      },
      codexCaps.optionDescriptors?.[1],
    ]);
    expect(inherited?.optionDescriptors?.[1]).toBe(codexCaps.optionDescriptors?.[1]);
    expect(codexCaps.optionDescriptors?.[0]?.currentValue).toBe("high");
    expect(input.modelSelection.options).toBeUndefined();
    expect(
      buildExplicitProviderOptionSelectionsFromDescriptors(inherited?.optionDescriptors, undefined),
    ).toBeUndefined();
  });

  it("reads only the current configured effort when defaults change", () => {
    const changed = {
      ...input,
      defaultModelSelection: {
        ...input.defaultModelSelection,
        options: [{ id: "reasoningEffort", value: "high" }],
      },
    };
    expect(getConfiguredReasoningEffort(changed)).toBe("high");
    expect(
      applyConfiguredReasoningEffortDefault(changed)?.optionDescriptors?.[0]?.currentValue,
    ).toBe("high");
    expect(getConfiguredReasoningEffort(input)).toBe("xhigh");
    expect(
      getConfiguredReasoningEffort({
        ...input,
        defaultModelSelection: { ...input.defaultModelSelection, model: "gpt-5.3-codex" },
      }),
    ).toBeUndefined();
  });
});

describe("applyClaudePromptEffortPrefix", () => {
  it("keeps slash commands intact when ultrathink is selected", () => {
    expect(applyClaudePromptEffortPrefix("/compact", "ultrathink")).toBe("/compact");
    expect(applyClaudePromptEffortPrefix(" /compact keep recent errors ", "ultrathink")).toBe(
      "/compact keep recent errors",
    );
    expect(applyClaudePromptEffortPrefix(" /review src/model.ts ", "ultrathink")).toBe(
      "/review src/model.ts",
    );
    expect(applyClaudePromptEffortPrefix("/security-review", "ultrathink")).toBe(
      "/security-review",
    );
    expect(applyClaudePromptEffortPrefix("/plugin:skill run", "ultrathink")).toBe(
      "/plugin:skill run",
    );
    expect(applyClaudePromptEffortPrefix("/deploy.prod to staging", "ultrathink")).toBe(
      "/deploy.prod to staging",
    );
  });

  it("still adds the ultrathink prefix to ordinary prompts", () => {
    expect(applyClaudePromptEffortPrefix("Investigate this failure", "ultrathink")).toBe(
      "Ultrathink:\nInvestigate this failure",
    );
    expect(applyClaudePromptEffortPrefix("/home/theo/app.ts crashed on load", "ultrathink")).toBe(
      "Ultrathink:\n/home/theo/app.ts crashed on load",
    );
  });
});

describe("readCustomModelEntries", () => {
  const capabilities: ModelCapabilities = {
    optionDescriptors: [
      {
        id: "effort",
        label: "Reasoning",
        type: "select",
        options: [{ id: "high", label: "High", isDefault: true }],
        currentValue: "high",
      },
    ],
  };

  it("resolves bare slugs and entries, trimming and deduplicating on slug", () => {
    expect(
      readCustomModelEntries([
        " bare ",
        { slug: "named", name: " Named ", capabilities },
        "bare",
        { slug: "named", name: "Second" },
        "",
        { name: "no slug" },
        42,
      ]),
    ).toEqual([
      { slug: "bare", name: "bare", capabilities: null },
      { slug: "named", name: "Named", capabilities },
    ]);
  });

  it("drops unparseable capabilities but keeps the entry", () => {
    expect(
      readCustomModelEntries([{ slug: "x", capabilities: { optionDescriptors: "nope" } }]),
    ).toEqual([{ slug: "x", name: "x", capabilities: null }]);
    expect(readCustomModelEntries("not a list")).toEqual([]);
  });

  it("writes the compact stored shape back", () => {
    expect(toCustomModelSetting({ slug: "x", name: "x", capabilities: null })).toBe("x");
    expect(
      toCustomModelSetting({ slug: "x", name: "x", capabilities: { optionDescriptors: [] } }),
    ).toBe("x");
    expect(toCustomModelSetting({ slug: "x", name: "X", capabilities })).toEqual({
      slug: "x",
      name: "X",
      capabilities,
    });
  });
});

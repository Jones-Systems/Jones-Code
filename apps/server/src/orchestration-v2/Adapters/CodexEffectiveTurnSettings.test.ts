import { ProviderDriverKind, ProviderInstanceId, type ModelSelection, type ServerProviderModel } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  codexTurnNeedsModelCatalog,
  resolveCodexEffectiveTurnSettings,
} from "./CodexEffectiveTurnSettings.ts";

const instanceId = ProviderInstanceId.make("codex-personal");
const selection: ModelSelection = { instanceId, model: "native-model" };
const model = (currentValue?: string): ServerProviderModel => ({
  slug: selection.model,
  name: "Native model",
  isCustom: false,
  capabilities: {
    optionDescriptors: [
      {
        id: "reasoningEffort",
        label: "Reasoning effort",
        type: "select",
        options: [
          { id: "low", label: "Low" },
          { id: "high", label: "High" },
          { id: "xhigh", label: "Extra high" },
          { id: "provider-default", label: "Provider default", isDefault: true },
        ],
        ...(currentValue === undefined ? {} : { currentValue }),
      },
    ],
  },
});

describe("Codex effective turn settings", () => {
  it("uses the current catalog value for an omitted effort", () => {
    expect(codexTurnNeedsModelCatalog({ instanceId, selection })).toBe(true);
    expect(
      resolveCodexEffectiveTurnSettings({ instanceId, selection, models: [model("low")], managed: false }),
    ).toEqual({ type: "resolved", settings: { model: selection.model, effort: "low" } });
  });

  it("uses the provider's marked default without inventing a fixed effort", () => {
    expect(
      resolveCodexEffectiveTurnSettings({ instanceId, selection, models: [model()], managed: false }),
    ).toEqual({ type: "resolved", settings: { model: selection.model, effort: "provider-default" } });
  });

  it("takes a refreshed catalog default on the next request without changing the selection", () => {
    const frozenSelection = Object.freeze({ ...selection });
    const resolve = (effort: string) => resolveCodexEffectiveTurnSettings({
      instanceId,
      selection: frozenSelection,
      models: [model(effort)],
      managed: false,
    });
    expect(resolve("low")).toEqual({ type: "resolved", settings: { model: selection.model, effort: "low" } });
    expect(resolve("high")).toEqual({ type: "resolved", settings: { model: selection.model, effort: "high" } });
    expect(frozenSelection).toEqual(selection);
    expect(frozenSelection).not.toHaveProperty("options");
  });

  it("preserves an explicit effort without touching the catalog", () => {
    const explicit = { ...selection, options: [{ id: "reasoningEffort", value: "xhigh" }] };
    expect(codexTurnNeedsModelCatalog({ instanceId, selection: explicit })).toBe(false);
    expect(resolveCodexEffectiveTurnSettings({
      instanceId,
      selection: explicit,
      get models(): ReadonlyArray<ServerProviderModel> { throw new Error("catalog must not be read"); },
      get configuredDefaultModelSelection(): never { throw new Error("configured default must not be read"); },
      managed: false,
    })).toEqual({ type: "resolved", settings: { model: selection.model, effort: "xhigh" } });
  });

  it.each([undefined, [], [{ ...model("high"), slug: "other-model" }], [{ ...model(), capabilities: null }]])(
    "omits effort when the selected model has no catalog default: %j",
    (models) => {
      expect(resolveCodexEffectiveTurnSettings({
        instanceId,
        selection,
        ...(models === undefined ? {} : { models }),
        managed: false,
      })).toEqual({ type: "resolved", settings: { model: selection.model } });
    },
  );

  it("does not treat a boolean descriptor as a reasoning default", () => {
    const booleanModel = { ...model(), capabilities: {
      optionDescriptors: [{ id: "reasoningEffort", label: "Reasoning", type: "boolean" as const, currentValue: true }],
    } };
    expect(resolveCodexEffectiveTurnSettings({ instanceId, selection, models: [booleanModel], managed: false }))
      .toEqual({ type: "resolved", settings: { model: selection.model } });
  });

  it.each([true, ""])("rejects an invalid explicit native effort %j without substituting a default", (value) => {
    const invalid = { ...selection, options: [{ id: "reasoningEffort", value }] };
    expect(codexTurnNeedsModelCatalog({ instanceId, selection: invalid })).toBe(false);
    expect(resolveCodexEffectiveTurnSettings({ instanceId, selection: invalid, models: [model("low")], managed: false }))
      .toEqual({ type: "rejected", reason: "invalid_native_settings" });
  });

  it("rejects another instance's selection before reading its catalog", () => {
    const other = { ...selection, instanceId: ProviderInstanceId.make("codex-work") };
    expect(codexTurnNeedsModelCatalog({ instanceId, selection: other })).toBe(false);
    expect(resolveCodexEffectiveTurnSettings({
      instanceId,
      selection: other,
      get models(): ReadonlyArray<ServerProviderModel> { throw new Error("foreign catalog must not be read"); },
      managed: false,
    })).toEqual({ type: "rejected", reason: "instance_mismatch" });
  });

  it.each([
    { options: [{ id: "fastMode", value: true }], expectedTier: "fast" },
    {
      options: [{ id: "serviceTier", value: "priority" }, { id: "fastMode", value: true }],
      expectedTier: "priority",
    },
  ])("preserves service-tier option precedence for an ordinary instance: %j", ({ options, expectedTier }) => {
    expect(resolveCodexEffectiveTurnSettings({ instanceId, selection: { ...selection, options }, managed: false }))
      .toEqual({ type: "resolved", settings: { model: selection.model, serviceTier: expectedTier } });
  });

  it("omits unsupported managed tiers while retaining effort and the user's options", () => {
    const options = Object.freeze([
      Object.freeze({ id: "serviceTier", value: "priority" }),
      Object.freeze({ id: "reasoningEffort", value: "high" }),
    ]);
    const managedSelection = Object.freeze({ ...selection, options });
    expect(resolveCodexEffectiveTurnSettings({ instanceId, selection: managedSelection, managed: true }))
      .toEqual({ type: "resolved", settings: { model: selection.model, effort: "high" } });
    expect(managedSelection.options).toBe(options);
    expect(managedSelection.options[0]?.value).toBe("priority");
  });

  it("inherits the current configured effort across accounts without changing the selected account", () => {
    const selected = Object.freeze({ ...selection });
    const resolve = (effort: string) => resolveCodexEffectiveTurnSettings({
      instanceId,
      selection: selected,
      models: [model("low")],
      managed: false,
      configuredDefaultModelSelection: {
        driver: ProviderDriverKind.make("codex"),
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex-other-account"),
          model: selection.model,
          options: [{ id: "reasoningEffort", value: effort }],
        },
      },
    });
    expect(resolve("high")).toEqual({ type: "resolved", settings: { model: selection.model, effort: "high" } });
    expect(resolve("xhigh")).toEqual({ type: "resolved", settings: { model: selection.model, effort: "xhigh" } });
    expect(selected).toEqual(selection);
    expect(selected).not.toHaveProperty("options");
  });

  it("uses Codex family and catalog aliases for capabilities while preserving the dispatch model", () => {
    expect(resolveCodexEffectiveTurnSettings({
      instanceId,
      selection: { ...selection, model: "5.4" },
      models: [{ ...model("low"), slug: "provider-catalog-name", aliases: ["openai.gpt-5.4"] }],
      managed: false,
      configuredDefaultModelSelection: {
        driver: ProviderDriverKind.make("codex"),
        modelSelection: {
          instanceId: ProviderInstanceId.make("codex-other-account"),
          model: "openai.gpt-5.4",
          options: [{ id: "reasoningEffort", value: "high" }],
        },
      },
    })).toEqual({ type: "resolved", settings: { model: "5.4", effort: "high" } });
  });

  it.each([
    { driver: ProviderDriverKind.make("claudeAgent"), configuredModel: selection.model, value: "high" },
    { driver: ProviderDriverKind.make("codex"), configuredModel: "different-model", value: "high" },
    { driver: ProviderDriverKind.make("codex"), configuredModel: selection.model, value: "not-advertised" },
  ])("uses the catalog default when the current configured effort is ineligible: %j", ({ driver, configuredModel, value }) => {
    expect(resolveCodexEffectiveTurnSettings({
      instanceId,
      selection,
      models: [model("low")],
      managed: false,
      configuredDefaultModelSelection: {
        driver,
        modelSelection: { ...selection, model: configuredModel, options: [{ id: "reasoningEffort", value }] },
      },
    })).toEqual({ type: "resolved", settings: { model: selection.model, effort: "low" } });
  });

  it("does not inherit a configured effort without current model capabilities", () => {
    expect(resolveCodexEffectiveTurnSettings({
      instanceId,
      selection,
      managed: false,
      configuredDefaultModelSelection: {
        driver: ProviderDriverKind.make("codex"),
        modelSelection: { ...selection, options: [{ id: "reasoningEffort", value: "high" }] },
      },
    })).toEqual({ type: "resolved", settings: { model: selection.model } });
  });
});

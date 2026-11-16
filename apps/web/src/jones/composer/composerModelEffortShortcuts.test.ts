import { describe, expect, it } from "vite-plus/test";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  type ServerProviderModel,
} from "@t3tools/contracts";
import { resolveModelEffortShortcutRows, withShortcutEffort } from "./composerModelEffortShortcuts";

const instanceId = ProviderInstanceId.make("shortcut-account");
function model(
  slug: string,
  values = ["medium", "high", "xhigh"],
  isLegacy = false,
): ServerProviderModel {
  return {
    slug,
    name: slug,
    isCustom: false,
    isLegacy,
    capabilities: {
      optionDescriptors: [
        {
          id: slug.includes("claude") ? "effort" : "reasoningEffort",
          label: "Effort",
          type: "select",
          options: values.map((id) => ({ id, label: id })),
        },
      ],
    },
  };
}
function rows(driver: string, models: ServerProviderModel[]) {
  return resolveModelEffortShortcutRows({
    driverKind: ProviderDriverKind.make(driver),
    instanceId,
    models,
  });
}
describe("model effort shortcuts", () => {
  it("resolves exact Codex families without rewriting provider-owned aliases", () => {
    const result = rows("codex", [
      model("gpt-6"),
      model("openai.gpt-6.1-sol"),
      model("gpt-6-astra"),
    ]);
    expect(result?.map((row) => row.model)).toEqual(["openai.gpt-6.1-sol", "gpt-6-astra"]);
    expect(result?.flatMap((row) => row.cells).every((cell) => cell.disabledReason === null)).toBe(
      true,
    );
  });
  it("selects current Claude families with full support before latest version, ignoring legacy and context variants", () => {
    const result = rows("claudeAgent", [
      model("claude-opus-5"),
      model("claude-opus-5-5"),
      model("claude-opus-6", ["high"]),
      model("claude-opus-7", undefined, true),
      model("claude-opus-8-1m"),
      model("claude-sonnet-5-5"),
    ]);
    expect(result?.map((row) => row.model)).toEqual(["claude-opus-5-5", "claude-sonnet-5-5"]);
  });
  it("disables absent families and unsupported efforts rather than replacing them", () => {
    const result = rows("codex", [model("gpt-6.1-sol", ["medium", "high"])]);
    expect(result?.[0]?.cells.map((cell) => !!cell.disabledReason)).toEqual([false, false, true]);
    expect(result?.[1]?.model).toBeUndefined();
    expect(
      result?.[1]?.cells.every((cell) => cell.disabledReason?.includes("isn't available")),
    ).toBe(true);
    expect(rows("cursor", [])).toBeNull();
  });
  it("fails closed on absent target metadata and missing account identity", () => {
    const target = { ...model("gpt-6.1-sol"), capabilities: null };
    expect(rows("codex", [target])?.[0]?.cells.every((cell) => cell.disabledReason)).toBe(true);
    expect(
      resolveModelEffortShortcutRows({
        driverKind: ProviderDriverKind.make("codex"),
        models: [model("gpt-6.1-sol")],
      })?.[0]?.cells.every((cell) => cell.disabledReason),
    ).toBe(true);
  });
  it("builds one explicit target selection without accepting a fallback or unsupported effort", () => {
    const selection = {
      instanceId,
      model: "gpt-6-astra",
      options: [
        { id: "reasoningEffort", value: "medium" },
        { id: "fastMode", value: true },
      ],
    };
    const models = [model("gpt-6-astra")];
    expect(
      withShortcutEffort(selection, "gpt-6-astra", models, {
        id: "reasoningEffort",
        value: "xhigh",
      }),
    ).toEqual({
      ...selection,
      options: [
        { id: "fastMode", value: true },
        { id: "reasoningEffort", value: "xhigh" },
      ],
    });
    expect(
      withShortcutEffort(selection, "gpt-6.1-sol", models, {
        id: "reasoningEffort",
        value: "high",
      }),
    ).toBeNull();
    expect(
      withShortcutEffort(selection, selection.model, models, {
        id: "reasoningEffort",
        value: "max",
      }),
    ).toBeNull();
    expect(
      withShortcutEffort(selection, selection.model, [], { id: "reasoningEffort", value: "high" }),
    ).toBeNull();
  });
});

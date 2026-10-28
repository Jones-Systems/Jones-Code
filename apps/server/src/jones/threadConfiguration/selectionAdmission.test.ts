import { describe, expect, it } from "vite-plus/test";
import {
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  type ModelSelection,
  type ServerProvider,
} from "@t3tools/contracts";

import { selectionAdmissionReason } from "./selectionAdmission.ts";

const instanceId = ProviderInstanceId.make("admission-codex");
const projectId = ProjectId.make("admission-project");
const selection: ModelSelection = { instanceId, model: "visible-model" };
const provider: ServerProvider = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  enabled: true,
  installed: true,
  version: "synthetic",
  status: "ready",
  auth: { status: "authenticated" },
  checkedAt: "2026-10-26T00:00:00.000Z",
  models: [
    {
      slug: selection.model,
      name: "Visible model",
      isCustom: false,
      capabilities: {
        optionDescriptors: [
          { id: "fastMode", label: "Fast", type: "boolean" },
          {
            id: "effort",
            label: "Effort",
            type: "select",
            options: [{ id: "medium", label: "Medium" }],
          },
        ],
      },
    },
    { slug: "hidden-model", name: "Hidden model", isCustom: false, capabilities: null },
    { slug: "custom-model", name: "Custom model", isCustom: true, capabilities: null },
    { slug: "no-options", name: "No options", isCustom: false, capabilities: null },
  ],
  slashCommands: [],
  skills: [],
};
const settings: Parameters<typeof selectionAdmissionReason>[0]["settings"] = {
  projectSettingsOverrides: {},
  providerModelPreferences: {
    [instanceId]: { hiddenModels: ["hidden-model", "custom-model"], modelOrder: [] },
  },
};

function admit(overrides: Partial<Parameters<typeof selectionAdmissionReason>[0]> = {}) {
  return selectionAdmissionReason({
    providers: [provider],
    selection,
    settings,
    projectId,
    ...overrides,
  });
}

describe("selection admission", () => {
  it.each([
    { name: "missing", providers: [] },
    { name: "disabled", providers: [{ ...provider, enabled: false }] },
    { name: "uninstalled", providers: [{ ...provider, installed: false }] },
    {
      name: "another instance",
      providers: [{ ...provider, instanceId: ProviderInstanceId.make("another-codex") }],
    },
  ])("rejects a $name provider before model or option validation", ({ providers }) => {
    expect(
      admit({
        providers,
        selection: { ...selection, model: "missing", options: [{ id: "unknown", value: true }] },
      }),
    ).toBe("provider_unavailable");
  });

  it("rejects an explicit project disable before model validation", () => {
    expect(
      admit({
        selection: { ...selection, model: "missing" },
        settings: {
          ...settings,
          projectSettingsOverrides: {
            [projectId]: { providerInstances: { [instanceId]: { enabled: false } } },
          },
        },
      }),
    ).toBe("provider_unavailable");
  });

  it("accepts an explicit project enable and ignores other projects' disables", () => {
    expect(
      admit({
        settings: {
          ...settings,
          projectSettingsOverrides: {
            [projectId]: { providerInstances: { [instanceId]: { enabled: true } } },
            [ProjectId.make("other-project")]: {
              providerInstances: { [instanceId]: { enabled: false } },
            },
          },
        },
      }),
    ).toBeNull();
  });

  it.each(["hidden-model", "missing", "VISIBLE-MODEL"])(
    "rejects an ordinary hidden or inexact model: %s",
    (model) => {
      expect(admit({ selection: { ...selection, model } })).toBe("invalid_selection");
    },
  );

  it("accepts a hidden custom model", () => {
    expect(admit({ selection: { ...selection, model: "custom-model" } })).toBeNull();
  });

  it.each([
    {
      options: [
        { id: "fastMode", value: true },
        { id: "fastMode", value: false },
      ],
    },
    { options: [{ id: "unknown", value: true }] },
    { options: [{ id: "fastMode", value: "true" }] },
    { options: [{ id: "effort", value: true }] },
    { options: [{ id: "effort", value: "high" }] },
  ] satisfies Array<{ options: NonNullable<ModelSelection["options"]> }>)(
    "rejects duplicate, unknown or invalid option values: $options",
    ({ options }) => {
      expect(admit({ selection: { ...selection, options } })).toBe("invalid_selection");
    },
  );

  it.each([
    { options: undefined },
    { options: [] },
    { options: [{ id: "fastMode", value: false }] },
    {
      options: [
        { id: "fastMode", value: true },
        { id: "effort", value: "medium" },
      ],
    },
  ] satisfies Array<{ options: ModelSelection["options"] }>)(
    "accepts valid or omitted options: $options",
    ({ options }) => {
      expect(
        admit({
          selection: { ...selection, ...(options === undefined ? {} : { options }) },
        }),
      ).toBeNull();
    },
  );

  it("accepts models without descriptors only when options are omitted", () => {
    expect(admit({ selection: { ...selection, model: "no-options" } })).toBeNull();
    expect(
      admit({
        selection: {
          ...selection,
          model: "no-options",
          options: [{ id: "fastMode", value: true }],
        },
      }),
    ).toBe("invalid_selection");
  });

  it("accepts visible models when model preferences are absent", () => {
    expect(admit({ settings: { ...settings, providerModelPreferences: null } })).toBeNull();
  });
});

import { describe, expect, it } from "vite-plus/test";
import { ProviderDriverKind, ProviderInstanceId, type ServerProvider } from "@t3tools/contracts";
import { DEFAULT_UNIFIED_SETTINGS } from "@t3tools/contracts/settings";
import { deriveProviderInstanceEntries } from "../../providerInstances";
import { getAppModelOptionsForInstance } from "../../modelSelection";
import { resolveProviderInstanceShortcut } from "./ProviderInstanceShortcuts";

const instanceId = ProviderInstanceId.make("codex_personal");
const entry = deriveProviderInstanceEntries([
  {
    instanceId,
    driver: ProviderDriverKind.make("codex"),
    displayName: "My Codex",
    enabled: true,
    installed: true,
    status: "ready",
    version: null,
    auth: { status: "authenticated" },
    checkedAt: "2026-09-29T00:00:00.000Z",
    models: [
      { slug: "default", name: "Default", isDefault: true, isCustom: false, capabilities: {} },
      {
        slug: "current",
        name: "Current",
        aliases: ["current-alias"],
        isCustom: false,
        capabilities: {},
      },
      { slug: "remembered", name: "Remembered", isCustom: false, capabilities: {} },
    ],
    slashCommands: [],
    skills: [],
  } satisfies ServerProvider,
])[0]!;
const options = getAppModelOptionsForInstance(DEFAULT_UNIFIED_SETTINGS, entry);
const base = {
  entry,
  options,
  rememberedSelection: undefined,
  currentDriver: ProviderDriverKind.make("codex"),
  currentModel: "current",
  lockedProvider: null,
  lockedContinuationGroupKey: null,
  getModelDisabledReason: () => null,
};

describe("provider account model resolution", () => {
  it("prefers the selectable model remembered for that exact account", () => {
    expect(
      resolveProviderInstanceShortcut({
        ...base,
        rememberedSelection: { instanceId, model: "remembered" },
      }).model?.slug,
    ).toBe("remembered");
  });

  it("rejects memory belonging to a sibling account and uses a catalog-supported same-driver model", () => {
    expect(
      resolveProviderInstanceShortcut({
        ...base,
        currentModel: "current-alias",
        rememberedSelection: {
          instanceId: ProviderInstanceId.make("codex_work"),
          model: "remembered",
        },
      }).model?.slug,
    ).toBe("current");
  });

  it.each([
    { currentModel: "unsupported" },
    { currentDriver: ProviderDriverKind.make("claudeAgent") },
    { rememberedSelection: { instanceId, model: "removed" }, currentModel: "removed" },
    {
      options: options.map((option) =>
        option.slug === "current" ? { ...option, isUnavailable: true } : option,
      ),
    },
  ])(
    "uses the target picker default instead of interpreting a fallback as support: %j",
    (change) => {
      expect(resolveProviderInstanceShortcut({ ...base, ...change }).model?.slug).toBe("default");
    },
  );

  it("skips session-blocked remembered and current models without choosing another arbitrary model", () => {
    const resolve = (slug: string) =>
      slug === "default" ? null : "This session cannot change models.";
    expect(
      resolveProviderInstanceShortcut({
        ...base,
        rememberedSelection: { instanceId, model: "remembered" },
        getModelDisabledReason: (_instanceId, slug) => resolve(slug),
      }).model?.slug,
    ).toBe("default");
    expect(
      resolveProviderInstanceShortcut({
        ...base,
        getModelDisabledReason: () => "This session cannot change accounts.",
      }).disabledReason,
    ).toBe("This session cannot change accounts.");
  });

  it.each([
    { entry: { ...entry, enabled: false } },
    { entry: { ...entry, status: "error" as const } },
    { entry: { ...entry, isAvailable: false } },
    { lockedProvider: ProviderDriverKind.make("claudeAgent") },
    { lockedProvider: entry.driverKind, lockedContinuationGroupKey: "other-account" },
    { options: [] },
    { options: options.map((option) => ({ ...option, isUnavailable: true })) },
  ])("disables unavailable, incompatible, or empty target catalogs: %j", (change) => {
    expect(resolveProviderInstanceShortcut({ ...base, ...change }).disabledReason).not.toBeNull();
  });

  it("allows same-driver account continuation only in the actual shared continuation group", () => {
    expect(
      resolveProviderInstanceShortcut({
        ...base,
        entry: { ...entry, continuationGroupKey: "shared" },
        lockedProvider: entry.driverKind,
        lockedContinuationGroupKey: "shared",
      }).disabledReason,
    ).toBeNull();
  });

  it("disables an unavailable picker default instead of inventing a different fallback", () => {
    const result = resolveProviderInstanceShortcut({
      ...base,
      currentModel: "missing",
      options: options.map((option) =>
        option.isDefault ? { ...option, isUnavailable: true } : option,
      ),
    });
    expect(result.model?.slug).toBe("default");
    expect(result.disabledReason).toBe("This model is unavailable.");
  });

  it("uses instance-specific custom catalogs and settings-hidden model preferences", () => {
    const settings = {
      ...DEFAULT_UNIFIED_SETTINGS,
      providerModelPreferences: {
        [instanceId]: { hiddenModels: ["current", "remembered"], modelOrder: [] },
      },
    };
    const targetOptions = getAppModelOptionsForInstance(settings, entry);
    expect(
      resolveProviderInstanceShortcut({
        ...base,
        options: targetOptions,
        rememberedSelection: { instanceId, model: "remembered" },
      }).model?.slug,
    ).toBe("default");
    const dynamic = { ...entry, driverKind: ProviderDriverKind.make("opencode"), models: [] };
    const missing = getAppModelOptionsForInstance(
      DEFAULT_UNIFIED_SETTINGS,
      dynamic,
      "removed-dynamic-model",
    );
    expect(missing[0]?.isUnavailable).toBe(true);
    expect(
      resolveProviderInstanceShortcut({ ...base, entry: dynamic, options: missing }).disabledReason,
    ).not.toBeNull();
  });

  it("never borrows a custom model from another account of the same driver", () => {
    const siblingId = ProviderInstanceId.make("codex_work");
    const settings = {
      ...DEFAULT_UNIFIED_SETTINGS,
      providerInstances: {
        [instanceId]: { driver: entry.driverKind, config: { customModels: ["personal-model"] } },
        [siblingId]: { driver: entry.driverKind, config: { customModels: ["work-model"] } },
      },
    };
    const personal = getAppModelOptionsForInstance(settings, entry);
    const sibling = getAppModelOptionsForInstance(settings, { ...entry, instanceId: siblingId });
    expect(
      resolveProviderInstanceShortcut({
        ...base,
        options: personal,
        currentModel: "personal-model",
      }).model?.slug,
    ).toBe("personal-model");
    expect(
      resolveProviderInstanceShortcut({ ...base, options: sibling, currentModel: "personal-model" })
        .model?.slug,
    ).toBe("default");
  });

  it("keeps an Antigravity lock without continuation metadata on the exact session account", () => {
    const driver = ProviderDriverKind.make("antigravity");
    const activeId = ProviderInstanceId.make("antigravity_work");
    const googleEntry = { ...entry, driverKind: driver, instanceId: activeId };
    const locked = {
      ...base,
      entry: googleEntry,
      currentDriver: driver,
      lockedProvider: driver,
      lockedInstanceId: activeId,
      lockedContinuationGroupKey: null,
    };
    expect(resolveProviderInstanceShortcut(locked).disabledReason).toBeNull();
    expect(
      resolveProviderInstanceShortcut({
        ...locked,
        entry: { ...googleEntry, instanceId: ProviderInstanceId.make("antigravity_personal") },
      }).disabledReason,
    ).toBe("Start a new thread to use this provider.");
  });

  it("permits Antigravity sibling accounts only with the same supplied continuation group", () => {
    const driver = ProviderDriverKind.make("antigravity");
    const input = {
      ...base,
      entry: {
        ...entry,
        driverKind: driver,
        instanceId: ProviderInstanceId.make("antigravity_personal"),
        continuationGroupKey: "shared-google-profile",
      },
      currentDriver: driver,
      lockedProvider: driver,
      lockedInstanceId: ProviderInstanceId.make("antigravity_work"),
      lockedContinuationGroupKey: "shared-google-profile",
    };
    expect(resolveProviderInstanceShortcut(input).disabledReason).toBeNull();
    expect(
      resolveProviderInstanceShortcut({
        ...input,
        entry: { ...input.entry, continuationGroupKey: "different-google-profile" },
      }).disabledReason,
    ).toBe("Start a new thread to use this provider.");
  });
});

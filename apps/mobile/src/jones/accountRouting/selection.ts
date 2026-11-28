import {
  isManagedDriverAllowed,
  ProviderDriverKind,
  resolveProjectProviderInstanceEnabled,
  textGeneratorForSelection,
  type ModelSelection,
  type ProjectId,
  type ServerConfig,
} from "@t3tools/contracts";
import { msg } from "gt-react-native";

import { m } from "../../i18n/messages";
import type { ModelOption, ProviderGroup } from "../../lib/modelOptions";

const driverLabels = {
  codex: msg("Codex Auto"),
  claudeAgent: msg("Claude Auto"),
};
type AutoDriver = keyof typeof driverLabels;

const groupKey = ":account-auto";
const optionKey = (driver: AutoDriver) => `${groupKey}:${driver}`;

export function isAccountAutoOption(option: ModelOption): boolean {
  return option.providerKey === groupKey;
}

export function sameModelSelection(left: ModelSelection | undefined, right: ModelSelection) {
  return (
    left?.instanceId === right.instanceId &&
    left.model === right.model &&
    JSON.stringify(left.options ?? []) === JSON.stringify(right.options ?? [])
  );
}

export function canUseAutomaticAccounts(config: ServerConfig | null | undefined): boolean {
  return config?.environment?.capabilities.accountAutoRouting === true;
}

export function automaticAccountOptions(input: {
  readonly config: ServerConfig | null | undefined;
  readonly projectId: ProjectId | null;
  readonly models: ReadonlyArray<ModelOption>;
  readonly preferred: ModelSelection | null;
}): ReadonlyArray<ModelOption> {
  const { config, projectId, models, preferred } = input;
  if (!config || !canUseAutomaticAccounts(config)) return [];

  return (["codex", "claudeAgent"] as const).flatMap((driver) => {
    if (!isManagedDriverAllowed(config.managedPolicy, ProviderDriverKind.make(driver))) return [];
    const accounts = new Set(
      config.providers
        .filter(
          (provider) =>
            provider.driver === driver &&
            config.settings.providerInstances[provider.instanceId]?.driver === driver &&
            resolveProjectProviderInstanceEnabled(
              config.settings,
              projectId,
              provider.instanceId,
            ) &&
            textGeneratorForSelection(config.settings, provider.instanceId) === undefined &&
            provider.installed &&
            provider.status !== "error" &&
            provider.status !== "disabled" &&
            provider.auth.status !== "unauthenticated" &&
            provider.availability !== "unavailable" &&
            provider.models.length > 0,
        )
        .map((provider) => provider.instanceId),
    );
    if (accounts.size < 2) return [];
    const candidates = models.filter(
      (model) =>
        !model.isUnavailable && !model.isHidden && accounts.has(model.selection.instanceId),
    );
    const chosen =
      candidates.find(
        (model) =>
          model.selection.instanceId === preferred?.instanceId &&
          model.selection.model === preferred.model,
      ) ??
      candidates.find((model) => model.isDefault && !model.isLegacy) ??
      candidates.find((model) => !model.isLegacy) ??
      candidates[0];
    if (!chosen) return [];
    const label = m(driverLabels[driver]);
    const selection =
      preferred?.instanceId === chosen.selection.instanceId &&
      preferred.model === chosen.selection.model
        ? preferred
        : chosen.selection;
    const {
      providerBadge: _badge,
      providerSubProviders: _subProviders,
      providerUpdateRequired: _update,
      ...display
    } = chosen;
    return [
      {
        ...display,
        key: optionKey(driver),
        label,
        shortName: label,
        subtitle: chosen.label,
        providerKey: groupKey,
        providerLabel: m(msg("Automatic accounts")),
        canHide: false,
        isDefault: false,
        isLegacy: false,
        selection,
      },
    ];
  });
}

export function withAutomaticAccountGroup(
  groups: ReadonlyArray<ProviderGroup>,
  options: ReadonlyArray<ModelOption>,
): ReadonlyArray<ProviderGroup> {
  return options.length === 0
    ? groups
    : [
        { providerKey: groupKey, providerLabel: m(msg("Automatic accounts")), models: options },
        ...groups,
      ];
}

export function automaticAccountLabel(
  config: ServerConfig | null | undefined,
  model: ModelSelection,
) {
  const driver =
    config?.providers.find((provider) => provider.instanceId === model.instanceId)?.driver ??
    config?.settings.providerInstances[model.instanceId]?.driver;
  if (driver === "codex") return m(driverLabels.codex);
  if (driver === "claudeAgent") return m(driverLabels.claudeAgent);
  return m(msg("Auto"));
}

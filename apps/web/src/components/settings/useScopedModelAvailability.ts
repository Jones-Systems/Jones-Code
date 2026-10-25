import type { ProviderInstanceId, UnifiedSettings } from "@t3tools/contracts";

import { mergeEnvironmentSettings } from "../../hooks/useSettings";
import { scopedModelDisabledReason } from "../../jones/settings/modelSelectionCorrespondence";
import { getCustomModelOptionsByInstance } from "../../modelSelection";
import {
  applyProviderInstanceSettings,
  deriveProviderInstanceEntries,
  type ProviderInstanceEntry,
} from "../../providerInstances";
import { useEnvironmentProviders, useEnvironments } from "../../state/environments";
import { useSettingsScope } from "./SettingsScopeContext";

export function useScopedModelDisabledReason(
  settings: UnifiedSettings,
  entries: readonly ProviderInstanceEntry[],
  options: { readonly textGeneration?: boolean } = { textGeneration: true },
) {
  const { target, targets } = useSettingsScope();
  const { environments } = useEnvironments();
  const providersByEnvironment = useEnvironmentProviders();
  return (instanceId: ProviderInstanceId, model: string): string | null => {
    const sourceEntry = entries.find((entry) => entry.instanceId === instanceId);
    for (const candidate of targets) {
      if (target && candidate.environmentId !== target.environmentId) {
        const reason = scopedModelDisabledReason(
          { representative: target, providersByEnvironment },
          candidate,
          { instanceId, model },
          mergeEnvironmentSettings(candidate.settings, settings),
          options.textGeneration,
        );
        if (reason) return reason;
        continue;
      }
      const environment = environments.find(
        (entry) => entry.environmentId === candidate.environmentId,
      );
      const providers = providersByEnvironment.get(candidate.environmentId);
      if (!environment?.serverConfig || !providers) continue;
      const entry = applyProviderInstanceSettings(
        deriveProviderInstanceEntries(providers),
        candidate.settings,
        candidate.projectId,
      ).find((option) => option.instanceId === instanceId);
      const modelOptions = getCustomModelOptionsByInstance(
        mergeEnvironmentSettings(candidate.settings, settings),
        providers,
      ).get(instanceId);
      if (
        !entry?.enabled ||
        !entry.isAvailable ||
        (options.textGeneration && entry.snapshot.supportsTextGeneration === false) ||
        entry.driverKind !== sourceEntry?.driverKind ||
        !modelOptions?.some((option) => option.slug === model && !option.isUnavailable)
      ) {
        return `This model is unavailable on ${environment?.label ?? "a selected environment"}. Select that environment to choose its model separately.`;
      }
    }
    return null;
  };
}

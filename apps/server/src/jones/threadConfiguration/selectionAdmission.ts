import type { ModelSelection, ProjectId, ServerProvider, ServerSettings } from "@t3tools/contracts";

export function selectionAdmissionReason(input: {
  readonly providers: ReadonlyArray<ServerProvider>;
  readonly selection: ModelSelection;
  readonly settings: Pick<ServerSettings, "projectSettingsOverrides" | "providerModelPreferences">;
  readonly projectId: ProjectId;
}): "provider_unavailable" | "invalid_selection" | null {
  const { providers, selection, settings, projectId } = input;
  const provider = providers.find((provider) => provider.instanceId === selection.instanceId);
  const projectEnabled =
    settings.projectSettingsOverrides[projectId]?.providerInstances?.[selection.instanceId]
      ?.enabled;
  if (
    provider === undefined ||
    !provider.enabled ||
    !provider.installed ||
    projectEnabled === false
  )
    return "provider_unavailable";
  const model = provider.models.find(
    (model) =>
      model.slug === selection.model &&
      (model.isCustom ||
        !settings.providerModelPreferences?.[provider.instanceId]?.hiddenModels.includes(
          model.slug,
        )),
  );
  if (model === undefined) return "invalid_selection";
  const seen = new Set<string>();
  for (const option of selection.options ?? []) {
    const descriptor = model.capabilities?.optionDescriptors?.find(
      (descriptor) => descriptor.id === option.id,
    );
    if (
      seen.has(option.id) ||
      descriptor === undefined ||
      (descriptor.type === "boolean"
        ? typeof option.value !== "boolean"
        : !descriptor.options.some((choice) => choice.id === option.value))
    )
      return "invalid_selection";
    seen.add(option.id);
  }
  return null;
}

import type { ProviderOptionDescriptor, ProviderOptionSelection } from "@t3tools/contracts";
import {
  getTraitSelectChange,
  replaceDescriptorCurrentValue,
} from "@t3tools/client-runtime/model-traits";
import { buildExplicitProviderOptionSelectionsFromDescriptors } from "@t3tools/shared/model";

type Options = ReadonlyArray<ProviderOptionSelection>;

export function buildTraitsOptionSelections(
  descriptors: ReadonlyArray<ProviderOptionDescriptor>,
  selections: Options | null | undefined,
  change: ProviderOptionSelection,
): Options | undefined {
  const descriptorIds = new Set(descriptors.map((descriptor) => descriptor.id));
  const normalized = buildExplicitProviderOptionSelectionsFromDescriptors(
    replaceDescriptorCurrentValue(descriptors, change.id, change.value),
    [...(selections ?? []), change],
  );
  const preserved = (selections ?? []).filter((selection) => !descriptorIds.has(selection.id));
  const next = [...(normalized ?? []), ...preserved];
  return next.length > 0 ? next : undefined;
}

export function getComposerTraitSelectChange(
  selected: Parameters<typeof getTraitSelectChange>[0],
  descriptor: Parameters<typeof getTraitSelectChange>[1],
  prompt: string,
  value: string,
  modelOptions: Options | null | undefined,
  allowPromptInjectedEffort: boolean,
) {
  if (descriptor.promptInjectedValues?.includes(value)) {
    if (
      !allowPromptInjectedEffort ||
      descriptor.id !== selected.primarySelectDescriptor?.id ||
      selected.ultrathinkInBodyText ||
      value !== "ultrathink"
    )
      return null;
    // The retained external shortcut edits the prompt; it never persists a synthetic option.
    return { prompt: `Ultrathink:\n${prompt.replace(/^Ultrathink:\s*/i, "")}` };
  }
  const change = getTraitSelectChange(selected, descriptor, prompt, value);
  if (!change) return null;
  return {
    ...change,
    modelOptions: buildTraitsOptionSelections(selected.descriptors, modelOptions, {
      id: descriptor.id,
      value,
    }),
  };
}

import type {
  ModelSelection,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderOptionDescriptor,
  ServerProviderModel,
} from "@t3tools/contracts";
import { codexModelFamily } from "@t3tools/shared/model";

export type ShortcutEffort = { id: string; value: string };
export type ComposerModelSelectOptions = { focusComposer?: boolean; effort?: ShortcutEffort };

const EFFORTS = [
  { value: "medium", label: "Medium" },
  { value: "high", label: "High" },
  { value: "xhigh", label: "Extra High" },
] as const;

function effortDescriptor(model: ServerProviderModel | undefined, id: string) {
  return model?.capabilities?.optionDescriptors?.find(
    (descriptor): descriptor is Extract<ProviderOptionDescriptor, { type: "select" }> =>
      descriptor.id === id && descriptor.type === "select",
  );
}

export function resolveModelEffortShortcutRows(input: {
  driverKind: ProviderDriverKind;
  instanceId?: ProviderInstanceId;
  models: ReadonlyArray<ServerProviderModel>;
  getModelDisabledReason?: (instanceId: ProviderInstanceId, model: string) => string | null;
}) {
  if (input.driverKind !== "codex" && input.driverKind !== "claudeAgent") return null;
  const codex = input.driverKind === "codex";
  const id = codex ? "reasoningEffort" : "effort";
  const families = codex
    ? [
        { slug: "gpt-6.1-sol", label: "Sol", name: "GPT-6.1 Sol" },
        { slug: "gpt-6-astra", label: "Astra", name: "GPT-6 Astra" },
      ]
    : [
        { slug: "opus", label: "Opus", name: "Claude Opus" },
        { slug: "sonnet", label: "Sonnet", name: "Claude Sonnet" },
      ];
  return families.map((family) => {
    const candidates = input.models.filter((model) => {
      if (model.isLegacy) return false;
      return codex
        ? codexModelFamily(model.slug) === family.slug
        : new RegExp(`^claude-${family.slug}-\\d+(?:-\\d{1,2})?$`).test(model.slug);
    });
    const supportsAll = (model: ServerProviderModel) =>
      EFFORTS.every(({ value }) =>
        effortDescriptor(model, id)?.options.some((option) => option.id === value),
      );
    const version = (model: ServerProviderModel) => {
      const match = model.slug.match(/-(\d+)(?:-(\d{1,2}))?$/);
      return Number(match?.[1] ?? 0) * 100 + Number(match?.[2] ?? 0);
    };
    candidates.sort(
      (a, b) =>
        Number(supportsAll(b)) - Number(supportsAll(a)) || (codex ? 0 : version(b) - version(a)),
    );
    const model = candidates[0];
    const descriptor = effortDescriptor(model, id);
    const disabledReason = !model
      ? `${family.name} isn't available on this account.`
      : !input.instanceId
        ? "Select a provider account first."
        : input.getModelDisabledReason?.(input.instanceId, model.slug);
    return {
      label: family.label,
      name: model?.name ?? family.name,
      model: model?.slug,
      descriptor,
      cells: EFFORTS.map(({ value, label }) => ({
        effort: { id, value },
        label,
        disabledReason:
          disabledReason ??
          (descriptor?.options.some((option) => option.id === value)
            ? null
            : `${family.name} doesn't support ${label} reasoning.`),
      })),
    };
  });
}

export function withShortcutEffort(
  selection: ModelSelection,
  requestedModel: string,
  models: ReadonlyArray<ServerProviderModel>,
  effort: ShortcutEffort,
): ModelSelection | null {
  if (selection.model !== requestedModel) return null;
  const model = models.find(
    (candidate) => candidate.slug === requestedModel && !candidate.isLegacy,
  );
  if (!effortDescriptor(model, effort.id)?.options.some((option) => option.id === effort.value)) {
    return null;
  }
  return {
    ...selection,
    options: [...(selection.options ?? []).filter((option) => option.id !== effort.id), effort],
  };
}

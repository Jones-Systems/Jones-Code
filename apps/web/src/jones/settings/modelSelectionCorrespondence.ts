import {
  PROVIDER_DISPLAY_NAMES,
  type EnvironmentId,
  type ModelSelection,
  type ProjectId,
  type ProviderInstanceId,
  type ServerProvider,
  type ServerSettings,
  type ServerSettingsPatch,
  type UnifiedSettings,
} from "@t3tools/contracts";
import * as Equal from "effect/Equal";

import { getCustomModelOptionsByInstance } from "../../modelSelection";
import {
  applyProviderInstanceSettings,
  deriveProviderInstanceEntries,
  type ProviderInstanceEntry,
} from "../../providerInstances";

export const MODEL_SELECTION_KEYS = [
  "defaultModelSelection",
  "sourceControlWriterModelSelection",
  "reviewGuideModelSelection",
  "textGenerationModelSelection",
] as const;

type ModelSelectionKey = (typeof MODEL_SELECTION_KEYS)[number];
type CorrespondenceFailure =
  | "not-loaded"
  | "unknown-source"
  | "id-conflict"
  | "no-match"
  | "ambiguous"
  | "account-mismatch"
  | "unavailable";
type AccountComparison = "matched" | "not-comparable" | "account-mismatch";

export type ProviderCorrespondence =
  | {
      readonly entry: ProviderInstanceEntry;
      readonly basis: "exact-id" | "explicit-name";
      readonly account: Exclude<AccountComparison, "account-mismatch">;
    }
  | { readonly failure: CorrespondenceFailure };

function explicitName(entry: ProviderInstanceEntry): string | undefined {
  const name = entry.snapshot.displayName?.trim();
  return name && name !== PROVIDER_DISPLAY_NAMES[entry.driverKind] ? name : undefined;
}

function compareAccounts(
  source: ProviderInstanceEntry,
  destination: ProviderInstanceEntry,
): AccountComparison {
  const a = source.snapshot.auth;
  const b = destination.snapshot.auth;
  if (
    a.status !== "authenticated" ||
    b.status !== "authenticated" ||
    (a.type && b.type && a.type !== b.type)
  ) {
    return "not-comparable";
  }
  const emails = [a.email?.trim().toLowerCase(), b.email?.trim().toLowerCase()];
  const hasEmails = Boolean(emails[0] && emails[1]);
  const hasWorkspaces = Boolean(a.workspaceId && b.workspaceId);
  if (
    (hasEmails && emails[0] !== emails[1]) ||
    (hasWorkspaces && a.workspaceId !== b.workspaceId)
  ) {
    return "account-mismatch";
  }
  return hasEmails || hasWorkspaces ? "matched" : "not-comparable";
}

function resolveOneWay(
  source: readonly ProviderInstanceEntry[],
  destination: readonly ProviderInstanceEntry[],
  instanceId: ProviderInstanceId,
): ProviderCorrespondence {
  const selected = source.find((entry) => entry.instanceId === instanceId);
  if (!selected) return { failure: "unknown-source" };
  const name = explicitName(selected);
  const exact = destination.find((entry) => entry.instanceId === instanceId);
  if (exact) {
    const account = compareAccounts(selected, exact);
    if (
      exact.driverKind !== selected.driverKind ||
      (name && explicitName(exact) && name !== explicitName(exact)) ||
      account === "account-mismatch" ||
      (name &&
        destination.some(
          (entry) =>
            entry.instanceId !== exact.instanceId &&
            entry.driverKind === selected.driverKind &&
            explicitName(entry) === name,
        )) ||
      (selected.driverKind === "acpRegistry" &&
        selected.acpRegistryAgentId &&
        exact.acpRegistryAgentId &&
        selected.acpRegistryAgentId !== exact.acpRegistryAgentId)
    ) {
      // A colliding routing key must never fall through to a label match.
      return { failure: "id-conflict" };
    }
    return { entry: exact, basis: "exact-id", account };
  }
  if (!name || selected.driverKind === "acpRegistry") return { failure: "no-match" };
  const matches = (entries: readonly ProviderInstanceEntry[]) =>
    entries.filter(
      (entry) => entry.driverKind === selected.driverKind && explicitName(entry) === name,
    );
  const candidates = matches(destination);
  if (matches(source).length > 1 || candidates.length > 1) return { failure: "ambiguous" };
  const match = candidates[0];
  if (!match) return { failure: "no-match" };
  const account = compareAccounts(selected, match);
  return account === "account-mismatch"
    ? { failure: account }
    : { entry: match, basis: "explicit-name", account };
}

export function resolveProviderCorrespondence(
  source: readonly ProviderInstanceEntry[] | undefined,
  destination: readonly ProviderInstanceEntry[] | undefined,
  instanceId: ProviderInstanceId,
): ProviderCorrespondence {
  if (!source || !destination) return { failure: "not-loaded" };
  const result = resolveOneWay(source, destination, instanceId);
  if ("failure" in result || result.basis === "exact-id") return result;
  const reverse = resolveOneWay(destination, source, result.entry.instanceId);
  return "failure" in reverse || reverse.entry.instanceId !== instanceId
    ? { failure: "id-conflict" }
    : result;
}

export interface ModelSelectionDestination {
  readonly environmentId: EnvironmentId;
  readonly label: string;
  readonly settings: ServerSettings;
  readonly projectId: ProjectId | null;
}

interface ModelSelectionContext {
  readonly representative: ModelSelectionDestination | null;
  readonly providersByEnvironment: ReadonlyMap<EnvironmentId, readonly ServerProvider[]>;
}

function catalog(context: ModelSelectionContext, destination: ModelSelectionDestination) {
  const providers = context.providersByEnvironment.get(destination.environmentId);
  return providers
    ? applyProviderInstanceSettings(
        deriveProviderInstanceEntries(providers),
        destination.settings,
        destination.projectId,
      )
    : undefined;
}

export function modelSelectionFailureMessage(
  failure: CorrespondenceFailure,
  destination: Pick<ModelSelectionDestination, "label">,
): string {
  return failure === "not-loaded"
    ? `Provider models are still loading on ${destination.label}.`
    : `This model cannot be selected on ${destination.label} (${failure}). Select that environment to choose its model separately.`;
}

export function modelSelectionReadiness(
  selection: ModelSelection,
  destination: ModelSelectionDestination,
  providers: readonly ServerProvider[] | undefined,
  settings: UnifiedSettings,
  textGeneration = false,
): CorrespondenceFailure | null {
  if (!providers) return "not-loaded";
  const entry = applyProviderInstanceSettings(
    deriveProviderInstanceEntries(providers),
    destination.settings,
    destination.projectId,
  ).find((candidate) => candidate.instanceId === selection.instanceId);
  const models = getCustomModelOptionsByInstance(settings, providers).get(selection.instanceId);
  return entry?.enabled &&
    entry.isAvailable &&
    (!textGeneration || entry.snapshot.supportsTextGeneration !== false) &&
    models?.some((model) => model.slug === selection.model && !model.isUnavailable)
    ? null
    : "unavailable";
}

export function scopedModelDisabledReason(
  context: ModelSelectionContext,
  destination: ModelSelectionDestination,
  selection: ModelSelection,
  settings: UnifiedSettings,
  textGeneration = false,
): string | null {
  const source = context.representative;
  if (!source) return modelSelectionFailureMessage("unknown-source", destination);
  const match = resolveProviderCorrespondence(
    catalog(context, source),
    catalog(context, destination),
    selection.instanceId,
  );
  if ("failure" in match) return modelSelectionFailureMessage(match.failure, destination);
  const failure = modelSelectionReadiness(
    { ...selection, instanceId: match.entry.instanceId },
    destination,
    context.providersByEnvironment.get(destination.environmentId),
    settings,
    textGeneration,
  );
  return failure ? modelSelectionFailureMessage(failure, destination) : null;
}

export function createModelSelectionResolver(
  context: ModelSelectionContext & {
    readonly settingsForDestination: (destination: ModelSelectionDestination) => UnifiedSettings;
    readonly targets?: readonly ModelSelectionDestination[];
  },
  options: { readonly literalModelSelections?: boolean } = {},
) {
  return (patch: ServerSettingsPatch, planned: ModelSelectionDestination) => {
    const destination =
      context.targets?.find(
        (target) =>
          target.environmentId === planned.environmentId && target.projectId === planned.projectId,
      ) ?? planned;
    if (context.representative?.environmentId === destination.environmentId) return { patch };
    const next = { ...patch };
    for (const key of MODEL_SELECTION_KEYS) {
      const value = patch[key];
      if (value === undefined || value === null) continue;
      if (!context.representative && !options.literalModelSelections) {
        return { refused: modelSelectionFailureMessage("unknown-source", destination) };
      }
      const base = destination.settings[key];
      let selection: ModelSelection;
      if (value.instanceId !== undefined) {
        let instanceId = value.instanceId;
        if (!options.literalModelSelections) {
          const match = resolveProviderCorrespondence(
            catalog(context, context.representative!),
            catalog(context, destination),
            instanceId,
          );
          if ("failure" in match) {
            return { refused: modelSelectionFailureMessage(match.failure, destination) };
          }
          instanceId = match.entry.instanceId;
        }
        const model = value.model ?? base?.model;
        if (model === undefined) {
          return { refused: modelSelectionFailureMessage("unavailable", destination) };
        }
        selection = { instanceId, model, ...(value.options ? { options: value.options } : {}) };
      } else {
        if (!base) return { refused: modelSelectionFailureMessage("unavailable", destination) };
        selection =
          value.model !== undefined
            ? {
                instanceId: base.instanceId,
                model: value.model,
                ...("options" in value ? { options: value.options } : {}),
              }
            : { ...base, ...value };
      }
      const failure = modelSelectionReadiness(
        selection,
        destination,
        context.providersByEnvironment.get(destination.environmentId),
        context.settingsForDestination(destination),
        key !== "defaultModelSelection",
      );
      if (failure) return { refused: modelSelectionFailureMessage(failure, destination) };
      next[key] = selection;
    }
    return { patch: next };
  };
}

export function modelSelectionsAreMixed(
  context: ModelSelectionContext,
  targets: readonly ModelSelectionDestination[],
  keys: readonly (keyof ServerSettings)[],
): boolean {
  const source = context.representative ?? targets[0];
  if (!source) return false;
  return targets.some((destination) =>
    keys.some((key) => {
      const value = source.settings[key];
      if (
        !MODEL_SELECTION_KEYS.includes(key as ModelSelectionKey) ||
        source.environmentId === destination.environmentId ||
        value === null
      ) {
        return !Equal.equals(value, destination.settings[key]);
      }
      if (!context.representative) return true;
      const selection = value as ModelSelection;
      const match = resolveProviderCorrespondence(
        catalog(context, source),
        catalog(context, destination),
        selection.instanceId,
      );
      return (
        "failure" in match ||
        !Equal.equals(
          { ...selection, instanceId: match.entry.instanceId },
          destination.settings[key],
        )
      );
    }),
  );
}

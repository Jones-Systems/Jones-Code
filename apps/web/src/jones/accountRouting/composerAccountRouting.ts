import {
  type AccountRouting,
  type ModelSelection,
  type ProviderDriverKind,
  type ProviderInstanceId,
} from "@t3tools/contracts";
import type { AppModelOption } from "../../modelSelection";
import {
  type ComposerThreadDraftState,
  type ComposerThreadTarget,
  useComposerDraftStore,
} from "../../composerDraftStore";
import { isProviderInstancePickerReady, type ProviderInstanceEntry } from "../../providerInstances";

export const AUTO_ACCOUNT_ROUTING = { mode: "auto" } as const satisfies AccountRouting;

const families = ["codex", "claudeAgent"] as const;
export type AccountRoutingFamily = (typeof families)[number];

export function accountRoutingFamily(
  driver: ProviderDriverKind | undefined,
): AccountRoutingFamily | null {
  if (driver === "codex") return "codex";
  if (driver === "claudeAgent") return "claudeAgent";
  return null;
}

function availableAccounts(
  entries: ReadonlyArray<ProviderInstanceEntry>,
  family: AccountRoutingFamily,
) {
  return entries.filter(
    (entry) =>
      entry.driverKind === family &&
      entry.installed &&
      entry.status !== "error" &&
      entry.status !== "disabled" &&
      entry.snapshot.auth.status !== "unauthenticated" &&
      entry.snapshot.availability !== "unavailable" &&
      isProviderInstancePickerReady(entry),
  );
}

export function canRouteAccountForSelection(
  entries: ReadonlyArray<ProviderInstanceEntry>,
  instanceId: ProviderInstanceId,
): boolean {
  const family = accountRoutingFamily(
    entries.find((entry) => entry.instanceId === instanceId)?.driverKind,
  );
  if (family === null) return false;
  const accounts = availableAccounts(entries, family);
  return accounts.length > 1 && accounts.some((entry) => entry.instanceId === instanceId);
}

export interface AccountRoutingChoice {
  readonly family: AccountRoutingFamily;
  readonly representative: ModelSelection;
}

export interface ComposerAccountRoutingPicker {
  readonly active: AccountRoutingFamily | "unavailable" | null;
  readonly choices: ReadonlyArray<AccountRoutingChoice>;
  readonly select: (instanceId: ProviderInstanceId, model: string) => void;
}

export interface AccountRoutingModelSelectOptions {
  readonly accountRouting?: AccountRouting;
}

export function acknowledgeAccountRouting(
  target: ComposerThreadTarget,
  submitted: ComposerThreadDraftState | null,
): void {
  const selection = submitted?.activeProvider
    ? submitted.modelSelectionByProvider[submitted.activeProvider]
    : undefined;
  if (selection && submitted?.accountRouting)
    useComposerDraftStore.getState().releaseModelSelection(target, selection);
}

export function accountRoutingChoices(
  entries: ReadonlyArray<ProviderInstanceEntry>,
  modelOptions: ReadonlyMap<ProviderInstanceId, ReadonlyArray<AppModelOption>>,
  current: ModelSelection,
): ReadonlyArray<AccountRoutingChoice> {
  return families.flatMap((family) => {
    const accounts = availableAccounts(entries, family);
    if (accounts.length < 2) return [];
    const entry =
      accounts.find((account) => account.instanceId === current.instanceId) ?? accounts[0]!;
    const models = modelOptions.get(entry.instanceId)?.filter((model) => !model.isUnavailable);
    const model =
      (entry.instanceId === current.instanceId
        ? models?.find(
            (option) => option.slug === current.model || option.aliases?.includes(current.model),
          )
        : undefined) ??
      models?.find((option) => option.isDefault) ??
      models?.[0];
    return model
      ? [{ family, representative: { instanceId: entry.instanceId, model: model.slug } }]
      : [];
  });
}

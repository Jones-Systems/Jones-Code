import {
  ANTIGRAVITY_DEFAULT_MODEL,
  type ModelSelection,
  type ProviderDriverKind,
  type ProviderInstanceId,
} from "@t3tools/contracts";
import type { UnifiedSettings } from "@t3tools/contracts/settings";
import { resolveSelectableModel } from "@t3tools/shared/model";
import type { Ref } from "react";
import { getAppModelOptionsForInstance, type AppModelOption } from "../../modelSelection";
import { isProviderInstancePickerReady, type ProviderInstanceEntry } from "../../providerInstances";
import { ComposerControl } from "../../components/chat/ComposerControl";
import { ProviderInstanceIcon } from "../../components/chat/ProviderInstanceIcon";
import { getProviderStatusMessage } from "../../components/chat/ProviderStatusBanner";

const PROVIDER_INSTANCE_SHORTCUT_LIMIT = 9;

export function matchesProviderModelLock(
  entry: Pick<ProviderInstanceEntry, "instanceId" | "driverKind" | "continuationGroupKey">,
  lockedProvider: ProviderDriverKind | null,
  lockedContinuationGroupKey: string | null | undefined,
  lockedInstanceId?: ProviderInstanceId | null,
): boolean {
  // Missing continuation metadata must keep Antigravity history on its session account.
  const requiresExactInstance =
    lockedProvider === "antigravity" &&
    lockedInstanceId != null &&
    lockedContinuationGroupKey == null;
  return (
    lockedProvider === null ||
    (entry.driverKind === lockedProvider &&
      (!lockedContinuationGroupKey || entry.continuationGroupKey === lockedContinuationGroupKey) &&
      (!requiresExactInstance || entry.instanceId === lockedInstanceId))
  );
}

function resolveSupportedShortcutModel(
  entry: ProviderInstanceEntry,
  options: ReadonlyArray<AppModelOption>,
  model: string | null | undefined,
): AppModelOption | undefined {
  const resolved = resolveSelectableModel(entry.driverKind, model, options);
  return options.find(
    (option) =>
      option.slug === resolved &&
      !option.isUnavailable &&
      !(entry.driverKind === "antigravity" && option.slug === ANTIGRAVITY_DEFAULT_MODEL),
  );
}

export function resolveProviderInstanceShortcut(input: {
  entry: ProviderInstanceEntry;
  options: ReadonlyArray<AppModelOption>;
  rememberedSelection: ModelSelection | undefined;
  currentDriver: ProviderDriverKind | undefined;
  currentModel: string;
  lockedProvider: ProviderDriverKind | null;
  lockedContinuationGroupKey: string | null | undefined;
  lockedInstanceId?: ProviderInstanceId | null;
  getModelDisabledReason: (instanceId: ProviderInstanceId, model: string) => string | null;
}): { model: AppModelOption | undefined; disabledReason: string | null } {
  const { entry, options } = input;
  const defaultModel =
    options.find(
      (option) =>
        option.isDefault &&
        !(entry.driverKind === "antigravity" && option.slug === ANTIGRAVITY_DEFAULT_MODEL),
    ) ??
    options.find(
      (option) =>
        !(entry.driverKind === "antigravity" && option.slug === ANTIGRAVITY_DEFAULT_MODEL),
    );
  if (!isProviderInstancePickerReady(entry)) {
    return { model: defaultModel, disabledReason: getProviderStatusMessage(entry.snapshot) };
  }
  if (
    !matchesProviderModelLock(
      entry,
      input.lockedProvider,
      input.lockedContinuationGroupKey,
      input.lockedInstanceId,
    )
  ) {
    return { model: defaultModel, disabledReason: "Start a new thread to use this provider." };
  }
  const candidates = [
    input.rememberedSelection?.instanceId === entry.instanceId
      ? input.rememberedSelection.model
      : null,
    input.currentDriver === entry.driverKind ? input.currentModel : null,
    defaultModel?.slug,
  ];
  for (const candidate of candidates) {
    const model = resolveSupportedShortcutModel(entry, options, candidate);
    if (model && input.getModelDisabledReason(entry.instanceId, model.slug) === null) {
      return { model, disabledReason: null };
    }
  }
  return {
    model: defaultModel,
    disabledReason: defaultModel
      ? (input.getModelDisabledReason(entry.instanceId, defaultModel.slug) ??
        (defaultModel.isUnavailable
          ? "This model is unavailable."
          : "No selectable models are available."))
      : "No selectable models are available.",
  };
}

export function ProviderInstanceShortcuts(props: {
  instanceEntries: ReadonlyArray<ProviderInstanceEntry>;
  settings: UnifiedSettings;
  modelOptionsByInstance?: ReadonlyMap<ProviderInstanceId, ReadonlyArray<AppModelOption>>;
  rememberedSelections?: Partial<Record<ProviderInstanceId, ModelSelection>>;
  activeInstanceId: ProviderInstanceId | null;
  model: string;
  selectedModels?: ReadonlyArray<ModelSelection> | null;
  lockedProvider: ProviderDriverKind | null;
  lockedContinuationGroupKey: string | null;
  lockedInstanceId?: ProviderInstanceId | null;
  disabled: boolean;
  visible?: boolean;
  groupRef?: Ref<HTMLDivElement>;
  getModelDisabledReason: (instanceId: ProviderInstanceId, model: string) => string | null;
  onSelect: (instanceId: ProviderInstanceId, model: string) => void;
}) {
  const entries = props.instanceEntries.filter((entry) => entry.enabled);
  if (entries.length === 0 || entries.length > PROVIDER_INSTANCE_SHORTCUT_LIMIT) return null;
  const currentDriver = props.instanceEntries.find(
    (entry) => entry.instanceId === props.activeInstanceId,
  )?.driverKind;
  const visible = props.visible !== false && props.selectedModels == null;
  const resolve = (entry: ProviderInstanceEntry) =>
    resolveProviderInstanceShortcut({
      entry,
      options:
        props.modelOptionsByInstance?.get(entry.instanceId) ??
        getAppModelOptionsForInstance(props.settings, entry),
      rememberedSelection: props.rememberedSelections?.[entry.instanceId],
      currentDriver,
      currentModel: props.model,
      lockedProvider: props.lockedProvider,
      lockedContinuationGroupKey: props.lockedContinuationGroupKey,
      lockedInstanceId: props.lockedInstanceId ?? props.activeInstanceId,
      getModelDisabledReason: props.getModelDisabledReason,
    });

  // Keep the same width and layout while hidden so measuring cannot toggle visibility.
  return (
    <div
      ref={props.groupRef}
      role="group"
      aria-label="Provider accounts"
      aria-hidden={!visible || undefined}
      inert={!visible || undefined}
      data-composer-shortcut-group="accounts"
      className="grid w-max grid-cols-[repeat(3,36px)] gap-1 rounded-(--control-radius) bg-background/50"
      style={{ visibility: visible ? "visible" : "hidden" }}
    >
      {entries.map((entry) => {
        const { model, disabledReason } = resolve(entry);
        const label = `${entry.displayName}: ${model?.name ?? "No selectable model"}`;
        return (
          <ComposerControl
            key={entry.instanceId}
            size="xs"
            className="w-9"
            aria-label={label}
            aria-description={disabledReason ?? undefined}
            aria-pressed={props.activeInstanceId === entry.instanceId}
            disabled={!visible || props.disabled || disabledReason !== null}
            tabIndex={visible ? undefined : -1}
            title={disabledReason ? `${label}. ${disabledReason}` : label}
            onClick={() => {
              const current = resolve(entry);
              if (!visible || props.disabled || current.disabledReason !== null || !current.model)
                return;
              props.onSelect(entry.instanceId, current.model.slug);
            }}
          >
            <ProviderInstanceIcon
              driverKind={entry.driverKind}
              displayName={entry.displayName}
              accentColor={entry.accentColor}
              showBadge
              className="size-4"
              iconClassName="size-4"
              indicatorBackground="var(--background)"
            />
          </ComposerControl>
        );
      })}
    </div>
  );
}

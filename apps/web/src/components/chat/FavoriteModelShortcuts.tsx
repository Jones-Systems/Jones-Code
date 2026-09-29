import type { ProviderDriverKind, ProviderInstanceId } from "@t3tools/contracts";
import { useLayoutEffect, useRef, useState } from "react";
import { useClientSettings } from "~/hooks/useSettings";
import { type UnifiedSettings } from "@t3tools/contracts/settings";
import { getAppModelOptionsForInstance } from "../../modelSelection";
import { isProviderInstancePickerReady, type ProviderInstanceEntry } from "../../providerInstances";
import { ComposerControl } from "./ComposerControl";
import { ProviderInstanceIcon } from "./ProviderInstanceIcon";

export function FavoriteModelShortcuts(props: {
  instanceEntries: ReadonlyArray<ProviderInstanceEntry>;
  settings: UnifiedSettings;
  activeInstanceId: ProviderInstanceId | null;
  model: string;
  selectedModels: ReadonlyArray<{ instanceId: ProviderInstanceId; model: string }> | null;
  lockedProvider: ProviderDriverKind | null;
  lockedContinuationGroupKey: string | null;
  disabled: boolean;
  getModelDisabledReason: (instanceId: ProviderInstanceId, model: string) => string | null;
  onSelect: (instanceId: ProviderInstanceId, model: string) => void;
}) {
  const favorites = useClientSettings((settings) => settings.favorites);
  const groupRef = useRef<HTMLDivElement>(null);
  const [height, setHeight] = useState(0);
  const shortcuts = (favorites ?? []).flatMap((favorite) => {
    const entry = props.instanceEntries.find((entry) => entry.instanceId === favorite.provider);
    if (!entry) return [];
    const model = getAppModelOptionsForInstance(props.settings, entry).find(
      (model) => model.slug === favorite.model,
    );
    if (!model) return [];
    const disabledReason =
      (!isProviderInstancePickerReady(entry) || model.isUnavailable
        ? "Provider or model is unavailable."
        : null) ??
      (props.lockedProvider !== null &&
      (entry.driverKind !== props.lockedProvider ||
        (props.lockedContinuationGroupKey !== null &&
          entry.continuationGroupKey !== props.lockedContinuationGroupKey))
        ? "Start a new thread to use this provider."
        : null) ??
      props.getModelDisabledReason(entry.instanceId, model.slug);
    return [{ entry, model, disabledReason }];
  });

  useLayoutEffect(() => {
    const group = groupRef.current;
    if (!group || shortcuts.length === 0) return;
    const measure = () => {
      const rows = new Set(Array.from(group.children, (child) => (child as HTMLElement).offsetTop));
      setHeight(rows.size > 0 && rows.size <= 2 ? group.offsetHeight : 0);
    };
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(group);
    return () => observer.disconnect();
  }, [shortcuts.length]);

  if (shortcuts.length === 0) return null;

  return (
    <div className="relative mx-3 shrink-0" style={{ height }}>
      {/* Keep the same width and layout while hidden so measuring cannot toggle visibility. */}
      <div
        ref={groupRef}
        role="group"
        aria-label="Favorite models"
        aria-hidden={height === 0}
        inert={height === 0}
        className="absolute inset-x-0 top-0 flex flex-wrap gap-1 py-1"
        style={{ visibility: height === 0 ? "hidden" : "visible" }}
      >
        {shortcuts.map(({ entry, model, disabledReason }) => (
          <ComposerControl
            key={`${entry.instanceId}:${model.slug}`}
            className="max-w-full"
            aria-label={`${entry.displayName}: ${model.name}`}
            aria-pressed={
              props.selectedModels !== null
                ? props.selectedModels.some(
                    (selection) =>
                      selection.instanceId === entry.instanceId && selection.model === model.slug,
                  )
                : props.activeInstanceId === entry.instanceId && props.model === model.slug
            }
            disabled={props.disabled || disabledReason !== null}
            title={disabledReason ?? `${entry.displayName}: ${model.name}`}
            onClick={() => props.onSelect(entry.instanceId, model.slug)}
          >
            <ProviderInstanceIcon
              driverKind={entry.driverKind}
              displayName={entry.displayName}
              accentColor={entry.accentColor}
              className="size-4"
              iconClassName="size-4"
            />
            <span className="truncate">{model.shortName ?? model.name}</span>
          </ComposerControl>
        ))}
      </div>
    </div>
  );
}

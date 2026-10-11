import type { Ref } from "react";
import type { ProviderInstanceId } from "@t3tools/contracts";
import {
  resolveModelEffortShortcutRows,
  type ComposerModelSelectOptions,
} from "./composerModelEffortShortcuts";
import { getProviderOptionCurrentValue } from "@t3tools/shared/model";
import { ComposerControl } from "../../components/chat/ComposerControl";
import {
  type TraitsMenuContentProps,
  type TraitsPersistence,
  useTraitsSelection,
} from "../../components/chat/TraitsPicker";

const EFFORT_SHORT_LABELS = new Map([
  ["none", "Off"],
  ["minimal", "Min"],
  ["low", "Low"],
  ["medium", "Med"],
  ["high", "High"],
  ["xhigh", "XH"],
  ["max", "Max"],
  ["ultrathink", "Ultra"],
]);

export function ReasoningEffortShortcuts({
  groupRef,
  ...props
}: TraitsMenuContentProps &
  TraitsPersistence & {
    groupRef?: Ref<HTMLDivElement>;
    visible: boolean;
    getModelDisabledReason?: (instanceId: ProviderInstanceId, model: string) => string | null;
    onProviderModelSelect?: (
      instanceId: ProviderInstanceId,
      model: string,
      options?: ComposerModelSelectOptions,
    ) => void;
  }) {
  const traits = useTraitsSelection(props);
  const descriptor = traits.selectDescriptors.find(
    ({ id }) =>
      id === "reasoningEffort" || id === "effort" || id === "variant" || id === "reasoning",
  );
  const rows = resolveModelEffortShortcutRows({
    driverKind: props.provider,
    models: props.models,
    ...(props.instanceId ? { instanceId: props.instanceId } : {}),
    ...(props.getModelDisabledReason
      ? { getModelDisabledReason: props.getModelDisabledReason }
      : {}),
  });
  if (rows) {
    return (
      <div
        ref={groupRef}
        data-composer-shortcut-group="effort"
        role="group"
        aria-label="Model and reasoning effort"
        aria-hidden={!props.visible || undefined}
        inert={!props.visible || undefined}
        className="grid w-max gap-1 rounded-(--control-radius) bg-background"
        style={{ visibility: props.visible ? "visible" : "hidden" }}
      >
        {rows.map((row) => (
          <div
            key={row.label}
            role="group"
            aria-label={row.name}
            className="grid grid-cols-[3rem_repeat(3,minmax(max-content,1fr))] items-center gap-1"
          >
            <span className="px-1 text-xs text-muted-foreground">{row.label}</span>
            {row.cells.map((cell) => {
              const sameModel = props.model === row.model;
              const disabledReason = traits.ultrathinkInBodyText
                ? 'Your prompt contains "ultrathink" in the text. Remove it to change this option.'
                : cell.disabledReason;
              const disabled =
                !props.visible || !!disabledReason || (!sameModel && !props.onProviderModelSelect);
              return (
                <ComposerControl
                  key={cell.effort.value}
                  size="xs"
                  aria-label={`${row.name}, ${cell.label} reasoning`}
                  aria-pressed={
                    sameModel &&
                    !traits.ultrathinkPromptControlled &&
                    !!descriptor &&
                    getProviderOptionCurrentValue(descriptor) === cell.effort.value
                  }
                  disabled={disabled}
                  tabIndex={props.visible ? undefined : -1}
                  title={disabledReason ?? `${row.name}, ${cell.label} reasoning`}
                  onClick={() => {
                    if (disabled || !row.model || !props.instanceId) return;
                    if (sameModel && row.descriptor) {
                      traits.handleSelectChange(row.descriptor, cell.effort.value);
                    } else {
                      props.onProviderModelSelect?.(props.instanceId, row.model, {
                        effort: cell.effort,
                      });
                      if (traits.ultrathinkPromptControlled)
                        props.onPromptChange(props.prompt.replace(/^Ultrathink:\s*/i, ""));
                    }
                  }}
                >
                  {cell.label}
                </ComposerControl>
              );
            })}
          </div>
        ))}
      </div>
    );
  }
  if (traits.modelIsUnavailable || !descriptor?.options.length) return null;
  const promptControlled =
    descriptor.id === traits.primarySelectDescriptor?.id && traits.ultrathinkPromptControlled;
  const bodyTextLocked =
    descriptor.id === traits.primarySelectDescriptor?.id && traits.ultrathinkInBodyText;
  const selectedValue = promptControlled ? "ultrathink" : getProviderOptionCurrentValue(descriptor);

  return (
    <div
      ref={groupRef}
      data-composer-shortcut-group="effort"
      role="group"
      aria-label={descriptor.label}
      aria-hidden={!props.visible || undefined}
      inert={!props.visible || undefined}
      className="grid w-max grid-cols-[repeat(3,minmax(max-content,1fr))] gap-1 rounded-(--control-radius) bg-background"
      style={{ visibility: props.visible ? "visible" : "hidden" }}
    >
      {descriptor.options.map((option) => (
        <ComposerControl
          key={option.id}
          size="xs"
          aria-label={`${descriptor.label}: ${option.label}`}
          aria-pressed={selectedValue === option.id}
          disabled={!props.visible || traits.isSelectChangeDisabled(descriptor, option.id)}
          tabIndex={props.visible ? undefined : -1}
          title={
            bodyTextLocked
              ? 'Your prompt contains "ultrathink" in the text. Remove it to change this option.'
              : option.description
                ? `${option.label}. ${option.description}`
                : option.label
          }
          onClick={() => {
            if (!props.visible) return;
            traits.handleSelectChange(descriptor, option.id);
          }}
        >
          {EFFORT_SHORT_LABELS.get(option.id) ?? option.label}
        </ComposerControl>
      ))}
    </div>
  );
}

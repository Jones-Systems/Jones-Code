import type { ModelSelection } from "@t3tools/contracts";
import { createModelSelection } from "@t3tools/shared/model";
import { type ComponentProps, useState } from "react";

import {
  ComposerEffortColumn,
  ComposerPickerFooter,
} from "../../components/chat/ComposerModelEffort";
import { ProviderModelPicker } from "../../components/chat/ProviderModelPicker";
import type { TraitsSelectionInput } from "../../components/chat/TraitsPicker";
import { Button } from "../../components/ui/button";

type DefaultModelPickerProps = Pick<
  ComponentProps<typeof ProviderModelPicker>,
  | "environmentId"
  | "getModelDisabledReason"
  | "instanceEntries"
  | "modelOptionsByInstance"
  | "onOpenProviderSetup"
  | "triggerClassName"
  | "triggerLabel"
> & {
  selection: ModelSelection;
  onSave: (selection: ModelSelection) => void;
};

export function DefaultModelPicker({ selection, onSave, ...pickerProps }: DefaultModelPickerProps) {
  const [open, setOpen] = useState(false);
  const [draft, setDraft] = useState<ModelSelection | null>(null);
  const current = draft ?? selection;
  const entry = pickerProps.instanceEntries.find(
    (entry) => entry.instanceId === current.instanceId,
  );
  const disabledReason = pickerProps.getModelDisabledReason?.(current.instanceId, current.model);
  const traits: TraitsSelectionInput | null =
    entry && !disabledReason
      ? {
          provider: entry.driverKind,
          instanceId: current.instanceId,
          models: entry.models,
          model: current.model,
          modelOptions: current.options,
          allowPromptInjectedEffort: false,
          onModelOptionsChange: (options) =>
            setDraft(createModelSelection(current.instanceId, current.model, options)),
        }
      : null;

  function changeOpen(next: boolean) {
    setOpen(next);
    if (!next) setDraft(null);
  }

  return (
    <ProviderModelPicker
      {...pickerProps}
      activeInstanceId={current.instanceId}
      model={current.model}
      lockedProvider={null}
      popupAlign="end"
      open={open}
      onOpenChange={changeOpen}
      onInstanceModelChange={(instanceId, model) =>
        setDraft(
          current.instanceId === instanceId && current.model === model
            ? current
            : createModelSelection(instanceId, model),
        )
      }
      effortColumn={<ComposerEffortColumn traits={traits} />}
      footer={
        <ComposerPickerFooter
          traits={traits}
          action={
            <Button
              size="sm"
              disabled={!draft || !!disabledReason || !entry}
              onClick={() => {
                if (!draft || disabledReason || !entry) return;
                onSave(draft);
                changeOpen(false);
              }}
            >
              Done
            </Button>
          }
        />
      }
    />
  );
}

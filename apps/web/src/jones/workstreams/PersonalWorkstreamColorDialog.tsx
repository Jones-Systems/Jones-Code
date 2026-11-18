import { CheckIcon } from "lucide-react";
import { useId, useState } from "react";
import { Button } from "../../components/ui/button";
import {
  Dialog,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogPanel,
  DialogPopup,
  DialogTitle,
} from "../../components/ui/dialog";
import { Input } from "../../components/ui/input";
import { PERSONAL_WORKSTREAM_PALETTE } from "./personalRail";

const hexColor = /^#[a-f0-9]{6}$/i;

export function PersonalWorkstreamColorDialog({
  color,
  automaticColor,
  pending,
  onOpenChange,
  onSave,
}: {
  color: string | null;
  automaticColor: string;
  pending: boolean;
  onOpenChange: (open: boolean) => void;
  onSave: (color: string | null) => void | Promise<void>;
}) {
  const [draft, setDraft] = useState(color ?? "");
  const formId = useId();
  const hexInputId = useId();
  const colorInputId = useId();
  const valid = hexColor.test(draft);
  const normalized = valid ? draft.toUpperCase() : null;

  return (
    <Dialog open onOpenChange={onOpenChange}>
      <DialogPopup>
        <DialogHeader>
          <DialogTitle>Workstream color</DialogTitle>
          <DialogDescription>
            This color is used for the Workstream’s tint and accent.
          </DialogDescription>
        </DialogHeader>
        <DialogPanel>
          <form
            id={formId}
            onSubmit={(event) => {
              event.preventDefault();
              if (!pending && (draft === "" || valid)) void onSave(normalized);
            }}
          >
            <fieldset disabled={pending} className="flex min-w-0 flex-col items-start gap-3">
              <legend className="text-sm font-medium">Palette</legend>
              <div className="mt-2 grid grid-cols-6 gap-1.5">
                {PERSONAL_WORKSTREAM_PALETTE.map((swatch) => (
                  <Button
                    key={swatch.color}
                    type="button"
                    size="icon-sm"
                    variant="outline"
                    aria-label={swatch.label}
                    aria-pressed={normalized === swatch.color}
                    title={swatch.label}
                    onClick={() => setDraft(swatch.color)}
                  >
                    <span
                      className="grid size-4 place-items-center rounded-sm"
                      style={{ backgroundColor: swatch.color }}
                    >
                      {normalized === swatch.color ? (
                        <CheckIcon aria-hidden className="size-3 text-black" />
                      ) : null}
                    </span>
                  </Button>
                ))}
              </div>
              <Button
                type="button"
                size="compact"
                variant="outline"
                aria-pressed={draft === ""}
                onClick={() => setDraft("")}
              >
                <span
                  className="size-3 rounded-sm"
                  aria-hidden
                  style={{ backgroundColor: automaticColor }}
                />
                Automatic
                {draft === "" ? <CheckIcon aria-hidden /> : null}
              </Button>
              <div className="flex w-full min-w-0 items-end gap-3">
                <div className="flex shrink-0 flex-col gap-1">
                  <label className="text-sm font-medium" htmlFor={colorInputId}>
                    Custom color
                  </label>
                  <input
                    id={colorInputId}
                    type="color"
                    className="h-8 w-14 cursor-pointer rounded-md border border-input bg-background p-1"
                    value={normalized ?? automaticColor}
                    onChange={(event) => setDraft(event.currentTarget.value.toUpperCase())}
                  />
                </div>
                <div className="flex min-w-0 flex-1 flex-col gap-1">
                  <label className="text-sm font-medium" htmlFor={hexInputId}>
                    Hex color
                  </label>
                  <Input
                    id={hexInputId}
                    size="sm"
                    maxLength={7}
                    placeholder={automaticColor}
                    spellCheck={false}
                    value={draft}
                    aria-invalid={draft !== "" && !valid}
                    onChange={(event) => setDraft(event.target.value)}
                  />
                </div>
              </div>
            </fieldset>
          </form>
        </DialogPanel>
        <DialogFooter>
          <Button
            type="button"
            variant="outline"
            disabled={pending}
            onClick={() => onOpenChange(false)}
          >
            Cancel
          </Button>
          <Button type="submit" form={formId} disabled={pending || (draft !== "" && !valid)}>
            Save
          </Button>
        </DialogFooter>
      </DialogPopup>
    </Dialog>
  );
}

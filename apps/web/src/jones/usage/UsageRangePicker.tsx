import { EllipsisIcon } from "lucide-react";
import { useState } from "react";

import { Button } from "../../components/ui/button";
import { Input } from "../../components/ui/input";
import { Popover, PopoverPopup, PopoverTrigger } from "../../components/ui/popover";
import { Toggle, ToggleGroup } from "../../components/ui/toggle-group";
import type { CustomUsageWindowValidation } from "./usageDateRange";
import { QUICK_USAGE_HOUR_OPTIONS, type UsageWindowSelection } from "./useUsageWindow";

export function UsageRangePicker({
  selection,
  timeZone,
  sinceValue,
  untilValue,
  validation,
  disabled,
  onSinceValueChange,
  onUntilValueChange,
  onSelectHours,
  onApplyCustom,
  onClear,
}: {
  readonly selection: UsageWindowSelection;
  readonly timeZone: string;
  readonly sinceValue: string;
  readonly untilValue: string;
  readonly validation: CustomUsageWindowValidation;
  readonly disabled: boolean;
  readonly onSinceValueChange: (value: string) => void;
  readonly onUntilValueChange: (value: string) => void;
  readonly onSelectHours: (hours: (typeof QUICK_USAGE_HOUR_OPTIONS)[number]) => void;
  readonly onApplyCustom: () => void;
  readonly onClear: () => void;
}) {
  const [open, setOpen] = useState(false);
  const hasAlternateSelection = selection.kind !== "day";
  const hasRangeDraft = sinceValue !== "" || untilValue !== "";
  const validationMessage =
    sinceValue !== "" && untilValue !== "" && !validation.ok ? validation.error : null;

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger
        render={
          <Button
            aria-label="Additional usage ranges"
            title="Additional usage ranges"
            disabled={disabled}
            size="icon-sm"
            variant={hasAlternateSelection ? "secondary" : "ghost"}
          >
            <EllipsisIcon aria-hidden />
          </Button>
        }
      />
      <PopoverPopup align="end" width="lg" aria-label="Additional usage ranges">
        <div className="flex w-full flex-col gap-4 p-4">
          <section className="flex flex-col gap-2">
            <h2 className="text-xs font-medium text-muted-foreground">Short ranges</h2>
            <ToggleGroup
              aria-label="Hourly usage range"
              variant="segmented"
              value={selection.kind === "hours" ? [String(selection.hours)] : []}
              onValueChange={(next) => {
                const selectedHours = Number(next[0]);
                if (
                  QUICK_USAGE_HOUR_OPTIONS.includes(
                    selectedHours as (typeof QUICK_USAGE_HOUR_OPTIONS)[number],
                  )
                ) {
                  onSelectHours(selectedHours as (typeof QUICK_USAGE_HOUR_OPTIONS)[number]);
                  setOpen(false);
                }
              }}
            >
              {QUICK_USAGE_HOUR_OPTIONS.map((hours) => (
                <Toggle key={hours} value={String(hours)}>
                  {hours}h
                </Toggle>
              ))}
            </ToggleGroup>
          </section>

          <div className="border-t border-border/60" />

          <section className="flex flex-col gap-3">
            <div className="flex flex-col gap-1">
              <h2 className="text-xs font-medium text-muted-foreground">Custom range</h2>
              <p className="text-xs text-muted-foreground">
                Times use {timeZone}. The end is exclusive.
              </p>
            </div>
            <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
              <label className="flex min-w-0 flex-col gap-1 text-xs text-muted-foreground">
                Start (inclusive)
                <Input
                  aria-label="Custom range start"
                  nativeInput
                  type="datetime-local"
                  step={60}
                  value={sinceValue}
                  onChange={(event) => onSinceValueChange(event.target.value)}
                />
              </label>
              <label className="flex min-w-0 flex-col gap-1 text-xs text-muted-foreground">
                End (exclusive)
                <Input
                  aria-label="Custom range end"
                  nativeInput
                  type="datetime-local"
                  step={60}
                  value={untilValue}
                  onChange={(event) => onUntilValueChange(event.target.value)}
                />
              </label>
            </div>
            {validationMessage ? (
              <p role="alert" className="text-xs text-destructive">
                {validationMessage}
              </p>
            ) : null}
            <div className="flex items-center justify-between gap-2">
              <Button
                size="sm"
                variant="ghost"
                disabled={!hasAlternateSelection && !hasRangeDraft}
                onClick={() => {
                  onClear();
                  setOpen(false);
                }}
              >
                Clear custom selection
              </Button>
              <Button
                size="sm"
                disabled={!validation.ok}
                onClick={() => {
                  onApplyCustom();
                  setOpen(false);
                }}
              >
                Apply range
              </Button>
            </div>
          </section>
        </div>
      </PopoverPopup>
    </Popover>
  );
}

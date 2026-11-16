import { useState } from "react";
import type {
  Duration,
  Item,
  Mutation,
} from "@t3tools/client-runtime/jones/remoteAdmission/controller";
import { Tooltip, TooltipTrigger, TooltipPopup } from "../../components/ui/tooltip";
import { Button } from "../../components/ui/button";
import {
  Select,
  SelectTrigger,
  SelectValue,
  SelectPopup,
  SelectItem,
} from "../../components/ui/select";

const durations = [
  { value: "3600", label: "1 hour" },
  { value: "86400", label: "1 day" },
  { value: "172800", label: "2 days" },
  { value: "259200", label: "3 days" },
  { value: "604800", label: "1 week" },
];
export function DurationSelect({
  value,
  onChange,
  once = false,
  disabled = false,
}: {
  value: string;
  onChange: (value: string) => void;
  once?: boolean;
  disabled?: boolean;
}) {
  const items = [...durations, ...(once ? [{ value: "once", label: "This request only" }] : [])];
  return (
    <Select
      value={value || null}
      onValueChange={(next) => onChange(next ?? "")}
      items={items}
      disabled={disabled}
    >
      <SelectTrigger size="compact" valueSizing aria-label="Duration">
        <SelectValue placeholder="Choose Duration" />
      </SelectTrigger>
      <SelectPopup matchTriggerWidth={false} alignItemWithTrigger={false}>
        {items.map((item) => (
          <SelectItem key={item.value} value={item.value}>
            {item.label}
          </SelectItem>
        ))}
      </SelectPopup>
    </Select>
  );
}
export function ReviewControls({
  item,
  disabled,
  grantSupported,
  mutate,
}: {
  item: Item;
  disabled: string | null;
  grantSupported: boolean;
  mutate: (input: Mutation) => void;
}) {
  const [duration, setDuration] = useState("86400");
  const eligible = grantSupported && item.grantEligible;
  return (
    <div className="space-y-2">
      <div className="flex flex-wrap items-center gap-2">
        {eligible ? (
          <DurationSelect
            value={duration}
            onChange={setDuration}
            once
            disabled={Boolean(disabled)}
          />
        ) : (
          <span className="text-xs text-muted-foreground">This request only</span>
        )}
        <div className="ml-auto flex gap-2">
          <Button
            size="compact"
            variant="outline"
            aria-disabled={Boolean(disabled)}
            onClick={() => {
              if (!disabled)
                mutate({
                  action: "decide",
                  input: {
                    id: item.record.id,
                    bindingDigest: item.record.bindingDigest,
                    decision: "deny",
                  },
                });
            }}
          >
            Deny
          </Button>
          <Button
            size="compact"
            aria-disabled={Boolean(disabled)}
            onClick={() => {
              if (!disabled)
                mutate({
                  action: "decide",
                  input: {
                    id: item.record.id,
                    bindingDigest: item.record.bindingDigest,
                    decision: "approve",
                    ...(eligible && duration !== "once"
                      ? { grant: { durationSeconds: Number(duration) as Duration } }
                      : {}),
                  },
                });
            }}
          >
            Approve
          </Button>
        </div>
      </div>
      <Tooltip>
        <TooltipTrigger
          render={
            <p
              role="status"
              tabIndex={disabled ? 0 : -1}
              className="h-8 line-clamp-2 text-xs text-muted-foreground"
            />
          }
        >
          {disabled}
        </TooltipTrigger>
        {disabled ? <TooltipPopup>{disabled}</TooltipPopup> : null}
      </Tooltip>
    </div>
  );
}

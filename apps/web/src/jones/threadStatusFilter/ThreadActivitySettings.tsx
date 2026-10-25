import { useState } from "react";
import { SettingsRow, SettingsSection } from "../../components/settings/settingsLayout";
import { searchableSetting } from "../../components/settings/settingsSearch";
import { Input } from "../../components/ui/input";
import { Switch } from "../../components/ui/switch";
import {
  Select,
  SelectItem,
  SelectPopup,
  SelectTrigger,
  SelectValue,
} from "../../components/ui/select";
import { persistClientSettingsPatch, useClientSettings } from "../../hooks/useSettings";

const units = ["minutes", "hours", "days", "weeks"] as const;

export function ThreadActivitySettings() {
  const enabled = useClientSettings((settings) => settings.threadActivityWindowEnabled);
  const amount = useClientSettings((settings) => settings.threadActivityWindowAmount);
  const unit = useClientSettings((settings) => settings.threadActivityWindowUnit);
  const [draft, setDraft] = useState<string | null>(null);
  const validAmount = Number(draft ?? amount);
  const valid = Number.isInteger(validAmount) && validAmount >= 1 && validAmount <= 10_000;
  const commitAmount = () => {
    if (valid) {
      void persistClientSettingsPatch({ threadActivityWindowAmount: validAmount });
      setDraft(null);
    }
  };
  return (
    <SettingsSection title="Sidebar activity">
      <SettingsRow
        {...searchableSetting("thread-activity-window")}
        description="Limit Working, Input and Done counts and views to recent thread activity. Done includes completions you have already read. All still shows every thread. Saved on this device."
        control={
          <Switch
            aria-label="Thread activity window"
            checked={enabled}
            onCheckedChange={(checked) =>
              void persistClientSettingsPatch({ threadActivityWindowEnabled: Boolean(checked) })
            }
          />
        }
      >
        <div className="flex flex-wrap items-center gap-2 py-2">
          <span className="text-sm text-muted-foreground">In the past</span>
          <div className="w-24 max-w-full">
            <Input
              type="number"
              aria-label="Activity window amount"
              min={1}
              max={10_000}
              step={1}
              disabled={!enabled}
              value={draft ?? String(amount)}
              aria-invalid={!valid}
              onChange={(event) => setDraft(event.target.value)}
              onBlur={commitAmount}
              onKeyDown={(event) => {
                if (event.key === "Enter") commitAmount();
              }}
            />
          </div>
          <Select
            value={unit}
            disabled={!enabled}
            onValueChange={(value) => {
              if (units.some((entry) => entry === value)) {
                void persistClientSettingsPatch({ threadActivityWindowUnit: value as typeof unit });
              }
            }}
          >
            <SelectTrigger aria-label="Activity window unit" className="w-32 min-w-0 max-w-full">
              <SelectValue>{unit}</SelectValue>
            </SelectTrigger>
            <SelectPopup>
              {units.map((value) => (
                <SelectItem key={value} value={value}>
                  {value}
                </SelectItem>
              ))}
            </SelectPopup>
          </Select>
        </div>
        {!valid && (
          <p role="alert" className="pb-2 text-sm text-destructive">
            Enter a whole number from 1 to 10,000.
          </p>
        )}
      </SettingsRow>
    </SettingsSection>
  );
}

import { ThreadActivitySettings } from "../threadStatusFilter/ThreadActivitySettings";
import { PresentationOrderSettings } from "../presentationOrder/PresentationOrderSettings";
import { persistClientSettingsPatch, useClientSettings } from "../../hooks/useSettings";
import { SettingsRow, SettingsSection } from "../../components/settings/settingsLayout";
import { searchableSetting } from "../../components/settings/settingsSearch";
import {
  Select,
  SelectItem,
  SelectPopup,
  SelectTrigger,
  SelectValue,
} from "../../components/ui/select";

const stageLabels = { pending: "Pending", queued: "Queued", sent: "Sent" } as const;

export function PromptsSettings() {
  const stage = useClientSettings((settings) => settings.promptsDefaultStage);
  return (
    <>
      <SettingsSection title="Prompts">
        <SettingsRow
          {...searchableSetting("prompts-default-stage")}
          description="View shown when you open Prompts."
          control={
            <Select
              value={stage}
              onValueChange={(value) => {
                if (value === "pending" || value === "queued" || value === "sent") {
                  void persistClientSettingsPatch({ promptsDefaultStage: value });
                }
              }}
            >
              <SelectTrigger size="sm" className="w-full sm:w-40" aria-label="Default prompt view">
                <SelectValue>{stageLabels[stage]}</SelectValue>
              </SelectTrigger>
              <SelectPopup align="end" alignItemWithTrigger={false}>
                <SelectItem hideIndicator value="pending">
                  Pending
                </SelectItem>
                <SelectItem hideIndicator value="queued">
                  Queued
                </SelectItem>
                <SelectItem hideIndicator value="sent">
                  Sent
                </SelectItem>
              </SelectPopup>
            </Select>
          }
        />
      </SettingsSection>
      <ThreadActivitySettings />
      <PresentationOrderSettings />
    </>
  );
}

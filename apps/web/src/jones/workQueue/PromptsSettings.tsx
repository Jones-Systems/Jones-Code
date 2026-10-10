import { persistClientSettingsPatch, useClientSettings } from "../../hooks/useSettings";
import {
  SettingsPageContainer,
  SettingsRow,
  SettingsSection,
} from "../../components/settings/settingsLayout";
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
    <SettingsPageContainer>
      <SettingsSection title="Jones Code">
        <SettingsRow
          {...searchableSetting("prompts-default-stage")}
          description="Choose the view shown when you open Prompts. Saved on this device."
          control={
            <Select
              value={stage}
              onValueChange={(value) => {
                if (value === "pending" || value === "queued" || value === "sent") {
                  void persistClientSettingsPatch({ promptsDefaultStage: value });
                }
              }}
            >
              <SelectTrigger
                aria-label="Default prompt view"
                className="min-w-0 max-w-none shrink-0"
              >
                <SelectValue>{stageLabels[stage]}</SelectValue>
              </SelectTrigger>
              <SelectPopup>
                <SelectItem value="pending">Pending</SelectItem>
                <SelectItem value="queued">Queued</SelectItem>
                <SelectItem value="sent">Sent</SelectItem>
              </SelectPopup>
            </Select>
          }
        />
      </SettingsSection>
    </SettingsPageContainer>
  );
}

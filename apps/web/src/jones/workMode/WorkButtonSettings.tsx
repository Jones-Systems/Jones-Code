import { SettingsRow, SettingsSection } from "../../components/settings/settingsLayout";
import { useSearchableSetting } from "../../components/settings/settingsSearch";
import { Switch } from "../../components/ui/switch";
import { persistClientSettingsPatch, useClientSettings } from "../../hooks/useSettings";

export function WorkButtonSettings() {
  const searchableSetting = useSearchableSetting();
  const visible = useClientSettings((settings) => settings.showWorkButton);
  return (
    <SettingsSection title="Sidebar appearance">
      <SettingsRow
        {...searchableSetting("show-work-button")}
        description="Show the Work button at the top of the sidebar. Hiding it does not turn Work mode off."
        control={
          <Switch
            aria-label="Show Work button"
            checked={visible}
            onCheckedChange={(checked) =>
              void persistClientSettingsPatch({ showWorkButton: Boolean(checked) })
            }
          />
        }
      />
    </SettingsSection>
  );
}

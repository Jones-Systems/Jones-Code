import { DEFAULT_SERVER_SETTINGS } from "@t3tools/contracts";

import { Switch } from "~/components/ui/switch";
import {
  SettingResetButton,
  SettingsRow,
  SettingsSection,
} from "~/components/settings/settingsLayout";
import {
  useScopedSettings,
  useScopedSettingsMixed,
  useUpdateScopedSettings,
} from "~/components/settings/useScopedSettings";

const SETTING_KEYS = ["workstreamAutoRouteEnabled"] as const;
const TITLE = "Assign queued prompts to a Workstream automatically";

/** Environment switch for routing queue-dispatched threads to a clearly matching Workstream. */
export function WorkstreamAutoRouteSettings() {
  const enabled = useScopedSettings((settings) => settings.workstreamAutoRouteEnabled);
  const updateSettings = useUpdateScopedSettings();
  const mixed = useScopedSettingsMixed(SETTING_KEYS);
  return (
    <SettingsSection id="workstream-auto-route" title="Workstream routing">
      <SettingsRow
        serverScoped
        settingKeys={SETTING_KEYS}
        mixed={mixed}
        title={TITLE}
        description="When a prompt arrives from a work queue for a thread with no Workstream, assign the thread when one Workstream is clearly the right place. Unclear matches are left alone."
        resetAction={
          enabled !== DEFAULT_SERVER_SETTINGS.workstreamAutoRouteEnabled ? (
            <SettingResetButton
              label="Workstream routing"
              onClick={() =>
                updateSettings({
                  workstreamAutoRouteEnabled: DEFAULT_SERVER_SETTINGS.workstreamAutoRouteEnabled,
                })
              }
            />
          ) : null
        }
        control={
          <Switch
            aria-label={TITLE}
            mixed={mixed}
            checked={mixed ? false : enabled}
            onCheckedChange={(checked) =>
              updateSettings({ workstreamAutoRouteEnabled: Boolean(checked) })
            }
          />
        }
      />
    </SettingsSection>
  );
}

import { DEFAULT_SERVER_SETTINGS } from "@t3tools/contracts";

import { Switch } from "~/components/ui/switch";
import {
  SettingResetButton,
  SettingsRow,
  SettingsSection,
} from "~/components/settings/settingsLayout";
import { searchableSetting } from "~/components/settings/settingsSearch";
import { useSettingsScope } from "~/components/settings/SettingsScopeContext";
import {
  useScopedSettings,
  useScopedSettingsMixed,
  useUpdateScopedSettings,
} from "~/components/settings/useScopedSettings";

const SETTING_KEYS = ["enableAgentDecisionSnapshotAccess"] as const;

/**
 * Explicit opt-in for the `decision_snapshot` MCP tool. Thread credentials
 * pick up a change the next time an agent session is prepared; a credential
 * issued under the previous value is rotated rather than reused.
 */
export function DecisionSnapshotAccessSettings() {
  const { scope } = useSettingsScope();
  const settings = useScopedSettings();
  const updateSettings = useUpdateScopedSettings();
  const mixed = useScopedSettingsMixed(SETTING_KEYS);
  const isProjectScope = scope.kind === "project" || scope.kind === "checkout";
  const enabled = settings.enableAgentDecisionSnapshotAccess;

  return (
    <SettingsSection id="decision-snapshot" title="Decision snapshot">
      <SettingsRow
        serverScoped
        settingKeys={SETTING_KEYS}
        mixed={mixed}
        {...searchableSetting("agent-decision-snapshot-access")}
        description={
          isProjectScope
            ? "Allow agents in this project to read the decision snapshot of thread, workstream and host usage. Applies when the agent session next starts."
            : "Allow agents to read the decision snapshot of thread, workstream and host usage from this environment's installed collector. Off by default; projects can override it."
        }
        resetAction={
          enabled !== DEFAULT_SERVER_SETTINGS.enableAgentDecisionSnapshotAccess ? (
            <SettingResetButton
              label="default decision snapshot access"
              onClick={() =>
                updateSettings({
                  enableAgentDecisionSnapshotAccess:
                    DEFAULT_SERVER_SETTINGS.enableAgentDecisionSnapshotAccess,
                })
              }
            />
          ) : null
        }
        control={
          <Switch
            aria-label="Agent decision snapshot access"
            mixed={mixed}
            checked={mixed ? false : enabled}
            onCheckedChange={(checked) =>
              updateSettings({ enableAgentDecisionSnapshotAccess: Boolean(checked) })
            }
          />
        }
      />
    </SettingsSection>
  );
}

import { SettingsRow, SettingsSection } from "../../components/settings/settingsLayout";
import { useEnvironments } from "../../state/environments";
import { usePersonalWorkstreams } from "../../state/workstreamsPersonal";
export const personalStatusLabels = {
  ready: "Ready",
  offline: "Offline",
  update_required: "Update required",
  read_only: "Read only",
  sync_failed: "Couldn't sync",
} as const;
export function WorkstreamsSetupStatus() {
  const { state } = usePersonalWorkstreams();
  const { environments } = useEnvironments();
  return (
    <SettingsSection title="Workstreams">
      {state.environments.map((entry) => (
        <SettingsRow
          key={entry.environmentId}
          title={
            environments.find((item) => item.environmentId === entry.environmentId)?.label ??
            "Environment"
          }
          control={
            <span className="text-sm text-muted-foreground">
              {personalStatusLabels[entry.status]}
            </span>
          }
        />
      ))}
    </SettingsSection>
  );
}

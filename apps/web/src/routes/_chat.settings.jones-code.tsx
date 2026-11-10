import { createFileRoute } from "@tanstack/react-router";
import { SettingsPageContainer } from "../components/settings/settingsLayout";
import { PromptsSettings } from "../jones/workQueue/PromptsSettings";

import { WorkstreamsSetupStatus } from "../jones/workstreams/WorkstreamsSetupStatus";

function JonesCodeSettings() {
  return (
    <SettingsPageContainer>
      <PromptsSettings />
      <WorkstreamsSetupStatus />
    </SettingsPageContainer>
  );
}

export const Route = createFileRoute("/_chat/settings/jones-code")({
  component: JonesCodeSettings,
});

import { createFileRoute } from "@tanstack/react-router";
import { SettingsPageContainer } from "../components/settings/settingsLayout";
import { WorkButtonSettings } from "../jones/workMode/WorkButtonSettings";
import { PromptsSettings } from "../jones/workQueue/PromptsSettings";

import { WorkstreamsSetupStatus } from "../jones/workstreams/WorkstreamsSetupStatus";
import { WorkstreamAutoRouteSettings } from "../jones/settings/WorkstreamAutoRouteSettings";

function JonesCodeSettings() {
  return (
    <SettingsPageContainer>
      <WorkButtonSettings />
      <PromptsSettings />
      <WorkstreamsSetupStatus />
      <WorkstreamAutoRouteSettings />
    </SettingsPageContainer>
  );
}

export const Route = createFileRoute("/_chat/settings/jones-code")({
  component: JonesCodeSettings,
});

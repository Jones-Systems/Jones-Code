import { createFileRoute } from "@tanstack/react-router";
import { PromptsSettings } from "../jones/workQueue/PromptsSettings";

import { WorkstreamsSetupStatus } from "../jones/workstreams/WorkstreamsSetupStatus";

function JonesCodeSettings() {
  return (
    <div className="min-h-0 flex-1 overflow-y-auto">
      <PromptsSettings />
      <WorkstreamsSetupStatus />
    </div>
  );
}

export const Route = createFileRoute("/settings/jones-code")({ component: JonesCodeSettings });

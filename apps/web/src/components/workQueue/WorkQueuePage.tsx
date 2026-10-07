import { useState } from "react";
import type { EnvironmentId } from "@t3tools/contracts";
import { useEnvironments, usePrimaryEnvironmentId } from "../../state/environments";
import { WorkQueueMetadataPanel } from "../../jones/workQueue/WorkQueueMetadataPanel";
import { useWorkQueueMetadata } from "../../jones/workQueue/useWorkQueueMetadata";
import { useBlocker } from "@tanstack/react-router";
import { isElectron } from "../../env";
import { WorkspacePageHeader } from "../WorkspacePageHeader";
import { WorkspacePageContainer } from "../WorkspacePageContainer";
import { SidebarInset } from "../ui/sidebar";
import { Button } from "../ui/button";
import { Alert, AlertAction, AlertDescription, AlertTitle } from "../ui/alert";
import { createMockWorkQueueSource } from "./mockWorkQueue";
import { WorkQueuePanel } from "./WorkQueuePanel";

export function WorkQueuePage() {
  const { environments } = useEnvironments();
  const primaryId = usePrimaryEnvironmentId();
  const [requestedId, setRequestedId] = useState<EnvironmentId | null>(null);
  const selected =
    environments.find((item) => item.environmentId === requestedId) ??
    environments.find((item) => item.environmentId === primaryId) ??
    environments[0];
  const [source] = useState(createMockWorkQueueSource);
  const [dirty, setDirty] = useState(false);
  const blocker = useBlocker({
    shouldBlockFn: () => dirty,
    enableBeforeUnload: () => dirty,
    withResolver: true,
  });
  return (
    <SidebarInset className="h-dvh overflow-hidden">
      <WorkspacePageHeader electron={isElectron}>
        <span className="text-sm font-medium">Submitted work</span>
      </WorkspacePageHeader>
      <div className="min-h-0 flex-1 overflow-y-auto">
        <WorkspacePageContainer width="expanded">
          <label className="flex items-center gap-3 text-sm">
            Environment
            <select
              aria-label="Submitted work environment"
              value={selected?.environmentId ?? ""}
              className="rounded-md border bg-background px-2 py-1"
              onChange={(event) => {
                const environment = environments.find(
                  (item) => item.environmentId === event.target.value,
                );
                if (environment) setRequestedId(environment.environmentId);
              }}
            >
              {environments.map((item) => (
                <option key={item.environmentId} value={item.environmentId}>
                  {item.label}
                </option>
              ))}
            </select>
          </label>
          {!selected ? (
            <p>No environment is connected.</p>
          ) : !selected.entry.enabled ? (
            <p>Queue metadata unsupported: this environment is disabled.</p>
          ) : selected.connection.phase !== "connected" ? (
            <p>Queue metadata unavailable: this environment is not connected.</p>
          ) : selected.serverConfig?.environment.capabilities.workQueueMetadata !== true ? (
            <p>Queue metadata unsupported by this environment.</p>
          ) : (
            <EnvironmentWorkQueue
              key={selected.environmentId}
              environmentId={selected.environmentId}
            />
          )}
          <section aria-label="Mock queue preview" className="mt-8 space-y-4">
            <h2 className="text-lg font-medium">Mock queue preview</h2>
            <p className="text-sm text-muted-foreground">
              Synthetic preview data is separate from the sampled queue metadata above.
            </p>
            {blocker.status === "blocked" && (
              <Alert variant="warning">
                <AlertTitle>Leave this preview?</AlertTitle>
                <AlertDescription>Your unsaved draft will be discarded.</AlertDescription>
                <AlertAction>
                  <Button variant="outline" onClick={() => blocker.reset()}>
                    Keep editing
                  </Button>
                  <Button onClick={() => blocker.proceed()}>Discard changes and leave</Button>
                </AlertAction>
              </Alert>
            )}
            <WorkQueuePanel source={source} onDirtyChange={setDirty} />
          </section>
        </WorkspacePageContainer>
      </div>
    </SidebarInset>
  );
}

function EnvironmentWorkQueue({ environmentId }: { environmentId: EnvironmentId }) {
  const load = useWorkQueueMetadata(environmentId);
  return <WorkQueueMetadataPanel load={load} />;
}

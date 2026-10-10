import { useCallback, useRef, useState } from "react";
import { useBlocker } from "@tanstack/react-router";
import type { EnvironmentId } from "@t3tools/contracts";
import { useEnvironments, usePrimaryEnvironmentId } from "../../state/environments";
import { isElectron } from "../../env";
import { WorkspacePageHeader } from "../../components/WorkspacePageHeader";
import { WorkspacePageContainer } from "../../components/WorkspacePageContainer";
import { SidebarInset } from "../../components/ui/sidebar";
import { Button } from "../../components/ui/button";
import { Alert, AlertAction, AlertDescription, AlertTitle } from "../../components/ui/alert";
import { EnvironmentVoiceReview } from "../../components/voiceReview/VoiceReviewPage";
import { WorkQueueMetadataPanel } from "./WorkQueueMetadataPanel";
import { useWorkQueueMetadata } from "./useWorkQueueMetadata";

export type PromptQueueView = "pending" | "queued" | "sent";
const views = ["pending", "queued", "sent"] as const;

export function UnifiedQueuePage({ defaultView = "pending" }: { defaultView?: PromptQueueView }) {
  const { environments } = useEnvironments();
  const primaryId = usePrimaryEnvironmentId();
  const [requestedId, setRequestedId] = useState<EnvironmentId | null>(null);
  const selected =
    environments.find((item) => item.environmentId === requestedId) ??
    environments.find((item) => item.environmentId === primaryId) ??
    environments[0];
  const [pane, setPane] = useState<PromptQueueView>(defaultView);
  const tabs = useRef<Array<HTMLButtonElement | null>>([]);
  const [dirty, setDirty] = useState(false);
  const reportDirty = useCallback((value: boolean) => setDirty(value), []);
  const [nextEnvironment, setNextEnvironment] = useState<EnvironmentId | null>(null);
  const blocker = useBlocker({
    shouldBlockFn: () => dirty,
    enableBeforeUnload: () => dirty,
    withResolver: true,
  });
  const changeEnvironment = (environmentId: EnvironmentId) => {
    if (environmentId === selected?.environmentId) return;
    if (dirty) setNextEnvironment(environmentId);
    else setRequestedId(environmentId);
  };
  return (
    <SidebarInset className="h-dvh min-h-0 overflow-hidden">
      <WorkspacePageHeader electron={isElectron}>
        <h1 className="text-sm font-medium">Prompts</h1>
      </WorkspacePageHeader>
      <div className="min-h-0 flex-1 overflow-y-auto">
        <WorkspacePageContainer width="expanded">
          <div className="flex flex-wrap items-center justify-between gap-4">
            <div
              role="tablist"
              aria-label="Prompt views"
              className="inline-flex rounded-full bg-muted/60 p-1"
            >
              {views.map((value, index) => (
                <button
                  key={value}
                  ref={(element) => {
                    tabs.current[index] = element;
                  }}
                  type="button"
                  role="tab"
                  id={`queue-${value}-tab`}
                  aria-controls="queue-view-panel"
                  aria-selected={pane === value}
                  tabIndex={pane === value ? 0 : -1}
                  className={`min-w-28 rounded-full px-6 py-2 text-sm font-medium transition-colors focus-visible:outline-2 focus-visible:outline-ring ${pane === value ? "bg-muted text-foreground shadow-sm" : "text-muted-foreground hover:text-foreground"}`}
                  onClick={() => setPane(value)}
                  onKeyDown={(event) => {
                    if (!["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) return;
                    event.preventDefault();
                    const next =
                      event.key === "Home"
                        ? 0
                        : event.key === "End"
                          ? views.length - 1
                          : (index + (event.key === "ArrowRight" ? 1 : -1) + views.length) %
                            views.length;
                    setPane(views[next]!);
                    tabs.current[next]?.focus();
                  }}
                >
                  {value === "pending" ? "Pending" : value === "queued" ? "Queued" : "Sent"}
                </button>
              ))}
            </div>
            <label className="flex items-center gap-3 text-sm">
              Environment
              <select
                aria-label="Prompts environment"
                value={selected?.environmentId ?? ""}
                className="rounded-md border bg-background px-2 py-1"
                onChange={(event) => {
                  const environment = environments.find(
                    (item) => item.environmentId === event.target.value,
                  );
                  if (environment) changeEnvironment(environment.environmentId);
                }}
              >
                {environments.map((item) => (
                  <option key={item.environmentId} value={item.environmentId}>
                    {item.label}
                  </option>
                ))}
              </select>
            </label>
          </div>
          <p className="text-sm text-muted-foreground">
            {pane === "pending"
              ? "Review prompts before release. Editing a voice prompt pauses its delivery."
              : pane === "queued"
                ? "Work waiting to be sent and prompts released for processing."
                : "Confirmed handoffs to conversations. Sent does not mean an agent completed the work."}
          </p>
          {blocker.status === "blocked" || nextEnvironment !== null ? (
            <Alert variant="warning">
              <AlertTitle>Leave Prompts?</AlertTitle>
              <AlertDescription>
                An edit or action is in progress. Leaving discards local unsaved text; it does not
                resume a paused prompt or retry an uncertain action.
              </AlertDescription>
              <AlertAction>
                <Button
                  variant="outline"
                  onClick={() => {
                    setNextEnvironment(null);
                    if (blocker.status === "blocked") blocker.reset();
                  }}
                >
                  Keep reviewing
                </Button>
                <Button
                  onClick={() => {
                    if (nextEnvironment !== null) {
                      setRequestedId(nextEnvironment);
                      setNextEnvironment(null);
                      setDirty(false);
                    }
                    if (blocker.status === "blocked") blocker.proceed();
                  }}
                >
                  Leave Prompts
                </Button>
              </AlertAction>
            </Alert>
          ) : null}
          <div
            role="tabpanel"
            id="queue-view-panel"
            aria-labelledby={`queue-${pane}-tab`}
            className="space-y-6"
          >
            {!selected ? (
              <p>No environment is connected.</p>
            ) : (
              <>
                {!selected.entry.enabled ? (
                  <p>Queue unavailable: this environment is disabled.</p>
                ) : selected.connection.phase !== "connected" ? (
                  <p>
                    Queue unavailable: this environment is not connected. Previously observed
                    prompts may be stale.
                  </p>
                ) : null}
                <EnvironmentVoiceReview
                  key={selected.environmentId}
                  environmentId={selected.environmentId}
                  pane={pane}
                  onDirtyChange={reportDirty}
                  unavailable={!selected.entry.enabled || selected.connection.phase !== "connected"}
                />
                <div hidden={pane !== "queued"}>
                  {!selected.entry.enabled || selected.connection.phase !== "connected" ? (
                    <p>
                      Queue metadata unavailable while this environment is disconnected or disabled.
                    </p>
                  ) : selected.serverConfig?.environment.capabilities.workQueueMetadata !== true ? (
                    <p>Queue metadata unsupported by this environment.</p>
                  ) : (
                    <EnvironmentWorkQueue
                      key={selected.environmentId}
                      environmentId={selected.environmentId}
                    />
                  )}
                </div>
                {pane === "sent" ? (
                  <p className="text-sm text-muted-foreground">
                    Sent history is not available from this connection yet. Prompts whose handoff
                    cannot be confirmed remain under Queued. This does not mean no work has been
                    sent.
                  </p>
                ) : null}
              </>
            )}
          </div>
        </WorkspacePageContainer>
      </div>
    </SidebarInset>
  );
}

function EnvironmentWorkQueue({ environmentId }: { environmentId: EnvironmentId }) {
  const load = useWorkQueueMetadata(environmentId);
  return <WorkQueueMetadataPanel load={load} />;
}

export function PromptQueueSettingsStatus({
  status,
  onRetry,
}: {
  status: "pending" | "failed" | "retrying";
  onRetry: () => void;
}) {
  return (
    <SidebarInset className="h-dvh min-h-0 overflow-hidden">
      <WorkspacePageHeader electron={isElectron}>
        <h1 className="text-sm font-medium">Prompts</h1>
      </WorkspacePageHeader>
      <WorkspacePageContainer>
        {status === "failed" ? (
          <>
            <p role="alert">Prompts preferences could not be loaded.</p>
            <Button variant="outline" onClick={onRetry}>
              Retry preferences
            </Button>
          </>
        ) : (
          <p role="status">Loading Prompts preferences…</p>
        )}
      </WorkspacePageContainer>
    </SidebarInset>
  );
}

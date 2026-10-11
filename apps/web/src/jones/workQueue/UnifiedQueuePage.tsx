import { useCallback, useEffect, useRef, useState } from "react";
import { useBlocker } from "@tanstack/react-router";
import { ChevronDownIcon } from "lucide-react";
import type { EnvironmentId } from "@t3tools/contracts";
import { useEnvironments, type EnvironmentPresentation } from "../../state/environments";
import { isElectron } from "../../env";
import { WorkspacePageHeader } from "../../components/WorkspacePageHeader";
import { WorkspacePageContainer } from "../../components/WorkspacePageContainer";
import {
  WorkspaceBreadcrumb,
  WorkspaceBreadcrumbItem,
  WorkspaceBreadcrumbSeparator,
} from "../../components/WorkspaceBreadcrumb";
import { SidebarInset } from "../../components/ui/sidebar";
import { Button, InlineButton } from "../../components/ui/button";
import { Alert, AlertAction, AlertDescription, AlertTitle } from "../../components/ui/alert";
import {
  Menu,
  MenuTrigger,
  MenuPopup,
  MenuCheckboxItem,
  MenuSeparator,
} from "../../components/ui/menu";
import { Toggle, ToggleGroup } from "../../components/ui/toggle-group";
import { EnvironmentVoiceReview } from "../../components/voiceReview/VoiceReviewPage";
import { WorkQueueMetadataPanel } from "./WorkQueueMetadataPanel";
import { SentPrompts } from "./SentPrompts";
import { useWorkQueueMetadata } from "./useWorkQueueMetadata";

export type PromptQueueView = "pending" | "queued" | "sent";
const views = ["pending", "queued", "sent"] as const;

export function UnifiedQueuePage({ defaultView = "pending" }: { defaultView?: PromptQueueView }) {
  const { environments } = useEnvironments();
  const [requestedId, setRequestedId] = useState<EnvironmentId | null>(null);
  const selected = environments.filter(
    (environment) => requestedId === null || environment.environmentId === requestedId,
  );
  const [pane, setPane] = useState<PromptQueueView>(defaultView);
  const tabs = useRef<Array<HTMLButtonElement | null>>([]);
  const [dirtyEnvironments, setDirtyEnvironments] = useState<ReadonlySet<EnvironmentId>>(
    () => new Set(),
  );
  const reportDirty = useCallback((environmentId: EnvironmentId, dirty: boolean) => {
    setDirtyEnvironments((current) => {
      if (current.has(environmentId) === dirty) return current;
      const next = new Set(current);
      if (dirty) next.add(environmentId);
      else next.delete(environmentId);
      return next;
    });
  }, []);
  const [pendingScope, setPendingScope] = useState<{ environmentId: EnvironmentId | null } | null>(
    null,
  );
  const dirty = dirtyEnvironments.size > 0;
  const blocker = useBlocker({
    shouldBlockFn: () => dirty,
    enableBeforeUnload: () => dirty,
    withResolver: true,
  });
  const applyScope = (environmentId: EnvironmentId | null) => {
    setRequestedId(environmentId);
    setDirtyEnvironments((current) =>
      environmentId === null ? current : new Set([...current].filter((id) => id === environmentId)),
    );
    setPendingScope(null);
  };
  const changeEnvironment = (environmentId: EnvironmentId | null) => {
    if (environmentId === requestedId) return;
    const removesDirtyEnvironment = [...dirtyEnvironments].some(
      (id) => environmentId !== null && id !== environmentId,
    );
    if (removesDirtyEnvironment) setPendingScope({ environmentId });
    else applyScope(environmentId);
  };
  return (
    <SidebarInset className="h-dvh min-h-0 overflow-hidden">
      <WorkspacePageHeader electron={isElectron}>
        <WorkspaceBreadcrumb ariaLabel="Prompts breadcrumb" className="min-w-0 flex-1">
          <WorkspaceBreadcrumbItem>
            <h1>Prompts</h1>
          </WorkspaceBreadcrumbItem>
          <WorkspaceBreadcrumbSeparator />
          <WorkspaceBreadcrumbItem current className="min-w-10">
            <Menu>
              <MenuTrigger
                render={<InlineButton />}
                aria-label="Prompts environment"
                className="group/prompts-environment min-w-0 max-w-full"
              >
                <span className="min-w-0 truncate">
                  {requestedId === null
                    ? "All environments"
                    : (selected[0]?.label ?? "Unavailable")}
                </span>
                <ChevronDownIcon
                  className="size-3.5 shrink-0 text-muted-foreground opacity-0 transition-opacity group-hover/prompts-environment:opacity-100 group-focus-visible/prompts-environment:opacity-100 group-data-popup-open/prompts-environment:opacity-100"
                  aria-hidden
                />
              </MenuTrigger>
              <MenuPopup align="start">
                <MenuCheckboxItem
                  checked={requestedId === null}
                  closeOnClick
                  onClick={() => changeEnvironment(null)}
                >
                  All environments
                </MenuCheckboxItem>
                <MenuSeparator />
                {environments.map((environment) => (
                  <MenuCheckboxItem
                    key={environment.environmentId}
                    checked={requestedId === environment.environmentId}
                    closeOnClick
                    onClick={() => changeEnvironment(environment.environmentId)}
                  >
                    {environment.label}
                  </MenuCheckboxItem>
                ))}
              </MenuPopup>
            </Menu>
          </WorkspaceBreadcrumbItem>
        </WorkspaceBreadcrumb>
        <ToggleGroup
          role="tablist"
          aria-label="Prompt views"
          variant="segmented"
          value={[pane]}
          onValueChange={(next) => {
            const value = next[0];
            if (views.some((view) => view === value)) setPane(value as PromptQueueView);
          }}
        >
          {views.map((value, index) => (
            <Toggle
              key={value}
              ref={(element) => {
                tabs.current[index] = element;
              }}
              value={value}
              role="tab"
              id={`queue-${value}-tab`}
              aria-controls="queue-view-panel"
              aria-selected={pane === value}
              aria-pressed={undefined}
              tabIndex={pane === value ? 0 : -1}
              onKeyDownCapture={(event) => {
                if (!["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) return;
                event.preventDefault();
                event.stopPropagation();
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
            </Toggle>
          ))}
        </ToggleGroup>
      </WorkspacePageHeader>
      <div className="min-h-0 flex-1 overflow-y-auto">
        <WorkspacePageContainer width="expanded">
          {blocker.status === "blocked" || pendingScope !== null ? (
            <Alert variant="warning">
              <AlertTitle>Leave Prompts?</AlertTitle>
              <AlertDescription>
                An edit or action is in progress. Leaving discards local unsaved text in
                environments you leave; it does not resume a paused prompt or retry an uncertain
                action.
              </AlertDescription>
              <AlertAction>
                <Button
                  variant="outline"
                  onClick={() => {
                    setPendingScope(null);
                    if (blocker.status === "blocked") blocker.reset();
                  }}
                >
                  Keep reviewing
                </Button>
                <Button
                  onClick={() => {
                    if (blocker.status === "blocked") {
                      setPendingScope(null);
                      blocker.proceed();
                    } else if (pendingScope !== null) {
                      applyScope(pendingScope.environmentId);
                    }
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
            {selected.length === 0 ? (
              <p className="text-sm text-muted-foreground">
                {environments.length === 0
                  ? "No environments configured."
                  : "The selected environment is unavailable."}
              </p>
            ) : (
              selected.map((environment) => (
                <EnvironmentPrompts
                  key={environment.environmentId}
                  environment={environment}
                  pane={pane}
                  grouped={requestedId === null}
                  onDirtyChange={reportDirty}
                />
              ))
            )}
          </div>
        </WorkspacePageContainer>
      </div>
    </SidebarInset>
  );
}

function EnvironmentPrompts({
  environment,
  pane,
  grouped,
  onDirtyChange,
}: {
  environment: EnvironmentPresentation;
  pane: PromptQueueView;
  grouped: boolean;
  onDirtyChange: (environmentId: EnvironmentId, dirty: boolean) => void;
}) {
  const { environmentId } = environment;
  const reportDirty = useCallback(
    (dirty: boolean) => onDirtyChange(environmentId, dirty),
    [environmentId, onDirtyChange],
  );
  useEffect(() => () => onDirtyChange(environmentId, false), [environmentId, onDirtyChange]);
  const unavailable = !environment.entry.enabled || environment.connection.phase !== "connected";
  return (
    <section aria-label={`${environment.label} prompts`} className="space-y-4">
      {grouped ? <h2 className="text-sm font-medium">{environment.label}</h2> : null}
      {!environment.entry.enabled ? (
        <p className="text-sm text-muted-foreground">This environment is disabled.</p>
      ) : environment.connection.phase !== "connected" ? (
        <p className="text-sm text-muted-foreground">
          This environment is disconnected. Previously observed prompts may be stale.
        </p>
      ) : null}
      <EnvironmentVoiceReview
        environmentId={environmentId}
        pane={pane}
        onDirtyChange={reportDirty}
        unavailable={unavailable}
      />
      <div hidden={pane !== "queued"}>
        {unavailable ? null : environment.serverConfig?.environment.capabilities
            .workQueueMetadata !== true ? (
          <p className="text-sm text-muted-foreground">
            Queue metadata unsupported by this environment.
          </p>
        ) : (
          <EnvironmentWorkQueue environmentId={environmentId} />
        )}
      </div>
      {pane === "sent" ? <SentPrompts state={{ status: "unavailable" }} /> : null}
    </section>
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

import {
  useEffect,
  useEffectEvent,
  useRef,
  useState,
  useSyncExternalStore,
  type ReactNode,
} from "react";
import { useBlocker } from "@tanstack/react-router";
import { ArrowUpRightIcon, ChevronDownIcon } from "lucide-react";
import {
  readWorkQueueLocalNames,
  subscribeWorkQueueLocalNames,
  workQueueDestinationLabel,
} from "@t3tools/client-runtime/work-queue-metadata";
import { EnvironmentId, type WorkQueueMetadataResult } from "@t3tools/contracts";
import { usePersonalWorkstreams } from "../../state/workstreamsPersonal";
import { useQuietThreadShells } from "../../state/entities";
import { useEnvironments, type EnvironmentPresentation } from "../../state/environments";
import { isElectron } from "../../env";
import { useStableCallback } from "../../hooks/useStableCallback";
import { useMediaQuery } from "../../hooks/useMediaQuery";
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
import { Empty, EmptyDescription, EmptyHeader, EmptyTitle } from "../../components/ui/empty";
import {
  Menu,
  MenuTrigger,
  MenuPopup,
  MenuCheckboxItem,
  MenuSeparator,
} from "../../components/ui/menu";
import { Toggle, ToggleGroup } from "../../components/ui/toggle-group";
import {
  EnvironmentVoiceReview,
  useVoiceReviewMetadata,
  type VoiceReviewMetadata,
} from "../../components/voiceReview/VoiceReviewPage";
import { RoutingDiagnostics } from "../../components/voiceReview/RoutingDiagnostics";
import {
  recentThread,
  recentThreadPurpose,
  VoiceWorkstreamCorrections,
  VoiceWorkstreamCorrectionSession,
} from "../../components/voiceReview/RecentVoicePrompts";
import { useVoiceReview } from "../../components/voiceReview/useVoiceReview";
import { WorkQueueMetadataPanel } from "./WorkQueueMetadataPanel";
import { SentPrompts } from "./SentPrompts";
import { useWorkQueueMetadata } from "./useWorkQueueMetadata";
import { buildPromptItems, type PromptDestination, type PromptItem } from "./promptItems";
import { promptAttention } from "./promptAttention";
import { combinePromptCounts, type PromptCount } from "./promptOrganize";
import {
  itemStatus,
  promptProgressSteps,
  queueStateLabel,
  requestKindLabel,
  routingStateLabel,
  textStateLabel,
  turnStateLabel,
} from "./promptPresentation";
import {
  PromptToolbar,
  PromptFilteredEmpty,
  filteredPromptGroups,
  usePromptListView,
  type PromptListView,
} from "./PromptToolbar";
import { promptLink, type PromptNavigation } from "./promptSearch";
import { PromptWorkstreams } from "./PromptWorkstreamDetails";
import { promptWorkstreams, type PromptWorkstreamLine } from "./promptWorkstreams";
import { PromptMasterDetail } from "./PromptList";
import { PromptDetail, PromptDisclosure, PromptEvidence } from "./PromptDetail";
import { PromptProgress } from "./PromptProgress";
import { displayText, threadHref, usePromptTime, type PromptRowView } from "./PromptRow";
import { PromptThreadStatus, PromptThreadTitle, usePromptThreadShell } from "./PromptThreadStatus";

export type PromptQueueView = "pending" | "queued" | "sent";
const views = ["pending", "queued", "sent"] as const;
const viewLabels: Readonly<Record<PromptQueueView, string>> = {
  pending: "Pending",
  queued: "Queued",
  sent: "Sent",
};
const stageSentences: Readonly<Record<PromptQueueView, string>> = {
  pending: "Held before release. You can still pause, edit, send, or delete these.",
  queued: "Released and waiting to be handed to a thread. Read-only.",
  sent: "Handoff to a thread is confirmed. This does not mean the work is finished.",
};
const RECENT_LIMIT = 50;

interface StageCounts {
  readonly pending: PromptCount | null;
  readonly queued: PromptCount | null;
}

const countKey = (count: PromptCount | null) =>
  count === null ? "none" : `${count.count}${count.qualifier}`;
const stageCountsKey = (counts: StageCounts | undefined) =>
  counts === undefined ? "absent" : `${countKey(counts.pending)}/${countKey(counts.queued)}`;

export function UnifiedQueuePage({
  defaultView = "pending",
  navigation,
}: {
  defaultView?: PromptQueueView;
  navigation?: PromptNavigation | undefined;
}) {
  const { environments } = useEnvironments();
  const search = navigation?.search;
  const [localScope, setLocalScope] = useState<EnvironmentId | null>(null);
  const requestedId =
    search?.source && !search.prompt ? EnvironmentId.make(search.source) : localScope;
  const linkedPrompt = search?.prompt;
  const [sources, setSources] = useState<ReadonlyMap<EnvironmentId, PromptSources>>(
    () => new Map(),
  );
  const [sourceRevision, setSourceRevision] = useState(0);
  const reportSources = useStableCallback((environmentId: EnvironmentId, result: PromptSources) => {
    setSources((current) => new Map(current).set(environmentId, result));
  });
  const [counts, setCounts] = useState<ReadonlyMap<EnvironmentId, StageCounts>>(() => new Map());
  const reportCounts = useStableCallback(
    (environmentId: EnvironmentId, next: StageCounts | null) => {
      setCounts((current) => {
        const previous = current.get(environmentId);
        if (
          next === null ? previous === undefined : stageCountsKey(previous) === stageCountsKey(next)
        )
          return current;
        const updated = new Map(current);
        if (next === null) updated.delete(environmentId);
        else updated.set(environmentId, next);
        return updated;
      });
    },
  );
  const [localPane, setLocalPane] = useState<PromptQueueView>(defaultView);
  const pane = search?.stage ?? localPane;
  const listControls = usePromptListView(pane, navigation);
  const setPane = (stage: PromptQueueView) => {
    if (stage === pane) return;
    setLocalPane(stage);
    navigation?.replace({
      stage,
      source: requestedId ?? undefined,
      prompt: undefined,
      filter: undefined,
      group: undefined,
    });
  };
  const linkSelection = (environmentId: EnvironmentId, key: string) => {
    if (requestedId !== localScope) setLocalScope(requestedId);
    navigation?.replace({ source: environmentId, stage: pane, prompt: key });
  };
  const tabs = useRef<Array<HTMLButtonElement | null>>([]);
  const [dirtyEnvironments, setDirtyEnvironments] = useState<ReadonlySet<EnvironmentId>>(
    () => new Set(),
  );
  const configured = environments.filter((environment) => {
    const source = sources.get(environment.environmentId);
    return (
      source?.voice === "configured" ||
      source?.queue === "configured" ||
      dirtyEnvironments.has(environment.environmentId)
    );
  });
  const selected = configured.filter(
    (environment) => requestedId === null || environment.environmentId === requestedId,
  );
  const unresolved = environments.filter((environment) => {
    const source = sources.get(environment.environmentId);
    return !source || source.voice === "unknown" || source.queue === "unknown" || source.checking;
  });
  const unconfigured = environments.filter((environment) => {
    const source = sources.get(environment.environmentId);
    return (
      environment.entry.enabled &&
      environment.connection.phase === "connected" &&
      source?.voice === "absent" &&
      source.queue === "absent" &&
      !source.checking
    );
  });
  const reportDirty = useStableCallback((environmentId: EnvironmentId, dirty: boolean) => {
    setDirtyEnvironments((current) => {
      if (current.has(environmentId) === dirty) return current;
      const next = new Set(current);
      if (dirty) next.add(environmentId);
      else next.delete(environmentId);
      return next;
    });
  });
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
    setLocalScope(environmentId);
    navigation?.replace({ source: environmentId ?? undefined, prompt: undefined });
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
  const linkedSourceMissing =
    linkedPrompt &&
    search?.source &&
    !selected.some((environment) => environment.environmentId === search.source) &&
    !unresolved.some((environment) => environment.environmentId === search.source);
  const stageCount = (stage: PromptQueueView) =>
    stage === "sent"
      ? null
      : combinePromptCounts(
          environments.flatMap(({ environmentId }) => {
            if (requestedId !== null && environmentId !== requestedId) return [];
            const source = sources.get(environmentId);
            if (
              source?.voice === "absent" &&
              !source.voiceChecking &&
              (stage === "pending" || (source.queue === "absent" && !source.queueChecking))
            )
              return [];
            return [counts.get(environmentId)?.[stage] ?? null];
          }),
        );
  return (
    <SidebarInset className="h-dvh min-h-0 overflow-hidden">
      <WorkspacePageHeader
        electron={isElectron}
        className="max-sm:grid max-sm:h-auto! max-sm:min-h-0! max-sm:max-h-none! max-sm:grid-cols-[minmax(0,1fr)_auto] max-sm:gap-y-1.5 max-sm:py-2 max-sm:[&>[data-generic-monitor-control]]:col-start-2 max-sm:[&>[data-generic-monitor-control]]:row-start-1"
      >
        <WorkspaceBreadcrumb
          ariaLabel="Prompts breadcrumb"
          className="min-w-0 flex-1 max-sm:col-start-1 max-sm:row-start-1"
        >
          <WorkspaceBreadcrumbItem>
            <h1>Prompts</h1>
          </WorkspaceBreadcrumbItem>
          {configured.length > 0 ? <WorkspaceBreadcrumbSeparator /> : null}
          <WorkspaceBreadcrumbItem current>
            {configured.length === 1 && requestedId === null ? (
              <span className="truncate">{configured[0]!.label}</span>
            ) : configured.length > 1 || requestedId !== null ? (
              <Menu>
                <MenuTrigger
                  render={<InlineButton />}
                  aria-label="Prompts environment"
                  className="group/prompts-environment min-w-0 max-w-full"
                >
                  <span className="min-w-0 truncate">
                    {requestedId === null
                      ? "All prompt sources"
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
                    All prompt sources
                  </MenuCheckboxItem>
                  <MenuSeparator />
                  {configured.map((environment) => (
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
            ) : null}
          </WorkspaceBreadcrumbItem>
        </WorkspaceBreadcrumb>
        <ToggleGroup
          role="tablist"
          aria-label="Prompt views"
          className="max-sm:col-span-2 max-sm:row-start-2 max-sm:justify-self-start"
          variant="segmented"
          value={[pane]}
          onValueChange={(next) => {
            const value = next[0];
            if (views.some((view) => view === value)) setPane(value as PromptQueueView);
          }}
        >
          {views.map((value, index) => {
            const count = stageCount(value);
            const countText = count === null ? null : `${count.count}${count.qualifier}`;
            return (
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
                aria-label={
                  count === null
                    ? undefined
                    : `${viewLabels[value]}, ${count.qualifier === "+" ? "at least " : ""}${count.count} ${count.count === 1 ? "prompt" : "prompts"}${count.qualifier === "?" ? ", some sources unknown" : ""}`
                }
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
                {viewLabels[value]}
                {countText === null ? null : (
                  <span className="tabular-nums text-muted-foreground"> · {countText}</span>
                )}
              </Toggle>
            );
          })}
        </ToggleGroup>
      </WorkspacePageHeader>
      <div className="min-h-0 flex-1 overflow-y-auto">
        <WorkspacePageContainer width="expanded">
          {environments.map((environment) => (
            <PromptSourceDiscovery
              key={environment.environmentId}
              environment={environment}
              revision={sourceRevision}
              onChange={reportSources}
            />
          ))}
          {configured.length > 0 ? (
            <div className="space-y-1 text-sm text-muted-foreground">
              <p className="text-foreground">{stageSentences[pane]}</p>
              <p>
                Prompts are stored on their source server and accessible from any connected device.
              </p>
            </div>
          ) : null}
          {unresolved.length > 0 ? (
            <div role="status" className="space-y-2 text-sm text-muted-foreground">
              {unresolved.map((environment) => (
                <p key={environment.environmentId}>
                  {sources.get(environment.environmentId)?.checking
                    ? `Checking prompt sources on ${environment.label}…`
                    : !environment.entry.enabled
                      ? `Cannot check prompt sources on ${environment.label}: server disabled.`
                      : environment.connection.phase !== "connected"
                        ? `Cannot check prompt sources on ${environment.label}: server disconnected.`
                        : `Could not check prompt sources on ${environment.label}.`}
                </p>
              ))}
              <Button
                variant="outline"
                size="compact"
                onClick={() => setSourceRevision((value) => value + 1)}
              >
                Retry sources
              </Button>
            </div>
          ) : null}
          {configured.length === 0 && unresolved.length > 0 && unconfigured.length > 0 ? (
            <p className="text-sm text-muted-foreground">
              No prompt review or queue is configured on{" "}
              {unconfigured.map((environment) => environment.label).join(", ")}.
            </p>
          ) : null}
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
            {selected.length > 0 ? <PromptToolbar stage={pane} controls={listControls} /> : null}
            {linkedSourceMissing ? (
              <p role="status" className="rounded-md border p-4 text-sm">
                This prompt is not in the current sample.
              </p>
            ) : null}
            {selected.length === 0 ? (
              requestedId === null && unresolved.length === 0 ? (
                <Empty size="compact">
                  <EmptyHeader>
                    <EmptyTitle>No prompt sources configured.</EmptyTitle>
                    <EmptyDescription>
                      Connect to the server that hosts your queue or prompt review. Prompts arrive
                      from your voice source and from work queues connected to this server.
                    </EmptyDescription>
                  </EmptyHeader>
                </Empty>
              ) : (
                <p className="text-sm text-muted-foreground">
                  {requestedId !== null
                    ? "The selected prompt source is unavailable. Choose another source."
                    : "Prompts from available sources appear as each check finishes. Reconnect unavailable servers to access their prompts."}
                </p>
              )
            ) : (
              selected.map((environment) => (
                <EnvironmentPrompts
                  key={environment.environmentId}
                  environment={environment}
                  pane={pane}
                  grouped={requestedId === null && configured.length > 1}
                  sources={sources.get(environment.environmentId)!}
                  preserveEditor={dirtyEnvironments.has(environment.environmentId)}
                  onDirtyChange={reportDirty}
                  onCountsChange={reportCounts}
                  listView={listControls.view}
                  linkedKey={
                    linkedPrompt && (!search?.source || search.source === environment.environmentId)
                      ? linkedPrompt
                      : undefined
                  }
                  onLinkSelection={navigation ? linkSelection : undefined}
                />
              ))
            )}
          </div>
        </WorkspacePageContainer>
      </div>
    </SidebarInset>
  );
}

function sampleQualifier(
  parts: readonly ("ok" | "partial" | "unknown")[],
): PromptCount["qualifier"] {
  return parts.includes("unknown") ? "?" : parts.includes("partial") ? "+" : "";
}

function EnvironmentPrompts({
  environment,
  pane,
  grouped,
  sources,
  preserveEditor,
  onDirtyChange,
  onCountsChange,
  listView,
  linkedKey,
  onLinkSelection,
}: {
  environment: EnvironmentPresentation;
  pane: PromptQueueView;
  grouped: boolean;
  sources: PromptSources;
  preserveEditor: boolean;
  onDirtyChange: (environmentId: EnvironmentId, dirty: boolean) => void;
  onCountsChange: (environmentId: EnvironmentId, counts: StageCounts | null) => void;
  listView: PromptListView;
  linkedKey?: string | undefined;
  onLinkSelection?: ((environmentId: EnvironmentId, key: string) => void) | undefined;
}) {
  const { environmentId } = environment;
  const [metadata, setMetadata] = useState<WorkQueueMetadataResult | null>(null);
  const [queueFailed, setQueueFailed] = useState(false);
  const [pendingCount, setPendingCount] = useState<PromptCount | null>(null);
  const [now, setNow] = useState(Date.now);
  const snapshot = metadata && "snapshot" in metadata ? metadata.snapshot : null;
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Own metadata expiry timing so cached queue evidence stops being presented as current.
  useEffect(() => {
    if (!metadata || !("snapshot" in metadata) || metadata.expires_at_ms <= now) return;
    const timer = window.setTimeout(
      () => setNow(Date.now()),
      Math.max(0, Math.min(metadata.expires_at_ms - Date.now(), 2_147_483_647)),
    );
    return () => window.clearTimeout(timer);
  }, [metadata, now]);
  const voiceConfigured = sources.voice === "configured";
  const voice = useVoiceReviewMetadata(environmentId, voiceConfigured);
  const [correctionSession] = useState(() => new VoiceWorkstreamCorrectionSession(voice.review));
  const nativeEnvironmentId =
    environment.serverConfig?.environment.environmentId === environmentId ? environmentId : null;
  const built = buildPromptItems({
    drafts: [],
    recent: voice.recent?.entries ?? [],
    snapshot,
    nativeEnvironmentId,
  });
  const reportDirty = useStableCallback((dirty: boolean) => onDirtyChange(environmentId, dirty));
  const unavailable = !environment.entry.enabled || environment.connection.phase !== "connected";
  const queueConfigured = !unavailable && sources.queue === "configured";
  const stale =
    unavailable ||
    queueFailed ||
    metadata?.status === "stale" ||
    (metadata !== null && "snapshot" in metadata && metadata.expires_at_ms <= now);
  const voicePartial =
    voice.recent !== null &&
    (voice.recent.partial ||
      voice.recent.unavailable.length > 0 ||
      voice.recent.entries.length >= RECENT_LIMIT);
  const queuedParts = [
    ...(sources.voice === "unknown" || sources.voiceChecking ? ["unknown" as const] : []),
    ...(sources.queue === "unknown" || sources.queueChecking ? ["unknown" as const] : []),
    ...(voiceConfigured
      ? [
          voice.recent === null || voice.recentFailed
            ? ("unknown" as const)
            : voicePartial
              ? ("partial" as const)
              : ("ok" as const),
        ]
      : []),
    ...(sources.queue === "configured"
      ? [
          snapshot === null || stale
            ? ("unknown" as const)
            : snapshot.coverage === "partial" || metadata?.status === "partial"
              ? ("partial" as const)
              : ("ok" as const),
        ]
      : []),
  ];
  const queuedKnown = voice.recent !== null || snapshot !== null;
  const queuedTotal = queuedKnown ? built.queued.length : null;
  const queuedQualifier = sampleQualifier(queuedParts);
  const pendingTotal = pendingCount?.count ?? null;
  const pendingQualifier = pendingCount?.qualifier ?? "";
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Publish this environment's stage counts to the page tabs, and withdraw them on unmount.
  useEffect(() => {
    onCountsChange(environmentId, {
      pending: pendingTotal === null ? null : { count: pendingTotal, qualifier: pendingQualifier },
      queued: queuedTotal === null ? null : { count: queuedTotal, qualifier: queuedQualifier },
    });
  }, [environmentId, onCountsChange, pendingTotal, pendingQualifier, queuedTotal, queuedQualifier]);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Withdraw this environment's counts when its section unmounts.
  useEffect(() => () => onCountsChange(environmentId, null), [environmentId, onCountsChange]);
  const onSelect = (key: string) => onLinkSelection?.(environmentId, key);
  const stage = (kind: "queued" | "sent") => (
    <PromptStageList
      environmentId={environmentId}
      stage={kind}
      items={kind === "queued" ? built.queued : built.sent}
      nativeEnvironmentId={nativeEnvironmentId}
      queueEnvironmentRef={snapshot?.source.environment_ref ?? null}
      voice={voiceConfigured ? voice : null}
      correctionSession={correctionSession}
      unavailable={unavailable}
      listView={listView}
      linkedKey={linkedKey}
      onLinkSelection={onLinkSelection ? onSelect : undefined}
      partial={
        kind === "queued"
          ? queuedQualifier !== ""
          : stale || snapshot?.coverage === "partial" || metadata?.status === "partial"
      }
      empty={
        kind === "queued"
          ? queuedParts.length > 0 && queuedParts.every((part) => part === "ok")
            ? "Nothing in the queue."
            : queuedKnown
              ? "No items in this sample. The current queue may contain work."
              : null
          : null
      }
    />
  );
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
      {voiceConfigured || preserveEditor ? (
        <EnvironmentVoiceReview
          environmentId={environmentId}
          pane={pane}
          onDirtyChange={reportDirty}
          unavailable={unavailable}
          onPendingCountChange={setPendingCount}
          listView={pane === "pending" ? listView : undefined}
          linkedKey={pane === "pending" ? linkedKey : undefined}
          onLinkSelection={onLinkSelection ? onSelect : undefined}
        />
      ) : pane === "pending" ? (
        <p className="text-sm text-muted-foreground">
          {linkedKey && !sources.voiceChecking
            ? "This prompt is not in the current sample."
            : sources.voice === "absent"
              ? "No pending prompt review configured on this source."
              : sources.voiceChecking
                ? "Checking prompt review on this source…"
                : "Prompt review could not be checked on this source. Retry sources."}
        </p>
      ) : null}
      <div hidden={pane !== "queued"} className="min-w-0 space-y-3">
        {voiceConfigured && voice.recentFailed ? (
          <p role="alert" className="text-sm">
            Recent voice prompts are unavailable. Previously observed prompts may be stale.
          </p>
        ) : null}
        {voiceConfigured &&
        voice.recent &&
        (voice.recent.partial || voice.recent.unavailable.length > 0) ? (
          <p className="text-xs text-muted-foreground">
            Only part of the recent prompts is available
            {voice.recent.unavailable.length > 0
              ? `. Unavailable: ${voice.recent.unavailable.join(", ")}`
              : ""}
            .
          </p>
        ) : null}
        {snapshot && voiceConfigured && !built.sourceBound ? (
          <p className="text-xs text-muted-foreground">
            Voice text cannot be matched to queue items on this source. Voice prompts stay under
            Queued.
          </p>
        ) : null}
        {!unavailable && sources.queue === "unknown" ? (
          <p className="text-sm text-muted-foreground">
            {sources.queueChecking
              ? "Checking the queue on this source…"
              : "The queue could not be checked on this source. Retry sources."}
          </p>
        ) : null}
        {queueConfigured ? (
          <EnvironmentWorkQueue
            environmentId={environmentId}
            onResult={setMetadata}
            onFailedChange={setQueueFailed}
          >
            {pane === "queued" ? stage("queued") : null}
          </EnvironmentWorkQueue>
        ) : pane === "queued" ? (
          stage("queued")
        ) : null}
      </div>
      {pane === "sent" ? (
        <>
          {snapshot && !built.sourceBound ? (
            <p className="text-sm text-muted-foreground">
              Voice text matching is unavailable: the queue source is not bound to this native
              environment. Voice prompts remain under Queued.
            </p>
          ) : null}
          <SentPrompts
            state={
              snapshot
                ? {
                    status: "ready",
                    count: built.sent.length,
                    omitted: built.sentOmitted,
                    observation: {
                      sampledAt: snapshot.observed_at_ms,
                      coverage: metadata?.status === "partial" ? "partial" : snapshot.coverage,
                      stale,
                    },
                  }
                : { status: "unavailable" }
            }
          >
            {built.sent.length > 0 || (linkedKey && snapshot) ? stage("sent") : null}
          </SentPrompts>
          {linkedKey && !snapshot && (queueFailed || !queueConfigured || metadata) ? (
            <p role="status">This prompt is not in the current sample.</p>
          ) : null}
        </>
      ) : null}
    </section>
  );
}

/** A destination thread this environment can resolve: the queue named this environment, or none. */
function resolvableThread(
  destination: PromptDestination | null,
  nativeEnvironmentId: string | null,
) {
  if (!destination?.threadId) return null;
  return destination.targetEnvironmentRef === null ||
    destination.targetEnvironmentRef === nativeEnvironmentId
    ? destination.threadId
    : null;
}

function destinationFallback(
  destination: PromptDestination | null,
  nativeEnvironmentId: string | null,
) {
  if (!destination?.threadId) return null;
  const thread = displayText(destination.threadId, "unnamed thread");
  return resolvableThread(destination, nativeEnvironmentId) === null
    ? `Thread ${thread} on another environment`
    : `Thread ${thread}`;
}

function PromptStageList({
  environmentId,
  stage,
  items,
  nativeEnvironmentId,
  queueEnvironmentRef,
  voice,
  correctionSession,
  unavailable,
  empty,
  listView,
  linkedKey,
  onLinkSelection,
  partial,
}: {
  environmentId: EnvironmentId;
  stage: "queued" | "sent";
  items: readonly PromptItem[];
  nativeEnvironmentId: string | null;
  queueEnvironmentRef: string | null;
  voice: VoiceReviewMetadata | null;
  correctionSession: VoiceWorkstreamCorrectionSession;
  unavailable: boolean;
  empty: string | null;
  listView: PromptListView;
  linkedKey?: string | undefined;
  onLinkSelection?: ((key: string) => void) | undefined;
  partial: boolean;
}) {
  const localNames = useSyncExternalStore(
    subscribeWorkQueueLocalNames,
    readWorkQueueLocalNames,
    readWorkQueueLocalNames,
  );
  const formatTime = usePromptTime();
  const { state: personal } = usePersonalWorkstreams();
  const personalNames = new Map(
    personal.environments
      .find((entry) => entry.environmentId === environmentId)
      ?.workstreams?.map((workstream) => [workstream.id, workstream]),
  );
  const queueBound = queueEnvironmentRef === environmentId && nativeEnvironmentId === environmentId;
  const workstreamName = (id: string) =>
    queueEnvironmentRef === null
      ? id
      : ((queueBound ? personalNames.get(id)?.name : undefined) ??
        workQueueDestinationLabel(id, queueEnvironmentRef, localNames));
  const threads = voice?.registry?.threads ?? [];
  const title = (item: PromptItem) => {
    const destination = item.destinations[0] ?? null;
    if (item.entry) {
      const purpose = recentThreadPurpose(recentThread(item.entry, threads));
      if (purpose) return displayText(purpose, "Destination not observed yet");
    }
    return (
      (item.draft?.routing_target
        ? displayText(item.draft.routing_target, "Destination not observed yet")
        : null) ??
      destinationFallback(destination, nativeEnvironmentId) ??
      (item.origin === "voice" ? "Destination not observed yet" : "Destination not available")
    );
  };
  const source = (item: PromptItem) => {
    const destination = item.destinations[0] ?? null;
    if (item.origin === "voice")
      return destination?.backend
        ? `Voice · ${displayText(destination.backend, "voice")}`
        : "Voice";
    return requestKindLabel(destination?.queueItem?.request_kind);
  };
  const linesByItem = new Map(
    items.map((item) => [
      item.key,
      promptWorkstreams(item, {
        environmentId,
        queueBound,
        names: personalNames,
        labels: voice?.workstreams ?? null,
        registryThread: item.entry ? recentThread(item.entry, threads) : null,
        threadAssignments: personal.assignmentsByThreadKey,
        autoAssignedThreadKeys: personal.autoAssignedThreadKeys,
      }),
    ]),
  );
  const lines = (item: PromptItem) => linesByItem.get(item.key) ?? [];
  const liveTitles = new Map(
    useQuietThreadShells()
      .filter((shell) => shell.environmentId === environmentId)
      .map((shell): [string, string] => [shell.id, shell.title]),
  );
  const destinationLabel = (item: PromptItem) => {
    if (item.destinations.length > 1) return `${item.destinations.length} destinations`;
    const threadId = resolvableThread(item.destinations[0] ?? null, nativeEnvironmentId);
    return displayText(threadId ? liveTitles.get(threadId) : undefined, title(item));
  };
  const statusOf = (item: PromptItem) => {
    const status = itemStatus(item);
    const turn = item.destinations[0]?.handoff?.providerState;
    return stage === "sent" && item.destinations.length === 1 && turn
      ? { ...status, label: `${status.label} · ${turnStateLabel(turn)}` }
      : status;
  };
  const groups = filteredPromptGroups(
    items,
    stage,
    listView,
    {
      attention: (item) => promptAttention(item),
      destinationLabel,
      workstream: (item) => {
        const candidates = lines(item);
        const first = candidates.find((line) => line.name !== null) ?? candidates[0];
        return first === undefined
          ? null
          : {
              key: first.name === null ? "unrecognized" : first.filterKey,
              label: first.name ?? "Unrecognized workstream",
              unrecognized: first.name === null,
            };
      },
      queueStateLabel: (item) => statusOf(item).label,
    },
    lines,
  );
  const wide = useMediaQuery("md");
  const [localSelection, setSelected] = useState<string | null>(null);
  const selected = linkedKey ?? localSelection;
  const firstKey = groups[0]?.items[0]?.key ?? null;
  if (wide && selected === null && firstKey !== null) setSelected(firstKey);
  const filtered = items.length > 0 && groups.length === 0;
  const choose = (key: string) => {
    setSelected(key);
    onLinkSelection?.(key);
  };
  const selectedItem = items.find((item) => item.key === selected) ?? null;
  const row = (item: PromptItem): PromptRowView => {
    const text = item.text.kind === "available" ? item.text.text : null;
    return {
      key: item.key,
      environmentId,
      threadId:
        item.destinations.length === 1
          ? resolvableThread(item.destinations[0]!, nativeEnvironmentId)
          : null,
      fallbackTitle: title(item),
      destinationCount: item.destinations.length,
      status: statusOf(item),
      preview: text ? displayText(text, textStateLabel(item.text)) : textStateLabel(item.text),
      previewMuted: text === null,
      source: source(item),
      workstreams: lines(item),
      promptHref: onLinkSelection
        ? promptLink({ source: environmentId, stage, prompt: item.key })
        : undefined,
      time: item.time,
      dirty: false,
      gone: false,
    };
  };
  return (
    <div className="min-w-0 space-y-2">
      {partial && items.length > 0 ? (
        <p className="text-xs text-muted-foreground">Searching {items.length} loaded prompts.</p>
      ) : null}
      <PromptMasterDetail
        label={stage === "queued" ? "Queued prompts" : "Sent prompts list"}
        groups={groups.map((group) => ({
          key: group.key,
          label: group.label,
          rows: group.items.map(row),
        }))}
        selectedKey={selected}
        onSelect={choose}
        onFilterWorkstream={listView.filterWorkstream}
        viewKey={listView.key}
        empty={
          filtered ? (
            <PromptFilteredEmpty view={listView} />
          ) : empty === null ? null : (
            <Empty size="compact">
              <EmptyHeader>
                <EmptyTitle>{empty}</EmptyTitle>
              </EmptyHeader>
            </Empty>
          )
        }
        detail={
          selectedItem ? (
            <PromptItemDetail
              key={selectedItem.key}
              environmentId={environmentId}
              item={selectedItem}
              title={title(selectedItem)}
              source={source(selectedItem)}
              status={statusOf(selectedItem)}
              nativeEnvironmentId={nativeEnvironmentId}
              voice={voice}
              correctionSession={correctionSession}
              unavailable={unavailable}
              formatTime={formatTime}
              workstreamName={workstreamName}
              lines={lines(selectedItem)}
              namesUnavailable={Boolean(
                voice?.labelsFailed ||
                personal.environments.some(
                  (environment) =>
                    environment.environmentId === environmentId &&
                    environment.status === "sync_failed",
                ),
              )}
            />
          ) : (
            <section className="rounded-md border p-4 text-sm text-muted-foreground">
              {selected === null
                ? "Select a prompt to see where it is and what was observed."
                : linkedKey
                  ? "This prompt is not in the current sample."
                  : stage === "queued"
                    ? "This prompt left Queued. It may now be under Sent."
                    : "This prompt is no longer in this sample."}
            </section>
          )
        }
      />
    </div>
  );
}

function PromptItemDetail({
  environmentId,
  item,
  title,
  source,
  status,
  nativeEnvironmentId,
  voice,
  correctionSession,
  unavailable,
  formatTime,
  workstreamName,
  lines,
  namesUnavailable,
}: {
  environmentId: EnvironmentId;
  item: PromptItem;
  title: string;
  source: string;
  status: ReturnType<typeof itemStatus>;
  nativeEnvironmentId: string | null;
  voice: VoiceReviewMetadata | null;
  correctionSession: VoiceWorkstreamCorrectionSession;
  unavailable: boolean;
  formatTime: (ms: number) => string;
  workstreamName: (id: string) => string;
  lines: readonly PromptWorkstreamLine[];
  namesUnavailable: boolean;
}) {
  const [routingOpen, setRoutingOpen] = useState(false);
  const primary = item.destinations[0] ?? null;
  const threadId =
    item.destinations.length === 1 ? resolvableThread(primary, nativeEnvironmentId) : null;
  const shell = usePromptThreadShell(environmentId, threadId);
  const heading = item.destinations.length > 1 ? `${item.destinations.length} destinations` : title;
  const draft = item.draft;
  const routing = draft
    ? routingStateLabel(draft.routing_state, draft.routing_target, draft.dependencies)
    : null;
  const thread = item.entry ? recentThread(item.entry, voice?.registry?.threads ?? []) : null;
  const correctionsBlocked =
    unavailable ||
    voice === null ||
    voice.labelsFailed ||
    voice.recentFailed ||
    voice.recent?.partial === true;
  return (
    <PromptDetail
      title={
        item.destinations.length > 1 ? (
          heading
        ) : (
          <PromptThreadTitle environmentId={environmentId} threadId={threadId} fallback={heading} />
        )
      }
      status={status}
      threadStatus={<PromptThreadStatus environmentId={environmentId} threadId={threadId} />}
      openThread={
        shell && threadId ? (
          <Button
            size="xs"
            variant="outline"
            render={<a href={threadHref(environmentId, threadId)} />}
            aria-label={`Open thread for ${title}`}
          >
            Open thread
            <ArrowUpRightIcon aria-hidden />
          </Button>
        ) : null
      }
      meta={
        <span>
          {source}
          {item.time === null
            ? ""
            : ` · ${item.timeKind === "handoff" ? "handed off" : item.timeKind === "submitted" ? "submitted" : "captured"} ${formatTime(item.time)}`}
        </span>
      }
      progress={
        item.destinations.length <= 1 ? (
          <PromptProgress
            label={`Progress for ${title}`}
            steps={promptProgressSteps(item, primary)}
            formatTime={formatTime}
          />
        ) : (
          <div className="space-y-3">
            {item.destinations.map((destination, index) => (
              <div key={destination.key} className="space-y-1">
                <p className="text-xs font-medium">
                  Destination {index + 1} of {item.destinations.length}
                  {destination.threadId ? (
                    <>
                      {" · "}
                      <bdi>
                        {destinationFallback(destination, nativeEnvironmentId) ??
                          "Thread not observed"}
                      </bdi>
                    </>
                  ) : null}
                </p>
                <PromptProgress
                  label={`Progress for destination ${index + 1} of ${item.destinations.length}`}
                  steps={promptProgressSteps(item, destination)}
                  formatTime={formatTime}
                />
              </div>
            ))}
          </div>
        )
      }
      workstreams={
        <PromptWorkstreams
          environmentId={environmentId}
          lines={lines}
          namesUnavailable={namesUnavailable}
          corrections={
            item.entry && item.stage === "queued" && voice ? (
              <VoiceWorkstreamCorrections
                entry={item.entry}
                thread={thread}
                registry={voice.registry}
                workstreams={voice.workstreams}
                transport={voice.review}
                session={correctionSession}
                onRefresh={voice.refresh}
                unavailable={correctionsBlocked || namesUnavailable}
              />
            ) : null
          }
        />
      }
      actionStatus={
        item.stage === "queued" && primary?.queueItem
          ? `Queue state: ${queueStateLabel(primary.queueItem.queue_state).label}`
          : null
      }
      routing={
        draft && routing ? (
          <PromptDisclosure summary="Routing details" onToggle={setRoutingOpen}>
            <p className="text-foreground">{routing.label}</p>
            {routing.detail ? <p>{routing.detail}</p> : null}
            <p>Routing is decided by the voice source. It cannot be changed here.</p>
            {routingOpen && voice ? (
              <RoutingDiagnostics draftId={draft.id} fetchDiagnostics={voice.review.diagnostics} />
            ) : null}
          </PromptDisclosure>
        ) : null
      }
      evidence={
        <PromptEvidence item={item} formatTime={formatTime} workstreamName={workstreamName} />
      }
    >
      {item.text.kind === "available" ? (
        <p className="whitespace-pre-wrap text-sm wrap-anywhere">{item.text.text}</p>
      ) : (
        <p className="text-sm text-muted-foreground">{textStateLabel(item.text)}</p>
      )}
    </PromptDetail>
  );
}

function EnvironmentWorkQueue({
  environmentId,
  onResult,
  onFailedChange,
  children,
}: {
  environmentId: EnvironmentId;
  onResult: (result: WorkQueueMetadataResult | null) => void;
  onFailedChange: (failed: boolean) => void;
  children?: ReactNode;
}) {
  const load = useWorkQueueMetadata(environmentId);
  return (
    <WorkQueueMetadataPanel load={load} onResult={onResult} onFailedChange={onFailedChange}>
      {children}
    </WorkQueueMetadataPanel>
  );
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

type SourcePresence = "configured" | "absent" | "unknown";
type PromptSources = {
  voice: SourcePresence;
  queue: SourcePresence;
  voiceChecking: boolean;
  queueChecking: boolean;
  checking: boolean;
};

function PromptSourceDiscovery({
  environment,
  revision,
  onChange,
}: {
  environment: EnvironmentPresentation;
  revision: number;
  onChange: (environmentId: EnvironmentId, sources: PromptSources) => void;
}) {
  const { environmentId } = environment;
  const { fetchList } = useVoiceReview(environmentId);
  const loadQueue = useWorkQueueMetadata(environmentId);
  const probeVoice = useEffectEvent(() => fetchList("pending"));
  const probeQueue = useEffectEvent((signal: AbortSignal) => loadQueue(signal));
  const connected = environment.entry.enabled && environment.connection.phase === "connected";
  const supportsQueue =
    environment.serverConfig?.environment.capabilities.workQueueMetadata === true;
  const requestKey = `${connected}:${supportsQueue}:${revision}`;
  const [sources, setSources] = useState<{
    voice: SourcePresence;
    queue: SourcePresence;
    voiceObservedFor: string | null;
    queueObservedFor: string | null;
  }>({
    voice: "unknown",
    queue: "unknown",
    voiceObservedFor: null,
    queueObservedFor: null,
  });

  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Publish committed source capability and checking state to the containing unified queue.
  useEffect(
    () =>
      onChange(environmentId, {
        voice: sources.voice,
        queue: sources.queue,
        voiceChecking: connected && sources.voiceObservedFor !== requestKey,
        queueChecking: connected && sources.queueObservedFor !== requestKey,
        checking:
          connected &&
          (sources.voiceObservedFor !== requestKey || sources.queueObservedFor !== requestKey),
      }),
    [connected, environmentId, onChange, requestKey, sources],
  );
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Probe voice and queue sources for the connected request identity and abort superseded reads.
  useEffect(() => {
    if (!connected) return;
    const controller = new AbortController();
    const publish = (kind: "voice" | "queue", presence: SourcePresence) => {
      if (controller.signal.aborted) return;
      setSources((current) => ({
        ...current,
        [kind]: presence === "unknown" && current[kind] === "configured" ? "configured" : presence,
        [kind === "voice" ? "voiceObservedFor" : "queueObservedFor"]: requestKey,
      }));
    };
    void probeVoice().then(
      () => publish("voice", "configured"),
      (error) =>
        publish("voice", error?._tag === "VoiceReviewNotConfiguredError" ? "absent" : "unknown"),
    );
    void (
      supportsQueue
        ? probeQueue(controller.signal)
        : Promise.resolve({ status: "unconfigured" } as const)
    ).then(
      (result) => publish("queue", result.status === "unconfigured" ? "absent" : "configured"),
      () => publish("queue", "unknown"),
    );
    return () => controller.abort();
  }, [connected, requestKey, supportsQueue]);
  if (!connected || sources.voiceObservedFor !== requestKey || sources.voice === "configured")
    return null;
  return (
    <p className="text-sm text-muted-foreground">
      Prompt review is unavailable on {environment.label}.
    </p>
  );
}

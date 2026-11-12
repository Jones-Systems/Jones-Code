import {
  jonesCandidatePresentation,
  jonesStagedAlreadyRunning,
  JONES_STAGED_ALREADY_RUNNING_MESSAGE,
  jonesAlternativePresentation,
  jonesFollowerHold,
  jonesFollowerSummary,
  jonesUpdatePresentation,
  shortJonesSource,
  type JonesSourceComparison,
} from "@t3tools/client-runtime/jones/updates";
import type { EnvironmentId } from "@t3tools/contracts";
import type {
  JonesUpdateDownloadInput,
  JonesUpdateInstallInput,
  JonesUpdateReconcileInput,
  JonesUpdateState,
} from "@t3tools/contracts/jones/jonesUpdates";
import { ArrowRightIcon } from "lucide-react";
import { useId, useState, type ReactNode } from "react";
import { Badge } from "../../components/ui/badge";
import { Button } from "../../components/ui/button";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../../components/ui/tooltip";
import { ITEM_ROW_CLASSNAME, ITEM_ROW_INNER_CLASSNAME } from "../../components/settings/itemRows";
import { jonesBuildLabel, jonesBuildLabels } from "./buildVersion";
import { outcomeBuildDetail, outcomeBuildRefs } from "./buildPairs";
import type { JonesInstalledComparison } from "./installedSourceComparison";
import {
  DETAIL_PANEL_CLASSNAME,
  DetailItem,
  DetailList,
  DisclosureButton,
  INLINE_ACTIONS_CLASSNAME,
} from "./disclosure";
import { BuildVersion, CopyableValue } from "./updateDisplay";
import { cn, randomUUID } from "../../lib/utils";
import { ProductionExactSourceUpdateAction } from "./ProductionExactSourceUpdateAction";

export type JonesBuildRequest =
  | { readonly action: "check" }
  | { readonly action: "download"; readonly input: JonesUpdateDownloadInput }
  | { readonly action: "reconcile"; readonly input: JonesUpdateReconcileInput }
  | { readonly action: "install"; readonly input: JonesUpdateInstallInput };

export type JonesBuildCheck = { readonly kind: string; readonly text: string };

export type JonesBuildOperation = {
  readonly headline: string;
  readonly operationId: string;
  readonly held: boolean;
  readonly unknown: boolean;
  readonly planCorrelated: boolean;
};

const FAILED_CHECKS: ReadonlySet<string> = new Set(["failure", "qualification-failed"]);

type StatusNote = {
  readonly text: string;
  readonly tone: "muted" | "warning" | "error";
  /** Alerts are announced; other notes are polite status. */
  readonly alert?: boolean;
  /** Replaces the plain text on the status line, keeping `text` for Details. */
  readonly node?: ReactNode;
};

const NOTE_TONE = {
  muted: "text-muted-foreground",
  warning: "text-warning",
  error: "text-destructive",
} as const;

const LAYOUTS = {
  // A direct row in a grouped settings section, aligned with its SettingsRow siblings.
  row: {
    root: ITEM_ROW_CLASSNAME,
    header: ITEM_ROW_INNER_CLASSNAME,
    summary: "min-w-0 flex-1 space-y-1 text-xs",
    actions: "flex w-full shrink-0 items-center gap-2 sm:w-auto sm:justify-end",
    button: "sm",
    after: "mt-2 space-y-1.5 text-xs empty:hidden",
  },
  // Under a machine's name in its environment row, using the row's full width.
  inline: {
    root: "mt-1.5",
    header: "flex flex-wrap items-center justify-between gap-x-4 gap-y-1.5",
    summary: "min-w-0 space-y-0.5 text-xs",
    actions: INLINE_ACTIONS_CLASSNAME,
    button: "xs",
    after: "mt-1.5 space-y-1.5 text-xs empty:hidden",
  },
} as const;

export type JonesBuildStatusLayout = keyof typeof LAYOUTS;

const COMPARISON = {
  matches: {
    label: "Matches laptop",
    variant: "success",
    detail: "Built from the source commit installed on this laptop.",
  },
  different: {
    label: "Different source",
    variant: "warning",
    detail: "Built from a different source commit than the one installed on this laptop.",
  },
  unavailable: {
    label: null,
    variant: "outline",
    detail:
      "Not compared. Comparing needs a verified local desktop connection and the full source commit of both builds.",
  },
} as const;

const INSTALLED_COMPARISON = {
  matches: {
    label: "Matches",
    variant: "success",
    detail: "Installed from the same source commit as this laptop.",
  },
  older: {
    label: "Older",
    variant: "warning",
    detail: "Installed from an older source commit than this laptop.",
  },
  newer: {
    label: "Newer",
    variant: "info",
    detail: "Installed from a newer source commit than this laptop.",
  },
  different: {
    label: "Different source",
    variant: "warning",
    detail:
      "Installed from a different source commit than this laptop. Which is newer is not known.",
  },
  diverged: {
    label: "Diverged",
    variant: "warning",
    detail:
      "Installed from a source commit that has diverged from this laptop's; neither contains the other.",
  },
  unavailable: {
    label: null,
    variant: "outline",
    detail:
      "Not compared. Comparing needs the full installed source commit of both this host and this laptop.",
  },
} as const;

/**
 * Presentation of one host's Jones main builds. Installed and offered builds share one line;
 * exact versions, commits and the update record stay one disclosure away.
 */
export function JonesBuildStatus({
  environmentId,
  state,
  freshness,
  freshnessMessage,
  comparison = null,
  installedComparison = null,
  pending = false,
  actionError = null,
  layout = "inline",
  extraAction,
  check,
  operation,
  onRequest,
}: {
  readonly environmentId: EnvironmentId;
  readonly state: JonesUpdateState | null;
  readonly freshness: "loading" | "fresh" | "stale" | "unsupported";
  readonly freshnessMessage?: string | undefined;
  /** Set when this host's build is compared with this laptop's installed source. */
  readonly comparison?: JonesSourceComparison | null;
  /** Set when this host's installed source is compared with this laptop's installed source. */
  readonly installedComparison?: JonesInstalledComparison | null;
  readonly pending?: boolean;
  readonly actionError?: string | null;
  readonly layout?: JonesBuildStatusLayout;
  readonly extraAction?: ReactNode;
  /** The latest package check. Only a failed check reaches the status line; the rest is detail. */
  readonly check?: JonesBuildCheck | undefined;
  /** The host's current update operation receipt. Its ID and plan stay in Details. */
  readonly operation?: JonesBuildOperation | undefined;
  readonly onRequest: (request: JonesBuildRequest) => void;
}) {
  const [detailsOpen, setDetailsOpen] = useState(false);
  const detailsId = useId();
  const styles = LAYOUTS[layout];
  const freshnessReason =
    freshness === "fresh"
      ? undefined
      : (freshnessMessage ??
        {
          loading: "Waiting for this host's update status.",
          stale: "Update status is unavailable. Reconnect before downloading.",
          unsupported: "This host does not support Jones build updates.",
        }[freshness]);
  if (state === null) {
    const reason = freshnessReason ?? "No update status is available for this host.";
    return (
      <div className={styles.root}>
        <div className={styles.header}>
          <div className={styles.summary}>
            {layout === "row" ? (
              <h3 className="text-sm font-medium text-foreground">Jones builds</h3>
            ) : null}
            <p role="status" className="text-muted-foreground">
              {reason}
            </p>
          </div>
          <div className={styles.actions}>
            {extraAction}
            <DownloadAction
              key="download"
              size={styles.button}
              reason={reason}
              onDownload={() => undefined}
            />
          </div>
        </div>
      </div>
    );
  }

  const alreadyRunning = jonesStagedAlreadyRunning(state);
  const presentation = jonesUpdatePresentation(state);
  const candidate = jonesCandidatePresentation(state);
  const alternative = jonesAlternativePresentation(state);
  const busy = pending || presentation.busy || freshness !== "fresh";
  const provenance = state.provenance;
  const outcome = state.outcome;
  const installedRef = {
    version: state.currentVersion ?? "",
    startedAt: state.currentBuildStartedAt,
    sourceSha: state.installedSource?.sha,
  };
  const offeredRef = {
    version: candidate?.version ?? "",
    startedAt: provenance?.buildStartedAt,
    sourceSha: candidate?.sourceSha,
  };
  const alternativeRef = {
    version: alternative?.version ?? "",
    startedAt: alternative?.buildStartedAt,
    sourceSha: alternative?.sourceSha,
  };
  const [installedLabel, offeredLabel, alternativeLabel] = jonesBuildLabels(
    [installedRef, offeredRef, alternativeRef],
    "day",
  );
  const outcomeLabels = outcome === undefined ? [] : jonesBuildLabels(outcomeBuildRefs(state));
  const outcomeText =
    outcome === undefined
      ? undefined
      : `${outcomeLabels[0]!.text} → ${outcomeLabels[1]!.text}${outcome.reason ? ` · ${outcome.reason}` : ""}`;
  const hasBuilds = state.currentVersion !== undefined || candidate !== undefined;
  const badge = comparison === null ? null : COMPARISON[comparison];
  const installedBadge =
    installedComparison === null ? null : INSTALLED_COMPARISON[installedComparison];
  const candidateLabel = candidate?.label ?? "Offered";
  // A host that follows its laptop takes only the laptop's build; the host refuses others too.
  const followerHold = jonesFollowerHold(state);
  const followerSummary = jonesFollowerSummary(state);
  const downloadReason =
    freshnessReason ??
    (pending
      ? "An update request is in progress. Wait for it to finish."
      : presentation.busy
        ? presentation.message
        : (followerHold ??
          (!state.capability.download
            ? DOWNLOAD_CAPABILITY_REASONS[state.capability.reason ?? "blocked"]
            : state.phase !== "available"
              ? (state.message ??
                (state.phase === "no-new"
                  ? "No newer qualified build is available to download."
                  : state.phase === "building"
                    ? "The new build is still being prepared."
                    : "Check for an available build before downloading."))
              : provenance === undefined
                ? "No qualified build is available to download."
                : undefined)));

  const builds = hasBuilds ? (
    <div className="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-0.5">
      {state.currentVersion !== undefined ? (
        <span className="inline-flex items-baseline gap-1.5">
          <span className="text-muted-foreground">Installed</span>
          <BuildVersion
            label="installed version"
            version={state.currentVersion}
            sourceSha={state.installedSource?.sha}
            buildLabel={installedLabel}
          />
        </span>
      ) : null}
      {state.currentVersion !== undefined && installedBadge?.label ? (
        <SourceBadge
          subject="Installed"
          label={installedBadge.label}
          variant={installedBadge.variant}
          detail={installedBadge.detail}
        />
      ) : null}
      {state.currentVersion !== undefined && candidate !== undefined ? (
        <ArrowRightIcon aria-hidden className="size-3 shrink-0 text-muted-foreground/70" />
      ) : null}
      {candidate !== undefined ? (
        <span className="inline-flex items-baseline gap-1.5">
          <span className="text-muted-foreground">{candidate.label}</span>
          {candidate.version !== undefined ? (
            <BuildVersion
              label={`${candidate.label.toLowerCase()} version`}
              version={candidate.version}
              sourceSha={candidate.sourceSha}
              buildLabel={offeredLabel}
            />
          ) : (
            <CopyableValue
              label="source commit"
              value={candidate.sourceSha}
              display={shortJonesSource(candidate.sourceSha)}
            />
          )}
        </span>
      ) : null}
      {badge?.label ? (
        <SourceBadge
          subject={candidateLabel}
          label={badge.label}
          variant={badge.variant}
          detail={`${candidateLabel}: ${badge.detail}`}
        />
      ) : null}
      {alternative !== undefined ? (
        <span className="inline-flex items-baseline gap-1.5">
          <span className="text-muted-foreground">Alternative</span>
          <BuildVersion
            label="alternative version"
            version={alternative.version ?? "Version not reported"}
            sourceSha={alternative.sourceSha}
            buildLabel={alternativeLabel}
          />
        </span>
      ) : null}
    </div>
  ) : null;

  // Every message this host could show, most actionable first. The status line shows only the
  // first; the rest stay in Details so the row never repeats itself.
  const outcomeFailed = outcome !== undefined && outcome.status !== "committed";
  const candidates: ReadonlyArray<StatusNote | false | undefined | null> = [
    actionError ? { text: actionError, tone: "error", alert: true } : null,
    presentation.refusalMessage !== undefined && {
      text: presentation.refusalMessage,
      tone: "error",
      alert: true,
    },
    operation !== undefined &&
      (operation.held || operation.unknown) && { text: operation.headline, tone: "warning" },
    presentation.busy && { text: presentation.message, tone: "muted" },
    presentation.settlementMessage !== undefined && {
      text: presentation.settlementMessage,
      tone: presentation.settled ? "muted" : "error",
    },
    outcomeFailed &&
      presentation.settlementMessage === undefined && {
        text: `${presentation.settled ? "No changes" : outcome.status === "rolled-back" ? "Rolled back" : "Blocked"}: ${outcomeText}`,
        tone: presentation.settled ? "muted" : "error",
        node: (
          <Tooltip>
            <TooltipTrigger
              render={
                <span
                  className="min-w-0"
                  tabIndex={0}
                  aria-label={`${outcome.status}: ${outcomeBuildDetail(state)}${outcome.reason ? `; ${outcome.reason}` : ""}`}
                />
              }
            >
              {presentation.settled
                ? "No changes"
                : outcome.status === "rolled-back"
                  ? "Rolled back"
                  : "Blocked"}
              : {outcomeText}
            </TooltipTrigger>
            <TooltipPopup side="top">{outcomeBuildDetail(state)}</TooltipPopup>
          </Tooltip>
        ),
      },
    freshnessReason !== undefined && { text: freshnessReason, tone: "muted" },
    check !== undefined && FAILED_CHECKS.has(check.kind) && { text: check.text, tone: "error" },
    // An operation's own phase (stopped, restarting); its idle headline repeats the host message.
    operation !== undefined &&
      operation.headline !== presentation.message && { text: operation.headline, tone: "muted" },
    freshness === "fresh" &&
      state.environmentId === environmentId &&
      alreadyRunning && { text: JONES_STAGED_ALREADY_RUNNING_MESSAGE, tone: "muted" },
    followerHold !== undefined &&
      candidate !== undefined && {
        text:
          state.policy?.leaderSource === null
            ? "Waiting for the laptop's installed build"
            : `${candidate.label} build is not the laptop's, so it will not install`,
        tone: "muted",
      },
    (state.launcherRefresh === "recommended" || state.launcherSettlement === "unsupported") && {
      text: "Launcher refresh recommended",
      tone: "muted",
    },
    state.capability.reason === "bootstrap-required" && {
      text: "Installing needs a qualified launcher on this host first.",
      tone: "muted",
    },
    { text: presentation.message, tone: "muted" },
    check !== undefined && { text: check.text, tone: "muted" },
  ];
  const notes: StatusNote[] = [];
  for (const note of candidates) {
    if (note && note.text !== "" && !notes.some((seen) => seen.text === note.text))
      notes.push(note);
  }
  const [primary, ...otherNotes] = notes;

  const statusLine = (
    <div className="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-0.5 text-muted-foreground">
      {primary !== undefined ? (
        <span
          role={primary.alert ? "alert" : "status"}
          className={cn("min-w-0", NOTE_TONE[primary.tone])}
        >
          {primary.node ?? primary.text}
        </span>
      ) : null}
      {primary !== undefined ? <span aria-hidden="true">·</span> : null}
      <DisclosureButton
        open={detailsOpen}
        controls={detailsId}
        onToggle={() => setDetailsOpen((open) => !open)}
      >
        Details
      </DisclosureButton>
    </div>
  );

  return (
    <div className={styles.root}>
      <div className={styles.header}>
        <div className={styles.summary}>
          {layout === "row" ? (
            <h3 className="text-sm font-medium text-foreground">Jones builds</h3>
          ) : null}
          {builds}
          {statusLine}
        </div>
        <div className={styles.actions}>
          {extraAction}
          {alternative !== undefined && state.alternativeCandidate !== undefined ? (
            <ProductionExactSourceUpdateAction
              environmentId={environmentId}
              label="this host"
              targetSource={alternative.sourceSha}
              alternative={state.alternativeCandidate}
              allowInstall={false}
              buttonLabel="Replace download"
              compact
            />
          ) : null}
          {presentation.canReconcile ? (
            <Button
              size={styles.button}
              variant="outline"
              disabled={busy}
              onClick={() => {
                if (state.updateId === undefined) return;
                onRequest({
                  action: "reconcile",
                  input: { operationId: state.updateId, requestId: randomUUID() },
                });
              }}
            >
              Reconcile
            </Button>
          ) : null}
          <Button
            size={styles.button}
            variant="outline"
            aria-label="Check for new builds"
            disabled={busy || !state.capability.check}
            onClick={() => onRequest({ action: "check" })}
          >
            Check
          </Button>
          {state.stagedHandle !== undefined ? (
            <Button
              size={styles.button}
              disabled={
                busy ||
                alreadyRunning ||
                !state.capability.install ||
                state.currentVersion === undefined ||
                followerHold !== undefined
              }
              onClick={() => {
                if (
                  alreadyRunning ||
                  state.stagedHandle === undefined ||
                  state.currentVersion === undefined
                )
                  return;
                onRequest({
                  action: "install",
                  input: {
                    stagedHandle: state.stagedHandle,
                    environmentId,
                    currentVersion: state.currentVersion,
                  },
                });
              }}
            >
              Install
            </Button>
          ) : (
            <DownloadAction
              key="download"
              size={styles.button}
              reason={downloadReason}
              onDownload={() => {
                if (downloadReason !== undefined || provenance === undefined) return;
                onRequest({
                  action: "download",
                  input: { artifactId: provenance.artifactId, sourceSha: provenance.sourceSha },
                });
              }}
            />
          )}
        </div>
      </div>
      <div className={styles.after}>
        {detailsOpen ? (
          <div id={detailsId} className={DETAIL_PANEL_CLASSNAME}>
            <DetailList>
              {otherNotes.length > 0 ? (
                <DetailItem label="Status">
                  <span className="flex min-w-0 flex-col gap-0.5">
                    {otherNotes.map((note) => (
                      <span key={note.text} className={NOTE_TONE[note.tone]}>
                        {note.text}
                      </span>
                    ))}
                  </span>
                </DetailItem>
              ) : null}
              {followerSummary !== undefined ? (
                <DetailItem label="Policy">{followerSummary}</DetailItem>
              ) : null}
              {operation !== undefined ? (
                <DetailItem label="Operation">
                  <CopyableValue label="operation ID" value={operation.operationId} />
                  {operation.planCorrelated ? <span>Part of this laptop's update plan</span> : null}
                </DetailItem>
              ) : null}
              {state.launcherVersion === undefined ? null : (
                <DetailItem label="Launcher version">{state.launcherVersion}</DetailItem>
              )}
              {state.childVersion === undefined ? null : (
                <DetailItem label="Server version">{state.childVersion}</DetailItem>
              )}
              {state.currentVersion !== undefined ? (
                <DetailItem label="Installed">
                  <CopyableValue label="installed version" value={state.currentVersion} />
                </DetailItem>
              ) : null}
              <DetailItem label="Installed built">
                {jonesBuildLabel(installedRef).detail}
              </DetailItem>
              <DetailItem label="Installed source">
                {state.installedSource === undefined ? (
                  "not reported"
                ) : (
                  <CopyableValue
                    label="installed source commit"
                    value={state.installedSource.sha}
                  />
                )}
              </DetailItem>
              {candidate !== undefined ? (
                <>
                  <DetailItem label={candidate.label}>
                    {candidate.version === undefined ? (
                      "version not reported"
                    ) : (
                      <CopyableValue
                        label={`${candidate.label.toLowerCase()} version`}
                        value={candidate.version}
                      />
                    )}
                  </DetailItem>
                  <DetailItem label={`${candidate.label} built`}>
                    {jonesBuildLabel(offeredRef).detail}
                  </DetailItem>
                  <DetailItem label={`${candidate.label} source`}>
                    <CopyableValue label="source commit" value={candidate.sourceSha} />
                  </DetailItem>
                </>
              ) : null}
              {installedBadge !== null ? (
                <DetailItem label="Installed vs this laptop">{installedBadge.detail}</DetailItem>
              ) : null}
              {badge !== null ? (
                <DetailItem label={`${candidateLabel} vs this laptop`}>{badge.detail}</DetailItem>
              ) : null}
              {state.updateId !== undefined ? (
                <DetailItem label="Update">
                  <CopyableValue label="update ID" value={state.updateId} />
                </DetailItem>
              ) : null}
              {state.migrationPlan !== undefined ? (
                <DetailItem
                  label={alternative === undefined ? "Migrations" : "Downloaded migrations"}
                >
                  {state.migrationPlan.pendingUpstream.length} upstream,{" "}
                  {state.migrationPlan.pendingJones.length} Jones pending
                </DetailItem>
              ) : null}
              {alternative !== undefined ? (
                <>
                  <DetailItem label="Alternative source">
                    <CopyableValue
                      label="alternative source commit"
                      value={alternative.sourceSha}
                    />
                  </DetailItem>
                  <DetailItem label="Alternative built">
                    {jonesBuildLabel(alternativeRef).detail}
                  </DetailItem>
                  <DetailItem label="Alternative migrations">Checked after download</DetailItem>
                </>
              ) : null}
              {state.recovery !== undefined ? (
                <DetailItem label="Recovery">
                  {state.recovery.method} · {(state.recovery.bytes / 1024 ** 3).toFixed(2)} GiB
                </DetailItem>
              ) : null}
              {outcome !== undefined ? (
                <>
                  <DetailItem label="Last update">{presentation.outcomeMessage}</DetailItem>
                  <DetailItem label="From version">
                    <CopyableValue label="outcome from version" value={outcome.fromVersion} />
                  </DetailItem>
                  <DetailItem label="Target version">
                    <CopyableValue label="outcome target version" value={outcome.targetVersion} />
                  </DetailItem>
                </>
              ) : null}
            </DetailList>
          </div>
        ) : null}
      </div>
    </div>
  );
}

/** A source-comparison badge whose spoken name says which build is compared with this laptop. */
function SourceBadge({
  subject,
  label,
  variant,
  detail,
}: {
  readonly subject: string;
  readonly label: string;
  readonly variant: "success" | "warning" | "info" | "outline";
  readonly detail: string;
}) {
  return (
    <Tooltip>
      <TooltipTrigger render={<Badge variant={variant} size="sm" />}>
        <span className="sr-only">{subject} compared with this laptop: </span>
        {label}
      </TooltipTrigger>
      <TooltipPopup side="top">{detail}</TooltipPopup>
    </Tooltip>
  );
}

const DOWNLOAD_CAPABILITY_REASONS = {
  "bootstrap-required": "This host needs a qualified launcher before downloading builds.",
  "source-unqualified": "This host's source is not qualified for build downloads.",
  "unsupported-platform": "Build downloads are not supported on this host's platform.",
  "native-consent-required": "This host requires native consent before downloading builds.",
  blocked: "Build downloads are not available on this host.",
} as const;

function DownloadAction({
  size,
  reason,
  onDownload,
}: {
  readonly size: "xs" | "sm";
  readonly reason: string | undefined;
  readonly onDownload: () => void;
}) {
  const reasonId = useId();
  const button = (
    <Button
      size={size}
      aria-disabled={reason === undefined ? undefined : true}
      aria-describedby={reason === undefined ? undefined : reasonId}
      onClick={() => {
        if (reason !== undefined) return;
        onDownload();
      }}
    >
      Download
    </Button>
  );
  return (
    <Tooltip disabled={reason === undefined}>
      <TooltipTrigger render={button} />
      <TooltipPopup side="top" role="tooltip" id={reasonId}>
        {reason}
      </TooltipPopup>
    </Tooltip>
  );
}

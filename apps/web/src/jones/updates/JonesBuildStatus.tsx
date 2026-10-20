import {
  jonesCandidatePresentation,
  jonesUpdatePresentation,
  shortJonesSource,
  type JonesSourceComparison,
} from "@t3tools/client-runtime/jones/updates";
import type { EnvironmentId } from "@t3tools/contracts";
import type {
  JonesUpdateDownloadInput,
  JonesUpdateInstallInput,
  JonesUpdateState,
} from "@t3tools/contracts/jones/jonesUpdates";
import { ArrowRightIcon } from "lucide-react";
import { useId, useState } from "react";
import { Badge } from "../../components/ui/badge";
import { Button } from "../../components/ui/button";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../../components/ui/tooltip";
import { ITEM_ROW_CLASSNAME, ITEM_ROW_INNER_CLASSNAME } from "../../components/settings/itemRows";
import { compactJonesVersionText } from "./buildVersion";
import type { JonesInstalledComparison } from "./installedSourceComparison";
import {
  DETAIL_PANEL_CLASSNAME,
  DetailItem,
  DetailList,
  DisclosureButton,
  INLINE_ACTIONS_CLASSNAME,
} from "./disclosure";
import { BuildVersion, CopyableValue } from "./updateDisplay";

export type JonesBuildRequest =
  | { readonly action: "check" }
  | { readonly action: "download"; readonly input: JonesUpdateDownloadInput }
  | { readonly action: "install"; readonly input: JonesUpdateInstallInput };

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
  readonly onRequest: (request: JonesBuildRequest) => void;
}) {
  const [detailsOpen, setDetailsOpen] = useState(false);
  const detailsId = useId();
  if (state === null)
    return freshness === "stale" ? (
      <p role="status" className="mt-1.5 text-xs text-muted-foreground">
        {freshnessMessage}
      </p>
    ) : null;

  const styles = LAYOUTS[layout];
  const presentation = jonesUpdatePresentation(state);
  const candidate = jonesCandidatePresentation(state);
  const busy = pending || presentation.busy || freshness !== "fresh";
  const provenance = state.provenance;
  const outcome = state.outcome;
  const outcomeText =
    outcome === undefined
      ? undefined
      : `${compactJonesVersionText(outcome.fromVersion)} → ${compactJonesVersionText(outcome.targetVersion)}${outcome.reason ? ` · ${outcome.reason}` : ""}`;
  const hasBuilds = state.currentVersion !== undefined || candidate !== undefined;
  const badge = comparison === null ? null : COMPARISON[comparison];
  const installedBadge =
    installedComparison === null ? null : INSTALLED_COMPARISON[installedComparison];
  const candidateLabel = candidate?.label ?? "Offered";

  const builds = hasBuilds ? (
    <div className="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-0.5">
      {state.currentVersion !== undefined ? (
        <span className="inline-flex items-baseline gap-1.5">
          <span className="text-muted-foreground">Installed</span>
          <BuildVersion
            label="installed version"
            version={state.currentVersion}
            sourceSha={state.installedSource?.sha}
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
    </div>
  ) : null;

  const statusLine = (
    <div className="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-0.5 text-muted-foreground">
      <span className="min-w-0">{presentation.message}</span>
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
              disabled={busy || !state.capability.install || state.currentVersion === undefined}
              onClick={() => {
                if (state.stagedHandle === undefined || state.currentVersion === undefined) return;
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
          ) : provenance !== undefined ? (
            <Button
              size={styles.button}
              disabled={busy || !state.capability.download || state.phase !== "available"}
              onClick={() =>
                onRequest({
                  action: "download",
                  input: { artifactId: provenance.artifactId, sourceSha: provenance.sourceSha },
                })
              }
            >
              Download
            </Button>
          ) : null}
        </div>
      </div>
      <div className={styles.after}>
        {freshness !== "fresh" ? (
          <p role="status" className="text-muted-foreground">
            {freshnessMessage}
          </p>
        ) : null}
        {actionError ? (
          <p role="alert" className="text-destructive">
            {actionError}
          </p>
        ) : null}
        {outcome !== undefined && outcome.status !== "committed" ? (
          <p role="status" className="text-destructive">
            {outcome.status === "rolled-back" ? "Rolled back" : "Blocked"}: {outcomeText}
          </p>
        ) : null}
        {state.capability.reason === "bootstrap-required" ? (
          <p className="text-muted-foreground">
            Installing needs a qualified launcher on this host first.
          </p>
        ) : null}
        {detailsOpen ? (
          <div id={detailsId} className={DETAIL_PANEL_CLASSNAME}>
            <DetailList>
              {state.currentVersion !== undefined ? (
                <DetailItem label="Installed">
                  <CopyableValue label="installed version" value={state.currentVersion} />
                </DetailItem>
              ) : null}
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
                <DetailItem label="Migrations">
                  {state.migrationPlan.pendingUpstream.length} upstream,{" "}
                  {state.migrationPlan.pendingJones.length} Jones pending
                </DetailItem>
              ) : null}
              {state.recovery !== undefined ? (
                <DetailItem label="Recovery">
                  {state.recovery.method} · {(state.recovery.bytes / 1024 ** 3).toFixed(2)} GiB
                </DetailItem>
              ) : null}
              {outcome !== undefined ? (
                <DetailItem label="Last update">{presentation.outcomeMessage}</DetailItem>
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

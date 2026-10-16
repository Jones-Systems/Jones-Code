import { useId, useState } from "react";
import { Button } from "../../components/ui/button";
import { RefreshIcon } from "../../components/ui/refresh-icon";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../../components/ui/tooltip";
import { DETAIL_PANEL_CLASSNAME, DisclosureButton } from "../updates/disclosure";

/** What this laptop knows about the host's ability to join automatic updates. */
export type FleetHostReadiness = "loading" | "unavailable" | "setup-required" | "qualified";

const READINESS_HINT: Record<FleetHostReadiness, string | null> = {
  loading: "checking host…",
  unavailable: "host status unavailable",
  "setup-required": "host setup required",
  qualified: null,
};

/**
 * One line of automatic-update state and its primary action for a host. Policy and
 * setup explanations stay behind the About disclosure.
 */
export function FleetEnrollmentStatus({
  label,
  enrolled,
  readiness,
  campaign,
  notice,
  continueThreads,
  busy,
  onContinueThreadsChange,
  onToggle,
  onRefresh,
}: {
  readonly label: string;
  readonly enrolled: boolean;
  readonly readiness: FleetHostReadiness;
  /** Progress of the latest laptop update this host takes part in. */
  readonly campaign?:
    | { readonly laptopPhase: string; readonly hostPhase: string; readonly reason?: string }
    | undefined;
  readonly notice?: string | null | undefined;
  readonly continueThreads: boolean;
  readonly busy: boolean;
  readonly onContinueThreadsChange: (value: boolean) => void;
  readonly onToggle: () => void;
  readonly onRefresh: () => void;
}) {
  const [aboutOpen, setAboutOpen] = useState(false);
  const aboutId = useId();
  const hint = enrolled && readiness !== "unavailable" ? null : READINESS_HINT[readiness];
  return (
    <div className="mt-1.5 text-xs">
      <div className="flex flex-wrap items-center justify-between gap-x-4 gap-y-1.5">
        <div className="flex min-w-0 flex-wrap items-center gap-x-1.5 gap-y-0.5">
          <span className="text-muted-foreground">Automatic updates</span>
          <span className="font-medium text-foreground">{enrolled ? "On" : "Off"}</span>
          {hint !== null ? <span className="text-muted-foreground">· {hint}</span> : null}
          <DisclosureButton
            open={aboutOpen}
            controls={aboutId}
            onToggle={() => setAboutOpen((open) => !open)}
          >
            About
          </DisclosureButton>
        </div>
        <div className="flex shrink-0 items-center gap-1.5">
          {!enrolled && readiness === "qualified" ? (
            <label className="flex items-center gap-1.5 text-muted-foreground">
              <input
                type="checkbox"
                checked={continueThreads}
                disabled={busy}
                onChange={(event) => onContinueThreadsChange(event.target.checked)}
              />
              Continue eligible threads after update
            </label>
          ) : null}
          <Button
            size="xs"
            variant="outline"
            aria-label={`${enrolled ? "Turn off" : "Turn on"} automatic updates for ${label}`}
            disabled={busy || (!enrolled && readiness !== "qualified")}
            onClick={onToggle}
          >
            {enrolled ? "Turn off" : "Turn on"}
          </Button>
          <Tooltip>
            <TooltipTrigger
              render={
                <Button
                  size="icon-xs"
                  variant="ghost-muted"
                  aria-label={`Refresh automatic update status for ${label}`}
                  disabled={busy}
                  onClick={onRefresh}
                />
              }
            >
              <RefreshIcon refreshing={busy} />
            </TooltipTrigger>
            <TooltipPopup side="top">Refresh status</TooltipPopup>
          </Tooltip>
        </div>
      </div>
      <div className="mt-1 space-y-1 empty:hidden">
        {campaign !== undefined ? (
          <p role="status" className="text-muted-foreground">
            Laptop update: {campaign.laptopPhase} · Host: {campaign.hostPhase}
            {campaign.reason ? ` · ${campaign.reason}` : ""}
          </p>
        ) : null}
        {notice ? <p role="status">{notice}</p> : null}
        {aboutOpen ? (
          <div id={aboutId} className={DETAIL_PANEL_CLASSNAME}>
            <p>
              Turning this on enrolls only {label}’s qualified service. It installs a build after
              this laptop commits the same source; an offline host updates when it reconnects.
            </p>
            <p>
              Turning it off stops dispatch from this laptop first. Host transactions already
              accepted still finish.
            </p>
            {readiness === "setup-required" && !enrolled ? (
              <p>Before enrolling, verify and adopt a source-qualified service on this host.</p>
            ) : null}
            {readiness === "unavailable" ? (
              <p>
                This host didn’t answer the automatic-update status request. It may be offline, or
                an older host may need explicit launcher setup. Refresh after it reconnects.
              </p>
            ) : null}
          </div>
        ) : null}
      </div>
    </div>
  );
}

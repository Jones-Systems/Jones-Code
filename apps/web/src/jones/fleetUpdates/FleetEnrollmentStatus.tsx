import { useId, useState } from "react";
import { Button } from "../../components/ui/button";
import { Checkbox } from "../../components/ui/checkbox";
import { Popover, PopoverPopup, PopoverTrigger } from "../../components/ui/popover";
import { RefreshIcon } from "../../components/ui/refresh-icon";
import { Switch } from "../../components/ui/switch";
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
 * A host's automatic-update state as one compact action beside Check and Download. The switch,
 * progress and policy explanations open from it, so the row itself stays one status line.
 */
export function FleetEnrollmentStatus({
  label,
  enrolled,
  readiness,
  campaign,
  follower,
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
  /** The host's own progress toward this laptop's build, when it updates itself. */
  readonly follower?: string | undefined;
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
  const progress =
    follower !== undefined
      ? `Host: ${follower}`
      : campaign !== undefined
        ? `Laptop update: ${campaign.laptopPhase} · Host: ${campaign.hostPhase}${campaign.reason ? ` · ${campaign.reason}` : ""}`
        : undefined;
  const state = enrolled ? "On" : "Off";
  return (
    <Popover>
      <PopoverTrigger
        render={
          <Button
            size="xs"
            variant="ghost-muted"
            aria-label={`Automatic updates for ${label}: ${state}${hint === null ? "" : `, ${hint}`}`}
          />
        }
      >
        Auto-update <span className="font-medium text-foreground">{state}</span>
      </PopoverTrigger>
      <PopoverPopup side="bottom" align="end" width="md" padding="compact">
        <div className="space-y-2 text-xs">
          <div className="flex items-center justify-between gap-3">
            <div className="min-w-0">
              <p className="text-sm font-medium text-foreground">Automatic updates</p>
              <p className="text-muted-foreground">
                {hint === null ? "Install this laptop's build after each laptop update." : hint}
              </p>
            </div>
            <div className="flex shrink-0 items-center gap-1">
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
              <Switch
                size="sm"
                checked={enrolled}
                aria-label={`${enrolled ? "Turn off" : "Turn on"} automatic updates for ${label}`}
                disabled={busy || (!enrolled && readiness !== "qualified")}
                onCheckedChange={onToggle}
              />
            </div>
          </div>
          {!enrolled && readiness === "qualified" ? (
            <label className="flex items-center gap-2 text-muted-foreground">
              <Checkbox
                checked={continueThreads}
                disabled={busy}
                onCheckedChange={(checked) => onContinueThreadsChange(checked === true)}
              />
              Continue eligible threads after update
            </label>
          ) : null}
          {progress !== undefined ? (
            <p role="status" className="text-muted-foreground">
              {progress}
            </p>
          ) : null}
          {notice ? <p role="status">{notice}</p> : null}
          <DisclosureButton
            open={aboutOpen}
            controls={aboutId}
            onToggle={() => setAboutOpen((open) => !open)}
          >
            About
          </DisclosureButton>
          {aboutOpen ? (
            <div id={aboutId} className={DETAIL_PANEL_CLASSNAME}>
              <p>
                Turning this on enrolls only {label}’s qualified service. It installs a build after
                this laptop commits the same source; an offline host updates when it reconnects.
              </p>
              <p>
                A host that updates itself prepares this laptop’s installed build as soon as it
                learns of it, and installs it only after no client has had input for 30 minutes and
                nothing a restart would cut is running. Automatic installs are at least 15 minutes
                apart.
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
      </PopoverPopup>
    </Popover>
  );
}

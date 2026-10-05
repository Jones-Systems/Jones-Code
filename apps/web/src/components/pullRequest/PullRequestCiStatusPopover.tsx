import type { EnvironmentId, PullRequestCiStatusResult } from "@t3tools/contracts";
import { ListChecksIcon, RefreshCwIcon, ServerIcon } from "lucide-react";
import { useEffect, useState } from "react";

import { useLiveRefresh } from "~/hooks/useLiveRefresh";
import { pullRequestEnvironment } from "~/state/pullRequests";
import { useEnvironmentQuery } from "~/state/query";
import { Button } from "../ui/button";
import { Popover, PopoverPopup, PopoverTitle, PopoverTrigger } from "../ui/popover";
import { Select, SelectItem, SelectPopup, SelectTrigger, SelectValue } from "../ui/select";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../ui/tooltip";

type CiRunner = PullRequestCiStatusResult["runners"]["items"][number];
type CiJob = PullRequestCiStatusResult["jobs"]["items"][number];

export function runnerRuntime(runner: CiRunner, jobs: readonly CiJob[], now: number): string {
  if (runner.status === "offline") return "Offline";
  if (!runner.busy) return runner.status === "online" ? "Idle" : "Status unknown";
  const job = jobs.find((item) => item.runnerId === runner.id && item.status === "in_progress");
  const startedAt = job?.startedAt ? Date.parse(job.startedAt) : NaN;
  if (!Number.isFinite(startedAt) || startedAt > now) return "Busy · duration unavailable";
  const seconds = Math.floor((now - startedAt) / 1_000);
  const minutes = Math.floor(seconds / 60);
  const elapsed =
    minutes >= 60
      ? `${Math.floor(minutes / 60)}h ${minutes % 60}m`
      : minutes > 0
        ? `${minutes}m ${seconds % 60}s`
        : `${seconds}s`;
  return `Busy · ${elapsed}`;
}

export function PullRequestCiStatusPopover({
  environments,
  scopedEnvironmentId,
}: {
  environments: readonly { environmentId: EnvironmentId; label: string }[];
  scopedEnvironmentId: EnvironmentId | null;
}) {
  const [open, setOpen] = useState(false);
  const [chosenEnvironmentId, setChosenEnvironmentId] = useState<EnvironmentId | null>(null);
  const environmentId =
    scopedEnvironmentId ??
    environments.find((environment) => environment.environmentId === chosenEnvironmentId)
      ?.environmentId ??
    environments[0]?.environmentId ??
    null;
  return (
    <Popover open={open} onOpenChange={setOpen}>
      <Tooltip>
        <TooltipTrigger
          render={
            <PopoverTrigger
              render={<Button variant="ghost" size="icon" aria-label="Open CI testing queue" />}
            >
              <ListChecksIcon aria-hidden className="size-4" />
            </PopoverTrigger>
          }
        />
        <TooltipPopup>CI testing queue</TooltipPopup>
      </Tooltip>
      <PopoverPopup align="end" width="lg" padding="compact">
        <PopoverTitle className="px-1 pt-2">CI jobs — connected repositories</PopoverTitle>
        <div className="space-y-3 px-1 py-3">
          {scopedEnvironmentId === null && environments.length > 1 ? (
            <Select
              value={environmentId}
              onValueChange={(value) => setChosenEnvironmentId(value as EnvironmentId)}
            >
              <SelectTrigger size="sm" className="w-full" aria-label="CI environment">
                <SelectValue />
              </SelectTrigger>
              <SelectPopup>
                {environments.map((environment) => (
                  <SelectItem key={environment.environmentId} value={environment.environmentId}>
                    {environment.label}
                  </SelectItem>
                ))}
              </SelectPopup>
            </Select>
          ) : (
            <p className="text-xs text-muted-foreground">
              {environments.find((environment) => environment.environmentId === environmentId)
                ?.label ?? "No connected environment"}
            </p>
          )}
          {open && environmentId !== null ? (
            <CiStatus key={environmentId} environmentId={environmentId} />
          ) : (
            <p className="text-xs text-muted-foreground">
              Connect an environment to read CI status.
            </p>
          )}
        </div>
      </PopoverPopup>
    </Popover>
  );
}

function SectionNotice({ state, reasons }: { state: string; reasons: readonly string[] }) {
  if (state === "available" && reasons.length === 0) return null;
  return (
    <p className="text-xs text-warning" role="status">
      {state === "partial" ? "Partial results" : "Unavailable"}
      {reasons.length ? ` · ${reasons.join(" · ")}` : ""}
    </p>
  );
}

function CiStatus({ environmentId }: { environmentId: EnvironmentId }) {
  const [visible, setVisible] = useState(() => document.visibilityState === "visible");
  const [now, setNow] = useState(Date.now);
  useEffect(() => {
    const update = () => {
      setVisible(document.visibilityState === "visible");
      setNow(Date.now());
    };
    document.addEventListener("visibilitychange", update);
    const timer = visible ? setInterval(() => setNow(Date.now()), 15_000) : undefined;
    return () => {
      document.removeEventListener("visibilitychange", update);
      clearInterval(timer);
    };
  }, [visible]);
  const query = useEnvironmentQuery(
    visible
      ? pullRequestEnvironment.ciStatus({
          environmentId,
          input: { host: "github.com", organization: "Jones-Systems" },
        })
      : null,
  );
  const [lastData, setLastData] = useState(query.data);
  if (query.data !== null && query.data !== lastData) setLastData(query.data);
  const data = query.data ?? lastData;
  useLiveRefresh(query.refresh, {
    enabled: visible,
    intervalMs: 60_000,
    key: `ci-status:${environmentId}`,
  });
  const stale =
    data !== null && (query.error !== null || now - Date.parse(data.observedAt) >= 120_000);
  return (
    <CiStatusPresentation
      data={data}
      now={now}
      stale={stale}
      refreshing={query.isPending}
      onRefresh={query.refresh}
      refreshDisabled={!visible}
      refreshUnavailable={query.error !== null}
    />
  );
}

function CiStatusPresentation({
  data,
  now,
  stale,
  refreshing,
  onRefresh,
  refreshDisabled = false,
  refreshUnavailable = false,
}: {
  data: PullRequestCiStatusResult | null;
  now: number;
  stale: boolean;
  refreshing: boolean;
  onRefresh: () => void;
  refreshDisabled?: boolean;
  refreshUnavailable?: boolean;
}) {
  const waiting = data?.jobs.items.filter((job) => job.status === "queued").length ?? 0;
  const onlineRunners = data?.runners.items.filter((runner) => runner.status === "online") ?? [];
  const offlineRunners = data?.runners.items.filter((runner) => runner.status === "offline") ?? [];
  const unknownRunners = data?.runners.items.filter((runner) => runner.status === "unknown") ?? [];
  const busyRunners = onlineRunners.filter((runner) => runner.busy).length;
  return (
    <>
      <div className="flex items-center justify-between gap-2 text-xs">
        <span className="text-muted-foreground">github.com / Jones-Systems</span>
        <Button
          variant="ghost"
          size="xs"
          disabled={refreshDisabled || refreshing}
          onClick={onRefresh}
          aria-label="Refresh CI status"
        >
          <RefreshCwIcon aria-hidden className={refreshing ? "size-3 animate-spin" : "size-3"} />{" "}
          Refresh
        </Button>
      </div>
      {refreshUnavailable ? (
        <p className="text-xs text-warning" role="status">
          CI refresh unavailable. Try again after checking the environment connection and GitHub
          access.
        </p>
      ) : null}
      {data === null ? (
        <p className="text-xs text-muted-foreground" role="status">
          {refreshing ? "Reading CI jobs and runners…" : "CI status unavailable."}
        </p>
      ) : (
        <>
          <div
            className="grid grid-cols-2 gap-3 rounded-md bg-muted/50 p-3 text-xs"
            aria-label="CI summary"
          >
            <div>
              <p className="font-medium">
                {data.jobs.state === "unavailable" ? "Unavailable" : waiting}
              </p>
              <p className="text-muted-foreground">
                Waiting jobs
                {data.jobs.state === "partial" || data.scopeTruncated ? " observed" : ""}
              </p>
            </div>
            <div>
              <p className="font-medium">
                {data.runners.state === "unavailable"
                  ? "Unavailable"
                  : `${busyRunners} / ${onlineRunners.length}`}
              </p>
              <p className="text-muted-foreground">
                Busy / online runners{data.runners.state === "partial" ? " observed" : ""}
              </p>
            </div>
          </div>
          <p className="text-xs text-muted-foreground">
            {stale ? "Stale · " : ""}Observed{" "}
            {new Date(data.observedAt).toLocaleTimeString("en-US", {
              timeZone: "America/New_York",
              hour: "numeric",
              minute: "2-digit",
              timeZoneName: "short",
            })}
          </p>
          <div className="space-y-2">
            <h3 className="text-xs font-medium">
              {data.jobs.state === "unavailable"
                ? "Waiting jobs unavailable"
                : `${waiting} waiting job${waiting === 1 ? "" : "s"}${data.jobs.state === "partial" || data.scopeTruncated ? " observed" : ""}`}
            </h3>
            <SectionNotice state={data.jobs.state} reasons={data.jobs.reasons} />
            <p className="text-xs text-muted-foreground">
              Scope:{" "}
              {data.repositories.length
                ? data.repositories.join(", ")
                : "No connected repositories"}
              {data.scopeTruncated ? " · scope truncated" : ""}. Waiting can include dependencies,
              approvals, or capacity.
            </p>
            <ul className="max-h-48 space-y-2 overflow-y-auto">
              {data.jobs.items.map((job) => (
                <li key={`${job.repository}:${job.id}`} className="text-xs">
                  <div className="flex items-start justify-between gap-3">
                    {job.url ? (
                      <a
                        href={job.url}
                        target="_blank"
                        rel="noreferrer"
                        className="min-w-0 break-words hover:underline"
                      >
                        {job.name}
                      </a>
                    ) : (
                      <span>{job.name}</span>
                    )}
                    <span className="shrink-0 text-muted-foreground">
                      {job.status === "queued" ? "Waiting" : "Running"}
                    </span>
                  </div>
                  <p className="text-muted-foreground">{job.repository}</p>
                </li>
              ))}
            </ul>
            {data.jobs.items.length === 0 && data.jobs.state === "available" ? (
              <p className="text-xs text-muted-foreground">No active jobs found in this scope.</p>
            ) : null}
            <SectionNotice state={data.workflows.state} reasons={data.workflows.reasons} />
            {data.workflows.items.length > 0 ? (
              <div className="space-y-2">
                <h3 className="text-xs font-medium">
                  {data.workflows.items.length} workflows without job details
                </h3>
                <ul className="max-h-28 space-y-2 overflow-y-auto">
                  {data.workflows.items.map((run) => (
                    <li key={`${run.repository}:${run.id}`} className="text-xs">
                      {run.url ? (
                        <a
                          href={run.url}
                          target="_blank"
                          rel="noreferrer"
                          className="hover:underline"
                        >
                          {run.name}
                        </a>
                      ) : (
                        run.name
                      )}
                      <span className="text-muted-foreground">
                        {" "}
                        · {run.status} · {run.repository}
                      </span>
                    </li>
                  ))}
                </ul>
              </div>
            ) : null}
          </div>
          <div className="space-y-2 border-t border-border/50 pt-3">
            <h3 className="flex items-center gap-2 text-xs font-medium">
              <ServerIcon aria-hidden className="size-3.5" />
              Jones-Systems runners
            </h3>
            <p className="text-xs text-muted-foreground">
              Organization inventory · {data.runners.items.length} observed. Busy jobs outside the
              repository scope may have no duration.
            </p>
            <SectionNotice state={data.runners.state} reasons={data.runners.reasons} />
            <RunnerList
              runners={onlineRunners}
              jobs={data.jobs.items}
              now={now}
              label="Online runners"
            />
            {onlineRunners.length === 0 && data.runners.state === "available" ? (
              <p className="text-xs text-muted-foreground">No online runners found.</p>
            ) : null}
            {unknownRunners.length > 0 ? (
              <div className="space-y-2 text-muted-foreground">
                <p className="text-xs">{unknownRunners.length} runners with unknown status</p>
                <RunnerList
                  runners={unknownRunners}
                  jobs={data.jobs.items}
                  now={now}
                  label="Runners with unknown status"
                />
              </div>
            ) : null}
            {offlineRunners.length > 0 ? (
              <details className="space-y-2 text-muted-foreground">
                <summary className="cursor-pointer text-xs">
                  {offlineRunners.length} offline runners
                </summary>
                <RunnerList
                  runners={offlineRunners}
                  jobs={data.jobs.items}
                  now={now}
                  label="Offline runners"
                />
              </details>
            ) : null}
            {data.runners.items.length === 0 && data.runners.state === "available" ? (
              <p className="text-xs text-muted-foreground">No organization runners found.</p>
            ) : null}
          </div>
        </>
      )}
    </>
  );
}

function RunnerList({
  runners,
  jobs,
  now,
  label,
}: {
  runners: readonly CiRunner[];
  jobs: readonly CiJob[];
  now: number;
  label: string;
}) {
  if (runners.length === 0) return null;
  return (
    <ul aria-label={label} className="max-h-56 space-y-2 overflow-y-auto">
      {runners.map((runner) => (
        <li key={runner.id} className="flex items-start justify-between gap-3 text-xs">
          <span className="min-w-0 break-words">{runner.name}</span>
          <span className="shrink-0 text-muted-foreground">{runnerRuntime(runner, jobs, now)}</span>
        </li>
      ))}
    </ul>
  );
}

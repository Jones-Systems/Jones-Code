import type { StorageCleanupReport } from "@t3tools/contracts";
import { useState } from "react";
import { SettingsRow } from "../../components/settings/settingsLayout";
import { Button, InlineButton } from "../../components/ui/button";
import { formatBytes } from "../../lib/formatBytes";

type CleanupReport = {
  environmentId: string;
  environmentLabel: string;
  report: StorageCleanupReport;
  onRetire?: (entry: StorageCleanupReport["entries"][number]) => void;
};

export function CleanupResults({
  selectedEnvironmentIds,
  reports,
}: {
  selectedEnvironmentIds: readonly string[];
  reports: readonly CleanupReport[];
}) {
  const identity = JSON.stringify([
    [...selectedEnvironmentIds].sort(),
    [...reports]
      .sort((left, right) => left.environmentId.localeCompare(right.environmentId))
      .map(({ environmentId, report }) => [environmentId, report.startedAt, report.finishedAt]),
  ]);
  return <CleanupResultList key={identity} reports={reports} />;
}

type CleanupEntry = StorageCleanupReport["entries"][number];

function canRetire(entry: CleanupEntry) {
  return Boolean(
    entry.threadId &&
      entry.path &&
      entry.retirement?.fingerprint &&
      entry.retirement.paths.length <= 200 &&
      entry.retirement.reasonCodes.every((reason) =>
        ["unattested_ignored", "sensitive_ignored", "untracked"].includes(reason),
      ) &&
      entry.retirement.state !== "retired",
  );
}

const categories = ["Ready to retire", "Needs attention", "Kept", "Removed"] as const;

function categoryFor(entry: CleanupEntry, actionable: boolean) {
  if (entry.outcome === "removed" || entry.retirement?.state === "retired") return "Removed";
  if (actionable && canRetire(entry)) return "Ready to retire";
  if (entry.outcome === "failed" || (entry.retirement && ["held", "partial", "unknown", "not_retired"].includes(entry.retirement.state))) return "Needs attention";
  return "Kept";
}

function CleanupResultList({ reports }: { reports: readonly CleanupReport[] }) {
  return reports.map((report) => <DeviceResults key={report.environmentId} {...report} />);
}

function DeviceResults(props: CleanupReport) {
  const [expanded, setExpanded] = useState(true);
  const groups = categories.map((category) => ({
    category,
    entries: props.report.entries.filter(
      (entry) => categoryFor(entry, Boolean(props.onRetire)) === category,
    ),
  })).filter(({ entries }) => entries.length > 0);
  return (
    <section aria-label={`${props.environmentLabel} cleanup results`}>
      <button
        type="button"
        aria-expanded={expanded}
        className="flex w-full items-center gap-2 px-3 py-2.5 text-left text-sm font-medium sm:px-4"
        onClick={() => setExpanded((value) => !value)}
      >
        <span aria-hidden="true">{expanded ? "▾" : "▸"}</span>
        <span className="min-w-0 wrap-anywhere">{props.environmentLabel}</span>
        <span className="text-xs text-muted-foreground">{props.report.entries.length.toLocaleString()} results</span>
      </button>
      {expanded && (
        <div className="ml-3 border-l border-border/60 sm:ml-4">
          {groups.map(({ category, entries }, index) => (
            <ResultCategory key={category} {...props} category={category} entries={entries} initiallyOpen={index === 0} />
          ))}
          {props.report.omittedCount > 0 && (
            <p className="px-3 py-2.5 text-xs text-muted-foreground sm:px-4">
              {props.environmentLabel}: {props.report.omittedCount.toLocaleString()} additional results omitted from this report. These were not classified here.
            </p>
          )}
          {props.report.entries.length === 0 && props.report.omittedCount === 0 && (
            <p className="px-3 py-2.5 text-xs text-muted-foreground sm:px-4">{props.environmentLabel}: Nothing needed cleanup.</p>
          )}
        </div>
      )}
    </section>
  );
}

function ResultCategory({
  category, entries, initiallyOpen, ...props
}: CleanupReport & {
  category: (typeof categories)[number];
  entries: StorageCleanupReport["entries"];
  initiallyOpen: boolean;
}) {
  const [expanded, setExpanded] = useState(initiallyOpen);
  const [visibleCount, setVisibleCount] = useState(5);
  const shown = Math.min(visibleCount, entries.length);
  return (
    <section aria-label={`${props.environmentLabel}: ${category}`}>
      <button
        type="button"
        aria-expanded={expanded}
        className="flex w-full items-center gap-2 px-3 py-2 text-left text-xs font-medium sm:px-4"
        onClick={() => setExpanded((value) => !value)}
      >
        <span aria-hidden="true">{expanded ? "▾" : "▸"}</span>
        {category} <span className="text-muted-foreground">{entries.length.toLocaleString()}</span>
      </button>
      {expanded && (
        <>
          <CleanupReportRows {...props} entries={entries.slice(0, shown)} />
          {entries.length > 5 && (
            <div className="flex flex-wrap items-center gap-x-3 gap-y-1 px-3 py-2.5 text-xs text-muted-foreground sm:px-4">
              <span>Showing {shown.toLocaleString()} of {entries.length.toLocaleString()} results.</span>
              {shown < entries.length && (
                <>
                  <InlineButton tone="muted" onClick={() => setVisibleCount((count) => Math.min(count + 10, entries.length))}>Show more</InlineButton>
                  <InlineButton tone="muted" onClick={() => setVisibleCount(entries.length)}>Show all</InlineButton>
                </>
              )}
              {shown > 5 && <InlineButton tone="muted" onClick={() => setVisibleCount(5)}>Show less</InlineButton>}
            </div>
          )}
        </>
      )}
    </section>
  );
}

function factsWithKeys<A>(values: readonly A[], identity: (value: A) => string) {
  const occurrences = new Map<string, number>();
  return values.map((value) => {
    const fact = identity(value);
    const occurrence = occurrences.get(fact) ?? 0;
    occurrences.set(fact, occurrence + 1);
    return { value, key: `${fact}:${occurrence}` };
  });
}

function lineageIdentity(lineage: Readonly<Record<string, string>>) {
  return JSON.stringify(
    Object.entries(lineage).sort(([left], [right]) => left.localeCompare(right)),
  );
}

function CleanupReportRows({
  entries,
  environmentLabel,
  onRetire,
}: {
  report: StorageCleanupReport;
  entries: StorageCleanupReport["entries"];
  environmentLabel: string;
  onRetire?: (entry: StorageCleanupReport["entries"][number]) => void;
}) {
  return (
    <>
      {entries.map((entry, index) => {
        const name =
          entry.kind === "worktree"
            ? ((entry.threadId === null
                ? entry.path
                : entry.path?.split(/[\\/]/).findLast(Boolean)) ?? "Worktrees")
            : entry.kind === "logs"
              ? "Rotated logs"
              : "Browser artifacts";
        return (
          <SettingsRow
            key={`${entry.path ?? entry.kind}:${entry.threadId ?? index}`}
            title={<span className="wrap-anywhere select-text">{name}</span>}
            description={
              <span className="block space-y-2 wrap-anywhere select-text">
                <span className={`block ${entry.outcome === "failed" ? "text-destructive" : ""}`}>
                  {entry.reason}
                </span>
                <span className="grid grid-cols-[auto_minmax(0,1fr)] gap-x-3 gap-y-1 border-l border-border/60 pl-3">
                  <span>Machine</span>
                  <span className="text-foreground/80">{environmentLabel}</span>
                  {entry.projectPath && (
                    <>
                      <span>Project</span>
                      <code className="text-foreground/80">{entry.projectPath}</code>
                    </>
                  )}
                  {entry.path && (
                    <>
                      <span>Folder</span>
                      <code className="text-foreground/80">{entry.path}</code>
                    </>
                  )}
                  {entry.threadTitle && (
                    <>
                      <span>Thread</span>
                      <span>{entry.threadTitle}</span>
                    </>
                  )}
                </span>
              </span>
            }
            control={
              <>
                {onRetire && canRetire(entry) && (
                    <Button variant="outline" size="sm" onClick={() => onRetire(entry)}>
                      Retire
                    </Button>
                  )}
                {(entry.outcome === "removed" ||
                  (entry.outcome === "kept" && entry.bytes !== null)) && (
                  <span className="text-xs text-muted-foreground">
                    {[
                      entry.kind !== "worktree" && entry.files !== null
                        ? `${entry.files.toLocaleString()} ${entry.files === 1 ? "file" : "files"}`
                        : null,
                      entry.bytes !== null ? formatBytes(entry.bytes) : null,
                    ]
                      .filter(Boolean)
                      .join(" · ")}
                  </span>
                )}
              </>
            }
          >
            {entry.retirement &&
              (entry.retirement.receiptId ||
                entry.retirement.details.length > 0 ||
                entry.retirement.outputObligations.length > 0) && (
                <details className="pb-2 text-xs text-muted-foreground wrap-anywhere select-text">
                  <summary className="cursor-pointer">Retirement details</summary>
                  <span className="mt-1 block space-y-1">
                    {entry.retirement.receiptId && (
                      <span className="block">
                        Receipt: {entry.retirement.receiptId} · {entry.retirement.state}
                      </span>
                    )}
                    {factsWithKeys(entry.retirement.details, (detail) =>
                      JSON.stringify([
                        detail.target,
                        detail.root,
                        detail.reason,
                        detail.fact,
                        detail.missingFact,
                        detail.nextAction,
                        detail.lineage.map(lineageIdentity),
                      ]),
                    ).map(({ value: detail, key }) => (
                      <span key={key} className="block">
                        <code>{detail.root}</code>: {detail.fact}{" "}
                        {detail.missingFact ? `Missing: ${detail.missingFact}. ` : ""}
                        {detail.nextAction}
                        {factsWithKeys(detail.lineage, lineageIdentity).map(
                          ({ value: lineage, key }) => (
                            <span key={key} className="block">
                              {Object.entries(lineage)
                                .map(([key, value]) => `${key}: ${value}`)
                                .join(" · ")}
                            </span>
                          ),
                        )}
                      </span>
                    ))}
                    {entry.retirement.outputObligations.map((output) => (
                      <span key={output.id} className="block">
                        Output {output.id}: <code>{output.path}</code> · Owner {output.owner} ·{" "}
                        {output.state}
                      </span>
                    ))}
                  </span>
                </details>
              )}
          </SettingsRow>
        );
      })}
    </>
  );
}

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

function CleanupResultList({ reports }: { reports: readonly CleanupReport[] }) {
  const [visibleCount, setVisibleCount] = useState(5);
  const total = reports.reduce((count, { report }) => count + report.entries.length, 0);
  const shown = Math.min(visibleCount, total);
  let remaining = shown;
  return (
    <>
      {reports.map(({ environmentId, ...group }) => {
        const entries = group.report.entries.slice(0, remaining);
        remaining -= entries.length;
        return <CleanupReportRows key={environmentId} {...group} entries={entries} />;
      })}
      {total > 5 && (
        <div className="flex flex-wrap items-center gap-x-3 gap-y-1 px-3 py-2.5 text-xs text-muted-foreground sm:px-4">
          <span>
            Showing {shown.toLocaleString()} of {total.toLocaleString()} results.
          </span>
          {shown < total && (
            <>
              <InlineButton
                tone="muted"
                onClick={() => setVisibleCount((count) => Math.min(count + 10, total))}
              >
                Show more
              </InlineButton>
              <InlineButton tone="muted" onClick={() => setVisibleCount(total)}>
                Show all
              </InlineButton>
            </>
          )}
          {shown > 5 && (
            <InlineButton tone="muted" onClick={() => setVisibleCount(5)}>
              Show less
            </InlineButton>
          )}
        </div>
      )}
    </>
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
  report,
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
                {onRetire &&
                  entry.threadId &&
                  entry.path &&
                  entry.retirement?.fingerprint &&
                  entry.retirement.paths.length <= 200 &&
                  entry.retirement.reasonCodes.every((reason) =>
                    ["unattested_ignored", "sensitive_ignored", "untracked"].includes(reason),
                  ) &&
                  entry.retirement.state !== "retired" && (
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
      {report.omittedCount > 0 && (
        <p className="px-3 py-2.5 text-xs text-muted-foreground sm:px-4">
          {environmentLabel}: {report.omittedCount.toLocaleString()} additional results omitted from
          this report.
        </p>
      )}
      {report.entries.length === 0 && report.omittedCount === 0 && (
        <p className="px-3 py-2.5 text-xs text-muted-foreground sm:px-4">
          {environmentLabel}: Nothing needed cleanup.
        </p>
      )}
    </>
  );
}

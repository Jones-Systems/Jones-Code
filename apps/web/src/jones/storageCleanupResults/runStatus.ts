import type { StorageCleanupReport, TimestampFormat } from "@t3tools/contracts";
import { formatDayAwareTimestamp, resolveHour12 } from "../../timestampFormat.ts";

export type CleanupOperation = NonNullable<StorageCleanupReport["operation"]>;
export type CleanupRequest = {
  operation: CleanupOperation;
  status: "queued" | "running" | "unconfirmed" | "completed";
  detail?: string;
  report?: StorageCleanupReport;
};

export function cleanupRequestIsOutstanding(
  request: CleanupRequest,
  retained: StorageCleanupReport | null,
) {
  if (request.status !== "completed" || !request.report || !retained) return true;
  const completed = request.report;
  if (completed.startedAt === retained.startedAt && completed.finishedAt === retained.finishedAt)
    return false;
  return !(
    Date.parse(retained.startedAt) >= Date.parse(completed.startedAt) &&
    Date.parse(retained.finishedAt) >= Date.parse(completed.finishedAt)
  );
}

function cleanupOperationLabel(operation: CleanupOperation | undefined) {
  switch (operation) {
    case "propose":
      return "Retirement proposal";
    case "retire":
      return "Checkout retirement";
    case "cleanup":
      return "Cleanup";
    default:
      return "Run (action unavailable)";
  }
}

export function cleanupReportSummary(report: StorageCleanupReport) {
  const operation = report.operation ?? (report.trigger === "automatic" ? "cleanup" : undefined);
  const partial = report.entries.some((entry) => entry.retirement?.state === "partial");
  const unknown = report.entries.some((entry) => entry.retirement?.state === "unknown");
  const outcome = unknown
    ? "Completion unknown"
    : partial
      ? "Partial result"
      : report.counts.failed > 0
        ? report.counts.removed + report.counts.kept > 0
          ? "Completed with failures"
          : "Failed"
        : "Completed";
  const counts =
    operation === "propose"
      ? `${report.counts.kept} reviewed, ${report.counts.failed} failed; no deletion requested`
      : `${report.counts.removed} removed, ${report.counts.kept} kept${report.counts.failed ? `, ${report.counts.failed} failed` : ""}`;
  const label =
    report.trigger === "automatic" ? "Automatic cleanup" : cleanupOperationLabel(operation);
  return `${label}: ${outcome} · ${counts}`;
}

export function cleanupRequestSummary(request: CleanupRequest) {
  const label = cleanupOperationLabel(request.operation);
  switch (request.status) {
    case "queued":
      return `${label}: Waiting…`;
    case "running":
      return `${label}: Running…`;
    case "unconfirmed":
      return `${label}: Could not confirm completion. ${request.detail ?? ""}`.trim();
    case "completed":
      return request.report ? cleanupReportSummary(request.report) : `${label}: Completed`;
  }
}

export function cleanupTimestamp(
  iso: string,
  timestampFormat: TimestampFormat,
  nowMs: number,
  timeZone?: string,
) {
  if (!Number.isFinite(Date.parse(iso))) return null;
  return {
    label: formatDayAwareTimestamp(iso, timestampFormat, nowMs, timeZone),
    exact: new Intl.DateTimeFormat(undefined, {
      year: "numeric",
      month: "short",
      day: "numeric",
      hour: "numeric",
      minute: "2-digit",
      second: "2-digit",
      hour12: resolveHour12(timestampFormat),
      timeZoneName: "short",
      ...(timeZone ? { timeZone } : {}),
    }).format(new Date(iso)),
  };
}

export function cleanupFleetTimestamp(
  reports: readonly { label: string; report: StorageCleanupReport | null }[],
  timestampFormat: TimestampFormat,
  nowMs: number,
) {
  const times = reports.map(({ label, report }) => ({
    label,
    iso: report?.finishedAt,
    time: report ? cleanupTimestamp(report.finishedAt, timestampFormat, nowMs) : null,
  }));
  const first = times[0];
  if (!first || reports.every(({ report }) => !report)) {
    return { label: "No report", exact: "No completed report available." };
  }
  const exact = times
    .map(({ label, time }) => `${label}: ${time?.exact ?? "No completion time available"}`)
    .join("\n");
  if (times.some(({ time }) => !time)) return { label: "Time unavailable", exact };
  if (!times.every(({ iso }) => Date.parse(iso!) === Date.parse(first.iso!))) {
    const sorted = [...times].sort((left, right) => Date.parse(left.iso!) - Date.parse(right.iso!));
    return { label: `${sorted[0]!.time!.label} – ${sorted.at(-1)!.time!.label}`, exact };
  }
  return { label: first.time!.label, exact };
}

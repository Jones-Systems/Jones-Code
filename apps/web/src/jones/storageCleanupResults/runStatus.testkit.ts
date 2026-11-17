// @effect-diagnostics nodeBuiltinImport:off -- Pure report presentation cases also run without installed dependencies.
import * as NodeAssert from "node:assert/strict";
import type { StorageCleanupReport } from "@t3tools/contracts";
import {
  cleanupFleetTimestamp,
  cleanupReportSummary,
  cleanupRequestSummary,
  cleanupRequestIsOutstanding,
  cleanupTimestamp,
} from "./runStatus.ts";

const finishedAt = "2026-11-16T23:00:00.000Z";
const now = Date.parse("2026-11-16T23:30:00.000Z");
const report = (patch: Partial<StorageCleanupReport> = {}): StorageCleanupReport => ({
  trigger: "manual",
  startedAt: "2026-11-16T22:59:00.000Z",
  finishedAt,
  entries: [],
  counts: { removed: 0, kept: 0, failed: 0 },
  bytesFreed: 0,
  omittedCount: 0,
  ...patch,
});
const entry = (state: "partial" | "unknown"): StorageCleanupReport["entries"][number] => ({
  kind: "worktree",
  outcome: "kept",
  reason: "Synthetic observation",
  path: null,
  projectPath: null,
  threadId: null,
  threadTitle: null,
  bytes: null,
  files: null,
  retirement: {
    state,
    reasonCodes: [],
    receiptId: null,
    fingerprint: null,
    paths: [],
    details: [],
    outputObligations: [],
  },
});

export const runStatusCases = {
  "completed requests accept their retained report and a later authoritative manual report": () => {
    const completed = report({ operation: "propose" });
    const request = { operation: "propose", status: "completed", report: completed } as const;
    const previous = report({
      startedAt: "2026-11-16T20:00:00.000Z",
      finishedAt: "2026-11-16T20:01:00.000Z",
    });
    const later = report({
      startedAt: finishedAt,
      finishedAt: "2026-11-16T23:01:00.000Z",
      operation: "cleanup",
    });
    NodeAssert.equal(cleanupRequestIsOutstanding(request, previous), true);
    NodeAssert.equal(cleanupRequestIsOutstanding(request, null), true);
    NodeAssert.equal(cleanupRequestIsOutstanding(request, completed), false);
    NodeAssert.equal(cleanupRequestIsOutstanding(request, later), false);
    NodeAssert.equal(cleanupRequestIsOutstanding(request, report({ finishedAt: "invalid" })), true);
    NodeAssert.equal(
      cleanupRequestIsOutstanding({ ...request, status: "unconfirmed" }, later),
      true,
    );
  },
  "zero counts complete without implying a failed or destructive proposal": () => {
    const summary = cleanupReportSummary(report({ operation: "propose" }));
    NodeAssert.match(summary, /Retirement proposal: Completed/);
    NodeAssert.match(summary, /no deletion requested/);
    NodeAssert.doesNotMatch(summary, /removed|recovered|Failed/);
  },
  "retire summary shows recovered size": () => {
    const summary = cleanupReportSummary(
      report({
        operation: "retire",
        counts: { removed: 1, kept: 0, failed: 0 },
        bytesFreed: 3 * 1024 ** 3,
      }),
    );
    NodeAssert.match(summary, /Checkout retirement: Completed · 1 removed, 0 kept/);
    NodeAssert.match(summary, /about 3\.00 GB recovered/);
    NodeAssert.doesNotMatch(cleanupReportSummary(report({ operation: "retire" })), /recovered/);
  },
  "legacy manual reports preserve unknown action instead of inferring delete": () => {
    NodeAssert.match(cleanupReportSummary(report()), /Run \(action unavailable\): Completed/);
    NodeAssert.match(
      cleanupReportSummary(report({ trigger: "automatic" })),
      /Automatic cleanup: Completed/,
    );
  },
  "failed and partially successful reports keep distinct outcomes": () => {
    NodeAssert.match(
      cleanupReportSummary(report({ counts: { removed: 0, kept: 0, failed: 2 } })),
      /: Failed/,
    );
    NodeAssert.match(
      cleanupReportSummary(report({ counts: { removed: 1, kept: 0, failed: 2 } })),
      /Completed with failures/,
    );
    NodeAssert.match(
      cleanupReportSummary(report({ entries: [entry("partial")] })),
      /Partial result/,
    );
    NodeAssert.match(
      cleanupReportSummary(report({ entries: [entry("unknown")] })),
      /Completion unknown/,
    );
  },
  "request progress and unconfirmed failures never claim completion": () => {
    NodeAssert.match(cleanupRequestSummary({ operation: "propose", status: "queued" }), /Waiting/);
    NodeAssert.match(cleanupRequestSummary({ operation: "propose", status: "running" }), /Running/);
    NodeAssert.match(
      cleanupRequestSummary({
        operation: "propose",
        status: "unconfirmed",
        detail: "Connection lost",
      }),
      /Could not confirm completion. Connection lost/,
    );
    NodeAssert.match(
      cleanupRequestSummary({
        operation: "propose",
        status: "completed",
        report: report({ operation: "propose" }),
      }),
      /Retirement proposal: Completed/,
    );
  },
  "no retained reports do not invent a last run": () => {
    NodeAssert.equal(
      cleanupFleetTimestamp([{ label: "Mini", report: null }], "locale", now).label,
      "No report",
    );
    NodeAssert.equal(cleanupTimestamp("invalid", "locale", now), null);
    NodeAssert.equal(
      cleanupFleetTimestamp(
        [{ label: "Mini", report: report({ finishedAt: "invalid" }) }],
        "locale",
        now,
      ).label,
      "Time unavailable",
    );
  },
  "a common finish instant has a localized timestamp and exact per-machine tooltip": () => {
    const time = cleanupTimestamp(finishedAt, "24-hour", now)!;
    const fleet = cleanupFleetTimestamp(
      [
        { label: "Mini", report: report() },
        { label: "VPS", report: report({ finishedAt: "2026-11-16T18:00:00-05:00" }) },
      ],
      "24-hour",
      now,
    );
    NodeAssert.equal(fleet.label, time.label);
    NodeAssert.match(fleet.exact, /Mini:/);
    NodeAssert.match(fleet.exact, /VPS:/);
    NodeAssert.ok(time.exact.includes("2026"));
    NodeAssert.ok(time.exact.includes("00"));
  },
  "mixed fleet times display a range and preserve each completion instant": () => {
    const earlier = "2026-11-15T22:00:00.000Z";
    const fleet = cleanupFleetTimestamp(
      [
        { label: "Mini", report: report() },
        { label: "VPS", report: report({ finishedAt: earlier }) },
      ],
      "12-hour",
      now,
    );
    NodeAssert.equal(
      fleet.label,
      `${cleanupTimestamp(earlier, "12-hour", now)!.label} – ${cleanupTimestamp(finishedAt, "12-hour", now)!.label}`,
    );
    NodeAssert.ok(fleet.exact.includes(cleanupTimestamp(earlier, "12-hour", now)!.exact));
    NodeAssert.ok(fleet.exact.includes(cleanupTimestamp(finishedAt, "12-hour", now)!.exact));
  },
  "exact completion times include the seasonally correct time zone": () => {
    const timeZone = "America/New_York";
    const winter = cleanupTimestamp(finishedAt, "12-hour", now, timeZone)!;
    const summerDate = "2026-06-16T23:00:00.000Z";
    const summer = cleanupTimestamp(summerDate, "12-hour", now, timeZone)!;
    const zone = (iso: string) =>
      new Intl.DateTimeFormat(undefined, { timeZone, timeZoneName: "short" })
        .formatToParts(new Date(iso))
        .find((part) => part.type === "timeZoneName")!.value;
    NodeAssert.notEqual(zone(summerDate), zone(finishedAt));
    NodeAssert.ok(winter.exact.includes(zone(finishedAt)));
    NodeAssert.ok(summer.exact.includes(zone(summerDate)));
  },
  "missing fleet reports cannot inherit a different machine's fresh timestamp": () => {
    const fleet = cleanupFleetTimestamp(
      [
        { label: "Mini", report: report() },
        { label: "VPS", report: null },
      ],
      "locale",
      now,
    );
    NodeAssert.equal(fleet.label, "Time unavailable");
    NodeAssert.match(fleet.exact, /VPS: No completion time available/);
  },
};

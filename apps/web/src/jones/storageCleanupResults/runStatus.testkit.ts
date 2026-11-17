// @effect-diagnostics nodeBuiltinImport:off -- Pure report presentation cases also run without installed dependencies.
import assert from "node:assert/strict";
import type { StorageCleanupReport } from "@t3tools/contracts";
import {
  cleanupFleetTimestamp,
  cleanupReportSummary,
  cleanupRequestSummary,
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
  "zero counts complete without implying a failed or destructive proposal": () => {
    const summary = cleanupReportSummary(report({ operation: "propose" }));
    assert.match(summary, /Retirement proposal: Completed/);
    assert.match(summary, /no deletion requested/);
    assert.doesNotMatch(summary, /removed|recovered|Failed/);
  },
  "legacy manual reports preserve unknown action instead of inferring delete": () => {
    assert.match(cleanupReportSummary(report()), /Run \(action unavailable\): Completed/);
    assert.match(
      cleanupReportSummary(report({ trigger: "automatic" })),
      /Automatic cleanup: Completed/,
    );
  },
  "failed and partially successful reports keep distinct outcomes": () => {
    assert.match(
      cleanupReportSummary(report({ counts: { removed: 0, kept: 0, failed: 2 } })),
      /: Failed/,
    );
    assert.match(
      cleanupReportSummary(report({ counts: { removed: 1, kept: 0, failed: 2 } })),
      /Completed with failures/,
    );
    assert.match(cleanupReportSummary(report({ entries: [entry("partial")] })), /Partial result/);
    assert.match(
      cleanupReportSummary(report({ entries: [entry("unknown")] })),
      /Completion unknown/,
    );
  },
  "request progress and unconfirmed failures never claim completion": () => {
    assert.match(cleanupRequestSummary({ operation: "propose", status: "queued" }), /Waiting/);
    assert.match(cleanupRequestSummary({ operation: "propose", status: "running" }), /Running/);
    assert.match(
      cleanupRequestSummary({
        operation: "propose",
        status: "unconfirmed",
        detail: "Connection lost",
      }),
      /Could not confirm completion. Connection lost/,
    );
    assert.match(
      cleanupRequestSummary({
        operation: "propose",
        status: "completed",
        report: report({ operation: "propose" }),
      }),
      /Retirement proposal: Completed/,
    );
  },
  "no retained reports do not invent a last run": () => {
    assert.equal(
      cleanupFleetTimestamp([{ label: "Mini", report: null }], "locale", now).label,
      "No report",
    );
    assert.equal(cleanupTimestamp("invalid", "locale", now), null);
    assert.equal(
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
    assert.equal(fleet.label, time.label);
    assert.match(fleet.exact, /Mini:/);
    assert.match(fleet.exact, /VPS:/);
    assert.ok(time.exact.includes("2026"));
    assert.ok(time.exact.includes("00"));
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
    assert.equal(
      fleet.label,
      `${cleanupTimestamp(earlier, "12-hour", now)!.label} – ${cleanupTimestamp(finishedAt, "12-hour", now)!.label}`,
    );
    assert.ok(fleet.exact.includes(cleanupTimestamp(earlier, "12-hour", now)!.exact));
    assert.ok(fleet.exact.includes(cleanupTimestamp(finishedAt, "12-hour", now)!.exact));
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
    assert.notEqual(zone(summerDate), zone(finishedAt));
    assert.ok(winter.exact.includes(zone(finishedAt)));
    assert.ok(summer.exact.includes(zone(summerDate)));
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
    assert.equal(fleet.label, "Time unavailable");
    assert.match(fleet.exact, /VPS: No completion time available/);
  },
};

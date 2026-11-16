import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  StorageCleanupReportReadInput,
  ThreadId,
  type StorageCleanupReport,
  type StorageCleanupReportEntry,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import {
  MAX_STORAGE_CLEANUP_REPORT_RESPONSE_BYTES,
  pageStorageCleanupReport,
  StorageCleanupReportReadError,
} from "./ReportRead.ts";

const environmentId = EnvironmentId.make("report-environment");
const threadId = ThreadId.make("report-thread");
const entry = (index: number): StorageCleanupReportEntry => ({
  kind: "worktree",
  outcome: "kept",
  reason: `Held ${index}`,
  path: `/worktrees/task-${index}`,
  projectPath: "/projects/synthetic",
  threadId,
  threadTitle: `Task ${index}`,
  bytes: index,
  files: 1,
  retirement: {
    state: "held",
    reasonCodes: ["uncovered-output"],
    receiptId: null,
    fingerprint: "synthetic-fingerprint",
    paths: [`/worktrees/task-${index}`],
    details: [
      {
        reason: "uncovered-output",
        target: `task-${index}`,
        root: "/worktrees",
        fact: "Retained output",
        missingFact: "Coverage",
        lineage: [{ owner: "synthetic-owner", output: `output-${index}` }],
        nextAction: "Review output coverage",
      },
    ],
    outputObligations: [{ id: `output-${index}`, path: "output", owner: "owner", state: "held" }],
  },
});
const report = (entries: StorageCleanupReport["entries"]): StorageCleanupReport => ({
  trigger: "manual",
  startedAt: "2026-11-16T12:00:00.000Z",
  finishedAt: "2026-11-16T12:00:01.000Z",
  entries,
  counts: { removed: 3, kept: 205, failed: 2 },
  omittedCount: 10,
  bytesFreed: 12345,
});
const page = (
  source: StorageCleanupReport | null,
  input: typeof StorageCleanupReportReadInput.Type = {},
) => {
  const result = pageStorageCleanupReport(environmentId, source, input);
  if (Schema.is(StorageCleanupReportReadError)(result)) throw result;
  return result;
};

it("distinguishes no session report from an empty finished report", () => {
  const missing = page(null);
  const empty = page(report([]));
  expect(missing).toMatchObject({
    authorityEffect: "none",
    environmentId,
    retention: "session-only",
    report: null,
    retainedTotal: 0,
    matchedCount: 0,
    nextCursor: null,
  });
  expect(empty.report).toEqual(report([]));
  expect(empty.snapshotId).not.toBe(missing.snapshotId);
});

it("reconstructs all retained full-detail entries without changing omissions or global totals", () => {
  const entries = Object.freeze(Array.from({ length: 25 }, (_, index) => Object.freeze(entry(index))));
  const source = Object.freeze(report(entries));
  const before = JSON.stringify(source);
  const first = page(source);
  expect(first.report?.entries).toHaveLength(10);
  expect(first).toMatchObject({ retainedTotal: 25, matchedCount: 25, cursor: 0, nextCursor: 10 });
  const second = page(source, { cursor: first.nextCursor!, expectedSnapshot: first.snapshotId });
  const last = page(source, { cursor: second.nextCursor!, expectedSnapshot: first.snapshotId });
  expect(last.nextCursor).toBeNull();
  expect([
    ...(first.report?.entries ?? []),
    ...(second.report?.entries ?? []),
    ...(last.report?.entries ?? []),
  ]).toEqual(entries);
  for (const result of [first, second, last]) {
    expect(result.snapshotId).toBe(first.snapshotId);
    expect(result.report).toMatchObject({
      trigger: source.trigger,
      startedAt: source.startedAt,
      finishedAt: source.finishedAt,
      counts: source.counts,
      bytesFreed: source.bytesFreed,
      omittedCount: 10,
    });
  }
  expect(JSON.stringify(source)).toBe(before);
  expect(
    page(report(Array.from({ length: 55 }, (_, index) => entry(index))), { limit: 50 }).report
      ?.entries,
  ).toHaveLength(50);
});

it("uses exact worktree and thread filters together and preserves the retained total", () => {
  const selected = entry(1);
  const source = report([
    selected,
    { ...selected, kind: "logs" },
    { ...selected, path: `${selected.path}/child` },
    { ...selected, threadId: ThreadId.make("other-thread") },
    { ...selected, path: `${selected.path}/` },
  ]);
  expect(page(source, { worktreePath: selected.path!, threadId })).toMatchObject({
    selectors: { worktreePath: selected.path, threadId },
    retainedTotal: 5,
    matchedCount: 1,
    report: { entries: [selected], omittedCount: 10 },
  });
  expect(page(source, { threadId }).matchedCount).toBe(4);
  expect(page(source, { worktreePath: "/missing" })).toMatchObject({
    retainedTotal: 5,
    matchedCount: 0,
    nextCursor: null,
    report: { entries: [], counts: source.counts, omittedCount: 10 },
  });
});

it("binds the snapshot to the whole report, environment and selectors, including unseen rows", () => {
  const source = report([entry(0), entry(1), entry(2)]);
  const first = page(source, { limit: 1 });
  const later = { cursor: 1, expectedSnapshot: first.snapshotId };
  for (const changed of [
    null,
    { ...source, entries: [entry(0), entry(1), { ...entry(2), reason: "Changed unseen row" }] },
    { ...source, omittedCount: source.omittedCount + 1 },
    { ...source, bytesFreed: 999 },
    { ...source, finishedAt: "2026-11-16T12:00:02.000Z" },
  ])
    expect(pageStorageCleanupReport(environmentId, changed, later)).toMatchObject({
      reason: "stale_snapshot",
    });
  expect(pageStorageCleanupReport(EnvironmentId.make("different"), source, later)).toMatchObject({
    reason: "stale_snapshot",
  });
  expect(pageStorageCleanupReport(environmentId, source, { ...later, threadId })).toMatchObject({
    reason: "stale_snapshot",
  });
  expect(
    pageStorageCleanupReport(environmentId, source, { ...later, worktreePath: entry(0).path! }),
  ).toMatchObject({ reason: "stale_snapshot" });
  expect(page(source, { ...later, limit: 2 }).report?.entries).toEqual(source.entries.slice(1));
});

it("rejects invalid and unbound cursors while allowing the exact end of retained matches", () => {
  const source = report([entry(0)]);
  const first = page(source);
  for (const cursor of [-1, 0.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1, 2])
    expect(
      pageStorageCleanupReport(environmentId, source, { cursor, expectedSnapshot: first.snapshotId }),
    ).toMatchObject({ reason: "invalid_cursor" });
  expect(pageStorageCleanupReport(environmentId, source, { cursor: 1 })).toMatchObject({
    reason: "snapshot_required",
  });
  expect(pageStorageCleanupReport(environmentId, source, { expectedSnapshot: "bad" })).toMatchObject({
    reason: "invalid_snapshot",
  });
  expect(page(source, { cursor: 1, expectedSnapshot: first.snapshotId })).toMatchObject({
    nextCursor: null,
    report: { entries: [] },
  });
  for (const limit of [0, -1, 1.5, 51, NaN, Infinity])
    expect(pageStorageCleanupReport(environmentId, source, { limit })).toMatchObject({
      reason: "invalid_limit",
    });
});

it("bounds serialized nested detail without truncating or silently skipping an oversized entry", () => {
  const large = {
    ...entry(1),
    retirement: {
      ...entry(1).retirement!,
      details: [
        {
          ...entry(1).retirement!.details[0]!,
          fact: 'nested "detail" 🦊'.repeat(10000),
        },
      ],
    },
  };
  const source = report([entry(0), large, entry(2)]);
  const first = page(source);
  expect(first.report?.entries).toEqual([entry(0)]);
  expect(first.nextCursor).toBe(1);
  expect(
    pageStorageCleanupReport(environmentId, source, { cursor: 1, expectedSnapshot: first.snapshotId }),
  ).toMatchObject({ reason: "response_too_large" });
  expect(pageStorageCleanupReport(environmentId, report([large]), {})).toMatchObject({
    reason: "response_too_large",
  });
  const medium = { ...entry(3), reason: '"🦊"'.repeat(3000) };
  const bounded = page(report(Array.from({ length: 20 }, () => medium)), { limit: 20 });
  expect(bounded.report!.entries.length).toBeGreaterThan(0);
  expect(bounded.report!.entries.length).toBeLessThan(20);
  expect(bounded.report!.entries.every((value) => value.reason === medium.reason)).toBe(true);
  expect(
    Buffer.byteLength(
      JSON.stringify({
        content: [{ type: "text", text: JSON.stringify(bounded) }],
        structuredContent: bounded,
        isError: false,
      }),
      "utf8",
    ),
  ).toBeLessThanOrEqual(MAX_STORAGE_CLEANUP_REPORT_RESPONSE_BYTES);
  expect(pageStorageCleanupReport(environmentId, report([]), {
    worktreePath: "x".repeat(MAX_STORAGE_CLEANUP_REPORT_RESPONSE_BYTES),
  })).toMatchObject({ reason: "response_too_large" });
});

it("decodes bounded numeric cursors, limits and SHA-256 snapshots", () => {
  const decode = Schema.decodeUnknownSync(StorageCleanupReportReadInput);
  expect(
    decode({ cursor: Number.MAX_SAFE_INTEGER, limit: 50, expectedSnapshot: "a".repeat(64) }),
  ).toMatchObject({ cursor: Number.MAX_SAFE_INTEGER });
  for (const input of [
    { cursor: -1 },
    { cursor: 1.5 },
    { cursor: Number.MAX_SAFE_INTEGER + 1 },
    { limit: 0 },
    { limit: 51 },
    { limit: 1.5 },
    { expectedSnapshot: "a".repeat(63) },
    { expectedSnapshot: "a".repeat(64) + "\n" },
    { worktreePath: "" },
    { threadId: "" },
  ]) expect(() => decode(input)).toThrow();
});

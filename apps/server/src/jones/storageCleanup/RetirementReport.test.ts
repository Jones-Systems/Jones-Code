import { describe, expect, it } from "@effect/vitest";
import {
  ThreadId,
  type StorageCleanupReport,
  type StorageCleanupReportEntry,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { mergeRetirementReport, retainOwnerReport } from "./RetirementReport.ts";

const entry = (
  name: string,
  outcome: StorageCleanupReportEntry["outcome"],
  bytes: number | null = null,
): StorageCleanupReportEntry => ({
  kind: "worktree",
  outcome,
  reason: outcome === "kept" ? "Retirement proposed; confirmation is required." : outcome,
  path: `/synthetic/${name}`,
  projectPath: "/synthetic/project",
  threadId: ThreadId.make(`thread-${name}`),
  threadTitle: name,
  bytes,
  files: null,
});
const report = (
  operation: "propose" | "retire" | "cleanup",
  trigger: "manual" | "automatic",
  entries: ReadonlyArray<StorageCleanupReportEntry>,
  at: string,
  bytesFreed = 0,
): StorageCleanupReport => ({
  operation,
  trigger,
  startedAt: at,
  finishedAt: at,
  entries,
  counts: {
    removed: entries.filter((e) => e.outcome === "removed").length,
    kept: entries.filter((e) => e.outcome === "kept").length,
    failed: entries.filter((e) => e.outcome === "failed").length,
  },
  omittedCount: 0,
  bytesFreed,
});
const retire = { action: "retire" } as const;
const proposal = report(
  "propose",
  "manual",
  [entry("a", "kept"), entry("b", "kept"), entry("c", "kept")],
  "2026-11-17T10:00:00.000Z",
);

describe("retained retirement report", () => {
  it("retire merges into the retained proposal and keeps other entries", () => {
    const result = report(
      "retire",
      "manual",
      [entry("b", "removed", 4096)],
      "2026-11-17T10:05:00.000Z",
      4096,
    );
    const merged = mergeRetirementReport(retire, proposal, result);
    expect(merged.entries.map((e) => [e.threadTitle, e.outcome, e.bytes])).toEqual([
      ["a", "kept", null],
      ["b", "removed", 4096],
      ["c", "kept", null],
    ]);
    expect(merged.counts).toEqual({ removed: 1, kept: 2, failed: 0 });
    expect(merged.operation).toBe("retire");
    expect(merged.startedAt).toBe(result.startedAt);
    expect(merged.finishedAt).toBe(result.finishedAt);
    expect(merged.bytesFreed).toBe(4096);
  });

  it("accumulates freed bytes across successive retires and appends unseen entries", () => {
    const first = mergeRetirementReport(
      retire,
      proposal,
      report("retire", "manual", [entry("a", "removed", 100)], "2026-11-17T10:05:00.000Z", 100),
    );
    const second = mergeRetirementReport(
      retire,
      first,
      report(
        "retire",
        "manual",
        [entry("b", "removed", 50), entry("d", "kept")],
        "2026-11-17T10:06:00.000Z",
        50,
      ),
    );
    expect(second.entries.map((e) => [e.threadTitle, e.outcome])).toEqual([
      ["a", "removed"],
      ["b", "removed"],
      ["c", "kept"],
      ["d", "kept"],
    ]);
    expect(second.counts).toEqual({ removed: 2, kept: 2, failed: 0 });
    expect(second.bytesFreed).toBe(150);
  });

  it("does not merge into automatic, cleanup or missing reports, or merge a proposal", () => {
    const result = report("retire", "manual", [entry("b", "removed", 1)], "t", 1);
    expect(mergeRetirementReport(retire, null, result)).toBe(result);
    expect(
      mergeRetirementReport(
        retire,
        report("cleanup", "automatic", [entry("a", "kept")], "t"),
        result,
      ),
    ).toBe(result);
    expect(
      mergeRetirementReport(retire, report("cleanup", "manual", [entry("a", "kept")], "t"), result),
    ).toBe(result);
    const nextProposal = report("propose", "manual", [entry("a", "kept")], "t2");
    expect(mergeRetirementReport({ action: "propose" }, proposal, nextProposal)).toBe(nextProposal);
  });

  it.effect("retains and returns the merged report", () =>
    Effect.gen(function* () {
      const ref = yield* SubscriptionRef.make<StorageCleanupReport | null>(proposal);
      const result = report("retire", "manual", [entry("c", "failed")], "2026-11-17T10:07:00.000Z");
      const returned = yield* retainOwnerReport(ref, retire, result);
      expect(returned.entries).toHaveLength(3);
      expect(yield* SubscriptionRef.get(ref)).toBe(returned);
    }),
  );
});

import type {
  StorageCleanupReport,
  StorageCleanupReportEntry,
  WorkspaceRetirementAction,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as SubscriptionRef from "effect/SubscriptionRef";

const entryKey = (entry: StorageCleanupReportEntry) =>
  `${entry.threadId ?? ""}\u0000${entry.path ?? ""}`;

const removedBytes = (entry: StorageCleanupReportEntry) =>
  entry.outcome === "removed" ? (entry.bytes ?? 0) : 0;

/**
 * Fold one owner retire into the retained manual report so the other reviewed
 * proposals stay listed. The retire's timestamps replace the retained ones so
 * a client waiting on that retire sees its own completion. A report from an
 * automatic run, a proposal, or no report at all is not merged into.
 */
export function mergeRetirementReport(
  input: WorkspaceRetirementAction,
  retained: StorageCleanupReport | null,
  retire: StorageCleanupReport,
): StorageCleanupReport {
  if (
    input.action !== "retire" ||
    retained === null ||
    retained.trigger !== "manual" ||
    (retained.operation !== "propose" && retained.operation !== "retire")
  )
    return retire;
  const replacements = new Map(retire.entries.map((entry) => [entryKey(entry), entry]));
  const placed = new Set<string>();
  let replacedFreed = 0;
  const entries = retained.entries.map((entry) => {
    const key = entryKey(entry);
    const replacement = replacements.get(key);
    if (replacement === undefined) return entry;
    placed.add(key);
    replacedFreed += removedBytes(entry);
    return replacement;
  });
  for (const entry of retire.entries) if (!placed.has(entryKey(entry))) entries.push(entry);
  return {
    ...retire,
    entries,
    counts: {
      removed: entries.filter((entry) => entry.outcome === "removed").length,
      kept: entries.filter((entry) => entry.outcome === "kept").length,
      failed: entries.filter((entry) => entry.outcome === "failed").length,
    },
    omittedCount: Math.max(retained.omittedCount, retire.omittedCount),
    bytesFreed: Math.max(0, retained.bytesFreed - replacedFreed) + retire.bytesFreed,
  };
}

/** Atomically retain a finished owner action's report and return what was retained. */
export const retainOwnerReport = (
  reportRef: SubscriptionRef.SubscriptionRef<StorageCleanupReport | null>,
  input: WorkspaceRetirementAction,
  report: StorageCleanupReport,
) =>
  SubscriptionRef.modify(reportRef, (retained) => {
    const merged = mergeRetirementReport(input, retained, report);
    return [merged, merged] as const;
  }).pipe(Effect.withSpan("StorageCleanup.retainOwnerReport"));

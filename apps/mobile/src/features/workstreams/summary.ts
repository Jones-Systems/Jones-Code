import * as Schema from "effect/Schema";
import {
  WorkstreamsRollupRead,
  type WorkstreamsRollupSummary,
} from "@t3tools/contracts/jones/workstreamsRollups";
export type MobileSummary =
  | { readonly state: "loading" | "unavailable"; readonly message: string }
  | {
      readonly state: "ready";
      readonly summary: WorkstreamsRollupSummary;
      readonly freshness: "current" | "stale" | "unknown";
      readonly notice: string | null;
      readonly sections: readonly { readonly title: string; readonly items: readonly string[] }[];
    };
const isRead = Schema.is(WorkstreamsRollupRead);
export function projectSummary(workstreamId: string, value: unknown): MobileSummary {
  if (
    !isRead(value) ||
    value.state !== "ready" ||
    !value.summary ||
    value.summary.workstream_id !== workstreamId ||
    value.freshness === "inaccessible"
  )
    return { state: "unavailable", message: "Summary unavailable." };
  const { summary, freshness } = value;
  return {
    state: "ready",
    summary,
    freshness,
    notice:
      freshness === "stale"
        ? "This summary may be out of date."
        : freshness === "unknown"
          ? "Summary freshness is unknown."
          : null,
    sections: [
      { title: "Blockers", items: summary.result.blockers.map((s) => s.text) },
      { title: "Decisions", items: summary.result.decisions.map((s) => s.text) },
      { title: "Next steps", items: summary.result.next_steps.map((s) => s.text) },
      { title: "Follow-ups", items: summary.result.follow_up_candidates.map((s) => s.text) },
    ].filter((s) => s.items.length > 0),
  };
}
export async function loadSummary(
  workstreamId: string,
  read: (id: string, signal: AbortSignal) => Promise<unknown>,
  signal: AbortSignal,
): Promise<MobileSummary> {
  if (signal.aborted) return { state: "unavailable", message: "Summary unavailable." };
  try {
    const value = await read(workstreamId, signal);
    return signal.aborted
      ? { state: "unavailable", message: "Summary unavailable." }
      : projectSummary(workstreamId, value);
  } catch {
    return { state: "unavailable", message: "Summary unavailable." };
  }
}

import type { DesktopUpdateState } from "@t3tools/contracts";
import { jonesTargetStatus } from "@t3tools/client-runtime/jones/updates/target-status";

export function classifiedCheckFeedback(state: DesktopUpdateState): { type: "info" | "error"; title: string; description: string } | null {
  if (state.jones?.checkClassification === undefined) return null;
  const check = jonesTargetStatus({ state: state.jones, freshness: "fresh" }).check;
  if (check === undefined) return null;
  const failed = check.kind === "failure" || check.kind === "qualification-failed";
  return { type: failed ? "error" : "info",
    title: check.kind === "failure" ? "Could not check for updates" : check.kind === "qualification-pending" ? "Update still being qualified" : check.kind === "qualification-failed" ? "Update qualification failed" : "Update check finished",
    description: check.text };
}

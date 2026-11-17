import type { PromptItem } from "./promptItems";
import { destinationNeedsAttention, turnNeedsAttention } from "./promptPresentation";

/**
 * Whether a prompt needs the person, and which band it sorts into. Pending items also depend
 * on this session's action state, which the caller passes in; it is not part of the item.
 */

export type PromptBand = "needs-you" | "releasing" | "paused" | "waiting" | "other";
export interface PromptAttention {
  readonly needsYou: boolean;
  readonly band: PromptBand;
}
export interface PromptLocalAttention {
  readonly uncertain: boolean;
  /** This session holds the edit handle for the draft. */
  readonly ownsEdit: boolean;
}

const noLocalState: PromptLocalAttention = { uncertain: false, ownsEdit: false };
const waitingQueueStates: ReadonlySet<string> = new Set(["queued", "reserved", "waiting_remote"]);

export function promptAttention(
  item: PromptItem,
  local: PromptLocalAttention = noLocalState,
): PromptAttention {
  if (item.stage === "pending") {
    const draft = item.draft;
    const needsYou =
      local.uncertain ||
      draft?.state === "blocked" ||
      draft?.routing_state === "owner_required" ||
      (draft?.state === "editing" && !local.ownsEdit);
    if (needsYou) return { needsYou: true, band: "needs-you" };
    if (draft?.state === "held") return { needsYou: false, band: "releasing" };
    if (draft?.state === "paused" || draft?.state === "editing")
      return { needsYou: false, band: "paused" };
    return { needsYou: false, band: "other" };
  }
  if (item.stage === "queued") {
    if (item.destinations.some(destinationNeedsAttention))
      return { needsYou: true, band: "needs-you" };
    if (
      item.destinations.some(
        (destination) =>
          destination.queueItem !== null &&
          waitingQueueStates.has(destination.queueItem.queue_state),
      )
    )
      return { needsYou: false, band: "waiting" };
    return { needsYou: false, band: "other" };
  }
  return item.destinations.some((destination) =>
    turnNeedsAttention(destination.handoff?.providerState),
  )
    ? { needsYou: true, band: "needs-you" }
    : { needsYou: false, band: "other" };
}

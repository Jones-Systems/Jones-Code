import type {
  VoiceReviewDraft,
  WorkQueueAssignment,
  WorkQueueMetadataResult,
} from "@t3tools/contracts";
import type { PromptDestination, PromptItem, PromptTextState } from "./promptItems";

/**
 * Plain-language labels for Prompts. Every function is total: an unrecognized value returns a
 * fallback that carries the raw value, so a newer producer never blanks or breaks the page.
 */

export type PromptTone = "neutral" | "info" | "warning" | "success";
export interface PromptStatus {
  readonly label: string;
  readonly tone: PromptTone;
  /** One sentence shown above the actions, saying what the state means. */
  readonly explanation: string | null;
}

function lookup(labels: Readonly<Record<string, string>>, value: string) {
  return Object.hasOwn(labels, value) ? labels[value]! : null;
}
function unrecognized(kind: string, value: string | null | undefined) {
  return `Unrecognized ${kind} (${value ?? "missing"})`;
}

export const NEEDS_DESTINATION =
  "Needs a destination. The voice source could not choose where this goes, and Prompts cannot set one. You can edit the prompt, send it again, or delete it.";

const blockedReasons: Readonly<Record<string, string>> = {
  source_unavailable: "Voice source unavailable",
  grant_denied: "Access not granted",
  backend_unavailable: "Destination server unavailable",
  capacity: "Destination at capacity",
  route_required: "Needs a destination",
  awaiting_dependency: "Waiting on an earlier prompt",
  owner_required: "Needs your decision",
};
export function blockedReasonLabel(reason: string | null | undefined) {
  if (reason === null || reason === undefined) return "Reason not reported";
  return lookup(blockedReasons, reason) ?? unrecognized("reason", reason);
}

const draftStates: Readonly<Record<string, string>> = {
  held: "Held",
  paused: "Paused",
  editing: "Editing",
  blocked: "Blocked",
  released: "Released",
  deleted: "Deleted",
  expired: "Expired",
};
export function draftStateLabel(state: string | null | undefined) {
  return state ? (lookup(draftStates, state) ?? unrecognized("state", state)) : "State unknown";
}

export interface DraftLocalState {
  /** Whole seconds left on the release countdown, or null when the source reports none. */
  readonly seconds: number | null;
  readonly busy: boolean;
  readonly uncertain: boolean;
  readonly conflict: boolean;
  /** This session holds the edit handle. */
  readonly ownsEdit: boolean;
  /** The server is disconnected or the source failed its last read. */
  readonly unavailable: boolean;
}

/** Badge and explanation for a pending draft, in the precedence the page acts on. */
export function draftStatus(draft: VoiceReviewDraft, local: DraftLocalState): PromptStatus {
  if (local.unavailable)
    return {
      label: "Last known",
      tone: "neutral",
      explanation: "Server disconnected. Actions are unavailable.",
    };
  if (local.busy) return { label: "Working…", tone: "neutral", explanation: null };
  if (local.uncertain)
    return {
      label: "Outcome unknown",
      tone: "warning",
      explanation: "The last action may or may not have applied. Check before acting again.",
    };
  if (local.conflict)
    return {
      label: "Changed elsewhere",
      tone: "warning",
      explanation: "This prompt changed. Your unsaved text is kept.",
    };
  switch (draft.state) {
    case "held":
      return local.seconds === null
        ? { label: "Held", tone: "info", explanation: "Waiting to release." }
        : {
            label: `Releasing in ${local.seconds}s`,
            tone: "info",
            explanation: "Releases to the queue when the countdown ends.",
          };
    case "paused":
      return {
        label: local.seconds === null ? "Paused" : `Paused · ${local.seconds}s left`,
        tone: "neutral",
        explanation: "Will not release until you resume or send.",
      };
    case "editing":
      return local.ownsEdit
        ? { label: "Editing", tone: "neutral", explanation: "Release is held while you edit." }
        : {
            label: "Being edited elsewhere",
            tone: "warning",
            explanation: "Another tab or device holds this edit. Pause releases an abandoned hold.",
          };
    case "blocked":
      return {
        label: `Blocked · ${blockedReasonLabel(draft.reason)}`,
        tone: "warning",
        explanation:
          draft.reason === "route_required" || draft.reason === "owner_required"
            ? NEEDS_DESTINATION
            : "Release is blocked. Correct the cause, then resume or send.",
      };
    case "released":
      return {
        label: "Released",
        tone: "neutral",
        explanation: "Released to the queue. It moves to Queued on the next read.",
      };
    case "deleted":
      return { label: "Deleted", tone: "neutral", explanation: null };
    case "expired":
      return { label: "Expired", tone: "neutral", explanation: null };
    default:
      return { label: draftStateLabel(draft.state), tone: "warning", explanation: null };
  }
}

export interface RoutingLabel {
  readonly label: string;
  readonly detail: string | null;
}
export function routingStateLabel(
  state: string | null | undefined,
  target: string | null | undefined,
  dependencies: readonly string[] = [],
): RoutingLabel {
  const destination = target ?? "destination not reported";
  switch (state) {
    case undefined:
    case null:
      return { label: "Destination not observed yet", detail: null };
    case "unclassified":
      return { label: "Not routed yet", detail: null };
    case "classifying":
      return { label: "Choosing a destination…", detail: null };
    case "proposed":
      return { label: `Proposed: ${destination}`, detail: "Will go here on release." };
    case "frozen":
      return { label: `Destination locked: ${destination}`, detail: null };
    case "awaiting_dependency":
      return {
        label:
          dependencies.length === 1
            ? "Waiting on 1 earlier prompt"
            : dependencies.length > 1
              ? `Waiting on ${dependencies.length} earlier prompts`
              : "Waiting on an earlier prompt",
        detail: null,
      };
    case "owner_required":
      return {
        label: "Needs your decision",
        detail: "The voice source could not choose a destination. This page cannot set one.",
      };
    default:
      return { label: unrecognized("routing state", state), detail: null };
  }
}

const queueStates: Readonly<Record<string, { label: string; tone: PromptTone }>> = {
  queued: { label: "Queued", tone: "neutral" },
  reserved: { label: "Picked up", tone: "info" },
  waiting_remote: { label: "Waiting on destination", tone: "info" },
  held: { label: "Held in queue", tone: "warning" },
  reconciliation_required: { label: "Needs reconciliation", tone: "warning" },
  observed_terminal: { label: "Ended in queue · handoff not confirmed", tone: "neutral" },
  cancelled: { label: "Cancelled", tone: "neutral" },
  unknown: { label: "State unknown", tone: "warning" },
};
export function queueStateLabel(state: string | null | undefined): {
  label: string;
  tone: PromptTone;
} {
  if (!state) return { label: "Queue intake not observed", tone: "neutral" };
  return Object.hasOwn(queueStates, state)
    ? queueStates[state]!
    : { label: unrecognized("queue state", state), tone: "warning" };
}

const turnStates: Readonly<Record<string, string>> = {
  pending: "Turn not started",
  running: "Agent working",
  completed: "Turn completed",
  interrupted: "Turn interrupted",
  error: "Turn failed",
};
export function turnStateLabel(state: string | null | undefined) {
  if (!state) return "Not observed";
  return lookup(turnStates, state) ?? unrecognized("turn state", state);
}
export function turnNeedsAttention(state: string | null | undefined) {
  return state === "error" || state === "interrupted";
}

const requestKinds: Readonly<Record<string, string>> = {
  initial: "Initial",
  owner_followup: "Follow-up",
  auto_continue: "Auto-continue",
  bootstrap: "Bootstrap",
};
export function requestKindLabel(kind: string | null | undefined) {
  if (!kind) return "Queue request";
  return lookup(requestKinds, kind) ?? unrecognized("request kind", kind);
}

const queueUnavailable: Readonly<Record<string, string>> = {
  source_unavailable: "the source cannot be read",
  invalid_artifact: "the source data is invalid",
  source_mismatch: "the sample does not match the configured source",
  future_sample: "the sample timestamp is in the future",
  invalid_configuration: "the source configuration is invalid",
};
export function queueUnavailableMessage(
  reason: Extract<WorkQueueMetadataResult, { status: "unavailable" }>["reason"] | (string & {}),
) {
  return lookup(queueUnavailable, reason) ?? `the source reported ${reason}`;
}

const assignmentStates: Readonly<Record<WorkQueueAssignment["state"], string>> = {
  issued: "Issued",
  pending: "Pending",
  satisfied: "Satisfied",
  conflict: "Conflict",
  rejected: "Rejected",
  unavailable: "Unavailable",
  unsupported: "Unsupported",
  permanent: "Permanent hold",
  excluded: "Excluded",
  cancelled: "Cancelled",
};
export function assignmentStateLabel(state: string) {
  return lookup(assignmentStates, state) ?? unrecognized("assignment state", state);
}

const assignmentReasons: Readonly<Record<WorkQueueAssignment["reason"], string>> = {
  assignment_pending: "Assignment pending",
  assignment_satisfied: "Assignment satisfied",
  assignment_conflict: "Thread belongs to another workstream",
  assignment_slot_excluded: "Thread excluded from this assignment",
  assignment_unsupported: "Assignment is not supported",
  assignment_cancelled: "Assignment cancelled",
  "assignment_rejected:foreign_thread": "Thread belongs to another environment",
  "assignment_rejected:thread_not_found": "Thread not found",
  "assignment_rejected:registry_mismatch": "Workstream registry does not match",
  "assignment_rejected:permission_denied": "Assignment permission denied",
  "assignment_rejected:workstream_not_found": "Workstream not found",
  "assignment_rejected:identity_conflict": "Thread identity conflicts",
  "assignment_rejected:verification_rejected": "Thread verification rejected",
  "assignment_rejected:registry_rejected": "Workstream registry rejected the assignment",
  "assignment_rejected:operation_id_conflict": "Assignment operation ID conflicts",
  "assignment_unavailable:not_configured": "Workstreams are not configured",
  "assignment_unavailable:offline": "Workstreams are offline",
  "assignment_unavailable:registration_unavailable": "Thread registration unavailable",
  "assignment_unavailable:placements_not_ready": "Thread assignments are not ready",
  "assignment_unavailable:contention": "Another assignment is in progress",
  "assignment_unavailable:backpressure": "Assignment service is busy",
  "assignment_unavailable:verification_pending": "Thread verification pending",
  "assignment_unavailable:verification_unresolved": "Thread verification unresolved",
  "assignment_unavailable:destination_scan_bound": "Destination could not be fully checked",
};
export function assignmentReasonLabel(reason: string) {
  return lookup(assignmentReasons, reason) ?? unrecognized("assignment reason", reason);
}

/**
 * One line for a v4 assignment result. `name` resolves a queue workstream ID for display; the
 * caller keeps the same-environment rule, so an unresolved ID is shown as itself.
 */
export function assignmentSummary(
  assignment: Pick<
    WorkQueueAssignment,
    "state" | "reason" | "workstream_id" | "observed_workstream_id"
  >,
  name: (workstreamId: string) => string,
) {
  const desired = name(assignment.workstream_id);
  switch (assignment.state) {
    case "satisfied":
      return `In ${desired}`;
    case "pending":
    case "issued":
      return `Assigning to ${desired}…`;
    case "conflict":
      return assignment.observed_workstream_id === null
        ? `Thread is in another workstream; the queue asked for ${desired}`
        : `Thread is in ${name(assignment.observed_workstream_id)}; the queue asked for ${desired}`;
    case "rejected":
    case "unavailable":
    case "unsupported":
      return `Could not assign: ${assignmentReasonLabel(assignment.reason)}`;
    default:
      return `${assignmentStateLabel(assignment.state)}: ${assignmentReasonLabel(assignment.reason)}`;
  }
}

export function textStateLabel(text: PromptTextState) {
  switch (text.kind) {
    case "available":
      return text.text;
    case "deleted":
      return "Text deleted";
    case "expired":
      return "Text expired";
    case "not_carried":
      return "Text not available for this request";
    default:
      return "Prompt text is unavailable.";
  }
}

/** The assignment carried by a v4 queue item, when the snapshot schema has one. */
export function destinationAssignment(destination: PromptDestination | null) {
  const item = destination?.queueItem;
  return item && "assignment" in item ? item.assignment : undefined;
}

export function destinationNeedsAttention(destination: PromptDestination) {
  const item = destination.queueItem;
  const assignment = destinationAssignment(destination);
  return (
    item?.queue_state === "held" ||
    item?.queue_state === "reconciliation_required" ||
    item?.queue_state === "unknown" ||
    item?.dispatch_status === "rejected" ||
    assignment?.state === "conflict" ||
    assignment?.state === "rejected"
  );
}

/** Badge for a queued or sent item. Pending items use `draftStatus`. */
export function itemStatus(item: PromptItem): PromptStatus {
  if (item.stage === "sent") {
    const turn = item.destinations.find((destination) =>
      turnNeedsAttention(destination.handoff?.providerState),
    );
    return {
      label: "Handed off",
      tone: turn ? "warning" : "success",
      explanation: null,
    };
  }
  if (item.stage === "queued") {
    const destination =
      item.destinations.find(destinationNeedsAttention) ??
      item.destinations.find((candidate) => candidate.queueItem !== null) ??
      null;
    const explanation =
      "Queued items are read-only here. Accepted dispatch does not prove a handoff.";
    if (!destination?.queueItem)
      return {
        label: "Released · queue intake not observed",
        tone: "neutral",
        explanation,
      };
    const state = queueStateLabel(destination.queueItem.queue_state);
    return { ...state, explanation };
  }
  return item.draft
    ? { label: draftStateLabel(item.draft.state), tone: "neutral", explanation: null }
    : { label: "State unknown", tone: "warning", explanation: null };
}

export type PromptStepStatus = "observed" | "current" | "not_observed" | "not_applicable";
export interface PromptStep {
  readonly key: "captured" | "held" | "released" | "queued" | "handoff" | "turn" | "finished";
  readonly label: string;
  readonly status: PromptStepStatus;
  readonly detail: string;
  readonly time: number | null;
}

/**
 * The progress rail for one destination of an item. Each step reports only its own evidence:
 * a later step being observed never marks an earlier one as observed. The last line is fixed.
 */
export function promptProgressSteps(
  item: PromptItem,
  destination: PromptDestination | null,
  pendingStatus?: PromptStatus,
): readonly PromptStep[] {
  const draft = item.draft;
  const created = draft ? Date.parse(draft.created_at) : Number.NaN;
  const released = draft !== null && (draft.state === "released" || draft.command_id !== null);
  const queueItem = destination?.queueItem ?? null;
  const handoff = destination?.handoff ?? null;
  const queueState =
    queueItem?.queue_state === "observed_terminal" && handoff
      ? "Queue entry ended"
      : queueStateLabel(queueItem?.queue_state).label;
  const notVoice = "Not a voice prompt";
  return [
    {
      key: "captured",
      label: "Captured",
      status: draft ? "observed" : "not_applicable",
      detail: draft ? "Spoken prompt received" : notVoice,
      time: Number.isFinite(created) ? created : null,
    },
    {
      key: "held",
      label: "Held for review",
      status: !draft
        ? "not_applicable"
        : item.stage === "pending" && !released
          ? "current"
          : "observed",
      detail: !draft
        ? notVoice
        : item.stage === "pending" && !released
          ? (pendingStatus?.label ?? draftStateLabel(draft.state))
          : draft.edited
            ? "Review hold ended · edited during review"
            : "Review hold ended",
      time: null,
    },
    {
      key: "released",
      label: "Released",
      status: !draft ? "not_applicable" : released ? "observed" : "not_observed",
      detail: !draft
        ? notVoice
        : released
          ? draft.command_status
            ? `Command ${draft.command_status}`
            : "Command status not observed"
          : "Not released",
      time: null,
    },
    {
      key: "queued",
      label: "In queue",
      status: queueItem ? "observed" : "not_observed",
      detail: queueItem
        ? `${queueState} · ${queueItem.lane === "high" ? "high" : "normal"} lane`
        : "Queue intake not observed",
      time: queueItem?.submitted_at_ms ?? null,
    },
    {
      key: "handoff",
      label: "Handed to thread",
      status: handoff ? "observed" : "not_observed",
      detail: handoff ? "Handoff confirmed" : "Handoff not confirmed in this sample",
      time: handoff?.sentAt ?? null,
    },
    {
      key: "turn",
      label: "Agent turn",
      status: handoff?.providerState ? "observed" : "not_observed",
      detail: turnStateLabel(handoff?.providerState),
      time: null,
    },
    {
      key: "finished",
      label: "Finished",
      status: "not_applicable",
      detail: "Not tracked",
      time: null,
    },
  ];
}

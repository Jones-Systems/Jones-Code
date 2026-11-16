import type { OrchestrationV2ThreadProjection } from "@t3tools/contracts";
import { derivePendingThreadRequests } from "@t3tools/client-runtime/state/thread-requests";
import {
  DESKTOP_THREAD_MONITOR_PROTOCOL,
  THREAD_MONITOR_LIMITS,
  decodeThreadMonitorFrame,
  type ThreadMonitorDetail,
} from "@t3tools/contracts/jones/desktopThreadMonitor";
import { findLatestProposedPlan, hasActionableProposedPlan } from "../../session-logic";
import type { MonitorSelection } from "./store";

export function projectMonitorDetail(input: {
  readonly selection: MonitorSelection;
  readonly revision: number;
  readonly projection: OrchestrationV2ThreadProjection | null;
  readonly unavailableReason: string | null;
  readonly planUnavailableReason: string | null;
  readonly childAttention: boolean;
}): ThreadMonitorDetail {
  const { selection, projection, revision } = input;
  const unavailable = input.unavailableReason;
  const detail: ThreadMonitorDetail = {
    ...selection,
    revision,
    entries: [],
    request: null,
    plan: null,
    unavailableReason: unavailable,
  };
  if (!projection) return detail;
  const requests = derivePendingThreadRequests(projection);
  const approval = requests.approvals[0];
  const question = requests.userInputs[0];
  const plan = findLatestProposedPlan(projection, projection.runs.at(-1)?.id);
  const ownRequest = approval !== undefined || question !== undefined;
  const childReason =
    input.childAttention && !ownRequest
      ? "A child thread needs attention. Open this thread to answer its approval or reach its question."
      : null;
  let next: ThreadMonitorDetail = {
    ...detail,
    entries: projection.messages.slice(-THREAD_MONITOR_LIMITS.detailEntries).map((message) => ({
      id: message.id,
      role: message.role,
      text: message.text.slice(0, THREAD_MONITOR_LIMITS.detailEntryCodeUnits),
      excerpt: message.text.length > THREAD_MONITOR_LIMITS.detailEntryCodeUnits,
    })),
    unavailableReason: unavailable ?? childReason,
    request: approval
      ? {
          kind: "approval",
          requestId: approval.requestId,
          prompt: approval.detail ?? approval.reason ?? "Approval requested",
          options: approval.options ?? [],
          availability: {
            available:
              unavailable === null &&
              approval.responseCapability === "live" &&
              approval.options !== undefined,
            reason:
              unavailable ??
              (approval.responseCapability !== "live"
                ? "This approval cannot be resumed"
                : approval.options === undefined
                  ? "Offered approval decisions are unavailable"
                  : null),
          },
        }
      : question
        ? {
            kind: "user_input",
            requestId: question.requestId,
            questions: question.questions.map((item) => ({
              id: item.id,
              header: item.header,
              question: item.question,
              options: item.options.map((option) => ({
                label: option.label,
                description: option.description,
                value: option.value ?? option.label,
              })),
              allowCustomAnswer: item.allowCustomAnswer !== false,
              multiSelect: item.multiSelect,
            })),
            availability: {
              available: unavailable === null && question.responseCapability !== "not_resumable",
              reason:
                unavailable ??
                (question.responseCapability === "not_resumable"
                  ? "This question cannot be resumed"
                  : null),
            },
          }
        : null,
    plan:
      hasActionableProposedPlan(plan) && plan
        ? {
            planId: plan.id,
            markdown: plan.planMarkdown,
            availability: {
              available: input.planUnavailableReason === null,
              reason: input.planUnavailableReason,
            },
          }
        : null,
  };
  try {
    decodeThreadMonitorFrame({
      schemaVersion: DESKTOP_THREAD_MONITOR_PROTOCOL,
      payload: { type: "detail", detail: next },
    });
  } catch {
    next = {
      ...detail,
      entries: next.entries.slice(-1),
      unavailableReason:
        "This request or plan exceeds the monitor’s supported bounds. Open it in Jones Code.",
    };
  }
  return next;
}

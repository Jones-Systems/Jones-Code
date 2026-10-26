import type { OrchestrationV2ThreadProjection } from "@t3tools/contracts";
import type { ThreadMonitorAction } from "@t3tools/contracts/jones/desktopThreadMonitor";
import { derivePendingThreadRequests } from "@t3tools/client-runtime/state/thread-requests";
import {
  buildPendingUserInputAnswers,
  type PendingUserInputDraftAnswer,
} from "../../pendingUserInput";

export function normalizeMonitorAnswers(
  projection: OrchestrationV2ThreadProjection,
  action: Extract<ThreadMonitorAction, { type: "respondToUserInput" }>,
) {
  const request = derivePendingThreadRequests(projection).userInputs.find(
    (request) => request.requestId === action.requestId,
  );
  if (!request || request.responseCapability === "not_resumable") return null;
  if (
    Object.keys(action.answers).some(
      (id) => !request.questions.some((question) => question.id === id),
    )
  )
    return null;
  const drafts: Record<string, PendingUserInputDraftAnswer> = {};
  for (const question of request.questions) {
    const answer = action.answers[question.id];
    if (answer === undefined) return null;
    if (Array.isArray(answer)) {
      if (
        !question.multiSelect ||
        answer.some(
          (value) => !question.options.some((option) => (option.value ?? option.label) === value),
        )
      )
        return null;
      drafts[question.id] = { selectedOptionValues: [...answer] };
    } else {
      if (typeof answer !== "string") return null;
      const option = question.options.find((option) => (option.value ?? option.label) === answer);
      if (!option && question.allowCustomAnswer === false) return null;
      drafts[question.id] = option ? { selectedOptionValues: [answer] } : { customAnswer: answer };
    }
  }
  return buildPendingUserInputAnswers(request.questions, drafts);
}

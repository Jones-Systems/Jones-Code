import type { OrchestrationV2ThreadProjection } from "@t3tools/contracts";
import type {
  ThreadMonitorAction,
  ThreadMonitorQuestion,
} from "@t3tools/contracts/jones/desktopThreadMonitor";
import { formatUserResponseChoiceWithNote } from "@t3tools/contracts/jones/userResponseRequest";
import { derivePendingThreadRequests } from "@t3tools/client-runtime/state/thread-requests";
import {
  buildPendingUserInputAnswers,
  type PendingUserInputDraftAnswer,
} from "../../pendingUserInput";

export function monitorInputDraft(
  question: Pick<ThreadMonitorQuestion, "options" | "multiSelect">,
  answer: string | ReadonlyArray<string> | undefined,
  notes: boolean,
): { selected: ReadonlyArray<string>; custom: string } {
  if (typeof answer !== "string") return { selected: answer ?? [], custom: "" };
  const exact = question.options.find((option) => option.value === answer);
  if (exact) return { selected: [exact.value], custom: "" };
  if (notes && !question.multiSelect) {
    // Values may contain the note delimiter; prefer the longest offered value.
    const option = question.options
      .toSorted((a, b) => b.value.length - a.value.length)
      .find((option) => answer.startsWith(`${option.value}\nNote: `));
    if (option) return { selected: [option.value], custom: answer.slice(option.value.length + 7) };
  }
  return { selected: [], custom: answer };
}

export function monitorInputChoice(
  question: ThreadMonitorQuestion,
  answer: string | ReadonlyArray<string> | undefined,
  value: string,
  notes: boolean,
): string | ReadonlyArray<string> {
  const draft = monitorInputDraft(question, answer, notes);
  if (!question.multiSelect)
    return notes ? formatUserResponseChoiceWithNote(value, draft.custom) : value;
  return draft.selected.includes(value)
    ? draft.selected.filter((entry) => entry !== value)
    : [...draft.selected, value];
}

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
      const parsed =
        request.responseCapability === "message" && !question.multiSelect
          ? monitorInputDraft(
              {
                ...question,
                options: question.options.map((option) => ({
                  ...option,
                  value: option.value ?? option.label,
                })),
              },
              answer,
              true,
            )
          : null;
      const option = question.options.find((option) => (option.value ?? option.label) === answer);
      if (question.allowCustomAnswer === false && (!option || (parsed?.custom.length ?? 0) > 0))
        return null;
      drafts[question.id] = parsed?.selected.length
        ? { selectedOptionValues: [...parsed.selected], customAnswer: parsed.custom }
        : option
          ? { selectedOptionValues: [answer] }
          : { customAnswer: answer };
    }
  }
  return buildPendingUserInputAnswers(request.questions, drafts, {
    notes: request.responseCapability === "message",
  });
}

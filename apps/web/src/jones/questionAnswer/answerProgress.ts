import type { UserInputQuestion } from "@t3tools/contracts";
import type { PendingUserInputDraftAnswer } from "./pendingAnswers";
export { derivePendingUserInputProgress, type PendingUserInputProgress, type PendingUserInputDraftAnswer } from "../../pendingUserInput";
export function setPendingUserInputCustomAnswer(
  draft: PendingUserInputDraftAnswer | undefined,
  customAnswer: string,
  question?: Pick<UserInputQuestion, "initialAnswer" | "multiSelect">,
  options: { notes?: boolean } = {},
): PendingUserInputDraftAnswer {
  const selectedOptionValues =
    !(options.notes && !question?.multiSelect) &&
    (question?.initialAnswer !== undefined || customAnswer.trim().length > 0)
      ? undefined
      : [...new Set(draft?.selectedOptionValues ?? [])];

  return {
    customAnswer,
    ...(selectedOptionValues && selectedOptionValues.length > 0 ? { selectedOptionValues } : {}),
  };
}

export { carryDisplacedCustomAnswerIntoPrompt, withdrawDisplacedCustomAnswerFromPrompt } from "../../pendingUserInput";
export { buildPendingUserInputAnswers, togglePendingUserInputOptionSelection } from "./pendingAnswers";

export { projectPendingUserInputAnswer } from "./threadReferenceAnswer";

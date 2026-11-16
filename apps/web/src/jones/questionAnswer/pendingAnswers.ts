import type { UserInputQuestion } from "@t3tools/contracts";
import { formatUserResponseChoiceWithNote } from "@t3tools/contracts/jones/userResponseRequest";
import {
  normalizeSelectedOptionValues,
  resolvePendingUserInputAnswer as resolveUpstreamAnswer,
  setPendingUserInputCustomAnswer as setUpstreamAnswer,
  togglePendingUserInputOptionSelection as toggleUpstreamOption,
  type PendingUserInputDraftAnswer,
  type PendingUserInputAnswer,
} from "@t3tools/client-runtime/state/pending-user-input";
export type { PendingUserInputDraftAnswer, PendingUserInputAnswer };
export { normalizeSelectedOptionValues };

export function resolvePendingUserInputAnswer(
  question: UserInputQuestion,
  draft: PendingUserInputDraftAnswer | undefined,
  options: { notes?: boolean } = {},
): PendingUserInputAnswer | null {
  if (draft?.attachmentsBlocked) return null;
  if (options.notes && !question.multiSelect) {
    const choice = normalizeSelectedOptionValues(question, draft?.selectedOptionValues)[0];
    if (choice !== undefined)
      return formatUserResponseChoiceWithNote(
        choice,
        question.allowCustomAnswer === false ? "" : (draft?.customAnswer ?? ""),
      );
  }
  return resolveUpstreamAnswer(question, draft);
}
export function setPendingUserInputCustomAnswer(
  question: UserInputQuestion,
  draft: PendingUserInputDraftAnswer | undefined,
  customAnswer: string,
  options: { notes?: boolean } = {},
): PendingUserInputDraftAnswer {
  if (options.notes && !question.multiSelect && question.allowCustomAnswer !== false)
    return { ...draft, customAnswer };
  return setUpstreamAnswer(question, draft, customAnswer);
}
export function togglePendingUserInputOptionSelection(
  question: UserInputQuestion,
  draft: PendingUserInputDraftAnswer | undefined,
  optionValue: string,
  options: { notes?: boolean } = {},
): PendingUserInputDraftAnswer {
  if (
    options.notes &&
    !question.multiSelect &&
    normalizeSelectedOptionValues(question, draft?.selectedOptionValues).includes(optionValue)
  )
    return { ...draft, selectedOptionValues: [] };
  const next = toggleUpstreamOption(question, draft, optionValue);
  return options.notes && !question.multiSelect && question.allowCustomAnswer !== false
    ? { ...next, customAnswer: draft?.customAnswer ?? "" }
    : next;
}
export function buildPendingUserInputAnswers(
  questions: ReadonlyArray<UserInputQuestion>,
  drafts: Record<string, PendingUserInputDraftAnswer>,
  options: { notes?: boolean } = {},
): Record<string, PendingUserInputAnswer> | null {
  const answers: Record<string, PendingUserInputAnswer> = {};
  for (const question of questions) {
    const answer = resolvePendingUserInputAnswer(question, drafts[question.id], options);
    if (answer === null) return null;
    answers[question.id] = answer;
  }
  return answers;
}

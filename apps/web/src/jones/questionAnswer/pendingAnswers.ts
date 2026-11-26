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
  options?: { notes?: boolean },
): PendingUserInputDraftAnswer;
export function setPendingUserInputCustomAnswer(
  draft: PendingUserInputDraftAnswer | undefined,
  customAnswer: string,
  question?: Pick<UserInputQuestion, "initialAnswer" | "multiSelect">,
  options?: { notes?: boolean },
): PendingUserInputDraftAnswer;
export function setPendingUserInputCustomAnswer(
  questionOrDraft: UserInputQuestion | PendingUserInputDraftAnswer | undefined,
  draftOrAnswer: PendingUserInputDraftAnswer | string | undefined,
  answerOrQuestion?: string | Pick<UserInputQuestion, "initialAnswer" | "multiSelect">,
  options: { notes?: boolean } = {},
): PendingUserInputDraftAnswer {
  // Retained callers pass the draft first; current upstream passes the question first.
  if (typeof draftOrAnswer === "string") {
    const draft = questionOrDraft as PendingUserInputDraftAnswer | undefined;
    const question = answerOrQuestion as
      | Pick<UserInputQuestion, "initialAnswer" | "multiSelect">
      | undefined;
    const selectedOptionValues =
      !(options.notes && !question?.multiSelect) &&
      (question?.initialAnswer !== undefined || draftOrAnswer.trim().length > 0)
        ? undefined
        : [...new Set(draft?.selectedOptionValues ?? [])];
    return {
      customAnswer: draftOrAnswer,
      ...(selectedOptionValues && selectedOptionValues.length > 0 ? { selectedOptionValues } : {}),
    };
  }
  const question = questionOrDraft as UserInputQuestion;
  const customAnswer = answerOrQuestion as string;
  if (options.notes && !question.multiSelect && question.allowCustomAnswer !== false)
    return { ...draftOrAnswer, customAnswer };
  return setUpstreamAnswer(question, draftOrAnswer, customAnswer);
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
    const draft = drafts[question.id];
    const answer = resolvePendingUserInputAnswer(question, draft, options);
    if (answer === null) {
      if (question.required === false && !draft?.attachmentsBlocked) continue;
      return null;
    }
    answers[question.id] = answer;
  }
  return answers;
}

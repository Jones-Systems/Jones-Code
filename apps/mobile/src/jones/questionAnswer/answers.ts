import {
  isPendingUserInputOptionSelected as upstreamOptionSelected,
  normalizeSelectedOptionValues,
  resolvePendingUserInputAnswer as upstreamAnswer,
  setPendingUserInputCustomAnswer as upstreamSetCustomAnswer,
  togglePendingUserInputOptionSelection as upstreamToggleOption,
  type PendingUserInputDraftAnswer,
} from "@t3tools/client-runtime/state/pending-user-input";
import type { ThreadUserInputQuestion } from "@t3tools/client-runtime/state/thread-requests";
import { formatUserResponseChoiceWithNote } from "@t3tools/contracts/jones/userResponseRequest";

export type { PendingUserInputDraftAnswer } from "@t3tools/client-runtime/state/pending-user-input";
type AnswerOptions = { readonly notes?: boolean };

export function resolvePendingUserInputAnswer(
  question: ThreadUserInputQuestion,
  draft: PendingUserInputDraftAnswer | undefined,
  options: AnswerOptions = {},
): string | ReadonlyArray<string> | null {
  if (draft?.attachmentsBlocked) return null;
  const selected = normalizeSelectedOptionValues(question, draft?.selectedOptionValues)[0];
  if (options.notes && !question.multiSelect && selected !== undefined) {
    const custom = upstreamAnswer(question, { ...draft, selectedOptionValues: [] });
    return formatUserResponseChoiceWithNote(selected, typeof custom === "string" ? custom : "");
  }
  return upstreamAnswer(question, draft);
}

export function setPendingUserInputCustomAnswer(
  question: ThreadUserInputQuestion,
  draft: PendingUserInputDraftAnswer | undefined,
  customAnswer: string,
  options: AnswerOptions = {},
): PendingUserInputDraftAnswer {
  const next = upstreamSetCustomAnswer(question, draft, customAnswer);
  if (!options.notes || question.multiSelect || question.allowCustomAnswer === false) return next;
  const selectedOptionValues = normalizeSelectedOptionValues(question, draft?.selectedOptionValues);
  return { ...next, ...(selectedOptionValues.length > 0 ? { selectedOptionValues } : {}) };
}

export function isPendingUserInputOptionSelected(
  question: ThreadUserInputQuestion,
  draft: PendingUserInputDraftAnswer | undefined,
  optionValue: string,
  options: AnswerOptions = {},
): boolean {
  return options.notes && !question.multiSelect
    ? upstreamOptionSelected(question, { ...draft, customAnswer: "" }, optionValue)
    : upstreamOptionSelected(question, draft, optionValue);
}

export function togglePendingUserInputOptionSelection(
  question: ThreadUserInputQuestion,
  draft: PendingUserInputDraftAnswer | undefined,
  optionValue: string,
  options: AnswerOptions = {},
): PendingUserInputDraftAnswer {
  if (!options.notes || question.multiSelect)
    return upstreamToggleOption(question, draft, optionValue);
  const value = normalizeSelectedOptionValues(question, [optionValue])[0];
  if (value === undefined) return draft ?? {};
  const selected = normalizeSelectedOptionValues(question, draft?.selectedOptionValues);
  return { ...draft, selectedOptionValues: selected.includes(value) ? [] : [value] };
}

export function buildPendingUserInputAnswers(
  questions: ReadonlyArray<ThreadUserInputQuestion>,
  drafts: Record<string, PendingUserInputDraftAnswer>,
  options: AnswerOptions = {},
): Record<string, string | ReadonlyArray<string>> | null {
  const answers: Record<string, string | ReadonlyArray<string>> = {};
  for (const question of questions) {
    const answer = resolvePendingUserInputAnswer(question, drafts[question.id], options);
    if (answer === null) return null;
    answers[question.id] = answer;
  }
  return answers;
}

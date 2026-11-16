import type { UserInputQuestion } from "@t3tools/contracts";
import { buildPendingUserInputAnswers, normalizeSelectedOptionValues, resolvePendingUserInputAnswer, type PendingUserInputAnswer, type PendingUserInputDraftAnswer } from "./pendingAnswers";
export type { PendingUserInputDraftAnswer } from "./pendingAnswers";
export interface PendingUserInputProgress {
  questionIndex: number;
  activeQuestion: UserInputQuestion | null;
  activeDraft: PendingUserInputDraftAnswer | undefined;
  selectedOptionValues: ReadonlyArray<string>;
  customAnswer: string;
  resolvedAnswer: PendingUserInputAnswer | null;
  usingCustomAnswer: boolean;
  answeredQuestionCount: number;
  answeredByIndex: boolean[];
  firstUnansweredIndex: number;
  isLastQuestion: boolean;
  isComplete: boolean;
  canAdvance: boolean;
}

function countAnsweredPendingUserInputQuestions(
  questions: ReadonlyArray<UserInputQuestion>,
  draftAnswers: Record<string, PendingUserInputDraftAnswer>,
  options: { notes?: boolean } = {},
): number {
  return questions.reduce((count, question) => {
    return resolvePendingUserInputAnswer(question, draftAnswers[question.id], options) !== null
      ? count + 1
      : count;
  }, 0);
}

export function derivePendingUserInputProgress(
  questions: ReadonlyArray<UserInputQuestion>,
  draftAnswers: Record<string, PendingUserInputDraftAnswer>,
  questionIndex: number,
  options: { notes?: boolean } = {},
): PendingUserInputProgress {
  const normalizedQuestionIndex =
    questions.length === 0 ? 0 : Math.max(0, Math.min(questionIndex, questions.length - 1));
  const activeQuestion = questions[normalizedQuestionIndex] ?? null;
  const activeDraft = activeQuestion ? draftAnswers[activeQuestion.id] : undefined;
  const resolvedAnswer = activeQuestion
    ? resolvePendingUserInputAnswer(activeQuestion, activeDraft, options)
    : null;
  const customAnswer =
    activeQuestion?.allowCustomAnswer === false ? "" : (activeDraft?.customAnswer ?? "");
  const answeredQuestionCount = countAnsweredPendingUserInputQuestions(
    questions,
    draftAnswers,
    options,
  );
  const isLastQuestion =
    questions.length === 0 ? true : normalizedQuestionIndex >= questions.length - 1;

  return {
    questionIndex: normalizedQuestionIndex,
    activeQuestion,
    activeDraft,
    selectedOptionValues: activeQuestion
      ? normalizeSelectedOptionValues(activeQuestion, activeDraft?.selectedOptionValues)
      : [],
    customAnswer,
    resolvedAnswer,
    usingCustomAnswer:
      (activeQuestion?.initialAnswer !== undefined ? customAnswer : customAnswer.trim()).length > 0,
    answeredQuestionCount,
    answeredByIndex: questions.map(
      (question) =>
        resolvePendingUserInputAnswer(question, draftAnswers[question.id], options) !== null,
    ),
    firstUnansweredIndex: questions.findIndex(
      (question) =>
        resolvePendingUserInputAnswer(question, draftAnswers[question.id], options) === null,
    ),
    isLastQuestion,
    isComplete: buildPendingUserInputAnswers(questions, draftAnswers, options) !== null,
    canAdvance: resolvedAnswer !== null,
  };
}

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

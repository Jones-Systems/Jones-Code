import type { ThreadUserInputQuestion } from "@t3tools/client-runtime/state/thread-requests";
import {
  resolvePendingUserInputAnswer,
  type PendingUserInputDraftAnswer,
} from "../../lib/threadActivity";

export function clampQuestionIndex(index: number, count: number): number {
  return Math.max(0, Math.min(index, count - 1));
}

export function sequentialQuestionProgress(
  questions: ReadonlyArray<ThreadUserInputQuestion>,
  drafts: Record<string, PendingUserInputDraftAnswer>,
  notes: boolean,
) {
  const answered = questions.map(
    (question) => resolvePendingUserInputAnswer(question, drafts[question.id], { notes }) !== null,
  );
  const firstUnansweredIndex = answered.indexOf(false);
  return {
    answered,
    firstUnansweredIndex,
    initialIndex: firstUnansweredIndex < 0 ? 0 : firstUnansweredIndex,
    answeredCount: answered.filter(Boolean).length,
    complete: firstUnansweredIndex < 0,
  };
}

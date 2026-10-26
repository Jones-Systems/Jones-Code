import { expect, it } from "vite-plus/test";
import { clampQuestionIndex, sequentialQuestionProgress } from "./sequentialQuestions";

it("starts at the first unanswered question, clamps jumps and derives the final gate with note semantics", () => {
  const questions = [1, 2, 3].map((i) => ({
    id: `q${i}`,
    header: `Question ${i}`,
    question: "Which?",
    multiSelect: false,
    options: [{ label: "Local", description: "This host" }],
  }));
  const drafts = {
    q1: { selectedOptionValues: ["Local"], customAnswer: "Note" },
    q3: { customAnswer: "Text" },
  };
  expect(sequentialQuestionProgress(questions, drafts, true)).toEqual({
    answered: [true, false, true],
    firstUnansweredIndex: 1,
    initialIndex: 1,
    answeredCount: 2,
    complete: false,
  });
  expect(
    sequentialQuestionProgress(questions, { ...drafts, q2: { customAnswer: "Done" } }, true),
  ).toMatchObject({ complete: true, initialIndex: 0, answeredCount: 3 });
  expect(sequentialQuestionProgress(questions, {}, true).initialIndex).toBe(0);
  expect(clampQuestionIndex(-1, 3)).toBe(0);
  expect(clampQuestionIndex(10, 3)).toBe(2);
  expect(clampQuestionIndex(0, 0)).toBe(0);
});

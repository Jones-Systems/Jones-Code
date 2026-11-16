import { expect, it } from "vite-plus/test";
import type { OrchestrationV2UserInputQuestion } from "@t3tools/contracts";
import { withApprovalChoices } from "./approvalChoices.ts";

const question: OrchestrationV2UserInputQuestion = {
  id: "response",
  header: "Action needed",
  question:
    "Approve one additional diagnostic run in one new 45-minute window?\n\nKeep all existing limits.",
  options: [],
  allowCustomAnswer: true,
  required: true,
};

it("adds explained approval choices to existing app-owned requests without rewriting them", () => {
  const result = withApprovalChoices(question, "user-response:existing");
  expect(result.options.map((option) => option.label)).toEqual(["Approve", "Deny"]);
  expect(result.options.every((option) => option.description !== option.label)).toBe(true);
  expect(result).toMatchObject({ ...question, options: result.options });
  expect(question.options).toEqual([]);
  expect(withApprovalChoices(result, "user-response:existing")).toBe(result);
});

it.each([
  "Please approve this bounded run.",
  "Do you approve the attached plan?",
  "**Approve** one bounded run?",
])("recognizes an explicit approval request: %s", (prompt) => {
  expect(
    withApprovalChoices({ ...question, question: prompt }, "user-response:new").options,
  ).toHaveLength(2);
});

it("preserves authored options, provider questions, editable text, and non-approval requests", () => {
  expect(withApprovalChoices(question, "provider:question")).toBe(question);
  for (const candidate of [
    { ...question, options: [{ label: "Approve once", description: "Only this attempt" }] },
    { ...question, multiSelect: true },
    { ...question, initialAnswer: "Approve something" },
    ...[
      "Review the plan.",
      "Explain how to approve a request.",
      "Which action needs approval?",
      "Open Browser.",
    ].map((prompt) => ({ ...question, question: prompt })),
  ]) {
    expect(withApprovalChoices(candidate, "user-response:existing")).toBe(candidate);
  }
});

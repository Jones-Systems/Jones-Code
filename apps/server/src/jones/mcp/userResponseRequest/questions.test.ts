import { expect, it } from "vite-plus/test";
import { normalizeUserResponseQuestions } from "./questions.ts";

it("preserves the pre-change legacy normalization, including multiline text", () => {
  for (const instruction of ["Review the plan.", "  Setup\n\n- detail\n**Question?**  "]) {
    expect(
      normalizeUserResponseQuestions({
        instruction,
        choices: ["Local", "Remote"],
        clientRequestId: "legacy",
      }),
    ).toEqual({
      question: {
        id: "response",
        header: "Action needed",
        question: instruction,
        options: [
          { label: "Local", description: "Local" },
          { label: "Remote", description: "Remote" },
        ],
        allowCustomAnswer: true,
        required: true,
      },
    });
  }
});
it("normalizes structured questions deterministically with stable IDs and descriptions", () => {
  const input = {
    clientRequestId: "batch",
    questions: [
      {
        header: " Scope ",
        prompt: " **Question?** ",
        choices: [{ label: " Local ", description: " On this host " }, { label: "Remote" }],
      },
      { prompt: "Next?" },
    ],
  };
  const result = normalizeUserResponseQuestions(input);
  expect(result).toEqual(normalizeUserResponseQuestions(input));
  expect(result.questions).toEqual([
    {
      id: "q1",
      header: "Scope",
      question: "**Question?**",
      options: [
        { label: "Local", description: "On this host" },
        { label: "Remote", description: "Remote" },
      ],
      allowCustomAnswer: true,
      required: true,
    },
    {
      id: "q2",
      header: "Question 2",
      question: "Next?",
      options: [],
      allowCustomAnswer: true,
      required: true,
    },
  ]);
  expect(result.question).toEqual(result.questions?.[0]);
});

import { expect, it } from "vite-plus/test";
import { normalizeUserResponseQuestions } from "./questions.ts";

it("supplies Done only for opted-in action requests and keeps explicit choices", () => {
  const input = { instruction: "Open the Browser panel.", clientRequestId: "action" };
  expect(normalizeUserResponseQuestions(input).question.options).toEqual([]);
  expect(normalizeUserResponseQuestions({ ...input, defaultDone: false })).toEqual(
    normalizeUserResponseQuestions(input),
  );
  expect(normalizeUserResponseQuestions({ ...input, defaultDone: true }).question).toMatchObject({
    options: [{ label: "Done", description: "Done" }],
    allowCustomAnswer: true,
  });
  expect(
    normalizeUserResponseQuestions({
      ...input,
      defaultDone: true,
      choices: ["Browser is open", "Still blocked"],
    }).question.options,
  ).toEqual([
    { label: "Browser is open", description: "Browser is open" },
    { label: "Still blocked", description: "Still blocked" },
  ]);
});

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

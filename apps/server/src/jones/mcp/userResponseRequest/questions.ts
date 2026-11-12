import type { OrchestrationV2UserInputQuestion } from "@t3tools/contracts";
import type { UserResponseRequestInput } from "@t3tools/contracts/jones/userResponseRequest";

export function normalizeUserResponseQuestions(input: UserResponseRequestInput): {
  question: OrchestrationV2UserInputQuestion;
  questions?: ReadonlyArray<OrchestrationV2UserInputQuestion>;
} {
  if (input.questions !== undefined) {
    const count = input.questions.length;
    const questions = input.questions.map((question, index) => ({
      id: `q${index + 1}`,
      header: question.header?.trim() ?? (count === 1 ? "Action needed" : `Question ${index + 1}`),
      question: question.prompt.trim(),
      options: (question.choices ?? []).map((choice) => ({
        label: choice.label.trim(),
        description: choice.description?.trim() ?? choice.label.trim(),
      })),
      allowCustomAnswer: true,
      required: true,
    }));
    return { question: questions[0]!, questions };
  }
  // Legacy retry identity includes the original instruction and repeated option descriptions.
  return {
    question: {
      id: "response",
      header: "Action needed",
      question: input.instruction!,
      options: (input.choices?.length
        ? input.choices
        : input.defaultDone
          ? ["Done"]
          : (input.choices ?? [])
      ).map((label) => ({ label, description: label })),
      allowCustomAnswer: true,
      required: true,
    },
  };
}

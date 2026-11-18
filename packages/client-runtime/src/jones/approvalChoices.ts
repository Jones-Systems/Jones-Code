import type { OrchestrationV2UserInputQuestion } from "@t3tools/contracts";

// Decorate old app-owned requests without changing persisted questions or retry identity.
export function withApprovalChoices(
  question: OrchestrationV2UserInputQuestion,
  requestId: string,
): OrchestrationV2UserInputQuestion {
  if (
    !requestId.startsWith("user-response:") ||
    question.options.length > 0 ||
    question.multiSelect ||
    question.initialAnswer !== undefined ||
    !/^(?:please\s+)?(?:approve\b|do you approve\b)/iu.test(
      question.question.trim().replace(/^(?:\*\*|__)/u, ""),
    )
  ) {
    return question;
  }
  return {
    ...question,
    options: [
      {
        label: "Approve",
        description: "Authorize only the action and limits described above.",
      },
      {
        label: "Deny",
        description: "Do not authorize the requested action; keep it on hold.",
      },
    ],
  };
}

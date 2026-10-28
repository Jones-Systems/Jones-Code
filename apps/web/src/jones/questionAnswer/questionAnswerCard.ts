import type { PendingUserInput } from "../../session-logic";
import type { PendingUserInputDraftAnswer, PendingUserInputProgress } from "../../pendingUserInput";

export const ANSWER_FIELD_MAX_LINES = 10;

export function questionSummaryLine(markdown: string): string {
  return (
    markdown
      .split(/\r?\n/u)
      .find((line) => line.trim().length > 0)
      ?.replace(/^[#*> -]+/u, "")
      .replace(/\*\*|__|`/gu, "")
      .trim() ?? ""
  );
}

// Question state belongs to the card; the main editor always owns the thread draft.
export const INERT_QUESTION_COMPOSER_PROPS = {
  pendingUserInputs: [] as PendingUserInput[],
  activePendingProgress: null,
  activePendingResolvedAnswers: null,
  activePendingIsResponding: false,
  activePendingDraftAnswers: {} as Record<string, PendingUserInputDraftAnswer>,
  activePendingQuestionIndex: 0,
};

export function pendingQuestionBlocksSteering(
  inputs: ReadonlyArray<Pick<PendingUserInput, "responseCapability">>,
): boolean {
  return inputs.some((input) => input.responseCapability === "live");
}

export function deriveQuestionAnswerFooterState(input: {
  progress: PendingUserInputProgress;
  prompt: Pick<PendingUserInput, "responseCapability" | "dismissible">;
  disabled: boolean;
  isResponding: boolean;
}) {
  const { progress, prompt, disabled, isResponding } = input;
  const unavailable = prompt.responseCapability === "not_resumable";
  const fieldDisabled = disabled || isResponding || unavailable;
  const notes = prompt.responseCapability === "message" && !progress.activeQuestion?.multiSelect;
  const hasChoice = progress.selectedOptionValues.length > 0;
  const blockedReason =
    progress.isLastQuestion && !progress.isComplete
      ? `Answer question ${progress.firstUnansweredIndex + 1} to send · ${progress.answeredQuestionCount} of ${progress.answeredByIndex.length} answered`
      : null;
  return {
    notes,
    hasChoice,
    fieldLabel: notes && hasChoice ? "Note (optional): sent with your choice" : "Your answer",
    blockedReason,
    showField: progress.activeQuestion?.allowCustomAnswer !== false,
    fieldDisabled,
    placeholder: progress.activeQuestion?.options.length
      ? notes && hasChoice
        ? "Add a note (optional)"
        : "Or write your own response"
      : "Write your response",
    primaryLabel: isResponding ? "Sending…" : progress.isLastQuestion ? "Send" : "Continue",
    primaryDisabled:
      fieldDisabled || !progress.canAdvance || (progress.isLastQuestion && !progress.isComplete),
    showBack: progress.questionIndex > 0,
    literal: progress.activeQuestion?.initialAnswer !== undefined,
    notice: unavailable
      ? "This question can no longer be answered; stop or restart the run."
      : progress.activeDraft?.attachmentsBlocked
        ? "Wait for attachments to finish uploading, or remove unavailable attachments."
        : null,
  };
}

export function isQuestionAnswerSubmitKey(
  event: Pick<
    KeyboardEvent,
    "key" | "shiftKey" | "altKey" | "ctrlKey" | "metaKey" | "isComposing" | "keyCode" | "repeat"
  >,
): boolean {
  return (
    event.key === "Enter" &&
    !event.shiftKey &&
    !event.altKey &&
    !event.isComposing &&
    event.keyCode !== 229 &&
    !event.repeat
  );
}

export interface QuestionAnswerAttachmentCapabilities {
  supportsQuestionAttachments: boolean;
  attachmentUploadsCapabilityKnown: boolean;
  supportsAttachmentUploads: boolean;
  maxFileAttachmentBytes: number | null;
}

export const NO_QUESTION_ATTACHMENT_CAPABILITIES: QuestionAnswerAttachmentCapabilities = {
  supportsQuestionAttachments: false,
  attachmentUploadsCapabilityKnown: false,
  supportsAttachmentUploads: false,
  maxFileAttachmentBytes: null,
};

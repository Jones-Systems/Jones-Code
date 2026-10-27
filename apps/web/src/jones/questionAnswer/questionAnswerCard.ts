import type { PendingUserInput } from "../../session-logic";
import type { PendingUserInputDraftAnswer, PendingUserInputProgress } from "../../pendingUserInput";

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
  return {
    showField: progress.activeQuestion?.allowCustomAnswer !== false,
    fieldDisabled,
    placeholder: progress.activeQuestion?.options.length
      ? "Or write your own response"
      : "Write your response",
    primaryLabel: isResponding ? "Sending…" : progress.isLastQuestion ? "Send" : "Next",
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
  literal: boolean,
): boolean {
  return (
    event.key === "Enter" &&
    !event.shiftKey &&
    !event.altKey &&
    !event.isComposing &&
    event.keyCode !== 229 &&
    !event.repeat &&
    (!literal || event.ctrlKey || event.metaKey)
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

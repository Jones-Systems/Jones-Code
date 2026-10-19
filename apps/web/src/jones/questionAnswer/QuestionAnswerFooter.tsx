import { useLayoutEffect, useRef } from "react";
import type { RuntimeRequestId, ScopedThreadRef } from "@t3tools/contracts";
import { Button } from "../../components/ui/button";
import { ComposerBanner } from "../../components/chat/ComposerBanner";
import {
  threadContextDropTargetProps,
  useThreadContextDrop,
} from "../../components/chat/threadContextDrag";
import { COMPOSER_MENTION_DRAG_TYPE } from "../../components/chat/composerMentionDrag";
import { readPastedComposerContext } from "../../components/composerInlineTokenPaste";
import type { PendingUserInputProgress } from "../../pendingUserInput";
import type { PendingUserInput } from "../../session-logic";
import {
  deriveQuestionAnswerFooterState,
  isQuestionAnswerSubmitKey,
  type QuestionAnswerAttachmentCapabilities,
} from "./questionAnswerCard";
import { useQuestionAnswerAttachments } from "./useQuestionAnswerAttachments";

export function QuestionAnswerFooter({
  prompt,
  progress,
  threadRef,
  disabled,
  isResponding,
  capabilities,
  onChangeCustomAnswer,
  onPrevious,
  onAdvance,
  onDismiss,
}: {
  prompt: PendingUserInput;
  progress: PendingUserInputProgress;
  threadRef: ScopedThreadRef;
  disabled: boolean;
  isResponding: boolean;
  capabilities: QuestionAnswerAttachmentCapabilities;
  onChangeCustomAnswer: (questionId: string, value: string) => void;
  onPrevious: () => void;
  onAdvance: () => void;
  onDismiss: (requestId: RuntimeRequestId) => void;
}) {
  const state = deriveQuestionAnswerFooterState({ prompt, progress, disabled, isResponding });
  const fieldRef = useRef<HTMLTextAreaElement>(null);
  const surfaceRef = useRef<HTMLDivElement>(null);
  const questionId = progress.activeQuestion?.id ?? "";
  const attachments = useQuestionAnswerAttachments({
    threadRef,
    prompt,
    questionId,
    disabled: disabled || isResponding,
    allowAdding: state.showField && !state.fieldDisabled,
    capabilities,
  });
  const insert = (text: string) => {
    if (state.fieldDisabled || !state.showField) return;
    const field = fieldRef.current;
    const start = field?.selectionStart ?? progress.customAnswer.length;
    const end = field?.selectionEnd ?? start;
    onChangeCustomAnswer(
      questionId,
      progress.customAnswer.slice(0, start) + text + progress.customAnswer.slice(end),
    );
  };
  useThreadContextDrop(surfaceRef, threadRef.environmentId, attachments.target, (refs, isActive) =>
    attachments.addThreads(refs, insert, isActive),
  );
  useLayoutEffect(() => {
    const field = fieldRef.current;
    if (!field || !state.showField) return;
    field.style.height = "auto";
    field.style.height =
      progress.customAnswer.length === 0
        ? "68px"
        : `${Math.min(Math.max(field.scrollHeight, 68), 168)}px`;
  }, [progress.customAnswer, state.showField]);

  return (
    <ComposerBanner.Body>
      <div
        ref={surfaceRef}
        {...threadContextDropTargetProps()}
        onKeyDown={(event) => event.stopPropagation()}
        onDragOver={(event) => {
          event.preventDefault();
          event.stopPropagation();
          event.dataTransfer.dropEffect = event.dataTransfer.types.includes(
            COMPOSER_MENTION_DRAG_TYPE,
          )
            ? "move"
            : "copy";
        }}
        onDrop={(event) => {
          event.preventDefault();
          event.stopPropagation();
          if (state.fieldDisabled || !state.showField) return;
          const mention = event.dataTransfer.getData(COMPOSER_MENTION_DRAG_TYPE);
          if (mention) insert(`${mention} `);
          else if (event.dataTransfer.files.length)
            void attachments.addFiles(Array.from(event.dataTransfer.files));
          else insert(event.dataTransfer.getData("text/plain"));
        }}
        className="space-y-2 pb-2"
      >
        {state.showField ? (
          <textarea
            ref={fieldRef}
            aria-label="Your answer"
            placeholder={state.placeholder}
            value={progress.customAnswer}
            disabled={state.fieldDisabled}
            rows={2}
            className="w-full resize-none rounded-md border border-input bg-background px-3 py-2 text-sm outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring disabled:opacity-50"
            onChange={(event) => onChangeCustomAnswer(questionId, event.target.value)}
            onKeyDown={(event) => {
              event.stopPropagation();
              if (!isQuestionAnswerSubmitKey(event.nativeEvent, state.literal)) return;
              event.preventDefault();
              if (!state.primaryDisabled) onAdvance();
            }}
            onPaste={(event) => {
              event.stopPropagation();
              if (state.fieldDisabled) {
                event.preventDefault();
                return;
              }
              if (event.clipboardData.files.length) {
                event.preventDefault();
                void attachments.addFiles(Array.from(event.clipboardData.files));
                return;
              }
              if (state.literal) return;
              const fragment = readPastedComposerContext(event.clipboardData);
              if (fragment) {
                event.preventDefault();
                insert(
                  attachments.importClipboard(event.clipboardData.getData("text/plain"), fragment),
                );
              }
            }}
          />
        ) : null}
        {attachments.attachments.length > 0 ? (
          <ul className="space-y-1 text-xs" aria-label="Answer attachments">
            {attachments.attachments.map((attachment) => {
              const upload = attachments.uploads[attachment.id];
              return (
                <li key={attachment.id} className="flex items-center gap-2">
                  <span className="min-w-0 flex-1 truncate">
                    {attachment.name} ·{" "}
                    {upload?.status === "ready"
                      ? "Ready"
                      : upload?.status === "failed"
                        ? "Upload failed"
                        : "Uploading…"}
                  </span>
                  {upload?.status === "failed" ? (
                    <Button
                      type="button"
                      variant="ghost"
                      size="xs"
                      disabled={!attachments.canAttach}
                      onClick={() => attachments.retry(attachment)}
                    >
                      Retry
                    </Button>
                  ) : null}
                  <Button
                    type="button"
                    variant="ghost"
                    size="xs"
                    disabled={disabled || isResponding}
                    onClick={() => attachments.remove(attachment)}
                    aria-label={`Remove ${attachment.name}`}
                  >
                    Remove
                  </Button>
                </li>
              );
            })}
          </ul>
        ) : null}
        {attachments.preparationCount > 0 ? (
          <p className="text-xs text-muted-foreground">Preparing answer attachments…</p>
        ) : null}
        {attachments.error ? (
          <p role="alert" className="text-xs text-destructive">
            {attachments.error}
          </p>
        ) : null}
        {state.notice ? (
          <p role="status" className="text-xs text-muted-foreground">
            {state.notice}
          </p>
        ) : null}
        <div className="flex flex-wrap items-center gap-2">
          {state.showField && capabilities.supportsQuestionAttachments ? (
            <label className="relative text-xs text-muted-foreground">
              <span>Attach files</span>
              <input
                type="file"
                multiple
                aria-label="Attach files to answer"
                disabled={!attachments.canAttach}
                className="absolute inset-0 w-full cursor-pointer opacity-0"
                onChange={(event) => {
                  void attachments.addFiles(Array.from(event.currentTarget.files ?? []));
                  event.currentTarget.value = "";
                }}
              />
            </label>
          ) : null}
          {attachments.attachments.length > 0 || attachments.preparationCount > 0 ? (
            <Button
              type="button"
              variant="ghost"
              size="xs"
              disabled={disabled || isResponding}
              onClick={attachments.clear}
            >
              Remove all attachments
            </Button>
          ) : null}
          <div className="flex-1" />
          {state.showBack ? (
            <Button
              type="button"
              variant="ghost"
              size="sm"
              disabled={disabled || isResponding}
              onClick={onPrevious}
            >
              Back
            </Button>
          ) : null}
          {state.showSkip ? (
            <Button
              type="button"
              variant="ghost"
              size="sm"
              disabled={disabled || isResponding}
              onClick={() => onDismiss(prompt.requestId)}
            >
              Skip
            </Button>
          ) : null}
          <Button
            type="button"
            size="sm"
            disabled={state.primaryDisabled || attachments.preparationCount > 0}
            onClick={onAdvance}
          >
            {state.primaryLabel}
          </Button>
        </div>
      </div>
    </ComposerBanner.Body>
  );
}

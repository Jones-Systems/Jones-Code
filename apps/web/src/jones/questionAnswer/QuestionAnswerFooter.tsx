import { useId, useLayoutEffect, useRef } from "react";
import type { ScopedThreadRef } from "@t3tools/contracts";
import { PaperclipIcon } from "lucide-react";
import { Button } from "../../components/ui/button";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../../components/ui/tooltip";
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
  ANSWER_FIELD_MAX_LINES,
  deriveQuestionAnswerFooterState,
  isQuestionAnswerSubmitKey,
  type QuestionAnswerAttachmentCapabilities,
} from "./questionAnswerCard";
import { useQuestionAnswerAttachments } from "./useQuestionAnswerAttachments";
import { observeResize } from "../../lib/observeResize";

export function QuestionAnswerFooter({
  prompt,
  progress,
  threadRef,
  disabled,
  isResponding,
  capabilities,
  onChangeCustomAnswer,
  onToggleOption,
  onPrevious,
  onAdvance,
}: {
  prompt: PendingUserInput;
  progress: PendingUserInputProgress;
  threadRef: ScopedThreadRef;
  disabled: boolean;
  isResponding: boolean;
  capabilities: QuestionAnswerAttachmentCapabilities;
  onChangeCustomAnswer: (questionId: string, value: string) => void;
  onToggleOption: (questionId: string, optionValue: string) => void;
  onPrevious: () => void;
  onAdvance: () => void;
}) {
  const reasonId = useId();
  const state = deriveQuestionAnswerFooterState({ prompt, progress, disabled, isResponding });
  const fieldRef = useRef<HTMLTextAreaElement>(null);
  const attachmentInputRef = useRef<HTMLInputElement>(null);
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
  // oxlint-disable-next-line t3code/react-effect-budget -- Synchronize textarea DOM height with wrapped content and observed width before paint.
  useLayoutEffect(() => {
    const field = fieldRef.current;
    if (!field || !state.showField) return;
    const resize = () => {
      const style = getComputedStyle(field);
      const border =
        Number.parseFloat(style.borderTopWidth) + Number.parseFloat(style.borderBottomWidth);
      const padding = Number.parseFloat(style.paddingTop) + Number.parseFloat(style.paddingBottom);
      const maxHeight =
        ANSWER_FIELD_MAX_LINES * Number.parseFloat(style.lineHeight) + padding + border;
      field.style.height = progress.customAnswer.length === 0 ? "auto" : "0px";
      field.style.height = `${Math.min(field.scrollHeight + border, maxHeight)}px`;
    };
    resize();
    let width = field.clientWidth;
    return observeResize(field, () => {
      if (field.clientWidth === width) return;
      width = field.clientWidth;
      resize();
    });
  }, [progress.customAnswer, state.showField]);

  return (
    <ComposerBanner.Body className="pe-4">
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
        className="ms-5 space-y-2 pb-2"
      >
        {state.showField &&
        (state.fieldLabel || (!state.notes && progress.activeQuestion?.options.length)) ? (
          <div className="flex items-center justify-between gap-2 text-xs text-muted-foreground">
            <span>
              {state.fieldLabel}
              {!state.notes && progress.activeQuestion?.options.length
                ? "Typing replaces your choice"
                : ""}
            </span>
            {state.notes && state.hasChoice ? (
              <Button
                type="button"
                variant="ghost"
                size="xs"
                disabled={state.fieldDisabled}
                onClick={() => onToggleOption(questionId, progress.selectedOptionValues[0]!)}
              >
                Clear choice
              </Button>
            ) : null}
          </div>
        ) : null}
        <div className="flex items-end gap-2">
          {state.showField ? (
            <div className="relative min-w-0 flex-1">
              <textarea
                ref={fieldRef}
                aria-label="Your answer"
                placeholder={state.placeholder}
                value={progress.customAnswer}
                disabled={state.fieldDisabled}
                rows={1}
                className="block w-full resize-none overflow-y-auto rounded-md border border-input bg-background py-1.5 ps-3 pe-10 text-sm/5 outline-none [scrollbar-width:none] [&::-webkit-scrollbar]:hidden focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring disabled:opacity-50"
                onChange={(event) => onChangeCustomAnswer(questionId, event.target.value)}
                onKeyDown={(event) => {
                  event.stopPropagation();
                  if (!isQuestionAnswerSubmitKey(event.nativeEvent)) return;
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
                      attachments.importClipboard(
                        event.clipboardData.getData("text/plain"),
                        fragment,
                      ),
                    );
                  }
                }}
              />
              {capabilities.supportsQuestionAttachments ? (
                <div className="absolute end-0.5 bottom-0.5">
                  <input
                    ref={attachmentInputRef}
                    type="file"
                    multiple
                    aria-label="Attach files to answer"
                    disabled={!attachments.canAttach}
                    className="hidden"
                    onChange={(event) => {
                      const files = Array.from(event.currentTarget.files ?? []);
                      event.currentTarget.value = "";
                      void attachments.addFiles(files);
                    }}
                  />
                  <Tooltip>
                    <TooltipTrigger
                      render={
                        <Button
                          type="button"
                          variant="ghost"
                          size="icon-sm"
                          disabled={!attachments.canAttach}
                          onPointerDown={(event) => event.preventDefault()}
                          onClick={() => attachmentInputRef.current?.click()}
                          aria-label="Attach files to answer"
                        />
                      }
                    >
                      <PaperclipIcon />
                    </TooltipTrigger>
                    <TooltipPopup>Attach files to answer</TooltipPopup>
                  </Tooltip>
                </div>
              ) : null}
            </div>
          ) : (
            <div className="flex-1" />
          )}
          <div className="pb-0.5">
            <Button
              type="button"
              size="sm"
              disabled={
                state.fieldDisabled ||
                attachments.preparationCount > 0 ||
                progress.activeDraft?.attachmentsBlocked ||
                (!progress.isLastQuestion && !progress.canAdvance)
              }
              aria-disabled={state.primaryDisabled || attachments.preparationCount > 0}
              aria-describedby={state.blockedReason ? reasonId : undefined}
              onClick={() => {
                if (!state.primaryDisabled && attachments.preparationCount === 0) onAdvance();
              }}
            >
              {state.primaryLabel}
            </Button>
          </div>
        </div>
        {state.blockedReason ? (
          <p id={reasonId} className="text-xs text-muted-foreground">
            {state.blockedReason}
          </p>
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
        {state.showBack ||
        attachments.attachments.length > 0 ||
        attachments.preparationCount > 0 ? (
          <div className="flex flex-wrap items-center gap-2">
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
          </div>
        ) : null}
      </div>
    </ComposerBanner.Body>
  );
}

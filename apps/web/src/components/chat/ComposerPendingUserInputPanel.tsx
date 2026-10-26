import { supportsUserResponseAnswerNotes } from "@t3tools/contracts/jones/userResponseRequest";
import { QuestionProgress } from "../../jones/questionAnswer/QuestionProgress";
import { questionSummaryLine } from "../../jones/questionAnswer/questionAnswerCard";
import { QuestionAnswerFooter } from "../../jones/questionAnswer/QuestionAnswerFooter";
import {
  NO_QUESTION_ATTACHMENT_CAPABILITIES,
  type QuestionAnswerAttachmentCapabilities,
} from "../../jones/questionAnswer/questionAnswerCard";
import { type RuntimeRequestId, type ScopedThreadRef } from "@t3tools/contracts";
import { memo, use, useEffect, useEffectEvent, useState } from "react";
import { BackgroundThreadContext } from "./backgroundThread";
import { type PendingUserInput } from "../../session-logic";
import {
  derivePendingUserInputProgress,
  type PendingUserInputDraftAnswer,
} from "../../pendingUserInput";
import { CheckboxMark } from "../ui/checkbox";
import { Collapsible, CollapsiblePanel, CollapsibleTrigger } from "../ui/collapsible";
import { Kbd } from "../ui/kbd";
import { cn } from "~/lib/utils";
import { ComposerBanner } from "./ComposerBanner";
import ChatMarkdown from "../ChatMarkdown";

interface PendingUserInputPanelProps {
  pendingUserInputs: PendingUserInput[];
  cwd: string | undefined;
  threadRef: ScopedThreadRef;
  disabled?: boolean;
  respondingRequestIds: RuntimeRequestId[];
  answers: Record<string, PendingUserInputDraftAnswer>;
  questionIndex: number;
  onToggleOption: (questionId: string, optionValue: string) => void;
  onChangeCustomAnswer: (questionId: string, value: string) => void;
  onPrevious: () => void;
  onSelectQuestion?: ((index: number) => void) | undefined;
  attachmentCapabilities?: QuestionAnswerAttachmentCapabilities;
  onAdvance: () => void;
  onDismiss: (requestId: RuntimeRequestId) => void;
}

export const ComposerPendingUserInputPanel = memo(function ComposerPendingUserInputPanel({
  pendingUserInputs,
  cwd,
  threadRef,
  disabled = false,
  respondingRequestIds,
  answers,
  questionIndex,
  onToggleOption,
  onChangeCustomAnswer,
  onPrevious,
  onSelectQuestion,
  attachmentCapabilities = NO_QUESTION_ATTACHMENT_CAPABILITIES,
  onAdvance,
  onDismiss,
}: PendingUserInputPanelProps) {
  if (pendingUserInputs.length === 0) return null;
  const activePrompt = pendingUserInputs[0];
  if (!activePrompt) return null;

  return (
    <ComposerPendingUserInputCard
      key={activePrompt.requestId}
      prompt={activePrompt}
      cwd={cwd}
      threadRef={threadRef}
      disabled={disabled}
      isResponding={respondingRequestIds.includes(activePrompt.requestId)}
      answers={answers}
      questionIndex={questionIndex}
      onToggleOption={onToggleOption}
      onChangeCustomAnswer={onChangeCustomAnswer}
      onPrevious={onPrevious}
      onSelectQuestion={onSelectQuestion}
      attachmentCapabilities={attachmentCapabilities}
      onAdvance={onAdvance}
      onDismiss={onDismiss}
    />
  );
});

const ComposerPendingUserInputCard = memo(function ComposerPendingUserInputCard({
  prompt,
  cwd,
  threadRef,
  disabled,
  isResponding,
  answers,
  questionIndex,
  onToggleOption,
  onChangeCustomAnswer,
  onPrevious,
  onSelectQuestion,
  attachmentCapabilities = NO_QUESTION_ATTACHMENT_CAPABILITIES,
  onAdvance,
  onDismiss,
}: {
  prompt: PendingUserInput;
  cwd: string | undefined;
  threadRef: ScopedThreadRef;
  disabled: boolean;
  isResponding: boolean;
  answers: Record<string, PendingUserInputDraftAnswer>;
  questionIndex: number;
  onToggleOption: (questionId: string, optionValue: string) => void;
  onChangeCustomAnswer: (questionId: string, value: string) => void;
  onPrevious: () => void;
  onSelectQuestion?: ((index: number) => void) | undefined;
  attachmentCapabilities?: QuestionAnswerAttachmentCapabilities;
  onAdvance: () => void;
  onDismiss: (requestId: RuntimeRequestId) => void;
}) {
  // Message-mode requests remain answerable after their provider turn ends.
  const canRespond = prompt.responseCapability !== "not_resumable";
  const responseDisabled = disabled || isResponding || !canRespond;
  const [collapsedQuestionId, setCollapsedQuestionId] = useState<string | null>(null);
  // Keep question identity stable for its shortcut listener.
  const notes = supportsUserResponseAnswerNotes(prompt.responseCapability);
  const progress = derivePendingUserInputProgress(prompt.questions, answers, questionIndex, {
    notes,
  });
  const activeQuestion = progress.activeQuestion;
  const customAnswerActive = progress.customAnswer.trim().length > 0;
  // Read before later hooks to preserve the question's compiler cache.
  const draftSelectedOptions = activeQuestion?.options.map(
    (option) =>
      (!customAnswerActive || (notes && !activeQuestion.multiSelect)) &&
      progress.selectedOptionValues.includes(option.value ?? option.label),
  );
  const isCollapsed = collapsedQuestionId !== null && collapsedQuestionId === activeQuestion?.id;

  const handleOptionSelection = (questionId: string, optionValue: string) => {
    if (!responseDisabled) onToggleOption(questionId, optionValue);
  };

  const selectOptionFromShortcut = useEffectEvent(handleOptionSelection);
  const backgroundThread = use(BackgroundThreadContext);

  useEffect(() => {
    if (!activeQuestion || responseDisabled || isCollapsed || backgroundThread) return;
    const handler = (event: globalThis.KeyboardEvent) => {
      if (event.metaKey || event.ctrlKey || event.altKey) return;
      const target = event.target;
      if (target instanceof HTMLInputElement || target instanceof HTMLTextAreaElement) {
        return;
      }
      if (
        target instanceof HTMLElement &&
        target.closest('[contenteditable]:not([contenteditable="false"])')
      ) {
        return;
      }
      const digit = Number.parseInt(event.key, 10);
      if (Number.isNaN(digit) || digit < 1 || digit > 9) return;
      const optionIndex = digit - 1;
      if (optionIndex >= activeQuestion.options.length) return;
      const option = activeQuestion.options[optionIndex];
      if (!option) return;
      event.preventDefault();
      selectOptionFromShortcut(activeQuestion.id, option.value ?? option.label);
    };
    document.addEventListener("keydown", handler);
    return () => document.removeEventListener("keydown", handler);
  }, [activeQuestion, backgroundThread, isCollapsed, responseDisabled]);

  if (!activeQuestion) {
    return null;
  }

  return (
    <Collapsible
      open={!isCollapsed}
      onOpenChange={(open) => {
        setCollapsedQuestionId(open ? null : activeQuestion.id);
      }}
    >
      <ComposerBanner.Row className="flex items-center gap-2 px-4 pt-1.5">
        <CollapsibleTrigger
          render={<button type="button" />}
          title={
            isCollapsed ? "Show the question and its options" : "Hide the question and its options"
          }
          data-pending-user-input-toggle={isCollapsed ? "collapsed" : "expanded"}
          className="flex min-w-0 flex-1 items-center gap-2 rounded outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring"
        >
          <ComposerBanner.Content>
            <span className="min-w-0 truncate text-muted-foreground">{activeQuestion.header}</span>
            {isCollapsed ? (
              <>
                <ComposerBanner.Separator />
                <span className="min-w-0 flex-1 truncate text-secondary-label">
                  {questionSummaryLine(activeQuestion.question)}
                </span>
              </>
            ) : null}
          </ComposerBanner.Content>
          <ComposerBanner.ToggleIcon expanded={!isCollapsed} />
        </CollapsibleTrigger>
        <QuestionProgress
          index={progress.questionIndex}
          answered={progress.answeredByIndex}
          questionIds={prompt.questions.map((question) => question.id)}
          disabled={disabled || isResponding}
          onSelect={
            onSelectQuestion
              ? (index) => {
                  setCollapsedQuestionId(null);
                  onSelectQuestion(index);
                }
              : undefined
          }
        />
        {prompt.dismissible ? (
          <ComposerBanner.Dismiss
            aria-label="Dismiss question without answering"
            title="Dismiss question without answering"
            disabled={disabled || isResponding}
            data-pending-user-input-dismiss
            onClick={() => {
              if (!disabled && !isResponding) onDismiss(prompt.requestId);
            }}
          />
        ) : null}
      </ComposerBanner.Row>
      <CollapsiblePanel>
        <ComposerBanner.Scroll>
          <ComposerBanner.Body className="ps-6 pe-4 pb-1 wrap-anywhere sm:ps-5.5">
            <ChatMarkdown
              text={activeQuestion.question}
              cwd={cwd}
              threadRef={threadRef}
              className="pt-2 text-foreground/85"
            />
            {/* Rows span the header row's width so the radios align with the question text and the shortcut column sits under the dismiss button. */}
            <div className="-ms-2 mt-1.5 sm:-ms-1.5">
              {activeQuestion.options.map((option, index) => {
                const optionValue = option.value ?? option.label;
                const isSelected = draftSelectedOptions?.[index] === true;
                const className = cn(
                  "group flex w-full items-start gap-2.5 rounded-md bg-transparent py-1.5 ps-2 text-left sm:ps-1.5 outline-none transition-colors duration-150 focus-visible:ring-1 focus-visible:ring-inset focus-visible:ring-primary/25",
                  isSelected ? "text-foreground" : "text-foreground/70 hover:bg-muted/30",
                  (disabled || isResponding) && "opacity-50 cursor-not-allowed",
                  !disabled && !isResponding && "cursor-pointer",
                );
                // The h-5 slots match the label's line box, so the indicator and shortcut stay on its first line.
                const content = (
                  <>
                    <span className="pointer-events-none flex h-5 shrink-0 items-center">
                      {activeQuestion.multiSelect ? (
                        <CheckboxMark checked={isSelected} />
                      ) : (
                        <OptionRadio checked={isSelected} />
                      )}
                    </span>
                    <div className="min-w-0 flex-1 flex flex-col gap-0.5">
                      <span className="text-sm font-medium">{option.label}</span>
                      {option.description && option.description !== option.label ? (
                        <span className="text-secondary-label text-2xs">{option.description}</span>
                      ) : null}
                    </div>
                    {index < 9 ? (
                      <span className="flex h-5 w-7 shrink-0 items-center justify-center sm:w-6">
                        <Kbd>{index + 1}</Kbd>
                      </span>
                    ) : null}
                  </>
                );
                return (
                  <button
                    key={`${activeQuestion.id}:${optionValue}`}
                    type="button"
                    aria-pressed={isSelected}
                    disabled={responseDisabled}
                    onClick={() => {
                      handleOptionSelection(activeQuestion.id, optionValue);
                    }}
                    className={className}
                  >
                    {content}
                  </button>
                );
              })}
            </div>
          </ComposerBanner.Body>
        </ComposerBanner.Scroll>
        <QuestionAnswerFooter
          prompt={prompt}
          progress={progress}
          threadRef={threadRef}
          disabled={disabled}
          isResponding={isResponding}
          capabilities={attachmentCapabilities}
          onChangeCustomAnswer={onChangeCustomAnswer}
          onToggleOption={onToggleOption}
          onPrevious={onPrevious}
          onAdvance={onAdvance}
        />
      </CollapsiblePanel>
    </Collapsible>
  );
});

// Drawn with ui/radio-group's look instead of using its Radio: Radio only checks inside a
// RadioGroup, whose arrow keys would select an answer while navigating the card.
function OptionRadio({ checked }: { checked: boolean }) {
  return (
    <span
      aria-hidden
      className={cn(
        "relative inline-flex size-4.5 shrink-0 items-center justify-center rounded-full border border-input bg-background not-dark:bg-clip-padding sm:size-4 dark:bg-input/32",
        checked ? "shadow-none" : "shadow-xs/5",
      )}
    >
      {checked ? (
        <span className="-inset-px absolute flex items-center justify-center rounded-full bg-primary before:size-2 before:rounded-full before:bg-primary-foreground sm:before:size-1.5" />
      ) : null}
    </span>
  );
}

import { PROVIDER_SEND_TURN_MAX_INPUT_CHARS } from "@t3tools/contracts";
import { expandAssistantCitationsForProvider } from "@t3tools/shared/assistantCitations";
import { DEFAULT_RESOLVED_KEYBINDINGS } from "@t3tools/shared/keybindings";

import {
  composerSubmissionIntentForKey,
  type ComposerSubmissionIntent,
} from "../../composer-logic";

type ComposerSubmitEvent = { preventDefault: () => void };

type ComposerSubmissionInput = {
  prompt: string;
  providerInput?: string;
  submissionTarget: "provider-turn" | "pending-user-input";
};

export function getComposerPromptLengthValidationMessage(prompt: string): string | null {
  const normalizedPrompt = prompt.trim();
  const inputLength = Math.max(
    normalizedPrompt.length,
    expandAssistantCitationsForProvider(normalizedPrompt).length,
  );
  const excessCharacters = inputLength - PROVIDER_SEND_TURN_MAX_INPUT_CHARS;
  if (excessCharacters <= 0) return null;

  const characterLabel = excessCharacters === 1 ? "character" : "characters";
  return `Prompt is ${excessCharacters.toLocaleString("en-US")} ${characterLabel} over the ${PROVIDER_SEND_TURN_MAX_INPUT_CHARS.toLocaleString("en-US")}-character limit. Shorten or split it before sending.`;
}

export function getComposerSubmissionValidationMessage(
  options: ComposerSubmissionInput,
): string | null {
  return options.submissionTarget === "provider-turn"
    ? getComposerPromptLengthValidationMessage(options.providerInput ?? options.prompt)
    : null;
}

export function submitComposerDraft(
  options: ComposerSubmissionInput & {
    event: ComposerSubmitEvent | undefined;
    onSend: (event?: ComposerSubmitEvent) => boolean | void;
  },
): { validationMessage: string | null; didDispatch: boolean } {
  const validationMessage = getComposerSubmissionValidationMessage(options);
  if (validationMessage) {
    options.event?.preventDefault();
    return { validationMessage, didDispatch: false };
  }

  if (options.onSend(options.event) === false) {
    options.event?.preventDefault();
    return { validationMessage: null, didDispatch: false };
  }
  return { validationMessage: null, didDispatch: true };
}

export function handleComposerEnter(options: {
  event: Pick<
    KeyboardEvent,
    "shiftKey" | "altKey" | "metaKey" | "ctrlKey" | "isComposing" | "keyCode" | "repeat"
  >;
  intent: Omit<
    Parameters<typeof composerSubmissionIntentForKey>[0],
    "event" | "keybindings" | "platform"
  >;
  submissionIntent?: ComposerSubmissionIntent | null;
  hasDraftContext: boolean;
  queueActionDisabled: boolean;
  onSteerNextQueuedMessage: () => boolean;
  onSubmit: (intent: ComposerSubmissionIntent) => void;
}): boolean {
  const { event } = options;
  if (event.isComposing || event.keyCode === 229) return false;
  if (
    !event.shiftKey &&
    !event.altKey &&
    !event.metaKey &&
    !event.ctrlKey &&
    options.intent.prompt === "" &&
    !options.hasDraftContext &&
    !options.queueActionDisabled
  ) {
    // Held Enter must not drain the queue after a completed promotion. The queue's
    // promotion lock also covers an arrow click racing a fresh key press.
    if (event.repeat || options.onSteerNextQueuedMessage()) return true;
  }
  if (event.repeat || event.altKey) return false;
  const intent =
    options.submissionIntent === undefined
      ? composerSubmissionIntentForKey({
          ...options.intent,
          event: { ...event, key: "Enter" },
          keybindings: DEFAULT_RESOLVED_KEYBINDINGS,
        })
      : options.submissionIntent;
  if (!intent) return false;
  options.onSubmit(intent);
  return true;
}

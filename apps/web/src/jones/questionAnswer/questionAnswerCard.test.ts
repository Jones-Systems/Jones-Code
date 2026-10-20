import { DEFAULT_RESOLVED_KEYBINDINGS } from "@t3tools/shared/keybindings";
import { describe, expect, it } from "vite-plus/test";
import { derivePendingUserInputProgress } from "../../pendingUserInput";
import { handleComposerEnter, submitComposerDraft } from "../../components/chat/composerSubmission";
import { resolveComposerDispatchMode } from "@t3tools/client-runtime/state/composer-dispatch";
import {
  deriveQuestionAnswerFooterState,
  isQuestionAnswerSubmitKey,
  pendingQuestionBlocksSteering,
} from "./questionAnswerCard";

const question = {
  id: "q",
  header: "Question",
  question: "Choose",
  options: [{ label: "One", description: "First" }],
  multiSelect: false,
};
const event = {
  key: "Enter",
  shiftKey: false,
  altKey: false,
  ctrlKey: false,
  metaKey: false,
  isComposing: false,
  keyCode: 13,
  repeat: false,
};

it("preserves literal answers including a deliberately emptied field", () => {
  for (const answer of ["  initial\n  ", ""]) {
    const progress = derivePendingUserInputProgress(
      [{ ...question, initialAnswer: "initial" }],
      { q: { customAnswer: answer } },
      0,
    );
    const state = deriveQuestionAnswerFooterState({
      progress,
      prompt: { responseCapability: "live", dismissible: false },
      disabled: false,
      isResponding: false,
    });
    expect(progress.resolvedAnswer).toBe(answer);
    expect(state.primaryDisabled).toBe(false);
    expect(state.literal).toBe(true);
  }
});

it("keeps options-only questions answerable without a custom field", () => {
  const progress = derivePendingUserInputProgress(
    [{ ...question, allowCustomAnswer: false }],
    { q: { selectedOptionValues: ["One"] } },
    0,
  );
  expect(
    deriveQuestionAnswerFooterState({
      progress,
      prompt: { responseCapability: "live", dismissible: false },
      disabled: false,
      isResponding: false,
    }),
  ).toMatchObject({ showField: false, primaryDisabled: false });
});

it.each([
  { disabled: true, isResponding: false, responseCapability: "live" as const },
  { disabled: false, isResponding: true, responseCapability: "message" as const },
  { disabled: false, isResponding: false, responseCapability: "not_resumable" as const },
])("prevents answering while unavailable: %o", ({ responseCapability, ...input }) => {
  const progress = derivePendingUserInputProgress([question], { q: { customAnswer: "Answer" } }, 0);
  expect(
    deriveQuestionAnswerFooterState({
      ...input,
      progress,
      prompt: { responseCapability, dismissible: false },
    }),
  ).toMatchObject({ fieldDisabled: true, primaryDisabled: true });
});

it("blocks final submission while another question's attachment is unready", () => {
  const progress = derivePendingUserInputProgress(
    [question, { ...question, id: "last" }],
    { q: { customAnswer: "First", attachmentsBlocked: true }, last: { customAnswer: "Last" } },
    1,
  );
  expect(
    deriveQuestionAnswerFooterState({
      progress,
      prompt: { responseCapability: "live", dismissible: false },
      disabled: false,
      isResponding: false,
    }).primaryDisabled,
  ).toBe(true);
});

it("ignores composing, repeat and newline keys; literal editing requires Mod+Enter", () => {
  expect(isQuestionAnswerSubmitKey(event, false)).toBe(true);
  expect(isQuestionAnswerSubmitKey(event, true)).toBe(false);
  expect(isQuestionAnswerSubmitKey({ ...event, metaKey: true }, true)).toBe(true);
  expect(isQuestionAnswerSubmitKey({ ...event, ctrlKey: true }, true)).toBe(true);
  for (const change of [
    { isComposing: true },
    { keyCode: 229 },
    { repeat: true },
    { shiftKey: true },
    { altKey: true },
  ])
    expect(isQuestionAnswerSubmitKey({ ...event, ...change }, false)).toBe(false);
});

describe("normal message delivery while a question is pending", () => {
  it.each(["live", "message", "not_resumable"] as const)(
    "uses the question capability %s to preserve normal dispatch rules",
    (responseCapability) => {
      const blocked = pendingQuestionBlocksSteering([{ responseCapability }]);
      for (const alternateModifier of [false, true]) {
        const sent: string[] = [];
        const mode = resolveComposerDispatchMode({
          running: true,
          activeTurnDefault: "steer",
          alternateModifier,
          canSteer: !blocked,
        });
        const outcome = submitComposerDraft({
          prompt: "normal message",
          submissionTarget: "provider-turn",
          event: undefined,
          onSend: () => {
            sent.push(mode);
          },
        });
        expect(outcome.didDispatch).toBe(true);
        expect(sent).toEqual([blocked || alternateModifier ? "queue" : "steer"]);
      }
    },
  );
  it("does not promote a queued message on empty Enter during a live question", () => {
    let promoted = 0;
    handleComposerEnter({
      event,
      intent: {
        keybindings: DEFAULT_RESOLVED_KEYBINDINGS,
        isMobileViewport: false,
        isDraftThread: false,
        isRunning: true,
        sendShortcut: "enter",
        prompt: "",
      },
      hasDraftContext: false,
      queueActionDisabled: pendingQuestionBlocksSteering([{ responseCapability: "live" }]),
      onSteerNextQueuedMessage: () => {
        promoted++;
        return true;
      },
      onSubmit: () => {},
    });
    expect(promoted).toBe(0);
  });
});

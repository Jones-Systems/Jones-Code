import { EnvironmentId, RuntimeRequestId, ThreadId } from "@t3tools/contracts";
import { act, render, fakeTimers } from "../../../test/render";
import { expect, it, vi } from "vite-plus/test";

vi.mock(import("~/components/ChatMarkdown"), async (importOriginal) => ({
  ...(await importOriginal()),
  default: (({ text }: { text: string }) => <p>{text}</p>) as never,
}));

import { ComposerPendingUserInputPanel } from "~/components/chat/ComposerPendingUserInputPanel";

const threadRef = { environmentId: EnvironmentId.make("env"), threadId: ThreadId.make("thread") };

it("blocks option selection on revocation and restores select-only behavior after a new grant", async () => {
  using _timers = fakeTimers();
  const onToggleOption = vi.fn();
  const onAdvance = vi.fn();
  const onDismiss = vi.fn();
  const prompt = {
    requestId: RuntimeRequestId.make("scoped-answer"),
    createdAt: "2026-09-05T00:00:00.000Z",
    dismissible: false,
    responseCapability: "live" as const,
    questions: [
      {
        id: "approach",
        header: "Approach",
        question: "Which approach?",
        options: [{ label: "Incremental", description: "One module at a time" }],
        multiSelect: false,
      },
    ],
  };
  const panel = (disabled: boolean) => (
    <ComposerPendingUserInputPanel
      pendingUserInputs={[prompt]}
      cwd="/repo"
      threadRef={threadRef}
      disabled={disabled}
      respondingRequestIds={[]}
      answers={{}}
      questionIndex={0}
      onToggleOption={onToggleOption}
      onAdvance={onAdvance}
      onChangeCustomAnswer={() => {}}
      onPrevious={() => {}}
      onDismiss={onDismiss}
    />
  );
  await using screen = await render(panel(false));
  const container = screen.container;
  // A disabled button swallows the click, as it would for a user.
  const chooseOption = () =>
    [...container.querySelectorAll("button")]
      .find((button) => button.textContent?.includes("Incremental"))!
      .click();
  await act(async () => chooseOption());
  expect(onToggleOption).toHaveBeenCalledWith("approach", "Incremental");
  expect(onAdvance).not.toHaveBeenCalled();

  await screen.rerender(panel(true));
  await act(async () => {
    chooseOption();
    vi.runAllTimers();
  });
  expect(onToggleOption).toHaveBeenCalledTimes(1);
  expect(onAdvance).not.toHaveBeenCalled();

  await screen.rerender(panel(false));
  await act(async () => {
    chooseOption();
    vi.runAllTimers();
  });
  expect(onToggleOption).toHaveBeenCalledTimes(2);
  expect(onAdvance).not.toHaveBeenCalled();
});

it("allows a live question but fences option and submit actions for a non-resumable question", async () => {
  const onToggleOption = vi.fn();
  const onAdvance = vi.fn();
  const panel = (responseCapability: "live" | "not_resumable") => (
    <ComposerPendingUserInputPanel
      pendingUserInputs={[
        {
          requestId: RuntimeRequestId.make("non-resumable"),
          createdAt: "2026-10-26T00:00:00.000Z",
          dismissible: false,
          responseCapability,
          questions: [
            {
              id: "choice",
              header: "Choice",
              question: "Choose?",
              multiSelect: false,
              options: [{ label: "Continue", description: "Continue execution" }],
            },
          ],
        },
      ]}
      cwd="/repo"
      threadRef={threadRef}
      disabled={false}
      respondingRequestIds={[]}
      answers={{}}
      questionIndex={0}
      onToggleOption={onToggleOption}
      onAdvance={onAdvance}
      onChangeCustomAnswer={() => {}}
      onPrevious={() => {}}
      onDismiss={() => {}}
    />
  );
  await using screen = await render(panel("live"));
  const option = () =>
    [...screen.container.querySelectorAll("button")].find((button) =>
      button.textContent?.includes("Continue execution"),
    )!;
  expect(option().disabled).toBe(false);
  await screen.rerender(panel("not_resumable"));
  expect(option().disabled).toBe(true);
  await act(() => option().click());
  expect(onToggleOption).not.toHaveBeenCalled();
  expect(onAdvance).not.toHaveBeenCalled();
});

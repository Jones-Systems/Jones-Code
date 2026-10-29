import * as Cause from "effect/Cause";
import { AsyncResult } from "effect/reactivity";
import {
  EnvironmentId,
  NodeId,
  ProviderSessionId,
  RuntimeRequestId,
  ThreadId,
  TurnItemId,
  type UserInputQuestion,
} from "@t3tools/contracts";
import { useLayoutEffect } from "react";
import { beforeEach, expect, it, vi } from "vite-plus/test";
import { act, render } from "../../../test/render";
import { makeThreadProjectionFixture } from "../../test-fixtures";
import { ComposerPendingUserInputPanel } from "../../components/chat/ComposerPendingUserInputPanel";
import { usePendingThreadRequests } from "../../components/chat/usePendingThreadRequests";
import {
  changeQuestionAttachmentPreparation,
  questionAttachmentDraftId,
} from "../../questionAttachments";

const { respond } = vi.hoisted(() => ({ respond: vi.fn() }));
vi.mock(import("../../state/use-orchestration-command"), () => ({
  useOrchestrationCommand: () => respond,
}));
vi.mock(import("../../state/session"), async (original) => ({
  ...(await original()),
  readEnvironmentScope: () => true,
}));
vi.mock(import("../../components/ChatMarkdown"), async (original) => ({
  ...(await original()),
  default: (({ text }: { text: string }) => <p>{text}</p>) as never,
}));

let requests: ReturnType<typeof usePendingThreadRequests>;
let fixture = 0;
function Questions({
  questions,
  capability = "message",
}: {
  questions: UserInputQuestion[];
  capability?: "message" | "live";
}) {
  const projection = makeThreadProjectionFixture();
  const threadId = ThreadId.make(`quick-response-${fixture}`);
  const requestId = RuntimeRequestId.make(`quick-response-${fixture}`);
  const environmentId = EnvironmentId.make("quick-response-env");
  const nodeId = NodeId.make(requestId);
  const state = usePendingThreadRequests({
    environmentId,
    activeThreadId: threadId,
    activeThread: { id: threadId },
    serverProjection: {
      ...projection,
      thread: { ...projection.thread, id: threadId },
      runtimeRequests: [
        {
          id: requestId,
          nodeId,
          providerTurnId: null,
          nativeRequestRef: null,
          kind: "user_input",
          status: "pending",
          responseCapability:
            capability === "live"
              ? { type: "live", providerSessionId: ProviderSessionId.make("synthetic-provider") }
              : { type: "message" },
          createdAt: projection.updatedAt,
          resolvedAt: null,
        },
      ],
      turnItems: [
        {
          id: TurnItemId.make(requestId),
          threadId,
          runId: null,
          nodeId,
          providerThreadId: null,
          providerTurnId: null,
          nativeItemRef: null,
          parentItemId: null,
          ordinal: 0,
          status: "completed",
          title: null,
          startedAt: projection.updatedAt,
          completedAt: projection.updatedAt,
          updatedAt: projection.updatedAt,
          type: "user_input_request",
          requestId,
          questions,
        },
      ],
    },
    supportsQuestionAttachments: true,
    composerDraftTarget: { environmentId, threadId },
    setComposerDraftPrompt: () => {},
    setThreadError: () => {},
  });
  useLayoutEffect(() => {
    requests = state;
  });
  return (
    <ComposerPendingUserInputPanel
      pendingUserInputs={state.pendingUserInputs}
      cwd="/synthetic"
      threadRef={{ environmentId, threadId }}
      respondingRequestIds={state.respondingRequestIds}
      answers={state.activePendingDraftAnswers}
      questionIndex={state.activePendingQuestionIndex}
      onToggleOption={state.onSelectActivePendingUserInputOption}
      onChangeCustomAnswer={state.onChangeActivePendingUserInputCustomAnswer}
      onPrevious={state.onPreviousActivePendingUserInputQuestion}
      onSelectQuestion={state.onSelectActivePendingUserInputQuestion}
      onAdvance={state.onAdvanceActivePendingUserInput}
      onDismiss={state.onDismissUserInput}
    />
  );
}
const question = (id: string, multiSelect = false): UserInputQuestion => ({
  id,
  header: id,
  question: `Choose ${id}`,
  multiSelect,
  options: [
    { label: "Done", description: "Task completed", value: "completed" },
    { label: "Blocked", description: "Needs attention", value: "blocked" },
  ],
});
async function click(label: string) {
  const button = [...document.querySelectorAll("button")].find((entry) =>
    entry.textContent?.includes(label),
  );
  expect(button, label).toBeDefined();
  await act(() => button!.click());
}
beforeEach(() => {
  fixture++;
  respond.mockReset();
  respond.mockResolvedValue(AsyncResult.success(undefined));
});

it.each(["message", "live"] as const)(
  "submits a %s single choice on click without Send",
  async (capability) => {
    await using _screen = await render(
      <Questions questions={[question("action")]} capability={capability} />,
    );
    await click("Done");
    expect(respond).toHaveBeenCalledTimes(1);
    expect(respond).toHaveBeenCalledWith(
      expect.objectContaining({
        input: expect.objectContaining({ answers: { action: "completed" } }),
      }),
    );
  },
);

it("includes typed notes, fences duplicate clicks and retries the same choice after failure", async () => {
  let finish!: (value: unknown) => void;
  respond.mockImplementation(
    () =>
      new Promise((resolve) => {
        finish = resolve;
      }),
  );
  await using _screen = await render(<Questions questions={[question("action")]} />);
  await act(() =>
    requests.onChangeActivePendingUserInputCustomAnswer("action", "Verified workspace B"),
  );
  await act(() => {
    requests.onSelectActivePendingUserInputOption("action", "completed");
    requests.onSelectActivePendingUserInputOption("action", "completed");
  });
  expect(respond).toHaveBeenCalledTimes(1);
  await act(() => finish(AsyncResult.failure(Cause.fail(new Error("Synthetic failure")))));
  await click("Done");
  expect(respond).toHaveBeenCalledTimes(2);
  expect(respond).toHaveBeenLastCalledWith(
    expect.objectContaining({
      input: expect.objectContaining({
        answers: { action: "completed\nNote: Verified workspace B" },
      }),
    }),
  );
  await act(() => finish(AsyncResult.success(undefined)));
});

it("advances choices and sends the complete batch even when the last missing answer is earlier", async () => {
  await using _screen = await render(
    <Questions questions={[question("first"), question("second"), question("third")]} />,
  );
  await act(() => requests.onSelectActivePendingUserInputQuestion(2));
  await click("Done");
  expect(requests.activePendingQuestionIndex).toBe(0);
  await click("Done");
  expect(requests.activePendingQuestionIndex).toBe(1);
  expect(respond).not.toHaveBeenCalled();
  await click("Blocked");
  expect(respond).toHaveBeenCalledTimes(1);
  expect(respond).toHaveBeenCalledWith(
    expect.objectContaining({
      input: expect.objectContaining({
        answers: { first: "completed", second: "blocked", third: "completed" },
      }),
    }),
  );
});

it("keeps multi-select editable until explicit submission", async () => {
  await using _screen = await render(<Questions questions={[question("many", true)]} />);
  await click("Done");
  await click("Blocked");
  expect(respond).not.toHaveBeenCalled();
  await click("Send");
  expect(respond).toHaveBeenCalledWith(
    expect.objectContaining({
      input: expect.objectContaining({ answers: { many: ["completed", "blocked"] } }),
    }),
  );
});

it("blocks unfinished attachment preparation and never submits merely because it finishes", async () => {
  await using _screen = await render(<Questions questions={[question("action")]} />);
  const key = questionAttachmentDraftId(
    EnvironmentId.make("quick-response-env"),
    ThreadId.make(`quick-response-${fixture}`),
    `quick-response-${fixture}`,
    "action",
  );
  await act(() => changeQuestionAttachmentPreparation(key, 1));
  await click("Done");
  expect(respond).not.toHaveBeenCalled();
  expect(requests.activePendingDraftAnswers.action?.selectedOptionValues).toEqual(["completed"]);
  await act(() => changeQuestionAttachmentPreparation(key, -1));
  expect(respond).not.toHaveBeenCalled();
  await click("Done");
  expect(respond).toHaveBeenCalledTimes(1);
});

it("uses digit shortcuts to advance and send, while ignoring held and composing keys", async () => {
  await using _screen = await render(
    <Questions questions={[question("first"), question("second")]} />,
  );
  const digit = (extra = {}) =>
    act(() =>
      document.dispatchEvent(
        new KeyboardEvent("keydown", { key: "1", bubbles: true, cancelable: true, ...extra }),
      ),
    );
  await digit();
  expect(requests.activePendingQuestionIndex).toBe(1);
  await digit({ repeat: true });
  await digit({ isComposing: true });
  expect(respond).not.toHaveBeenCalled();
  await digit();
  expect(respond).toHaveBeenCalledTimes(1);
});

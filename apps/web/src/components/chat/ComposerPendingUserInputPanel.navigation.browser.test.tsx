import * as Cause from "effect/Cause";
import { AsyncResult } from "effect/reactivity";
import { type PendingUserInputDraftAnswer } from "../../pendingUserInput";
import { togglePendingUserInputOptionSelection } from "../../pendingUserInput";
import { ComposerPendingUserInputPanel } from "./ComposerPendingUserInputPanel";
import {
  EnvironmentId,
  NodeId,
  RuntimeRequestId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import { useLayoutEffect, useState, type ComponentProps } from "react";
import { expect, it, vi } from "vite-plus/test";

vi.mock(import("../ChatMarkdown"), async (importOriginal) => ({
  ...(await importOriginal()),
  default: (({ text }: { text: string }) => <p>{text}</p>) as never,
}));

import { act, fakeTimers, render } from "../../../test/render";
import { makeThreadProjectionFixture } from "../../test-fixtures";
import { usePendingThreadRequests } from "./usePendingThreadRequests";

const { respond } = vi.hoisted(() => ({ respond: vi.fn() }));
vi.mock(import("../../state/use-orchestration-command"), () => ({
  useOrchestrationCommand: () => respond,
}));
vi.mock(import("../../state/session"), async (original) => ({
  ...(await original()),
  readEnvironmentScope: () => true,
}));

const threadRef = { environmentId: EnvironmentId.make("env"), threadId: ThreadId.make("thread") };

const prompt = {
  requestId: RuntimeRequestId.make("question-navigation"),
  createdAt: "2026-10-09T00:00:00.000Z",
  dismissible: false,
  responseCapability: "live" as const,
  questions: [0, 1, 2].map((index) => ({
    id: `question-${index}`,
    header: `Question ${index + 1}`,
    question: `Choose for question ${index + 1}`,
    options: [{ label: "Incremental", description: "One module at a time" }],
    multiSelect: false,
  })),
};

function Questions({ initialIndex = 0 }: { initialIndex?: number }) {
  const [questionIndex, setQuestionIndex] = useState(initialIndex);
  const [answers, setAnswers] = useState<Record<string, PendingUserInputDraftAnswer>>({});
  const advance = () => setQuestionIndex(questionIndex + 1);
  return (
    <>
      <output>{questionIndex}</output>
      <button onClick={advance}>Next</button>
      <button onClick={() => setQuestionIndex(questionIndex - 1)}>Previous</button>
      <ComposerPendingUserInputPanel
        pendingUserInputs={[prompt]}
        cwd="/repo"
        threadRef={threadRef}
        disabled={false}
        respondingRequestIds={[]}
        answers={answers}
        questionIndex={questionIndex}
        onSelectQuestion={setQuestionIndex}
        onAdvance={advance}
        onToggleOption={(questionId, value) => {
          const question = prompt.questions.find((entry) => entry.id === questionId)!;
          setAnswers((existing) => ({
            ...existing,
            [questionId]: togglePendingUserInputOptionSelection(
              question,
              existing[questionId],
              value,
            ),
          }));
        }}
        onChangeCustomAnswer={() => {}}
        onPrevious={() => {}}
        onDismiss={() => {}}
      />
    </>
  );
}

function questionProjection(threadId: ThreadId, requestIds = ["first", "second"]) {
  const projection = makeThreadProjectionFixture();
  return {
    ...projection,
    thread: { ...projection.thread, id: threadId },
    runtimeRequests: requestIds.map((id) => ({
      id: RuntimeRequestId.make(id),
      nodeId: NodeId.make(id),
      providerTurnId: null,
      nativeRequestRef: null,
      kind: "user_input" as const,
      status: "pending" as const,
      responseCapability: { type: "message" as const },
      createdAt: projection.updatedAt,
      resolvedAt: null,
    })),
    turnItems: requestIds.map((id, ordinal) => ({
      id: TurnItemId.make(id),
      threadId,
      runId: null,
      nodeId: NodeId.make(id),
      providerThreadId: null,
      providerTurnId: null,
      nativeItemRef: null,
      parentItemId: null,
      ordinal,
      status: "completed" as const,
      title: null,
      startedAt: projection.updatedAt,
      completedAt: projection.updatedAt,
      updatedAt: projection.updatedAt,
      type: "user_input_request" as const,
      requestId: RuntimeRequestId.make(id),
      questions: prompt.questions,
    })),
  } satisfies OrchestrationV2ThreadProjection;
}

let requests: ReturnType<typeof usePendingThreadRequests>;

function PendingRequestsProbe({
  environmentId,
  projection,
  loading = false,
}: {
  environmentId: EnvironmentId;
  projection: OrchestrationV2ThreadProjection;
  loading?: boolean;
}) {
  const state = usePendingThreadRequests({
    environmentId,
    activeThreadId: projection.thread.id,
    activeThread: projection.thread,
    serverProjection: loading ? null : projection,
    supportsQuestionAttachments: false,
    composerDraftTarget: { environmentId, threadId: projection.thread.id },
    setComposerDraftPrompt: () => {},
    setThreadError: () => {},
  });
  useLayoutEffect(() => {
    requests = state;
  });
  return null;
}

it("retains answers, selections and question position after leaving chat, including while the projection reloads", async () => {
  using _timers = fakeTimers();
  const environmentId = EnvironmentId.make("remount-environment");
  const projection = questionProjection(threadRef.threadId);
  const probe = (loading = false) => (
    <PendingRequestsProbe environmentId={environmentId} projection={projection} loading={loading} />
  );
  await using screen = await render(probe());
  await act(() =>
    requests.onChangeActivePendingUserInputCustomAnswer("question-0", "Keep my text"),
  );
  await act(() => requests.onAdvanceActivePendingUserInput());
  await act(() => requests.onSelectActivePendingUserInputOption("question-1", "Incremental"));
  await act(() => requests.onAdvanceActivePendingUserInput());
  const expected = {
    activePendingQuestionIndex: 2,
    activePendingDraftAnswers: {
      "question-0": { customAnswer: "Keep my text" },
      "question-1": { selectedOptionValues: ["Incremental"] },
    },
  };
  expect(requests).toMatchObject(expected);

  // New thread and Settings both unmount the chat that owns this hook.
  await screen.rerender(null);
  await screen.rerender(probe());
  expect(requests).toMatchObject(expected);

  await screen.rerender(null);
  await screen.rerender(probe(true));
  expect(requests.activePendingUserInput).toBeNull();
  await screen.rerender(probe());
  expect(requests).toMatchObject(expected);
});

it("isolates drafts by environment, thread and request and discards only resolved requests", async () => {
  using _timers = fakeTimers();
  const environmentId = EnvironmentId.make("scope-environment");
  const projection = questionProjection(threadRef.threadId);
  const scopes = [
    { environmentId, projection },
    { environmentId: EnvironmentId.make("other-environment"), projection },
    { environmentId, projection: questionProjection(ThreadId.make("other-thread")) },
    { environmentId, projection: questionProjection(threadRef.threadId, ["second", "first"]) },
  ];
  await using screen = await render(null);
  const show = (scope: ComponentProps<typeof PendingRequestsProbe>) =>
    screen.rerender(<PendingRequestsProbe {...scope} />);
  for (const [index, scope] of scopes.entries()) {
    await show(scope);
    expect(requests.activePendingQuestionIndex).toBe(0);
    expect(requests.activePendingDraftAnswers["question-0"]?.customAnswer).toBeUndefined();
    await act(() =>
      requests.onChangeActivePendingUserInputCustomAnswer("question-0", `Answer ${index}`),
    );
    await act(() => requests.onAdvanceActivePendingUserInput());
  }

  await show({
    environmentId,
    projection: {
      ...projection,
      runtimeRequests: projection.runtimeRequests.map((request) =>
        request.id === "first"
          ? { ...request, status: "resolved", resolvedAt: projection.updatedAt }
          : request,
      ),
    },
  });
  expect(requests.activePendingUserInput?.requestId).toBe("second");
  for (const [index, scope] of scopes.entries()) {
    await show(scope);
    expect(requests.activePendingQuestionIndex).toBe(index === 0 ? 0 : 1);
    expect(requests.activePendingDraftAnswers["question-0"]?.customAnswer).toBe(
      index === 0 ? undefined : `Answer ${index}`,
    );
  }
});

function buttonWithText(text: string) {
  const button = [...document.querySelectorAll("button")].find((element) =>
    element.textContent?.includes(text),
  );
  expect(button, `button ${text}`).toBeDefined();
  return button!;
}

async function chooseOption() {
  await act(async () => buttonWithText("Incremental").click());
}

async function navigate(label: string) {
  await act(async () => buttonWithText(label).click());
}

function activeIndex() {
  return document.querySelector("output")!.textContent;
}

it("selects without advancing and waits for manual continuation", async () => {
  using _timers = fakeTimers();
  await using _screen = await render(<Questions />);
  await chooseOption();
  await navigate("Next");
  expect(activeIndex()).toBe("1");
  await act(async () => vi.advanceTimersByTime(200));
  expect(activeIndex()).toBe("1");

  await chooseOption();
  await act(async () => vi.advanceTimersByTime(200));
  expect(activeIndex()).toBe("1");
});

it("preserves previous-question navigation after a selection", async () => {
  using _timers = fakeTimers();
  await using _screen = await render(<Questions initialIndex={1} />);
  await chooseOption();
  await navigate("Previous");
  await act(async () => vi.advanceTimersByTime(200));
  expect(activeIndex()).toBe("0");
});

it("never auto-advances after the answer draft rerenders the same question", async () => {
  using _timers = fakeTimers();
  await using _screen = await render(<Questions />);
  await act(() =>
    document.dispatchEvent(
      new KeyboardEvent("keydown", { key: "1", bubbles: true, cancelable: true }),
    ),
  );
  expect(document.querySelector('button[aria-pressed="true"]')).not.toBeNull();
  await chooseOption();
  await act(async () => vi.advanceTimersByTime(200));
  expect(activeIndex()).toBe("0");
  await act(async () => vi.advanceTimersByTime(200));
  expect(activeIndex()).toBe("0");
});

it("guards duplicate sends and preserves drafts while clearing sending state after failure", async () => {
  const projection = questionProjection(ThreadId.make("failed-submission"), ["failure"]);
  const environmentId = EnvironmentId.make("failure-env");
  let finish!: (value: unknown) => void;
  respond.mockReset();
  respond.mockImplementation(
    () =>
      new Promise((resolve) => {
        finish = resolve;
      }),
  );
  await using _screen = await render(
    <PendingRequestsProbe environmentId={environmentId} projection={projection} />,
  );
  for (const question of prompt.questions)
    await act(() =>
      requests.onChangeActivePendingUserInputCustomAnswer(question.id, `Answer ${question.id}`),
    );
  await act(() => requests.onSelectActivePendingUserInputQuestion(2));
  await act(() => {
    requests.onAdvanceActivePendingUserInput();
    requests.onAdvanceActivePendingUserInput();
  });
  expect(respond).toHaveBeenCalledTimes(1);
  expect(requests.activePendingIsResponding).toBe(true);
  await act(async () =>
    finish(AsyncResult.failure(Cause.fail(new Error("Synthetic response failure")))),
  );
  expect(requests.activePendingIsResponding).toBe(false);
  expect(requests.activePendingResolvedAnswers).toEqual(
    Object.fromEntries(prompt.questions.map((q) => [q.id, `Answer ${q.id}`])),
  );
  await act(() => requests.onAdvanceActivePendingUserInput());
  expect(respond).toHaveBeenCalledTimes(2);
  await act(async () => finish(AsyncResult.success(undefined)));
  expect(requests.activePendingIsResponding).toBe(false);
});

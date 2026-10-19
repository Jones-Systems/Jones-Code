import { seedUserInputDraftAnswers } from "@t3tools/client-runtime/state/thread-requests";
import {
  EnvironmentId,
  RuntimeRequestId,
  ThreadId,
  type UserInputQuestion,
} from "@t3tools/contracts";
import { act, useState } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";
import { ComposerPendingUserInputPanel } from "../../components/chat/ComposerPendingUserInputPanel";
import {
  buildPendingUserInputAnswers,
  setPendingUserInputCustomAnswer,
  togglePendingUserInputOptionSelection,
  type PendingUserInputDraftAnswer,
} from "../../pendingUserInput";

vi.mock(import("../../components/ChatMarkdown"), async (importOriginal) => ({
  ...(await importOriginal()),
  default: (({ text }: { text: string }) => <p>{text}</p>) as never,
}));

const threadRef = {
  environmentId: EnvironmentId.make("question-test"),
  threadId: ThreadId.make("thread"),
};
const question: UserInputQuestion = {
  id: "approach",
  header: "Approach",
  question: "Which approach?",
  options: [{ label: "Incremental", description: "One module at a time" }],
  multiSelect: false,
};

const DEFAULT_QUESTIONS = [question];

function Harness({
  questions = DEFAULT_QUESTIONS,
  disabled = false,
  capability = "live",
  dismissible = false,
}: {
  questions?: UserInputQuestion[];
  disabled?: boolean;
  capability?: "live" | "message" | "not_resumable";
  dismissible?: boolean;
}) {
  const [draft, setDraft] = useState("independent thread draft");
  const [messages, setMessages] = useState<string[]>([]);
  const [answers, setAnswers] = useState<Record<string, PendingUserInputDraftAnswer>>(() =>
    seedUserInputDraftAnswers(questions, {}),
  );
  const [index, setIndex] = useState(0);
  const [result, setResult] = useState<Record<string, string | string[]> | null>(null);
  const [dismissed, setDismissed] = useState(false);
  const cardCallbacks = {
    onChangeCustomAnswer: (id: string, value: string) =>
      setAnswers((existing) => ({
        ...existing,
        [id]: setPendingUserInputCustomAnswer(
          existing[id],
          value,
          questions.find((q) => q.id === id),
        ),
      })),
    onPrevious: () => setIndex((i) => Math.max(0, i - 1)),
  };
  return (
    <form
      onSubmit={(event) => {
        event.preventDefault();
        setMessages((existing) => [...existing, draft]);
        setDraft("");
      }}
    >
      {!result && !dismissed && (
        <ComposerPendingUserInputPanel
          pendingUserInputs={[
            {
              requestId: RuntimeRequestId.make("request"),
              createdAt: "2026-10-19T00:00:00.000Z",
              responseCapability: capability,
              dismissible,
              questions: questions.map((q) => ({ ...q, multiSelect: q.multiSelect ?? false })),
            },
          ]}
          cwd="/repo"
          threadRef={threadRef}
          disabled={disabled}
          respondingRequestIds={[]}
          answers={answers}
          questionIndex={index}
          {...cardCallbacks}
          onToggleOption={(id, value) =>
            setAnswers((existing) => ({
              ...existing,
              [id]: togglePendingUserInputOptionSelection(
                questions.find((q) => q.id === id)!,
                existing[id],
                value,
              ),
            }))
          }
          onAdvance={() => {
            if (index < questions.length - 1) setIndex(index + 1);
            else setResult(buildPendingUserInputAnswers(questions, answers));
          }}
          onDismiss={() => setDismissed(true)}
        />
      )}
      <textarea
        aria-label="Thread message"
        value={draft}
        onChange={(event) => setDraft(event.target.value)}
      />
      <button type="submit">Send thread message</button>
      <output data-testid="answer">{JSON.stringify(result)}</output>
      <output data-testid="messages">{JSON.stringify(messages)}</output>
      <output data-testid="dismissed">{String(dismissed)}</output>
    </form>
  );
}

let root: Root;
let container: HTMLDivElement;
beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
});
afterEach(async () => {
  await act(async () => root.unmount());
  container.remove();
  vi.unstubAllGlobals();
  vi.useRealTimers();
});

function answerField() {
  const field = container.querySelector<HTMLTextAreaElement>('textarea[aria-label="Your answer"]');
  expect(field, "the question card has its own written-answer field").not.toBeNull();
  return field!;
}
async function typeAnswer(value: string) {
  const field = answerField();
  await act(async () => {
    Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, "value")!.set!.call(
      field,
      value,
    );
    field.dispatchEvent(new Event("input", { bubbles: true }));
  });
}
function button(label: string) {
  return [...container.querySelectorAll("button")].find((b) => b.textContent === label)!;
}
function output(id: string) {
  return container.querySelector(`[data-testid="${id}"]`)!.textContent;
}

it("answers inside the card without sending or changing the independent thread draft", async () => {
  await act(async () => root.render(<Harness />));
  await typeAnswer("My written answer");
  await act(async () => button("Send").click());
  expect(output("answer")).toBe(JSON.stringify({ approach: "My written answer" }));
  expect(output("messages")).toBe("[]");
  expect(container.querySelector<HTMLTextAreaElement>('[aria-label="Thread message"]')!.value).toBe(
    "independent thread draft",
  );
});

it("sending the thread message leaves the question and its written answer pending", async () => {
  await act(async () => root.render(<Harness />));
  await typeAnswer("Keep this answer");
  await act(async () => button("Send thread message").click());
  expect(output("answer")).toBe("null");
  expect(output("messages")).toBe('["independent thread draft"]');
  expect(answerField().value).toBe("Keep this answer");
});

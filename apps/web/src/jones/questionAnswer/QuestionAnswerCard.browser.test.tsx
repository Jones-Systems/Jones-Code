import { act, render as renderTest, type Screen } from "../../../test/render";
import { seedUserInputDraftAnswers } from "@t3tools/client-runtime/state/thread-requests";
import {
  EnvironmentId,
  RuntimeRequestId,
  ThreadId,
  type UserInputQuestion,
} from "@t3tools/contracts";
import { useState } from "react";
import { expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";
import { ComposerSurface } from "../../components/chat/ComposerSurface";
import { ComposerPendingUserInputPanel } from "../../components/chat/ComposerPendingUserInputPanel";
import { ComposerBanner } from "../../components/chat/ComposerBanner";
import "@fontsource-variable/inter";
import "../../index.css";
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
  layout = false,
  initialIndex = 0,
  initialAnswers,
}: {
  questions?: UserInputQuestion[];
  disabled?: boolean;
  capability?: "live" | "message" | "not_resumable";
  dismissible?: boolean;
  layout?: boolean;
  initialIndex?: number;
  initialAnswers?: Record<string, PendingUserInputDraftAnswer>;
}) {
  const [draft, setDraft] = useState("independent thread draft");
  const [messages, setMessages] = useState<string[]>([]);
  const [answers, setAnswers] = useState<Record<string, PendingUserInputDraftAnswer>>(
    () => initialAnswers ?? seedUserInputDraftAnswers(questions, {}),
  );
  const [index, setIndex] = useState(initialIndex);
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
          { notes: capability === "message" },
        ),
      })),
    onPrevious: () => setIndex((i) => Math.max(0, i - 1)),
  };
  const card = !result && !dismissed && (
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
      onSelectQuestion={setIndex}
      {...cardCallbacks}
      onToggleOption={(id, value) =>
        setAnswers((existing) => ({
          ...existing,
          [id]: togglePendingUserInputOptionSelection(
            questions.find((q) => q.id === id)!,
            existing[id],
            value,
            { notes: capability === "message" },
          ),
        }))
      }
      onAdvance={() => {
        if (index < questions.length - 1) setIndex(index + 1);
        else
          setResult(
            buildPendingUserInputAnswers(questions, answers, {
              notes: capability === "message",
            }),
          );
      }}
      onDismiss={() => setDismissed(true)}
    />
  );
  return (
    <form
      onSubmit={(event) => {
        event.preventDefault();
        setMessages((existing) => [...existing, draft]);
        setDraft("");
      }}
    >
      {layout ? (
        <ComposerSurface.Shell>
          <ComposerBanner.Attachment>
            <ComposerBanner.Root data-testid="layout-card">{card}</ComposerBanner.Root>
          </ComposerBanner.Attachment>
          <ComposerSurface.Host>
            <ComposerSurface.Main>
              <div className="p-4" data-testid="thread-composer">
                Independent thread composer
              </div>
            </ComposerSurface.Main>
          </ComposerSurface.Host>
        </ComposerSurface.Shell>
      ) : (
        card
      )}
      <div hidden={layout}>
        <textarea
          aria-label="Thread message"
          value={draft}
          onChange={(event) => setDraft(event.target.value)}
        />
        <button type="submit">Send thread message</button>
        <output data-testid="answer">{JSON.stringify(result)}</output>
        <output data-testid="messages">{JSON.stringify(messages)}</output>
        <output data-testid="dismissed">{String(dismissed)}</output>
      </div>
    </form>
  );
}

let root: Screen;
let container: HTMLDivElement;
async function setupTest(): Promise<AsyncDisposable> {
  let initialized = false;
  let ownedContainer: HTMLDivElement | undefined;
  async function cleanup() {
    const screen = initialized ? root : undefined;
    try {
      await act(async () => screen?.unmount());
    } finally {
      try {
        await screen?.[Symbol.asyncDispose]();
      } finally {
        ownedContainer?.remove();
        vi.unstubAllGlobals();
        vi.useRealTimers();
      }
    }
  }
  try {
    ownedContainer = container = document.createElement("div");
    document.body.append(container);
    root = await renderTest(null, { container: container, router: false });
    initialized = true;

    return { [Symbol.asyncDispose]: cleanup };
  } catch (error) {
    await cleanup();
    throw error;
  }
}

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
  await using _fixture = await setupTest();
  await act(async () => await root.rerender(<Harness />));
  await typeAnswer("My written answer");
  await act(async () => button("Send").click());
  expect(output("answer")).toBe(JSON.stringify({ approach: "My written answer" }));
  expect(output("messages")).toBe("[]");
  expect(container.querySelector<HTMLTextAreaElement>('[aria-label="Thread message"]')!.value).toBe(
    "independent thread draft",
  );
});

it("sending the thread message leaves the question and its written answer pending", async () => {
  await using _fixture = await setupTest();
  await act(async () => await root.rerender(<Harness />));
  await typeAnswer("Keep this answer");
  await act(async () => button("Send thread message").click());
  expect(output("answer")).toBe("null");
  expect(output("messages")).toBe('["independent thread draft"]');
  expect(answerField().value).toBe("Keep this answer");
});

it("aligns the one-line answer and Send with the option content, then caps growth at ten lines", async () => {
  await using _fixture = await setupTest();
  container.style.width = "680px";
  await act(
    async () =>
      await root.rerender(
        <ComposerBanner.Root>
          <Harness />
        </ComposerBanner.Root>,
      ),
  );
  const field = answerField();
  const style = getComputedStyle(field);
  const lineHeight = Number.parseFloat(style.lineHeight);
  const inset =
    Number.parseFloat(style.paddingTop) +
    Number.parseFloat(style.paddingBottom) +
    Number.parseFloat(style.borderTopWidth) +
    Number.parseFloat(style.borderBottomWidth);
  const optionText = [...container.querySelectorAll("span")].find(
    (span) => span.textContent === "Incremental",
  )!;
  const option = optionText.closest("button")!;
  const optionStyle = getComputedStyle(option);
  const initial = field.getBoundingClientRect();
  const send = button("Send").getBoundingClientRect();
  expect(initial.height).toBeCloseTo(lineHeight + inset, 0);
  expect(initial.left).toBeCloseTo(optionText.getBoundingClientRect().left, 0);
  expect(send.right).toBeCloseTo(
    option.getBoundingClientRect().right - Number.parseFloat(optionStyle.paddingRight),
    0,
  );
  expect(send.left).toBeGreaterThan(initial.right);
  expect(send.top).toBeGreaterThanOrEqual(initial.top);
  expect(send.bottom).toBeLessThanOrEqual(initial.bottom);

  await typeAnswer("One\nTwo");
  expect(field.getBoundingClientRect().height).toBeCloseTo(2 * lineHeight + inset, 0);
  await typeAnswer(Array.from({ length: 12 }, (_, i) => `Line ${i}`).join("\n"));
  expect(field.getBoundingClientRect().height).toBeCloseTo(10 * lineHeight + inset, 0);
  expect(field.scrollHeight).toBeGreaterThan(field.clientHeight);
  field.scrollTop = field.scrollHeight;
  expect(field.scrollTop).toBeGreaterThan(0);
  expect(getComputedStyle(field).scrollbarWidth).toBe("none");
  await typeAnswer("");
  expect(field.getBoundingClientRect().height).toBeCloseTo(initial.height, 0);
});

const layoutQuestions: UserInputQuestion[] = [1, 2, 3].map((number) => ({
  id: `question-${number}`,
  header: `Question ${number}`,
  question: `Explain the delivery approach for question ${number}, including the verification steps.`,
  options: [],
  multiSelect: false,
}));
const longAnswer = Array.from(
  { length: 34 },
  (_, index) =>
    `Step ${index + 1}: preserve the draft, verify the result, and retain the evidence.`,
)
  .join("\n")
  .slice(0, 2380);

it.each([
  { width: 360, fontSize: 16 },
  { width: 360, fontSize: 32 },
  { width: 1280, fontSize: 16 },
  { width: 1280, fontSize: 32 },
  { width: 320, fontSize: 16 },
])(
  "keeps long-answer controls reachable at $width px with $fontSize px root text",
  async ({ width, fontSize }) => {
    const viewport = { width: window.innerWidth, height: window.innerHeight };
    const originalFontSize = document.documentElement.style.fontSize;
    try {
      await page.viewport(width, 800);
      document.documentElement.style.fontSize = `${(fontSize / 16) * 100}%`;
      await using _fixture = await setupTest();
      Object.assign(container.style, {
        position: "fixed",
        inset: "0",
        display: "flex",
        flexDirection: "column",
        justifyContent: "flex-end",
        paddingBottom: "16px",
      });
      await act(
        async () =>
          await root.rerender(
            <Harness
              questions={layoutQuestions}
              capability="message"
              layout
              initialIndex={2}
              initialAnswers={{
                "question-1": { customAnswer: "First answer" },
                "question-2": { customAnswer: "Second answer" },
                "question-3": { customAnswer: longAnswer },
              }}
            />,
          ),
      );
      await document.fonts.ready;
      const card = container.querySelector<HTMLElement>('[data-testid="layout-card"]')!;
      const scroll = card.querySelector<HTMLElement>('[data-slot="scroll-area-viewport"]')!;
      const composer = container.querySelector<HTMLElement>('[data-testid="thread-composer"]')!;
      expect(card.getBoundingClientRect().height).toBeLessThanOrEqual(400);
      expect(card.getBoundingClientRect().top).toBeGreaterThanOrEqual(0);
      expect(composer.getBoundingClientRect().bottom).toBeLessThanOrEqual(800);
      expect(scroll.scrollWidth).toBeLessThanOrEqual(scroll.clientWidth + 1);
      const reachable = (element: HTMLElement) => {
        element.scrollIntoView({ block: "nearest", inline: "nearest" });
        const bounds = element.getBoundingClientRect();
        const visible = (
          element.closest<HTMLElement>('[data-slot="scroll-area-viewport"]') ?? card
        ).getBoundingClientRect();
        expect(bounds.left).toBeGreaterThanOrEqual(visible.left - 1);
        expect(bounds.right).toBeLessThanOrEqual(visible.right + 1);
        expect(bounds.top).toBeGreaterThanOrEqual(Math.max(0, visible.top) - 1);
        expect(bounds.bottom).toBeLessThanOrEqual(Math.min(800, visible.bottom) + 1);
      };
      const prompt = [...card.querySelectorAll("p")].find(
        (p) => p.textContent === layoutQuestions[2]!.question,
      )!;
      reachable(prompt);
      const progress = [
        ...card.querySelectorAll<HTMLButtonElement>('button[aria-label^="Question "]'),
      ];
      expect(progress).toHaveLength(3);
      for (const control of progress) reachable(control);
      const back = button("Back");
      reachable(back);
      await act(async () => back.click());
      expect(answerField().value).toBe("Second answer");
      await act(async () =>
        card.querySelector<HTMLButtonElement>('[aria-label="Question 3 of 3, answered"]')!.click(),
      );
      expect(answerField().value).toBe(longAnswer);
      const field = answerField();
      expect(field.scrollHeight).toBeGreaterThan(field.clientHeight);
      field.scrollTop = field.scrollHeight;
      expect(field.scrollTop).toBeGreaterThan(0);
      const send = button("Send");
      reachable(send);
      const fieldBounds = field.getBoundingClientRect();
      const sendBounds = send.getBoundingClientRect();
      expect(
        fieldBounds.right <= sendBounds.left + 1 || fieldBounds.bottom <= sendBounds.top + 1,
      ).toBe(true);
      const fieldStyle = getComputedStyle(field);
      expect(field.clientWidth).toBeGreaterThan(
        Number.parseFloat(fieldStyle.paddingLeft) + Number.parseFloat(fieldStyle.paddingRight),
      );
      field.focus();
      await userEvent.keyboard("{Tab}");
      expect(document.activeElement).toBe(send);
      expect(send.matches(":focus-visible")).toBe(true);
      await act(async () => send.click());
      expect(JSON.parse(output("answer")!)).toEqual({
        "question-1": "First answer",
        "question-2": "Second answer",
        "question-3": longAnswer,
      });
      expect(output("messages")).toBe("[]");
      expect(
        container.querySelector<HTMLTextAreaElement>('[aria-label="Thread message"]')!.value,
      ).toBe("independent thread draft");
    } finally {
      document.documentElement.style.fontSize = originalFontSize;
      await page.viewport(viewport.width, viewport.height);
    }
  },
);

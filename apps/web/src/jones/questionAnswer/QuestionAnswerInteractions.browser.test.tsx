import { seedUserInputDraftAnswers } from "@t3tools/client-runtime/state/thread-requests";
import { useComposerDraftStore } from "../../composerDraftStore";
import {
  questionAttachmentDraftId,
  clearQuestionAttachmentDraft,
  useQuestionAttachmentPreparation,
} from "../../questionAttachments";
import { getUploadedAttachments, useAttachmentUploadStore } from "../../lib/attachmentUploadQueue";
import {
  encodeComposerContextFragment,
  COMPOSER_CONTEXT_CLIPBOARD_MIME,
} from "@t3tools/shared/composerContextClipboard";
import { threadContextRecord, threadContextReference } from "../../lib/composerContextRecords";
import { formatInlineContextReference } from "../../lib/composerContextReferences";
import { formatThreadLink } from "@t3tools/shared/threadLinks";
import {
  NO_QUESTION_ATTACHMENT_CAPABILITIES,
  type QuestionAnswerAttachmentCapabilities,
} from "./questionAnswerCard";
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
  projectPendingUserInputAnswer,
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
  capabilities = NO_QUESTION_ATTACHMENT_CAPABILITIES,
  responding = false,
}: {
  questions?: UserInputQuestion[];
  disabled?: boolean;
  capability?: "live" | "message" | "not_resumable";
  dismissible?: boolean;
  capabilities?: QuestionAnswerAttachmentCapabilities;
  responding?: boolean;
}) {
  const [draft, setDraft] = useState("independent thread draft");
  const [messages, setMessages] = useState<string[]>([]);
  const [answers, setAnswers] = useState<Record<string, PendingUserInputDraftAnswer>>(() =>
    seedUserInputDraftAnswers(questions, {}),
  );
  const [index, setIndex] = useState(0);
  const [result, setResult] = useState<Record<string, string | string[]> | null>(null);
  const [dismissed, setDismissed] = useState(false);
  const [sentAttachments, setSentAttachments] = useState<unknown>(null);
  const drafts = useComposerDraftStore((state) => state.draftsByThreadKey);
  const uploads = useAttachmentUploadStore((state) => state.uploadsByImageId);
  const preparations = useQuestionAttachmentPreparation((state) => state.counts);
  const resolvedDrafts = Object.fromEntries(
    questions.map((q) => {
      const key = questionAttachmentDraftId(
        threadRef.environmentId,
        threadRef.threadId,
        "request",
        q.id,
      );
      const draft = drafts[key];
      const files = [...(draft?.images ?? []), ...(draft?.files ?? [])];
      return [
        q.id,
        {
          ...answers[q.id],
          attachmentCount: files.length,
          attachmentsBlocked:
            (preparations[key] ?? 0) > 0 ||
            files.some((file) => uploads[file.id]?.status !== "ready"),
        },
      ];
    }),
  );
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
          respondingRequestIds={responding ? [RuntimeRequestId.make("request")] : []}
          attachmentCapabilities={capabilities}
          answers={resolvedDrafts}
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
            else {
              const result = buildPendingUserInputAnswers(questions, resolvedDrafts);
              if (!result) return;
              setResult(
                Object.fromEntries(
                  questions.map((q) => {
                    const draft =
                      drafts[
                        questionAttachmentDraftId(
                          threadRef.environmentId,
                          threadRef.threadId,
                          "request",
                          q.id,
                        )
                      ];
                    return [
                      q.id,
                      projectPendingUserInputAnswer(result[q.id], draft?.threadContexts ?? []) as
                        | string
                        | string[],
                    ];
                  }),
                ),
              );
              const draft =
                drafts[
                  questionAttachmentDraftId(
                    threadRef.environmentId,
                    threadRef.threadId,
                    "request",
                    questions[index]!.id,
                  )
                ];
              setSentAttachments(
                getUploadedAttachments({
                  environmentId: threadRef.environmentId,
                  images: [...(draft?.images ?? []), ...(draft?.files ?? [])],
                }),
              );
            }
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
      <output data-testid="attachments">{JSON.stringify(sentAttachments)}</output>
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
  for (const id of ["approach", "second"])
    clearQuestionAttachmentDraft(
      questionAttachmentDraftId(threadRef.environmentId, threadRef.threadId, "request", id),
    );
  useAttachmentUploadStore.setState({ uploadsByImageId: {} });
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

async function key(init: KeyboardEventInit) {
  const event = new KeyboardEvent("keydown", {
    key: "Enter",
    bubbles: true,
    cancelable: true,
    ...init,
  });
  await act(async () => answerField().dispatchEvent(event));
  return event;
}

it("ignores digits, Shift+Enter and IME while typing, then submits once with Enter", async () => {
  await act(async () => root.render(<Harness />));
  await typeAnswer("1 custom answer");
  await key({ key: "1" });
  expect(answerField().value).toBe("1 custom answer");
  expect((await key({ shiftKey: true })).defaultPrevented).toBe(false);
  expect((await key({ isComposing: true })).defaultPrevented).toBe(false);
  expect(output("answer")).toBe("null");
  await key({});
  expect(output("answer")).toBe('{"approach":"1 custom answer"}');
  expect(output("messages")).toBe("[]");
});

it.each(["  literal\n  ", ""])("sends literal answer %j exactly with Mod+Enter", async (value) => {
  await act(async () =>
    root.render(<Harness questions={[{ ...question, initialAnswer: "  original\n  " }]} />),
  );
  expect(answerField().value).toBe("  original\n  ");
  await typeAnswer(value);
  expect((await key({})).defaultPrevented).toBe(false);
  expect(output("answer")).toBe("null");
  await key({ ctrlKey: true });
  expect(output("answer")).toBe(JSON.stringify({ approach: value }));
  expect(output("messages")).toBe("[]");
});

it("keeps each answer across Next, Back, minimize and restore", async () => {
  await act(async () =>
    root.render(
      <Harness questions={[question, { ...question, id: "second", header: "Second" }]} />,
    ),
  );
  await typeAnswer("First answer");
  await act(async () => button("Next").click());
  await typeAnswer("Second answer");
  await act(async () => button("Back").click());
  expect(answerField().value).toBe("First answer");
  await act(async () =>
    container
      .querySelector<HTMLButtonElement>('[data-pending-user-input-toggle="expanded"]')!
      .click(),
  );
  await act(async () =>
    container
      .querySelector<HTMLButtonElement>('[data-pending-user-input-toggle="collapsed"]')!
      .click(),
  );
  expect(answerField().value).toBe("First answer");
  await act(async () => button("Next").click());
  expect(answerField().value).toBe("Second answer");
  await act(async () => button("Send").click());
  expect(output("answer")).toBe('{"approach":"First answer","second":"Second answer"}');
  expect(output("messages")).toBe("[]");
});

it("choosing an option replaces the custom answer and auto-submits without sending a message", async () => {
  vi.useFakeTimers();
  await act(async () => root.render(<Harness />));
  await typeAnswer("Discard this answer");
  const option = [...container.querySelectorAll("button")].find((b) =>
    b.textContent?.includes("Incremental"),
  )!;
  await act(async () => option.click());
  expect(answerField().value).toBe("");
  await act(async () => vi.advanceTimersByTime(200));
  expect(output("answer")).toBe('{"approach":"Incremental"}');
  expect(output("messages")).toBe("[]");
  expect(container.querySelector<HTMLTextAreaElement>('[aria-label="Thread message"]')!.value).toBe(
    "independent thread draft",
  );
});

it("typing cancels a pending option auto-advance", async () => {
  vi.useFakeTimers();
  await act(async () => root.render(<Harness />));
  await act(async () =>
    [...container.querySelectorAll("button")]
      .find((b) => b.textContent?.includes("Incremental"))!
      .click(),
  );
  await typeAnswer("Changed my mind");
  await act(async () => vi.advanceTimersByTime(200));
  expect(output("answer")).toBe("null");
  await act(async () => button("Send").click());
  expect(output("answer")).toBe('{"approach":"Changed my mind"}');
});

it.each([false, true])(
  "preserves options-only selection with multiSelect=%s",
  async (multiSelect) => {
    vi.useFakeTimers();
    await act(async () =>
      root.render(<Harness questions={[{ ...question, allowCustomAnswer: false, multiSelect }]} />),
    );
    expect(container.querySelector('[aria-label="Your answer"]')).toBeNull();
    await act(async () =>
      [...container.querySelectorAll("button")]
        .find((b) => b.textContent?.includes("Incremental"))!
        .click(),
    );
    await act(async () => vi.advanceTimersByTime(200));
    if (multiSelect) {
      expect(output("answer")).toBe("null");
      await act(async () => button("Send").click());
    }
    expect(output("answer")).toBe(
      JSON.stringify({ approach: multiSelect ? ["Incremental"] : "Incremental" }),
    );
    expect(output("messages")).toBe("[]");
  },
);

it("Skip dismisses only a dismissible question without a thread submit", async () => {
  await act(async () => root.render(<Harness />));
  expect(button("Skip")).toBeUndefined();
  await act(async () => root.render(<Harness dismissible capability="message" />));
  await act(async () => button("Skip").click());
  expect(output("dismissed")).toBe("true");
  expect(output("answer")).toBe("null");
  expect(output("messages")).toBe("[]");
});

it.each([{ disabled: true }, { responding: true }, { capability: "not_resumable" as const }])(
  "blocks card actions while unavailable: %o",
  async (state) => {
    await act(async () => root.render(<Harness {...state} />));
    expect(answerField().disabled).toBe(true);
    expect(button(state.responding ? "Sending…" : "Send").disabled).toBe(true);
    await act(async () =>
      [...container.querySelectorAll("button")]
        .find((b) => b.textContent?.includes("Incremental"))!
        .click(),
    );
    expect(output("answer")).toBe("null");
  },
);

it("does not steal focus when a question arrives", async () => {
  const focused = document.createElement("textarea");
  document.body.append(focused);
  focused.focus();
  try {
    await act(async () => root.render(<Harness />));
    expect(document.activeElement).toBe(focused);
  } finally {
    focused.remove();
  }
});

const capabilities = {
  supportsQuestionAttachments: true,
  attachmentUploadsCapabilityKnown: true,
  supportsAttachmentUploads: false,
  maxFileAttachmentBytes: 1024,
};

it("pastes files into the answer draft only, blocks until ready, then submits them with the answer", async () => {
  const key = questionAttachmentDraftId(
    threadRef.environmentId,
    threadRef.threadId,
    "request",
    "approach",
  );
  await act(async () =>
    root.render(<Harness capabilities={{ ...capabilities, supportsAttachmentUploads: true }} />),
  );
  await typeAnswer("Read this file");
  const clipboardData = new DataTransfer();
  clipboardData.items.add(new File(["contents"], "answer.txt", { type: "text/plain" }));
  await act(async () =>
    answerField().dispatchEvent(
      new ClipboardEvent("paste", { clipboardData, bubbles: true, cancelable: true }),
    ),
  );
  const files = useComposerDraftStore.getState().getComposerDraft(key)!.files;
  expect(files.map((file) => file.name)).toEqual(["answer.txt"]);
  expect(useComposerDraftStore.getState().getComposerDraft(threadRef)?.files ?? []).toEqual([]);
  expect(button("Send").disabled).toBe(true);
  const file = files[0]!;
  await act(async () =>
    useAttachmentUploadStore.setState({
      uploadsByImageId: {
        [file.id]: {
          status: "ready",
          environmentId: threadRef.environmentId,
          attachmentId: "uploaded-answer",
        },
      },
    }),
  );
  await act(async () => button("Send").click());
  expect(output("answer")).toBe('{"approach":"Read this file"}');
  expect(output("attachments")).toContain("uploaded-answer");
  expect(output("messages")).toBe("[]");
});

it("shows legacy unavailable attachments and removes them without changing the thread draft", async () => {
  const key = questionAttachmentDraftId(
    threadRef.environmentId,
    threadRef.threadId,
    "request",
    "approach",
  );
  useComposerDraftStore.getState().addFiles(
    key,
    [
      {
        type: "file",
        id: "legacy-answer",
        name: "legacy.txt",
        mimeType: "text/plain",
        sizeBytes: 3,
        file: new File(["old"], "legacy.txt"),
      },
    ],
    { appendReference: false },
  );
  await act(async () => root.render(<Harness />));
  await typeAnswer("Answer without the file");
  expect(button("Send").disabled).toBe(true);
  await act(async () =>
    container.querySelector<HTMLButtonElement>('[aria-label="Remove legacy.txt"]')!.click(),
  );
  expect(useComposerDraftStore.getState().getComposerDraft(key)?.files ?? []).toEqual([]);
  expect(button("Send").disabled).toBe(false);
  expect(container.querySelector<HTMLTextAreaElement>('[aria-label="Thread message"]')!.value).toBe(
    "independent thread draft",
  );
});

it("pastes a thread reference into the answer without adding context to the thread draft", async () => {
  const record = threadContextRecord(
    { environmentId: threadRef.environmentId, threadId: ThreadId.make("related") },
    "Related thread",
  );
  const clipboardData = new DataTransfer();
  clipboardData.setData("text/plain", formatInlineContextReference(threadContextReference(record)));
  clipboardData.setData(
    COMPOSER_CONTEXT_CLIPBOARD_MIME,
    encodeComposerContextFragment({
      version: 1,
      source: { environmentId: threadRef.environmentId },
      records: [record],
    })!,
  );
  await act(async () => root.render(<Harness />));
  await act(async () =>
    answerField().dispatchEvent(
      new ClipboardEvent("paste", { clipboardData, bubbles: true, cancelable: true }),
    ),
  );
  expect(
    useComposerDraftStore.getState().getComposerDraft(threadRef)?.threadContexts ?? [],
  ).toEqual([]);
  await act(async () => button("Send").click());
  expect(output("answer")).toBe(
    JSON.stringify({ approach: formatThreadLink(record.threadId, "Related thread") }),
  );
  expect(output("messages")).toBe("[]");
});

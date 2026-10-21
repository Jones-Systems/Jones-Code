import { readPreparedConnection } from "../../state/session";
import { readThreadShell } from "../../state/entities";
import { THREAD_CONTEXT_DROP_EVENT } from "../../components/chat/threadContextDrag";
import { listenForWorkspaceFileDragEnd } from "./workspaceFileDragLifecycle";
import { makeWorkspaceFileDropHandlers } from "../../components/chat/workspaceFileDrop";
import { seedUserInputDraftAnswers } from "@t3tools/client-runtime/state/thread-requests";
import { useComposerDraftStore } from "../../composerDraftStore";
import {
  questionAttachmentDraftId,
  clearQuestionAttachmentDraft,
  useQuestionAttachmentPreparation,
} from "../../questionAttachments";
import { getUploadedAttachments, useAttachmentUploadStore } from "../../lib/attachmentUploadQueue";
import { prepareImageForAttachment } from "../../lib/imageCompression";
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
import { act, useEffect, useState } from "react";
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

vi.mock(import("../../state/entities"), async (importOriginal) => ({
  ...(await importOriginal()),
  readThreadShell: vi.fn(),
}));

vi.mock(import("../../lib/imageCompression"), async (importOriginal) => {
  const original = await importOriginal();
  return { ...original, prepareImageForAttachment: vi.fn(original.prepareImageForAttachment) };
});

const { excerptRequest } = vi.hoisted(() => ({ excerptRequest: vi.fn() }));
vi.mock(import("../../state/use-atom-command"), () => ({ useAtomCommand: () => excerptRequest }));
vi.mock(import("../../state/session"), async (importOriginal) => ({
  ...(await importOriginal()),
  readPreparedConnection: vi.fn(() => ({ httpBaseUrl: "http://synthetic.invalid" }) as never),
}));
const capturedExcerpt = {
  status: "captured" as const,
  capturedAt: "2026-10-21T00:00:00.000Z",
  sourceSnapshotSequence: 12,
  earlierHistory: "omitted" as const,
  text: "Synthetic bounded thread excerpt",
};

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
                      projectPendingUserInputAnswer(
                        result[q.id],
                        draft?.threadContexts ?? [],
                        threadRef.environmentId,
                      ) as string | string[],
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
  vi.mocked(readPreparedConnection).mockReturnValue({
    httpBaseUrl: "http://synthetic.invalid",
  } as never);
  excerptRequest.mockReset();
  excerptRequest.mockImplementation(async ({ input }: { input: { threadId: string } }) => ({
    _tag: "Success",
    value: { threadId: input.threadId, excerpt: capturedExcerpt },
  }));
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
  vi.restoreAllMocks();
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

it("omits Skip and dismisses only through the question header without a thread submit", async () => {
  await act(async () => root.render(<Harness />));
  expect(button("Skip")).toBeUndefined();
  await act(async () => root.render(<Harness dismissible capability="message" />));
  expect(button("Skip")).toBeUndefined();
  await act(async () =>
    container.querySelector<HTMLButtonElement>("[data-pending-user-input-dismiss]")!.click(),
  );
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

it("uses the inline paperclip picker for the question while preserving normal attachments", async () => {
  const normalFile = {
    type: "file" as const,
    id: "normal-file",
    name: "normal.txt",
    mimeType: "text/plain",
    sizeBytes: 6,
    file: new File(["normal"], "normal.txt", { type: "text/plain" }),
  };
  const store = useComposerDraftStore.getState();
  store.addFiles(threadRef, [normalFile], { appendReference: false });
  try {
    await act(async () =>
      root.render(<Harness capabilities={{ ...capabilities, supportsAttachmentUploads: true }} />),
    );
    await typeAnswer("Question response");
    const picker = container.querySelector<HTMLInputElement>('input[type="file"]')!;
    const paperclip = container.querySelector<HTMLButtonElement>(
      'button[aria-label="Attach files to answer"]',
    )!;
    const nativePickerClick = vi.spyOn(picker, "click").mockImplementation(() => {});
    try {
      await act(async () => paperclip.click());
      expect(nativePickerClick).toHaveBeenCalledOnce();
    } finally {
      nativePickerClick.mockRestore();
    }
    const transfer = new DataTransfer();
    transfer.items.add(new File(["answer"], "question.txt", { type: "text/plain" }));
    await act(async () => {
      picker.files = transfer.files;
      picker.dispatchEvent(new Event("change", { bubbles: true }));
    });
    const questionKey = questionAttachmentDraftId(
      threadRef.environmentId,
      threadRef.threadId,
      "request",
      "approach",
    );
    const files = useComposerDraftStore.getState().getComposerDraft(questionKey)!.files;
    expect(files.map((file) => file.name)).toEqual(["question.txt"]);
    expect(
      useComposerDraftStore
        .getState()
        .getComposerDraft(threadRef)!
        .files.map((file) => file.name),
    ).toEqual(["normal.txt"]);
    expect(button("Send").disabled).toBe(true);
    const attachmentId = "uploaded-question-picker";
    await act(async () =>
      useAttachmentUploadStore.setState({
        uploadsByImageId: {
          [files[0]!.id]: { status: "ready", environmentId: threadRef.environmentId, attachmentId },
        },
      }),
    );
    await act(async () => button("Send").click());
    expect(output("attachments")).toContain(attachmentId);
    expect(output("attachments")).not.toContain("normal-file");
    expect(output("answer")).toBe('{"approach":"Question response"}');
    expect(output("messages")).toBe("[]");
    expect(
      useComposerDraftStore
        .getState()
        .getComposerDraft(threadRef)!
        .files.map((file) => file.name),
    ).toEqual(["normal.txt"]);
  } finally {
    useComposerDraftStore.getState().clearDraftThread(threadRef);
  }
});

it("releases a failed image preparation so the written answer can still be sent", async () => {
  const key = questionAttachmentDraftId(
    threadRef.environmentId,
    threadRef.threadId,
    "request",
    "approach",
  );
  let rejectPreparation!: (reason: Error) => void;
  vi.mocked(prepareImageForAttachment).mockImplementationOnce(
    () =>
      new Promise((_resolve, reject) => {
        rejectPreparation = reject;
      }),
  );
  await act(async () => root.render(<Harness capabilities={capabilities} />));
  await typeAnswer("Answer survives attachment failure");
  const clipboardData = new DataTransfer();
  clipboardData.items.add(new File(["image"], "failed.png", { type: "image/png" }));
  await act(async () =>
    answerField().dispatchEvent(
      new ClipboardEvent("paste", { clipboardData, bubbles: true, cancelable: true }),
    ),
  );
  expect(useQuestionAttachmentPreparation.getState().counts[key]).toBe(1);
  expect(button("Send").disabled).toBe(true);
  await act(async () => rejectPreparation(new Error("Image preparation failed")));
  expect(useQuestionAttachmentPreparation.getState().counts[key] ?? 0).toBe(0);
  expect(container.textContent).toContain("'failed.png' could not be attached.");
  expect(useComposerDraftStore.getState().getComposerDraft(key)?.images ?? []).toEqual([]);
  expect(useComposerDraftStore.getState().getComposerDraft(threadRef)?.images ?? []).toEqual([]);
  expect(button("Send").disabled).toBe(false);
  await act(async () => button("Send").click());
  expect(output("answer")).toBe('{"approach":"Answer survives attachment failure"}');
  expect(output("messages")).toBe("[]");
  expect(container.querySelector<HTMLTextAreaElement>('[aria-label="Thread message"]')!.value).toBe(
    "independent thread draft",
  );
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

it.each(["question-test", "foreign-environment"])(
  "pastes a %s thread with bounded source context without exporting or uploading files",
  async (source) => {
    const fetchSpy = vi.spyOn(globalThis, "fetch");
    const record = threadContextRecord(
      { environmentId: EnvironmentId.make(source), threadId: ThreadId.make("related") },
      "Related thread",
    );
    const clipboardData = new DataTransfer();
    clipboardData.setData(
      "text/plain",
      formatInlineContextReference(threadContextReference(record)),
    );
    clipboardData.setData(
      COMPOSER_CONTEXT_CLIPBOARD_MIME,
      encodeComposerContextFragment({
        version: 1,
        source: { environmentId: record.environmentId },
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
    const answerDraft = useComposerDraftStore
      .getState()
      .getComposerDraft(
        questionAttachmentDraftId(
          threadRef.environmentId,
          threadRef.threadId,
          "request",
          question.id,
        ),
      );
    expect(answerDraft?.threadContexts).toEqual([{ ...record, excerpt: capturedExcerpt }]);
    expect(excerptRequest).toHaveBeenCalledWith({
      environmentId: record.environmentId,
      input: { threadId: record.threadId },
    });
    expect(answerDraft?.files ?? []).toEqual([]);
    expect(answerField().value).toBe(formatInlineContextReference(threadContextReference(record)));
    expect(fetchSpy).not.toHaveBeenCalled();
    fetchSpy.mockRestore();
    await act(async () => button("Send").click());
    const sentAnswer = JSON.parse(output("answer")!) as { approach: string };
    expect(sentAnswer.approach).toContain(
      source === "question-test"
        ? formatThreadLink(record.threadId, "Related thread")
        : "environmentId: foreign-environment",
    );
    expect(sentAnswer.approach).toContain("Synthetic bounded thread excerpt");
    expect(output("messages")).toBe("[]");
  },
);

function WorkspaceDropHarness({ onWorkspaceFiles }: { onWorkspaceFiles: (files: File[]) => void }) {
  const [active, setActive] = useState(false);
  useEffect(() => {
    if (!active) return;
    return listenForWorkspaceFileDragEnd(() => setActive(false));
  }, [active]);
  const handlers = makeWorkspaceFileDropHandlers({
    setDragActive: setActive,
    addFiles: onWorkspaceFiles,
    addFolders: onWorkspaceFiles,
  });
  return (
    <div {...handlers}>
      {active && <div data-testid="workspace-drop-overlay">Drop files to attach</div>}
      <Harness capabilities={{ ...capabilities, supportsAttachmentUploads: true }} />
    </div>
  );
}

it("clears the workspace overlay after a stopped reply drop and attaches only to the reply", async () => {
  const onWorkspaceFiles = vi.fn();
  await act(async () => root.render(<WorkspaceDropHarness onWorkspaceFiles={onWorkspaceFiles} />));
  const dataTransfer = new DataTransfer();
  dataTransfer.items.add(
    new File(["screenshot contents"], "screenshot.txt", { type: "text/plain" }),
  );
  const png = Uint8Array.from(
    atob(
      "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aWQkAAAAASUVORK5CYII=",
    ),
    (character) => character.charCodeAt(0),
  );
  dataTransfer.items.add(new File([png], "screenshot.png", { type: "image/png" }));
  await act(async () =>
    answerField().dispatchEvent(
      new DragEvent("dragenter", {
        bubbles: true,
        cancelable: true,
        dataTransfer,
      }),
    ),
  );
  expect(container.querySelector('[data-testid="workspace-drop-overlay"]')).not.toBeNull();
  await act(async () =>
    answerField().dispatchEvent(
      new DragEvent("drop", {
        bubbles: true,
        cancelable: true,
        dataTransfer,
      }),
    ),
  );
  const key = questionAttachmentDraftId(
    threadRef.environmentId,
    threadRef.threadId,
    "request",
    "approach",
  );
  expect(
    useComposerDraftStore
      .getState()
      .getComposerDraft(key)!
      .files.map((file) => file.name),
  ).toEqual(["screenshot.txt"]);
  await act(async () => {
    await expect
      .poll(() =>
        useComposerDraftStore
          .getState()
          .getComposerDraft(key)
          ?.images.map((image) => image.name),
      )
      .toEqual(["screenshot.png"]);
  });
  expect(useComposerDraftStore.getState().getComposerDraft(threadRef)?.images ?? []).toEqual([]);
  expect(onWorkspaceFiles).not.toHaveBeenCalled();
  expect(useComposerDraftStore.getState().getComposerDraft(threadRef)?.files ?? []).toEqual([]);
  expect(container.querySelector('[data-testid="workspace-drop-overlay"]')).toBeNull();
});

it("clears the workspace overlay on dragend and disposes global listeners", async () => {
  await act(async () => root.render(<WorkspaceDropHarness onWorkspaceFiles={vi.fn()} />));
  const dataTransfer = new DataTransfer();
  dataTransfer.items.add(new File(["file"], "answer.txt", { type: "text/plain" }));
  await act(async () =>
    answerField().dispatchEvent(
      new DragEvent("dragenter", {
        bubbles: true,
        cancelable: true,
        dataTransfer,
      }),
    ),
  );
  expect(container.querySelector('[data-testid="workspace-drop-overlay"]')).not.toBeNull();
  await act(async () => window.dispatchEvent(new Event("dragend")));
  expect(container.querySelector('[data-testid="workspace-drop-overlay"]')).toBeNull();
  const clearDrag = vi.fn();
  const dispose = listenForWorkspaceFileDragEnd(clearDrag);
  dispose();
  window.dispatchEvent(new Event("drop"));
  window.dispatchEvent(new Event("dragend"));
  expect(clearDrag).not.toHaveBeenCalled();
});

it.each(["question-test", "foreign-environment"])(
  "drops a %s thread without exporting or uploading its history",
  async (source) => {
    const ref = { environmentId: EnvironmentId.make(source), threadId: ThreadId.make("dropped") };
    vi.mocked(readThreadShell).mockReturnValue({ title: "Dropped thread" } as never);
    const fetchSpy = vi.spyOn(globalThis, "fetch");
    await act(async () => root.render(<Harness />));
    await act(async () =>
      container
        .querySelector("[data-thread-context-drop]")!
        .dispatchEvent(new CustomEvent(THREAD_CONTEXT_DROP_EVENT, { detail: [ref] })),
    );
    const draft = useComposerDraftStore
      .getState()
      .getComposerDraft(
        questionAttachmentDraftId(
          threadRef.environmentId,
          threadRef.threadId,
          "request",
          question.id,
        ),
      );
    const record = threadContextRecord(ref, "Dropped thread");
    expect(draft?.threadContexts).toEqual([{ ...record, excerpt: capturedExcerpt }]);
    expect(excerptRequest).toHaveBeenCalledWith({
      environmentId: ref.environmentId,
      input: { threadId: ref.threadId },
    });
    expect(draft?.files ?? []).toEqual([]);
    expect(answerField().value).toBe(
      formatInlineContextReference(threadContextReference(record)) + " ",
    );
    expect(fetchSpy).not.toHaveBeenCalled();
  },
);

it("blocks question submission while thread context is captured, then submits the snapshot", async () => {
  let resolve!: (value: unknown) => void;
  excerptRequest.mockImplementation(
    () =>
      new Promise((done) => {
        resolve = done;
      }),
  );
  const ref = {
    environmentId: EnvironmentId.make("foreign-environment"),
    threadId: ThreadId.make("pending-thread"),
  };
  vi.mocked(readThreadShell).mockReturnValue({ title: "Pending thread" } as never);
  await act(async () => root.render(<Harness />));
  await act(async () =>
    container
      .querySelector("[data-thread-context-drop]")!
      .dispatchEvent(new CustomEvent(THREAD_CONTEXT_DROP_EVENT, { detail: [ref] })),
  );
  expect(button("Send").disabled).toBe(true);
  expect(output("answer")).toBe("null");
  await act(async () =>
    resolve({ _tag: "Success", value: { threadId: ref.threadId, excerpt: capturedExcerpt } }),
  );
  expect(button("Send").disabled).toBe(false);
  await act(async () => button("Send").click());
  expect(output("answer")).toContain("Synthetic bounded thread excerpt");
});

it("copies a captured question reference without requesting a newer snapshot", async () => {
  const record = {
    ...threadContextRecord(
      {
        environmentId: EnvironmentId.make("foreign-environment"),
        threadId: ThreadId.make("copied"),
      },
      "Copied",
    ),
    excerpt: capturedExcerpt,
  };
  const clipboardData = new DataTransfer();
  clipboardData.setData("text/plain", formatInlineContextReference(threadContextReference(record)));
  clipboardData.setData(
    COMPOSER_CONTEXT_CLIPBOARD_MIME,
    encodeComposerContextFragment({
      version: 1,
      source: { environmentId: record.environmentId },
      records: [record],
    })!,
  );
  await act(async () => root.render(<Harness />));
  await act(async () =>
    answerField().dispatchEvent(
      new ClipboardEvent("paste", { clipboardData, bubbles: true, cancelable: true }),
    ),
  );
  expect(excerptRequest).not.toHaveBeenCalled();
  await act(async () => button("Send").click());
  expect(output("answer")).toContain("Synthetic bounded thread excerpt");
});

it("keeps disconnected source identity without trying the current environment", async () => {
  vi.mocked(readPreparedConnection).mockReturnValue(null);
  const ref = {
    environmentId: EnvironmentId.make("disconnected-source"),
    threadId: ThreadId.make("unavailable"),
  };
  vi.mocked(readThreadShell).mockReturnValue({ title: "Unavailable thread" } as never);
  await act(async () => root.render(<Harness />));
  await act(async () =>
    container
      .querySelector("[data-thread-context-drop]")!
      .dispatchEvent(new CustomEvent(THREAD_CONTEXT_DROP_EVENT, { detail: [ref] })),
  );
  expect(readPreparedConnection).toHaveBeenCalledWith(ref.environmentId);
  expect(excerptRequest).not.toHaveBeenCalled();
  expect(button("Send").disabled).toBe(false);
  await act(async () => button("Send").click());
  expect(output("answer")).toContain("source_disconnected");
  expect(output("answer")).toContain("environmentId: disconnected-source");
});

it("does not apply a capture after its question component unmounts", async () => {
  let resolve!: (value: unknown) => void;
  excerptRequest.mockImplementation(
    () =>
      new Promise((done) => {
        resolve = done;
      }),
  );
  const ref = {
    environmentId: EnvironmentId.make("foreign-environment"),
    threadId: ThreadId.make("late-thread"),
  };
  vi.mocked(readThreadShell).mockReturnValue({ title: "Late thread" } as never);
  await act(async () => root.render(<Harness />));
  await act(async () =>
    container
      .querySelector("[data-thread-context-drop]")!
      .dispatchEvent(new CustomEvent(THREAD_CONTEXT_DROP_EVENT, { detail: [ref] })),
  );
  await act(async () => root.render(<div />));
  await act(async () =>
    resolve({ _tag: "Success", value: { threadId: ref.threadId, excerpt: capturedExcerpt } }),
  );
  const target = questionAttachmentDraftId(
    threadRef.environmentId,
    threadRef.threadId,
    "request",
    question.id,
  );
  expect(useComposerDraftStore.getState().getComposerDraft(target)?.threadContexts).toEqual([
    threadContextRecord(ref, "Late thread"),
  ]);
  expect(useQuestionAttachmentPreparation.getState().counts[target]).toBe(0);
});

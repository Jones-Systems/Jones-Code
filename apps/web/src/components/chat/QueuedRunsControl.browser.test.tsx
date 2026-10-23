import { createRef, type ComponentProps } from "react";
import { describe, expect, it, vi } from "vite-plus/test";
import { EnvironmentId, MessageId, ThreadId } from "@t3tools/contracts";
import { serializeAssistantCitation } from "@t3tools/shared/assistantCitations";
import { page } from "vite-plus/test/browser";

const state = vi.hoisted(() => ({
  projection: null as unknown,
  workflow: null as unknown,
  queuedRunStart: undefined as boolean | undefined,
  review: vi.fn(),
  promote: vi.fn(async (_input: unknown): Promise<void> => undefined),
}));

vi.mock(import("../../state/server"), async (original) => {
  const actual = await original();
  const { Atom } = await import("effect/reactivity");
  const config = Atom.make(null);
  return {
    ...actual,
    serverEnvironment: { ...actual.serverEnvironment, configValueAtom: () => config },
  } as never;
});

vi.mock(import("@t3tools/client-runtime/state/thread-workflows"), async (importOriginal) => ({
  ...(await importOriginal()),
  deriveThreadQueueWorkflowState: (() => state.workflow) as never,
}));

vi.mock(import("../../state/entities"), async (importOriginal) => ({
  ...(await importOriginal()),
  useThreadProjection: (() => state.projection) as never,
  useServerConfigs: (() =>
    new Map([
      [
        "environment:test",
        { environment: { capabilities: { queuedRunStart: state.queuedRunStart } } },
      ],
    ])) as never,
}));

vi.mock(import("../../state/use-atom-command"), async (importOriginal) => ({
  ...(await importOriginal()),
  useAtomCommand: ((command: unknown) =>
    command === "imported-review" ? state.review : state.promote) as never,
}));

vi.mock(import("../../assets/assetUrls"), async (importOriginal) => ({
  ...(await importOriginal()),
  useAssetUrls: ((_environmentId: never, resources: ReadonlyArray<{ attachmentId: string }>) =>
    resources.map((resource) => `https://assets.test/${resource.attachmentId}`)) as never,
}));

vi.mock(
  import("../../jones/importedHistory/ContinuationChoiceBanner"),
  () =>
    ({
      importedHistoryCommands: { review: "imported-review" },
      ContinuationChoiceBanner: () => (
        <div data-testid="duplicate-imported-banner">Saved continuation request</div>
      ),
    }) as never,
);

import { DEFAULT_RESOLVED_KEYBINDINGS } from "@t3tools/shared/keybindings";
import { handleComposerEnter } from "./composerSubmission";
import { QueuedRunsControl, type QueuedRunsControlHandle } from "./QueuedRunsControl";
import { act, render as renderUi } from "../../../test/render";

function setup() {
  state.projection = null;
  state.workflow = null;
  state.queuedRunStart = undefined;
  state.review.mockReset();
  state.promote.mockReset().mockResolvedValue(undefined);
}

const baseProps = {
  environmentId: "environment:test" as never,
  optimisticMessages: [],
  threadId: "thread:test" as never,
  editingRunId: null,
  onEditQueuedRun: () => undefined,
  onCancelEdit: () => undefined,
} satisfies ComponentProps<typeof QueuedRunsControl>;

async function renderQueue(props: Partial<ComponentProps<typeof QueuedRunsControl>> = {}) {
  return renderUi(<QueuedRunsControl {...baseProps} {...props} />);
}

it("previews assistant quotes as readable text and preserves the citation when editing", async () => {
  setup();
  const quotedText = "Cache the parsed config per worktree.";
  const quote = serializeAssistantCitation({
    version: 1,
    environmentId: EnvironmentId.make("environment:test"),
    threadId: ThreadId.make("thread:test"),
    messageId: MessageId.make("message:assistant"),
    text: quotedText,
    start: 0,
    end: quotedText.length,
    prefix: "",
    suffix: "",
  });
  const quoteAndText = `${quote}\n\nWhy per worktree?`;
  state.projection = { projection: { thread: { historyOrigin: "native" }, messages: [] } };
  state.workflow = {
    activeRun: { id: "run:active" },
    canPromoteToSteer: true,
    canReorder: true,
    queuedRuns: [
      {
        run: { id: "run:quote-only", userMessageId: "message:quote-only" },
        text: quote,
        attachments: [],
      },
      {
        run: { id: "run:quote-and-text", userMessageId: "message:quote-and-text" },
        text: quoteAndText,
        attachments: [],
      },
    ],
  };
  const onEditQueuedRun = vi.fn();
  await using _screen = await renderQueue({ onEditQueuedRun });

  await expect.element(page.getByText(quotedText, { exact: true })).toBeVisible();
  await expect
    .element(page.getByText(`${quotedText} Why per worktree?`, { exact: true }))
    .toBeVisible();

  await page.getByRole("button", { name: "Edit queued message", exact: true }).nth(1).click();
  expect(onEditQueuedRun).toHaveBeenCalledExactlyOnceWith(
    expect.objectContaining({ text: quoteAndText }),
  );
});

const labels = (container: HTMLElement) =>
  [...container.querySelectorAll("[aria-label]")].map((element) =>
    element.getAttribute("aria-label"),
  );

describe("QueuedRunsControl automatic completion delivery", () => {
  it("does not render a queue control when only hidden delivery remains", async () => {
    state.projection = {
      projection: {
        thread: { historyOrigin: "native" },
        messages: [
          {
            delegatedCompletion: {
              parentRunId: "run:parent",
              generation: 1,
              taskIds: ["task:child"],
            },
            id: "message:completion",
          },
        ],
      },
    };
    state.workflow = {
      activeRun: { id: "run:active" },
      canPromoteToSteer: true,
      canReorder: true,
      queuedRuns: [],
    };

    await using screen = await renderQueue();
    const view = screen.container;

    expect(view.childElementCount).toBe(0);
  });
});

describe("QueuedRunsControl attachments and edit mode", () => {
  const workflowWithAttachment = () => ({
    activeRun: { id: "run:active" },
    canPromoteToSteer: true,
    canReorder: true,
    queuedRuns: [
      {
        run: { id: "run:queued", userMessageId: "message:queued" },
        text: "Queued with a screenshot",
        attachments: [
          {
            type: "image",
            id: "attachment-1",
            name: "screenshot.png",
            mimeType: "image/png",
            sizeBytes: 128,
          },
        ],
      },
    ],
  });

  it("renders an attachment thumbnail on the queued row", async () => {
    state.projection = { projection: { thread: { historyOrigin: "native" }, messages: [] } };
    state.workflow = workflowWithAttachment();

    await using screen = await renderQueue();
    const view = screen.container;

    expect(view.querySelector("img")?.getAttribute("src")).toBe("https://assets.test/attachment-1");
    expect(view.textContent).toContain("Queued with a screenshot");
    expect(labels(view)).toContain("Edit queued message");
    expect(labels(view)).toContain("Reorder queued message (drag, or press the arrow keys)");
    expect(labels(view)).not.toContain("Move queued message up");
  });

  it("drops the optimistic pending row once the projection holds its message", async () => {
    state.projection = {
      projection: {
        thread: { historyOrigin: "native" },
        messages: [{ id: "message:acknowledged", text: "hello" }],
      },
    };
    state.workflow = {
      activeRun: { id: "run:active" },
      canPromoteToSteer: true,
      canReorder: true,
      queuedRuns: [],
    };

    await using screen = await renderQueue({
      optimisticMessages: [
        {
          id: "message:acknowledged" as never,
          inputIntent: "queued_turn",
          text: "hello",
          attachments: [],
        },
      ],
    });
    const view = screen.container;

    expect(view.childElementCount).toBe(0);
  });

  it("keeps the original queued message visible while editing", async () => {
    state.projection = { projection: { thread: { historyOrigin: "native" }, messages: [] } };
    state.workflow = workflowWithAttachment();

    await using screen = await renderQueue({ editingRunId: "run:queued" as never });
    const view = screen.container;

    expect(view.textContent).toContain("Queued with a screenshot");
  });
});

describe("QueuedRunsControl empty composer Enter", () => {
  it("uses one queue item in order and shares the send lock with the arrow action", async () => {
    const ref = createRef<QueuedRunsControlHandle>();
    const onSubmit = vi.fn();
    let finishSend: (() => void) | undefined;
    state.promote.mockClear();
    state.promote.mockImplementation(
      () =>
        new Promise<void>((resolve) => {
          finishSend = resolve;
        }),
    );
    const first = {
      run: { id: "run:first", userMessageId: "message:first" },
      text: "first",
      attachments: [],
    };
    const second = {
      run: { id: "run:second", userMessageId: "message:second" },
      text: "second",
      attachments: [],
    };
    const workflow = {
      activeRun: { id: "run:active" },
      canPromoteToSteer: true,
      canReorder: true,
      queuedRuns: [second, first],
    };
    state.workflow = workflow;
    state.projection = { projection: { thread: { historyOrigin: "native" }, messages: [] } };
    const control = () => (
      <QueuedRunsControl
        ref={ref}
        environmentId={"environment:test" as never}
        optimisticMessages={[]}
        threadId={"thread:test" as never}
        editingRunId={null}
        onEditQueuedRun={() => undefined}
        onCancelEdit={() => undefined}
      />
    );
    const enter = (repeat = false) =>
      handleComposerEnter({
        event: {
          key: "Enter",
          shiftKey: false,
          altKey: false,
          metaKey: false,
          ctrlKey: false,
          isComposing: false,
          keyCode: 13,
          repeat,
        },
        intent: {
          keybindings: DEFAULT_RESOLVED_KEYBINDINGS,
          platform: "Linux",
          isSoftwareKeyboardOpen: false,
          isDraftThread: false,
          isRunning: true,
          prompt: "",
        },
        hasDraftContext: false,
        queueActionDisabled: false,
        onSteerNextQueuedMessage: () => ref.current?.steerNext(false) ?? false,
        onSubmit,
      });
    await using screen = await renderUi(control());
    const container = screen.container;
    try {
      const rowAction = [...container.querySelectorAll("button")].find(
        (button) => button.textContent?.trim() === "Steer",
      );
      expect(rowAction).toBeDefined();
      // Enter and a row click race before React can publish its busy state.
      await act(async () => {
        expect(enter()).toBe(true);
        rowAction!.click();
        expect(enter()).toBe(true);
      });
      expect(state.promote).toHaveBeenCalledExactlyOnceWith({
        environmentId: "environment:test",
        input: { threadId: "thread:test", queuedRunId: "run:second", targetRunId: "run:active" },
      });
      expect(onSubmit).not.toHaveBeenCalled();
      await act(async () => {
        finishSend!();
      });
      state.workflow = { ...workflow, queuedRuns: [first] };
      state.projection = { projection: { thread: { historyOrigin: "native" }, messages: [] } };
      await screen.rerender(control());
      await act(async () => {
        expect(enter(true)).toBe(true);
      });
      expect(state.promote).toHaveBeenCalledTimes(1);
      await act(async () => {
        expect(enter()).toBe(true);
      });
      expect(state.promote).toHaveBeenLastCalledWith({
        environmentId: "environment:test",
        input: { threadId: "thread:test", queuedRunId: "run:first", targetRunId: "run:active" },
      });
      expect(state.promote).toHaveBeenCalledTimes(2);
      await act(async () => {
        finishSend!();
      });
      state.workflow = { ...workflow, queuedRuns: [] };
      state.projection = { projection: { thread: { historyOrigin: "native" }, messages: [] } };
      await screen.rerender(control());
      expect(enter()).toBe(true);
      expect(onSubmit).toHaveBeenCalledExactlyOnceWith("foreground");
      expect(state.promote).toHaveBeenCalledTimes(2);
    } finally {
      await act(async () => {
        finishSend?.();
      });
      await screen.unmount();
      state.promote.mockReset();
      state.promote.mockImplementation(async () => undefined);
      state.projection = null;
      state.workflow = null;
    }
  });
});

describe("QueuedRunsControl selected run start", () => {
  function idleQueue(historyOrigin: "native" | "v1_import" = "native") {
    setup();
    state.projection = {
      projection: { thread: { historyOrigin }, messages: [], runs: [], providerThreads: [] },
    };
    state.workflow = {
      activeRun: null,
      canSendNow: true,
      canPromoteToSteer: false,
      canReorder: true,
      queuedRuns: [
        {
          run: { id: "run:queued", userMessageId: "message:queued" },
          messageId: "message:queued",
          text: "Queued request",
          attachments: [],
        },
      ],
    };
  }

  function sendButton(view: HTMLElement) {
    return [...view.querySelectorAll("button")].find(
      (button) => button.textContent?.trim() === "Send now",
    )!;
  }

  it.each([undefined, false])("refuses older/unsupported servers (%s)", async (capability) => {
    idleQueue();
    state.queuedRunStart = capability;
    const ref = createRef<QueuedRunsControlHandle>();
    await using screen = await renderQueue({ ref });
    expect(sendButton(screen.container).disabled).toBe(true);
    await act(() => {
      sendButton(screen.container).click();
      expect(ref.current?.steerNext(false)).toBe(false);
    });
    expect(state.promote).not.toHaveBeenCalled();
  });

  it("starts exactly the selected queued run when the server advertises support", async () => {
    idleQueue();
    state.queuedRunStart = true;
    await using screen = await renderQueue();
    expect(sendButton(screen.container).disabled).toBe(false);
    await act(() => sendButton(screen.container).click());
    expect(state.promote).toHaveBeenCalledExactlyOnceWith({
      environmentId: "environment:test",
      input: { threadId: "thread:test", runId: "run:queued" },
    });
  });

  it.each(["completed run", "native context"])(
    "returns to ordinary queued sending after continuation establishes %s",
    async (transition) => {
      idleQueue("v1_import");
      state.queuedRunStart = true;
      const onImportedHistoryChoice = vi.fn();
      const ref = createRef<QueuedRunsControlHandle>();
      const control = () => (
        <QueuedRunsControl
          {...baseProps}
          ref={ref}
          onImportedHistoryChoice={onImportedHistoryChoice}
        />
      );
      await using screen = await renderUi(control());
      expect(sendButton(screen.container).disabled).toBe(true);
      expect(screen.container.textContent).toContain("Review imported history");
      state.projection = {
        projection: {
          thread: { historyOrigin: "v1_import" },
          messages: [],
          runs: transition === "completed run" ? [{ status: "completed" }] : [],
          providerThreads:
            transition === "native context"
              ? [
                  {
                    providerSessionId: "session:continued",
                    nativeThreadRef: null,
                    nativeConversationHeadRef: null,
                  },
                ]
              : [],
        },
      };
      await screen.rerender(control());
      expect(screen.container.textContent).not.toContain("Review imported history");
      expect(sendButton(screen.container).disabled).toBe(false);
      await act(async () => {
        expect(ref.current?.steerNext(false)).toBe(true);
      });
      expect(state.promote).toHaveBeenCalledExactlyOnceWith({
        environmentId: baseProps.environmentId,
        input: { threadId: baseProps.threadId, runId: "run:queued" },
      });
      expect(state.review).not.toHaveBeenCalled();
      expect(onImportedHistoryChoice).not.toHaveBeenCalled();
    },
  );

  it("keeps imported history behind review even when selected-run start is supported", async () => {
    idleQueue("v1_import");
    state.queuedRunStart = true;
    const ref = createRef<QueuedRunsControlHandle>();
    await using screen = await renderQueue({ ref, onImportedHistoryChoice: vi.fn() });
    expect(sendButton(screen.container).disabled).toBe(true);
    expect(
      [...screen.container.querySelectorAll("button")].some(
        (button) => button.textContent?.trim() === "Review imported history" && !button.disabled,
      ),
    ).toBe(true);
    await act(() => {
      sendButton(screen.container).click();
      expect(ref.current?.steerNext(false)).toBe(false);
    });
    expect(state.promote).not.toHaveBeenCalled();
  });
});

describe("imported history queue review ownership", () => {
  const queued = {
    run: { id: "run:queued", userMessageId: "message:queued" },
    messageId: "message:queued",
    text: "Continue from history",
    attachments: [],
  };
  const setup = (hasQueue = true, providerThreads: unknown[] = []) => {
    state.projection = {
      projection: {
        thread: { historyOrigin: "v1_import" },
        messages: [],
        runs: [],
        providerThreads,
      },
    };
    state.workflow = {
      activeRun: null,
      canPromoteToSteer: false,
      canReorder: true,
      queuedRuns: hasQueue ? [queued] : [],
    };
  };
  it.each([false, true])("does not own a saved-choice banner with queue=%s", async (hasQueue) => {
    setup(hasQueue);
    await using screen = await renderQueue();
    const view = screen.container;
    expect(view.querySelector('[data-testid="duplicate-imported-banner"]')).toBeNull();
  });
  it("delivers the reviewed snapshot to the sole banner owner", async () => {
    setup();
    state.review.mockResolvedValue({
      _tag: "Success",
      value: {
        status: "available",
        reviewedBasis: "a".repeat(64),
        reason: null,
      },
    });
    const onImportedHistoryChoice = vi.fn();
    await using screen = await renderQueue({ onImportedHistoryChoice });
    const view = screen.container;
    const review = [...view.querySelectorAll("button")].find((button) =>
      button.textContent?.includes("Review imported history"),
    );
    expect(review).toBeDefined();
    await act(async () => review!.click());
    expect(onImportedHistoryChoice).toHaveBeenCalledTimes(1);
    const choice = onImportedHistoryChoice.mock.calls[0]![0];
    expect(choice).toMatchObject({
      environmentId: baseProps.environmentId,
      threadId: baseProps.threadId,
      prepared: {
        delivery: { type: "queued_run", runId: queued.run.id, messageId: queued.messageId },
      },
    });
    expect(choice.prepared.unchanged()).toBe(true);
    expect(view.querySelector('[data-testid="duplicate-imported-banner"]')).toBeNull();
  });
  it.each(["queue changed", "route changed", "continuation started"])(
    "discards a review when %s while it is pending",
    async (change) => {
      setup();
      let finishReview!: (result: unknown) => void;
      state.review.mockImplementationOnce(
        () =>
          new Promise((resolve) => {
            finishReview = resolve;
          }),
      );
      const onImportedHistoryChoice = vi.fn();
      await using screen = await renderQueue({ onImportedHistoryChoice });
      await act(async () => {
        [...screen.container.querySelectorAll("button")]
          .find((button) => button.textContent?.includes("Review imported history"))!
          .click();
      });
      if (change === "queue changed") {
        state.workflow = {
          activeRun: null,
          canPromoteToSteer: false,
          canReorder: true,
          queuedRuns: [queued, { ...queued, run: { ...queued.run, id: "run:second" } }],
        };
      } else if (change === "continuation started") {
        setup(true, [
          {
            providerSessionId: "session:continued",
            nativeThreadRef: null,
            nativeConversationHeadRef: null,
          },
        ]);
      }
      await screen.rerender(
        <QueuedRunsControl
          {...baseProps}
          threadId={change === "route changed" ? ThreadId.make("thread:other") : baseProps.threadId}
          onImportedHistoryChoice={onImportedHistoryChoice}
        />,
      );
      await act(async () => {
        finishReview({
          _tag: "Success",
          value: { status: "available", reviewedBasis: "a".repeat(64), reason: null },
        });
      });
      expect(onImportedHistoryChoice).not.toHaveBeenCalled();
    },
  );

  it("offers no imported review once native provider context exists", async () => {
    setup(true, [
      {
        providerSessionId: "session:existing",
        nativeThreadRef: null,
        nativeConversationHeadRef: null,
      },
    ]);
    await using screen = await renderQueue({ onImportedHistoryChoice: vi.fn() });
    const view = screen.container;
    expect(view.textContent).not.toContain("Review imported history");
    expect(view.textContent).toContain(queued.text);
  });
});

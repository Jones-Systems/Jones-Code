import { RegistryContext } from "@effect/atom-react";
import type { EnvironmentThreadState } from "@t3tools/client-runtime/state/threads";
import { EnvironmentId, MessageId, ThreadId } from "@t3tools/contracts";
import { serializeAssistantCitation } from "@t3tools/shared/assistantCitations";
import {
  formatComposerContextReference,
  toComposerContextId,
} from "@t3tools/shared/composerContextReferences";
import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";
import { AsyncResult, type Atom, AtomRegistry } from "effect/reactivity";
import { createRef, Profiler, useState, type ComponentProps, type ReactNode } from "react";
import { describe, expect, it, vi } from "vite-plus/test";
import { page } from "vite-plus/test/browser";

const state = vi.hoisted(() => ({
  threadState: null as unknown as Atom.Writable<AsyncResult.AsyncResult<EnvironmentThreadState>>,
  workflow: null as unknown,
  queuedRunStart: undefined as boolean | undefined,
  review: vi.fn(),
  promote: vi.fn(async (_input: unknown): Promise<void> => undefined),
}));

vi.mock(import("../../state/server"), async (original) => {
  const actual = await original();
  const { Atom } = await import("effect/reactivity");
  const config = () =>
    Atom.make({ environment: { capabilities: { queuedRunStart: state.queuedRunStart } } });
  return {
    ...actual,
    serverEnvironment: { ...actual.serverEnvironment, configValueAtom: config },
  } as never;
});

vi.mock(import("@t3tools/client-runtime/state/thread-workflows"), async (importOriginal) => ({
  ...(await importOriginal()),
  deriveThreadQueueWorkflowState: (() => state.workflow) as never,
}));

vi.mock(import("../../state/entities"), async (importOriginal) => ({
  ...(await importOriginal()),
  // The linked environment is connected, but its thread is not in the loaded shell.
  useQuietThreadShell: ((target: { readonly environmentId: string } | null) =>
    target === null || target.environmentId === "environment:linked"
      ? null
      : { title: "Release lead" }) as never,
}));

vi.mock(import("../../state/environments"), async (importOriginal) => ({
  ...(await importOriginal()),
  useEnvironment: ((environmentId: string | null) =>
    environmentId === "environment:linked" ? { environmentId } : null) as never,
}));

vi.mock(import("../../state/query"), async (importOriginal) => ({
  ...(await importOriginal()),
  useEnvironmentQuery: (() => ({
    data: { tasks: [{ id: "task:webhook", title: "PR dispatcher" }] },
  })) as never,
}));

// The thread detail atoms are real, fed by one state atom the test writes.
vi.mock(import("../../state/threads"), async (importOriginal) => {
  const { createEnvironmentThreadDetailAtoms, EMPTY_ENVIRONMENT_THREAD_STATE } =
    await import("@t3tools/client-runtime/state/threads");
  const { AsyncResult, Atom } = await import("effect/reactivity");
  const Option = await import("effect/Option");
  const { makeThreadProjectionFixture } = await import("../../test-fixtures");
  state.threadState = Atom.make<AsyncResult.AsyncResult<EnvironmentThreadState>>(
    AsyncResult.success({
      ...EMPTY_ENVIRONMENT_THREAD_STATE,
      data: Option.some(makeThreadProjectionFixture()),
    }),
  ).pipe(Atom.keepAlive);
  return {
    ...(await importOriginal()),
    environmentThreadDetails: createEnvironmentThreadDetailAtoms(() => state.threadState),
  };
});

// Imported-history review answers from `state.review`; every queue command from `state.promote`.
vi.mock(import("../../state/use-atom-command"), async (importOriginal) => ({
  ...(await importOriginal()),
  useAtomCommand: ((command: unknown) =>
    command === "imported-review" ? state.review : state.promote) as never,
}));

vi.mock(
  import("../../jones/importedHistory/commands"),
  () => ({ importedHistoryCommands: { review: "imported-review" } }) as never,
);

vi.mock(import("../../assets/assetUrls"), async (importOriginal) => ({
  ...(await importOriginal()),
  useAssetUrls: ((_environmentId: never, resources: ReadonlyArray<{ attachmentId: string }>) =>
    resources.map((resource) => `https://assets.test/${resource.attachmentId}`)) as never,
}));

import "~/index.css";
import { EMPTY_ENVIRONMENT_THREAD_STATE } from "@t3tools/client-runtime/state/threads";
import { DEFAULT_RESOLVED_KEYBINDINGS } from "@t3tools/shared/keybindings";

import { act, render as renderUi } from "../../../test/render";
import { makeStreamingTimelineFixture, makeThreadProjectionFixture } from "../../test-fixtures";
import { handleComposerEnter } from "./composerSubmission";
import { QueuedRunsControl, type QueuedRunsControlHandle } from "./QueuedRunsControl";
import { ExpandedImageDialog } from "./ExpandedImageDialog";
import type { ExpandedImagePreview } from "./ExpandedImagePreview";

function setup() {
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
  editBlockedReason: null,
  onEditQueuedRun: () => undefined,
  onCancelEdit: () => undefined,
  timestampFormat: "locale",
  onOpenThread: () => undefined,
  onImageExpand: () => undefined,
} satisfies ComponentProps<typeof QueuedRunsControl>;

/** The fixture projection with `changes` laid over it; `thread` merges into its thread. */
function threadStateOf(
  changes: Readonly<Record<string, unknown>>,
): AsyncResult.AsyncResult<EnvironmentThreadState> {
  const base = makeThreadProjectionFixture();
  const { thread, ...rest } = changes as { readonly thread?: object };
  return AsyncResult.success<EnvironmentThreadState>({
    ...EMPTY_ENVIRONMENT_THREAD_STATE,
    data: Option.some({ ...base, ...rest, thread: { ...base.thread, ...thread } } as never),
  });
}

/** A registry whose thread holds `changes`; `write` replaces them, as a server update would. */
function threadRegistry(changes: Readonly<Record<string, unknown>>) {
  const registry = AtomRegistry.make();
  registry.set(state.threadState, threadStateOf(changes));
  return {
    registry,
    write: (next: Readonly<Record<string, unknown>>) =>
      registry.set(state.threadState, threadStateOf(next)),
    [Symbol.dispose]: () => registry.dispose(),
  };
}

const inRegistry = (registry: AtomRegistry.AtomRegistry, ui: ReactNode) => (
  <RegistryContext.Provider value={registry}>{ui}</RegistryContext.Provider>
);

const nativeThread = { thread: { historyOrigin: "native" } };

async function renderQueue(
  props: Partial<ComponentProps<typeof QueuedRunsControl>> = {},
  registry?: AtomRegistry.AtomRegistry,
) {
  const queue = <QueuedRunsControl {...baseProps} {...props} />;
  return renderUi(registry ? inRegistry(registry, queue) : queue);
}

it.each([false, true])(
  "opens queued images in the existing gallery, pending=%s",
  async (pending) => {
    setup();
    const attachments = ["first.png", "second.png"].map((name) => ({
      type: "image" as const,
      id: name,
      name,
      mimeType: "image/png",
      sizeBytes: 128,
    }));
    state.workflow = {
      activeRun: null,
      queuedRuns: pending
        ? []
        : [
            {
              run: { id: "run:queued", userMessageId: "message:queued" },
              text: "Later",
              attachments,
            },
          ],
    };
    function QueueWithGallery() {
      const [preview, setPreview] = useState<ExpandedImagePreview | null>(null);
      return (
        <>
          <QueuedRunsControl
            {...baseProps}
            optimisticMessages={
              pending
                ? [
                    {
                      id: MessageId.make("message:pending"),
                      inputIntent: "queued_turn",
                      text: "Later",
                      attachments: attachments.map((attachment) => ({
                        ...attachment,
                        previewUrl: `https://assets.test/${attachment.id}`,
                      })),
                    },
                  ]
                : []
            }
            onImageExpand={setPreview}
          />
          {preview ? (
            <ExpandedImageDialog preview={preview} onClose={() => setPreview(null)} />
          ) : null}
        </>
      );
    }
    await using _screen = await renderUi(<QueueWithGallery />);

    await page.getByRole("button", { name: "Preview second.png", exact: true }).click();
    await expect.element(page.getByRole("dialog")).toBeVisible();
    await expect.element(page.getByText("second.png (2/2)", { exact: true })).toBeVisible();
    await page.getByRole("button", { name: "Previous media", exact: true }).click();
    await expect.element(page.getByText("first.png (1/2)", { exact: true })).toBeVisible();
    await page.getByRole("button", { name: "Back, close image preview", exact: true }).click();
    await expect.element(page.getByRole("dialog")).not.toBeInTheDocument();
    await expect.element(page.getByText("Later", { exact: true })).toBeVisible();
  },
);

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
  const inlineImage = formatComposerContextReference({
    kind: "image",
    contextId: toComposerContextId("header"),
    label: "header.png",
  });
  state.workflow = {
    activeRun: { id: "run:active" },
    canPromoteToSteer: true,
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
      {
        run: { id: "run:inline-image", userMessageId: "message:inline-image" },
        text: `Make the header look like ${inlineImage} and keep the logo.`,
        attachments: [{ type: "image", id: "attachment:header", name: "header.png" }],
      },
    ],
  };
  const onEditQueuedRun = vi.fn();
  await using _screen = await renderQueue({ onEditQueuedRun });

  await expect.element(page.getByText(quotedText, { exact: true })).toBeVisible();
  await expect
    .element(page.getByText(`${quotedText} Why per worktree?`, { exact: true }))
    .toBeVisible();
  await expect
    .element(
      page.getByText("Make the header look like header.png and keep the logo.", { exact: true }),
    )
    .toBeVisible();

  await page.getByRole("button", { name: "Edit queued message", exact: true }).nth(1).click();
  expect(onEditQueuedRun).toHaveBeenCalledExactlyOnceWith(
    expect.objectContaining({ text: quoteAndText }),
  );
});

it("shows who sent a queued message and folds automation and agent prompts", async () => {
  setup();
  const message = (id: string, fields: Record<string, unknown>) => ({
    id,
    role: "user",
    createdAt: DateTime.makeUnsafe("2026-10-27T12:00:00.000Z"),
    ...fields,
  });
  state.workflow = {
    activeRun: { id: "run:active" },
    canPromoteToSteer: true,
    queuedRuns: [
      [
        "run:automation",
        "GitHub labeled a pull request.\nFollow the runbook.",
        {
          createdBy: "agent",
          scheduledTaskId: "task:webhook",
        },
      ],
      [
        "run:agent",
        "Review finished.",
        {
          createdBy: "agent",
          creationSource: "provider",
          senderThreadId: "thread:lead",
        },
      ],
      [
        "run:linked",
        "Linked review finished.",
        {
          createdBy: "agent",
          creationSource: "mcp",
          linkedSender: {
            environmentId: "environment:linked",
            threadId: "thread:linked-lead",
            title: "Title when sent",
          },
        },
      ],
      [
        "run:server",
        "Continue where you left off.",
        {
          createdBy: "agent",
          creationSource: "server",
        },
      ],
    ].map(([runId, text, fields]) => ({
      run: { id: runId, userMessageId: `${runId}:message` },
      text,
      attachments: [],
      message: message(`${runId}:message`, { text, ...(fields as object) }),
    })),
  };
  const onOpenThread = vi.fn();
  await using _screen = await renderQueue({ onOpenThread });
  // Four rows collapse the queue.
  await page.getByRole("button", { name: "Show all 4 queued" }).click();

  const automation = page.getByRole("button", { name: /PR dispatcher/ });
  await expect.element(automation).toHaveAttribute("aria-expanded", "false");
  await expect.element(page.getByText(/Follow the runbook/)).not.toBeInTheDocument();
  await automation.click();
  await expect.element(page.getByText(/Follow the runbook/)).toBeVisible();
  await expect.element(page.getByRole("link", { name: "Open automation" })).toBeInTheDocument();

  await expect.element(page.getByRole("button", { name: /Release lead/ })).toBeVisible();
  await page.getByRole("button", { name: "Open thread" }).nth(0).click({ force: true });
  expect(onOpenThread).toHaveBeenCalledExactlyOnceWith("thread:lead", "environment:test");
  // A sender in a connected linked environment keeps the title it sent and opens there.
  await expect.element(page.getByRole("button", { name: /Title when sent/ })).toBeVisible();
  await page.getByRole("button", { name: "Open thread" }).nth(1).click({ force: true });
  expect(onOpenThread).toHaveBeenLastCalledWith("thread:linked-lead", "environment:linked");

  await expect.element(page.getByText("Sent by T3 Code")).toBeVisible();
  await expect.element(page.getByText("Continue where you left off.")).toBeVisible();
});

it("stays inside the timeline scroller instead of growing the chat column", async () => {
  setup();
  state.workflow = {
    activeRun: { id: "run:active" },
    canPromoteToSteer: true,
    queuedRuns: [
      {
        run: { id: "run:queued", userMessageId: "message:queued" },
        text: "Later",
        attachments: [],
      },
    ],
  };
  // Mirrors ChatView: a scrollable column, a positioned wrapper, then a non-positioned scroller.
  await using screen = await renderUi(
    <div data-testid="column" style={{ height: 200, overflowY: "auto" }}>
      <div style={{ position: "relative", height: "100%" }}>
        <div style={{ height: "100%", overflowY: "auto" }}>
          <div style={{ height: 1000 }} />
          <QueuedRunsControl {...baseProps} />
        </div>
      </div>
    </div>,
  );
  await expect.element(page.getByText("Later", { exact: true })).toBeInTheDocument();
  const column = screen.getByTestId("column").element();
  expect(column.scrollHeight).toBe(column.clientHeight);
});

it("does not re-render while assistant text streams into the thread", async () => {
  setup();
  state.workflow = {
    activeRun: { id: "run:active" },
    canPromoteToSteer: true,
    canReorder: true,
    queuedRuns: [
      {
        run: { id: "run:queued", userMessageId: "message:queued" },
        text: "Run the tests next",
        attachments: [],
      },
    ],
  };
  const base = makeThreadProjectionFixture();
  // Each delta replaces only the streaming reply; every other item keeps its identity.
  const timeline = makeStreamingTimelineFixture().visibleTurnItems;
  const thread = (text: string, done = false) => {
    const items = timeline.map((entry) =>
      entry.item.type === "assistant_message" && entry.item.streaming
        ? {
            ...entry,
            item: done
              ? { ...entry.item, text, streaming: false, status: "completed" as const }
              : { ...entry.item, text },
          }
        : entry,
    );
    return AsyncResult.success<EnvironmentThreadState>({
      ...EMPTY_ENVIRONMENT_THREAD_STATE,
      data: Option.some({
        ...base,
        turnItems: items.map(({ item }) => item),
        visibleTurnItems: items,
      }),
    });
  };
  const registry = AtomRegistry.make();
  await using stack = new AsyncDisposableStack();
  stack.defer(() => registry.dispose());
  registry.set(state.threadState, thread("Hel"));
  let renders = 0;
  await using _screen = await renderUi(
    <RegistryContext.Provider value={registry}>
      <Profiler id="queue" onRender={() => (renders += 1)}>
        <QueuedRunsControl {...baseProps} />
      </Profiler>
    </RegistryContext.Provider>,
  );
  await expect.element(page.getByText("Run the tests next", { exact: true })).toBeVisible();
  const settled = renders;

  await act(() => registry.set(state.threadState, thread("Hello")));
  await act(() => registry.set(state.threadState, thread("Hello there")));
  expect(renders).toBe(settled);

  await act(() => registry.set(state.threadState, thread("Hello there", true)));
  expect(renders).toBeGreaterThan(settled);
});

const labels = (container: HTMLElement) =>
  [...container.querySelectorAll("[aria-label]")].map((element) =>
    element.getAttribute("aria-label"),
  );

describe("QueuedRunsControl automatic completion delivery", () => {
  it("does not render a queue control when only hidden delivery remains", async () => {
    setup();
    using thread = threadRegistry({
      ...nativeThread,
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
    });
    state.workflow = {
      activeRun: { id: "run:active" },
      canPromoteToSteer: true,
      canReorder: true,
      queuedRuns: [],
    };

    await using screen = await renderQueue({}, thread.registry);
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
    setup();
    using thread = threadRegistry(nativeThread);
    state.workflow = workflowWithAttachment();

    await using screen = await renderQueue({}, thread.registry);
    const view = screen.container;

    expect(view.querySelector("img")?.getAttribute("src")).toBe("https://assets.test/attachment-1");
    expect(view.textContent).toContain("Queued with a screenshot");
    expect(labels(view)).toContain("Edit queued message");
    expect(labels(view)).toContain("Reorder queued message (drag, or press the arrow keys)");
    expect(labels(view)).not.toContain("Move queued message up");
  });

  it("drops the optimistic pending row once the projection holds its message", async () => {
    setup();
    using thread = threadRegistry({
      ...nativeThread,
      messages: [{ id: "message:acknowledged", text: "hello" }],
    });
    state.workflow = {
      activeRun: { id: "run:active" },
      canPromoteToSteer: true,
      canReorder: true,
      queuedRuns: [],
    };

    await using screen = await renderQueue(
      {
        optimisticMessages: [
          {
            id: "message:acknowledged" as never,
            inputIntent: "queued_turn",
            text: "hello",
            attachments: [],
          },
        ],
      },
      thread.registry,
    );
    const view = screen.container;

    expect(view.childElementCount).toBe(0);
  });

  it("keeps the original queued message visible while editing", async () => {
    setup();
    using thread = threadRegistry(nativeThread);
    state.workflow = workflowWithAttachment();

    await using screen = await renderQueue(
      { editingRunId: "run:queued" as never },
      thread.registry,
    );
    const view = screen.container;

    expect(view.textContent).toContain("Queued with a screenshot");
  });
});

describe("QueuedRunsControl empty composer Enter", () => {
  it("uses one queue item in order and shares the send lock with the arrow action", async () => {
    setup();
    using thread = threadRegistry(nativeThread);
    const ref = createRef<QueuedRunsControlHandle>();
    const onSubmit = vi.fn();
    let finishSend: (() => void) | undefined;
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
    const control = () =>
      inRegistry(
        thread.registry,
        <QueuedRunsControl
          ref={ref}
          environmentId={"environment:test" as never}
          optimisticMessages={[]}
          threadId={"thread:test" as never}
          editingRunId={null}
          editBlockedReason={null}
          timestampFormat="locale"
          onOpenThread={() => undefined}
          onEditQueuedRun={() => undefined}
          onCancelEdit={() => undefined}
          onImageExpand={() => undefined}
        />,
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
          context: {},
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
      state.workflow = null;
    }
  });
});

describe("QueuedRunsControl selected run start", () => {
  const idleProjection = (
    historyOrigin: "native" | "v1_import",
    changes: Readonly<Record<string, unknown>> = {},
  ) => ({ thread: { historyOrigin }, messages: [], runs: [], providerThreads: [], ...changes });

  function idleQueue(historyOrigin: "native" | "v1_import" = "native") {
    setup();
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
    return threadRegistry(idleProjection(historyOrigin));
  }

  function sendButton(view: HTMLElement) {
    return [...view.querySelectorAll("button")].find(
      (button) => button.textContent?.trim() === "Send now",
    )!;
  }

  it.each([undefined, false])("refuses older/unsupported servers (%s)", async (capability) => {
    using thread = idleQueue();
    state.queuedRunStart = capability;
    const ref = createRef<QueuedRunsControlHandle>();
    await using screen = await renderQueue({ ref }, thread.registry);
    expect(sendButton(screen.container).disabled).toBe(true);
    await act(() => {
      sendButton(screen.container).click();
      expect(ref.current?.steerNext(false)).toBe(false);
    });
    expect(state.promote).not.toHaveBeenCalled();
  });

  it("starts exactly the selected queued run when the server advertises support", async () => {
    using thread = idleQueue();
    state.queuedRunStart = true;
    await using screen = await renderQueue({}, thread.registry);
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
      using thread = idleQueue("v1_import");
      state.queuedRunStart = true;
      const onImportedHistoryChoice = vi.fn();
      const ref = createRef<QueuedRunsControlHandle>();
      const control = () =>
        inRegistry(
          thread.registry,
          <QueuedRunsControl
            {...baseProps}
            ref={ref}
            onImportedHistoryChoice={onImportedHistoryChoice}
          />,
        );
      await using screen = await renderUi(control());
      expect(sendButton(screen.container).disabled).toBe(true);
      expect(screen.container.textContent).toContain("Review imported history");
      await act(() =>
        thread.write(
          idleProjection("v1_import", {
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
          }),
        ),
      );
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
    using thread = idleQueue("v1_import");
    state.queuedRunStart = true;
    const ref = createRef<QueuedRunsControlHandle>();
    await using screen = await renderQueue(
      { ref, onImportedHistoryChoice: vi.fn() },
      thread.registry,
    );
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
  const importedProjection = (providerThreads: unknown[] = []) => ({
    thread: { historyOrigin: "v1_import" },
    messages: [],
    runs: [],
    providerThreads,
  });
  const continuedProviderThread = {
    providerSessionId: "session:continued",
    nativeThreadRef: null,
    nativeConversationHeadRef: null,
  };
  const importedQueue = (hasQueue = true, providerThreads: unknown[] = []) => {
    setup();
    state.workflow = {
      activeRun: null,
      canPromoteToSteer: false,
      canReorder: true,
      queuedRuns: hasQueue ? [queued] : [],
    };
    return threadRegistry(importedProjection(providerThreads));
  };
  it.each([false, true])("does not own a saved-choice banner with queue=%s", async (hasQueue) => {
    using thread = importedQueue(hasQueue);
    await using screen = await renderQueue({}, thread.registry);
    const view = screen.container;
    expect(view.querySelector('[data-testid="duplicate-imported-banner"]')).toBeNull();
  });
  it("delivers the reviewed snapshot to the sole banner owner", async () => {
    using thread = importedQueue();
    state.review.mockResolvedValue({
      _tag: "Success",
      value: {
        status: "available",
        reviewedBasis: "a".repeat(64),
        reason: null,
      },
    });
    const onImportedHistoryChoice = vi.fn();
    await using screen = await renderQueue({ onImportedHistoryChoice }, thread.registry);
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
      using thread = importedQueue();
      let finishReview!: (result: unknown) => void;
      state.review.mockImplementationOnce(
        () =>
          new Promise((resolve) => {
            finishReview = resolve;
          }),
      );
      const onImportedHistoryChoice = vi.fn();
      await using screen = await renderQueue({ onImportedHistoryChoice }, thread.registry);
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
        await act(() => thread.write(importedProjection()));
      } else if (change === "continuation started") {
        await act(() => thread.write(importedProjection([continuedProviderThread])));
      }
      await screen.rerender(
        inRegistry(
          thread.registry,
          <QueuedRunsControl
            {...baseProps}
            threadId={
              change === "route changed" ? ThreadId.make("thread:other") : baseProps.threadId
            }
            onImportedHistoryChoice={onImportedHistoryChoice}
          />,
        ),
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
    using thread = importedQueue(true, [
      {
        providerSessionId: "session:existing",
        nativeThreadRef: null,
        nativeConversationHeadRef: null,
      },
    ]);
    await using screen = await renderQueue({ onImportedHistoryChoice: vi.fn() }, thread.registry);
    const view = screen.container;
    expect(view.textContent).not.toContain("Review imported history");
    expect(view.textContent).toContain(queued.text);
  });
});

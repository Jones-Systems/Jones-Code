import { act, createRef, type ComponentProps } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

const state = vi.hoisted(() => ({
  projection: null as unknown,
  workflow: null as unknown,
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

const roots: Root[] = [];

afterEach(async () => {
  await act(async () => {
    for (const root of roots.splice(0)) root.unmount();
  });
  document.body.replaceChildren();
  vi.unstubAllGlobals();
});

const baseProps = {
  environmentId: "environment:test" as never,
  optimisticMessages: [],
  threadId: "thread:test" as never,
  editingRunId: null,
  onEditQueuedRun: () => undefined,
  onCancelEdit: () => undefined,
} satisfies ComponentProps<typeof QueuedRunsControl>;

async function render(props: Partial<ComponentProps<typeof QueuedRunsControl>> = {}) {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  const container = document.createElement("div");
  document.body.append(container);
  const root = createRoot(container);
  roots.push(root);
  await act(async () => root.render(<QueuedRunsControl {...baseProps} {...props} />));
  return container;
}

const labels = (container: HTMLElement) =>
  [...container.querySelectorAll("[aria-label]")].map((element) =>
    element.getAttribute("aria-label"),
  );

describe("QueuedRunsControl automatic completion delivery", () => {
  it("does not render a queue control when only hidden delivery remains", async () => {
    state.projection = {
      projection: {
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

    const view = await render();

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
    state.projection = { projection: { messages: [] } };
    state.workflow = workflowWithAttachment();

    const view = await render();

    expect(view.querySelector("img")?.getAttribute("src")).toBe("https://assets.test/attachment-1");
    expect(view.textContent).toContain("Queued with a screenshot");
    expect(labels(view)).toContain("Edit queued message");
    expect(labels(view)).toContain("Reorder queued message (drag, or press the arrow keys)");
    expect(labels(view)).not.toContain("Move queued message up");
  });

  it("drops the optimistic pending row once the projection holds its message", async () => {
    state.projection = {
      projection: { messages: [{ id: "message:acknowledged", text: "hello" }] },
    };
    state.workflow = {
      activeRun: { id: "run:active" },
      canPromoteToSteer: true,
      canReorder: true,
      queuedRuns: [],
    };

    const view = await render({
      optimisticMessages: [
        {
          id: "message:acknowledged" as never,
          inputIntent: "queued_turn",
          text: "hello",
          attachments: [],
        },
      ],
    });

    expect(view.childElementCount).toBe(0);
  });

  it("keeps the original queued message visible while editing", async () => {
    state.projection = { projection: { messages: [] } };
    state.workflow = workflowWithAttachment();

    const view = await render({ editingRunId: "run:queued" as never });

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
    state.projection = { projection: { messages: [] } };
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
          isMobileViewport: false,
          isDraftThread: false,
          isRunning: true,
          prompt: "",
        },
        hasDraftContext: false,
        queueActionDisabled: false,
        onSteerNextQueuedMessage: () => ref.current?.steerNext(false) ?? false,
        onSubmit,
      });
    let renderer: Root | undefined;
    const container = document.createElement("div");
    document.body.append(container);
    vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
    try {
      await act(async () => {
        renderer = createRoot(container);
        renderer.render(control());
      });
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
      state.projection = { projection: { messages: [] } };
      await act(async () => {
        renderer!.render(control());
      });
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
      state.projection = { projection: { messages: [] } };
      await act(async () => {
        renderer!.render(control());
      });
      expect(enter()).toBe(true);
      expect(onSubmit).toHaveBeenCalledExactlyOnceWith("foreground");
      expect(state.promote).toHaveBeenCalledTimes(2);
    } finally {
      await act(async () => {
        finishSend?.();
        renderer?.unmount();
      });
      container.remove();
      state.promote.mockReset();
      state.promote.mockImplementation(async () => undefined);
      state.projection = null;
      state.workflow = null;
      vi.unstubAllGlobals();
    }
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
    const view = await render();
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
    const view = await render({ onImportedHistoryChoice });
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
  it("offers no imported review once native provider context exists", async () => {
    setup(true, [
      {
        providerSessionId: "session:existing",
        nativeThreadRef: null,
        nativeConversationHeadRef: null,
      },
    ]);
    const view = await render({ onImportedHistoryChoice: vi.fn() });
    expect(view.textContent).not.toContain("Review imported history");
    expect(view.textContent).toContain(queued.text);
  });
});

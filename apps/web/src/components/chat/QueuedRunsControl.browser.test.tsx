import { EnvironmentId, MessageId, ThreadId } from "@t3tools/contracts";
import { serializeAssistantCitation } from "@t3tools/shared/assistantCitations";
import type { ComponentProps } from "react";
import { expect, it, vi } from "vite-plus/test";
import { page } from "vite-plus/test/browser";

const state = vi.hoisted(() => ({
  projection: null as unknown,
  workflow: null as unknown,
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
  useAtomCommand: (() => state.promote) as never,
}));

vi.mock(import("../../assets/assetUrls"), async (importOriginal) => ({
  ...(await importOriginal()),
  useAssetUrls: ((_environmentId: never, resources: ReadonlyArray<{ attachmentId: string }>) =>
    resources.map((resource) => `https://assets.test/${resource.attachmentId}`)) as never,
}));

import { render as renderUi } from "../../../test/render";
import { QueuedRunsControl } from "./QueuedRunsControl";

function setup() {
  state.projection = null;
  state.workflow = null;
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
  state.projection = { projection: { messages: [] } };
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

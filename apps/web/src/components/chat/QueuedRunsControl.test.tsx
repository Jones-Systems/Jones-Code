// @vitest-environment jsdom
import { renderToStaticMarkup } from "react-dom/server";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

const state = vi.hoisted(() => ({
  projection: null as unknown,
  workflow: null as unknown,
  promote: vi.fn(),
}));

vi.mock("@t3tools/client-runtime/environment", async (importOriginal) => ({
  ...await importOriginal<typeof import("@t3tools/client-runtime/environment")>(),
}));

vi.mock("@t3tools/client-runtime/state/thread-workflows", () => ({
  deriveThreadQueueWorkflowState: () => state.workflow,
}));

vi.mock("../../state/entities", () => ({
  useThreadProjection: () => state.projection,
}));

vi.mock("../../state/threads", () => ({
  threadEnvironment: {
    cancelQueuedRun: Symbol("cancelQueuedRun"),
    promoteQueuedRun: Symbol("promoteQueuedRun"),
    reorderQueuedRun: Symbol("reorderQueuedRun"),
    reviewImportedHistoryStart: Symbol("reviewImportedHistoryStart"),
    deliverImportedContinuation: Symbol("deliverImportedContinuation"),
    observeImportedHistoryStart: Symbol("observeImportedHistoryStart"),
  },
}));

vi.mock("../../state/use-atom-command", () => ({
  useAtomCommand: (command: symbol) => command.description === "promoteQueuedRun" ? state.promote : unavailableCommand,
}));

const unavailableCommand = async () => ({ _tag: "Failure" });

vi.mock("../../assets/assetUrls", () => ({
  useAssetUrls: (_environmentId: never, resources: ReadonlyArray<{ attachmentId: string }>) =>
    resources.map((resource) => `https://assets.test/${resource.attachmentId}`),
}));

import { act, createRef } from "react";
import { createRoot, type Root } from "react-dom/client";
import { handleComposerEnter } from "./composerSubmission";
import { QueuedRunsControl, type QueuedRunsControlHandle } from "./QueuedRunsControl";

describe("QueuedRunsControl automatic completion delivery", () => {
  it("does not render a queue control when only hidden delivery remains", () => {
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

    const html = renderToStaticMarkup(
      <QueuedRunsControl
        environmentId={"environment:test" as never}
        optimisticMessages={[]}
        threadId={"thread:test" as never}
        editingRunId={null}
        onEditQueuedRun={() => undefined}
        onCancelEdit={() => undefined}
      />,
    );

    expect(html).toBe("");
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

  it("renders an attachment thumbnail on the queued row", () => {
    state.projection = { projection: { messages: [] } };
    state.workflow = workflowWithAttachment();

    const html = renderToStaticMarkup(
      <QueuedRunsControl
        environmentId={"environment:test" as never}
        optimisticMessages={[]}
        threadId={"thread:test" as never}
        editingRunId={null}
        onEditQueuedRun={() => undefined}
        onCancelEdit={() => undefined}
      />,
    );

    expect(html).toContain("https://assets.test/attachment-1");
    expect(html).toContain("Queued with a screenshot");
    expect(html).toContain("Edit queued message");
    expect(html).toContain("Reorder queued message");
    expect(html).not.toContain("Move queued message up");
  });

  it("drops the optimistic pending row once the projection holds its message", () => {
    state.projection = {
      projection: { messages: [{ id: "message:acknowledged", text: "hello" }] },
    };
    state.workflow = {
      activeRun: { id: "run:active" },
      canPromoteToSteer: true,
      canReorder: true,
      queuedRuns: [],
    };

    const html = renderToStaticMarkup(
      <QueuedRunsControl
        environmentId={"environment:test" as never}
        optimisticMessages={[
          {
            id: "message:acknowledged" as never,
            inputIntent: "queued_turn",
            text: "hello",
            attachments: [],
          },
        ]}
        threadId={"thread:test" as never}
        editingRunId={null}
        onEditQueuedRun={() => undefined}
        onCancelEdit={() => undefined}
      />,
    );

    expect(html).toBe("");
  });

  it("keeps the original queued message visible while editing", () => {
    state.projection = { projection: { messages: [] } };
    state.workflow = workflowWithAttachment();

    const html = renderToStaticMarkup(
      <QueuedRunsControl
        environmentId={"environment:test" as never}
        optimisticMessages={[]}
        threadId={"thread:test" as never}
        editingRunId={"run:queued" as never}
        onEditQueuedRun={() => undefined}
        onCancelEdit={() => undefined}
      />,
    );

    expect(html).toContain("Queued with a screenshot");
  });
});


describe("QueuedRunsControl promotion actions", () => {
  let root: Root;
  let container: HTMLDivElement;
  beforeEach(() => {
    container = document.createElement("div");
    document.body.append(container);
    root = createRoot(container);
    state.projection = { projection: { messages: [] } };
    state.promote.mockReset();
  });
  afterEach(async () => {
    await act(async () => root.unmount());
    container.remove();
  });

  function workflow(canPromoteToSteer = true) {
    return {
      activeRun: { id: "run:active" },
      canPromoteToSteer,
      canReorder: true,
      queuedRuns: ["first", "second"].map((name) => ({
        run: { id: `run:${name}`, userMessageId: `message:${name}` },
        text: name,
        attachments: [],
      })),
    };
  }

  async function mount(ref = createRef<QueuedRunsControlHandle>()) {
    await act(async () => root.render(
      <QueuedRunsControl
        ref={ref}
        environmentId={"environment:test" as never}
        optimisticMessages={[]}
        threadId={"thread:test" as never}
        editingRunId={null}
        onEditQueuedRun={() => undefined}
        onCancelEdit={() => undefined}
      />,
    ));
    return ref;
  }

  it("promotes one server entry through empty Enter and shares the arrow's in-flight guard", async () => {
    state.workflow = workflow();
    let finish: () => void = () => undefined;
    state.promote.mockImplementationOnce(() => new Promise<void>((resolve) => { finish = resolve; }));
    const ref = await mount();
    const enter = (repeat = false) => handleComposerEnter({
      event: { shiftKey: false, altKey: false, metaKey: false, ctrlKey: false, isComposing: false, keyCode: 13, repeat },
      intent: { isMobileViewport: false, isDraftThread: false, isRunning: true, prompt: "" },
      hasDraftContext: false,
      queueActionDisabled: false,
      onSteerNextQueuedMessage: () => ref.current?.steerNext(false) ?? false,
      onSubmit: () => { throw new Error("An empty queued Enter must not submit a draft"); },
    });
    await act(async () => {
      expect(enter()).toBe(true);
      const arrow = [...container.querySelectorAll<HTMLButtonElement>("button")].find((button) => button.textContent?.includes("Steer"));
      expect(arrow).toBeDefined();
      arrow!.click();
      expect(enter()).toBe(true);
    });
    expect(state.promote).toHaveBeenCalledExactlyOnceWith({
      environmentId: "environment:test",
      input: { threadId: "thread:test", queuedRunId: "run:first", targetRunId: "run:active" },
    });
    await act(async () => {
      const arrow = [...container.querySelectorAll<HTMLButtonElement>("button")].find((button) => button.textContent?.includes("Steer"));
      expect(arrow).toBeDefined();
      arrow!.click();
      expect(enter()).toBe(true);
    });
    expect(state.promote).toHaveBeenCalledTimes(1);
    await act(async () => finish());
    state.workflow = { ...workflow(), queuedRuns: workflow().queuedRuns.slice(1) };
    // A server queue event replaces the immutable V2 projection that derives the workflow.
    state.projection = { projection: { messages: [{ id: "message:first" }] } };
    await mount(ref);
    expect(container.textContent).not.toContain("first");
    expect(container.textContent).toContain("second");
    await act(async () => { expect(enter(true)).toBe(true); });
    expect(state.promote).toHaveBeenCalledTimes(1);
    await act(async () => {
      expect(enter()).toBe(true);
      const arrow = [...container.querySelectorAll<HTMLButtonElement>("button")].find((button) => button.textContent?.includes("Steer"));
      expect(arrow).toBeDefined();
      arrow!.click();
    });
    expect(state.promote).toHaveBeenCalledTimes(2);
    expect(state.promote).toHaveBeenLastCalledWith({
      environmentId: "environment:test",
      input: { threadId: "thread:test", queuedRunId: "run:second", targetRunId: "run:active" },
    });
  });

  it("declines a held server queue and preserves the draft path", async () => {
    state.workflow = workflow(false);
    const ref = await mount();
    expect(ref.current?.steerNext(false)).toBe(false);
    expect(state.promote).not.toHaveBeenCalled();
  });
});

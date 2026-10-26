import { createRef, type ComponentProps } from "react";
import { describe, expect, it, vi } from "vite-plus/test";
import { act, render } from "../../../test/render";

const state = vi.hoisted(() => ({
  workflow: null as unknown,
  cancel: vi.fn(),
  other: vi.fn(),
}));
vi.mock(import("@t3tools/client-runtime/state/thread-workflows"), async (original) => ({
  ...(await original()),
  deriveThreadQueueWorkflowState: (() => state.workflow) as never,
}));
vi.mock(import("../../state/entities"), async (original) => ({
  ...(await original()),
  useThreadProjection: (() => ({ projection: { messages: [] } })) as never,
  useServerConfigs: (() => new Map()) as never,
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
vi.mock(import("../../state/use-atom-command"), async (original) => ({
  ...(await original()),
  useAtomCommand: ((command: { label: string }) =>
    command.label.includes("cancel-queued-run") ? state.cancel : state.other) as never,
}));
vi.mock(import("../../assets/assetUrls"), async (original) => ({
  ...(await original()),
  useAssetUrls: (() => []) as never,
}));
import {
  QueuedRunsControl,
  type QueuedRunsControlHandle,
} from "../../components/chat/QueuedRunsControl";

const props = {
  environmentId: "environment:test" as never,
  threadId: "thread:test" as never,
  optimisticMessages: [],
  editingRunId: null,
  onEditQueuedRun: vi.fn(),
  onCancelEdit: vi.fn(),
} satisfies ComponentProps<typeof QueuedRunsControl>;

function workflow(ids = ["a", "b", "c"]) {
  return {
    activeRun: { id: "active" },
    canPromoteToSteer: true,
    canReorder: true,
    queuedRuns: ids.map((id) => ({
      run: { id, userMessageId: `message-${id}` },
      messageId: `message-${id}`,
      text: `Prompt ${id}`,
      attachments: [],
    })),
  };
}
function setup() {
  state.workflow = workflow();
  state.cancel.mockReset();
  state.other.mockReset().mockResolvedValue({ _tag: "Success" });
  props.onEditQueuedRun.mockClear();
  props.onCancelEdit.mockClear();
}
function deferred() {
  let resolve!: (result: { _tag: string }) => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<{ _tag: string }>((yes, no) => {
    resolve = yes;
    reject = no;
  });
  return { promise, resolve, reject };
}
function button(container: HTMLElement, name: string) {
  return [...container.querySelectorAll("button")].find(
    (node) => node.getAttribute("aria-label") === name || node.textContent?.trim() === name,
  )!;
}
function removeButtons(container: HTMLElement) {
  return [
    ...container.querySelectorAll<HTMLButtonElement>('button[aria-label="Remove queued message"]'),
  ];
}
function cancelledIds() {
  return state.cancel.mock.calls.map(([request]) => request.input.runId);
}

describe("independent queued prompt removal", () => {
  it("allows rapid independent X clicks, deduplicates a repeated X, and fences send/edit/reorder", async () => {
    setup();
    const first = deferred();
    const second = deferred();
    state.cancel.mockReturnValueOnce(first.promise).mockReturnValueOnce(second.promise);
    const ref = createRef<QueuedRunsControlHandle>();
    await using screen = await render(<QueuedRunsControl {...props} ref={ref} />, {
      router: false,
    });
    const view = screen.container;
    await act(() => {
      const buttons = removeButtons(view);
      buttons[0]!.click();
      buttons[0]!.click();
      buttons[1]!.click();
      button(view, "Steer").click();
      button(view, "Edit queued message").click();
      button(view, "Reorder queued message (drag, or press the arrow keys)").dispatchEvent(
        new KeyboardEvent("keydown", { key: "ArrowDown", bubbles: true }),
      );
      ref.current!.steerNext(false);
      expect(ref.current!.editLatest(false)).toBe(false);
    });
    expect(cancelledIds()).toEqual(["a", "b"]);
    expect(removeButtons(view).map((node) => node.disabled)).toEqual([true, true, false]);
    expect(state.other).not.toHaveBeenCalled();
    expect(props.onEditQueuedRun).not.toHaveBeenCalled();
    await act(() => second.resolve({ _tag: "Failure" }));
    expect(removeButtons(view).map((node) => node.disabled)).toEqual([true, false, false]);
    expect(view.querySelector('[role="alert"]')?.textContent).toContain(
      "1 queued removal was not confirmed",
    );
    await act(() => first.resolve({ _tag: "Success" }));
    expect(removeButtons(view)[0]!.disabled).toBe(true);
    expect(button(view, "Steer").disabled).toBe(true);
    expect(view.querySelectorAll("li")).toHaveLength(3);
    state.workflow = workflow(["b", "c"]);
    await screen.rerender(<QueuedRunsControl {...props} ref={ref} />);
    expect(button(view, "Steer").disabled).toBe(false);
  });

  it("clears only the click snapshot, preserves later arrivals and failures, and retries only remaining IDs", async () => {
    setup();
    const requests = [deferred(), deferred(), deferred()];
    for (const request of requests) state.cancel.mockReturnValueOnce(request.promise);
    await using screen = await render(<QueuedRunsControl {...props} />, { router: false });
    const view = screen.container;
    const clear = button(view, "Clear All");
    expect(clear.parentElement?.getAttribute("data-slot")).toBe("composer-banner-content");
    expect(clear.previousSibling?.textContent).toContain("Queued");
    await act(() => {
      removeButtons(view)[0]!.click();
      clear.click();
      clear.click();
    });
    expect(cancelledIds()).toEqual(["a", "b", "c"]);
    expect(clear.disabled).toBe(true);
    state.workflow = workflow(["a", "b", "c", "later"]);
    await screen.rerender(<QueuedRunsControl {...props} />);
    expect(removeButtons(view)[3]!.disabled).toBe(false);
    await act(() => {
      requests[2]!.reject(new Error("connection lost"));
      requests[0]!.resolve({ _tag: "Success" });
      requests[1]!.resolve({ _tag: "Failure" });
    });
    expect(view.querySelectorAll("li")).toHaveLength(4);
    expect(view.querySelector('[role="alert"]')?.textContent).toContain(
      "2 queued removals were not confirmed",
    );
    expect(cancelledIds()).not.toContain("later");
    state.workflow = workflow(["b", "c", "later"]);
    await screen.rerender(<QueuedRunsControl {...props} />);
    state.cancel.mockResolvedValue({ _tag: "Success" });
    await act(() => button(view, "Clear All").click());
    expect(cancelledIds()).toEqual(["a", "b", "c", "b", "c", "later"]);
    expect(state.other).not.toHaveBeenCalled();
    state.workflow = workflow([]);
    await screen.rerender(<QueuedRunsControl {...props} />);
    expect(view.childElementCount).toBe(0);
  });

  it("blocks X and Clear All when steering starts before React publishes its busy state", async () => {
    setup();
    const send = deferred();
    state.other.mockReturnValue(send.promise);
    await using screen = await render(<QueuedRunsControl {...props} />, { router: false });
    const view = screen.container;
    await act(() => {
      button(view, "Steer").click();
      removeButtons(view)[0]!.click();
      button(view, "Clear All").click();
    });
    expect(state.cancel).not.toHaveBeenCalled();
    expect(state.other).toHaveBeenCalledTimes(1);
    expect(removeButtons(view).every((node) => node.disabled)).toBe(true);
    expect(button(view, "Clear All").disabled).toBe(true);
    await act(() => send.resolve({ _tag: "Success" }));
    expect(removeButtons(view).every((node) => !node.disabled)).toBe(true);
  });

  it.each(["editing", "saving"])("keeps Clear All disabled while %s", async (condition) => {
    setup();
    const input = {
      ...props,
      ...(condition === "editing"
        ? { editingRunId: "a" as never }
        : {
            optimisticMessages: [
              { id: "saving" as never, inputIntent: "queued_turn" as const, text: "Unsaved" },
            ],
          }),
    };
    await using screen = await render(<QueuedRunsControl {...input} />, { router: false });
    expect(button(screen.container, "Clear All").disabled).toBe(true);
    await act(() => button(screen.container, "Clear All").click());
    expect(state.cancel).not.toHaveBeenCalled();
    expect(props.onCancelEdit).not.toHaveBeenCalled();
  });

  it("does not let old-thread completions unlock a new thread's removal", async () => {
    setup();
    const oldRequest = deferred();
    const newRequest = deferred();
    state.cancel.mockReturnValueOnce(oldRequest.promise).mockReturnValueOnce(newRequest.promise);
    await using screen = await render(<QueuedRunsControl {...props} />, { router: false });
    await act(() => removeButtons(screen.container)[0]!.click());
    const nextProps = { ...props, threadId: "thread:next" as never };
    await screen.rerender(<QueuedRunsControl {...nextProps} />);
    expect(removeButtons(screen.container)[0]!.disabled).toBe(false);
    await act(() => removeButtons(screen.container)[0]!.click());
    await act(() => oldRequest.resolve({ _tag: "Failure" }));
    expect(removeButtons(screen.container)[0]!.disabled).toBe(true);
    expect(screen.container.querySelector('[role="alert"]')).toBeNull();
    expect(state.cancel.mock.calls[1]![0].input.threadId).toBe("thread:next");
    await act(() => newRequest.resolve({ _tag: "Failure" }));
    expect(removeButtons(screen.container)[0]!.disabled).toBe(false);
  });
});

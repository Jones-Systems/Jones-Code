import { act, render as renderTest } from "../../../test/render";
import { createRef } from "react";
import { describe, expect, it, vi } from "vite-plus/test";
import {
  QUEUED_STEER_BATCH_CAPABILITY,
  QUEUED_STEER_BATCH_LIMITS,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { page } from "vite-plus/test/browser";
import "~/index.css";

const state = vi.hoisted(() => ({
  projection: null as unknown,
  capability: undefined as unknown,
  batch: vi.fn(),
  row: vi.fn(),
}));
vi.mock(import("@t3tools/client-runtime/state/thread-workflows"), async (original) => {
  const actual = await original();
  return {
    ...actual,
    deriveThreadQueueWorkflowState: ((
      ...args: Parameters<typeof actual.deriveThreadQueueWorkflowState>
    ) => ({ ...actual.deriveThreadQueueWorkflowState(...args), canReorder: true })) as never,
  };
});
vi.mock(import("../../state/entities"), async (original) => ({
  ...(await original()),
  useQuietThreadProjection: (() => ({ projection: state.projection })) as never,
}));
vi.mock(import("../../state/server"), async (original) => {
  const actual = await original();
  const { Atom } = await import("effect/reactivity");
  return {
    ...actual,
    serverEnvironment: {
      ...actual.serverEnvironment,
      configValueAtom: () =>
        Atom.make({ environment: { capabilities: { queuedSteerBatch: state.capability } } }),
    },
  } as never;
});
vi.mock(import("../../state/use-atom-command"), async (original) => ({
  ...(await original()),
  useAtomCommand: ((command: { label: string }) =>
    command.label.includes("promote-queued-batch") ? state.batch : state.row) as never,
}));
vi.mock(import("../../assets/assetUrls"), async (original) => ({
  ...(await original()),
  useAssetUrls: (() => []) as never,
}));

// Model the frozen additive queue command interface; exact package/runtime qualification remains blocked by L5-REQ05.
vi.mock(import("../../state/threads"), async (original) => {
  const actual = await original();
  return {
    ...actual,
    threadEnvironment: {
      ...actual.threadEnvironment,
      reorderQueuedRun: { label: "environment-data:commands:thread:reorder-queued-run" },
    },
  } as never;
});
import {
  QueuedRunsControl,
  type QueuedRunsControlHandle,
} from "../../components/chat/QueuedRunsControl";

function queueProjection(count = 22): OrchestrationV2ThreadProjection {
  const modelSelection = { instanceId: "codex", model: "gpt-test" };
  return {
    thread: {
      id: "thread:test",
      modelSelection,
      providerInstanceId: "codex",
      activeProviderThreadId: "provider",
      archivedAt: null,
      deletedAt: null,
    },
    runs: [
      {
        id: "active",
        status: "running",
        activeAttemptId: "attempt",
        rootNodeId: "active-node",
        providerInstanceId: "codex",
        providerThreadId: "provider",
        modelSelection,
      },
      ...Array.from({ length: count }, (_, i) => ({
        id: `queued-${i}`,
        userMessageId: `message-${i}`,
        ordinal: i + 2,
        status: "queued",
        providerThreadId: "provider",
        providerInstanceId: "codex",
        rootNodeId: `node-${i}`,
        activeAttemptId: `attempt-${i}`,
        modelSelection,
      })),
    ],
    messages: Array.from({ length: count }, (_, i) => ({
      id: `message-${i}`,
      text: `Queued context ${i}`,
      attachments: [],
      createdBy: "user",
      creationSource: "web",
      createdAt: DateTime.makeUnsafe("2026-10-20T12:00:00Z"),
      updatedAt: DateTime.makeUnsafe("2026-10-20T12:00:00Z"),
    })),
    runtimeRequests: [],
    nodes: Array.from({ length: count }, (_, i) => ({ id: `node-${i}` })),
    attempts: Array.from({ length: count }, (_, i) => ({ id: `attempt-${i}` })),
    providerThreads: [{ id: "provider", providerSessionId: "session" }],
    providerSessions: [
      {
        id: "session",
        driver: "codex",
        status: "running",
        capabilities: {
          turns: {
            supportsActiveSteering: true,
            supportsQueuedMessages: true,
          },
        },
      },
    ],
    providerTurns: [{ runAttemptId: "attempt", status: "running" }],
  } as unknown as OrchestrationV2ThreadProjection;
}

function setupTest() {
  state.projection = queueProjection();
  state.capability = QUEUED_STEER_BATCH_LIMITS;
  state.batch.mockReset().mockResolvedValue({ _tag: "Success", value: { sequence: 1 } });
  state.row.mockReset().mockResolvedValue({ _tag: "Success" });
}

async function render(props: Partial<React.ComponentProps<typeof QueuedRunsControl>> = {}) {
  return renderTest(
    <QueuedRunsControl
      environmentId={"environment:test" as never}
      threadId={"thread:test" as never}
      optimisticMessages={[]}
      editingRunId={null}
      editBlockedReason={null}
      timestampFormat="locale"
      onOpenThread={() => undefined}
      onEditQueuedRun={() => undefined}
      onCancelEdit={() => undefined}
      onImageExpand={() => undefined}
      {...props}
    />,
    { router: false },
  );
}
function button(view: HTMLElement, label: string) {
  return [...view.querySelectorAll("button")].find(
    (node) => node.textContent?.trim() === label || node.getAttribute("aria-label") === label,
  );
}

describe("queue header batch action", () => {
  it("one click dispatches all 22 IDs once, keeps the queue expanded, and locks row actions", async () => {
    setupTest();
    const ref = createRef<QueuedRunsControlHandle>();
    const onEditQueuedRun = vi.fn();
    let finish!: (result: unknown) => void;
    state.batch.mockImplementation(
      () =>
        new Promise((resolve) => {
          finish = resolve;
        }),
    );
    await using screen = await render({ ref, onEditQueuedRun });
    const view = screen.container;
    const send = button(view, "Steer all (22)")!;
    expect(send).toBeDefined();
    expect(send.closest("button")?.parentElement?.closest("button")).toBeNull();
    await act(async () => {
      send.click();
      send.click();
      ref.current?.steerNext(false);
      button(view, "Edit queued message")!.click();
      button(view, "Remove queued message")!.click();
    });
    expect(state.batch).toHaveBeenCalledTimes(1);
    expect(state.row).not.toHaveBeenCalled();
    expect(onEditQueuedRun).not.toHaveBeenCalled();
    for (const row of view.querySelectorAll("li")) {
      for (const action of row.querySelectorAll("button")) expect(action.disabled).toBe(true);
    }
    const command = state.batch.mock.calls[0]![0];
    expect(command).toMatchObject({
      environmentId: "environment:test",
      input: { threadId: "thread:test", targetRunId: "active", commandId: expect.any(String) },
    });
    expect(command.input.items).toEqual(
      Array.from({ length: 22 }, (_, i) => ({
        queuedRunId: `queued-${i}`,
        messageId: `message-${i}`,
        messageUpdatedAt: "2026-10-20T12:00:00.000Z",
      })),
    );
    expect(button(view, "Collapse queued messages")?.getAttribute("aria-expanded")).toBe("true");
    for (const label of [
      "Edit queued message",
      "Remove queued message",
      "Steer",
      "Reorder queued message (drag, or press the arrow keys)",
    ]) {
      expect(button(view, label)?.disabled).toBe(true);
    }
    await act(async () => {
      finish({ _tag: "Success" });
    });
    expect(button(view, "Steer all (22)")?.disabled).toBe(true);
    await act(async () => {
      button(view, "Collapse queued messages")!.click();
    });
    expect(button(view, "Expand queued messages")?.getAttribute("aria-expanded")).toBe("false");
  });

  it("steers all four messages queued before an account switch in one dispatch", async () => {
    setupTest();
    const projection = queueProjection(4);
    state.projection = {
      ...projection,
      runs: projection.runs.map((run) =>
        run.status !== "queued"
          ? run
          : {
              ...run,
              providerInstanceId: "old-account",
              providerThreadId: "old-provider-thread",
              modelSelection: {
                instanceId: "old-account",
                model: "old-model",
                options: [{ id: "reasoningEffort", value: "low" }],
              },
            },
      ),
    };
    await using screen = await render();
    const view = screen.container;
    const send = button(view, "Steer all (4)")!;
    expect(send.disabled).toBe(false);
    await act(async () => {
      send.click();
    });
    expect(state.batch).toHaveBeenCalledTimes(1);
    expect(
      state.batch.mock.calls[0]![0].input.items.map(
        (item: { messageId: string }) => item.messageId,
      ),
    ).toEqual(["message-0", "message-1", "message-2", "message-3"]);
    expect(state.row).not.toHaveBeenCalled();
  });

  it("hides on old servers and never dispatches a fallback", async () => {
    setupTest();
    state.capability = undefined;
    await using screen = await render();
    const view = screen.container;
    expect(button(view, "Steer all (22)")).toBeUndefined();
    expect(state.batch).not.toHaveBeenCalled();
    expect(state.row).not.toHaveBeenCalled();
  });

  it("sends all 19 messages into the running turn after saving a new speed", async () => {
    setupTest();
    state.capability = QUEUED_STEER_BATCH_CAPABILITY;
    const projection = queueProjection(19);
    state.projection = {
      ...projection,
      thread: {
        ...projection.thread,
        modelSelection: {
          ...projection.thread.modelSelection,
          options: [{ id: "serviceTier", value: "ultrafast" }],
        },
      },
    };
    await using screen = await render();
    const send = button(screen.container, "Steer all (19)")!;
    expect(send.disabled).toBe(false);
    await act(async () => {
      send.click();
    });
    expect(state.batch).toHaveBeenCalledExactlyOnceWith({
      environmentId: "environment:test",
      input: {
        commandId: expect.any(String),
        threadId: "thread:test",
        targetRunId: "active",
        items: Array.from({ length: 19 }, (_, index) => ({
          queuedRunId: `queued-${index}`,
          messageId: `message-${index}`,
          messageUpdatedAt: "2026-10-20T12:00:00.000Z",
        })),
      },
    });
  });

  it("shows the bounded prefix and remainder instead of claiming all", async () => {
    setupTest();
    state.capability = { ...QUEUED_STEER_BATCH_LIMITS, maxItems: 3 };
    await using screen = await render();
    const view = screen.container;
    expect(button(view, "Steer all (22)")).toBeUndefined();
    expect(view.textContent).toContain("19 still queued. Batch size limit");
    await act(async () => {
      button(view, "Steer next 3")!.click();
    });
    expect(state.batch.mock.calls[0]![0].input.items).toHaveLength(3);
    expect(view.querySelectorAll("li")).toHaveLength(22);
  });

  it.each(["editing", "unsaved", "pending question", "ended turn", "restart only", "oversized"])(
    "disables while %s",
    async (condition) => {
      setupTest();
      const projection = state.projection as OrchestrationV2ThreadProjection;
      if (condition === "pending question")
        state.projection = {
          ...projection,
          runtimeRequests: [
            {
              id: "request:test",
              status: "pending",
              kind: "user_input",
              responseCapability: { type: "live" },
              createdAt: DateTime.makeUnsafe("2026-10-20T12:00:00Z"),
            },
          ],
          turnItems: [
            {
              type: "user_input_request",
              requestId: "request:test",
              questions: [{ id: "q1", header: "Choice", question: "Which option?", options: [] }],
            },
          ],
        };
      if (condition === "ended turn") state.projection = { ...projection, providerTurns: [] };
      if (condition === "restart only")
        state.projection = {
          ...projection,
          providerSessions: [
            {
              ...projection.providerSessions[0],
              capabilities: {
                turns: { supportsActiveSteering: false, supportsSteeringByInterruptRestart: true },
              },
            },
          ],
        };
      if (condition === "oversized")
        state.capability = { ...QUEUED_STEER_BATCH_LIMITS, maxInputChars: 1 };
      await using screen = await render({
        ...(condition === "editing" ? { editingRunId: "queued-0" as never } : {}),
        ...(condition === "unsaved"
          ? {
              optimisticMessages: [
                {
                  id: "unsaved" as never,
                  text: "Unsaved",
                  inputIntent: "queued_turn" as const,
                  attachments: [],
                },
              ],
            }
          : {}),
      });
      const view = screen.container;
      expect(button(view, "Steer all (22)")?.disabled).toBe(true);
      expect(state.batch).not.toHaveBeenCalled();
    },
  );

  it.each(["failure", "rejection"])(
    "keeps all rows after %s and retries the identical batch",
    async (outcome) => {
      setupTest();
      if (outcome === "rejection")
        state.batch.mockRejectedValueOnce(new Error("Transport unavailable"));
      else state.batch.mockResolvedValueOnce({ _tag: "Failure" });
      await using screen = await render();
      const view = screen.container;
      await act(async () => {
        button(view, "Steer all (22)")!.click();
      });
      expect(view.querySelectorAll("li")).toHaveLength(22);
      expect(view.querySelector("[role=alert]")?.textContent).toContain("not confirmed");
      expect(button(view, "Remove queued message")?.disabled).toBe(false);
      await act(async () => {
        button(view, "Retry steer (22)")!.click();
      });
      expect(state.batch).toHaveBeenCalledTimes(2);
      expect(state.batch.mock.calls[1]![0].input).toBe(state.batch.mock.calls[0]![0].input);
      expect(state.row).not.toHaveBeenCalled();
    },
  );
});

it("keeps queue controls grouped and contained at narrow widths", async () => {
  setupTest();
  const projection = queueProjection(2);
  state.projection = {
    ...projection,
    messages: projection.messages.map((message) => ({
      ...message,
      text: "A long queued message with an identifier: " + "long-identifier".repeat(30),
    })),
  };
  const originalViewport = { width: window.innerWidth, height: window.innerHeight };
  try {
    await page.viewport(800, 700);
    await using screen = await render();
    const container = screen.container;
    container.style.width = "640px";
    const row = container.querySelector("li")!;
    const grip = button(row, "Reorder queued message (drag, or press the arrow keys)")!;
    const edit = button(row, "Edit queued message")!;
    const remove = button(row, "Remove queued message")!;
    const center = (node: Element) => {
      const rect = node.getBoundingClientRect();
      return rect.top + rect.height / 2;
    };
    expect(Math.abs(center(grip) - center(edit))).toBeLessThan(1);
    expect(Math.abs(center(grip) - center(remove))).toBeLessThan(1);
    await page.viewport(320, 700);
    container.style.width = "240px";
    await vi.waitFor(() => {
      expect(container.scrollWidth).toBeLessThanOrEqual(container.clientWidth);
      for (const control of container.querySelectorAll("button")) {
        const rect = control.getBoundingClientRect();
        expect(rect.left).toBeGreaterThanOrEqual(container.getBoundingClientRect().left - 1);
        expect(rect.right).toBeLessThanOrEqual(container.getBoundingClientRect().right + 1);
      }
    });
  } finally {
    await page.viewport(originalViewport.width, originalViewport.height);
  }
});

import { act, render as renderTest } from "../../../test/render";
import { createRef } from "react";
import { describe, expect, it, vi } from "vite-plus/test";
import {
  QUEUED_STEER_BATCH_LIMITS,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

const state = vi.hoisted(() => ({
  projection: null as unknown,
  capability: undefined as unknown,
  batch: vi.fn(),
  row: vi.fn(),
}));
vi.mock(import("../../state/entities"), async (original) => ({
  ...(await original()),
  useThreadProjection: (() => ({ projection: state.projection })) as never,
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

import {
  QueuedRunsControl,
  type QueuedRunsControlHandle,
} from "../../components/chat/QueuedRunsControl";

function queueProjection(): OrchestrationV2ThreadProjection {
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
      ...Array.from({ length: 22 }, (_, i) => ({
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
    messages: Array.from({ length: 22 }, (_, i) => ({
      id: `message-${i}`,
      text: `Queued context ${i}`,
      attachments: [],
      createdBy: "user",
      creationSource: "web",
      updatedAt: DateTime.makeUnsafe("2026-10-20T12:00:00Z"),
    })),
    runtimeRequests: [],
    nodes: Array.from({ length: 22 }, (_, i) => ({ id: `node-${i}` })),
    attempts: Array.from({ length: 22 }, (_, i) => ({ id: `attempt-${i}` })),
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
      onEditQueuedRun={() => undefined}
      onCancelEdit={() => undefined}
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

  it("hides on old servers and never dispatches a fallback", async () => {
    setupTest();
    state.capability = undefined;
    await using screen = await render();
    const view = screen.container;
    expect(button(view, "Steer all (22)")).toBeUndefined();
    expect(state.batch).not.toHaveBeenCalled();
    expect(state.row).not.toHaveBeenCalled();
  });

  it("shows the bounded prefix and remainder instead of claiming all", async () => {
    setupTest();
    state.capability = { ...QUEUED_STEER_BATCH_LIMITS, maxItems: 3 };
    await using screen = await render();
    const view = screen.container;
    expect(button(view, "Steer all (22)")).toBeUndefined();
    expect(view.textContent).toContain("19 still queued — batch size limit");
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
        state.projection = { ...projection, runtimeRequests: [{ status: "pending" }] };
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

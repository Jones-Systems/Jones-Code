import { describe, expect, it, vi } from "vite-plus/test";
import {
  CommandId,
  QUEUED_STEER_BATCH_LIMITS,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { v2Projection } from "../../state/orchestrationV2TestFixtures.ts";
import { createQueuedSteerBatchSubmission, deriveQueuedSteerBatch } from "./queuedSteerBatch.ts";

function projection(count = 22): OrchestrationV2ThreadProjection {
  const modelSelection = v2Projection.thread.modelSelection;
  return {
    ...v2Projection,
    thread: { ...v2Projection.thread, activeProviderThreadId: "provider" as never },
    runs: [
      {
        id: "active",
        status: "running",
        activeAttemptId: "attempt",
        rootNodeId: "active-node",
        providerInstanceId: "codex",
        providerThreadId: "provider",
        userMessageId: "active-message",
        modelSelection,
      },
      ...Array.from({ length: count }, (_, i) => ({
        id: `queued-${i}`,
        status: "queued",
        ordinal: i + 2,
        userMessageId: `message-${i}`,
        providerThreadId: "provider",
        providerInstanceId: "codex",
        rootNodeId: `node-${i}`,
        activeAttemptId: `attempt-${i}`,
        modelSelection,
      })),
    ],
    messages: Array.from({ length: count }, (_, i) => ({
      id: `message-${i}`,
      text: `Context ${i}`,
      attachments: [],
      createdBy: "user",
      creationSource: "web",
      updatedAt: DateTime.makeUnsafe("2026-10-20T12:00:00Z"),
    })),
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
            supportsSteeringByInterruptRestart: true,
          },
        },
      },
    ],
    providerTurns: [{ runAttemptId: "attempt", status: "running" }],
  } as unknown as OrchestrationV2ThreadProjection;
}

const input = () => ({
  projection: projection(),
  capability: QUEUED_STEER_BATCH_LIMITS,
  editing: false,
  hasUnsavedMessages: false,
  busy: false,
});

describe("queued batch client state", () => {
  it("captures every ordered message version and hides on older servers", () => {
    const state = input();
    const view = deriveQueuedSteerBatch(state)!;
    expect(view.label).toBe("Steer all (22)");
    expect(view.reason).toBeNull();
    expect(view.items).toEqual(
      Array.from({ length: 22 }, (_, i) => ({
        queuedRunId: `queued-${i}`,
        messageId: `message-${i}`,
        messageUpdatedAt: "2026-10-20T12:00:00.000Z",
      })),
    );
    expect(deriveQueuedSteerBatch({ ...state, capability: undefined })).toBeNull();
  });

  it.each([
    ["editing", { editing: true }, "editing"],
    ["saving", { hasUnsavedMessages: true }, "saving"],
    ["busy", { busy: true }, "finish"],
  ])("blocks %s", (_name, flags, reason) => {
    expect(deriveQueuedSteerBatch({ ...input(), ...flags })?.reason).toContain(reason);
  });

  it("blocks restart-only, ended turns and pending questions", () => {
    const state = input();
    const session = state.projection.providerSessions[0]!;
    const restartOnly = {
      ...state.projection,
      providerSessions: [
        {
          ...session,
          capabilities: {
            ...session.capabilities,
            turns: { ...session.capabilities.turns, supportsActiveSteering: false },
          },
        },
      ],
    };
    expect(deriveQueuedSteerBatch({ ...state, projection: restartOnly })?.reason).toContain(
      "Codex or Claude",
    );
    expect(
      deriveQueuedSteerBatch({ ...state, projection: { ...state.projection, providerTurns: [] } })
        ?.reason,
    ).toContain("no active");
    expect(
      deriveQueuedSteerBatch({
        ...state,
        projection: {
          ...state.projection,
          runtimeRequests: [{ status: "pending", kind: "user_input" } as never],
        },
      })?.reason,
    ).toContain("approval or question");
  });

  it("labels a bounded prefix and explains oversized first messages", () => {
    const state = input();
    const limited = deriveQueuedSteerBatch({
      ...state,
      capability: { ...state.capability, maxItems: 2 },
    })!;
    expect(limited.label).toBe("Steer next 2");
    expect(limited.items).toHaveLength(2);
    expect(limited.detail).toContain("20 still queued");
    expect(limited.detail).toContain("size limit");
    expect(
      deriveQueuedSteerBatch({ ...state, capability: { ...state.capability, maxInputChars: 1 } })
        ?.reason,
    ).toContain("individual Steer");
  });

  it("stops before special commands, model changes and automatic completions remain queued", () => {
    const state = input();
    const modified = {
      ...state.projection,
      messages: state.projection.messages.map((message, i) =>
        i === 1 ? { ...message, text: "/compact" } : message,
      ),
    };
    expect(deriveQueuedSteerBatch({ ...state, projection: modified })?.items).toHaveLength(1);
    const changedModel = {
      ...state.projection,
      runs: state.projection.runs.map((run, i) =>
        i === 2 ? { ...run, modelSelection: { ...run.modelSelection, model: "different" } } : run,
      ),
    };
    expect(deriveQueuedSteerBatch({ ...state, projection: changedModel })?.items).toHaveLength(1);
    const completion = {
      ...state.projection,
      messages: state.projection.messages.map((message, i) =>
        i === 1 ? { ...message, notification: {} } : message,
      ),
    } as OrchestrationV2ThreadProjection;
    const items = deriveQueuedSteerBatch({ ...state, projection: completion })!.items;
    expect(items).toHaveLength(21);
    expect(items.some((item) => item.messageId === "message-1")).toBe(false);
  });
});

describe("queued batch submission identity", () => {
  it("makes one dispatch for 22 rows, locks concurrent clicks, and waits for projection", async () => {
    const controller = createQueuedSteerBatchSubmission(() => CommandId.make("batch-1"));
    const view = deriveQueuedSteerBatch(input())!;
    const command = {
      threadId: v2Projection.thread.id,
      targetRunId: view.targetRunId!,
      items: view.items,
    };
    let finish!: (result: { _tag: string }) => void;
    const dispatch = vi.fn(
      () =>
        new Promise<{ _tag: string }>((resolve) => {
          finish = resolve;
        }),
    );
    const first = controller.submit(command, dispatch);
    expect(controller.pending).toBe(true);
    expect(await controller.submit(command, dispatch)).toBe("pending");
    expect(dispatch).toHaveBeenCalledTimes(1);
    expect(dispatch.mock.calls[0]).toEqual([{ ...command, commandId: "batch-1" }]);
    finish({ _tag: "Success" });
    expect(await first).toBe("success");
    expect(controller.awaitingProjection(view.items)).toBe(true);
    expect(controller.awaitingProjection([])).toBe(false);
  });

  it("retries an unknown result with identical command and items even if queue changes", async () => {
    const allocate = vi.fn(() => CommandId.make("batch-1"));
    const controller = createQueuedSteerBatchSubmission(allocate);
    const view = deriveQueuedSteerBatch(input())!;
    const command = {
      threadId: v2Projection.thread.id,
      targetRunId: view.targetRunId!,
      items: view.items,
    };
    const dispatch = vi
      .fn()
      .mockRejectedValueOnce(new Error("connection lost"))
      .mockResolvedValueOnce({ _tag: "Success" });
    expect(await controller.submit(command, dispatch)).toBe("failure");
    expect(await controller.submit({ ...command, items: command.items.slice(1) }, dispatch)).toBe(
      "success",
    );
    expect(dispatch.mock.calls[1]![0]).toBe(dispatch.mock.calls[0]![0]);
    expect(allocate).toHaveBeenCalledTimes(1);
    expect(command.items).toHaveLength(22);
  });
});

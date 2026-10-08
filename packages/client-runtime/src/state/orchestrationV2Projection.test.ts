import { describe, expect, it } from "vite-plus/test";
import {
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Run,
  type OrchestrationV2ThreadProjection,
  type OrchestrationV2TurnItem,
  MessageId,
  EventId,
  RunAttemptId,
  ProviderSessionId,
  ProviderDriverKind,
  CheckpointId,
  CheckpointRef,
  CheckpointScopeId,
  ProjectId,
  ProviderInstanceId,
  NodeId,
  ProviderThreadId,
  ProviderTurnId,
  RunId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

import { applyOrchestrationV2ProjectionEvent } from "./orchestrationV2Projection.ts";

const now = DateTime.makeUnsafe("2026-06-20T00:00:00.000Z");
const threadId = ThreadId.make("thread-reducer");
const runId = RunId.make("run-reducer");
const run = {
  id: runId,
  threadId,
  ordinal: 1,
  providerInstanceId: ProviderInstanceId.make("codex"),
  modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" },
  providerThreadId: null,
  userMessageId: MessageId.make("message-reducer"),
  rootNodeId: null,
  activeAttemptId: null,
  status: "completed",
  requestedAt: now,
  startedAt: now,
  completedAt: now,
  checkpointId: null,
  contextHandoffId: null,
} satisfies OrchestrationV2Run;

function commandItem(id: string, output = "done", ordinal = 1): OrchestrationV2TurnItem {
  return {
    id: TurnItemId.make(id),
    threadId,
    runId,
    nodeId: null,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal,
    status: "completed",
    title: null,
    startedAt: now,
    completedAt: now,
    updatedAt: now,
    type: "command_execution",
    input: "pwd",
    output,
    exitCode: 0,
  };
}
const emptyProjection = {
  thread: {
    id: threadId,
    projectId: ProjectId.make("project-reducer"),
    title: "Reducer",
    providerInstanceId: ProviderInstanceId.make("codex"),
    modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { rootThreadId: threadId, parentThreadId: null, relationshipToParent: null },
    forkedFrom: null,
    createdBy: "user",
    creationSource: "web",
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  },
  runs: [],
  attempts: [],
  nodes: [],
  subagents: [],
  providerSessions: [],
  providerThreads: [],
  providerTurns: [],
  runtimeRequests: [],
  messages: [],
  plans: [],
  turnItems: [],
  checkpointScopes: [],
  checkpoints: [],
  contextHandoffs: [],
  contextTransfers: [],
  visibleTurnItems: [],
  updatedAt: now,
} as OrchestrationV2ThreadProjection;

describe("applyOrchestrationV2ProjectionEvent", () => {
  it("keeps live token usage when the terminal provider turn omits it", () => {
    const providerTurnId = ProviderTurnId.make("provider-turn-reducer");
    const running = {
      id: providerTurnId,
      providerThreadId: ProviderThreadId.make("provider-thread-reducer"),
      nodeId: NodeId.make("provider-node-reducer"),
      runAttemptId: null,
      nativeTurnRef: null,
      ordinal: 1,
      status: "running" as const,
      startedAt: now,
      completedAt: null,
      tokenUsage: {
        usedTokens: 50_000,
        maxTokens: 200_000,
        updatedAt: "2026-08-29T00:00:00.000Z",
      },
    };
    const projection = { ...emptyProjection, providerTurns: [running] };
    const event = {
      id: "event-provider-turn-terminal",
      type: "provider-turn.updated",
      threadId,
      driver: "codex",
      occurredAt: now,
      payload: {
        ...running,
        status: "completed",
        completedAt: now,
        tokenUsage: undefined,
      },
    } as OrchestrationV2DomainEvent;

    const next = applyOrchestrationV2ProjectionEvent(projection, event);

    expect(next?.providerTurns[0]?.status).toBe("completed");
    expect(next?.providerTurns[0]?.tokenUsage).toEqual(running.tokenUsage);
  });

  it("applies thread lifecycle payloads instead of leaving stale metadata", () => {
    const archivedAt = DateTime.makeUnsafe("2026-06-20T01:00:00.000Z");
    const event = {
      id: "event-archive",
      type: "thread.archived",
      threadId,
      occurredAt: archivedAt,
      payload: { ...emptyProjection.thread, archivedAt, updatedAt: archivedAt },
    } as OrchestrationV2DomainEvent;

    const next = applyOrchestrationV2ProjectionEvent(emptyProjection, event);
    expect(next?.thread.archivedAt).toEqual(archivedAt);
    expect(next?.updatedAt).toEqual(archivedAt);
  });

  it("ignores events for another thread", () => {
    const event = {
      id: "event-other",
      type: "thread.deleted",
      threadId: ThreadId.make("thread-other"),
      occurredAt: now,
      payload: { ...emptyProjection.thread, id: ThreadId.make("thread-other"), deletedAt: now },
    } as OrchestrationV2DomainEvent;

    expect(applyOrchestrationV2ProjectionEvent(emptyProjection, event)).toBe(emptyProjection);
  });

  it("preserves visible row identity when run updates do not change membership", () => {
    const item = commandItem("item-stable");
    const visibleTurnItems = [
      {
        position: 0,
        visibility: "local" as const,
        sourceThreadId: threadId,
        sourceItemId: item.id,
        item,
      },
    ];
    const projection = {
      ...emptyProjection,
      runs: [run],
      turnItems: [item],
      visibleTurnItems,
    };
    const event = {
      id: "event-run-update",
      type: "run.updated",
      threadId,
      runId,
      occurredAt: now,
      payload: { ...run, status: "completed" },
    } as OrchestrationV2DomainEvent;

    const next = applyOrchestrationV2ProjectionEvent(projection, event);
    expect(next?.visibleTurnItems).toBe(visibleTurnItems);
    expect(next?.visibleTurnItems[0]).toBe(visibleTurnItems[0]);
  });

  it("replaces only the updated visible item when membership is unchanged", () => {
    const first = commandItem("item-first", "first");
    const second = commandItem("item-second", "second");
    const firstRow = {
      position: 0,
      visibility: "local" as const,
      sourceThreadId: threadId,
      sourceItemId: first.id,
      item: first,
    };
    const secondRow = {
      position: 1,
      visibility: "local" as const,
      sourceThreadId: threadId,
      sourceItemId: second.id,
      item: second,
    };
    const updated = commandItem("item-first", "streamed output");
    const projection = {
      ...emptyProjection,
      runs: [run],
      turnItems: [first, second],
      visibleTurnItems: [firstRow, secondRow],
    };
    const event = {
      id: "event-item-update",
      type: "turn-item.updated",
      threadId,
      runId,
      occurredAt: now,
      payload: updated,
    } as OrchestrationV2DomainEvent;

    const next = applyOrchestrationV2ProjectionEvent(projection, event);
    expect(next?.visibleTurnItems).not.toBe(projection.visibleTurnItems);
    expect(next?.visibleTurnItems[0]).not.toBe(firstRow);
    expect(next?.visibleTurnItems[0]?.item).toBe(updated);
    expect(next?.visibleTurnItems[1]).toBe(secondRow);
  });

  it("inserts live turn items by authoritative ordinal", () => {
    const queuedFuture = commandItem("item-queued-future", "queued", 300);
    const activeAssistant = commandItem("item-active-assistant", "done", 201);
    const queuedRow = {
      position: 0,
      visibility: "local" as const,
      sourceThreadId: threadId,
      sourceItemId: queuedFuture.id,
      item: queuedFuture,
    };
    const projection = {
      ...emptyProjection,
      runs: [run],
      turnItems: [queuedFuture],
      visibleTurnItems: [queuedRow],
    };
    const event = {
      id: "event-active-assistant",
      type: "turn-item.updated",
      threadId,
      runId,
      occurredAt: now,
      payload: activeAssistant,
    } as OrchestrationV2DomainEvent;

    const next = applyOrchestrationV2ProjectionEvent(projection, event);
    expect(next?.visibleTurnItems.map((row) => row.item.id)).toEqual([
      activeAssistant.id,
      queuedFuture.id,
    ]);
    expect(next?.visibleTurnItems.map((row) => row.position)).toEqual([0, 1]);
  });

  it("removes only hidden local items while preserving inherited rows", () => {
    const inherited = commandItem("item-inherited");
    const local = commandItem("item-local");
    const inheritedRow = {
      position: 0,
      visibility: "inherited" as const,
      sourceThreadId: ThreadId.make("thread-source"),
      sourceItemId: inherited.id,
      item: inherited,
    };
    const localRow = {
      position: 1,
      visibility: "local" as const,
      sourceThreadId: threadId,
      sourceItemId: local.id,
      item: local,
    };
    const projection = {
      ...emptyProjection,
      runs: [run],
      turnItems: [local],
      visibleTurnItems: [inheritedRow, localRow],
    };
    const event = {
      id: "event-run-rollback",
      type: "run.updated",
      threadId,
      runId,
      occurredAt: now,
      payload: { ...run, status: "rolled_back" },
    } as OrchestrationV2DomainEvent;

    const next = applyOrchestrationV2ProjectionEvent(projection, event);
    expect(next?.visibleTurnItems).toEqual([inheritedRow]);
    expect(next?.visibleTurnItems[0]).toBe(inheritedRow);
  });
});

it("does not scan every row against every run for a streaming item update", () => {
  let runReads = 0;
  const runs = Array.from({ length: 100 }, (_, index) => ({
    ...run,
    get id() {
      runReads++;
      return RunId.make(`run-${index}`);
    },
  }));
  const items = Array.from({ length: 1000 }, (_, index) =>
    commandItem(`item-${index}`, "before", index),
  );
  const projection = {
    ...emptyProjection,
    runs,
    turnItems: items,
    visibleTurnItems: items.map((item, position) => ({
      item,
      position,
      visibility: "local" as const,
      sourceThreadId: threadId,
      sourceItemId: item.id,
    })),
  };
  const payload = commandItem("item-999", "after", 999);
  const next = applyOrchestrationV2ProjectionEvent(projection, {
    id: "stream-update",
    type: "turn-item.updated",
    threadId,
    occurredAt: now,
    payload,
  } as OrchestrationV2DomainEvent);
  expect(next?.visibleTurnItems.at(-1)?.item).toBe(payload);
  expect(next?.visibleTurnItems[0]).toBe(projection.visibleTurnItems[0]);
  expect(runReads).toBeLessThanOrEqual(100);
});

it("replays explicit provider settlement without inferring it from availability, text, intent or checkpoints", () => {
  const attemptId = RunAttemptId.make("attempt:explicit-settlement");
  const nodeId = NodeId.make("node:explicit-settlement");
  const providerThreadId = ProviderThreadId.make("provider-thread:explicit-settlement");
  const providerTurnId = ProviderTurnId.make("provider-turn:explicit-settlement");
  const attempt = {
    id: attemptId,
    runId,
    attemptOrdinal: 1,
    rootNodeId: nodeId,
    providerInstanceId: run.providerInstanceId,
    providerThreadId,
    providerTurnId,
    reason: "initial" as const,
    status: "running" as const,
    startedAt: now,
    completedAt: null,
    providerSettlement: null,
  };
  let projection = {
    ...emptyProjection,
    runs: [{ ...run, status: "running" as const, activeAttemptId: attemptId, completedAt: null }],
    attempts: [attempt],
  } as OrchestrationV2ThreadProjection;
  const apply = (event: OrchestrationV2DomainEvent) => {
    const next = applyOrchestrationV2ProjectionEvent(projection, event);
    expect(next).not.toBeNull();
    projection = next!;
  };
  const base = { id: EventId.make("event:explicit-settlement"), threadId, occurredAt: now };
  for (const status of ["starting", "ready", "stopped", "error"] as const) {
    apply({
      ...base,
      type: "provider-session.updated",
      payload: {
        id: ProviderSessionId.make("session:explicit-settlement"),
        driver: ProviderDriverKind.make("codex"),
        providerInstanceId: run.providerInstanceId,
        status,
        cwd: "/synthetic",
        model: null,
        capabilities: {} as never,
        createdAt: now,
        updatedAt: now,
        lastError: null,
      },
    });
    expect(projection.runs[0]?.status).toBe("running");
    expect(projection.attempts[0]?.providerSettlement).toBeNull();
  }
  apply({
    ...base,
    type: "message.updated",
    payload: {
      id: MessageId.make("assistant:explicit-settlement"),
      threadId,
      runId,
      nodeId,
      role: "assistant",
      createdBy: "agent",
      creationSource: "provider",
      text: "Final text",
      attachments: [],
      streaming: false,
      createdAt: now,
      updatedAt: now,
    },
  });
  const interruptIntent = {
    id: TurnItemId.make("interrupt-intent"),
    threadId,
    runId,
    nodeId: null,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal: 1,
    status: "completed",
    title: null,
    startedAt: now,
    completedAt: now,
    updatedAt: now,
    type: "run_interrupt_request",
    message: "Stopping",
  } satisfies OrchestrationV2TurnItem;
  apply({ ...base, type: "turn-item.updated", payload: interruptIntent });
  for (const status of ["ready", "error", "missing"] as const) {
    apply({
      ...base,
      type: "checkpoint.captured",
      payload: {
        id: CheckpointId.make(`checkpoint:${status}`),
        threadId,
        runId,
        nodeId,
        scopeId: CheckpointScopeId.make("scope:explicit"),
        parentCheckpointId: null,
        ordinalWithinScope: 1,
        appRunOrdinal: 1,
        ref: CheckpointRef.make(`checkpoint-ref:${status}`),
        files: [],
        status,
        capturedAt: now,
      },
    });
  }
  expect(projection.runs[0]?.completedAt).toBeNull();
  expect(projection.attempts[0]?.providerSettlement).toBeNull();
  const completedAt = DateTime.add(now, { seconds: 5 });
  const settlement = {
    runAttemptId: attemptId,
    providerTurnId,
    status: "interrupted" as const,
    completedAt,
  };
  const prior = projection;
  apply({
    ...base,
    type: "run-attempt.updated",
    payload: { ...attempt, providerSettlement: settlement, status: "interrupted", completedAt },
  });
  expect(projection.attempts[0]?.providerSettlement).toEqual(settlement);
  expect(prior.attempts[0]?.providerSettlement).toBeNull();
  apply({
    ...base,
    type: "checkpoint.captured",
    payload: {
      ...projection.checkpoints[0]!,
      capturedAt: DateTime.add(completedAt, { minutes: 1 }),
    },
  });
  expect(projection.attempts[0]?.providerSettlement).toEqual(settlement);
  const withoutEvidence = { ...attempt };
  delete (withoutEvidence as { providerSettlement?: unknown }).providerSettlement;
  apply({ ...base, type: "run-attempt.updated", payload: withoutEvidence });
  expect(Object.hasOwn(projection.attempts[0]!, "providerSettlement")).toBe(false);
  apply({
    ...base,
    type: "run-attempt.updated",
    payload: { ...attempt, providerSettlement: null },
  });
  expect(projection.attempts[0]?.providerSettlement).toBeNull();
});

it("requested runtime stop preserves active attachment and projection until its completion owner acts", () => {
  const activeProviderThreadId = ProviderThreadId.make("provider-thread:captured-stop");
  const projection = {
    ...emptyProjection,
    thread: { ...emptyProjection.thread, activeProviderThreadId },
  };
  const next = applyOrchestrationV2ProjectionEvent(projection, {
    id: EventId.make("event:stop:request"),
    type: "provider-session.detach-requested",
    threadId,
    occurredAt: DateTime.makeUnsafe("2026-10-07T12:00:00Z"),
    payload: { providerSessionId: ProviderSessionId.make("session:captured-stop") },
  });
  expect(next).toBe(projection);
  expect(next?.thread.activeProviderThreadId).toBe(activeProviderThreadId);
});

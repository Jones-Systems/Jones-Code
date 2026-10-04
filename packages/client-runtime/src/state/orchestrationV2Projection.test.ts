import { describe, expect, it } from "vite-plus/test";
import {
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Run,
  type OrchestrationV2ThreadProjection,
  type OrchestrationV2TurnItem,
  CheckpointId,
  CheckpointRef,
  CheckpointScopeId,
  EventId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ProviderDriverKind,
  ProviderSessionId,
  NodeId,
  ProviderThreadId,
  ProviderTurnId,
  RunId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

import { applyOrchestrationV2ProjectionEvent } from "./orchestrationV2Projection.ts";
import { deriveLatestThreadRun, deriveThreadActivityRun } from "./threadExecution.ts";
import { v2ProviderCapabilities } from "./orchestrationV2TestFixtures.ts";

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
  it("preserves the complete projection when detach acceptance is replayed", () => {
    const providerThreadId = ProviderThreadId.make("provider-thread-detach-acceptance");
    const session = {
      id: ProviderSessionId.make("session-detach-acceptance"),
      providerInstanceId: run.providerInstanceId,
      driver: ProviderDriverKind.make("codex"),
      status: "running" as const,
      cwd: "/workspace/project",
      model: run.modelSelection.model,
      capabilities: v2ProviderCapabilities,
      createdAt: now,
      updatedAt: now,
      lastError: null,
    };
    const siblingSession = {
      ...session,
      id: ProviderSessionId.make("session-detach-sibling"),
      status: "ready" as const,
    };
    const item = commandItem("item-detach-acceptance");
    const projection: OrchestrationV2ThreadProjection = {
      ...emptyProjection,
      thread: {
        ...emptyProjection.thread,
        activeProviderThreadId: providerThreadId,
        branch: "retained-branch",
        worktreePath: "/workspace/project",
        settledOverride: "active",
        pinnedAt: now,
        pinOrderKey: "a1",
        activeOrderKey: "a2",
        snoozedAt: now,
        snoozedUntil: DateTime.makeUnsafe("2026-06-21T00:00:00.000Z"),
        autoSettleDisabledAt: now,
        lastVisitedAt: now,
      },
      runs: [{ ...run, providerThreadId, status: "running", completedAt: null }],
      providerSessions: [session, siblingSession],
      providerThreads: [
        {
          id: providerThreadId,
          driver: session.driver,
          providerInstanceId: session.providerInstanceId,
          providerSessionId: session.id,
          appThreadId: threadId,
          ownerNodeId: null,
          nativeThreadRef: null,
          nativeConversationHeadRef: null,
          status: "active",
          firstRunOrdinal: 1,
          lastRunOrdinal: 1,
          handoffIds: [],
          forkedFrom: null,
          createdAt: now,
          updatedAt: now,
        },
      ],
      turnItems: [item],
      visibleTurnItems: [
        {
          position: 0,
          visibility: "local",
          sourceThreadId: threadId,
          sourceItemId: item.id,
          item,
        },
      ],
    };
    const occurredAt = DateTime.makeUnsafe("2026-06-20T01:00:00.000Z");
    const requested = {
      id: EventId.make("event-detach-acceptance"),
      type: "provider-session.detach-requested",
      threadId,
      occurredAt,
      payload: { providerSessionId: session.id, reason: "User requested stop" },
    } satisfies OrchestrationV2DomainEvent;

    const accepted = applyOrchestrationV2ProjectionEvent(projection, requested);
    expect(accepted).toBe(projection);
    expect(accepted).toEqual(projection);
    expect(applyOrchestrationV2ProjectionEvent(accepted, requested)).toBe(projection);
    expect(deriveThreadActivityRun(accepted!)).toEqual(deriveThreadActivityRun(projection));

    const detached = applyOrchestrationV2ProjectionEvent(projection, {
      ...requested,
      id: EventId.make("event-actual-detach"),
      type: "provider-session.detached",
      payload: { providerSessionId: session.id, detachedAt: occurredAt },
    });
    expect(detached).toEqual({
      ...projection,
      providerSessions: [siblingSession],
      updatedAt: occurredAt,
    });
  });

  it("settles a run only from its attributed lifecycle after output, checkpoint, and session readiness", () => {
    let projection: OrchestrationV2ThreadProjection = {
      ...emptyProjection,
      runs: [{ ...run, status: "running", completedAt: null }],
    };
    const assistantMessageId = MessageId.make("assistant-settlement");
    const eventBase = { threadId, occurredAt: now };
    const evidence = [
      {
        ...eventBase,
        id: EventId.make("event-final-output"),
        type: "message.updated",
        payload: {
          createdBy: "system",
          creationSource: "web",
          id: assistantMessageId,
          threadId,
          runId,
          nodeId: null,
          role: "assistant",
          text: "Finished output",
          attachments: [],
          streaming: false,
          createdAt: now,
          updatedAt: now,
        },
      },
      {
        ...eventBase,
        id: EventId.make("event-checkpoint-ready"),
        type: "checkpoint.captured",
        payload: {
          id: CheckpointId.make("checkpoint-settlement"),
          threadId,
          scopeId: CheckpointScopeId.make("scope-settlement"),
          runId,
          nodeId: NodeId.make("node-settlement"),
          parentCheckpointId: null,
          ordinalWithinScope: 1,
          appRunOrdinal: 1,
          ref: CheckpointRef.make("refs/checkpoints/settlement"),
          status: "ready",
          files: [],
          capturedAt: now,
        },
      },
      {
        ...eventBase,
        id: EventId.make("event-session-ready"),
        type: "provider-session.updated",
        payload: {
          id: ProviderSessionId.make("session-settlement"),
          providerInstanceId: run.providerInstanceId,
          driver: ProviderDriverKind.make("codex"),
          status: "ready",
          cwd: "/workspace/project",
          model: run.modelSelection.model,
          capabilities: v2ProviderCapabilities,
          createdAt: now,
          updatedAt: now,
          lastError: null,
        },
      },
      {
        ...eventBase,
        id: EventId.make("event-interrupt-intent"),
        type: "turn-item.updated",
        payload: {
          id: TurnItemId.make("interrupt-settlement"),
          threadId,
          runId,
          nodeId: null,
          providerThreadId: null,
          providerTurnId: null,
          nativeItemRef: null,
          parentItemId: null,
          ordinal: 1,
          title: null,
          startedAt: now,
          updatedAt: now,
          type: "run_interrupt_request",
          status: "pending",
          completedAt: null,
          message: "Interrupt requested",
        },
      },
    ] satisfies ReadonlyArray<OrchestrationV2DomainEvent>;
    for (const event of evidence) {
      projection = applyOrchestrationV2ProjectionEvent(projection, event)!;
      expect(deriveLatestThreadRun(projection)).toMatchObject({
        runId,
        status: "running",
        completedAt: null,
        assistantMessageId,
      });
    }

    const completedAt = DateTime.makeUnsafe("2026-06-20T00:01:00.000Z");
    projection = applyOrchestrationV2ProjectionEvent(projection, {
      id: EventId.make("event-attributed-settlement"),
      threadId,
      occurredAt: completedAt,
      type: "run.updated",
      payload: { ...run, completedAt },
    })!;
    expect(deriveLatestThreadRun(projection)).toMatchObject({
      runId,
      status: "completed",
      completedAt: "2026-06-20T00:01:00.000Z",
      assistantMessageId,
    });
  });

  it("presents settlement received before run creation without inheriting the previous run", () => {
    const prior = { ...run, id: RunId.make("run-prior"), ordinal: 1 };
    const recovered = { ...run, id: RunId.make("run-recovered"), ordinal: 2 };
    const projection = applyOrchestrationV2ProjectionEvent(
      { ...emptyProjection, runs: [prior] },
      {
        id: EventId.make("event-settlement-first"),
        threadId,
        occurredAt: now,
        type: "run.updated",
        payload: recovered,
      },
    )!;
    expect(deriveLatestThreadRun(projection)).toMatchObject({
      runId: recovered.id,
      status: "completed",
      assistantMessageId: null,
      completedAt: "2026-06-20T00:00:00.000Z",
    });
  });

  it.each([null, "running", "interrupted", "failed"] as const)(
    "does not replace a %s run outcome with checkpoint capture or rollback intent",
    (status) => {
      const projection = {
        ...emptyProjection,
        runs:
          status === null
            ? []
            : [{ ...run, status, completedAt: status === "running" ? null : now }],
      };
      const before = deriveLatestThreadRun(projection);
      const checkpointId = CheckpointId.make("checkpoint-outcome");
      const scopeId = CheckpointScopeId.make("scope-outcome");
      const checkpointed = applyOrchestrationV2ProjectionEvent(projection, {
        id: EventId.make("event-checkpoint-outcome"),
        threadId,
        occurredAt: now,
        type: "checkpoint.captured",
        payload: {
          id: checkpointId,
          threadId,
          scopeId,
          runId: status === null ? null : runId,
          nodeId: NodeId.make("node-outcome"),
          parentCheckpointId: null,
          ordinalWithinScope: 1,
          appRunOrdinal: status === null ? null : 1,
          ref: CheckpointRef.make("refs/checkpoints/outcome"),
          status: "ready",
          files: [],
          capturedAt: now,
        },
      })!;
      expect(checkpointed.checkpoints).toHaveLength(1);
      expect(deriveLatestThreadRun(checkpointed)).toEqual(before);
      const requested = applyOrchestrationV2ProjectionEvent(checkpointed, {
        id: EventId.make("event-rollback-intent"),
        threadId,
        occurredAt: now,
        type: "checkpoint.rollback-requested",
        payload: { scopeId, checkpointId, requestedAt: now },
      })!;
      expect(deriveLatestThreadRun(requested)).toEqual(before);
    },
  );

  it("keeps a retained active run unsettled when a later run is rolled back", () => {
    const retained = { ...run, status: "running" as const, completedAt: null };
    const later = { ...run, id: RunId.make("run-rolled-back"), ordinal: 2 };
    const retainedItem = commandItem("item-retained");
    const laterItem = { ...commandItem("item-rolled-back", "later", 2), runId: later.id };
    const projection = {
      ...emptyProjection,
      runs: [retained, later],
      turnItems: [retainedItem, laterItem],
      visibleTurnItems: [retainedItem, laterItem].map((item, position) => ({
        position,
        visibility: "local" as const,
        sourceThreadId: threadId,
        sourceItemId: item.id,
        item,
      })),
    };
    const rolledBack = applyOrchestrationV2ProjectionEvent(projection, {
      id: EventId.make("event-later-run-rolled-back"),
      threadId,
      occurredAt: now,
      type: "run.updated",
      payload: { ...later, status: "rolled_back" },
    })!;
    expect(rolledBack.visibleTurnItems.map((row) => row.sourceItemId)).toEqual([retainedItem.id]);
    expect(deriveThreadActivityRun(rolledBack)).toMatchObject({
      runId: retained.id,
      status: "running",
      completedAt: null,
    });
  });

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

import { describe, expect, it } from "vite-plus/test";
import {
  type ThreadPullRequestLink,
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

function commandItem(
  id: string,
  output = "done",
  ordinal = 1,
): Extract<OrchestrationV2TurnItem, { type: "command_execution" }> {
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
  it("applies a compact PR sync without replacing other links or thread metadata", () => {
    const link = (number: number): ThreadPullRequestLink => ({
      host: "github.com",
      repository: "pingdotgg/t3code",
      number,
      url: `https://github.com/pingdotgg/t3code/pull/${number}`,
      source: "manual",
      linkedAt: DateTime.formatIso(now),
      snapshot: null,
      stack: null,
    });
    const links = [link(1), link(2)];
    const projection = {
      ...emptyProjection,
      thread: { ...emptyProjection.thread, pullRequests: links },
    };
    const updated = { ...links[1]!, source: "agent" as const };
    const next = applyOrchestrationV2ProjectionEvent(projection, {
      id: "compact-sync" as OrchestrationV2DomainEvent["id"],
      type: "thread.pull-request-link-updated",
      threadId,
      occurredAt: now,
      payload: updated,
    });
    expect(next?.thread).toEqual({ ...projection.thread, pullRequests: [links[0], updated] });
    expect(next?.thread.pullRequests?.[0]).toBe(links[0]);
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

  it("moves an updated item to its new ordinal and renumbers the rows it passes", () => {
    const inherited = {
      ...commandItem("item-inherited", "parent", 0),
      threadId: ThreadId.make("parent"),
    };
    const items = [
      commandItem("item-a", "a", 1),
      commandItem("item-b", "b", 2),
      commandItem("item-c", "c", 2),
    ];
    const rows = [
      {
        position: 0,
        visibility: "inherited" as const,
        sourceThreadId: inherited.threadId,
        sourceItemId: inherited.id,
        item: inherited,
      },
      ...items.map((item, index) => ({
        position: index + 1,
        visibility: "local" as const,
        sourceThreadId: threadId,
        sourceItemId: item.id,
        item,
      })),
    ];
    const moved = commandItem("item-a", "moved", 2);
    const next = applyOrchestrationV2ProjectionEvent(
      { ...emptyProjection, runs: [run], turnItems: items, visibleTurnItems: rows },
      {
        id: "event-move",
        type: "turn-item.updated",
        threadId,
        runId,
        occurredAt: now,
        payload: moved,
      } as OrchestrationV2DomainEvent,
    );

    // Equal ordinals order by id, so item-a stays ahead of item-b.
    expect(next?.visibleTurnItems.map((row) => [row.item.id, row.position])).toEqual([
      ["item-inherited", 0],
      ["item-a", 1],
      ["item-b", 2],
      ["item-c", 3],
    ]);
    expect(next?.visibleTurnItems[0]).toBe(rows[0]);
    expect(next?.visibleTurnItems[1]?.item).toBe(moved);
    expect(next?.turnItems.map((item) => item.id)).toEqual(["item-a", "item-b", "item-c"]);

    const later = commandItem("item-a", "later", 3);
    const last = applyOrchestrationV2ProjectionEvent(next, {
      id: "event-move-later",
      type: "turn-item.updated",
      threadId,
      runId,
      occurredAt: now,
      payload: later,
    } as OrchestrationV2DomainEvent);
    expect(last?.visibleTurnItems.map((row) => [row.item.id, row.position])).toEqual([
      ["item-inherited", 0],
      ["item-b", 1],
      ["item-c", 2],
      ["item-a", 3],
    ]);
    expect(last?.turnItems.at(0)).toBe(later);
  });

  describe("partial timeline", () => {
    const queuedRunId = RunId.make("run-queued");
    const steerRunId = RunId.make("run-steer");
    const queuedRun = { ...run, id: queuedRunId, ordinal: 2, status: "queued" } as const;
    const steerRun = { ...run, id: steerRunId, ordinal: 3, status: "completed" } as const;
    const runItem = (id: string, itemRunId: RunId, ordinal: number) => ({
      ...commandItem(id, id, ordinal),
      runId: itemRunId,
    });
    const windowOf = (
      runs: ReadonlyArray<OrchestrationV2Run>,
      items: ReadonlyArray<OrchestrationV2TurnItem>,
    ): OrchestrationV2ThreadProjection => ({
      ...emptyProjection,
      runs,
      turnItems: items,
      visibleTurnItems: items.map((item, position) => ({
        position,
        visibility: "local" as const,
        sourceThreadId: threadId,
        sourceItemId: item.id,
        item,
      })),
    });
    const itemEvent = (payload: OrchestrationV2TurnItem) =>
      ({
        id: `event-${payload.id}`,
        type: "turn-item.updated",
        threadId,
        occurredAt: now,
        payload,
      }) as OrchestrationV2DomainEvent;
    const runEvent = (payload: OrchestrationV2Run) =>
      ({
        id: `event-${payload.id}-${payload.status}`,
        type: "run.updated",
        threadId,
        occurredAt: now,
        payload,
      }) as OrchestrationV2DomainEvent;

    it("shows a queued run that starts after a newer steer raised the watermark", () => {
      const steerItems = [
        runItem("item-steer-user", steerRunId, 3_000_001),
        runItem("item-steer-reply", steerRunId, 3_000_002),
      ];
      let projection: OrchestrationV2ThreadProjection | null = windowOf(
        [run, queuedRun, steerRun],
        steerItems,
      );
      let latestLocalTurnOrdinal = 3_000_002;
      const queuedUser = runItem("item-queued-user", queuedRunId, 2_000_001);
      const queuedReply = runItem("item-queued-reply", queuedRunId, 2_000_002);
      for (const event of [
        itemEvent(queuedUser),
        runEvent({ ...queuedRun, status: "running" }),
        itemEvent(queuedReply),
        runEvent({ ...queuedRun, status: "completed" }),
      ]) {
        projection = applyOrchestrationV2ProjectionEvent(projection, event, {
          partialTimeline: true,
          latestLocalTurnOrdinal,
        });
        if (event.type === "turn-item.updated") {
          latestLocalTurnOrdinal = Math.max(latestLocalTurnOrdinal, event.payload.ordinal);
        }
      }

      expect(projection?.visibleTurnItems.map((row) => row.item.id)).toEqual([
        queuedUser.id,
        queuedReply.id,
        ...steerItems.map((item) => item.id),
      ]);
      expect(projection?.turnItems.map((item) => item.id)).toContain(queuedReply.id);
    });

    it("keeps a running run's item when the window starts after its earlier items", () => {
      const runningRun = { ...queuedRun, status: "running" } as const;
      const steerItems = [
        runItem("item-steer-user", steerRunId, 3_000_001),
        runItem("item-steer-reply", steerRunId, 3_000_002),
      ];
      const projection = windowOf([run, runningRun, steerRun], steerItems);
      const reply = runItem("item-running-reply", queuedRunId, 2_000_003);

      const next = applyOrchestrationV2ProjectionEvent(projection, itemEvent(reply), {
        partialTimeline: true,
        latestLocalTurnOrdinal: 3_000_002,
      });

      expect(next?.visibleTurnItems.map((row) => row.item.id)).toEqual([
        reply.id,
        ...steerItems.map((item) => item.id),
      ]);
    });

    it.each([
      { name: "keeps an item of a run already in the window", itemRunId: steerRunId, kept: true },
      { name: "drops an item of a finished run outside the window", itemRunId: runId, kept: false },
    ])("$name that arrives below the watermark", ({ itemRunId, kept }) => {
      const first = runItem("item-first", steerRunId, 3_000_001);
      const later = runItem("item-later", steerRunId, 3_000_003);
      const late = runItem("item-late", itemRunId, 3_000_002);
      const projection = windowOf([run, steerRun], [first, later]);

      const next = applyOrchestrationV2ProjectionEvent(projection, itemEvent(late), {
        partialTimeline: true,
        latestLocalTurnOrdinal: later.ordinal,
      });
      if (kept) {
        expect(next?.visibleTurnItems.map((row) => row.item.id)).toEqual([
          first.id,
          late.id,
          later.id,
        ]);
      } else {
        expect(next).toBe(projection);
      }
    });
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

describe("stable local item replacement", () => {
  type Rows = OrchestrationV2ThreadProjection["visibleTurnItems"];
  const rowOf = (item: OrchestrationV2TurnItem, position: number): Rows[number] => ({
    item,
    position,
    visibility: "local",
    sourceThreadId: item.threadId,
    sourceItemId: item.id,
  });
  const eventOf = (payload: OrchestrationV2TurnItem): OrchestrationV2DomainEvent => ({
    id: EventId.make(`replacement-${payload.id}`),
    type: "turn-item.updated",
    threadId,
    occurredAt: now,
    payload,
  });
  const oldVisibleUpsert = (rows: Rows, item: OrchestrationV2TurnItem): Rows => {
    const index = rows.findIndex((row) => row.sourceItemId === item.id);
    const previous = rows[index];
    if (
      previous?.visibility === "local" &&
      previous.sourceThreadId === item.threadId &&
      previous.item === item
    ) {
      return rows;
    }
    const updated = index === -1 ? [...rows] : [...rows.slice(0, index), ...rows.slice(index + 1)];
    const insertionIndex = updated.findIndex(
      (row) =>
        row.visibility === "local" &&
        (row.item.ordinal > item.ordinal ||
          (row.item.ordinal === item.ordinal &&
            String(row.item.id).localeCompare(String(item.id)) > 0)),
    );
    updated.splice(insertionIndex === -1 ? updated.length : insertionIndex, 0, rowOf(item, 0));
    return updated.map((row, position) => (row.position === position ? row : { ...row, position }));
  };

  it("matches the old structural reducer for seeded replacements, ties, moves and insertions", () => {
    let seed = 0xa2c11e17;
    const random = (limit: number) => {
      seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0;
      return Math.floor((seed / 0x1_0000_0000) * limit);
    };
    const inherited = {
      ...rowOf(commandItem("inherited", "history", 500), 0),
      visibility: "inherited" as const,
      sourceThreadId: ThreadId.make("source-thread"),
    };
    let reference: Rows = [inherited];
    let projection: OrchestrationV2ThreadProjection = {
      ...emptyProjection,
      runs: [run],
      visibleTurnItems: reference,
    };
    for (let step = 0; step < 240; step += 1) {
      const id = TurnItemId.make(`seeded-${random(24)}`);
      const previous = projection.turnItems.find((item) => item.id === id);
      const item =
        previous !== undefined && random(4) !== 0
          ? { ...previous, title: `revision-${step}` }
          : commandItem(id, `revision-${step}`, random(16));
      reference = oldVisibleUpsert(reference, item);
      const before = projection;
      projection = applyOrchestrationV2ProjectionEvent(projection, eventOf(item))!;
      expect(projection.visibleTurnItems).toEqual(reference);
      expect(projection.visibleTurnItems.map((row) => row.position)).toEqual(
        projection.visibleTurnItems.map((_, index) => index),
      );
      expect(projection.visibleTurnItems[0]).toBe(inherited);
      for (const row of projection.visibleTurnItems) {
        const unchanged = before.visibleTurnItems[row.position];
        if (unchanged?.sourceItemId === row.sourceItemId && row.sourceItemId !== item.id) {
          expect(row).toBe(unchanged);
        }
      }
      expect(applyOrchestrationV2ProjectionEvent(projection, eventOf(item))?.visibleTurnItems).toBe(
        projection.visibleTurnItems,
      );
    }
  });

  it.each([
    "inherited",
    "synthetic",
    "inherited target",
    "source",
    "identity",
    "position",
    "following tie",
    "interrupt",
    "previous interrupt",
  ] as const)("preserves structural fallback for %s rows", (kind) => {
    const first = commandItem("first", "before", 1);
    const target =
      kind === "interrupt" || kind === "previous interrupt"
        ? {
            ...commandItem("target", "before", 2),
            type: "run_interrupt_request" as const,
            message: "Stop",
          }
        : commandItem("target", "before", 2);
    const last = commandItem("last", "before", kind === "following tie" ? 2 : 3);
    const rows = [rowOf(first, 0), rowOf(target, 1), rowOf(last, 2)];
    if (kind === "inherited" || kind === "synthetic") {
      rows[2] = { ...rows[2]!, visibility: kind };
    }
    if (kind === "inherited target") rows[1] = { ...rows[1]!, visibility: "inherited" };
    if (kind === "source") rows[1] = { ...rows[1]!, sourceThreadId: ThreadId.make("other") };
    if (kind === "identity") {
      rows[1] = { ...rows[1]!, item: commandItem("stale-identity", "before", 2) };
    }
    if (kind === "position") rows[1] = { ...rows[1]!, position: 7 };
    const item = {
      ...(kind === "previous interrupt" ? commandItem("target", "before", 2) : target),
      title: "updated",
    };
    const projection = {
      ...emptyProjection,
      runs: [run],
      turnItems: [first, target, last],
      visibleTurnItems: rows,
    };
    const next = applyOrchestrationV2ProjectionEvent(projection, eventOf(item));
    expect(next?.visibleTurnItems).toEqual(oldVisibleUpsert(rows, item));
  });

  it.each([
    { visibility: "inherited", ordinal: 1 },
    { visibility: "inherited", ordinal: 2 },
    { visibility: "synthetic", ordinal: 1 },
    { visibility: "synthetic", ordinal: 2 },
  ] as const)(
    "checks the nearest earlier local ordinal $ordinal across $visibility rows",
    ({ visibility, ordinal }) => {
      const first = commandItem("z-first", "before", ordinal);
      const inherited = commandItem("inherited", "before", 100);
      const target = commandItem("a-target", "before", 2);
      const last = commandItem("last", "before", 3);
      const rows: Rows = [
        rowOf(first, 0),
        { ...rowOf(inherited, 1), visibility, sourceThreadId: ThreadId.make("source") },
        rowOf(target, 2),
        rowOf(last, 3),
      ];
      const item = { ...target, title: "updated" };
      const projection = {
        ...emptyProjection,
        runs: [run],
        turnItems: [first, target, last],
        visibleTurnItems: rows,
      };
      expect(
        applyOrchestrationV2ProjectionEvent(projection, eventOf(item))?.visibleTurnItems,
      ).toEqual(oldVisibleUpsert(rows, item));
    },
  );

  it("bounds ordinal and position inspection for a tail replacement while preserving untouched rows", () => {
    let identityReads = 0;
    let positionReads = 0;
    let ordinalReads = 0;
    const items = Array.from({ length: 1000 }, (_, index) => ({
      ...commandItem(`tail-${index}`, "before", index),
      get ordinal() {
        ordinalReads += 1;
        return index;
      },
    }));
    const rows = items.map((item, index) => ({
      ...rowOf(item, index),
      get sourceItemId() {
        identityReads += 1;
        return item.id;
      },
      get position() {
        positionReads += 1;
        return index;
      },
    }));
    const projection = {
      ...emptyProjection,
      runs: [run],
      turnItems: items,
      visibleTurnItems: rows,
    };
    const item = commandItem("tail-999", "after", 999);
    const next = applyOrchestrationV2ProjectionEvent(projection, eventOf(item))!;
    expect(identityReads).toBeLessThanOrEqual(4);
    expect(positionReads).toBeLessThanOrEqual(1);
    expect(ordinalReads).toBeLessThanOrEqual(2);
    expect(next.visibleTurnItems).not.toBe(rows);
    expect(next.visibleTurnItems[999]?.item).toBe(item);
    for (let index = 0; index < 999; index += 1) {
      expect(next.visibleTurnItems[index]).toBe(rows[index]);
    }
    expect(rows[999]?.item).toBe(items[999]);
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

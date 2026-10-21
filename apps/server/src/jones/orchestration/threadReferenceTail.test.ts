import { assert, it } from "@effect/vitest";
import {
  EventId,
  ThreadId,
  RunId,
  RunAttemptId,
  ProviderInstanceId,
  ProviderThreadId,
  NodeId,
  OrchestrationV2AppThread,
  OrchestrationV2TurnItem,
  OrchestrationV2Run,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Tracer from "effect/Tracer";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";

const database = SqlitePersistence.layerMemory;
const testLayer = Layer.merge(ProjectionStore.layer, EventStore.layer).pipe(
  Layer.provide(database),
);
const now = DateTime.makeUnsafe("2026-10-21T00:00:00Z");
const decodeThread = Schema.decodeUnknownSync(OrchestrationV2AppThread);
const decodeItem = Schema.decodeUnknownSync(OrchestrationV2TurnItem);
const decodeRun = Schema.decodeUnknownSync(OrchestrationV2Run);
const source = ThreadId.make("tail-source");
const runId = RunId.make("tail-run");
const seed = Effect.gen(function* () {
  const store = yield* ProjectionStore.ProjectionStoreV2;
  const events = yield* EventStore.EventStoreV2;
  const app = decodeThread({
    id: source,
    createdBy: "user",
    creationSource: "web",
    projectId: "project",
    title: "Synthetic tail",
    providerInstanceId: "codex",
    modelSelection: { instanceId: "codex", model: "synthetic" },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: source },
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  });
  const apply = (event: OrchestrationV2DomainEvent) =>
    events.append({ events: [event] }).pipe(Effect.andThen(store.apply(event)));
  yield* apply({
    id: EventId.make("tail-create"),
    threadId: source,
    type: "thread.created",
    occurredAt: now,
    payload: app,
  });
  yield* apply({
    id: EventId.make("tail-run-create"),
    threadId: source,
    type: "run.created",
    occurredAt: now,
    runId,
    nodeId: NodeId.make("tail-node"),
    payload: decodeRun({
      id: runId,
      threadId: source,
      ordinal: 1,
      providerInstanceId: "codex",
      modelSelection: { instanceId: "codex", model: "synthetic" },
      providerThreadId: null,
      userMessageId: "message-0",
      rootNodeId: "tail-node",
      activeAttemptId: null,
      status: "completed",
      requestedAt: now,
      startedAt: now,
      completedAt: now,
      checkpointId: null,
      contextHandoffId: null,
    }),
  });
  for (let index = 0; index < 10; index++) {
    const item = decodeItem({
      id: `tail-item-${index}`,
      threadId: source,
      runId,
      nodeId: "tail-node",
      providerThreadId: null,
      providerTurnId: null,
      nativeItemRef: null,
      parentItemId: null,
      ordinal: index,
      status: "completed",
      title: null,
      startedAt: now,
      completedAt: now,
      updatedAt: now,
      ...(index === 0
        ? {
            type: "user_message",
            createdBy: "user",
            creationSource: "web",
            messageId: "message-0",
            inputIntent: "turn_start",
            text: "Synthetic input",
            attachments: [],
          }
        : {
            type: "command_execution",
            input: `command-${index}`,
            output: `canonical-output-${index}`,
          }),
    });
    yield* apply({
      id: EventId.make(`tail-event-${index}`),
      type: "turn-item.updated",
      threadId: source,
      runId,
      occurredAt: now,
      payload: item,
    });
  }
  return { store, apply, app };
});

it.effect("tail selects last N canonical payloads while default remains first N", () =>
  Effect.gen(function* () {
    const { store } = yield* seed;
    const statements: string[] = [];
    const tracer = Tracer.make({
      span(options) {
        const span = new Tracer.NativeSpan(options);
        const end = span.end.bind(span);
        span.end = (time, exit) => {
          const query = span.attributes.get("db.query.text");
          if (typeof query === "string") statements.push(query);
          end(time, exit);
        };
        return span;
      },
    });
    const tail = yield* store
      .getTimelinePage(source, { view: "activity", tail: true, limit: 2 })
      .pipe(Effect.withTracer(tracer));
    assert.deepEqual(
      tail.items.map((row) => row.sourceItemId),
      ["tail-item-8", "tail-item-9"],
    );
    assert.equal(tail.hasMore, true);
    assert.equal(tail.totalItems, 10);
    assert.equal(tail.snapshotSequence, 12);
    assert.equal(
      tail.items[1]?.item.type === "command_execution" ? tail.items[1].item.output : undefined,
      "canonical-output-9",
    );
    assert.equal(
      statements.filter((sql) =>
        /SELECT payload_json FROM orchestration_v2_projection_turn_items/.test(sql),
      ).length,
      2,
    );
    const first = yield* store.getTimelinePage(source, { view: "activity", limit: 2 });
    assert.deepEqual(
      first.items.map((row) => row.sourceItemId),
      ["tail-item-0", "tail-item-1"],
    );
    assert.equal(first.snapshotSequence, undefined);
  }).pipe(Effect.provide(testLayer)),
);

it.effect("tail preserves fork inheritance and uses the source payload", () =>
  Effect.gen(function* () {
    const { store, apply, app } = yield* seed;
    const child = ThreadId.make("tail-child");
    yield* apply({
      id: EventId.make("tail-fork-create"),
      type: "thread.created",
      threadId: child,
      occurredAt: now,
      payload: {
        ...app,
        id: child,
        lineage: { parentThreadId: source, relationshipToParent: "fork", rootThreadId: source },
        forkedFrom: { type: "run", threadId: source, runId },
      },
    });
    const tail = yield* store.getTimelinePage(child, { view: "activity", tail: true, limit: 3 });
    assert.equal(tail.hasMore, true);
    const inherited = tail.items.filter((row) => row.visibility === "inherited");
    assert.isTrue(inherited.length > 0);
    assert.isTrue(inherited.every((row) => row.sourceThreadId === source));
    assert.isTrue(
      inherited.some(
        (row) => row.item.type === "command_execution" && row.item.output === "canonical-output-9",
      ),
    );
  }).pipe(Effect.provide(testLayer)),
);

it.effect("tail excludes superseded interrupt activity", () =>
  Effect.gen(function* () {
    const { store, apply } = yield* seed;
    const nodeId = NodeId.make("superseded-node");
    yield* apply({
      id: EventId.make("superseded-attempt"),
      type: "run-attempt.created",
      threadId: source,
      runId,
      occurredAt: now,
      payload: {
        id: RunAttemptId.make("superseded-attempt"),
        runId,
        attemptOrdinal: 1,
        rootNodeId: nodeId,
        providerInstanceId: ProviderInstanceId.make("codex"),
        providerThreadId: ProviderThreadId.make("synthetic-provider"),
        providerTurnId: null,
        reason: "initial",
        status: "superseded",
        startedAt: now,
        completedAt: now,
      },
    });
    yield* apply({
      id: EventId.make("superseded-interrupt"),
      type: "turn-item.updated",
      threadId: source,
      runId,
      occurredAt: now,
      payload: decodeItem({
        id: "hidden-interrupt",
        type: "run_interrupt_result",
        message: "old interruption",
        threadId: source,
        runId,
        nodeId,
        providerThreadId: null,
        providerTurnId: null,
        nativeItemRef: null,
        parentItemId: null,
        ordinal: 11,
        status: "completed",
        title: null,
        startedAt: now,
        completedAt: now,
        updatedAt: now,
      }),
    });
    const page = yield* store.getTimelinePage(source, { view: "activity", tail: true, limit: 2 });
    assert.deepEqual(
      page.items.map((row) => row.sourceItemId),
      ["tail-item-8", "tail-item-9"],
    );
    const empty = yield* store.getTimelinePage(source, { view: "activity", tail: true, limit: 0 });
    assert.equal(empty.items.length, 0);
  }).pipe(Effect.provide(testLayer)),
);

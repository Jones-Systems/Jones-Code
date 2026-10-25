import { assert, it } from "@effect/vitest";
import {
  EventId,
  ThreadId,
  TurnItemId,
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
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Deferred from "effect/Deferred";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as SqlClient from "effect/sql/SqlClient";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Tracer from "effect/Tracer";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as ThreadManagement from "../../orchestration-v2/ThreadManagementService.ts";
import { encodeThreadHistoryCursor } from "../../orchestration-v2/threadHistoryPaging.ts";
import * as Excerpts from "./threadReferenceExcerpt.ts";

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
const seedItems = (itemCount: number) =>
  Effect.gen(function* () {
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
    for (let index = 0; index < itemCount; index++) {
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

const seed = seedItems(10);
const tracePayloadStatements = () => {
  const statements: string[] = [];
  const metadataStatements: string[] = [];
  const transactions: string[] = [];
  const tracer = Tracer.make({
    span(options) {
      const span = new Tracer.NativeSpan(options);
      const end = span.end.bind(span);
      span.end = (time, exit) => {
        const query = span.attributes.get("db.query.text");
        if (span.name === "sql.transaction" && span.attributes.get("db.connection") !== undefined) {
          transactions.push(span.name);
        }
        if (typeof query === "string" && /SELECT turn_item_id, run_id, node_id, type/.test(query)) {
          metadataStatements.push(query);
        }
        if (
          typeof query === "string" &&
          /SELECT\s+(?:item\.thread_id,\s*item\.turn_item_id,\s*item\.)?payload_json\s+FROM\s+orchestration_v2_projection_turn_items\b/.test(
            query,
          )
        ) {
          statements.push(query);
        }
        end(time, exit);
      };
      return span;
    },
  });
  return { statements, metadataStatements, transactions, tracer };
};

it.effect.each([0, 1, 2, 64, 100])(
  "timeline batches %i real payloads into one statement when nonempty",
  (count) =>
    Effect.gen(function* () {
      const { store } = yield* seedItems(count);
      for (const tail of [false, true]) {
        const { statements, tracer } = tracePayloadStatements();
        const page = yield* store
          .getTimelinePage(source, { view: "activity", tail, limit: 100 })
          .pipe(Effect.withTracer(tracer));
        assert.equal(statements.length, count === 0 ? 0 : 1);
        assert.equal(page.totalItems, count);
        assert.isFalse(page.hasMore);
        assert.deepEqual(
          page.items.map((row) => [
            row.position,
            row.sourceThreadId,
            row.sourceItemId,
            row.visibility,
          ]),
          Array.from({ length: count }, (_, index) => [
            index,
            source,
            `tail-item-${index}`,
            "local",
          ]),
        );
        for (const [index, row] of page.items.entries()) {
          if (index === 0) {
            assert.equal(row.item.type, "user_message");
            assert.equal(
              row.item.type === "user_message" ? row.item.text : undefined,
              "Synthetic input",
            );
          } else {
            assert.equal(row.item.type, "command_execution");
            assert.equal(
              row.item.type === "command_execution" ? row.item.output : undefined,
              `canonical-output-${index}`,
            );
          }
        }
      }
    }).pipe(Effect.provide(testLayer)),
);

it.effect("history batch keeps wire projection and skips empty payload selections", () =>
  Effect.gen(function* () {
    const { store } = yield* seed;
    const cursor = (position: number) =>
      encodeThreadHistoryCursor({
        snapshotSequence: 12,
        sourceThreadId: source,
        sourceItemId: `tail-item-${position}`,
        position,
      });
    const { statements, tracer } = tracePayloadStatements();
    const history = yield* store
      .getThreadHistoryPage(source, cursor(9))
      .pipe(Effect.withTracer(tracer));
    assert.equal(statements.length, 1);
    assert.equal(history.snapshotSequence, 12);
    assert.isFalse(history.hasMoreHistory);
    assert.equal(history.nextCursor, null);
    assert.deepEqual(
      history.items.map((row) => row.sourceItemId),
      Array.from({ length: 9 }, (_, index) => `tail-item-${index}`),
    );
    for (const row of history.items.slice(1)) {
      assert.equal(row.item.type, "command_execution");
      if (row.item.type === "command_execution") {
        assert.equal(row.item.output, undefined);
        assert.equal(row.item.outputOmitted, true);
      }
    }
    const conversation = yield* store.getThreadHistoryPage(source, cursor(9), undefined, true);
    assert.deepEqual(
      conversation.items.map((row) => row.sourceItemId),
      ["tail-item-0"],
    );
    const emptyTrace = tracePayloadStatements();
    const empty = yield* store
      .getThreadHistoryPage(source, cursor(0))
      .pipe(Effect.withTracer(emptyTrace.tracer));
    assert.equal(emptyTrace.statements.length, 0);
    assert.deepEqual(empty.items, []);
    assert.isFalse(empty.hasMoreHistory);
    assert.equal(empty.nextCursor, null);
  }).pipe(Effect.provide(testLayer)),
);

it.effect("tail selects last N canonical payloads while default remains first N", () =>
  Effect.gen(function* () {
    const { store } = yield* seed;
    const { statements, tracer } = tracePayloadStatements();
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
    assert.equal(statements.length, 1);
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
    const { statements, tracer } = tracePayloadStatements();
    const tail = yield* store
      .getTimelinePage(child, { view: "activity", tail: true, limit: 3 })
      .pipe(Effect.withTracer(tracer));
    assert.equal(statements.length, 1);
    assert.deepEqual(
      tail.items.map((row) => [row.position, row.visibility, row.item.type]),
      [
        [8, "inherited", "command_execution"],
        [9, "inherited", "command_execution"],
        [10, "synthetic", "fork"],
      ],
    );
    assert.equal(tail.hasMore, true);
    const inherited = tail.items.filter((row) => row.visibility === "inherited");
    assert.isTrue(inherited.length > 0);
    assert.isTrue(inherited.every((row) => row.sourceThreadId === source));
    assert.isTrue(
      inherited.some(
        (row) => row.item.type === "command_execution" && row.item.output === "canonical-output-9",
      ),
    );
    const syntheticTrace = tracePayloadStatements();
    const synthetic = yield* store
      .getTimelinePage(child, { view: "activity", tail: true, limit: 1 })
      .pipe(Effect.withTracer(syntheticTrace.tracer));
    assert.equal(syntheticTrace.statements.length, 0);
    assert.deepEqual(synthetic.items, tail.items.slice(-1));
    assert.equal(synthetic.totalItems, 11);
    assert.isTrue(synthetic.hasMore);
    const emptyTrace = tracePayloadStatements();
    const empty = yield* store
      .getTimelinePage(child, { view: "activity", afterPosition: 10, limit: 100 })
      .pipe(Effect.withTracer(emptyTrace.tracer));
    assert.equal(emptyTrace.statements.length, 0);
    assert.deepEqual(empty.items, []);
    assert.equal(empty.totalItems, 11);
    assert.isFalse(empty.hasMore);
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

it.effect("excerpt capture shares one SQL index, payload union and transaction", () =>
  Effect.gen(function* () {
    const { store } = yield* seed;
    const timeline = {
      getTimelinePage: (
        threadId: ThreadId,
        options: ProjectionStore.ProjectionTimelinePageOptions,
      ) =>
        store
          .getTimelinePage(threadId, options)
          .pipe(
            Effect.mapError(
              (cause) => new Orchestrator.OrchestratorProjectionError({ threadId, cause }),
            ),
          ),
      getTimelinePages: (
        threadId: ThreadId,
        selections: ReadonlyArray<ProjectionStore.ProjectionTimelinePageOptions>,
      ) =>
        store
          .getTimelinePages(threadId, selections)
          .pipe(
            Effect.mapError(
              (cause) => new Orchestrator.OrchestratorProjectionError({ threadId, cause }),
            ),
          ),
    };
    const trace = tracePayloadStatements();
    const result = yield* Effect.gen(function* () {
      const excerpts = yield* Excerpts.ThreadReferenceExcerpts;
      return yield* excerpts.capture({ threadId: source });
    }).pipe(
      Effect.provide(
        Excerpts.layer.pipe(
          Layer.provide(Layer.mock(ThreadManagement.ThreadManagementService)(timeline)),
        ),
      ),
      Effect.withTracer(trace.tracer),
    );
    assert.equal(result.excerpt.status, "captured");
    if (result.excerpt.status !== "captured") return;
    assert.include(result.excerpt.text, "Synthetic input");
    assert.include(result.excerpt.text, "canonical-output-9");
    assert.equal(result.excerpt.sourceSnapshotSequence, 12);
    assert.equal(trace.metadataStatements.length, 1);
    assert.equal(trace.statements.length, 1);
    assert.equal(trace.transactions.length, 1);
  }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "coherent timeline pages share one index and union payload read with identical page shapes",
  () =>
    Effect.gen(function* () {
      const { store } = yield* seed;
      const selections: ReadonlyArray<ProjectionStore.ProjectionTimelinePageOptions> = [
        { view: "messages", tail: true, limit: 64 },
        { view: "activity", tail: true, limit: 60 },
        { view: "activity", limit: 2 },
        { view: "activity", itemId: TurnItemId.make("tail-item-8"), limit: 1 },
        { view: "activity", afterPosition: 8, limit: 2 },
        { view: "activity", tail: true, limit: 0 },
      ];
      const expected = yield* Effect.forEach(selections, (options) =>
        store.getTimelinePage(source, options),
      );
      const trace = tracePayloadStatements();
      const actual = yield* store
        .getTimelinePages(source, selections)
        .pipe(Effect.withTracer(trace.tracer));
      assert.deepEqual(actual, expected);
      assert.equal(trace.statements.length, 1);
      assert.equal(trace.metadataStatements.length, 1);
      assert.equal(trace.transactions.length, 1);
      assert.equal(actual[2]?.snapshotSequence, undefined);
    }).pipe(Effect.provide(testLayer)),
);

it.effect.each(["missing", "deleted"] as const)("coherent timeline rejects %s source", (state) =>
  Effect.gen(function* () {
    const { store, apply, app } = yield* seed;
    if (state === "deleted")
      yield* apply({
        id: EventId.make("coherent-delete"),
        type: "thread.deleted",
        threadId: source,
        occurredAt: now,
        payload: { ...app, deletedAt: now },
      });
    const error = yield* Effect.flip(
      store.getTimelinePages(state === "missing" ? ThreadId.make("missing") : source, [
        { view: "messages", tail: true, limit: 64 },
        { view: "activity", tail: true, limit: 60 },
      ]),
    );
    assert.instanceOf(error, ProjectionStore.ProjectionStoreThreadNotFoundError);
  }).pipe(Effect.provide(testLayer)),
);

it.effect.each(["unrelated", "source", "ancestor", "cancellation"] as const)(
  "coherent timeline retains global snapshot across gated %s commit",
  (change) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "jones-coherent-reference-" });
      const persistence = SqlitePersistence.layerFromPath(
        path.join(directory, "userdata", "state.sqlite"),
      );
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        let gated = false;
        let transactions = 0;
        const withTransaction: typeof sql.withTransaction = (effect) =>
          sql.withTransaction(
            Effect.gen(function* () {
              if (gated) {
                transactions++;
                yield* sql`SELECT MAX(sequence) FROM orchestration_events`;
                yield* Deferred.succeed(entered, undefined);
                yield* Deferred.await(release);
              }
              return yield* effect;
            }),
          );
        const observedSql = new Proxy(sql, {
          get(target, property, receiver) {
            return property === "withTransaction"
              ? withTransaction
              : Reflect.get(target, property, receiver);
          },
        });
        const stores = Layer.merge(ProjectionStore.layer, EventStore.layer).pipe(
          Layer.provide(Layer.succeed(SqlClient.SqlClient, observedSql)),
        );
        yield* Effect.gen(function* () {
          const { store, apply, app } = yield* seed;
          const child = ThreadId.make("coherent-child");
          if (change === "ancestor")
            yield* apply({
              id: EventId.make("coherent-fork"),
              type: "thread.created",
              threadId: child,
              occurredAt: now,
              payload: {
                ...app,
                id: child,
                lineage: {
                  parentThreadId: source,
                  relationshipToParent: "fork",
                  rootThreadId: source,
                },
                forkedFrom: { type: "run", threadId: source, runId },
              },
            });
          const target = change === "ancestor" ? child : source;
          const selections = [
            { view: "messages" as const, tail: true, limit: 64 },
            { view: "activity" as const, tail: true, limit: 60 },
          ];
          const expected = yield* store
            .getTimelinePages(target, selections)
            .pipe(NodeSqliteClient.readOnly);
          const original = expected[1]!.items.find(
            (row) => row.sourceItemId === "tail-item-9",
          )!.item;
          gated = true;
          const pending = yield* Effect.forkChild(
            store.getTimelinePages(target, selections).pipe(NodeSqliteClient.readOnly),
          );
          yield* Deferred.await(entered);
          gated = false;
          if (change === "cancellation") {
            yield* Fiber.interrupt(pending);
            assert.isTrue(Exit.hasInterrupts(yield* Fiber.await(pending)));
            assert.deepEqual(
              yield* store.getTimelinePages(target, selections).pipe(NodeSqliteClient.readOnly),
              expected,
            );
            return;
          }
          if (change === "unrelated") {
            const unrelated = ThreadId.make("coherent-unrelated");
            yield* sql.withTransaction(
              apply({
                id: EventId.make("coherent-other"),
                type: "thread.created",
                threadId: unrelated,
                occurredAt: now,
                payload: {
                  ...app,
                  id: unrelated,
                  lineage: {
                    parentThreadId: null,
                    relationshipToParent: null,
                    rootThreadId: unrelated,
                  },
                },
              }),
            );
          } else {
            yield* sql.withTransaction(
              apply({
                id: EventId.make("coherent-update"),
                type: "turn-item.updated",
                threadId: source,
                runId,
                occurredAt: now,
                payload: decodeItem({ ...original, output: "committed-after-snapshot" }),
              }),
            );
          }
          yield* Deferred.succeed(release, undefined);
          assert.deepEqual(yield* Fiber.join(pending), expected);
          assert.equal(transactions, 1);
          const fresh = yield* store
            .getTimelinePages(target, selections)
            .pipe(NodeSqliteClient.readOnly);
          assert.equal(fresh[0]!.snapshotSequence, expected[0]!.snapshotSequence! + 1);
          assert.equal(fresh[1]!.snapshotSequence, fresh[0]!.snapshotSequence);
          if (change !== "unrelated")
            assert.isTrue(
              fresh[1]!.items.some(
                (row) =>
                  row.item.type === "command_execution" &&
                  row.item.output === "committed-after-snapshot",
              ),
            );
        }).pipe(Effect.provide(stores));
      }).pipe(Effect.provide(persistence));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  EventId,
  MessageId,
  NodeId,
  OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ExecutionNode,
  OrchestrationV2ProviderThread,
  type OrchestrationV2Run,
  RunAttemptId,
  RunId,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { withReadPriority } from "@t3tools/shared/jones/sqliteReadPriority";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Statement from "effect/sql/Statement";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as ProviderEventIngestor from "../../orchestration-v2/ProviderEventIngestor.ts";
import * as ThreadCommandExecutor from "../../orchestration-v2/ThreadCommandExecutor.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as GitVcsDriver from "../../vcs/GitVcsDriver.ts";

const routes = ["write", "writeIfRunCurrent", "writeIfProviderThreadOwner"] as const;
type Route = (typeof routes)[number];

const testLayer = Layer.unwrap(
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "provider-node-dedup-" });
    const database = SqlitePersistence.layerFromPath(path.join(directory, "state.sqlite"), {
      size: 2,
      interactiveQueueCapacity: 1,
      backgroundQueueCapacity: 1,
      interactiveQueueTimeoutMs: 50,
      backgroundQueueTimeoutMs: 50,
      leaseWatchdogMs: 60_000,
    });
    const stores = Layer.merge(EventStore.layer, ProjectionStore.layer).pipe(
      Layer.provide(database),
    );
    const sink = EventSink.layer.pipe(Layer.provide(Layer.merge(stores, database)));
    const ingestor = ProviderEventIngestor.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          stores,
          sink,
          IdAllocator.layer,
          ThreadCommandExecutor.layer,
          Layer.mock(GitVcsDriver.GitVcsDriver)({}),
        ),
      ),
    );
    return Layer.mergeAll(database, stores, sink, ingestor);
  }),
).pipe(Layer.provideMerge(NodeServices.layer));

const seed = Effect.gen(function* () {
  const sink = yield* EventSink.EventSinkV2;
  const now = DateTime.makeUnsafe("2026-11-06T12:00:00.000Z");
  const thread = Schema.decodeUnknownSync(OrchestrationV2AppThread)({
    createdBy: "user",
    creationSource: "web",
    id: "thread:node-dedup",
    projectId: "project:node-dedup",
    title: "Provider node deduplication",
    providerInstanceId: "codex",
    modelSelection: { instanceId: "codex", model: "test" },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: "provider-thread:node-dedup",
    lineage: {
      parentThreadId: null,
      relationshipToParent: null,
      rootThreadId: "thread:node-dedup",
    },
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  });
  const provider = Schema.decodeUnknownSync(OrchestrationV2ProviderThread)({
    id: thread.activeProviderThreadId,
    driver: "codex",
    providerInstanceId: "codex",
    providerSessionId: "session:node-dedup",
    appThreadId: thread.id,
    ownerNodeId: null,
    nativeThreadRef: { driver: "codex", nativeId: "native:node-dedup", strength: "strong" },
    nativeConversationHeadRef: null,
    status: "active",
    firstRunOrdinal: 1,
    lastRunOrdinal: 1,
    handoffIds: [],
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
  });
  const nodeId = NodeId.make("node:node-dedup");
  const run: OrchestrationV2Run = {
    id: RunId.make("run:node-dedup"),
    threadId: thread.id,
    ordinal: 1,
    providerInstanceId: thread.providerInstanceId,
    modelSelection: thread.modelSelection,
    providerThreadId: provider.id,
    userMessageId: MessageId.make("message:node-dedup"),
    rootNodeId: nodeId,
    activeAttemptId: RunAttemptId.make("attempt:node-dedup"),
    status: "running",
    requestedAt: now,
    startedAt: now,
    completedAt: null,
    checkpointId: null,
    contextHandoffId: null,
  };
  const node: OrchestrationV2ExecutionNode = {
    id: nodeId,
    threadId: thread.id,
    runId: run.id,
    parentNodeId: null,
    rootNodeId: nodeId,
    kind: "approval_request",
    status: "waiting",
    countsForRun: false,
    providerThreadId: provider.id,
    providerTurnId: null,
    nativeItemRef: null,
    runtimeRequestId: null,
    checkpointScopeId: null,
    startedAt: now,
    completedAt: null,
  };
  yield* sink.write({
    events: [
      {
        id: EventId.make("seed:thread"),
        type: "thread.created",
        threadId: thread.id,
        occurredAt: now,
        payload: thread,
      },
      {
        id: EventId.make("seed:provider"),
        type: "provider-thread.updated",
        threadId: thread.id,
        occurredAt: now,
        payload: provider,
      },
      {
        id: EventId.make("seed:run"),
        type: "run.created",
        threadId: thread.id,
        occurredAt: now,
        payload: run,
      },
      {
        id: EventId.make("seed:node"),
        type: "node.updated",
        threadId: thread.id,
        occurredAt: now,
        payload: node,
      },
    ],
  });
  return { now, thread, provider, run, node };
});

type Fixture = Effect.Success<typeof seed>;
const guardForRoute = (route: Route, { provider, run }: Fixture) => {
  switch (route) {
    case "write":
      return {};
    case "writeIfRunCurrent":
      return {
        writeIfRunCurrent: {
          runId: run.id,
          activeAttemptId: run.activeAttemptId!,
          expectedStatus: "running" as const,
        },
      };
    case "writeIfProviderThreadOwner":
      return {
        writeIfProviderThreadOwner: {
          providerThreadId: provider.id,
          runId: run.id,
          activeAttemptId: run.activeAttemptId!,
          expectedLastRunOrdinal: 1,
        },
      };
  }
};

const ingestNode = Effect.fn("test.ingestProviderNode")(function* (
  route: Route,
  fixture: Fixture,
  node: OrchestrationV2ExecutionNode,
) {
  const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
  const providerSessionId = fixture.provider.providerSessionId;
  if (providerSessionId === null) return yield* Effect.die("Fixture requires a provider session");
  return yield* ingestor.ingestNormalized({
    providerSessionId,
    providerInstanceId: fixture.thread.providerInstanceId,
    threadId: fixture.thread.id,
    runId: fixture.run.id,
    nodeId: node.id,
    event: { type: "node.updated", driver: fixture.provider.driver, node },
    ...guardForRoute(route, fixture),
  });
});

it.effect.each(routes)("ingests and deduplicates nodes through %s while all readers are held", (route) =>
  Effect.gen(function* () {
    const fixture = yield* seed;
    const sql = yield* SqlClient.SqlClient;
    const release = yield* Deferred.make<void>();
    const writerConnection = yield* sql.withTransaction(
      Effect.gen(function* () {
        const [connection] = yield* sql.transactionService;
        return connection;
      }),
    );
    const dedupConnections: unknown[] = [];
    const observe: Statement.Transformer = (statement, _sql, fiber) => {
      const [query] = statement.compile();
      if (/orchestration_v2_projection_nodes WHERE node_id/.test(query)) {
        dedupConnections.push(Context.getOrUndefined(fiber.context, sql.transactionService)?.[0]);
      }
      return Effect.succeed(statement);
    };
    yield* Effect.gen(function* () {
      // Background takes the shared slot first; interactive then takes the reserved slot.
      const holders = yield* Effect.forEach(["background", "interactive"] as const, (priority) =>
        Effect.gen(function* () {
          const ready = yield* Deferred.make<void>();
          const holder = yield* Effect.forkChild(
            Effect.scoped(
              Effect.gen(function* () {
                yield* sql.reserve;
                yield* Deferred.succeed(ready, undefined);
                yield* Deferred.await(release);
              }),
            ).pipe(NodeSqliteClient.readOnly, withReadPriority(priority)),
            { startImmediately: true },
          );
          yield* Deferred.await(ready);
          return holder;
        }),
        { concurrency: 1 },
      );
      const changed = { ...fixture.node, status: "completed" as const, completedAt: fixture.now };
      yield* Effect.gen(function* () {
        assert.lengthOf(yield* ingestNode(route, fixture, changed), 1);
        assert.isEmpty(yield* ingestNode(route, fixture, { ...changed }));
      }).pipe(Effect.provideService(Statement.CurrentTransformer, observe));
      assert.equal(dedupConnections.length, 2);
      assert.isTrue(dedupConnections.every((connection) => connection === writerConnection));
      yield* Deferred.succeed(release, undefined);
      for (const holder of holders) yield* Fiber.join(holder);
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      assert.isTrue(yield* projections.isNodeStored(changed));
    }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)));
  }).pipe(Effect.scoped, Effect.provide(testLayer)),
);

it.effect.each(routes)("keeps explicit %s writes and accepted empty deduplication distinct", (route) =>
  Effect.gen(function* () {
    const fixture = yield* seed;
    const sink = yield* EventSink.EventSinkV2;
    const event: OrchestrationV2DomainEvent = {
      id: EventId.make("explicit:node"),
      type: "node.updated",
      threadId: fixture.thread.id,
      occurredAt: fixture.now,
      payload: fixture.node,
    };
    assert.lengthOf(yield* sink.write({ events: [event] }), 1);
    const input = {
      events: [{ ...event, id: EventId.make("duplicate:node") }],
      skipUnchangedProviderNodes: true,
    };
    switch (route) {
      case "write":
        assert.isEmpty(yield* sink.write(input));
        break;
      case "writeIfRunCurrent": {
        const result = yield* sink.writeIfRunCurrent({
          ...input,
          threadId: fixture.thread.id,
          ...guardForRoute(route, fixture).writeIfRunCurrent!,
        });
        assert.isTrue(result.committed);
        assert.isEmpty(result.storedEvents);
        const stale = yield* sink.writeIfRunCurrent({
          ...input,
          threadId: fixture.thread.id,
          ...guardForRoute(route, fixture).writeIfRunCurrent!,
          expectedStatus: "completed",
        });
        assert.isFalse(stale.committed);
        assert.isEmpty(stale.storedEvents);
        break;
      }
      case "writeIfProviderThreadOwner": {
        const result = yield* sink.writeIfProviderThreadOwner({
          ...input,
          ...guardForRoute(route, fixture).writeIfProviderThreadOwner!,
        });
        assert.isTrue(result.committed);
        assert.isEmpty(result.storedEvents);
        const stale = yield* sink.writeIfProviderThreadOwner({
          ...input,
          ...guardForRoute(route, fixture).writeIfProviderThreadOwner!,
          expectedLastRunOrdinal: 2,
        });
        assert.isFalse(stale.committed);
        assert.isEmpty(stale.storedEvents);
        break;
      }
    }
  }).pipe(Effect.provide(testLayer)),
);

it.effect("preserves node write failures and rolls back their event", () =>
  Effect.gen(function* () {
    const fixture = yield* seed;
    const sql = yield* SqlClient.SqlClient;
    const events = yield* EventStore.EventStoreV2;
    const sequence = yield* events.latestSequence({ threadId: fixture.thread.id });
    yield* sql`CREATE TRIGGER reject_test_node BEFORE INSERT ON orchestration_v2_projection_nodes
      BEGIN SELECT RAISE(ABORT, 'synthetic node persistence failure'); END`;
    const error = yield* ingestNode("write", fixture, {
      ...fixture.node,
      status: "completed",
      completedAt: fixture.now,
    }).pipe(Effect.flip);
    assert.instanceOf(error, ProviderEventIngestor.ProviderEventPublishError);
    assert.equal(yield* events.latestSequence({ threadId: fixture.thread.id }), sequence);
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    assert.isTrue(yield* projections.isNodeStored(fixture.node));
  }).pipe(Effect.provide(testLayer)),
);

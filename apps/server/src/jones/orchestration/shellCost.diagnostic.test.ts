// @effect-diagnostics nodeBuiltinImport:off -- Synthetic diagnostic JSON goes to stdout; the caller owns retention outside the repository.
import * as NodeConsole from "node:console";
import { assert, it } from "@effect/vitest";
import {
  EventId,
  ProjectId,
  ProviderInstanceId,
  RunId,
  ThreadId,
  type ApplicationStoredEvent,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";
import * as Tracer from "effect/Tracer";
import * as SqlClient from "effect/sql/SqlClient";
import * as Statement from "effect/sql/Statement";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as ThreadManagementService from "../../orchestration-v2/ThreadManagementService.ts";
import * as ProjectStore from "../../orchestration-v2/ProjectStore.ts";
import * as OrchestrationEventStore from "../../persistence/OrchestrationEventStore.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as ProjectService from "../../project/ProjectService.ts";
import * as ProjectEnrichmentService from "../../project/ProjectEnrichmentService.ts";
import { subscribeOrchestrationV2Shell } from "../../ws.ts";

const layer = ProjectionStore.layer.pipe(Layer.provideMerge(SqlitePersistence.layerMemory));
const iso = "2026-10-01T00:00:00.000Z";
const now = DateTime.makeUnsafe(iso);
const targetId = ThreadId.make("shell-cost:target");
const otherId = ThreadId.make("shell-cost:other");
const sourceId = ThreadId.make("shell-cost:source");
const unrelatedId = ThreadId.make("shell-cost:unrelated");
const instanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId, model: "synthetic-shell-cost" };
const runId = (id: ThreadId, index: number) => RunId.make(`${id}:run:${index}`);

type Query = { text: string; parameters?: ReadonlyArray<unknown> };
type Measurement = { operation: string; milliseconds: number; statements: Query[] };

function boundedService<T extends object>(methods: Partial<T>): T {
  return new Proxy(methods, {
    get(target, key) {
      if (!(key in target)) throw new Error(`Unexpected diagnostic service access: ${String(key)}`);
      return Reflect.get(target, key);
    },
  }) as T;
}

function measure<A, E, R>(
  operation: string,
  effect: Effect.Effect<A, E, R>,
  measurements: Measurement[],
): Effect.Effect<A, E, R> {
  return Effect.suspend(() => {
    const statements: Query[] = [];
    const bindings = new WeakMap<Tracer.Span, Query>();
    const capture: Statement.Transformer = (statement, _sql, _fiber, span) => {
      const [text, parameters] = statement.compile();
      bindings.set(span, { text, parameters: [...parameters] });
      return Effect.succeed(statement);
    };
    const tracer = Tracer.make({
      span(options) {
        const span = new Tracer.NativeSpan(options);
        const end = span.end.bind(span);
        span.end = (endTime, exit) => {
          end(endTime, exit);
          const text = span.attributes.get("db.query.text");
          if (typeof text === "string") {
            const captured = bindings.get(span);
            statements.push(captured?.text === text ? captured : { text });
          }
        };
        return span;
      },
    });
    const start = performance.now();
    return effect.pipe(
      Effect.provideService(Statement.CurrentTransformer, capture),
      Effect.withTracer(tracer),
      Effect.ensuring(
        Effect.sync(() => {
          measurements.push({ operation, milliseconds: performance.now() - start, statements });
        }),
      ),
    );
  });
}

// These are diagnostic re-executions after timing, not row counts from the timed
// driver. Only the exact SELECT text and captured bindings may be replayed.
const queryEvidence = Effect.fn("shellCost.queryEvidence")(function* (
  measurements: ReadonlyArray<Measurement>,
) {
  const sql = yield* SqlClient.SqlClient;
  const distinct = new Map<string, Query>();
  for (const { statements } of measurements) {
    for (const query of statements) distinct.set(JSON.stringify(query), query);
  }
  return yield* Effect.forEach([...distinct.values()], (query) =>
    Effect.gen(function* () {
      if (!/^\s*SELECT\b/i.test(query.text) || query.parameters === undefined) {
        return { ...query, observation: "not replayed: non-SELECT or bindings unavailable" };
      }
      const rows = yield* sql.unsafe(query.text, query.parameters);
      const plan = yield* sql.unsafe(`EXPLAIN QUERY PLAN ${query.text}`, query.parameters);
      return { ...query, observation: "post-timing replay", resultCardinality: rows.length, plan };
    }),
  );
});

const seed = Effect.fn("shellCost.seed")(function* (historyCount: number) {
  const store = yield* ProjectionStore.ProjectionStoreV2;
  const sql = yield* SqlClient.SqlClient;
  for (const id of [sourceId, targetId, otherId, unrelatedId]) {
    yield* store.apply({
      id: EventId.make(`${id}:created`),
      type: "thread.created",
      threadId: id,
      occurredAt: now,
      payload: {
        id,
        projectId: ProjectId.make(id === unrelatedId ? "unrelated-project" : "cost-project"),
        title: id,
        providerInstanceId: instanceId,
        modelSelection,
        createdBy: "user",
        creationSource: "web",
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        activeProviderThreadId: null,
        lineage:
          id === targetId
            ? { parentThreadId: sourceId, relationshipToParent: "fork", rootThreadId: sourceId }
            : { parentThreadId: null, relationshipToParent: null, rootThreadId: id },
        forkedFrom:
          id === targetId ? { type: "run", threadId: sourceId, runId: runId(sourceId, 1) } : null,
        createdAt: now,
        updatedAt: now,
        archivedAt: id === sourceId ? now : null,
        deletedAt: null,
        settledOverride: null,
        settledAt: null,
        lastVisitedAt: null,
      },
    });
    const count = id === targetId ? historyCount : id === sourceId ? 3 : id === otherId ? 2 : 7;
    // Batch synthetic persisted projections to keep fixture construction bounded;
    // every measured read still uses the receiving ProjectionStore implementation.
    for (let offset = 0; offset < count; offset += 100) {
      const indexes = Array.from({ length: Math.min(100, count - offset) }, (_, i) => i + offset);
      yield* sql`INSERT INTO orchestration_v2_projection_runs ${sql.insert(
        indexes.map((index) => {
          const status = id === unrelatedId && index === count - 1 ? "running" : "completed";
          return {
            run_id: runId(id, index),
            thread_id: id,
            ordinal: index + 1,
            provider: instanceId,
            provider_instance_id: instanceId,
            provider_thread_id: null,
            status,
            requested_at: iso,
            completed_at: status === "completed" ? iso : null,
            payload_json: JSON.stringify({
              id: runId(id, index),
              threadId: id,
              ordinal: index + 1,
              providerInstanceId: instanceId,
              modelSelection,
              providerThreadId: null,
              userMessageId: `${id}:message:${index}`,
              rootNodeId: `${id}:node:${index}`,
              activeAttemptId: null,
              status,
              requestedAt: iso,
              startedAt: iso,
              completedAt: status === "completed" ? iso : null,
              checkpointId: null,
              contextHandoffId: null,
            }),
          };
        }),
      )}`;
      yield* sql`INSERT INTO orchestration_v2_projection_turn_items ${sql.insert(
        indexes.map((index) => ({
          turn_item_id: `${id}:item:${index}`,
          thread_id: id,
          run_id: runId(id, index),
          node_id: null,
          provider_thread_id: null,
          provider_turn_id: null,
          parent_item_id: null,
          ordinal: index + 1,
          type: "assistant_message",
          status: "completed",
          updated_at: iso,
          payload_json: JSON.stringify({
            id: `${id}:item:${index}`,
            threadId: id,
            runId: runId(id, index),
            nodeId: null,
            providerThreadId: null,
            providerTurnId: null,
            nativeItemRef: null,
            parentItemId: null,
            ordinal: index + 1,
            status: "completed",
            title: null,
            startedAt: iso,
            completedAt: iso,
            updatedAt: iso,
            type: "assistant_message",
            messageId: `${id}:message:${index}`,
            text: "synthetic",
            streaming: false,
          }),
        })),
      )}`;
    }
  }
});

function scenario(clients: number, ending: "complete" | "cancel" | "failure", historyCount = 4) {
  return Effect.gen(function* () {
    yield* seed(historyCount);
    const store = yield* ProjectionStore.ProjectionStoreV2;
    const sql = yield* SqlClient.SqlClient;
    const affectedThreads = yield* Effect.forEach([targetId, otherId], (id) => store.getThread(id));
    const fixtureCounts = yield* sql<{ thread_id: string; runs: number; items: number }>`
      SELECT t.thread_id,
        (SELECT COUNT(*) FROM orchestration_v2_projection_runs r WHERE r.thread_id = t.thread_id) AS runs,
        (SELECT COUNT(*) FROM orchestration_v2_projection_turn_items i WHERE i.thread_id = t.thread_id) AS items
      FROM orchestration_v2_projection_threads t ORDER BY t.thread_id
    `;
    assert.deepEqual(fixtureCounts, [
      { thread_id: otherId, runs: 2, items: 2 },
      { thread_id: sourceId, runs: 3, items: 3 },
      { thread_id: targetId, runs: historyCount, items: historyCount },
      { thread_id: unrelatedId, runs: 7, items: 7 },
    ]);
    const eventBus = yield* PubSub.unbounded<ApplicationStoredEvent>();
    const enrichmentBus =
      yield* PubSub.unbounded<ProjectEnrichmentService.ProjectEnrichmentChange>();
    const snapshotEntered = yield* Deferred.make<void>();
    const releaseSnapshot = yield* Deferred.make<void>();
    const allLive = yield* Deferred.make<void>();
    const allMarkers = yield* Deferred.make<void>();
    let activeLive = 0;
    let activeEnrichment = 0;
    let liveStarts = 0;
    let markerCount = 0;
    let releaseAt = 0;
    let publishedAt = 0;
    const observations = Array.from({ length: clients }, (_, subscriber) => ({
      subscriber,
      measurements: [] as Measurement[],
      frames: [] as Array<{
        kind: string;
        sequence?: number;
        threadId?: ThreadId;
        releaseMilliseconds: number;
      }>,
      shellCalls: [] as ThreadId[],
      firstLiveAfterPublishMilliseconds: null as number | null,
    }));
    const bounded = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
      effect.pipe(
        Effect.raceFirst(
          Effect.sleep("15 seconds").pipe(
            Effect.andThen(() =>
              Effect.die(
                new Error(
                  `shell cost milestone stalled: ${JSON.stringify({ clients, ending, activeLive, activeEnrichment, markerCount })}`,
                ),
              ),
            ),
            Effect.provideService(Clock.Clock, Clock.Clock.defaultValue()),
          ),
        ),
      );
    const applicationEvents = boundedService<
      OrchestrationEventStore.OrchestrationEventStore["Service"]
    >({
      latestApplicationSequence: Effect.succeed(0),
      streamProjectedApplicationEvents: ({ project }) =>
        Stream.unwrap(
          Effect.gen(function* () {
            const subscription = yield* PubSub.subscribe(eventBus);
            activeLive++;
            liveStarts++;
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => {
                activeLive--;
              }),
            );
            if (liveStarts === clients) yield* Deferred.succeed(allLive, undefined);
            return Stream.fromSubscription(subscription).pipe(
              Stream.mapEffect((event) =>
                ending === "failure"
                  ? Effect.die("synthetic cost stream failure")
                  : Effect.succeed(project(event)),
              ),
            );
          }),
        ),
    });
    const enrichment = boundedService<ProjectEnrichmentService.ProjectEnrichmentService["Service"]>(
      {
        subscribeChanges: Effect.gen(function* () {
          activeEnrichment++;
          yield* Effect.addFinalizer(() =>
            Effect.sync(() => {
              activeEnrichment--;
            }),
          );
          return yield* PubSub.subscribe(enrichmentBus);
        }),
      },
    );
    yield* Effect.scoped(
      Effect.gen(function* () {
        const fibers = yield* Effect.forEach(observations, (observation) => {
          const threadManagement = boundedService<
            ThreadManagementService.ThreadManagementService["Service"]
          >({
            readShellSnapshot: (options) =>
              Effect.gen(function* () {
                yield* Deferred.succeed(snapshotEntered, undefined);
                yield* Deferred.await(releaseSnapshot);
                const decode = yield* measure(
                  "snapshot-read",
                  store.readShellSnapshot(options),
                  observation.measurements,
                ).pipe(Effect.orDie);
                // The read returns an effect: decoding must be timed when it executes,
                // outside the handler's SQL transaction, rather than at construction.
                return measure("snapshot-decode", decode, observation.measurements).pipe(
                  Effect.orDie,
                );
              }),
            getThreadShell: (id) =>
              Effect.suspend(() => {
                observation.shellCalls.push(id);
                return measure(
                  `getThreadShell:${id}`,
                  store.getThreadShell(id),
                  observation.measurements,
                ).pipe(Effect.orDie);
              }),
          });
          const client = Effect.scoped(
            Effect.gen(function* () {
              const stream = yield* subscribeOrchestrationV2Shell({
                requestCompletionMarker: true,
              });
              const observed = stream.pipe(
                Stream.tap((item) =>
                  Effect.gen(function* () {
                    const sequence =
                      "sequence" in item
                        ? item.sequence
                        : item.kind === "snapshot"
                          ? item.snapshot.snapshotSequence
                          : undefined;
                    const threadId = item.kind === "thread.updated" ? item.thread.id : undefined;
                    observation.frames.push({
                      kind: item.kind,
                      ...(sequence === undefined ? {} : { sequence }),
                      ...(threadId === undefined ? {} : { threadId }),
                      releaseMilliseconds: performance.now() - releaseAt,
                    });
                    if (item.kind === "synchronized") {
                      assert.strictEqual(observation.frames[0]?.kind, "snapshot");
                      markerCount++;
                      if (markerCount === clients) yield* Deferred.succeed(allMarkers, undefined);
                    }
                    if (item.kind === "snapshot") {
                      assert.sameMembers(
                        item.snapshot.threads.map((thread) => thread.id),
                        [targetId, otherId, unrelatedId],
                      );
                      const target = item.snapshot.threads.find(
                        (thread) => thread.id === targetId,
                      )!;
                      assert.strictEqual(target.visibleItemCount, historyCount + 3);
                      assert.strictEqual(target.latestRunId, runId(targetId, historyCount - 1));
                      assert.strictEqual(target.status, "completed");
                      assert.strictEqual(target.activeRunId, null);
                      const unrelated = item.snapshot.threads.find(
                        (thread) => thread.id === unrelatedId,
                      )!;
                      assert.strictEqual(unrelated.status, "running");
                      assert.strictEqual(unrelated.activeRunId, runId(unrelatedId, 6));
                    }
                    if (item.kind === "thread.updated") {
                      assert.isAbove(
                        observation.frames.findIndex((frame) => frame.kind === "synchronized"),
                        0,
                      );
                      if (observation.firstLiveAfterPublishMilliseconds === null)
                        observation.firstLiveAfterPublishMilliseconds =
                          performance.now() - publishedAt;
                      assert.strictEqual(
                        item.thread.id,
                        item.sequence === 511 ? targetId : otherId,
                      );
                      assert.strictEqual(
                        item.thread.visibleItemCount,
                        item.thread.id === targetId ? historyCount + 3 : 2,
                      );
                      assert.strictEqual(item.thread.status, "completed");
                      assert.strictEqual(item.thread.activeRunId, null);
                    }
                  }),
                ),
              );
              yield* Stream.runDrain(
                ending === "complete" ? observed.pipe(Stream.take(4)) : observed,
              );
            }),
          ).pipe(
            Effect.provideService(SqlClient.SqlClient, sql),
            Effect.provideService(
              ThreadManagementService.ThreadManagementService,
              threadManagement,
            ),
            Effect.provideService(
              OrchestrationEventStore.OrchestrationEventStore,
              applicationEvents,
            ),
            Effect.provideService(ProjectEnrichmentService.ProjectEnrichmentService, enrichment),
            Effect.provideService(
              ProjectStore.ProjectStoreV2,
              boundedService<ProjectStore.ProjectStoreV2["Service"]>({
                listShells: () => Effect.succeed([]),
              }),
            ),
            Effect.provideService(
              ProjectService.ProjectService,
              boundedService<ProjectService.ProjectService["Service"]>({}),
            ),
          );
          return Effect.forkChild(Effect.exit(client));
        });
        yield* Effect.addFinalizer(() =>
          Effect.forEach(fibers, Fiber.interrupt).pipe(Effect.asVoid),
        );
        yield* bounded(Deferred.await(snapshotEntered));
        assert.isTrue(observations.every(({ frames }) => frames.length === 0));
        releaseAt = performance.now();
        yield* Deferred.succeed(releaseSnapshot, undefined);
        yield* bounded(Effect.all([Deferred.await(allLive), Deferred.await(allMarkers)]));
        if (ending === "cancel") {
          yield* bounded(Effect.forEach(fibers, Fiber.interrupt));
        } else {
          // Completion cases freeze the clock to force size-based coalescing;
          // publish once, only after every marker and live observer is ready.
          const events = Array.from({ length: 512 }, (_, index): ApplicationStoredEvent => {
            const thread = affectedThreads[index % 2]!;
            return {
              sequence: index + 1,
              commandId: null,
              event: {
                id: EventId.make(`shell-cost:event:${index}`),
                type: "thread.metadata-updated",
                threadId: thread.id,
                occurredAt: now,
                payload: thread,
              },
            };
          });
          publishedAt = performance.now();
          yield* PubSub.publishAll(eventBus, events);
          const results = yield* bounded(Effect.forEach(fibers, Fiber.join));
          for (const result of results) {
            assert.strictEqual(Exit.isSuccess(result), ending === "complete");
            if (ending === "failure" && Exit.isFailure(result)) {
              assert.isTrue(Cause.hasDies(result.cause));
              assert.isTrue(
                result.cause.reasons.some(
                  (reason) =>
                    reason._tag === "Die" && reason.defect === "synthetic cost stream failure",
                ),
              );
            }
          }
        }
      }),
    );
    assert.strictEqual(activeLive, 0);
    assert.strictEqual(activeEnrichment, 0);
    for (const observation of observations) {
      assert.lengthOf(
        observation.frames.filter(({ kind }) => kind === "synchronized"),
        1,
      );
      assert.sameMembers(observation.shellCalls, ending === "complete" ? [targetId, otherId] : []);
      assert.lengthOf(
        observation.measurements.filter(({ operation }) => operation === "snapshot-read"),
        1,
      );
      assert.lengthOf(
        observation.measurements.filter(({ operation }) => operation === "snapshot-decode"),
        1,
      );
      if (ending === "complete") {
        assert.deepEqual(
          observation.frames.map(({ kind }) => kind),
          ["snapshot", "synchronized", "thread.updated", "thread.updated"],
        );
        assert.deepEqual(
          observation.frames.slice(2).map(({ sequence }) => sequence),
          [511, 512],
        );
        assert.isNotNull(observation.firstLiveAfterPublishMilliseconds);
      }
      for (const measurement of observation.measurements.filter(({ operation }) =>
        operation.startsWith("getThreadShell:"),
      )) {
        assert.isAbove(measurement.statements.length, 0);
        assert.isTrue(measurement.statements.some(({ parameters }) => parameters !== undefined));
      }
    }
    const measurements = observations.flatMap(({ measurements }) => measurements);
    const queries = yield* queryEvidence(measurements);
    // @effect-diagnostics-next-line globalConsoleInEffect:off -- Raw diagnostic JSON must reach the invoking harness rather than Effect TestConsole.
    NodeConsole.log(
      JSON.stringify({
        diagnostic: "v2-shell-cost",
        surface: "direct handler service-stream delivery",
        clock: "performance.now monotonic milliseconds; instrumentation included",
        clients,
        ending,
        fixture: {
          targetRunsAndItems: historyCount,
          sourceRunsAndItems: 3,
          forkThroughRunOrdinal: 2,
          otherRunsAndItems: 2,
          unrelatedRunsAndItems: 7,
        },
        activeLive,
        activeEnrichment,
        fixtureCounts,
        subscribers: observations.map(({ measurements, ...observation }) => ({
          ...observation,
          measurements: measurements.map(({ statements, ...measurement }) => ({
            ...measurement,
            observedSqlStatements: statements.length,
          })),
        })),
        queries,
      }),
    );
  }).pipe(Effect.provide(layer));
}

it.effect.each([1, 2, 8, 32])(
  "%i real V2 shell subscribers: startup and post-marker cost",
  (clients) => scenario(clients, "complete"),
  { timeout: 30_000 },
);
it.effect("real V2 shell cancellation releases observers", () => scenario(1, "cancel"), {
  timeout: 30_000,
});
it.live("real V2 shell upstream failure releases observers", () => scenario(1, "failure"), {
  timeout: 30_000,
});

// Explicit opt-in keeps the 10,000-run/history fixture out of ordinary discovery.
it.effect.skipIf(process.env.JONES_SHELL_COST_LARGE !== "1")(
  "large history: real V2 shell cost with a bounded fork source",
  () => scenario(1, "complete", 10_000),
  { timeout: 120_000 },
);

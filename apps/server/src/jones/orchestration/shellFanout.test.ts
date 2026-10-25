import { assert, it } from "@effect/vitest";
import {
  OrchestrationV2GetShellSnapshotError,
  ProjectId,
  ThreadId,
  type ApplicationStoredEvent,
  type OrchestrationV2ShellStreamItem,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import { seedForkShell, withNativeWatchdog } from "./shellCost.testFixtures.ts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as OrchestrationEventStore from "../../persistence/OrchestrationEventStore.ts";
import * as ThreadManagementService from "../../orchestration-v2/ThreadManagementService.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as ProjectStore from "../../orchestration-v2/ProjectStore.ts";
import * as ProjectService from "../../project/ProjectService.ts";
import * as ProjectEnrichmentService from "../../project/ProjectEnrichmentService.ts";
import { subscribeOrchestrationV2Shell } from "../../ws.ts";
import * as ShellMaterialization from "./shellMaterialization.ts";

// Service-boundary accounting, not SQLite-query counts or WebSocket throughput.
// Only the exported V2 handler is exercised; unsupported fake calls fail closed.
function boundedService<T extends object>(methods: Partial<T>): T {
  return new Proxy(methods, {
    get(target, key) {
      if (!(key in target)) throw new Error(`Unexpected service access: ${String(key)}`);
      return Reflect.get(target, key);
    },
  }) as T;
}

const composedPair = Effect.fn("shellFanout.composedPair")(function* (input: {
  readonly threadId: ThreadId;
  readonly version: () => number;
  readonly read: ThreadManagementService.ThreadManagementService["Service"]["getThreadShell"];
  readonly terminalSequence: number;
}) {
  const events = yield* PubSub.unbounded<ApplicationStoredEvent>();
  const enrichment = yield* PubSub.unbounded<ProjectEnrichmentService.ProjectEnrichmentChange>();
  const ready = yield* Deferred.make<void>();
  const observed = yield* Queue.unbounded<{
    readonly client: number;
    readonly item: OrchestrationV2ShellStreamItem;
  }>();
  let liveStarts = 0;
  const threads = boundedService<ThreadManagementService.ThreadManagementService["Service"]>({
    readShellSnapshot: () =>
      Effect.succeed(
        Effect.succeed({ schemaVersion: 1, snapshotSequence: 0, threads: [], archivedThreads: [] }),
      ),
    getThreadShell: input.read,
  });
  const applicationEvents = boundedService<
    OrchestrationEventStore.OrchestrationEventStore["Service"]
  >({
    latestApplicationSequence: Effect.sync(input.version),
    streamProjectedApplicationEvents: ({ project }) =>
      Stream.unwrap(
        Effect.gen(function* () {
          const subscription = yield* PubSub.subscribe(events);
          if (++liveStarts === 2) yield* Deferred.succeed(ready, undefined);
          return Stream.fromSubscription(subscription).pipe(Stream.map(project));
        }),
      ),
  });
  const materializationContext = yield* Layer.build(ShellMaterialization.layer).pipe(
    Effect.provideService(ThreadManagementService.ThreadManagementService, threads),
    Effect.provideService(OrchestrationEventStore.OrchestrationEventStore, applicationEvents),
  );
  const materialization = Context.get(
    materializationContext,
    ShellMaterialization.ShellMaterialization,
  );
  const fibers = yield* Effect.forEach([0, 1], (client) =>
    Effect.gen(function* () {
      const stream = yield* subscribeOrchestrationV2Shell({});
      return yield* stream.pipe(
        Stream.tap((item) =>
          "sequence" in item ? Queue.offer(observed, { client, item }) : Effect.void,
        ),
        Stream.takeUntil((item) => "sequence" in item && item.sequence === input.terminalSequence),
        Stream.runCollect,
      );
    }).pipe(
      Effect.scoped,
      Effect.provideService(ShellMaterialization.ShellMaterialization, materialization),
      Effect.provideService(ThreadManagementService.ThreadManagementService, threads),
      Effect.provideService(OrchestrationEventStore.OrchestrationEventStore, applicationEvents),
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
      Effect.provideService(
        ProjectEnrichmentService.ProjectEnrichmentService,
        boundedService<ProjectEnrichmentService.ProjectEnrichmentService["Service"]>({
          subscribeChanges: PubSub.subscribe(enrichment),
        }),
      ),
      Effect.forkScoped,
    ),
  );
  yield* Deferred.await(ready);
  const publish = (throughSequence: number, barrier = false) =>
    PubSub.publishAll(
      events,
      Array.from({ length: 512 }, (_, index) => {
        const sequence = throughSequence - 511 + index;
        return barrier && index === 511
          ? {
              sequence,
              aggregateKind: "project",
              aggregateId: ProjectId.make(`shell-composed:barrier:${sequence}`),
              type: "project.deleted",
            }
          : { sequence, event: { threadId: input.threadId } };
      }) as ApplicationStoredEvent[],
    );
  const nextPair = Effect.forEach([0, 1], () => Queue.take(observed));
  return { publish, nextPair, collect: Effect.forEach(fibers, Fiber.join) };
});

const composedLayer = ProjectionStore.layer.pipe(Layer.provideMerge(SqlitePersistence.layerMemory));

it.effect("composed subscribers refresh a fork when only its inherited shell count changes", () =>
  Effect.gen(function* () {
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const { threadId, appendInheritedItem } = yield* seedForkShell();
    let version = 0;
    let reads = 0;
    const pair = yield* composedPair({
      threadId,
      version: () => version,
      terminalSequence: 1024,
      read: (id) =>
        Effect.sync(() => reads++).pipe(
          Effect.andThen(projections.getThreadShell(id)),
          Effect.orDie,
        ),
    });
    version = 512;
    yield* pair.publish(512);
    const before = yield* pair.nextPair;
    assert.sameMembers(
      before.map(({ client }) => client),
      [0, 1],
    );
    assert.strictEqual(reads, 1);
    yield* appendInheritedItem(2);
    version = 1024;
    yield* pair.publish(1024);
    const results = yield* pair.collect;
    for (const items of results) {
      const updates = items.filter((item) => item.kind === "thread.updated");
      assert.deepEqual(
        updates.map(({ sequence }) => sequence),
        [512, 1024],
      );
      const [first, second] = updates;
      assert.strictEqual(first!.thread.itemCount, 0);
      assert.strictEqual(second!.thread.itemCount, 0);
      assert.strictEqual(first!.thread.visibleItemCount, 2);
      assert.strictEqual(second!.thread.visibleItemCount, 3);
      assert.deepEqual(first!.thread.updatedAt, second!.thread.updatedAt);
    }
    assert.strictEqual(reads, 2);
  }).pipe(
    Effect.scoped,
    (effect) => withNativeWatchdog(effect, "5 seconds", () => new Error("Inherited shell stalled")),
    Effect.provide(composedLayer),
  ),
);

it.effect("composed subscribers resend an unchanged cached shell at five seconds", () =>
  Effect.gen(function* () {
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const { threadId } = yield* seedForkShell();
    let reads = 0;
    let version = 0;
    // One committed watermark covers all three post-startup batches, so resends reuse it.
    const pair = yield* composedPair({
      threadId,
      version: () => version,
      terminalSequence: 1536,
      read: (id) =>
        Effect.sync(() => reads++).pipe(
          Effect.andThen(projections.getThreadShell(id)),
          Effect.orDie,
        ),
    });
    version = 1536;
    yield* pair.publish(512);
    const first = yield* pair.nextPair;
    assert.sameMembers(
      first.map(({ client }) => client),
      [0, 1],
    );
    assert.isTrue(
      first.every(({ item }) => item.kind === "thread.updated" && item.sequence === 512),
    );
    yield* TestClock.adjust(4999);
    yield* pair.publish(1024, true);
    const suppressed = yield* pair.nextPair;
    // The project barrier is downstream of the skipped thread in each composed stream.
    assert.sameMembers(
      suppressed.map(({ client }) => client),
      [0, 1],
    );
    assert.isTrue(
      suppressed.every(({ item }) => item.kind === "project.removed" && item.sequence === 1024),
    );
    yield* TestClock.adjust(1);
    yield* pair.publish(1536, true);
    const results = yield* pair.collect;
    for (const items of results) {
      assert.deepEqual(
        items.flatMap((item) => (item.kind === "thread.updated" ? [item.sequence] : [])),
        [512, 1535],
      );
    }
    assert.strictEqual(reads, 1);
  }).pipe(
    Effect.scoped,
    (effect) => withNativeWatchdog(effect, "5 seconds", () => new Error("Shell resend stalled")),
    Effect.provide(composedLayer),
  ),
);

for (const clients of [1, 2, 8, 32]) {
  for (const ending of ["complete", "cancel", "failure"] as const) {
    // A controlled clock keeps the 512-event completion batch deterministic.
    const runTest = ending === "failure" ? it.live : it.effect;
    runTest(`${clients} V2 shell clients: ${ending} releases every subscription`, () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const changes = yield* PubSub.unbounded<ProjectEnrichmentService.ProjectEnrichmentChange>();
        const eventBus = yield* PubSub.unbounded<ApplicationStoredEvent>();
        const ready = yield* Deferred.make<void, OrchestrationV2GetShellSnapshotError>();
        let snapshots = 0;
        let shellReads = 0;
        let committedSequence = 0;
        let projectReads = 0;
        let activeEnrichment = 0;
        let activeLive = 0;
        let liveStarts = 0;
        const boundedPhase = <A, E, R>(phase: string, effect: Effect.Effect<A, E, R>) =>
          withNativeWatchdog(
            effect,
            "5 seconds",
            () =>
              new Error(
                `Shell fanout ${phase} timed out: ${JSON.stringify({
                  clients,
                  ending,
                  snapshots,
                  shellReads,
                  projectReads,
                  liveStarts,
                  activeLive,
                  activeEnrichment,
                })}`,
              ),
          );
        const threadId = ThreadId.make("fanout-deleted-thread");
        // A full 512-event batch flushes coalescing without sleeps. Every client
        // subscribes to the same publisher before a single shared publication.
        const events = Array.from({ length: 512 }, (_, index) => ({
          sequence: index + 1,
          event: { threadId },
        })) as ApplicationStoredEvent[];
        const live = Stream.unwrap(
          Effect.gen(function* () {
            const subscription = yield* PubSub.subscribe(eventBus);
            activeLive++;
            liveStarts++;
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => {
                activeLive--;
              }),
            );
            if (liveStarts === clients) yield* Deferred.succeed(ready, undefined);
            return Stream.fromSubscription(subscription).pipe(
              Stream.mapEffect((event) =>
                ending === "failure"
                  ? Effect.die("synthetic upstream failure")
                  : Effect.succeed(event),
              ),
            );
          }),
        );
        const threadManagement = boundedService<
          ThreadManagementService.ThreadManagementService["Service"]
        >({
          readShellSnapshot: () =>
            Effect.sync(() => {
              snapshots++;
              return Effect.succeed({
                schemaVersion: 1,
                snapshotSequence: 0,
                threads: [],
                archivedThreads: [],
              });
            }),
          getThreadShell: () =>
            Effect.sync(() => {
              shellReads++;
              return null;
            }),
        });
        const applicationEvents = boundedService<
          OrchestrationEventStore.OrchestrationEventStore["Service"]
        >({
          latestApplicationSequence: Effect.sync(() => committedSequence),
          streamProjectedApplicationEvents: ({ project }) => live.pipe(Stream.map(project)),
        });
        const materializationContext = yield* Layer.build(ShellMaterialization.layer).pipe(
          Effect.provideService(ThreadManagementService.ThreadManagementService, threadManagement),
          Effect.provideService(OrchestrationEventStore.OrchestrationEventStore, applicationEvents),
        );
        const materialization = Context.get(
          materializationContext,
          ShellMaterialization.ShellMaterialization,
        );
        const enrichment = boundedService<
          ProjectEnrichmentService.ProjectEnrichmentService["Service"]
        >({
          subscribeChanges: Effect.gen(function* () {
            activeEnrichment++;
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => {
                activeEnrichment--;
              }),
            );
            return yield* PubSub.subscribe(changes);
          }),
        });
        const client = Effect.scoped(
          Effect.gen(function* () {
            const stream = yield* subscribeOrchestrationV2Shell({});
            return yield* Stream.runCollect(
              ending === "complete" ? stream.pipe(Stream.take(2)) : stream,
            );
          }),
        ).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
          Effect.provideService(ShellMaterialization.ShellMaterialization, materialization),
          Effect.provideService(ThreadManagementService.ThreadManagementService, threadManagement),
          Effect.provideService(OrchestrationEventStore.OrchestrationEventStore, applicationEvents),
          Effect.provideService(
            ProjectStore.ProjectStoreV2,
            boundedService<ProjectStore.ProjectStoreV2["Service"]>({
              listShells: () =>
                Effect.sync(() => {
                  projectReads++;
                  return [];
                }),
            }),
          ),
          Effect.provideService(
            ProjectService.ProjectService,
            boundedService<ProjectService.ProjectService["Service"]>({}),
          ),
          Effect.provideService(ProjectEnrichmentService.ProjectEnrichmentService, enrichment),
        );
        // Preserve early failure causes before Effect.exit makes them fiber values.
        const observedClient = client.pipe(
          Effect.onExit((exit) => {
            if (liveStarts === clients) return Effect.void;
            return Exit.isFailure(exit)
              ? Deferred.failCause(ready, exit.cause)
              : Deferred.die(
                  ready,
                  new Error(`Shell client completed before readiness: ${liveStarts}/${clients}`),
                );
          }),
        );
        const fibers = yield* Effect.forEach(Array.from({ length: clients }), () =>
          Effect.forkChild(Effect.exit(observedClient)),
        );
        yield* boundedPhase("readiness", Deferred.await(ready));
        if (ending === "cancel") {
          yield* boundedPhase("cancellation", Effect.forEach(fibers, Fiber.interrupt));
        } else {
          committedSequence = 512;
          yield* PubSub.publishAll(eventBus, events);
          const results = yield* boundedPhase("completion", Effect.forEach(fibers, Fiber.join));
          for (const result of results) {
            if (ending === "failure") assert.isTrue(Exit.isFailure(result));
            else {
              assert.isTrue(Exit.isSuccess(result));
              if (Exit.isSuccess(result)) {
                const items = Array.from(result.value);
                assert.strictEqual(items[0]?.kind, "snapshot");
                assert.deepEqual(items[1], {
                  kind: "thread.removed",
                  sequence: 512,
                  location: "active",
                  threadId,
                });
              }
            }
          }
        }
        assert.strictEqual(snapshots, clients);
        assert.strictEqual(projectReads, clients);
        assert.strictEqual(shellReads, ending === "complete" ? 1 : 0);
        assert.strictEqual(liveStarts, clients);
        assert.strictEqual(activeLive, 0);
        assert.strictEqual(activeEnrichment, 0);
      }).pipe(Effect.scoped, Effect.provide(SqlitePersistence.layerMemory)),
    );
  }
}

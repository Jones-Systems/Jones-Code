import { assert, it } from "@effect/vitest";
import {
  OrchestrationV2GetShellSnapshotError,
  ThreadId,
  type ApplicationStoredEvent,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as OrchestrationEventStore from "../../persistence/OrchestrationEventStore.ts";
import * as ThreadManagementService from "../../orchestration-v2/ThreadManagementService.ts";
import * as ProjectStore from "../../orchestration-v2/ProjectStore.ts";
import * as ProjectService from "../../project/ProjectService.ts";
import * as ProjectEnrichmentService from "../../project/ProjectEnrichmentService.ts";
import { subscribeOrchestrationV2Shell } from "../../ws.ts";

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
        let projectReads = 0;
        let activeEnrichment = 0;
        let activeLive = 0;
        let liveStarts = 0;
        const boundedPhase = <A, E, R>(phase: string, effect: Effect.Effect<A, E, R>) =>
          effect.pipe(
            Effect.raceFirst(
              Effect.sleep("5 seconds").pipe(
                Effect.andThen(() =>
                  Effect.die(
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
                  ),
                ),
                // Only the watchdog uses wall time; leave stream coalescing on its test clock.
                Effect.provideService(Clock.Clock, Clock.Clock.defaultValue()),
              ),
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
          getShellSnapshot: () =>
            Effect.sync(() => {
              snapshots++;
              return { schemaVersion: 1, snapshotSequence: 0, threads: [], archivedThreads: [] };
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
          latestApplicationSequence: Effect.succeed(0),
          streamProjectedApplicationEvents: ({ project }) => live.pipe(Stream.map(project)),
        });
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
        assert.strictEqual(shellReads, ending === "complete" ? clients : 0);
        assert.strictEqual(liveStarts, clients);
        assert.strictEqual(activeLive, 0);
        assert.strictEqual(activeEnrichment, 0);
      }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
    );
  }
}

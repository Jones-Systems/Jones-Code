import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  ThreadId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RuntimeRequestId,
} from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { ReadPriority } from "@t3tools/shared/jones/sqliteReadPriority";
import type { SqliteReaderObservation } from "@t3tools/shared/jones/sqliteReaderContracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as SqlClient from "effect/sql/SqlClient";
import * as Persistence from "../../persistence/Sqlite.ts";
import * as Outbox from "../../orchestration-v2/EffectOutbox.ts";
import * as Worker from "../../orchestration-v2/EffectWorker.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as Projection from "../../orchestration-v2/ProjectionStore.ts";
import * as Projects from "../../orchestration-v2/ProjectStore.ts";
import * as Receipts from "../../orchestration-v2/CommandReceiptStore.ts";
import * as Positions from "../../orchestration-v2/TurnItemPositionStore.ts";
import { backgroundRead, interactiveRead } from "./readClassification.ts";
import { readerPoolOptions } from "./persistenceOptions.ts";

type ControlReceipt = {
  readonly type: "control-read-completed";
  readonly effectType: "provider-turn.interrupt" | "runtime-request.respond";
  readonly priority: "interactive" | "background";
  readonly value: number;
};

// Two exercises the real action reservation; three adds the urgent lane.
// One is the supported mixed-only daemon fallback and has no action reservation.
const controlCases = ([2, 3] as const).flatMap((size) =>
  [false, true].flatMap((inherited) =>
    [1, 2, 3].flatMap((concurrency) =>
      [false, true].map((response) => ({ size, inherited, concurrency, response })),
    ),
  ),
);
it.effect.each(controlCases)(
  "reserved control progress: readers=$size, lanes=$concurrency, inheritedBackground=$inherited, response=$response",
  ({ size, inherited, concurrency, response }) =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const directory = yield* fs.makeTempDirectoryScoped({
          prefix: "jones-control-readers-",
        });
        const observations: SqliteReaderObservation[] = [];
        const database = Persistence.layerFromPath(
          path.join(directory, "state.sqlite"),
          readerPoolOptions(size, (event) => {
            observations.push(event);
          }),
        );
        const stores = Layer.mergeAll(
          Outbox.layer,
          EventStore.layer,
          Projection.layer,
          Projects.layer,
          Receipts.layer,
          Positions.layer,
        ).pipe(Layer.provideMerge(database));
        const services = EventSink.layerFromStores.pipe(Layer.provideMerge(stores));
        yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          const outbox = yield* Outbox.EffectOutboxV2;
          const sink = yield* EventSink.EventSinkV2;
          const releaseBackground = yield* Deferred.make<void>();
          let backgroundReleased = false;
          // Finalizers release barriers before scoped fibers are interrupted.
          yield* Effect.addFinalizer(() => Deferred.succeed(releaseBackground, undefined));
          const holders = [];
          for (let index = 0; index < size - 1; index++) {
            const acquired = yield* Deferred.make<void>();
            holders.push(
              yield* sql
                .withTransaction(
                  Effect.gen(function* () {
                    yield* sql`SELECT 1`;
                    yield* Deferred.succeed(acquired, undefined);
                    yield* Deferred.await(releaseBackground);
                  }),
                )
                .pipe(NodeSqliteClient.readOnly, backgroundRead, Effect.forkScoped),
            );
            yield* Deferred.await(acquired);
          }
          const parked = yield* Deferred.make<void>();
          const resumeProbe = yield* Deferred.make<void>();
          const probePassed = yield* Deferred.make<"interactive" | "background">();
          const milestone = yield* Deferred.make<ControlReceipt>();
          let parkedOnce = false;
          const observedOutbox = Outbox.EffectOutboxV2.of({
            ...outbox,
            // Deterministic barrier around the actual SQL probe; no scheduler,
            // claim, queue or action-admission algorithm is substituted.
            nextClaimableAt: (input) =>
              Effect.gen(function* () {
                const target =
                  concurrency >= 3
                    ? input?.urgentOnly === true
                    : concurrency === 2
                      ? input?.excludeTitleGeneration === true
                      : true;
                if (target && !parkedOnce) {
                  parkedOnce = true;
                  yield* Deferred.succeed(parked, undefined);
                  yield* Deferred.await(resumeProbe);
                  const result = yield* outbox.nextClaimableAt(input);
                  yield* Deferred.succeed(probePassed, yield* ReadPriority);
                  return result;
                }
                return yield* outbox.nextClaimableAt(input);
              }),
          });
          const executor = Worker.OrchestrationEffectExecutorV2.of({
            // Only the external provider side effect is replaced. Its actual
            // executing read must still acquire the reserved SQLite connection.
            execute: (effect) =>
              Effect.gen(function* () {
                const rows = yield* sql<{ value: number }>`SELECT 17 AS value`.pipe(
                  NodeSqliteClient.readOnly,
                );
                assert.isFalse(backgroundReleased);
                assert.isTrue(
                  effect.request.type === "provider-turn.interrupt" ||
                    effect.request.type === "runtime-request.respond",
                );
                yield* Deferred.succeed(milestone, {
                  type: "control-read-completed" as const,
                  effectType: effect.request.type as ControlReceipt["effectType"],
                  priority: yield* ReadPriority,
                  value: rows[0]!.value,
                });
              }).pipe(Effect.orDie),
          });
          const workerLayer = Worker.layerWithOptions({
            workerId: "reserved-control",
          }).pipe(
            Layer.provide(Layer.succeed(Outbox.EffectOutboxV2, observedOutbox)),
            Layer.provide(Layer.succeed(Worker.OrchestrationEffectExecutorV2, executor)),
          );
          const daemon = yield* Worker.runDaemonWithOptions({
            concurrency,
            livenessPollIntervalMs: 30_000,
          }).pipe(
            Effect.provide(workerLayer),
            inherited ? backgroundRead : interactiveRead,
            Effect.forkScoped,
          );
          yield* Deferred.await(parked);
          const observationStart = observations.length;
          const commandId = CommandId.make("command:reserved-control");
          yield* sink.writeWithEffects({
            commandId,
            events: [],
            effects: [
              {
                id: "effect:reserved-control",
                commandId,
                threadId: ThreadId.make("thread:reserved-control"),
                request: response
                  ? {
                      type: "runtime-request.respond",
                      providerSessionId: ProviderSessionId.make("session:test"),
                      requestId: RuntimeRequestId.make("request:test"),
                    }
                  : {
                      type: "provider-turn.interrupt",
                      providerSessionId: ProviderSessionId.make("session:test"),
                      providerThreadId: ProviderThreadId.make("provider-thread:test"),
                      providerTurnId: ProviderTurnId.make("turn:test"),
                    },
              },
            ],
          });
          yield* Deferred.succeed(resumeProbe, undefined);
          assert.equal(yield* Deferred.await(probePassed), "interactive");
          const receipt = yield* Deferred.await(milestone);
          assert.equal(receipt.type, "control-read-completed");
          assert.equal(receipt.priority, "interactive");
          assert.equal(receipt.value, 17);
          assert.isTrue(
            observations
              .slice(observationStart)
              .some(
                (event) =>
                  event.slotId === 0 && event.priority === "interactive" && event.outcome === "ok",
              ),
          );
          yield* outbox.awaitSettled("effect:reserved-control");
          yield* Fiber.interrupt(daemon);
          backgroundReleased = true;
          yield* Deferred.succeed(releaseBackground, undefined);
          for (const holder of holders) yield* Fiber.join(holder);
        }).pipe(Effect.provide(services));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

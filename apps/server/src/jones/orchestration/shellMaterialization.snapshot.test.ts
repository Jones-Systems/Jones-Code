import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { EventId, ProjectId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Tracer from "effect/Tracer";
import * as SqlClient from "effect/sql/SqlClient";
import * as Statement from "effect/sql/Statement";

import { OrchestratorProjectionError } from "../../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import type { RecordedLifecycleEvent } from "../../orchestration-v2/RecordedTypes.ts";
import * as ThreadManagementService from "../../orchestration-v2/ThreadManagementService.ts";
import * as OrchestrationEventStore from "../../persistence/OrchestrationEventStore.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as ShellMaterialization from "./shellMaterialization.ts";

function boundedService<T extends object>(methods: Partial<T>): T {
  return new Proxy(methods, {
    get(target, key) {
      if (!(key in target)) throw new Error(`Unexpected service access: ${String(key)}`);
      return Reflect.get(target, key);
    },
  }) as T;
}

it.effect(
  "file-backed WAL fixes the version before the nested shell read on the same connection",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      // The enclosing scope closes the SQLite layer and workers before removing
      // this unique synthetic fixture, including on failure or interruption.
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "jones-shell-snapshot-" });
      const database = SqlitePersistence.layerFromPath(path.join(directory, "state.sqlite"));
      const stores = Layer.mergeAll(ProjectionStore.layer, OrchestrationEventStore.layer).pipe(
        Layer.provideMerge(database),
      );
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const events = yield* OrchestrationEventStore.OrchestrationEventStore;
        const threadId = ThreadId.make("materialization:wal");
        const now = DateTime.makeUnsafe("2026-10-01T00:00:00.000Z");
        const instanceId = ProviderInstanceId.make("codex");
        const created: RecordedLifecycleEvent = {
          id: EventId.make("materialization:wal:created"),
          type: "thread.created",
          threadId,
          occurredAt: now,
          payload: {
            id: threadId,
            projectId: ProjectId.make("materialization:project"),
            title: "before writer commit",
            providerInstanceId: instanceId,
            modelSelection: { instanceId, model: "synthetic" },
            createdBy: "user",
            creationSource: "web",
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: null,
            activeProviderThreadId: null,
            lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
            forkedFrom: null,
            createdAt: now,
            updatedAt: now,
            archivedAt: null,
            deletedAt: null,
            settledOverride: null,
            settledAt: null,
            lastVisitedAt: null,
          },
        };
        const updated: RecordedLifecycleEvent = {
          ...created,
          id: EventId.make("materialization:wal:updated"),
          type: "thread.metadata-updated",
          payload: { ...created.payload, title: "after writer commit" },
        };
        const commit = (event: RecordedLifecycleEvent) =>
          sql.withTransaction(
            Effect.gen(function* () {
              const stored = yield* events.appendAgentEvents({ events: [event] });
              yield* projections.apply(event);
              return stored[0]!.sequence;
            }),
          );
        const firstSequence = yield* commit(created);
        const versionRead = yield* Deferred.make<void>();
        const writerCommitted = yield* Deferred.make<
          void,
          Effect.Error<ReturnType<typeof commit>>
        >();
        // Spawn outside the reader context: this writer must commit while the
        // materialization holds its read snapshot open across both reads.
        const writer = yield* Effect.forkChild(
          Effect.gen(function* () {
            yield* Deferred.await(versionRead);
            const sequence = yield* commit(updated);
            yield* Deferred.succeed(writerCommitted, undefined);
            return sequence;
          }).pipe(
            Effect.onExit((exit) =>
              Exit.isFailure(exit) ? Deferred.failCause(writerCommitted, exit.cause) : Effect.void,
            ),
          ),
        );
        const order: string[] = [];
        let reads = 0;
        let nestedStatements = 0;
        let readerTransactions = 0;
        const tracer = Tracer.make({
          span(options) {
            const span = new Tracer.NativeSpan(options);
            const end = span.end.bind(span);
            span.end = (time, exit) => {
              if (
                span.name === "sql.transaction" &&
                span.attributes.get("db.connection") === "reader"
              ) {
                readerTransactions++;
              }
              end(time, exit);
            };
            return span;
          },
        });
        const threadManagement = boundedService<
          ThreadManagementService.ThreadManagementService["Service"]
        >({
          getThreadShell: (id) =>
            Effect.gen(function* () {
              reads++;
              order.push("shell");
              const outer = yield* Effect.serviceOption(sql.transactionService);
              assert.isTrue(Option.isSome(outer));
              const capture: Statement.Transformer = (statement, _sql, fiber) => {
                const nested = Context.getOption(fiber.context, sql.transactionService);
                assert.isTrue(Option.isSome(nested));
                if (Option.isSome(outer) && Option.isSome(nested)) {
                  assert.strictEqual(nested.value[0], outer.value[0]);
                  assert.isAbove(nested.value[1], outer.value[1]);
                  nestedStatements++;
                }
                return Effect.succeed(statement);
              };
              return yield* projections.getThreadShell(id).pipe(
                Effect.provideService(Statement.CurrentTransformer, capture),
                Effect.mapError(
                  (cause) => new OrchestratorProjectionError({ threadId: id, cause }),
                ),
              );
            }),
        });
        const versionedEvents = boundedService<
          OrchestrationEventStore.OrchestrationEventStore["Service"]
        >({
          latestApplicationSequence: Effect.gen(function* () {
            order.push("version");
            const sequence = yield* events.latestApplicationSequence;
            if (order.length === 1) {
              yield* Deferred.succeed(versionRead, undefined);
              yield* Deferred.await(writerCommitted).pipe(Effect.orDie);
            }
            return sequence;
          }),
        });
        yield* Effect.gen(function* () {
          const service = yield* ShellMaterialization.ShellMaterialization;
          const reader = yield* service.forSubscription;
          const before = yield* reader.get(threadId, firstSequence);
          assert.strictEqual(before?.title, "before writer commit");
          assert.deepEqual(order, ["version", "shell"]);
          assert.isAbove(readerTransactions, 0);
          assert.isAbove(nestedStatements, 0);
          const nextSequence = yield* Fiber.join(writer);
          assert.isAbove(nextSequence, firstSequence);
          const after = yield* reader.get(threadId, nextSequence);
          assert.strictEqual(after?.title, "after writer commit");
          assert.strictEqual(reads, 2);
          assert.deepEqual(order, ["version", "shell", "version", "shell"]);
        }).pipe(
          Effect.provide(ShellMaterialization.layer),
          Effect.provideService(ThreadManagementService.ThreadManagementService, threadManagement),
          Effect.provideService(OrchestrationEventStore.OrchestrationEventStore, versionedEvents),
          Effect.withTracer(tracer),
        );
      }).pipe(Effect.provide(stores));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { ThreadId } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as SqlClient from "effect/sql/SqlClient";
import * as Statement from "effect/sql/Statement";
import * as Tracer from "effect/Tracer";
import * as CommandReceiptStore from "../../orchestration-v2/CommandReceiptStore.ts";
import * as LegacyV1ThreadImporter from "../../orchestration-v2/legacy/LegacyV1ThreadImporter.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as ProviderSessionManager from "../../orchestration-v2/ProviderSessionManager.ts";
import * as ThreadManagement from "../../orchestration-v2/ThreadManagementService.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as ServerSettings from "../../serverSettings.ts";

it.effect("coherent reference hydrates on writer before entering reader transaction", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "jones-reference-hydration-" });
    const persistence = SqlitePersistence.layerFromPath(
      path.join(directory, "userdata", "state.sqlite"),
    );
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TABLE jones_test_hydration (value TEXT NOT NULL)`;
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let reads = 0;
      const connections: unknown[] = [];
      const hydrationConnections: unknown[] = [];
      const tracer = Tracer.make({
        span(options) {
          const span = new Tracer.NativeSpan(options);
          const end = span.end.bind(span);
          span.end = (time, exit) => {
            const query = span.attributes.get("db.query.text");
            if (span.name === "sql.transaction") {
              connections.push(span.attributes.get("db.connection"));
            }
            if (query === "INSERT INTO jones_test_hydration VALUES ('hydrated')") {
              hydrationConnections.push(span.attributes.get("db.connection"));
            }
            end(time, exit);
          };
          return span;
        },
      });
      const serviceLayer = ThreadManagement.layerWithLegacyImporter.pipe(
        Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
        Layer.provide(
          Layer.mock(Orchestrator.OrchestratorV2)({
            getTimelinePages: (threadId, selections) =>
              sql
                .withTransaction(
                  Effect.gen(function* () {
                    reads++;
                    const rows = yield* sql<{
                      value: string;
                    }>`SELECT value FROM jones_test_hydration`;
                    assert.deepEqual(rows, [{ value: "hydrated" }]);
                    return selections.map(() => ({
                      items: [],
                      totalItems: 0,
                      hasMore: false,
                      snapshotSequence: 1,
                    }));
                  }),
                )
                .pipe(
                  Effect.mapError(
                    (cause) => new Orchestrator.OrchestratorProjectionError({ threadId, cause }),
                  ),
                ),
          }),
        ),
        Layer.provide(
          Layer.mock(LegacyV1ThreadImporter.LegacyV1ThreadImporter)({
            ensureTranscript: (threadId) =>
              Effect.gen(function* () {
                yield* Deferred.succeed(started, undefined);
                yield* Deferred.await(release);
                yield* sql`INSERT INTO jones_test_hydration VALUES ('hydrated')`;
                return { importedThreadCount: 1, importedMessageCount: 1 };
              }).pipe(
                Effect.mapError(
                  (cause) =>
                    new LegacyV1ThreadImporter.LegacyV1ThreadImportError({
                      operation: "hydrate transcript for",
                      threadId,
                      cause,
                    }),
                ),
              ),
          }),
        ),
        Layer.provide(ServerSettings.layerTest()),
        Layer.provide(CommandReceiptStore.layer),
      );
      yield* Effect.gen(function* () {
        const threads = yield* ThreadManagement.ThreadManagementService;
        const pending = yield* Effect.forkChild(
          threads
            .getTimelinePages(ThreadId.make("hydrated"), [
              { view: "messages", tail: true, limit: 64 },
              { view: "activity", tail: true, limit: 60 },
            ])
            .pipe(
              Effect.provideService(Statement.SpanPropagationEnabled, true),
              Effect.withTracer(tracer),
            ),
        );
        yield* Deferred.await(started);
        assert.equal(reads, 0);
        yield* Deferred.succeed(release, undefined);
        const pages = yield* Fiber.join(pending);
        assert.equal(pages.length, 2);
        assert.equal(reads, 1);
        assert.deepEqual(hydrationConnections, ["writer"]);
        assert.deepEqual(connections, ["reader"]);
      }).pipe(Effect.provide(serviceLayer));
    }).pipe(Effect.provide(persistence));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect.each(["hydration", "projection"] as const)(
  "coherent reference preserves the exact %s domain error",
  (stage) =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("failed-reference");
      const hydrationError = new LegacyV1ThreadImporter.LegacyV1ThreadImportError({
        operation: "hydrate transcript for",
        threadId,
      });
      const projectionError = new Orchestrator.OrchestratorProjectionError({
        threadId,
        cause: new ProjectionStore.ProjectionStoreThreadNotFoundError({ threadId }),
      });
      let reads = 0;
      const serviceLayer = ThreadManagement.layerWithLegacyImporter.pipe(
        Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
        Layer.provide(
          Layer.mock(Orchestrator.OrchestratorV2)({
            getTimelinePages: () =>
              Effect.suspend(() => {
                reads++;
                return Effect.fail(projectionError);
              }),
          }),
        ),
        Layer.provide(
          Layer.mock(LegacyV1ThreadImporter.LegacyV1ThreadImporter)({
            ensureTranscript: () =>
              stage === "hydration"
                ? Effect.fail(hydrationError)
                : Effect.succeed({ importedThreadCount: 0, importedMessageCount: 0 }),
          }),
        ),
        Layer.provide(Layer.mock(ServerSettings.ServerSettingsService)({})),
        Layer.provide(Layer.mock(CommandReceiptStore.CommandReceiptStoreV2)({})),
      );
      yield* Effect.gen(function* () {
        const threads = yield* ThreadManagement.ThreadManagementService;
        const error = yield* Effect.flip(
          threads.getTimelinePages(threadId, [
            { view: "messages", tail: true, limit: 64 },
            { view: "activity", tail: true, limit: 60 },
          ]),
        );
        assert.instanceOf(error, Orchestrator.OrchestratorProjectionError);
        assert.equal(error.threadId, threadId);
        if (stage === "hydration") {
          assert.equal(error.cause, hydrationError);
          assert.equal(reads, 0);
        } else {
          assert.equal(error, projectionError);
          assert.equal(reads, 1);
        }
      }).pipe(Effect.provide(serviceLayer));
    }),
);

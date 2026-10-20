import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  type ApplicationStoredEvent,
  EventId,
  type ModelSelection,
  type OrchestrationV2AppThread,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";

import * as CommandReceiptStore from "../../orchestration-v2/CommandReceiptStore.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as LegacyV1ThreadImporter from "../../orchestration-v2/legacy/LegacyV1ThreadImporter.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as ProjectStore from "../../orchestration-v2/ProjectStore.ts";
import * as ProviderSessionManager from "../../orchestration-v2/ProviderSessionManager.ts";
import * as ThreadManagementService from "../../orchestration-v2/ThreadManagementService.ts";
import { encodeThreadHistoryCursor } from "../../orchestration-v2/threadHistoryPaging.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as OrchestrationEventStore from "../../persistence/OrchestrationEventStore.ts";
import * as ProjectEnrichmentService from "../../project/ProjectEnrichmentService.ts";
import * as ProjectService from "../../project/ProjectService.ts";
import * as ServerSettings from "../../serverSettings.ts";
import { subscribeOrchestrationV2Shell } from "../../ws.ts";

// Thread opens, history pages and live shell rows must not queue behind the
// SQLite writer. Each test holds the writer in an open transaction, as an
// ingestion commit does, requires the reads to finish within a wall-clock
// bound, then compares them with the same reads on the writer.

const modelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5.4",
} satisfies ModelSelection;
const projectId = ProjectId.make("project:foreground-reads");
const noImport = { importedThreadCount: 0, importedMessageCount: 0 };
// The two oldest messages are not shell previews, so hydration imports them.
const hydratedImport = { importedThreadCount: 1, importedMessageCount: 2 };

const bound = "5 seconds";

/**
 * Fails reads still queued behind the held writer after the bound, not at the
 * test timeout. SqlClient takes a transaction's connection uninterruptibly and
 * a timeout waits for what it interrupts, so the writer is released first.
 */
const withinBound =
  <E2>(release: Effect.Effect<void, E2>) =>
  <A, E, R>(reads: Effect.Effect<A, E, R>) =>
    Effect.gen(function* () {
      const fiber = yield* Effect.forkChild(reads);
      const result = yield* Fiber.join(fiber).pipe(Effect.timeoutOption(bound));
      if (Option.isSome(result)) return result.value;
      yield* release;
      yield* Fiber.interrupt(fiber);
      return yield* Effect.die(new Error(`Reads still queued behind the writer after ${bound}`));
    });

// The reader worker needs a file database. Nesting it keeps the native-state
// checks inside the scoped directory, which is removed when the suite closes.
const layerDatabase = Layer.unwrap(
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fileSystem.makeTempDirectoryScoped({
      prefix: "t3-foreground-reads-",
    });
    return SqlitePersistence.layerFromPath(path.join(directory, "userdata", "state.sqlite"));
  }),
);
const layerImporter = LegacyV1ThreadImporter.layer.pipe(
  Layer.provideMerge(EventSink.layer),
  Layer.provideMerge(Layer.mergeAll(EventStore.layer, ProjectionStore.layer)),
  Layer.provideMerge(layerDatabase),
);

const asOrchestratorRead = <A, E>(threadId: ThreadId, read: Effect.Effect<A, E>) =>
  read.pipe(
    Effect.mapError((cause) => new Orchestrator.OrchestratorProjectionError({ threadId, cause })),
  );

// The orchestrator's thread reads are the projection store's reads with mapped
// errors, so the service wrappers run the real SQL on the file database.
const layerOrchestratorReads = Layer.unwrap(
  Effect.gen(function* () {
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    return Layer.mock(Orchestrator.OrchestratorV2)({
      getThreadSnapshot: (threadId) =>
        asOrchestratorRead(threadId, projections.getThreadSnapshot(threadId)),
      getThreadSnapshotWindow: (threadId, options) =>
        asOrchestratorRead(threadId, projections.getThreadSnapshotWindow(threadId, options)),
      getThreadHistoryPage: (threadId, cursor, throughEntryId, conversationOnly) =>
        asOrchestratorRead(
          threadId,
          projections.getThreadHistoryPage(threadId, cursor, throughEntryId, conversationOnly),
        ),
    });
  }),
);
const layerSuite = ThreadManagementService.layerWithLegacyImporter.pipe(
  Layer.provide(
    Layer.mergeAll(
      layerOrchestratorReads,
      Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({}),
      ServerSettings.layerTest(),
      CommandReceiptStore.layer,
    ),
  ),
  Layer.provideMerge(layerImporter),
  Layer.provide(NodeServices.layer),
);

/**
 * Opens a write transaction and keeps it open until the returned effect
 * releases it or the test scope closes. The reader starts first, so the bound
 * covers reads rather than the worker's startup.
 */
const holdWriter = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`SELECT 1`.pipe(NodeSqliteClient.readOnly);
  const held = yield* Deferred.make<void>();
  const release = yield* Deferred.make<void>();
  const writer = yield* Effect.forkScoped(
    sql.withTransaction(
      Deferred.succeed(held, undefined).pipe(Effect.andThen(Deferred.await(release))),
    ),
  );
  yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined));
  yield* Deferred.await(held);
  return Deferred.succeed(release, undefined).pipe(Effect.andThen(Fiber.join(writer)));
});

const legacyMessageIds = (threadId: ThreadId) =>
  [1, 2, 3, 4].map((ordinal) => `${threadId}:message:${ordinal}`);

const seedLegacyThread = (threadId: ThreadId) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`
      INSERT INTO projection_threads (
        thread_id,
        project_id,
        title,
        model_selection_json,
        created_at,
        updated_at
      ) VALUES (
        ${threadId},
        ${projectId},
        'Migrated conversation',
        '{"instanceId":"codex","model":"gpt-5.4"}',
        '2026-01-01T00:00:00.000Z',
        '2026-01-04T00:00:00.000Z'
      )
    `;
    yield* Effect.forEach(
      legacyMessageIds(threadId),
      (messageId, index) =>
        sql`
          INSERT INTO projection_thread_messages (
            message_id,
            thread_id,
            role,
            text,
            attachments_json,
            is_streaming,
            created_at,
            updated_at
          ) VALUES (
            ${messageId},
            ${threadId},
            ${index % 2 === 0 ? "user" : "assistant"},
            ${`Message ${index + 1}`},
            '[]',
            0,
            ${`2026-01-0${index + 1}T01:00:00.000Z`},
            ${`2026-01-0${index + 1}T01:00:00.000Z`}
          )
        `,
      { discard: true },
    );
  });

const createNativeThread = (threadId: ThreadId) =>
  Effect.gen(function* () {
    const eventSink = yield* EventSink.EventSinkV2;
    const now = yield* DateTime.now;
    const thread: OrchestrationV2AppThread = {
      createdBy: "user",
      creationSource: "web",
      id: threadId,
      projectId,
      title: "Native thread",
      providerInstanceId: modelSelection.instanceId,
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      branchPullRequest: null,
      activeOrderKey: null,
      activeProviderThreadId: null,
      lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
      forkedFrom: null,
      createdAt: now,
      updatedAt: now,
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      lastVisitedAt: null,
      deletedAt: null,
    };
    yield* eventSink.write({
      events: [
        {
          id: EventId.make(`created:${threadId}`),
          type: "thread.created",
          threadId,
          occurredAt: now,
          payload: thread,
        },
      ],
    });
  });

const transcriptImportedAt = (threadId: ThreadId) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql<{ readonly transcript_imported_at: string | null }>`
      SELECT transcript_imported_at
      FROM orchestration_v2_legacy_imports
      WHERE thread_id = ${threadId}
    `;
    return rows[0]?.transcript_imported_at;
  });

const importedMessageEventCount = (threadId: ThreadId) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql<{ readonly count: number }>`
      SELECT COUNT(*) AS count
      FROM orchestration_events
      WHERE application_event_version = 2
        AND aggregate_kind = 'thread'
        AND stream_id = ${threadId}
        AND event_id LIKE 'migration:v1:message:%'
    `;
    return rows[0]?.count;
  });

// Unsupported fake calls fail closed.
function boundedService<T extends object>(methods: Partial<T>): T {
  return new Proxy(methods, {
    get(target, key) {
      if (!(key in target)) throw new Error(`Unexpected service access: ${String(key)}`);
      return Reflect.get(target, key);
    },
  }) as T;
}

it.layer(layerSuite, { excludeTestServices: true })("foreground reads", (it) => {
  it.effect("opens and pages threads on the reader while the writer is held", () =>
    Effect.gen(function* () {
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
      const threads = yield* ThreadManagementService.ThreadManagementService;
      const legacyId = ThreadId.make("thread:foreground-reads:legacy");
      const nativeId = ThreadId.make("thread:foreground-reads:native");
      yield* seedLegacyThread(legacyId);
      yield* importer.reconcileShells;
      yield* createNativeThread(nativeId);

      // The first open hydrates the transcript on the writer; the snapshot
      // read on the reader afterwards sees every committed message.
      const hydrated = yield* threads.getThreadSnapshot(legacyId);
      assert.sameMembers(
        hydrated.projection.messages.map((message) => message.id),
        legacyMessageIds(legacyId),
      );
      assert.isString(yield* transcriptImportedAt(legacyId));
      const windowOptions = { rowLimit: 2, userTurnLimit: 1 };
      const latest = hydrated.projection.visibleTurnItems.at(-1)!;
      const cursor = encodeThreadHistoryCursor({
        snapshotSequence: hydrated.snapshotSequence,
        sourceThreadId: latest.sourceThreadId,
        sourceItemId: latest.sourceItemId,
        position: latest.position,
      });

      const release = yield* holdWriter;
      const writerSnapshot = yield* Effect.forkChild(projections.getThreadSnapshot(legacyId));
      const held = yield* Effect.all({
        snapshot: threads.getThreadSnapshot(legacyId),
        window: threads.getThreadSnapshotWindow(legacyId, windowOptions),
        page: threads.getThreadHistoryPage(legacyId, cursor),
        native: threads.getThreadSnapshot(nativeId),
      }).pipe(withinBound(release));
      // The same read on the writer is still waiting, so the writer stayed held.
      assert.isUndefined(writerSnapshot.pollUnsafe());
      yield* release;

      assert.deepEqual(held.snapshot, yield* Fiber.join(writerSnapshot));
      assert.isAbove(held.snapshot.snapshotSequence, 0);
      assert.deepEqual(
        held.window,
        yield* projections.getThreadSnapshotWindow(legacyId, windowOptions),
      );
      assert.isNotEmpty(held.page.items);
      assert.deepEqual(held.page, yield* projections.getThreadHistoryPage(legacyId, cursor));
      assert.deepEqual(held.native, yield* projections.getThreadSnapshot(nativeId));
    }),
  );

  it.effect("checks migrated transcripts on the reader and imports them on the writer", () =>
    Effect.gen(function* () {
      const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
      const hydratedId = ThreadId.make("thread:foreground-reads:hydrated");
      const pendingId = ThreadId.make("thread:foreground-reads:pending");
      const laterId = ThreadId.make("thread:foreground-reads:imported-later");
      yield* seedLegacyThread(hydratedId);
      yield* seedLegacyThread(pendingId);
      yield* importer.reconcileShells;
      assert.deepEqual(yield* importer.ensureTranscript(hydratedId), hydratedImport);
      assert.isString(yield* transcriptImportedAt(hydratedId));
      assert.equal(yield* importedMessageEventCount(hydratedId), 4);
      // A restarted server has no confirmations cached, so the reader confirms
      // it. A local build keeps the suite's memoized importer out.
      const restartedCheck = Effect.service(LegacyV1ThreadImporter.LegacyV1ThreadImporter).pipe(
        Effect.flatMap((restarted) => restarted.ensureTranscript(hydratedId)),
        Effect.provide(LegacyV1ThreadImporter.layer, { local: true }),
      );

      const release = yield* holdWriter;
      const pendingImport = yield* Effect.forkChild(importer.ensureTranscript(pendingId));
      const checked = yield* Effect.all([importer.ensureTranscript(laterId), restartedCheck]).pipe(
        withinBound(release),
      );
      // The pending import's locked recheck and writes still wait for the writer.
      assert.isUndefined(pendingImport.pollUnsafe());
      yield* release;

      assert.deepEqual(checked, [noImport, noImport]);
      assert.deepEqual(yield* Fiber.join(pendingImport), hydratedImport);
      assert.isString(yield* transcriptImportedAt(pendingId));
      assert.equal(yield* importedMessageEventCount(pendingId), 4);
      assert.deepEqual(yield* importer.ensureTranscript(pendingId), noImport);

      // The earlier missing row was not cached, so a later import still hydrates.
      yield* seedLegacyThread(laterId);
      yield* importer.reconcileShells;
      assert.deepEqual(yield* importer.ensureTranscript(laterId), hydratedImport);
    }),
  );

  it.effect("reads live shell rows on the reader while the writer is held", () =>
    Effect.gen(function* () {
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const threadId = ThreadId.make("thread:foreground-reads:shell");
      yield* createNativeThread(threadId);
      const eventBus = yield* PubSub.unbounded<ApplicationStoredEvent>();
      const enrichmentChanges =
        yield* PubSub.unbounded<ProjectEnrichmentService.ProjectEnrichmentChange>();
      const liveReady = yield* Deferred.make<void>();
      const live = Stream.unwrap(
        Effect.gen(function* () {
          const subscription = yield* PubSub.subscribe(eventBus);
          yield* Deferred.succeed(liveReady, undefined);
          return Stream.fromSubscription(subscription);
        }),
      );
      const client = Effect.scoped(
        Effect.gen(function* () {
          const stream = yield* subscribeOrchestrationV2Shell({});
          return Array.from(yield* Stream.runCollect(stream.pipe(Stream.take(2))));
        }),
      ).pipe(
        Effect.provideService(
          ThreadManagementService.ThreadManagementService,
          boundedService<ThreadManagementService.ThreadManagementService["Service"]>({
            readShellSnapshot: () =>
              Effect.succeed(
                Effect.succeed({
                  schemaVersion: 1,
                  snapshotSequence: 0,
                  threads: [],
                  archivedThreads: [],
                }),
              ),
            getThreadShell: (id) => asOrchestratorRead(id, projections.getThreadShell(id)),
          }),
        ),
        Effect.provideService(
          OrchestrationEventStore.OrchestrationEventStore,
          boundedService<OrchestrationEventStore.OrchestrationEventStore["Service"]>({
            latestApplicationSequence: Effect.succeed(0),
            streamProjectedApplicationEvents: ({ project }) => live.pipe(Stream.map(project)),
          }),
        ),
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
            subscribeChanges: PubSub.subscribe(enrichmentChanges),
          }),
        ),
        Effect.onError((cause) => Deferred.die(liveReady, cause)),
      );
      const subscriber = yield* Effect.forkChild(client);
      yield* Deferred.await(liveReady).pipe(Effect.timeout(bound));

      const release = yield* holdWriter;
      const writerShell = yield* Effect.forkChild(projections.getThreadShell(threadId));
      // A full batch flushes the shell window without waiting for its timer.
      const events = Array.from({ length: 512 }, (_, index) => ({
        sequence: index + 1,
        event: { threadId },
      })) as ApplicationStoredEvent[];
      yield* PubSub.publishAll(eventBus, events);
      const items = yield* Fiber.join(subscriber).pipe(withinBound(release));
      assert.isUndefined(writerShell.pollUnsafe());
      yield* release;

      assert.equal(items[0]?.kind, "snapshot");
      assert.deepEqual(items[1], {
        kind: "thread.updated",
        sequence: 512,
        location: "active",
        thread: (yield* Fiber.join(writerShell))!,
      });
    }),
  );
});

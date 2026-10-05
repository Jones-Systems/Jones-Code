import { assert, it } from "@effect/vitest";
import { EventId, ThreadId, type OrchestrationV2StoredEvent } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Tracer from "effect/Tracer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import { listLinkedPullRequestThreads } from "../../pullRequest/linkedThreads.ts";
import * as EventSink from "../EventSink.ts";
import * as EventStore from "../EventStore.ts";
import * as LegacyV1ThreadImporter from "./LegacyV1ThreadImporter.ts";
import * as ProjectionMaintenance from "../ProjectionMaintenance.ts";
import * as ProjectionStore from "../ProjectionStore.ts";
import { makeCommitTransaction } from "../CommitTransaction.ts";

const databaseLayer = SqlitePersistenceMemory;
const eventStoreProvided = EventStore.layer.pipe(Layer.provideMerge(databaseLayer));
const projectionStoreProvided = ProjectionStore.layer.pipe(Layer.provideMerge(databaseLayer));
const storesProvided = Layer.mergeAll(databaseLayer, eventStoreProvided, projectionStoreProvided);
const eventSinkProvided = EventSink.layer.pipe(Layer.provide(storesProvided));
const importerProvided = LegacyV1ThreadImporter.layer.pipe(
  Layer.provide(Layer.mergeAll(storesProvided, eventSinkProvided)),
);
const projectionMaintenanceProvided = ProjectionMaintenance.layer.pipe(
  Layer.provide(storesProvided),
);
const TestLayer = Layer.mergeAll(
  storesProvided,
  eventSinkProvided,
  importerProvided,
  projectionMaintenanceProvided,
);

it.layer(TestLayer)("LegacyV1ThreadImporter", (it) => {
  it.effect("uses the created-thread index for startup migration checks", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
      const statements: string[] = [];
      const tracer = Tracer.make({
        span(options) {
          const span = new Tracer.NativeSpan(options);
          const end = span.end.bind(span);
          span.end = (endTime, exit) => {
            end(endTime, exit);
            const query = span.attributes.get("db.query.text");
            if (
              typeof query === "string" &&
              query.includes("FROM projection_threads AS thread") &&
              query.includes("FROM orchestration_events AS event")
            ) {
              statements.push(query);
            }
          };
          return span;
        },
      });

      assert.equal(yield* importer.pendingThreadCount.pipe(Effect.withTracer(tracer)), 0);
      assert.deepStrictEqual(yield* importer.reconcileShells.pipe(Effect.withTracer(tracer)), {
        importedThreadCount: 0,
        importedMessageCount: 0,
      });
      assert.lengthOf(statements, 2);
      for (const statement of statements) {
        const plan = yield* sql.unsafe<{ readonly detail: string }>(
          `EXPLAIN QUERY PLAN ${statement}`,
        );
        assert.match(
          plan.map((row) => row.detail).join("\n"),
          /SEARCH event USING INDEX orchestration_events_v2_created_threads_idx \(stream_id=\?\)/,
        );
      }
    }),
  );

  it.effect("imports lightweight shells, hydrates transcripts, and remains idempotent", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
      const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const eventSink = yield* EventSink.EventSinkV2;
      const threadId = ThreadId.make("thread:legacy-import");

      yield* sql`
        INSERT INTO projection_projects (
          project_id,
          title,
          workspace_root,
          default_model_selection_json,
          scripts_json,
          created_at,
          updated_at,
          deleted_at
        ) VALUES (
          'project:legacy-import',
          'Legacy project',
          '/tmp/legacy-project',
          '{"instanceId":"codex","model":"gpt-5.4"}',
          '[]',
          '2026-01-01T00:00:00.000Z',
          '2026-01-04T00:00:00.000Z',
          NULL
        )
      `;
      yield* sql`
        INSERT INTO projection_threads (
          thread_id,
          project_id,
          title,
          model_selection_json,
          runtime_mode,
          interaction_mode,
          branch,
          worktree_path,
          latest_turn_id,
          created_at,
          updated_at,
          archived_at,
          settled_override,
          settled_at,
          unsettled_at,
          snoozed_until,
          snoozed_at,
          pinned_at,
          pin_order_key,
          linked_pull_request_json,
          deleted_at
        ) VALUES (
          ${threadId},
          'project:legacy-import',
          'Migrated conversation',
          '{"instanceId":"codex","model":"gpt-5.4"}',
          'full-access',
          'default',
          ' main ',
          ' /tmp/legacy-project ',
          NULL,
          '2026-01-01T00:00:00.000Z',
          '2026-01-04T00:00:00.000Z',
          NULL,
          NULL,
          NULL,
          '2026-01-03T12:00:00.000Z',
          '2026-02-01T00:00:00.000Z',
          '2026-01-04T00:00:00.000Z',
          '2026-01-02T00:00:00.000Z',
          'm',
          '{"projectId":"project:legacy-import","repository":"pingdotgg/t3code","number":9000,"url":"https://github.com/pingdotgg/t3code/pull/9000"}',
          NULL
        )
      `;
      yield* sql`
        INSERT INTO projection_thread_messages (
          message_id,
          thread_id,
          turn_id,
          role,
          text,
          attachments_json,
          is_streaming,
          created_at,
          updated_at
        ) VALUES
          (
            'message:legacy:1',
            ${threadId},
            NULL,
            'user',
            'First question',
            '[]',
            0,
            '2026-01-01T01:00:00.000Z',
            '2026-01-01T01:00:00.000Z'
          ),
          (
            'message:legacy:2',
            ${threadId},
            NULL,
            'assistant',
            'First answer',
            '[]',
            0,
            '2026-01-02T01:00:00.000Z',
            '2026-01-02T01:00:00.000Z'
          ),
          (
            'message:legacy:3',
            ${threadId},
            NULL,
            'user',
            'Follow-up question',
            '[]',
            0,
            '2026-01-03T01:00:00.000Z',
            '2026-01-03T01:00:00.000Z'
          ),
          (
            'message:legacy:4',
            ${threadId},
            NULL,
            'assistant',
            'Partial answer',
            '[]',
            1,
            '2026-01-04T01:00:00.000Z',
            '2026-01-04T01:00:00.000Z'
        )
      `;

      yield* sql`
        INSERT INTO projection_thread_pull_requests (
          thread_id, host, repository, number, url, source, linked_at, snapshot_json
        ) VALUES
          (${threadId}, 'github.com', 'pingdotgg/t3code', 9002,
            'https://github.com/pingdotgg/t3code/pull/9002', 'created',
            '2026-01-03T00:00:00.000Z',
            '{"state":"open","title":"Second PR","headBranch":"feature-two","baseBranch":"main","isDraft":false,"updatedAt":"2026-01-03T00:00:00.000Z","syncedAt":"2026-01-03T00:00:00.000Z"}'),
          (${threadId}, 'github.com', 'pingdotgg/t3code', 9003,
            'https://github.com/pingdotgg/t3code/pull/9003', 'manual',
            '2026-01-04T00:00:00.000Z', NULL)
      `;

      assert.equal(yield* importer.pendingThreadCount, 1);
      const shellImport = yield* importer.reconcileShells;
      assert.equal(yield* importer.pendingThreadCount, 1);
      assert.deepStrictEqual(shellImport, {
        importedThreadCount: 1,
        importedMessageCount: 2,
      });
      const shellEventCount = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count
        FROM orchestration_events
        WHERE application_event_version = 2
          AND aggregate_kind = 'thread'
          AND stream_id = ${threadId}
      `;
      assert.equal(shellEventCount[0]?.count, 6);

      assert.isTrue((yield* maintenance.verify).valid);
      const shellProjection = yield* projections.getThreadProjection(threadId);
      assert.equal(shellProjection.thread.historyOrigin, "v1_import");
      assert.equal(shellProjection.thread.branch, "main");
      assert.equal(shellProjection.thread.worktreePath, "/tmp/legacy-project");
      assert.deepEqual(
        shellProjection.thread.pinnedAt,
        DateTime.makeUnsafe("2026-01-02T00:00:00.000Z"),
      );
      assert.equal(shellProjection.thread.pinOrderKey, "m");
      assert.deepEqual(
        shellProjection.thread.snoozedUntil,
        DateTime.makeUnsafe("2026-02-01T00:00:00.000Z"),
      );
      assert.deepEqual(
        shellProjection.thread.unsettledAt,
        DateTime.makeUnsafe("2026-01-03T12:00:00.000Z"),
      );
      assert.equal(shellProjection.thread.linkedPullRequest?.number, 9000);
      assert.deepStrictEqual(
        (shellProjection.thread.pullRequests ?? []).map((link) => link.number),
        [9002, 9003, 9000],
      );
      assert.equal(shellProjection.thread.pullRequests?.[0]?.snapshot?.title, "Second PR");
      assert.deepStrictEqual(
        (yield* listLinkedPullRequestThreads({
          host: "github.com",
          repository: "pingdotgg/t3code",
          number: 9002,
        })).threads.map((thread) => thread.id),
        [threadId],
      );
      const shellSnapshot = yield* projections.getShellSnapshot();
      assert.equal(
        shellSnapshot.threads.find((thread) => thread.id === threadId)?.historyOrigin,
        "v1_import",
      );
      assert.deepStrictEqual(
        shellProjection.messages.map((message) => message.id),
        ["message:legacy:3", "message:legacy:4"],
      );

      const renamedAt = DateTime.makeUnsafe("2026-01-05T00:00:00.000Z");
      yield* eventSink.write({
        events: [
          {
            id: EventId.make("event:legacy-import:metadata-after-shell"),
            type: "thread.metadata-updated",
            threadId,
            providerInstanceId: shellProjection.thread.providerInstanceId,
            occurredAt: renamedAt,
            payload: {
              ...shellProjection.thread,
              title: "Renamed after shell import",
              runtimeMode: "approval-required",
              interactionMode: "plan",
              archivedAt: renamedAt,
              settledOverride: "settled",
              settledAt: renamedAt,
              updatedAt: renamedAt,
            },
          },
        ],
      });

      const transcriptImport = yield* importer.ensureTranscript(threadId);
      assert.equal(yield* importer.pendingThreadCount, 0);
      assert.deepStrictEqual(transcriptImport, {
        importedThreadCount: 1,
        importedMessageCount: 2,
      });
      const projection = yield* projections.getThreadProjection(threadId);
      assert.equal(projection.thread.title, "Renamed after shell import");
      assert.equal(projection.thread.runtimeMode, "approval-required");
      assert.equal(projection.thread.interactionMode, "plan");
      assert.deepEqual(projection.thread.archivedAt, renamedAt);
      assert.equal(projection.thread.settledOverride, "settled");
      assert.deepEqual(projection.thread.settledAt, renamedAt);
      assert.deepStrictEqual(
        projection.messages.map((message) => message.id),
        ["message:legacy:1", "message:legacy:2", "message:legacy:3", "message:legacy:4"],
      );
      assert.deepStrictEqual(
        projection.turnItems
          .filter(
            (
              item,
            ): item is Extract<
              (typeof projection.turnItems)[number],
              { readonly type: "user_message" | "assistant_message" }
            > => item.type === "user_message" || item.type === "assistant_message",
          )
          .map((item) => [item.messageId, item.ordinal, item.status]),
        [
          ["message:legacy:1", 1, "completed"],
          ["message:legacy:2", 2, "completed"],
          ["message:legacy:3", 3, "completed"],
          ["message:legacy:4", 4, "interrupted"],
        ],
      );

      yield* eventSink.write({
        events: [
          {
            id: EventId.make("event:legacy-import:explicit-unpin"),
            type: "thread.unpinned",
            threadId,
            providerInstanceId: projection.thread.providerInstanceId,
            occurredAt: renamedAt,
            payload: { ...projection.thread, pinnedAt: null, pinOrderKey: null },
          },
        ],
      });
      yield* sql`
        UPDATE orchestration_v2_projection_threads
        SET payload_json = json_remove(
          payload_json,
          '$.snoozedUntil',
          '$.snoozedAt',
          '$.unsettledAt',
          '$.linkedPullRequest',
          '$.pullRequests'
        )
        WHERE thread_id = ${threadId}
      `;
      assert.deepStrictEqual(yield* importer.reconcileShells, {
        importedThreadCount: 1,
        importedMessageCount: 0,
      });
      const repaired = yield* projections.getThreadProjection(threadId);
      assert.isNull(repaired.thread.pinnedAt);
      assert.isNull(repaired.thread.pinOrderKey);
      assert.deepEqual(
        repaired.thread.snoozedUntil,
        DateTime.makeUnsafe("2026-02-01T00:00:00.000Z"),
      );
      assert.deepEqual(
        repaired.thread.unsettledAt,
        DateTime.makeUnsafe("2026-01-03T12:00:00.000Z"),
      );
      assert.equal(repaired.thread.linkedPullRequest?.number, 9000);
      assert.deepStrictEqual(
        (repaired.thread.pullRequests ?? []).map((link) => link.number),
        [9002, 9003, 9000],
      );
      assert.equal(repaired.thread.pullRequests?.[0]?.snapshot?.title, "Second PR");
      const eventCountBeforeRetry = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count
        FROM orchestration_events
        WHERE application_event_version = 2
          AND aggregate_kind = 'thread'
          AND stream_id = ${threadId}
      `;
      assert.deepStrictEqual(yield* importer.reconcileShells, {
        importedThreadCount: 0,
        importedMessageCount: 0,
      });
      assert.deepStrictEqual(yield* importer.ensureTranscript(threadId), {
        importedThreadCount: 0,
        importedMessageCount: 0,
      });
      const eventCountAfterRetry = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count
        FROM orchestration_events
        WHERE application_event_version = 2
          AND aggregate_kind = 'thread'
          AND stream_id = ${threadId}
      `;
      assert.equal(eventCountAfterRetry[0]?.count, eventCountBeforeRetry[0]?.count);
    }),
  );

  it.effect("repairs newly added metadata after an earlier metadata repair", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
      const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const eventSink = yield* EventSink.EventSinkV2;
      const threadId = ThreadId.make("thread:legacy-metadata-upgrade");
      const previousRepairId = EventId.make(`migration:v1:thread:${threadId}:metadata-repair`);

      yield* sql`
        INSERT INTO projection_projects (
          project_id, title, workspace_root, scripts_json, created_at, updated_at
        ) VALUES (
          'project:legacy-metadata-upgrade',
          'Legacy project',
          '/tmp/legacy-metadata-upgrade',
          '[]',
          '2026-01-01T00:00:00.000Z',
          '2026-01-01T00:00:00.000Z'
        )
      `;
      yield* sql`
        INSERT INTO projection_threads (
          thread_id, project_id, title, model_selection_json, runtime_mode,
          interaction_mode, created_at, updated_at, pinned_at, pin_order_key,
          linked_pull_request_json, branch_pull_request_json, active_order_key
        ) VALUES (
          ${threadId},
          'project:legacy-metadata-upgrade',
          'Original v1 title',
          '{"instanceId":"codex","model":"gpt-5.4"}',
          'full-access',
          'default',
          '2026-01-01T00:00:00.000Z',
          '2026-01-01T00:00:00.000Z',
          '2026-01-02T00:00:00.000Z',
          'm',
          '{"projectId":"project:legacy-metadata-upgrade","repository":"pingdotgg/t3code","number":9000,"url":"https://github.com/pingdotgg/t3code/pull/9000"}',
          '{"projectId":"project:legacy-metadata-upgrade","repository":"pingdotgg/t3code","number":9001,"url":"https://github.com/pingdotgg/t3code/pull/9001"}',
          'az'
        )
      `;
      yield* importer.reconcileShells;
      yield* maintenance.rebuild;
      const shellProjection = yield* projections.getThreadProjection(threadId);
      assert.deepStrictEqual(
        (shellProjection.thread.pullRequests ?? []).map((link) => link.number),
        [9000],
      );
      const previousRepairThread = {
        ...shellProjection.thread,
        title: "Renamed in v2",
        pinnedAt: null,
        pinOrderKey: null,
        linkedPullRequest: null,
        pullRequests: [],
      };
      delete previousRepairThread.branchPullRequest;
      delete previousRepairThread.activeOrderKey;
      yield* eventSink.write({
        events: [
          {
            id: previousRepairId,
            type: "thread.metadata-updated",
            threadId,
            providerInstanceId: previousRepairThread.providerInstanceId,
            occurredAt: DateTime.makeUnsafe("2026-01-03T00:00:00.000Z"),
            payload: previousRepairThread,
          },
        ],
      });

      assert.deepStrictEqual(yield* importer.reconcileShells, {
        importedThreadCount: 1,
        importedMessageCount: 0,
      });
      const repaired = yield* projections.getThreadProjection(threadId);
      assert.equal(repaired.thread.title, "Renamed in v2");
      assert.isNull(repaired.thread.pinnedAt);
      assert.isNull(repaired.thread.pinOrderKey);
      assert.isNull(repaired.thread.linkedPullRequest);
      assert.deepStrictEqual(repaired.thread.pullRequests, []);
      assert.equal(repaired.thread.branchPullRequest?.number, 9001);
      assert.equal(repaired.thread.activeOrderKey, "az");

      const eventsBeforeRetry = yield* sql<{ readonly event_id: string }>`
        SELECT event_id
        FROM orchestration_events
        WHERE application_event_version = 2 AND stream_id = ${threadId}
        ORDER BY sequence
      `;
      assert.equal(eventsBeforeRetry.length, 4);
      assert.isTrue(eventsBeforeRetry.some((event) => event.event_id === previousRepairId));
      assert.deepStrictEqual(yield* importer.reconcileShells, {
        importedThreadCount: 0,
        importedMessageCount: 0,
      });

      assert.isTrue((yield* maintenance.rebuild).valid);
      const replayed = yield* projections.getThreadProjection(threadId);
      assert.deepStrictEqual(replayed.thread, repaired.thread);
      assert.deepStrictEqual(
        yield* Effect.gen(function* () {
          const restartedImporter = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
          return yield* restartedImporter.reconcileShells;
        }).pipe(Effect.provide(LegacyV1ThreadImporter.layer)),
        { importedThreadCount: 0, importedMessageCount: 0 },
      );
      const eventsAfterRestart = yield* sql<{ readonly event_id: string }>`
        SELECT event_id
        FROM orchestration_events
        WHERE application_event_version = 2 AND stream_id = ${threadId}
        ORDER BY sequence
      `;
      assert.deepStrictEqual(eventsAfterRestart, eventsBeforeRetry);
    }),
  );
});

const seedCommitShell = (threadId: ThreadId) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const projectId = `project:${threadId}`;
    yield* sql`INSERT INTO projection_projects
      (project_id, title, workspace_root, default_model_selection_json, scripts_json, created_at, updated_at)
      VALUES (${projectId}, 'Synthetic legacy carrier', '/synthetic/legacy',
        '{"instanceId":"codex","model":"gpt-5.4"}', '[]',
        '2026-01-01T00:00:00.000Z', '2026-01-01T00:00:00.000Z')`;
    yield* sql`INSERT INTO projection_threads
      (thread_id, project_id, title, model_selection_json, runtime_mode, interaction_mode,
       created_at, updated_at)
      VALUES (${threadId}, ${projectId}, 'Original legacy shell',
        '{"instanceId":"codex","model":"gpt-5.4"}', 'full-access', 'default',
        '2026-01-01T00:00:00.000Z', '2026-01-01T00:00:00.000Z')`;
    yield* sql`INSERT INTO projection_thread_messages
      (message_id, thread_id, turn_id, role, text, attachments_json, is_streaming, created_at, updated_at)
      VALUES (${`message:${threadId}`}, ${threadId}, NULL, 'user', 'Retained original message', '[]', 0,
        '2026-01-01T00:00:00.000Z', '2026-01-01T00:00:00.000Z')`;
  });

const observeCommitImports = (barrierThreadId: ThreadId) =>
  Effect.gen(function* () {
    const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
    const sink = yield* EventSink.EventSinkV2;
    yield* seedCommitShell(barrierThreadId);
    yield* importer.reconcileShells;
    const readyId = `migration:v1:thread:${barrierThreadId}:shell`;
    const ready = yield* Deferred.make<void>();
    const observed: Array<OrchestrationV2StoredEvent> = [];
    const milestones = new Map<string, Deferred.Deferred<void>>([[readyId, ready]]);
    yield* sink.stream().pipe(
      Stream.runForEach((stored) =>
        Effect.gen(function* () {
          observed.push(stored);
          const milestone = milestones.get(stored.event.id);
          if (milestone !== undefined) yield* Deferred.succeed(milestone, undefined);
        }),
      ),
      Effect.forkScoped,
    );
    // The real stream subscribes before replay. Its final replayed shell event
    // proves the live observer is attached before the transaction under test.
    yield* Deferred.await(ready).pipe(Effect.timeout("2 seconds"));
    return {
      observed,
      awaitImport: (threadId: ThreadId) =>
        Effect.gen(function* () {
          const id = `migration:v1:thread:${threadId}:shell`;
          if (observed.some((item) => item.event.id === id)) return;
          const done = yield* Deferred.make<void>();
          milestones.set(id, done);
          if (observed.some((item) => item.event.id === id)) return;
          yield* Deferred.await(done).pipe(Effect.timeout("2 seconds"));
        }),
      barrier: Effect.gen(function* () {
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const projection = yield* projections.getThreadProjection(barrierThreadId);
        const id = EventId.make(`event:${barrierThreadId}:committed-barrier`);
        const done = yield* Deferred.make<void>();
        milestones.set(id, done);
        yield* sink.write({
          events: [
            {
              id,
              type: "thread.metadata-updated",
              threadId: barrierThreadId,
              providerInstanceId: projection.thread.providerInstanceId,
              occurredAt: yield* DateTime.now,
              payload: projection.thread,
            },
          ],
        });
        yield* Deferred.await(done).pipe(Effect.timeout("2 seconds"));
      }),
    };
  });

const readCommitImportState = (threadId: ThreadId) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    return {
      positions:
        yield* sql`SELECT * FROM orchestration_v2_turn_item_positions WHERE thread_id = ${threadId}`,
      events:
        yield* sql`SELECT * FROM orchestration_events WHERE application_event_version = 2 AND stream_id = ${threadId}`,
      projection: yield* sql`SELECT * FROM projection_threads WHERE thread_id = ${threadId}`,
      v2Projection:
        yield* sql`SELECT * FROM orchestration_v2_projection_threads WHERE thread_id = ${threadId}`,
      messages: yield* sql`SELECT * FROM projection_thread_messages WHERE thread_id = ${threadId}`,
      marker:
        yield* sql`SELECT * FROM orchestration_v2_legacy_imports WHERE thread_id = ${threadId}`,
    };
  });

it.live("nested shell import publishes only after its actual outer owning SQL commit", () =>
  Effect.gen(function* () {
    const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
    const owner = yield* makeCommitTransaction();
    const observer = yield* observeCommitImports(ThreadId.make("thread:import-owned-barrier"));
    const threadId = ThreadId.make("thread:import-owned-target");
    yield* seedCommitShell(threadId);
    yield* owner.withTransaction(
      Effect.gen(function* () {
        assert.deepStrictEqual(yield* importer.reconcileShells, {
          importedThreadCount: 1,
          importedMessageCount: 1,
        });
        const pending = yield* readCommitImportState(threadId);
        assert.lengthOf(pending.positions, 1);
        assert.lengthOf(pending.events, 4);
        assert.lengthOf(pending.marker, 1);
        assert.isFalse(observer.observed.some((item) => item.event.threadId === threadId));
      }),
    );
    yield* observer.awaitImport(threadId);
    assert.lengthOf(
      observer.observed.filter((item) => item.event.threadId === threadId),
      4,
    );
    const committed = yield* readCommitImportState(threadId);
    assert.deepStrictEqual(yield* importer.reconcileShells, {
      importedThreadCount: 0,
      importedMessageCount: 0,
    });
    assert.deepStrictEqual(yield* readCommitImportState(threadId), committed);
  }).pipe(Effect.provide(TestLayer)),
);

it.live(
  "a real import marker failure rolls back appended events, positions and projection without publication",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
      const observer = yield* observeCommitImports(ThreadId.make("thread:import-marker-barrier"));
      const threadId = ThreadId.make("thread:import-marker-target");
      yield* seedCommitShell(threadId);
      const before = yield* readCommitImportState(threadId);
      yield* sql`CREATE TRIGGER synthetic_import_marker_failure BEFORE INSERT ON orchestration_v2_legacy_imports
      WHEN NEW.thread_id = 'thread:import-marker-target'
      BEGIN SELECT CASE WHEN
        (SELECT COUNT(*) FROM orchestration_events WHERE application_event_version = 2 AND stream_id = NEW.thread_id) = 4
        AND (SELECT COUNT(*) FROM orchestration_v2_turn_item_positions WHERE thread_id = NEW.thread_id) = 1
        AND (SELECT COUNT(*) FROM orchestration_v2_projection_threads WHERE thread_id = NEW.thread_id) = 1
        THEN RAISE(ABORT, 'owned synthetic marker failure after event append')
        ELSE RAISE(ABORT, 'fixture did not reach original appended state') END; END`;
      const result = yield* Effect.exit(importer.reconcileShells);
      assert.isTrue(Exit.isFailure(result));
      if (Exit.isFailure(result))
        assert.match(
          Cause.pretty(result.cause),
          /owned synthetic marker failure after event append/,
        );
      assert.deepStrictEqual(yield* readCommitImportState(threadId), before);
      yield* observer.barrier;
      assert.isFalse(observer.observed.some((item) => item.event.threadId === threadId));
      yield* sql`DROP TRIGGER synthetic_import_marker_failure`;
      assert.deepStrictEqual(yield* importer.reconcileShells, {
        importedThreadCount: 1,
        importedMessageCount: 1,
      });
      yield* observer.awaitImport(threadId);
      assert.lengthOf(
        observer.observed.filter((item) => item.event.threadId === threadId),
        4,
      );
    }).pipe(Effect.provide(TestLayer)),
);

it.live(
  "an outer owned failure rolls back a completed inner import and discards all notifications",
  () =>
    Effect.gen(function* () {
      const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
      const owner = yield* makeCommitTransaction();
      const observer = yield* observeCommitImports(ThreadId.make("thread:import-rollback-barrier"));
      const threadId = ThreadId.make("thread:import-rollback-target");
      yield* seedCommitShell(threadId);
      const before = yield* readCommitImportState(threadId);
      const result = yield* Effect.exit(
        owner.withTransaction(
          Effect.gen(function* () {
            yield* importer.reconcileShells;
            assert.lengthOf((yield* readCommitImportState(threadId)).events, 4);
            assert.isFalse(observer.observed.some((item) => item.event.threadId === threadId));
            return yield* Effect.fail("owned outer failure after completed inner import");
          }),
        ),
      );
      assert.isTrue(Exit.isFailure(result));
      assert.deepStrictEqual(yield* readCommitImportState(threadId), before);
      yield* observer.barrier;
      assert.isFalse(observer.observed.some((item) => item.event.threadId === threadId));
    }).pipe(Effect.provide(TestLayer)),
);

it.live(
  "a raw unowned outer SQL transaction still refuses shell import without mutation or publication",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
      const observer = yield* observeCommitImports(ThreadId.make("thread:import-raw-barrier"));
      const threadId = ThreadId.make("thread:import-raw-target");
      yield* seedCommitShell(threadId);
      const before = yield* readCommitImportState(threadId);
      const result = yield* Effect.exit(sql.withTransaction(importer.reconcileShells));
      assert.isTrue(Exit.isFailure(result));
      if (Exit.isFailure(result))
        assert.match(Cause.pretty(result.cause), /UnownedCommitTransactionError/);
      assert.deepStrictEqual(yield* readCommitImportState(threadId), before);
      yield* observer.barrier;
      assert.isFalse(observer.observed.some((item) => item.event.threadId === threadId));
    }).pipe(Effect.provide(TestLayer)),
);

const seedAuthenticLegacyInventorySource = (threadId: ThreadId) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* seedCommitShell(threadId);
    const createdAt = "2026-01-01T00:00:00.000Z";
    const legacyBirthJson = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown))({
      threadId,
      projectId: `project:${threadId}`,
      createdAt,
    });
    yield* sql`INSERT INTO orchestration_events
      (event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,actor_kind,payload_json,metadata_json,application_event_version)
      VALUES (${`event:${threadId}:legacy-birth`},'thread',${threadId},1,'thread.created',${createdAt},'user',
        ${legacyBirthJson},'{}',1)`;
    const sequence = (yield* sql<{
      readonly sequence: number;
    }>`SELECT max(sequence) AS sequence FROM orchestration_events WHERE application_event_version=1`)[0]!
      .sequence;
    for (const projector of [
      "projection.threads",
      "projection.thread-messages",
      "projection.thread-activities",
      "projection.thread-turns",
    ])
      yield* sql`INSERT INTO projection_state (projector,last_applied_sequence,updated_at)
        VALUES (${projector},${sequence},${createdAt}) ON CONFLICT(projector) DO UPDATE SET last_applied_sequence=excluded.last_applied_sequence,updated_at=excluded.updated_at`;
  });

it.live(
  "the real importer atomically appends its legacy inventory and publishes only after the enclosing owner commits",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
      const sink = yield* EventSink.EventSinkV2;
      const owner = yield* makeCommitTransaction();
      const observer = yield* observeCommitImports(
        ThreadId.make("thread:inventory-commit-barrier"),
      );
      const threadId = ThreadId.make("thread:inventory-commit-target");
      yield* seedAuthenticLegacyInventorySource(threadId);
      yield* owner.withTransaction(
        Effect.gen(function* () {
          assert.deepStrictEqual(yield* importer.reconcileShells, {
            importedThreadCount: 1,
            importedMessageCount: 1,
          });
          assert.lengthOf(
            yield* sql`SELECT * FROM orchestration_v2_imported_application_attachment_inventories WHERE thread_id=${threadId}`,
            1,
          );
          assert.lengthOf(
            yield* sql`SELECT * FROM orchestration_v2_imported_application_attachment_carriers`,
            1,
          );
          assert.isFalse(observer.observed.some((value) => value.event.threadId === threadId));
        }),
      );
      yield* observer.awaitImport(threadId);
      assert.lengthOf(
        observer.observed.filter((value) => value.event.threadId === threadId),
        4,
      );
      const readBirth = sink.readApplicationBirthRecord;
      const ensure = importer.ensureApplicationAttachmentInventory;
      if (readBirth === undefined || ensure === undefined)
        return yield* Effect.die("Authentic inventory owner missing");
      const birth = yield* readBirth(threadId);
      assert.isNotNull(birth);
      if (birth === null) return;
      const original = yield* ensure({ threadId, expectedBirth: birth });
      assert.equal(original.status, "complete");
      const headers =
        yield* sql`SELECT * FROM orchestration_v2_imported_application_attachment_inventories WHERE thread_id=${threadId}`;
      assert.deepStrictEqual(yield* importer.ensureTranscript(threadId), {
        importedThreadCount: 1,
        importedMessageCount: 0,
      });
      assert.deepStrictEqual(yield* importer.ensureTranscript(threadId), {
        importedThreadCount: 0,
        importedMessageCount: 0,
      });
      assert.deepStrictEqual(yield* ensure({ threadId, expectedBirth: birth }), original);
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM orchestration_v2_imported_application_attachment_inventories WHERE thread_id=${threadId}`,
        headers,
      );
    }).pipe(Effect.provide(Layer.fresh(TestLayer))),
);

it.live(
  "a real inventory carrier failure rolls back shell events, projection, marker, header and publication together",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
      const observer = yield* observeCommitImports(
        ThreadId.make("thread:inventory-failure-barrier"),
      );
      const threadId = ThreadId.make("thread:inventory-failure-target");
      yield* seedAuthenticLegacyInventorySource(threadId);
      const before = yield* readCommitImportState(threadId);
      yield* sql`CREATE TRIGGER synthetic_inventory_carrier_failure BEFORE INSERT ON orchestration_v2_imported_application_attachment_carriers
      BEGIN SELECT CASE WHEN
        (SELECT COUNT(*) FROM orchestration_v2_imported_application_attachment_inventories)=1
        AND (SELECT COUNT(*) FROM orchestration_v2_legacy_imports WHERE thread_id='thread:inventory-failure-target')=1
        AND (SELECT COUNT(*) FROM orchestration_events WHERE application_event_version=2 AND stream_id='thread:inventory-failure-target')=4
        THEN RAISE(ABORT,'owned synthetic inventory carrier failure after header and marker')
        ELSE RAISE(ABORT,'fixture missed actual inventory append') END; END`;
      const result = yield* Effect.exit(importer.reconcileShells);
      assert.isTrue(Exit.isFailure(result));
      if (Exit.isFailure(result))
        assert.match(
          Cause.pretty(result.cause),
          /owned synthetic inventory carrier failure after header and marker/,
        );
      assert.deepStrictEqual(yield* readCommitImportState(threadId), before);
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM orchestration_v2_imported_application_attachment_inventories`,
        [],
      );
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM orchestration_v2_imported_application_attachment_carriers`,
        [],
      );
      yield* observer.barrier;
      assert.isFalse(observer.observed.some((value) => value.event.threadId === threadId));
    }).pipe(Effect.provide(Layer.fresh(TestLayer))),
);

it.live(
  "outer owned transcript rollback discards its new adoption and cache confirmation so the original caller can retry",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
      const owner = yield* makeCommitTransaction();
      const observer = yield* observeCommitImports(
        ThreadId.make("thread:inventory-transcript-barrier"),
      );
      const threadId = ThreadId.make("thread:inventory-transcript-target");
      yield* seedAuthenticLegacyInventorySource(threadId);
      yield* importer.reconcileShells;
      yield* observer.awaitImport(threadId);
      yield* sql`INSERT INTO projection_thread_messages (message_id,thread_id,turn_id,role,text,attachments_json,is_streaming,created_at,updated_at)
      VALUES ('message:inventory:later',${threadId},NULL,'user','Later retained original','[]',0,'2026-01-02T00:00:00.000Z','2026-01-02T00:00:00.000Z')`;
      const before = yield* readCommitImportState(threadId);
      const inventories =
        yield* sql`SELECT * FROM orchestration_v2_imported_application_attachment_inventories WHERE thread_id=${threadId}`;
      const failure = yield* Effect.exit(
        owner.withTransaction(
          Effect.gen(function* () {
            assert.deepStrictEqual(yield* importer.ensureTranscript(threadId), {
              importedThreadCount: 1,
              importedMessageCount: 1,
            });
            assert.lengthOf(
              yield* sql`SELECT * FROM orchestration_v2_imported_application_attachment_inventories WHERE thread_id=${threadId}`,
              2,
            );
            assert.lengthOf(
              observer.observed.filter((value) => value.event.threadId === threadId),
              4,
            );
            return yield* Effect.fail("rollback after real transcript marker and inventory");
          }),
        ),
      );
      assert.isTrue(Exit.isFailure(failure));
      assert.deepStrictEqual(yield* readCommitImportState(threadId), before);
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM orchestration_v2_imported_application_attachment_inventories WHERE thread_id=${threadId}`,
        inventories,
      );
      yield* observer.barrier;
      assert.lengthOf(
        observer.observed.filter((value) => value.event.threadId === threadId),
        4,
      );
      assert.deepStrictEqual(yield* importer.ensureTranscript(threadId), {
        importedThreadCount: 1,
        importedMessageCount: 1,
      });
      const sink = yield* EventSink.EventSinkV2;
      const committed = yield* sink
        .stream({ threadId })
        .pipe(Stream.take(6), Stream.runCollect, Effect.timeout("2 seconds"));
      assert.lengthOf(committed, 6);
      assert.deepStrictEqual(yield* importer.ensureTranscript(threadId), {
        importedThreadCount: 0,
        importedMessageCount: 0,
      });
    }).pipe(Effect.provide(Layer.fresh(TestLayer))),
);

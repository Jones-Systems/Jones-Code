import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  EventId,
  type OrchestrationV2AppThread,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as SqlClient from "effect/sql/SqlClient";
import * as Statement from "effect/sql/Statement";

import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import type { RecordedLifecycleEvent } from "../../orchestration-v2/RecordedTypes.ts";
import * as OrchestrationEventStore from "../../persistence/OrchestrationEventStore.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as ThreadMetadataReads from "./threadMetadataReads.ts";

const now = DateTime.makeUnsafe("2026-10-01T00:00:00.000Z");
const projectId = ProjectId.make("metadata:project");
const otherProjectId = ProjectId.make("metadata:other-project");
const instanceId = ProviderInstanceId.make("codex");
const thread = (
  id: string,
  overrides: Partial<OrchestrationV2AppThread> = {},
): OrchestrationV2AppThread => ({
  id: ThreadId.make(id),
  projectId,
  title: id,
  providerInstanceId: instanceId,
  modelSelection: { instanceId, model: "synthetic" },
  createdBy: "user",
  creationSource: "web",
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: "old-branch",
  worktreePath: "/synthetic/worktree",
  activeProviderThreadId: null,
  lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: ThreadId.make(id) },
  forkedFrom: null,
  createdAt: now,
  updatedAt: now,
  archivedAt: null,
  deletedAt: null,
  settledAt: null,
  settledOverride: null,
  lastVisitedAt: null,
  ...overrides,
});
const commit = Effect.fn("threadMetadataReads.test.commit")(function* (
  payload: OrchestrationV2AppThread,
  type: "thread.created" | "thread.metadata-updated" = "thread.created",
) {
  const sql = yield* SqlClient.SqlClient;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const events = yield* OrchestrationEventStore.OrchestrationEventStore;
  const event: RecordedLifecycleEvent = {
    id: EventId.make(`${payload.id}:${payload.title}:${type}`),
    type,
    threadId: payload.id,
    occurredAt: now,
    payload,
  };
  return yield* sql.withTransaction(
    Effect.gen(function* () {
      const stored = yield* events.appendAgentEvents({ events: [event] });
      yield* projections.apply(event);
      return stored[0]!.sequence;
    }),
  );
});
const services = Layer.mergeAll(
  ProjectionStore.layer,
  OrchestrationEventStore.layer,
  ThreadMetadataReads.layer,
);
const memory = services.pipe(Layer.provideMerge(SqlitePersistence.layerMemory));

it.effect(
  "metadata filters, fields, watermark and active-before-archived ordering match the shell",
  () =>
    Effect.gen(function* () {
      const metadata = yield* ThreadMetadataReads.ThreadMetadataReads;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      assert.equal(yield* metadata.count, 0);
      assert.equal(yield* metadata.watermark, 0);
      const rows = [
        thread("metadata:a-archive", { archivedAt: now }),
        thread("metadata:b-active", { updatedAt: DateTime.add(now, { minutes: 1 }) }),
        thread("metadata:c-settled", { settledAt: now }),
        thread("metadata:d-override", { settledOverride: "settled" }),
        thread("metadata:e-other", { projectId: otherProjectId, worktreePath: "/other" }),
        thread("metadata:f-branch", { branch: "different" }),
        thread("metadata:g-deleted", { deletedAt: now }),
        thread("metadata:h-archive", {
          archivedAt: now,
          updatedAt: DateTime.add(now, { hours: -1 }),
        }),
      ];
      for (const row of rows) yield* commit(row);
      const queries: ReadonlyArray<ThreadMetadataReads.ThreadMetadataQuery> = [
        {},
        { location: "active" },
        { location: "archive" },
        { unsettledOnly: true },
        { projectId },
        { projectId: otherProjectId },
        { threadIds: [rows[0]!.id, rows[1]!.id, rows[6]!.id] },
        { threadIds: [] },
        { worktree: { path: "/synthetic/worktree", branch: "old-branch" } },
        {
          location: "active",
          unsettledOnly: true,
          projectId,
          threadIds: rows.map((row) => row.id),
          worktree: { path: "/synthetic/worktree", branch: "old-branch" },
        },
        { location: "archive", unsettledOnly: true, projectId },
      ];
      for (const query of queries) {
        const shell = yield* projections.getShellSnapshot({
          ...(query.projectId === undefined ? {} : { projectId: query.projectId }),
          ...(query.location === undefined ? {} : { location: query.location }),
          ...(query.unsettledOnly === undefined ? {} : { unsettledOnly: query.unsettledOnly }),
        });
        const expected = [...shell.threads, ...shell.archivedThreads].filter(
          (row) =>
            (query.threadIds === undefined || query.threadIds.includes(row.id)) &&
            (query.worktree === undefined ||
              (row.worktreePath === query.worktree.path && row.branch === query.worktree.branch)),
        );
        const result = yield* metadata.list(query);
        assert.deepEqual(
          yield* metadata.listIds(query),
          expected.map((row) => row.id),
        );
        assert.deepEqual(
          result.threads,
          expected.map(({ id, lineage }) => ({ id, lineage })),
        );
        assert.equal(result.snapshotSequence, shell.snapshotSequence);
      }
      const shell = yield* projections.getShellSnapshot();
      assert.equal(yield* metadata.count, shell.threads.length + shell.archivedThreads.length);
      assert.equal(yield* metadata.watermark, shell.snapshotSequence);
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "ID discovery touches only thread rows, never decodes payloads or reads a watermark",
  () =>
    Effect.gen(function* () {
      const metadata = yield* ThreadMetadataReads.ThreadMetadataReads;
      const sql = yield* SqlClient.SqlClient;
      const selected = thread("metadata:selected");
      const unrelated = thread("metadata:unrelated", { projectId: otherProjectId });
      yield* commit(selected);
      yield* commit(unrelated);
      // Valid JSON with no decodable thread schema breaks full-shell materialization.
      yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json = '{}'
      WHERE thread_id = ${unrelated.id}`;
      const statements: string[] = [];
      const capture: Statement.Transformer = (statement) => {
        statements.push(statement.compile()[0]);
        return Effect.succeed(statement);
      };
      assert.deepEqual(
        yield* metadata
          .listIds({
            worktree: { path: "/synthetic/worktree", branch: "old-branch" },
          })
          .pipe(Effect.provideService(Statement.CurrentTransformer, capture)),
        [selected.id],
      );
      const selects = statements.filter((query) => /SELECT/i.test(query));
      assert.lengthOf(selects, 1);
      assert.match(selects[0]!, /FROM orchestration_v2_projection_threads/);
      assert.notMatch(
        selects[0]!,
        /orchestration_events|projection_(runs|turn_items|provider_threads)/,
      );
      assert.deepEqual(
        (yield* metadata.list({ projectId })).threads.map((row) => row.id),
        [selected.id],
      );
      const failure = yield* metadata.list({ projectId: otherProjectId }).pipe(Effect.flip);
      assert.equal(failure._tag, "ThreadMetadataReadError");
      assert.equal(yield* metadata.count, 2);
    }).pipe(Effect.provide(memory)),
);

const fileDatabase = Layer.unwrap(
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    // Scope closes SQLite and its worker before removing this synthetic fixture,
    // including failure and cancellation. No data survives the test.
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "jones-thread-metadata-" });
    return services.pipe(
      Layer.provideMerge(SqlitePersistence.layerFromPath(path.join(directory, "state.sqlite"))),
    );
  }),
);

it.live("file-WAL marked ID/list/count reads bypass a held writer; unmarked control waits", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const metadata = yield* ThreadMetadataReads.ThreadMetadataReads;
    const row = thread("metadata:held-writer");
    yield* commit(row);
    yield* metadata.count.pipe(NodeSqliteClient.readOnly);
    const held = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const writer = yield* Effect.forkScoped(
      sql.withTransaction(
        Deferred.succeed(held, undefined).pipe(Effect.andThen(Deferred.await(release))),
      ),
    );
    yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined));
    yield* Deferred.await(held);
    yield* Effect.gen(function* () {
      const controlStarted = yield* Deferred.make<void>();
      const control = yield* Effect.forkChild(
        Deferred.succeed(controlStarted, undefined).pipe(Effect.andThen(metadata.count)),
      );
      yield* Deferred.await(controlStarted);
      const reads = yield* Effect.forkChild(
        Effect.all([metadata.listIds(), metadata.list(), metadata.count]).pipe(
          NodeSqliteClient.readOnly,
        ),
      );
      const result = yield* Fiber.join(reads).pipe(Effect.timeoutOption("5 seconds"));
      const controlBeforeRelease = yield* Effect.sync(() => control.pollUnsafe());
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(writer);
      // Release before asserting/interrupting: blocked writer acquisition is uninterruptible.
      if (Option.isNone(result)) {
        yield* Fiber.interrupt(reads);
        assert.fail("Marked metadata reads waited behind the held writer");
      } else {
        assert.deepEqual(result.value[0], [row.id]);
        assert.deepEqual(
          result.value[1].threads.map((item) => item.id),
          [row.id],
        );
        assert.equal(result.value[2], 1);
      }
      assert.isUndefined(controlBeforeRelease);
      assert.equal(yield* Fiber.join(control), 1);
    }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)));
  }).pipe(Effect.provide(fileDatabase), Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("marked nested reads retain the writer and see its uncommitted rows", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const metadata = yield* ThreadMetadataReads.ThreadMetadataReads;
    const row = thread("metadata:uncommitted");
    yield* sql.withTransaction(
      Effect.gen(function* () {
        const sequence = yield* commit(row);
        const result = yield* metadata.list().pipe(NodeSqliteClient.readOnly);
        assert.deepEqual(
          result.threads.map((item) => item.id),
          [row.id],
        );
        assert.equal(result.snapshotSequence, sequence);
        assert.deepEqual(yield* metadata.listIds().pipe(NodeSqliteClient.readOnly), [row.id]);
        assert.equal(yield* metadata.count.pipe(NodeSqliteClient.readOnly), 1);
        assert.equal(yield* metadata.watermark.pipe(NodeSqliteClient.readOnly), sequence);
      }),
    );
  }).pipe(Effect.provide(fileDatabase), Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("rows and watermark share a WAL snapshot across an intervening writer commit", () =>
  Effect.gen(function* () {
    const metadata = yield* ThreadMetadataReads.ThreadMetadataReads;
    const before = thread("metadata:coherent");
    const firstSequence = yield* commit(before);
    const rowsRead = yield* Deferred.make<void>();
    const committed = yield* Deferred.make<void, Effect.Error<ReturnType<typeof commit>>>();
    const writer = yield* Effect.forkChild(
      Effect.gen(function* () {
        yield* Deferred.await(rowsRead);
        const sequence = yield* commit(
          {
            ...before,
            title: "after",
            lineage: {
              ...before.lineage,
              relationshipToParent: "subagent",
              parentThreadId: ThreadId.make("metadata:parent"),
            },
          },
          "thread.metadata-updated",
        );
        yield* Deferred.succeed(committed, undefined);
        return sequence;
      }).pipe(
        Effect.onExit((exit) =>
          Exit.isFailure(exit) ? Deferred.failCause(committed, exit.cause) : Effect.void,
        ),
      ),
    );
    let gates = 0;
    const capture: Statement.Transformer = (statement) =>
      Effect.gen(function* () {
        const query = statement.compile()[0];
        // The row SELECT has finished when the watermark statement is transformed.
        if (query.includes("MAX(sequence)") && gates++ === 0) {
          yield* Deferred.succeed(rowsRead, undefined);
          yield* Deferred.await(committed).pipe(Effect.orDie);
        }
        return statement;
      });
    const result = yield* metadata
      .list()
      .pipe(
        NodeSqliteClient.readOnly,
        Effect.provideService(Statement.CurrentTransformer, capture),
      );
    assert.equal(gates, 1);
    assert.equal(result.snapshotSequence, firstSequence);
    assert.deepEqual(result.threads, [{ id: before.id, lineage: before.lineage }]);
    const nextSequence = yield* Fiber.join(writer);
    assert.isAbove(nextSequence, firstSequence);
    const next = yield* metadata.list().pipe(NodeSqliteClient.readOnly);
    assert.equal(next.snapshotSequence, nextSequence);
    assert.equal(next.threads[0]?.lineage.relationshipToParent, "subagent");
  }).pipe(Effect.provide(fileDatabase), Effect.scoped, Effect.provide(NodeServices.layer)),
);

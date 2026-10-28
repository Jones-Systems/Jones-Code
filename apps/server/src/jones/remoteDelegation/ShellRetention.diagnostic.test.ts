// @effect-diagnostics nodeBuiltinImport:off -- Explicit synthetic diagnostic emits JSON for its invoking harness.
import * as NodeConsole from "node:console";
import { expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { EventId, ThreadId } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as SqlClient from "effect/sql/SqlClient";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import {
  buildActiveShellSnapshot,
  loadShellSnapshotParts,
  shellSnapshotForSubscriber,
} from "../../orchestration-v2/ShellStream.ts";
import {
  measure,
  withNativeWatchdog,
  type Measurement,
} from "../orchestration/shellCost.testFixtures.ts";
import { readRemoteTaskOwnerIds } from "./ShellRetention.ts";
import { now, parent } from "./ShellRetention.testkit.ts";

const shellId = (index: number) => ThreadId.make(`retention-cost:${index}`);

const seed = Effect.fn("ShellRetentionDiagnostic.seed")(function* (rows: number) {
  const store = yield* ProjectionStore.ProjectionStoreV2;
  const sql = yield* SqlClient.SqlClient;
  for (let index = 0; index < 2_000; index++) {
    const id = shellId(index);
    yield* store.apply({
      id: EventId.make(`create:${id}`),
      type: "thread.created",
      threadId: id,
      occurredAt: now,
      payload: {
        ...parent,
        id,
        lineage: { ...parent.lineage, rootThreadId: id },
        archivedAt: index % 10 === 0 ? now : null,
        settledAt: DateTime.makeUnsafe(1_700_000_000_000 + index * 1_000),
      },
    });
  }
  const expected = new Set<string>();
  // Some tasks reference missing or archived owners; retention never adds them to active input.
  for (let offset = 0; offset < rows; offset += 100) {
    const batch = Array.from({ length: Math.min(100, rows - offset) }, (_, batchIndex) => {
      const index = offset + batchIndex;
      const remote = index % 3 === 0;
      const bound = index % 2 === 0;
      const id = shellId(index % 2_400);
      if (remote && bound) expected.add(id);
      return {
        subagent_id: `retention-cost:task:${index}`,
        thread_id: id,
        run_id: null,
        parent_node_id: `node:${index}`,
        provider: "codex",
        driver: remote ? "remote" : "codex",
        provider_instance_id: "codex",
        provider_thread_id: null,
        child_thread_id: null,
        origin: "app_owned",
        status: ["running", "completed", "failed"][Math.floor(index / 3) % 3]!,
        started_at: null,
        completed_at: null,
        updated_at: DateTime.formatIso(now),
        payload_json: JSON.stringify(
          bound ? { remoteChild: { threadId: `destination:${index}` } } : {},
        ),
      };
    });
    yield* sql`INSERT INTO orchestration_v2_projection_subagents ${sql.insert(batch)}`;
  }
  return expected;
});

const bounded = <A, E, R>(effect: Effect.Effect<A, E, R>, phase: string) =>
  withNativeWatchdog(
    effect,
    "90 seconds",
    () => new Error(`retention diagnostic did not complete: ${phase}`),
  );

const scenario = Effect.fn("ShellRetentionDiagnostic.scenario")(function* (
  rows: number,
  fileBacked: boolean,
) {
  const expected = yield* seed(rows);
  const sql = yield* SqlClient.SqlClient;
  const store = yield* ProjectionStore.ProjectionStoreV2;
  const measurements: Measurement[] = [];
  for (let repeat = 0; repeat < 7; repeat++) {
    const ids = yield* measure(
      "retained-query-transaction",
      sql
        .withTransaction(measure("retained-query", readRemoteTaskOwnerIds(sql), measurements))
        .pipe(NodeSqliteClient.readOnly),
      measurements,
    );
    expect([...ids].sort()).toEqual([...expected].sort());
  }
  const load = (retain: boolean) =>
    loadShellSnapshotParts({
      sql,
      readThreads: store.readShellSnapshot({ location: "active" }),
      listProjects: Effect.succeed([]),
      latestSequence: sql<{
        sequence: number;
      }>`SELECT COALESCE(MAX(sequence), 0) AS sequence FROM orchestration_events`.pipe(
        Effect.map((result) => result[0]!.sequence),
      ),
      ...(retain ? { readRetainedThreadIds: readRemoteTaskOwnerIds(sql) } : {}),
    });
  const reduced = (parts: Effect.Success<ReturnType<typeof load>>) =>
    shellSnapshotForSubscriber(buildActiveShellSnapshot(parts), {
      omitIdleSubagents: true,
      omitOlderSettled: true,
      now,
      retainedThreadIds: parts.retainedThreadIds,
    }).snapshot;
  const without = yield* measure("loader-without-retention", load(false), measurements);
  const withRetention = yield* measure("loader-with-retention", load(true), measurements);
  expect(without.retainedThreadIds.size).toBe(0);
  expect([...withRetention.retainedThreadIds].sort()).toEqual([...expected].sort());
  expect(withRetention.threads).toEqual(without.threads);
  expect(withRetention.threads.threads).toHaveLength(1_800);
  const before = reduced(without);
  const after = reduced(withRetention);
  expect(after.olderSettled).toEqual(before.olderSettled);
  const activeRetained = withRetention.threads.threads.filter((thread) => expected.has(thread.id));
  const baselineIds = new Set(before.threads.map((thread) => thread.id));
  const expectedAfter = new Set([...baselineIds, ...activeRetained.map((thread) => thread.id)]);
  expect(after.threads.map((thread) => thread.id).sort()).toEqual([...expectedAfter].sort());

  // These are independent subscriber loads sharing the production reader, not parallel SQL in one transaction.
  const subscriberTimings: Array<{
    retain: boolean;
    subscribers: number;
    medianMs: number;
    slowestMs: number;
  }> = [];
  for (const count of [1, 4, 8])
    for (const retain of [false, true]) {
      const operation = `subscriber:${count}:${retain}:`;
      const subscribers = yield* bounded(
        Effect.all(
          Array.from({ length: count }, (_, subscriber) =>
            measure(`${operation}${subscriber}`, load(retain), measurements),
          ),
          { concurrency: count },
        ),
        `subscribers:${count}:${retain}`,
      );
      for (const parts of subscribers) expect(reduced(parts)).toEqual(retain ? after : before);
      const times = measurements
        .filter((entry) => entry.operation.startsWith(operation))
        .map((entry) => entry.milliseconds)
        .sort((a, b) => a - b);
      subscriberTimings.push({
        retain,
        subscribers: count,
        medianMs: times[Math.floor(times.length / 2)]!,
        slowestMs: times.at(-1)!,
      });
    }

  let writerHeldReader: "completed" | "unmeasured" = "unmeasured";
  if (fileBacked) {
    const entered = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const writer = yield* Effect.forkChild(
      sql.withTransaction(
        Effect.gen(function* () {
          yield* sql`UPDATE orchestration_v2_projection_threads SET title = 'uncommitted writer title' WHERE thread_id = ${shellId(1)}`;
          yield* Deferred.succeed(entered, undefined);
          yield* Deferred.await(release);
        }),
      ),
    );
    yield* Effect.addFinalizer(() => Fiber.interrupt(writer));
    yield* bounded(Deferred.await(entered), "writer-entered");
    yield* Effect.gen(function* () {
      // Snapshot reads use the WAL reader; decoding also reads existing dependency
      // metadata through the writer. Release the held writer after proving that
      // the reader query completes, then await the complete loader.
      const loaders = yield* Effect.forkChild(
        Effect.forEach(
          [false, true],
          (retain) => measure(`writer-held-loader:${retain}`, load(retain), measurements),
          { concurrency: 2 },
        ),
      );
      const ids = yield* bounded(
        measure(
          "writer-held-retained-query",
          sql.withTransaction(readRemoteTaskOwnerIds(sql)).pipe(NodeSqliteClient.readOnly),
          measurements,
        ),
        "writer-held-retained-query",
      );
      expect([...ids].sort()).toEqual([...expected].sort());
      yield* Deferred.succeed(release, undefined);
      const loaded = yield* bounded(Fiber.join(loaders), "writer-released-loaders");
      for (const [index, parts] of loaded.entries()) {
        expect(reduced(parts)).toEqual(index === 1 ? after : before);
      }
      writerHeldReader = "completed";
    }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)));
    yield* Fiber.join(writer);
  }
  const queryTimes = measurements
    .filter((measurement) => measurement.operation === "retained-query")
    .map((measurement) => measurement.milliseconds)
    .sort((a, b) => a - b);
  // @effect-diagnostics-next-line globalConsoleInEffect:off -- Raw diagnostic output is consumed by the coordinator's serial TestServer invocation.
  NodeConsole.log(
    JSON.stringify({
      diagnostic: "remote-owner-shell-retention",
      rows,
      shells: 2_000,
      fileBacked,
      historicalOwnerIds: expected.size,
      activeOwnerRows: activeRetained.length,
      rowsActuallyKept: after.threads.length - before.threads.length,
      subscriberTimings,
      reducedRowsBefore: before.threads.length,
      reducedRowsAfter: after.threads.length,
      serializedBytesBefore: Buffer.byteLength(JSON.stringify(before)),
      serializedBytesAfter: Buffer.byteLength(JSON.stringify(after)),
      queryInsideTransactionMedianMs: queryTimes[Math.floor(queryTimes.length / 2)],
      queryInsideTransactionSlowestMs: queryTimes.at(-1),
      writerHeldReader,
      timingScope:
        "instrumented monotonic wall clock; SQL spans include admission, execution and transfer",
      measurements: measurements.map(({ operation, milliseconds, databaseSpans }) => ({
        operation,
        milliseconds,
        readerWaitMs: databaseSpans.flatMap(({ attributes }) =>
          typeof attributes["db.lock_wait_ms"] === "number" ? [attributes["db.lock_wait_ms"]] : [],
        ),
        databaseSpans: databaseSpans.map(({ attributes, ...span }) => ({
          ...span,
          attributes: Object.fromEntries(
            Object.entries(attributes).filter(([key]) => key !== "db.query.text"),
          ),
        })),
      })),
    }),
  );
});

// Synthetic file-backed diagnostic; timing values are observations, never pass/fail thresholds.
it.live.each([1_000, 10_000, 100_000])("remote retention: %i task rows and 2k shells", (rows) =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      // This exact scoped directory owns the SQLite file and WAL/SHM; connections close before removal.
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "jones-shell-retention-" });
      yield* scenario(rows, true).pipe(
        Effect.provide(
          ProjectionStore.layer.pipe(
            Layer.provideMerge(
              SqlitePersistence.layerFromPath(path.join(directory, "synthetic.sqlite")),
            ),
          ),
        ),
      );
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

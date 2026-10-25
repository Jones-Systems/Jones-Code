import * as Schema from "effect/Schema";
// @effect-diagnostics nodeBuiltinImport:off -- Opt-in synthetic diagnostic; no application state or product telemetry.
import * as NodeConsole from "node:console";
import * as NodeFSP from "node:fs/promises";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert } from "@effect/vitest";
import { EventId, ProjectId, ProviderInstanceId, RunId, ThreadId } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as SqlClient from "effect/sql/SqlClient";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import { workModeCandidate, workModeTiming } from "../workMode/Policy.ts";
import { measure, queryEvidence, type Measurement } from "./shellCost.testFixtures.ts";

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

const iso = "2026-10-01T00:00:00.000Z";
const humanIso = "2026-09-30T00:00:00.000Z";
const now = DateTime.makeUnsafe(iso);
const instanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId, model: "synthetic-m1" };
const threadId = (index: number) => ThreadId.make(`shell-m1:thread:${index}`);
const runId = (thread: number, index: number) => RunId.make(`${threadId(thread)}:run:${index}`);
const providerId = (thread: number, index: number) => `${threadId(thread)}:provider:${index}`;

function integer(name: string, fallback: number, minimum: number, maximum: number) {
  const text = process.env[`JONES_SHELL_COST_${name}`];
  const value = text === undefined ? fallback : Number(text);
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum || text?.trim() === "")
    throw new Error(`JONES_SHELL_COST_${name} must be an integer in [${minimum}, ${maximum}]`);
  return value;
}

function configuration(large: boolean) {
  const threads = integer("THREADS", 6, 1, 256);
  const itemsPerThread = integer("ITEMS", large ? 10_000 : 16, 2, 10_000);
  const archiveRatio = Number(process.env.JONES_SHELL_COST_ARCHIVE_RATIO ?? "0.3333333333333333");
  if (!Number.isFinite(archiveRatio) || archiveRatio < 0 || archiveRatio > 1)
    throw new Error("JONES_SHELL_COST_ARCHIVE_RATIO must be in [0, 1]");
  const forkDepth = integer("FORK_DEPTH", Math.min(1, threads - 1), 0, 8);
  const providerHistory = integer("PROVIDER_HISTORY", 3, 0, 128);
  const pendingBytes = integer("PENDING_BYTES", 4096, 0, 1_048_576);
  if (forkDepth >= threads) throw new Error("FORK_DEPTH must be less than THREADS");
  if (
    threads * itemsPerThread > 100_000 ||
    threads * providerHistory > 8192 ||
    threads * pendingBytes > 32 * 1024 * 1024
  )
    throw new Error("M1 fixture exceeds its aggregate row/provider/pending-byte resource bounds");
  return {
    threads,
    itemsPerThread,
    archiveRatio,
    archivedThreads: Math.floor(threads * archiveRatio),
    forkDepth,
    providerHistory,
    pendingBytes,
  };
}
type Dimensions = ReturnType<typeof configuration>;

// Batch construction is bounded independently of fixture size. These persisted
// rows are synthetic projections; production migrations and readers stay intact.
const seed = Effect.fn("shellCost.seedM1")(function* (dimensions: Dimensions) {
  const sql = yield* SqlClient.SqlClient;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const { threads, itemsPerThread, archivedThreads, forkDepth, providerHistory, pendingBytes } =
    dimensions;
  for (let thread = 0; thread < threads; thread++) {
    const id = threadId(thread);
    const fork = thread > 0 && thread <= forkDepth;
    const status = thread % 3 === 0 ? "completed" : thread % 3 === 1 ? "failed" : "running";
    yield* projections.apply({
      id: EventId.make(`${id}:created`),
      type: "thread.created",
      threadId: id,
      occurredAt: now,
      payload: {
        id,
        projectId: ProjectId.make("shell-m1:project"),
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
        lineage: fork
          ? {
              parentThreadId: threadId(thread - 1),
              relationshipToParent: "fork",
              rootThreadId: threadId(0),
            }
          : { parentThreadId: null, relationshipToParent: null, rootThreadId: id },
        forkedFrom: fork
          ? {
              type: "run",
              threadId: threadId(thread - 1),
              runId: runId(thread - 1, itemsPerThread - 1),
            }
          : null,
        createdAt: now,
        updatedAt: now,
        archivedAt: thread < archivedThreads ? now : null,
        deletedAt: null,
        settledOverride: null,
        settledAt: null,
        lastVisitedAt: null,
      },
    });
    for (let offset = 0; offset < itemsPerThread; offset += 100) {
      const indexes = Array.from(
        { length: Math.min(100, itemsPerThread - offset) },
        (_, i) => offset + i,
      );
      yield* sql`INSERT INTO orchestration_v2_projection_runs ${sql.insert(
        indexes.map((index) => {
          const state = index === itemsPerThread - 1 ? status : "completed";
          return {
            run_id: runId(thread, index),
            thread_id: id,
            ordinal: index + 1,
            provider: instanceId,
            provider_instance_id: instanceId,
            provider_thread_id: null,
            status: state,
            requested_at: iso,
            completed_at: state === "running" ? null : iso,
            payload_json: encodeJson({
              id: runId(thread, index),
              threadId: id,
              ordinal: index + 1,
              providerInstanceId: instanceId,
              modelSelection,
              providerThreadId: null,
              userMessageId: `${id}:message:${index}`,
              rootNodeId: `${id}:node:${index}`,
              activeAttemptId: null,
              status: state,
              requestedAt: iso,
              startedAt: iso,
              completedAt: state === "running" ? null : iso,
              checkpointId: null,
              contextHandoffId: null,
            }),
          };
        }),
      )}`;
      yield* sql`INSERT INTO orchestration_v2_projection_turn_items ${sql.insert(
        indexes.map((index) => {
          const failed = status === "failed" && index === itemsPerThread - 1;
          const type = failed ? "error" : "assistant_message";
          const itemStatus = failed ? "failed" : "completed";
          return {
            turn_item_id: `${id}:item:${index}`,
            thread_id: id,
            run_id: runId(thread, index),
            node_id: `${id}:node:${index}`,
            provider_thread_id: null,
            provider_turn_id: null,
            parent_item_id: null,
            ordinal: index + 1,
            type,
            status: itemStatus,
            updated_at: iso,
            payload_json: encodeJson({
              id: `${id}:item:${index}`,
              threadId: id,
              runId: runId(thread, index),
              nodeId: `${id}:node:${index}`,
              providerThreadId: null,
              providerTurnId: null,
              nativeItemRef: null,
              parentItemId: null,
              ordinal: index + 1,
              status: itemStatus,
              title: null,
              startedAt: iso,
              completedAt: iso,
              updatedAt: iso,
              type,
              ...(failed
                ? {
                    failure: {
                      class: "provider_error",
                      message: "synthetic M1 failure",
                      code: null,
                      retryable: false,
                    },
                  }
                : { messageId: `${id}:message:${index}`, text: "synthetic", streaming: false }),
            }),
          };
        }),
      )}`;
      yield* sql`INSERT INTO orchestration_v2_projection_messages ${sql.insert(
        indexes.map((index) => {
          const at = index === 0 ? humanIso : iso;
          return {
            message_id: `${id}:message:${index}`,
            thread_id: id,
            run_id: runId(thread, index),
            node_id: null,
            role: "user",
            streaming: 0,
            created_at: at,
            updated_at: at,
            payload_json: encodeJson({
              id: `${id}:message:${index}`,
              threadId: id,
              runId: runId(thread, index),
              nodeId: null,
              role: "user",
              createdBy: index === 0 ? "user" : "agent",
              creationSource: "web",
              text: "synthetic M1 input",
              attachments: [],
              streaming: false,
              createdAt: at,
              updatedAt: at,
            }),
          };
        }),
      )}`;
    }
    for (let index = 0; index < providerHistory; index++) {
      yield* sql`INSERT INTO orchestration_v2_projection_provider_threads ${sql.insert({
        provider_thread_id: providerId(thread, index),
        thread_id: id,
        owner_node_id: null,
        provider: instanceId,
        provider_instance_id: instanceId,
        provider_session_id: null,
        status: "idle",
        first_run_ordinal: 1,
        last_run_ordinal: itemsPerThread,
        updated_at: iso,
        payload_json: encodeJson({
          id: providerId(thread, index),
          driver: "codex",
          providerInstanceId: instanceId,
          providerSessionId: null,
          appThreadId: id,
          ownerNodeId: null,
          nativeThreadRef: null,
          nativeConversationHeadRef: null,
          status: "idle",
          firstRunOrdinal: 1,
          lastRunOrdinal: itemsPerThread,
          handoffIds: [],
          forkedFrom: null,
          createdAt: iso,
          updatedAt: iso,
        }),
      })}`;
    }
    if (providerHistory > 0) {
      // Keep the indexed column and payload consistent without creating a live session.
      yield* sql`UPDATE orchestration_v2_projection_threads
        SET active_provider_thread_id = ${providerId(thread, providerHistory - 1)},
          payload_json = json_set(payload_json, '$.activeProviderThreadId', ${providerId(thread, providerHistory - 1)})
        WHERE thread_id = ${id}`;
    }
    if (status === "running" && pendingBytes > 0) {
      yield* sql`INSERT INTO orchestration_v2_projection_turn_items ${sql.insert({
        turn_item_id: `${id}:pending`,
        thread_id: id,
        run_id: runId(thread, itemsPerThread - 1),
        node_id: null,
        provider_thread_id: null,
        provider_turn_id: null,
        parent_item_id: null,
        ordinal: itemsPerThread + 1,
        type: "command_execution",
        status: "running",
        updated_at: iso,
        payload_json: encodeJson({
          id: `${id}:pending`,
          threadId: id,
          runId: runId(thread, itemsPerThread - 1),
          nodeId: null,
          providerThreadId: null,
          providerTurnId: null,
          nativeItemRef: null,
          parentItemId: null,
          ordinal: itemsPerThread + 1,
          status: "running",
          title: null,
          startedAt: iso,
          completedAt: null,
          updatedAt: iso,
          type: "command_execution",
          input: "synthetic",
          output: "x".repeat(pendingBytes),
        }),
      })}`;
    }
  }
  const counts = yield* sql<{
    thread_id: string;
    runs: number;
    items: number;
    providers: number;
    human_messages: number;
    pending_bytes: number;
    failures: number;
    fork_source: string | null;
  }>`SELECT t.thread_id,
    (SELECT COUNT(*) FROM orchestration_v2_projection_runs r WHERE r.thread_id = t.thread_id) AS runs,
    (SELECT COUNT(*) FROM orchestration_v2_projection_turn_items i WHERE i.thread_id = t.thread_id) AS items,
    (SELECT COUNT(*) FROM orchestration_v2_projection_provider_threads p WHERE p.thread_id = t.thread_id) AS providers,
    (SELECT COUNT(*) FROM orchestration_v2_projection_messages m WHERE m.thread_id = t.thread_id AND json_extract(m.payload_json, '$.createdBy') = 'user') AS human_messages,
    (SELECT COALESCE(SUM(length(json_extract(i.payload_json, '$.output'))), 0) FROM orchestration_v2_projection_turn_items i WHERE i.thread_id = t.thread_id AND i.status = 'running') AS pending_bytes,
    (SELECT COUNT(*) FROM orchestration_v2_projection_turn_items i WHERE i.thread_id = t.thread_id AND i.type = 'error' AND i.status = 'failed') AS failures,
    json_extract(t.payload_json, '$.forkedFrom.threadId') AS fork_source
    FROM orchestration_v2_projection_threads t`;
  assert.lengthOf(counts, threads);
  for (let thread = 0; thread < threads; thread++) {
    const pending = thread % 3 === 2 && pendingBytes > 0;
    assert.deepEqual(
      counts.find((row) => row.thread_id === threadId(thread)),
      {
        thread_id: threadId(thread),
        runs: itemsPerThread,
        items: itemsPerThread + Number(pending),
        providers: providerHistory,
        human_messages: 1,
        pending_bytes: pending ? pendingBytes : 0,
        failures: Number(thread % 3 === 1),
        fork_source: thread > 0 && thread <= forkDepth ? threadId(thread - 1) : null,
      },
    );
  }
  return counts;
});

// Isolated lookup probes mirror parts of the composite production SELECT.
// They are sensitivity experiments, never additive decomposition of that SELECT.
const lookupProbes = Effect.fn("shellCost.lookupProbes")(function* (measurements: Measurement[]) {
  const sql = yield* SqlClient.SqlClient;
  const probes = [
    [
      "H1:item-counts",
      sql`SELECT t.thread_id, (
      SELECT COALESCE(SUM(per_run.item_count), 0) FROM (
        SELECT i.run_id, COUNT(*) AS item_count FROM orchestration_v2_projection_turn_items i
        WHERE i.thread_id = t.thread_id GROUP BY i.run_id
      ) per_run LEFT JOIN orchestration_v2_projection_runs r ON r.run_id = per_run.run_id
      WHERE per_run.run_id IS NULL OR r.status <> 'rolled_back'
    ) AS item_count FROM orchestration_v2_projection_threads t`.pipe(Effect.asVoid),
    ],
    [
      "H2:latest-user-authored",
      sql`SELECT t.thread_id, (
      SELECT m.updated_at FROM orchestration_v2_projection_messages m
      WHERE m.thread_id = t.thread_id AND m.role = 'user' AND json_extract(m.payload_json, '$.createdBy') = 'user'
      ORDER BY m.updated_at DESC, m.message_id DESC LIMIT 1
    ) AS latest_user_authored_message_at FROM orchestration_v2_projection_threads t`.pipe(
        Effect.asVoid,
      ),
    ],
    [
      "H3:blocked-run-ordering",
      sql`SELECT t.thread_id, (
      SELECT candidate.run_id FROM orchestration_v2_projection_runs candidate
      WHERE candidate.thread_id = t.thread_id AND candidate.status <> 'queued'
        AND NOT (candidate.status = 'cancelled' AND json_extract(candidate.payload_json, '$.startedAt') IS NULL)
      ORDER BY candidate.completed_at IS NULL DESC, candidate.completed_at DESC,
        candidate.ordinal DESC, candidate.run_id DESC LIMIT 1
    ) AS blocking_run_id FROM orchestration_v2_projection_threads t`.pipe(Effect.asVoid),
    ],
    [
      "H4:failure-lookups",
      sql`SELECT r.thread_id, (
      SELECT item.payload_json FROM orchestration_v2_projection_turn_items item
      WHERE item.thread_id = r.thread_id AND item.run_id = r.run_id
        AND item.type = 'error' AND item.status = 'failed'
        AND item.node_id IS json_extract(r.payload_json, '$.rootNodeId')
      ORDER BY item.updated_at DESC, item.ordinal DESC, item.turn_item_id DESC LIMIT 1
    ) AS terminal_failure_payload_json FROM orchestration_v2_projection_runs r WHERE r.status = 'failed'`.pipe(
        Effect.asVoid,
      ),
    ],
  ] as const;
  for (const [hypothesis, query] of probes) {
    yield* measure(`isolated-lookup-probe:${hypothesis}`, query, measurements).pipe(
      NodeSqliteClient.readOnly,
    );
  }
});

export function fileBackedScenario(large: boolean) {
  return Effect.scoped(
    Effect.gen(function* () {
      const dimensions = configuration(large);
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = process.env.JONES_SHELL_COST_M1_ROOT;
      if (!root || !path.isAbsolute(root))
        throw new Error(
          "M1 requires an existing absolute JONES_SHELL_COST_M1_ROOT on approved disk-backed scratch",
        );
      const storage = yield* Effect.tryPromise(() => NodeFSP.statfs(root, { bigint: true }));
      const estimatedPeakBytes =
        64 * 1024 * 1024 +
        4 *
          dimensions.threads *
          (dimensions.itemsPerThread * 4096 +
            dimensions.providerHistory * 1024 +
            dimensions.pendingBytes);
      const reserveBytes = 256 * 1024 * 1024;
      const availableBytes = storage.bavail * storage.bsize;
      if (availableBytes < BigInt(estimatedPeakBytes + reserveBytes))
        throw new Error(
          `M1 scratch capacity refused: available=${availableBytes}, peak=${estimatedPeakBytes}, reserve=${reserveBytes}`,
        );
      // The SQLite layer is nested inside this scope: its connections and workers
      // close before the exact unique directory is removed on all ordinary exits.
      const directory = yield* fs.makeTempDirectoryScoped({
        directory: root,
        prefix: "jones-shell-m1-",
      });
      const database = SqlitePersistence.layerFromPath(path.join(directory, "state.sqlite"));
      yield* Effect.gen(function* () {
        const counts = yield* seed(dimensions);
        const store = yield* ProjectionStore.ProjectionStoreV2;
        const sql = yield* SqlClient.SqlClient;
        const sqliteVersion = yield* sql`SELECT sqlite_version() AS version`;
        const measurements: Measurement[] = [];
        const decode = yield* measure(
          "initial-shell:raw-read",
          sql.withTransaction(store.readShellSnapshot()).pipe(NodeSqliteClient.readOnly),
          measurements,
        );
        const snapshot = yield* measure("H5:initial-shell-decode-build", decode, measurements);
        assert.lengthOf(snapshot.threads, dimensions.threads - dimensions.archivedThreads);
        assert.lengthOf(snapshot.archivedThreads, dimensions.archivedThreads);
        const shells = [...snapshot.threads, ...snapshot.archivedThreads];
        for (let thread = 0; thread < dimensions.threads; thread++) {
          const shell = shells.find((value) => value.id === threadId(thread))!;
          const depth = thread <= dimensions.forkDepth ? thread : 0;
          const ownItems = (index: number) =>
            dimensions.itemsPerThread + Number(index % 3 === 2 && dimensions.pendingBytes > 0);
          const inheritedItems = Array.from(
            { length: depth },
            (_, index) => ownItems(index) + 1,
          ).reduce((a, b) => a + b, 0);
          assert.strictEqual(shell.itemCount, ownItems(thread));
          assert.strictEqual(shell.visibleItemCount, ownItems(thread) + inheritedItems);
          assert.strictEqual(shell.latestRunId, runId(thread, dimensions.itemsPerThread - 1));
          assert.strictEqual(DateTime.formatIso(shell.latestUserAuthoredMessageAt!), humanIso);
          assert.strictEqual(DateTime.formatIso(shell.latestUserMessageAt!), iso);
          if (thread % 3 === 1) assert.strictEqual(shell.lastError, "synthetic M1 failure");
        }
        const workMode = yield* measure(
          "work-mode:shell-refresh-read-decode",
          store.getShellSnapshot({
            location: "active",
            unsettledOnly: true,
          }),
          measurements,
        );
        assert.sameMembers(
          workMode.threads.map((thread) => thread.id),
          snapshot.threads.map((thread) => thread.id),
        );
        const candidates = workMode.threads.filter(
          (thread) => workModeCandidate(thread, DateTime.toEpochMillis(now)) !== null,
        );
        let refreshed = 0;
        yield* measure(
          "work-mode:cold-record-refresh-and-timing",
          Effect.gen(function* () {
            for (const thread of candidates) {
              const records = yield* store.getThreadRecords(
                thread.id,
                ["providerTurns", "runs", "messages"],
                { messageRoles: ["user"] },
              );
              workModeTiming(records, thread.activeProviderThreadId!);
              refreshed++;
            }
          }),
          measurements,
        );
        yield* measure(
          "fork-target:reader-read-decode",
          store.getThreadShell(threadId(dimensions.forkDepth)),
          measurements,
        ).pipe(NodeSqliteClient.readOnly);
        yield* lookupProbes(measurements);
        const queries = yield* queryEvidence(measurements).pipe(NodeSqliteClient.readOnly);
        // @effect-diagnostics-next-line globalConsoleInEffect:off -- Caller owns stdout evidence outside the repository.
        NodeConsole.log(
          encodeJson({
            diagnostic: "v2-shell-cost-m1",
            large,
            dimensions,
            fixtureCounts: counts,
            nodeVersion: process.versions.node,
            sqliteVersion,
            storage: {
              root,
              directory,
              availableBytes: String(availableBytes),
              estimatedPeakBytes,
              reserveBytes,
              cleanup:
                "scoped unique directory; SQLite/worker finalizers run before directory removal",
            },
            surface:
              "file-backed production ProjectionStore; serial synthetic read/decode and Work Mode data-refresh seams",
            scope:
              "initial shell is projection read/decode only; excludes projects/global watermark/transport/UI. Work Mode excludes scheduler/admission/dispatch; fork target combines read/decode.",
            attribution:
              "H1-H4 coexist in the production SELECT; isolated probes are non-additive analogues. H4 probes terminal errors; session-error bindings/attempts/requests are unpopulated. H5 is deferred main-thread schema decode/build. H6 is the production pending-item SELECT.",
            clock:
              "performance.now monotonic milliseconds; tracing/statement compilation included; no latency acceptance threshold",
            workMode: {
              candidates: candidates.length,
              refreshed,
              cache:
                "cold generation records; provider-turn usage intentionally absent, no dispatch",
            },
            measurements: measurements.map(({ statements, ...measurement }) => ({
              ...measurement,
              observedSqlStatements: statements.length,
            })),
            queries,
          }),
        );
      }).pipe(Effect.provide(ProjectionStore.layer.pipe(Layer.provideMerge(database))));
    }),
  ).pipe(Effect.provide(NodeServices.layer));
}

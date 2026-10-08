// @effect-diagnostics nodeBuiltinImport:off -- Timings describe the captured synthetic worker only.
// @effect-diagnostics globalTimers:off -- The bounded arrival schedule clears every owned timer in its finalizer.
// @effect-diagnostics globalTimersInEffect:off -- Actual offer timestamps measure host scheduling separately from acceptance.
import * as NodeAssert from "node:assert/strict";
import * as NodePerfHooks from "node:perf_hooks";
import {
  CommandId,
  EventId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationV2AppThread,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";

const encodeSnapshotJson = Schema.encodeSync(
  Schema.fromJsonString(
    Schema.Struct({
      sequence: Schema.Number,
      projection: Schema.Unknown,
      receipts: Schema.Array(Schema.Record(Schema.String, Schema.Unknown)),
      events: Schema.Array(Schema.Unknown),
      effects: Schema.Array(Schema.Record(Schema.String, Schema.Unknown)),
    }),
  ),
);

export interface V2Workload {
  readonly commands: number;
  readonly payloadBytes: number;
  readonly intervalMs: number;
}

export function validateV2Workload(input: V2Workload) {
  NodeAssert.deepEqual(Object.keys(input).sort(), ["commands", "intervalMs", "payloadBytes"]);
  for (const [value, minimum, maximum] of [
    [input.commands, 1, 64],
    [input.payloadBytes, 1, 4096],
    [input.intervalMs, 0, 100],
  ] as const) {
    NodeAssert.ok(
      Number.isInteger(value) && value >= minimum && value <= maximum,
      "invalid bounded V2 workload",
    );
  }
  return input;
}

function summarize(values: readonly number[]) {
  const sorted = [...values].sort((a, b) => a - b);
  return {
    samples: sorted.length,
    minimumMs: sorted[0] ?? null,
    medianMs: sorted[Math.floor(sorted.length / 2)] ?? null,
    p95Ms: sorted[Math.max(0, Math.ceil(sorted.length * 0.95) - 1)] ?? null,
    maximumMs: sorted.at(-1) ?? null,
  };
}

export const runV2Workload = (options: V2Workload) =>
  Effect.gen(function* () {
    validateV2Workload(options);
    const sink = yield* EventSink.EventSinkV2;
    const eventStore = yield* EventStore.EventStoreV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const sql = yield* SqlClient.SqlClient;
    const now = yield* DateTime.now;
    const threadId = ThreadId.make("benchmark:v2:thread");
    const instanceId = ProviderInstanceId.make("benchmark-synthetic");
    const thread: OrchestrationV2AppThread = {
      createdBy: "user",
      creationSource: "web",
      id: threadId,
      projectId: ProjectId.make("benchmark:v2:project"),
      title: "Seed",
      providerInstanceId: instanceId,
      modelSelection: { instanceId, model: "synthetic" },
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
      settledOverride: null,
      settledAt: null,
      lastVisitedAt: null,
      deletedAt: null,
    };
    yield* sink.write({
      events: [
        {
          id: EventId.make("benchmark:v2:birth"),
          threadId,
          type: "thread.created",
          providerInstanceId: instanceId,
          occurredAt: now,
          payload: thread,
        },
      ],
    });
    const input = (id: string, title: string) => ({
      commandId: CommandId.make(id),
      threadId,
      commandType: "thread.metadata.update",
      acceptedAt: now,
      events: [
        {
          id: EventId.make(`${id}:event`),
          threadId,
          type: "thread.metadata-updated" as const,
          providerInstanceId: instanceId,
          occurredAt: now,
          payload: { ...thread, title },
        },
      ],
      effects: [],
    });
    const capture = () =>
      Effect.gen(function* () {
        const sequence = yield* sink.latestSequence({ threadId });
        const projection = yield* projections.getThreadProjection(threadId);
        const receipts =
          yield* sql`SELECT command_id, status, result_sequence, error FROM orchestration_command_receipts ORDER BY command_id`;
        const events = yield* eventStore.read({ threadId }).pipe(Stream.runCollect);
        const effects = yield* sql`SELECT * FROM orchestration_v2_effect_outbox ORDER BY effect_id`;
        return encodeSnapshotJson({ sequence, projection, receipts, events, effects });
      });
    const timerLag: number[] = [],
      queueWait: number[] = [],
      dispatch: number[] = [],
      completion: number[] = [];
    const timers: ReturnType<typeof setTimeout>[] = [];
    const started = NodePerfHooks.performance.now();
    const offers = Array.from(
      { length: options.commands },
      (_, index) =>
        new Promise<{ planned: number; offered: number }>((resolve) => {
          const planned = started + index * options.intervalMs;
          timers.push(
            setTimeout(
              () => resolve({ planned, offered: NodePerfHooks.performance.now() }),
              Math.max(0, planned - NodePerfHooks.performance.now()),
            ),
          );
        }),
    );
    const commands = Array.from({ length: options.commands }, (_, index) =>
      input(`benchmark:v2:traffic:${index}`, `${index}:${"x".repeat(options.payloadBytes)}`),
    );
    yield* Effect.gen(function* () {
      for (const [index, command] of commands.entries()) {
        const arrival = yield* Effect.promise(() => offers[index]!);
        const start = NodePerfHooks.performance.now();
        const result = yield* sink.commitCommand(command);
        const end = NodePerfHooks.performance.now();
        NodeAssert.equal(result.committed, true);
        timerLag.push(arrival.offered - arrival.planned);
        queueWait.push(start - arrival.offered);
        dispatch.push(end - start);
        completion.push(end - arrival.offered);
      }
    }).pipe(Effect.ensuring(Effect.sync(() => timers.forEach(clearTimeout))));

    const protocol: number[] = [];
    const timed = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
      Effect.gen(function* () {
        const start = NodePerfHooks.performance.now();
        const value = yield* effect;
        protocol.push(NodePerfHooks.performance.now() - start);
        return value;
      });
    const beforeReplay = yield* capture();
    const replay = yield* timed(sink.commitCommand(commands[0]!));
    NodeAssert.equal(replay.committed, false);
    NodeAssert.equal(yield* capture(), beforeReplay);

    const rejectedInput = {
      commandId: CommandId.make("benchmark:v2:rejected"),
      threadId,
      commandType: "thread.metadata.update",
      rejectedAt: now,
      error: "synthetic rejection",
    };
    const rejected = yield* timed(sink.commitRejectedCommand(rejectedInput));
    NodeAssert.equal(rejected.status, "rejected");
    const beforeRejectedReplay = yield* capture();
    NodeAssert.deepEqual(yield* timed(sink.commitRejectedCommand(rejectedInput)), rejected);
    NodeAssert.equal(yield* capture(), beforeRejectedReplay);

    const retryInput = input("benchmark:v2:retry", "rollback-target");
    const beforeRollback = yield* capture();
    yield* sql`CREATE TEMP TRIGGER benchmark_v2_fail_projection BEFORE UPDATE ON orchestration_v2_projection_threads WHEN NEW.title = 'rollback-target' BEGIN SELECT RAISE(ABORT, 'synthetic benchmark rollback'); END`;
    const failed = yield* timed(sink.commitCommand(retryInput).pipe(Effect.result)).pipe(
      Effect.ensuring(sql`DROP TRIGGER benchmark_v2_fail_projection`.pipe(Effect.orDie)),
    );
    NodeAssert.equal(failed._tag, "Failure");
    NodeAssert.equal(yield* capture(), beforeRollback);
    const rollbackReceipts =
      yield* sql`SELECT command_id FROM orchestration_command_receipts WHERE command_id = ${retryInput.commandId}`;
    NodeAssert.deepEqual(rollbackReceipts, []);
    const rollbackEvents = yield* eventStore
      .readByCommandId({ commandId: retryInput.commandId })
      .pipe(Stream.runCollect);
    NodeAssert.deepEqual(rollbackEvents, []);
    NodeAssert.equal((yield* timed(sink.commitCommand(retryInput))).committed, true);
    const finalProjection = yield* projections.getThreadProjection(threadId);
    NodeAssert.equal(finalProjection.thread.title, "rollback-target");
    const finalSequence = yield* sink.latestSequence({ threadId });
    NodeAssert.equal(finalSequence, options.commands + 2);
    const integrity = yield* sql`PRAGMA integrity_check`;
    NodeAssert.deepEqual(
      integrity.map((row) => row.integrity_check),
      ["ok"],
    );
    const receiptCounts =
      yield* sql`SELECT status, COUNT(*) AS count FROM orchestration_command_receipts GROUP BY status ORDER BY status`;
    NodeAssert.deepEqual(
      receiptCounts.map((row) => [row.status, Number(row.count)]),
      [
        ["accepted", options.commands + 1],
        ["rejected", 1],
      ],
    );
    NodeAssert.deepEqual(yield* sql`PRAGMA foreign_key_check`, []);
    const engine = yield* sql`SELECT sqlite_version() AS version, sqlite_source_id() AS sourceId`;
    const journal = yield* sql`PRAGMA journal_mode`;
    return {
      schema: "jones-sqlite-v2-workload/v1",
      workload: "single-thread-metadata-acceptance",
      options,
      traffic: {
        timerLag: summarize(timerLag),
        harnessQueueWait: summarize(queueWait),
        eventSinkCompletion: summarize(dispatch),
        arrivalToCompletion: summarize(completion),
      },
      protocol: {
        eventSinkCompletion: summarize(protocol),
        acceptedReplay: true,
        rejectedReplay: true,
        rollbackUnchanged: true,
        retryCommitted: true,
      },
      consistency: {
        finalSequence,
        integrity: "ok",
        foreignKeyViolations: 0,
        projectedTitleMatches: true,
      },
      engine,
      journal,
      unavailable: [
        "individual SQL timing",
        "pure lock wait",
        "ThreadManagementService dispatch timing",
        "writer contention",
        "capacity",
        "provider execution",
      ],
    };
  });

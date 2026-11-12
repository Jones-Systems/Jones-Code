import {
  CommandId,
  type NodeId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ExecutionNode,
  OrchestrationV2ExecutionNodeJson,
  type OrchestrationV2ProviderTurn,
  OrchestrationV2ProviderTurnJson,
  type OrchestrationV2TurnItem,
  OrchestrationV2TurnItemJson,
  ProviderInstanceId,
  type ProviderTurnId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as EventSink from "../../orchestration-v2/EventSink.ts";

/**
 * Startup pass closing provider turns, nodes and items still open under ended work: a
 * terminal run, or a provider-native subagent's ended runless root turn. Nothing else ends
 * them, and they keep a thread looking busy without Stop. Runs after upstream recovery,
 * before commands or provider events are admitted.
 */

export const ORPHANED_RECORDS_COMMAND_TYPE = "provider-runtime.reconcile-orphaned-records";

/** The `command:runtime-reconcile:` prefix keeps queue, failover and delegation reactors from acting on the closures. */
export const orphanedRecordsCommandId = (threadId: ThreadId, acceptedAt: DateTime.Utc) =>
  CommandId.make(
    `command:runtime-reconcile:orphaned-records:${threadId}:${DateTime.formatIso(acceptedAt)}`,
  );

export class OrphanedRunRecordsError extends Schema.TaggedError<OrphanedRunRecordsError>()(
  "OrphanedRunRecordsError",
  { cause: Schema.Defect() },
) {
  override get message(): string {
    return "Reading orphaned run records failed.";
  }
}

export interface OrphanedRunRecordsSummary {
  readonly threads: number;
  readonly providerTurns: number;
  readonly nodes: number;
  readonly turnItems: number;
  /** Undecodable rows and records in threads whose commit failed. They stay as they were. */
  readonly skipped: number;
}

export class OrphanedRunRecords extends Context.Service<
  OrphanedRunRecords,
  {
    readonly closeAtStartup: Effect.Effect<OrphanedRunRecordsSummary, OrphanedRunRecordsError>;
  }
>()("t3/jones/orchestration/OrphanedRunRecords") {}

const LIVE_NODE_STATUSES = new Set(["pending", "starting", "running", "waiting"]);
const ENDED_RUN_STATUSES = new Set(["completed", "interrupted", "failed", "cancelled"]);
const LOOKUP_CHUNK = 400;

interface CandidateRow {
  readonly thread_id: string;
  readonly run_id: string | null;
  readonly provider_instance_id: string | null;
  readonly payload_json: string;
}

interface ItemRow extends CandidateRow {
  readonly turn_item_id: string;
  readonly node_id: string | null;
  readonly node_status: string | null;
  readonly run_status: string | null;
}

interface Closure<A> {
  readonly record: A;
  readonly runId: RunId | null;
  readonly providerInstanceId: ProviderInstanceId | null;
}

interface ThreadClosures {
  readonly providerTurns: Array<Closure<OrchestrationV2ProviderTurn>>;
  readonly nodes: Array<Closure<OrchestrationV2ExecutionNode>>;
  readonly turnItems: Array<Closure<OrchestrationV2TurnItem>>;
}

const decodeNode = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2ExecutionNodeJson),
);
const decodeProviderTurn = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2ProviderTurnJson),
);
const decodeTurnItem = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2TurnItemJson),
);

const chunked = <A>(values: ReadonlyArray<A>): ReadonlyArray<ReadonlyArray<A>> => {
  const result: Array<ReadonlyArray<A>> = [];
  for (let index = 0; index < values.length; index += LOOKUP_CHUNK) {
    result.push(values.slice(index, index + LOOKUP_CHUNK));
  }
  return result;
};

const byId = <A extends { readonly record: { readonly id: string } }>(left: A, right: A) =>
  left.record.id < right.record.id ? -1 : left.record.id > right.record.id ? 1 : 0;

const closeTurnItem = (item: OrchestrationV2TurnItem, now: DateTime.Utc): OrchestrationV2TurnItem =>
  ({
    ...item,
    status: "interrupted",
    completedAt: item.completedAt ?? now,
    updatedAt: now,
    ...("streaming" in item ? { streaming: false } : {}),
  }) as OrchestrationV2TurnItem;

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const eventSink = yield* EventSink.EventSinkV2;
  const ids = yield* IdAllocator.IdAllocatorV2;

  // Only ended runs match, so queued, held and live runs are never touched.
  // Status lists stay literal so the live-node partial index applies.
  const ended = sql.unsafe("('completed', 'interrupted', 'failed', 'cancelled')");
  // A run's provider instance, else the thread's, as the upstream stale-item pass resolves it.
  const providerInstance = sql.unsafe(
    "COALESCE(json_extract(run.payload_json, '$.providerInstanceId'), json_extract(thread.payload_json, '$.providerInstanceId'))",
  );

  const readProviderTurns = sql<CandidateRow>`
    SELECT turn.thread_id AS thread_id, attempt.run_id AS run_id,
      ${providerInstance} AS provider_instance_id, turn.payload_json AS payload_json
    FROM orchestration_v2_projection_provider_turns AS turn
    JOIN orchestration_v2_projection_threads AS thread
      ON thread.thread_id = turn.thread_id AND thread.deleted_at IS NULL
    LEFT JOIN orchestration_v2_projection_run_attempts AS attempt
      ON attempt.attempt_id = turn.run_attempt_id
    LEFT JOIN orchestration_v2_projection_runs AS run ON run.run_id = attempt.run_id
    WHERE turn.status IN ('pending', 'running')
      AND (
        run.status IN ${ended}
        OR (turn.run_attempt_id IS NULL AND EXISTS (
          SELECT 1 FROM orchestration_v2_projection_nodes AS root
          WHERE root.node_id = turn.node_id AND root.run_id IS NULL
            AND root.kind = 'root_turn' AND root.status IN ${ended}))
      )`;

  // Kept open: todo lists and plans, async questions (pending request) and active subagent rows.
  const readNodes = sql<CandidateRow>`
    SELECT node.thread_id AS thread_id, node.run_id AS run_id,
      ${providerInstance} AS provider_instance_id, node.payload_json AS payload_json
    FROM orchestration_v2_projection_nodes AS node
    JOIN orchestration_v2_projection_threads AS thread
      ON thread.thread_id = node.thread_id AND thread.deleted_at IS NULL
    LEFT JOIN orchestration_v2_projection_runs AS run ON run.run_id = node.run_id
    WHERE node.status IN ('pending', 'starting', 'running', 'waiting')
      AND node.kind NOT IN ('todo_list', 'plan')
      AND (
        run.status IN ${ended}
        OR (node.run_id IS NULL AND node.kind <> 'root_turn' AND EXISTS (
          SELECT 1 FROM orchestration_v2_projection_nodes AS root
          WHERE root.node_id = node.root_node_id AND root.run_id IS NULL
            AND root.kind = 'root_turn' AND root.status IN ${ended}))
      )
      AND NOT EXISTS (
        SELECT 1 FROM orchestration_v2_projection_runtime_requests AS request
        WHERE request.thread_id = node.thread_id AND request.status = 'pending'
          AND request.node_id = node.node_id)
      AND NOT EXISTS (
        SELECT 1 FROM orchestration_v2_projection_subagents AS task
        WHERE task.subagent_id = node.node_id
          AND task.status IN ('pending', 'starting', 'running', 'waiting'))`;

  // Open items reached from closed records or their ended runs, through the item indexes.
  // Background-capable items stay with the upstream stale-item pass.
  const readItems = (
    column: "node_id" | "provider_turn_id" | "run_id",
    values: ReadonlyArray<string>,
  ) =>
    sql<ItemRow>`
      SELECT item.turn_item_id AS turn_item_id, item.thread_id AS thread_id,
        item.run_id AS run_id, item.node_id AS node_id, node.status AS node_status,
        run.status AS run_status, ${providerInstance} AS provider_instance_id,
        item.payload_json AS payload_json
      FROM orchestration_v2_projection_turn_items AS item
      JOIN orchestration_v2_projection_threads AS thread
        ON thread.thread_id = item.thread_id AND thread.deleted_at IS NULL
      LEFT JOIN orchestration_v2_projection_nodes AS node ON node.node_id = item.node_id
      LEFT JOIN orchestration_v2_projection_runs AS run ON run.run_id = item.run_id
      WHERE ${sql.unsafe(`item.${column}`)} IN ${sql.in(values)}
        AND item.status IN ('pending', 'running', 'waiting')
        AND item.type NOT IN (
          'command_execution', 'dynamic_tool', 'subagent', 'todo_list', 'proposed_plan')
        AND NOT EXISTS (
          SELECT 1 FROM orchestration_v2_projection_runtime_requests AS request
          WHERE request.thread_id = item.thread_id AND request.status = 'pending'
            AND request.node_id = item.node_id)`;

  const findClosures = Effect.gen(function* () {
    let skipped = 0;
    const byThread = new Map<ThreadId, ThreadClosures>();
    const closuresFor = (threadId: string) => {
      const key = ThreadId.make(threadId);
      const existing = byThread.get(key);
      if (existing !== undefined) return existing;
      const created: ThreadClosures = { providerTurns: [], nodes: [], turnItems: [] };
      byThread.set(key, created);
      return created;
    };
    const decoded = <A>(
      decode: (json: string) => Effect.Effect<A, Schema.SchemaError>,
      row: CandidateRow,
    ) =>
      decode(row.payload_json).pipe(
        Effect.map((record) => ({
          record,
          runId: row.run_id === null ? null : RunId.make(row.run_id),
          providerInstanceId:
            row.provider_instance_id === null
              ? null
              : ProviderInstanceId.make(row.provider_instance_id),
        })),
        Effect.catch((cause) => {
          skipped += 1;
          return Effect.logWarning("orchestration-v2.orphaned-run-records.undecodable-row", {
            threadId: row.thread_id,
            cause,
          }).pipe(Effect.as(undefined));
        }),
      );

    const closedTurnIds = new Set<ProviderTurnId>();
    const closedNodeIds = new Set<NodeId>();
    const endedRunIds = new Set<RunId>();
    for (const row of yield* readProviderTurns) {
      const closure = yield* decoded(decodeProviderTurn, row);
      if (closure === undefined) continue;
      closuresFor(row.thread_id).providerTurns.push(closure);
      closedTurnIds.add(closure.record.id);
      if (closure.runId !== null) endedRunIds.add(closure.runId);
    }
    for (const row of yield* readNodes) {
      const closure = yield* decoded(decodeNode, row);
      if (closure === undefined) continue;
      closuresFor(row.thread_id).nodes.push(closure);
      closedNodeIds.add(closure.record.id);
      if (closure.runId !== null) endedRunIds.add(closure.runId);
    }

    const itemRows = new Map<string, ItemRow>();
    for (const [column, values] of [
      ["node_id", [...closedNodeIds]],
      ["provider_turn_id", [...closedTurnIds]],
      ["run_id", [...endedRunIds]],
    ] as const) {
      for (const chunk of chunked<string>(values)) {
        for (const row of yield* readItems(column, chunk)) itemRows.set(row.turn_item_id, row);
      }
    }
    for (const row of itemRows.values()) {
      // An item stays with a node left open by design, and with any run that has not ended.
      const keptByNode =
        row.node_id !== null &&
        !closedNodeIds.has(row.node_id as NodeId) &&
        row.node_status !== null &&
        LIVE_NODE_STATUSES.has(row.node_status);
      const keptByRun =
        row.run_id !== null && (row.run_status === null || !ENDED_RUN_STATUSES.has(row.run_status));
      if (keptByNode || keptByRun) continue;
      const closure = yield* decoded(decodeTurnItem, row);
      if (closure !== undefined) closuresFor(row.thread_id).turnItems.push(closure);
    }
    return { byThread, skipped };
  }).pipe(Effect.mapError((cause) => new OrphanedRunRecordsError({ cause })));

  const commitThread = Effect.fn("OrphanedRunRecords.commitThread")(function* (
    threadId: ThreadId,
    closures: ThreadClosures,
    now: DateTime.Utc,
  ) {
    const commandId = orphanedRecordsCommandId(threadId, now);
    const scope = (closure: Closure<unknown>, nodeId: NodeId | null) => ({
      threadId,
      ...(closure.runId === null ? {} : { runId: closure.runId }),
      ...(nodeId === null ? {} : { nodeId }),
      ...(closure.providerInstanceId === null
        ? {}
        : { providerInstanceId: closure.providerInstanceId }),
      occurredAt: now,
    });
    const events: Array<OrchestrationV2DomainEvent> = [];
    for (const closure of [...closures.providerTurns].sort(byId)) {
      events.push({
        ...scope(closure, closure.record.nodeId),
        id: yield* ids.allocate.event({ threadId, commandId }),
        type: "provider-turn.updated",
        payload: {
          ...closure.record,
          status: "interrupted",
          completedAt: closure.record.completedAt ?? now,
        },
      });
    }
    for (const closure of [...closures.nodes].sort(byId)) {
      events.push({
        ...scope(closure, closure.record.id),
        id: yield* ids.allocate.event({ threadId, commandId }),
        type: "node.updated",
        payload: {
          ...closure.record,
          status: "interrupted",
          completedAt: closure.record.completedAt ?? now,
        },
      });
    }
    for (const closure of [...closures.turnItems].sort(byId)) {
      events.push({
        ...scope(closure, closure.record.nodeId),
        id: yield* ids.allocate.event({ threadId, commandId }),
        type: "turn-item.updated",
        payload: closeTurnItem(closure.record, now),
      });
    }
    const result = yield* eventSink.commitCommand({
      commandId,
      threadId,
      commandType: ORPHANED_RECORDS_COMMAND_TYPE,
      acceptedAt: now,
      events,
      effects: [],
    });
    yield* Effect.logInfo("orchestration-v2.orphaned-run-records.closed-thread", {
      threadId,
      commandId,
      committed: result.committed,
      providerTurnIds: closures.providerTurns.map((closure) => closure.record.id),
      nodeIds: closures.nodes.map((closure) => closure.record.id),
      turnItemIds: closures.turnItems.map((closure) => closure.record.id),
    });
    return result.committed;
  });

  const closeAtStartup = Effect.gen(function* () {
    const now = yield* DateTime.now;
    const found = yield* findClosures;
    let skipped = found.skipped;
    let threads = 0;
    let providerTurns = 0;
    let nodes = 0;
    let turnItems = 0;
    for (const [threadId, closures] of found.byThread) {
      const count =
        closures.providerTurns.length + closures.nodes.length + closures.turnItems.length;
      if (count === 0) continue;
      // One thread's failure leaves its records for the next start and does not block the others.
      const committed = yield* commitThread(threadId, closures, now).pipe(
        Effect.catchCause((cause) =>
          Cause.hasInterruptsOnly(cause)
            ? Effect.interrupt
            : Effect.logWarning("orchestration-v2.orphaned-run-records.commit-failed", {
                threadId,
                cause,
              }).pipe(Effect.as(false)),
        ),
      );
      if (!committed) {
        skipped += count;
        continue;
      }
      threads += 1;
      providerTurns += closures.providerTurns.length;
      nodes += closures.nodes.length;
      turnItems += closures.turnItems.length;
    }
    const summary = { threads, providerTurns, nodes, turnItems, skipped };
    if (threads > 0 || skipped > 0) {
      yield* Effect.logInfo("orchestration-v2.orphaned-run-records.closed", summary);
    }
    return summary satisfies OrphanedRunRecordsSummary;
  });

  return OrphanedRunRecords.of({ closeAtStartup });
});

export const layer = Layer.effect(OrphanedRunRecords, make);

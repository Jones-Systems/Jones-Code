import {
  MessageId,
  NodeId,
  RunId,
  TurnItemId,
  type OrchestrationV2Run,
  type OrchestrationV2TurnItem,
  type OrchestrationV2UserMessageInputIntent,
  type ThreadId,
} from "@t3tools/contracts";
import { createOrchestrationV2TurnItemVisibility } from "@t3tools/shared/orchestrationV2Timeline";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import {
  createThreadHistoryPageBudget,
  decodeThreadHistoryCursor,
  encodeThreadHistoryCursor,
  isThreadHistoryUserTurn,
  OLDER_THREAD_USER_TURN_LIMIT,
  THREAD_HISTORY_PAGE_POLICY,
} from "../../orchestration-v2/threadHistoryPaging.ts";

type Metadata = {
  readonly turn_item_id: string;
  readonly ordinal: number;
  readonly run_id: string | null;
  readonly node_id: string | null;
  readonly type: OrchestrationV2TurnItem["type"];
  readonly input_intent: OrchestrationV2UserMessageInputIntent | null;
  readonly created_by: "user" | "agent" | "system" | null;
  readonly message_id: string | null;
};

type RunFacts = {
  readonly run_id: string;
  readonly status: OrchestrationV2Run["status"] | null;
  readonly has_request: number;
  readonly first_input_id: string | null;
};

const metadataItem = (threadId: ThreadId, row: Metadata) => ({
  id: TurnItemId.make(row.turn_item_id),
  threadId,
  runId: row.run_id === null ? null : RunId.make(row.run_id),
  nodeId: row.node_id === null ? null : NodeId.make(row.node_id),
  type: row.type,
  ...(row.input_intent === null ? {} : { inputIntent: row.input_intent }),
  ...(row.created_by === null ? {} : { createdBy: row.created_by }),
  ...(row.message_id === null ? {} : { messageId: MessageId.make(row.message_id) }),
});

/** Caller owns the transaction, thread/fork guard, and the original fallback. */
export const readLocalHistoryPage = Effect.fn("ProjectionStore.readLocalHistoryPage")(
  function* (threadId: ThreadId, cursor: string, snapshotSequence: number) {
    let decoded: ReturnType<typeof decodeThreadHistoryCursor>;
    try {
      decoded = decodeThreadHistoryCursor(cursor);
    } catch {
      return null;
    }
    if (decoded.st !== String(threadId)) return null;
    const sql = yield* SqlClient.SqlClient;
    const columns = sql`turn_item_id, ordinal, run_id, node_id, type,
      CASE WHEN type = 'user_message' THEN json_extract(payload_json, '$.inputIntent') END AS input_intent,
      CASE WHEN type = 'user_message' THEN json_extract(payload_json, '$.createdBy') END AS created_by,
      CASE WHEN type IN ('user_message', 'assistant_message') THEN json_extract(payload_json, '$.messageId') END AS message_id`;
    const anchors = yield* sql<Metadata>`SELECT ${columns}
      FROM orchestration_v2_projection_turn_items
      WHERE thread_id = ${threadId} AND turn_item_id = ${decoded.si}`;
    const anchor = anchors[0];
    if (anchor === undefined) return null;

    // Restrict correction work to runs/attempts that can hide rows. The separate
    // raw prefix count below stays covering-index-only, but is still O(N).
    const hidden = sql`WITH hidden AS (
      SELECT item.rowid AS item_rowid
      FROM orchestration_v2_projection_runs AS run
      CROSS JOIN orchestration_v2_projection_turn_items AS item ON item.run_id = run.run_id
      WHERE run.thread_id = ${threadId} AND run.status = 'rolled_back'
        AND item.thread_id = ${threadId}
        AND (item.ordinal, item.turn_item_id) < (${anchor.ordinal}, ${anchor.turn_item_id})
      UNION
      SELECT item.rowid
      FROM orchestration_v2_projection_runs AS run
      CROSS JOIN orchestration_v2_projection_turn_items AS item ON item.run_id = run.run_id
      WHERE run.thread_id = ${threadId} AND run.status = 'cancelled'
        AND item.thread_id = ${threadId} AND item.type = 'user_message'
        AND (item.ordinal, item.turn_item_id) < (${anchor.ordinal}, ${anchor.turn_item_id})
        AND json_extract(item.payload_json, '$.inputIntent') = 'queued_turn'
      UNION
      SELECT item.rowid
      FROM orchestration_v2_projection_run_attempts AS attempt
      CROSS JOIN orchestration_v2_projection_turn_items AS item ON item.run_id = attempt.run_id
      WHERE attempt.thread_id = ${threadId} AND attempt.status = 'superseded'
        AND item.thread_id = ${threadId} AND item.type = 'run_interrupt_result'
        AND (item.ordinal, item.turn_item_id) < (${anchor.ordinal}, ${anchor.turn_item_id})
        AND item.node_id = attempt.root_node_id
        AND NOT EXISTS (
          SELECT 1 FROM orchestration_v2_projection_turn_items AS request
          WHERE request.run_id = item.run_id AND request.thread_id = ${threadId}
            AND request.type = 'run_interrupt_request'
        )
    )`;
    const hiddenAnchor = yield* sql<{ readonly hidden: number }>`
      SELECT 1 AS hidden WHERE EXISTS (
        SELECT 1 FROM orchestration_v2_projection_runs
        WHERE thread_id = ${threadId} AND run_id = ${anchor.run_id}
          AND (status = 'rolled_back' OR (status = 'cancelled'
            AND ${anchor.type} = 'user_message' AND ${anchor.input_intent} = 'queued_turn'))
      ) OR (${anchor.type} = 'run_interrupt_result' AND EXISTS (
        SELECT 1 FROM orchestration_v2_projection_run_attempts
        WHERE thread_id = ${threadId} AND run_id = ${anchor.run_id}
          AND root_node_id = ${anchor.node_id} AND status = 'superseded'
      ) AND NOT EXISTS (
        SELECT 1 FROM orchestration_v2_projection_turn_items
        WHERE thread_id = ${threadId} AND run_id = ${anchor.run_id}
          AND type = 'run_interrupt_request'
      ))`;
    if (hiddenAnchor.length > 0) return null;

    const counts = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count
      FROM orchestration_v2_projection_turn_items
      WHERE thread_id = ${threadId} AND (ordinal, turn_item_id) < (${anchor.ordinal}, ${anchor.turn_item_id})`;
    const correction = yield* sql<{
      readonly hidden_count: number;
      readonly has_users: number;
    }>`${hidden} SELECT (SELECT COUNT(*) FROM hidden) AS hidden_count, EXISTS (
      SELECT 1 FROM orchestration_v2_projection_turn_items AS item
      WHERE item.thread_id = ${threadId} AND item.type = 'user_message'
        AND (item.ordinal, item.turn_item_id) < (${anchor.ordinal}, ${anchor.turn_item_id})
        AND json_extract(item.payload_json, '$.inputIntent') IN ('turn_start', 'queued_turn')
        AND json_extract(item.payload_json, '$.createdBy') = 'user'
        AND item.rowid NOT IN (SELECT item_rowid FROM hidden)
    ) AS has_users`;
    const anchorPosition = (counts[0]?.count ?? 0) - (correction[0]?.hidden_count ?? 0);
    const hasUserTurns = correction[0]?.has_users === 1;
    const budget = createThreadHistoryPageBudget(
      { ...THREAD_HISTORY_PAGE_POLICY, maxUserTurns: OLDER_THREAD_USER_TURN_LIMIT },
      hasUserTurns,
    );
    const facts = new Map<string, RunFacts>();
    const readInputs = new Set<string>();
    const readInterrupts = new Set<string>();
    const superseded = new Map<string, ReadonlyArray<string>>();
    const selected: Array<{
      sourceThreadId: ThreadId;
      sourceItemId: TurnItemId;
      visibility: "local";
      item: ReturnType<typeof metadataItem>;
      position: number;
      canonicalPosition: number;
      synthetic?: OrchestrationV2TurnItem;
    }> = [];
    let boundary = anchor;
    let done = false;
    while (!done) {
      const rows = yield* sql<Metadata>`SELECT ${columns}
        FROM orchestration_v2_projection_turn_items
        WHERE thread_id = ${threadId}
          AND (ordinal, turn_item_id) < (${boundary.ordinal}, ${boundary.turn_item_id})
        ORDER BY ordinal DESC, turn_item_id DESC LIMIT ${THREAD_HISTORY_PAGE_POLICY.maxItems + 2}`;
      if (rows.length === 0) break;
      const runIds = [
        ...new Set(rows.flatMap((row) => (row.run_id === null ? [] : [row.run_id]))),
      ];
      const inputRuns = new Set(
        rows.flatMap((row) =>
          row.type === "notification" && row.run_id !== null ? [row.run_id] : [],
        ),
      );
      const interruptRuns = new Set(
        rows.flatMap((row) =>
          row.type === "run_interrupt_result" && row.run_id !== null ? [row.run_id] : [],
        ),
      );
      const unseen = runIds.filter(
        (id) =>
          !facts.has(id) ||
          (inputRuns.has(id) && !readInputs.has(id)) ||
          (interruptRuns.has(id) && !readInterrupts.has(id)),
      );
      if (unseen.length > 0) {
        const runFacts = yield* sql<RunFacts>`SELECT wanted.value AS run_id, run.status,
          CASE WHEN wanted.value IN (SELECT value FROM json_each(${JSON.stringify([...interruptRuns])}))
          THEN EXISTS (SELECT 1 FROM orchestration_v2_projection_turn_items AS request
            WHERE request.thread_id = ${threadId} AND request.run_id = wanted.value
              AND request.type = 'run_interrupt_request') ELSE 0 END AS has_request,
          CASE WHEN wanted.value IN (SELECT value FROM json_each(${JSON.stringify([...inputRuns])}))
          THEN (SELECT first.turn_item_id FROM orchestration_v2_projection_turn_items AS first
            WHERE first.thread_id = ${threadId} AND first.run_id = wanted.value
              AND first.type IN ('user_message', 'notification')
              AND (run.status IS NULL OR run.status <> 'rolled_back')
              AND NOT (first.type = 'user_message'
                AND json_extract(first.payload_json, '$.inputIntent') = 'queued_turn'
                AND run.status IS 'cancelled')
            ORDER BY first.ordinal ASC, first.turn_item_id ASC LIMIT 1) END AS first_input_id
          FROM json_each(${JSON.stringify(unseen)}) AS wanted
          LEFT JOIN orchestration_v2_projection_runs AS run
            ON run.thread_id = ${threadId} AND run.run_id = wanted.value`;
        for (const fact of runFacts) {
          const previous = facts.get(fact.run_id);
          facts.set(fact.run_id, {
            ...fact,
            first_input_id: inputRuns.has(fact.run_id)
              ? fact.first_input_id
              : previous?.first_input_id ?? null,
            has_request: interruptRuns.has(fact.run_id)
              ? fact.has_request
              : previous?.has_request ?? 0,
          });
          if (inputRuns.has(fact.run_id)) readInputs.add(fact.run_id);
          if (interruptRuns.has(fact.run_id)) readInterrupts.add(fact.run_id);
        }
      }
      const unreadAttempts = [...interruptRuns].filter((id) => !superseded.has(id));
      if (unreadAttempts.length > 0) {
        const attempts = yield* sql<{ readonly run_id: string; readonly root_node_id: string }>`
          SELECT run_id, root_node_id FROM orchestration_v2_projection_run_attempts
          WHERE thread_id = ${threadId} AND run_id IN (SELECT value FROM json_each(${JSON.stringify(unreadAttempts)}))
            AND status = 'superseded'`;
        for (const id of unreadAttempts) {
          superseded.set(
            id,
            attempts
              .filter((attempt) => attempt.run_id === id)
              .map((attempt) => attempt.root_node_id),
          );
        }
      }
      const isVisible = createOrchestrationV2TurnItemVisibility({
        runs: runIds.flatMap((id) => {
          const status = facts.get(id)?.status;
          return status == null ? [] : [{ id: RunId.make(id), status }];
        }),
        attempts: [...interruptRuns].flatMap((id) =>
          (superseded.get(id) ?? []).map((root) => ({
            runId: RunId.make(id),
            rootNodeId: NodeId.make(root),
            status: "superseded" as const,
          })),
        ),
        items: runIds
          .filter((id) => facts.get(id)?.has_request)
          .map((id) => ({
            type: "run_interrupt_request",
            runId: RunId.make(id),
            nodeId: null,
          })),
      });
      for (const row of rows) {
        const item = metadataItem(threadId, row);
        if (!isVisible(item)) continue;
        const userTurn = isThreadHistoryUserTurn(item);
        const turnStart =
          row.type === "notification"
            ? row.run_id !== null && facts.get(row.run_id)?.first_input_id === row.turn_item_id
            : row.type === "user_message" &&
              (row.input_intent === "turn_start" || row.input_intent === "queued_turn");
        if (!budget.accept({ userTurn, turnStart, encodedBytes: 0 })) {
          done = true;
          break;
        }
        const position = anchorPosition - selected.length - 1;
        selected.push({
          sourceThreadId: threadId,
          sourceItemId: item.id,
          visibility: "local",
          item,
          position,
          canonicalPosition: position,
        });
      }
      boundary = rows[rows.length - 1]!;
    }
    selected.reverse();
    const oldest = selected[0];
    const hasMoreHistory = oldest !== undefined && oldest.canonicalPosition > 0;
    return {
      hasUserTurns,
      page: {
        items: selected.map((row, position) => ({ ...row, position })),
        nextCursor:
          hasMoreHistory && oldest
            ? encodeThreadHistoryCursor({
                snapshotSequence,
                sourceThreadId: threadId,
                sourceItemId: oldest.sourceItemId,
                position: oldest.canonicalPosition,
              })
            : null,
        hasMoreHistory,
      },
    };
  },
);

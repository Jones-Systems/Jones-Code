import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { strict as assert } from "node:assert";
import {
  EventId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  RunId,
  MessageId,
  NodeId,
  OrchestrationV2ConversationMessageJson,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";

// Benchmark-only successor to #63/#71. Legacy source c4c68bb and 77c0113
// remains historical evidence: its mixed/negative timings do not measure V2.
export const historyExperimentLayer = ProjectionStore.layer.pipe(
  Layer.provideMerge(SqlitePersistenceMemory),
);
export const historyThreadId = ThreadId.make("history-experiment");
const iso = "2026-10-02T00:00:00.000Z";
const now = DateTime.makeUnsafe(iso);
export const historyMessageId = (index: number) => `message-${String(index).padStart(6, "0")}`;
const decodeMessage = Schema.decodeUnknownSync(
  Schema.fromJsonString(OrchestrationV2ConversationMessageJson),
);
export const mapHistoryRows = (rows: ReadonlyArray<{ payload_json: string }>) =>
  rows.map((row) => decodeMessage(row.payload_json));

// Bind the baseline to its production owner, rejecting unknown interpolations.
// No schema/index changes are made for the UNION variant.
const source = readFileSync(
  new URL("../../orchestration-v2/ProjectionStore.ts", import.meta.url),
  "utf8",
);
const match = source.match(
  /SELECT payload_json FROM orchestration_v2_projection_messages AS message[\s\S]*?ORDER BY created_at ASC, message_id ASC/,
);
assert.ok(match, "V2 bounded message SQL must still exist");
export const historyBaselineSql = match[0];
export const historySourceSha256 = createHash("sha256").update(source).digest("hex");
const activeRuns =
  "SELECT run_id FROM orchestration_v2_projection_runs WHERE thread_id = ? AND status IN ('queued', 'preparing', 'starting', 'running', 'waiting')";
export function historyStatements(ids: ReadonlyArray<string>) {
  const cohort = JSON.stringify(ids);
  const values: string[] = [];
  const text = historyBaselineSql.replace(/\$\{([^}]+)\}/g, (_, name: string) => {
    assert.ok(
      name === "threadId" || name === "cohortMessageIds",
      `Unbound production parameter: ${name}`,
    );
    values.push(name === "threadId" ? historyThreadId : cohort);
    return "?";
  });
  return [
    { name: "production-or", text, values },
    {
      name: "disjoint-union",
      text: `SELECT payload_json FROM (
        SELECT payload_json, created_at, message_id FROM orchestration_v2_projection_messages
        WHERE thread_id = ? AND message_id IN (SELECT value FROM json_each(?))
        UNION ALL
        SELECT payload_json, created_at, message_id FROM orchestration_v2_projection_messages
        WHERE thread_id = ? AND run_id IN (${activeRuns})
          AND message_id NOT IN (SELECT value FROM json_each(?))
      ) ORDER BY created_at ASC, message_id ASC`,
      values: [historyThreadId, cohort, historyThreadId, historyThreadId, cohort],
    },
  ] as const;
}

export const seedHistory = Effect.fn("seedV2HistoryExperiment")(function* (count: number) {
  const sql = yield* SqlClient.SqlClient;
  const store = yield* ProjectionStore.ProjectionStoreV2;
  const instanceId = ProviderInstanceId.make("codex");
  yield* store.apply({
    id: EventId.make("history-thread-created"),
    type: "thread.created",
    threadId: historyThreadId,
    occurredAt: now,
    payload: {
      id: historyThreadId,
      projectId: ProjectId.make("history-project"),
      title: "Synthetic history",
      providerInstanceId: instanceId,
      modelSelection: { instanceId, model: "gpt-5.4" },
      createdBy: "user",
      creationSource: "web",
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      activeProviderThreadId: null,
      lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: historyThreadId },
      forkedFrom: null,
      createdAt: now,
      updatedAt: now,
      archivedAt: null,
      deletedAt: null,
      settledOverride: null,
      settledAt: null,
      lastVisitedAt: null,
    },
  });
  yield* store.apply({
    id: EventId.make("history-run-created"),
    type: "run.created",
    threadId: historyThreadId,
    occurredAt: now,
    payload: {
      id: RunId.make("active-run"),
      threadId: historyThreadId,
      ordinal: 1,
      providerInstanceId: instanceId,
      modelSelection: { instanceId, model: "gpt-5.4" },
      providerThreadId: null,
      userMessageId: MessageId.make(historyMessageId(0)),
      rootNodeId: NodeId.make("active-root"),
      activeAttemptId: null,
      status: "running",
      requestedAt: now,
      startedAt: now,
      completedAt: null,
      checkpointId: null,
      contextHandoffId: null,
    },
  });
  // Fixed timestamps force the secondary ID ordering; alternating null run IDs
  // and active-run membership exercise overlap and SQL NULL behavior.
  for (let start = 0; start < count; start += 100) {
    const indexes = Array.from(
      { length: Math.min(100, count - start) },
      (_, offset) => start + offset,
    );
    yield* sql`INSERT INTO orchestration_v2_projection_messages ${sql.insert(
      indexes.map((index) => {
        const id = historyMessageId(index);
        const runId = index % 3 === 0 ? "active-run" : null;
        return {
          message_id: id,
          thread_id: historyThreadId,
          run_id: runId,
          node_id: null,
          role: "user",
          streaming: 0,
          created_at: iso,
          updated_at: iso,
          payload_json: JSON.stringify({
            id,
            threadId: historyThreadId,
            runId,
            nodeId: null,
            role: "user",
            text: `Message ${index}`,
            attachments: [],
            streaming: false,
            createdBy: "user",
            creationSource: "web",
            createdAt: iso,
            updatedAt: iso,
          }),
        };
      }),
    )}`;
    yield* sql`INSERT INTO orchestration_v2_projection_turn_items ${sql.insert(
      indexes.map((index) => {
        const id = `item-${String(index).padStart(6, "0")}`;
        const item = {
          id,
          threadId: historyThreadId,
          runId: null,
          nodeId: null,
          providerThreadId: null,
          providerTurnId: null,
          nativeItemRef: null,
          parentItemId: null,
          ordinal: index + 1,
          status: "completed",
          title: null,
          startedAt: iso,
          completedAt: iso,
          updatedAt: iso,
          type: "user_message",
          messageId: historyMessageId(index),
          text: `Message ${index}`,
          attachments: [],
          createdBy: "user",
          creationSource: "web",
          inputIntent: "turn_start",
        };
        return {
          turn_item_id: id,
          thread_id: historyThreadId,
          run_id: null,
          node_id: null,
          provider_thread_id: null,
          provider_turn_id: null,
          parent_item_id: null,
          ordinal: index + 1,
          type: item.type,
          status: item.status,
          updated_at: iso,
          payload_json: JSON.stringify(item),
        };
      }),
    )}`;
  }
});

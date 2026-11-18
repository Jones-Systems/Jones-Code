import { assert, it } from "@effect/vitest";
import { RunId, ThreadId } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Records from "./RetainedRecords.ts";

const encodeHistoricalJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));

const memory = NodeSqliteClient.layer({ filename: ":memory:" });
const threadId = ThreadId.make("retained:T");
const runId = RunId.make("retained:R");
const date = "2026-11-17T10:00:00.000Z";
const policy = {
  version: 1,
  createCommandId: "create",
  birthCommandId: "birth",
  releaseCommandId: "C",
  threadId,
  projectId: "P",
  messageId: "M",
  payloadHash: "historical",
  ownsNewThread: false,
};
const thread = {
  id: threadId,
  projectId: "P",
  createdBy: "user",
  creationSource: "web",
  title: "Retained",
  providerInstanceId: "codex",
  modelSelection: { instanceId: "codex", model: "test" },
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  activeProviderThreadId: null,
  lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
  forkedFrom: null,
  createdAt: date,
  updatedAt: date,
  archivedAt: null,
  settledOverride: null,
  settledAt: null,
  lastVisitedAt: null,
  deletedAt: null,
  legacyBootstrapClaim: policy,
};
const run = {
  id: runId,
  threadId,
  ordinal: 1,
  providerInstanceId: "codex",
  modelSelection: { instanceId: "codex", model: "test" },
  providerThreadId: null,
  userMessageId: "M",
  rootNodeId: null,
  activeAttemptId: null,
  status: "preparing",
  requestedAt: date,
  startedAt: null,
  completedAt: null,
  checkpointId: null,
  contextHandoffId: null,
  legacyBootstrap: { ...policy, runId },
  workspaceRunSetupScript: false,
};
const seed = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE orchestration_v2_projection_threads(thread_id TEXT PRIMARY KEY,payload_json TEXT)`;
  yield* sql`CREATE TABLE orchestration_v2_projection_runs(run_id TEXT PRIMARY KEY,thread_id TEXT,ordinal INTEGER,payload_json TEXT)`;
  // Literal historical fixture bytes; this is deliberately not a fresh public write path.
  yield* sql`INSERT INTO orchestration_v2_projection_threads VALUES (${threadId},${yield* encodeHistoricalJson(thread)})`;
  yield* sql`INSERT INTO orchestration_v2_projection_runs VALUES (${runId},${threadId},1,${yield* encodeHistoricalJson(run)})`;
  return sql;
});
it.effect(
  "historical inline fallback needs no Jones schema and preserves the explicit empty run filter",
  () =>
    Effect.gen(function* () {
      const sql = yield* seed;
      const read = yield* Records.make;
      const historical = yield* read(threadId);
      assert.deepStrictEqual(historical.thread.legacyBootstrapClaim, policy);
      assert.equal(historical.runs[0]?.workspaceRunSetupScript, false);
      assert.equal((yield* read(threadId, { runIds: [runId] })).runs.length, 1);
      yield* sql`UPDATE orchestration_v2_projection_runs SET payload_json = 'corrupt'`;
      assert.deepStrictEqual((yield* read(threadId, { runIds: [] })).runs, []);
      assert.isTrue(Exit.isFailure(yield* read(threadId).pipe(Effect.exit)));
    }).pipe(Effect.provide(memory)),
);
it.effect("missing or mismatched upstream rows remain unavailable", () =>
  Effect.gen(function* () {
    const sql = yield* seed;
    const read = yield* Records.make;
    yield* sql`UPDATE orchestration_v2_projection_runs SET run_id = 'replacement'`;
    assert.isTrue(Exit.isFailure(yield* read(threadId).pipe(Effect.exit)));
    yield* sql`DELETE FROM orchestration_v2_projection_threads`;
    assert.isTrue(Exit.isFailure(yield* read(threadId, { runIds: [] }).pipe(Effect.exit)));
  }).pipe(Effect.provide(memory)),
);
it.effect("an absent SQL capability is a typed private-reader failure", () =>
  Effect.gen(function* () {
    const read = yield* Records.make;
    assert.isTrue(Exit.isFailure(yield* read(threadId).pipe(Effect.exit)));
  }),
);

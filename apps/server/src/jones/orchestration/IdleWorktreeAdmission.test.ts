import { assert, it } from "@effect/vitest";
import { CommandId, ThreadId } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/sql/SqlClient";
import migration from "../persistence/Migrations/135_JonesIdleWorktreeAdmission.ts";
import { makeIdleWorktreeAdmission, type Intent } from "./IdleWorktreeAdmission.ts";

const memory = NodeSqliteClient.layer({ filename: ":memory:" });
const intent: Intent = {
  threadId: ThreadId.make("idle"),
  commandId: CommandId.make("launch-idle"),
  requestKey: "requested-worktree",
  generation: "generation-1",
  claimType: "thread.create",
};
const fixture = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* migration;
  yield* sql`CREATE TABLE orchestration_v2_projection_threads
    (thread_id TEXT PRIMARY KEY, payload_json TEXT NOT NULL, deleted_at TEXT)`;
  yield* sql`INSERT INTO orchestration_v2_projection_threads VALUES ('idle', '{"worktreePath":null}', NULL)`;
  const store = makeIdleWorktreeAdmission(sql);
  yield* store.accept({
    intent,
    commandId: intent.commandId,
    threadId: intent.threadId,
    commandType: intent.claimType,
    events: [{ type: "thread.created", threadId: intent.threadId }],
  });
  const workspace = sql`UPDATE orchestration_v2_projection_threads
    SET payload_json = '{"worktreePath":"/worktrees/idle"}' WHERE thread_id = 'idle'`;
  return { sql, store, workspace };
});

it.effect("requires committed workspace metadata and the exact generation before readiness", () =>
  Effect.gen(function* () {
    const { store, workspace } = yield* fixture;
    yield* store.issueWorktree(intent);
    assert.equal(
      (yield* store.recordWorkspace(intent, "/worktrees/idle").pipe(Effect.flip)).reason,
      "identity_conflict",
    );
    assert.equal((yield* store.read(intent.threadId))?.phase, "worktree_issued");
    yield* workspace;
    yield* store.recordWorkspace(intent, "/worktrees/idle");
    yield* store.issueSetup(intent);
    assert.equal((yield* store.release(intent).pipe(Effect.flip)).reason, "unresolved");
    yield* store.prepareRelease(intent, "/worktrees/idle");
    assert.isFalse(yield* store.cleanupAllowed(intent));
    yield* store.release(intent);
    yield* store.release(intent);
    yield* store.markUnresolved(intent);
    assert.equal((yield* store.read(intent.threadId))?.phase, "ready");
    assert.isFalse(yield* store.cleanupAllowed(intent));
  }).pipe(Effect.provide(memory)),
);

it.effect(
  "keeps issued outcomes held across reconstruction and replays only completed preparation",
  () =>
    Effect.gen(function* () {
      const { sql, store, workspace } = yield* fixture;
      yield* store.issueWorktree(intent);
      const restarted = makeIdleWorktreeAdmission(sql);
      assert.equal((yield* restarted.read(intent.threadId))?.phase, "worktree_issued");
      assert.equal((yield* restarted.issueWorktree(intent).pipe(Effect.flip)).reason, "unresolved");
      yield* workspace;
      yield* store.recordWorkspace(intent, "/worktrees/idle");
      yield* store.prepareRelease(intent, "/worktrees/idle");
      yield* restarted.release(intent);
      assert.equal((yield* restarted.read(intent.threadId))?.phase, "ready");
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "stale generations cannot release, recover cleanup ownership, or mark a ready checkout unresolved",
  () =>
    Effect.gen(function* () {
      const { store, workspace } = yield* fixture;
      yield* store.issueWorktree(intent);
      yield* workspace;
      yield* store.recordWorkspace(intent, "/worktrees/idle");
      yield* store.prepareRelease(intent, "/worktrees/idle");
      const stale = { ...intent, generation: "old-generation" };
      assert.equal((yield* store.release(stale).pipe(Effect.flip)).reason, "identity_conflict");
      assert.equal(
        (yield* store.markUnresolved(stale).pipe(Effect.flip)).reason,
        "identity_conflict",
      );
      assert.isFalse(yield* store.cleanupAllowed(stale));
      assert.isFalse(yield* store.cleanupAllowed(intent));
      assert.equal((yield* store.read(intent.threadId))?.phase, "prepared");
      yield* store.release(intent);
      yield* store.markUnresolved(intent);
      assert.equal((yield* store.read(intent.threadId))?.phase, "ready");
    }).pipe(Effect.provide(memory)),
);

it.effect("treats deleted upstream threads and unmarked null worktrees as absent", () =>
  Effect.gen(function* () {
    const { sql, store } = yield* fixture;
    yield* sql`INSERT INTO orchestration_v2_projection_threads VALUES ('unmarked', '{"worktreePath":null}', NULL)`;
    assert.isNull(yield* store.read(ThreadId.make("unmarked")));
    yield* sql`PRAGMA foreign_keys = ON`;
    yield* sql`DELETE FROM orchestration_v2_projection_threads WHERE thread_id = 'idle'`;
    assert.isNull(yield* store.read(intent.threadId));
    assert.isFalse(yield* store.cleanupAllowed(intent));
    assert.lengthOf(yield* sql`SELECT * FROM jones_idle_worktree_admission`, 1);
  }).pipe(Effect.provide(memory)),
);

it.effect("rolls preparation state back with its owning transaction", () =>
  Effect.gen(function* () {
    const { sql, store } = yield* fixture;
    const result = yield* store
      .issueWorktree(intent)
      .pipe(Effect.andThen(Effect.fail("rollback")), sql.withTransaction, Effect.exit);
    assert.isTrue(Exit.isFailure(result));
    assert.equal((yield* store.read(intent.threadId))?.phase, "pending");
  }).pipe(Effect.provide(memory)),
);

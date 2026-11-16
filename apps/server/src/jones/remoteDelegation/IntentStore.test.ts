import { assert, it } from "@effect/vitest";
import { CommandId, NodeId, ProjectId, RunId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";
import * as Store from "./IntentStore.ts";
import { commandId, envelope, layer } from "./RemoteDelegation.testkit.ts";

it.effect("persists the exact intent before dispatch and recovers it through a new store", () =>
  Effect.gen(function* () {
    const createdAt = "2026-10-26T12:00:00.000Z";
    yield* TestClock.setTime(Date.parse(createdAt));
    const first = yield* Store.make;
    const request = { task: "Inspect fixture", target: { environmentId: "destination" } };
    const prepared = yield* first.prepare(commandId, request, envelope);
    const sql = yield* SqlClient.SqlClient;
    const [stored] = yield* sql<{ readonly created_at: string }>`
      SELECT created_at FROM jones_remote_delegation_intents WHERE command_id = ${commandId}
    `;
    assert.equal(stored?.created_at, createdAt);
    assert.equal(prepared.createdAt, stored?.created_at);
    yield* TestClock.adjust("1 hour");
    const restarted = yield* Store.make;
    const pending = yield* restarted.pending;
    assert.equal(pending.length, 1);
    assert.deepEqual(pending[0]?.envelope, envelope);
    assert.equal(pending[0]?.launchStarted, false);
    assert.equal(pending[0]?.taskId, null);
    assert.equal(pending[0]?.createdAt, createdAt);
    assert.equal((yield* restarted.get(commandId))?.createdAt, createdAt);
    assert.equal((yield* restarted.find(commandId, request))?.createdAt, createdAt);
    assert.equal((yield* restarted.prepare(commandId, request, envelope)).createdAt, createdAt);
    yield* restarted.reserved(commandId, NodeId.make("reserved:task"));
    assert.equal((yield* restarted.byTask(NodeId.make("reserved:task")))?.createdAt, createdAt);
    yield* restarted.beginLaunch(commandId);
    assert.equal((yield* first.get(commandId))?.launchStarted, true);
    assert.equal((yield* first.get(commandId))?.taskId, "reserved:task");
  }).pipe(Effect.provide(layer)),
);

it.effect("rejects same-key changed content while canonical object ordering replays", () =>
  Effect.gen(function* () {
    const store = yield* Store.make;
    yield* store.prepare(
      commandId,
      { task: "Inspect fixture", target: { environmentId: "destination" } },
      envelope,
    );
    const replay = yield* store.find(commandId, {
      target: { environmentId: "destination" },
      task: "Inspect fixture",
    });
    assert.deepEqual(replay?.envelope, envelope);
    const conflict = yield* store
      .find(commandId, { task: "Delete fixture", target: { environmentId: "destination" } })
      .pipe(Effect.result);
    assert.equal(conflict._tag, "Failure");
    if (conflict._tag === "Failure") assert.equal(conflict.failure.code, "invalid_request");
    assert.equal((yield* store.pending).length, 1);
  }).pipe(Effect.provide(layer)),
);

it.effect("cannot change the persisted namespace, link or payload after reservation", () =>
  Effect.gen(function* () {
    const store = yield* Store.make;
    yield* store.prepare(commandId, { task: "Inspect fixture" }, envelope);
    const sql = yield* SqlClient.SqlClient;
    const changed =
      yield* sql`UPDATE jones_remote_delegation_intents SET envelope_json = '{}' WHERE command_id = ${commandId}`.pipe(
        Effect.result,
      );
    assert.equal(changed._tag, "Failure");
    assert.deepEqual((yield* store.get(commandId))?.envelope, envelope);
  }).pipe(Effect.provide(layer)),
);

it.effect("preserves legacy supervision fingerprints and distinguishes explicit opt-ins", () =>
  Effect.gen(function* () {
    const store = yield* Store.make;
    const request = { task: "Inspect fixture", runtimeMode: "approval-required" };
    yield* store.prepare(commandId, request, envelope);
    assert.isDefined(yield* store.find(commandId, request));
    assert.isDefined(
      yield* store.find(commandId, {
        ...request,
        supervisionRequestedByUser: undefined,
      }),
    );
    for (const supervisionRequestedByUser of [false, true]) {
      const changed = yield* store
        .find(commandId, {
          ...request,
          supervisionRequestedByUser,
        })
        .pipe(Effect.result);
      assert.equal(changed._tag, "Failure");
      if (changed._tag === "Failure") {
        assert.equal(changed.failure.code, "invalid_request");
        assert.include(changed.failure.message, "already used with a different request");
      }
    }
    assert.equal((yield* store.pending).length, 1);
  }).pipe(Effect.provide(layer)),
);

it.effect("replays an explicitly supervised intent with the same flag", () =>
  Effect.gen(function* () {
    const store = yield* Store.make;
    const request = { task: "Inspect fixture", supervisionRequestedByUser: true };
    yield* store.prepare(commandId, request, envelope);
    assert.deepEqual((yield* store.find(commandId, request))?.envelope, envelope);
  }).pipe(Effect.provide(layer)),
);

const launched = {
  threadId: ThreadId.make("remote:child"),
  runId: RunId.make("remote:run"),
  status: "running",
  modelSelection: envelope.launch.modelSelection,
  projectId: ProjectId.make("project:destination"),
} as Store.LaunchResult;
const admission = { id: "held-1", bindingDigest: "a".repeat(64), status: "held" } as const;

const insertRaw = (
  id: string,
  status: "pending" | "recorded" | "stopped" | "denied",
  createdAt: string,
  envelopeJson = "{}",
) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO jones_remote_delegation_intents
      (command_id, request_json, envelope_json, status, created_at)
      VALUES (${id}, '{}', ${envelopeJson}, ${status}, ${createdAt})`;
  });
const remaining = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{ readonly command_id: string }>`
    SELECT command_id FROM jones_remote_delegation_intents ORDER BY command_id`;
  return rows.map((row) => row.command_id);
});

it.effect("round-trips the launch result and admission through schema-validated reads", () =>
  Effect.gen(function* () {
    const store = yield* Store.make;
    yield* store.prepare(commandId, { task: "Inspect fixture" }, envelope);
    yield* store.launched(commandId, launched);
    yield* store.admission(commandId, admission);
    const read = yield* store.get(commandId);
    assert.deepEqual(read?.launched, launched);
    assert.deepEqual(read?.admission, admission);
    assert.deepEqual((yield* store.pending)[0]?.launched, launched);
  }).pipe(Effect.provide(layer)),
);

it.effect("reports a stored row that does not match its schema as a storage failure", () =>
  Effect.gen(function* () {
    const store = yield* Store.make;
    const sql = yield* SqlClient.SqlClient;
    const request = { task: "Inspect fixture" };
    const taskId = NodeId.make("reserved:task");
    yield* store.prepare(commandId, request, envelope);
    yield* store.reserved(commandId, taskId);
    const failures = Effect.forEach(
      [store.get(commandId), store.find(commandId, request), store.byTask(taskId)],
      (read) =>
        read.pipe(
          Effect.flip,
          Effect.map((failure) => failure.code),
        ),
    );
    // Valid JSON that is not a launch result.
    yield* sql`UPDATE jones_remote_delegation_intents SET launched_json = '{"threadId":1}' WHERE command_id = ${commandId}`;
    assert.deepEqual(yield* failures, [
      "orchestration_error",
      "orchestration_error",
      "orchestration_error",
    ]);
    // Valid JSON that is not an admission.
    yield* sql`UPDATE jones_remote_delegation_intents SET launched_json = NULL, admission_json = '{"id":"","bindingDigest":"x","status":"nope"}' WHERE command_id = ${commandId}`;
    assert.deepEqual(yield* failures, [
      "orchestration_error",
      "orchestration_error",
      "orchestration_error",
    ]);
  }).pipe(Effect.provide(layer)),
);

it.effect("recovers the readable pending intents when one stored row cannot be read", () =>
  Effect.gen(function* () {
    const store = yield* Store.make;
    yield* insertRaw("remote:intent:corrupt", "pending", "2026-10-01T00:00:00.000Z");
    yield* store.prepare(commandId, { task: "Inspect fixture" }, envelope);
    const pending = yield* store.pending;
    assert.deepEqual(
      pending.map((intent) => intent.commandId),
      [commandId],
    );
    // The unreadable row is left in place for an operator, not deleted or closed.
    assert.deepEqual(yield* remaining, ["remote:intent:corrupt", commandId]);
    const failed = yield* store.get(CommandId.make("remote:intent:corrupt")).pipe(Effect.result);
    assert.equal(failed._tag, "Failure");
  }).pipe(Effect.provide(layer)),
);

it.effect("prunes only finished intents created before the cutoff", () =>
  Effect.gen(function* () {
    const store = yield* Store.make;
    const cutoff = "2026-10-11T12:00:00.000Z";
    const old = "2026-09-01T00:00:00.000Z";
    yield* insertRaw("old:pending", "pending", old);
    yield* insertRaw("old:recorded", "recorded", old);
    yield* insertRaw("old:stopped", "stopped", old);
    yield* insertRaw("old:denied", "denied", old);
    yield* insertRaw("edge:recorded", "recorded", cutoff);
    yield* insertRaw("new:recorded", "recorded", "2026-11-01T00:00:00.000Z");
    yield* insertRaw("new:pending", "pending", "2026-11-01T00:00:00.000Z");
    assert.equal(yield* store.pruneFinished(cutoff), 3);
    assert.deepEqual(yield* remaining, [
      "edge:recorded",
      "new:pending",
      "new:recorded",
      "old:pending",
    ]);
    assert.equal(yield* store.pruneFinished(cutoff), 0);
  }).pipe(Effect.provide(layer)),
);

it.effect("prunes a backlog larger than one batch without touching pending intents", () =>
  Effect.gen(function* () {
    const store = yield* Store.make;
    const old = "2026-01-01T00:00:00.000Z";
    yield* Effect.forEach(
      Array.from({ length: 450 }, (_, index) => index),
      (index) => insertRaw(`old:${String(index).padStart(3, "0")}`, "stopped", old),
    );
    yield* insertRaw("old:pending", "pending", old);
    assert.equal(yield* store.pruneFinished("2026-10-11T12:00:00.000Z"), 450);
    assert.deepEqual(yield* remaining, ["old:pending"]);
  }).pipe(Effect.provide(layer)),
);

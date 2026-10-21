import { assert, it } from "@effect/vitest";
import { NodeId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import * as Store from "./IntentStore.ts";
import { commandId, envelope, layer } from "./RemoteDelegation.testkit.ts";

it.effect("persists the exact intent before dispatch and recovers it through a new store", () =>
  Effect.gen(function* () {
    const first = yield* Store.make;
    yield* first.prepare(
      commandId,
      { task: "Inspect fixture", target: { environmentId: "destination" } },
      envelope,
    );
    const restarted = yield* Store.make;
    const pending = yield* restarted.pending;
    assert.equal(pending.length, 1);
    assert.deepEqual(pending[0]?.envelope, envelope);
    assert.equal(pending[0]?.launchStarted, false);
    assert.equal(pending[0]?.taskId, null);
    yield* restarted.reserved(commandId, NodeId.make("reserved:task"));
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

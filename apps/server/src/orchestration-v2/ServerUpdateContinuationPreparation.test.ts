import { assert, it } from "@effect/vitest";
import { RunId, ThreadId } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import { findPendingServerUpdateContinuation } from "./ServerUpdateContinuationPreparation.ts";

it.effect("finds only the pending prepared marker without decoding unrelated effects", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TABLE orchestration_v2_effect_outbox (
      effect_id TEXT PRIMARY KEY, command_id TEXT, thread_id TEXT,
      effect_type TEXT, payload_json TEXT, status TEXT
    )`;
    const thread = ThreadId.make("thread:preparation");
    const run = RunId.make("run:preparation");
    const prepared = JSON.stringify({ sourceRunId: run, preparedForRestart: true });
    for (const [id, command, type, payload, status] of [
      ["ordinary", "command:ordinary", "provider-runtime.continue", "invalid-json", "pending"],
      [
        "ended",
        "command:server-update-prepare:ended",
        "provider-runtime.continue",
        prepared,
        "succeeded",
      ],
      [
        "unprepared",
        "command:server-update-prepare:unprepared",
        "provider-runtime.continue",
        JSON.stringify({ sourceRunId: run }),
        "pending",
      ],
      [
        "other-run",
        "command:server-update-prepare:other",
        "provider-runtime.continue",
        JSON.stringify({ sourceRunId: "run:other", preparedForRestart: true }),
        "pending",
      ],
    ] as const) {
      yield* sql`INSERT INTO orchestration_v2_effect_outbox VALUES (${id}, ${command}, ${thread}, ${type}, ${payload}, ${status})`;
    }
    assert.deepEqual(yield* findPendingServerUpdateContinuation(thread, run), []);
    yield* sql`INSERT INTO orchestration_v2_effect_outbox VALUES ('prepared', 'command:server-update-prepare:exact', ${thread}, 'provider-runtime.continue', ${prepared}, 'pending')`;
    assert.deepEqual(yield* findPendingServerUpdateContinuation(thread, run), [
      { effect_id: "prepared" },
    ]);
    assert.deepEqual(
      yield* findPendingServerUpdateContinuation(ThreadId.make("thread:other"), run),
      [],
    );
    yield* sql`UPDATE orchestration_v2_effect_outbox SET payload_json = 'invalid-json' WHERE effect_id = 'prepared'`;
    assert.isTrue(
      (yield* Effect.exit(findPendingServerUpdateContinuation(thread, run)))._tag === "Failure",
    );
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

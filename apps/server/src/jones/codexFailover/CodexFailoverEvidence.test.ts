import { assert, it } from "@effect/vitest";
import {
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

import { migrationManifest, runMigrations } from "../../persistence/Migrations.ts";
import migration from "../persistence/Migrations/129_JonesCodexFailover.ts";
import * as Evidence from "./CodexFailoverEvidence.ts";

const input: Evidence.CodexFailoverEvidenceInput = {
  providerTurnId: ProviderTurnId.make("turn:evidence"),
  threadId: ThreadId.make("thread:evidence"),
  runId: RunId.make("run:evidence"),
  attemptId: RunAttemptId.make("attempt:evidence"),
  providerThreadId: ProviderThreadId.make("provider-thread:evidence"),
  providerInstanceId: ProviderInstanceId.make("codex-evidence"),
  occurredAt: "2026-10-26T20:00:00.000Z",
  status: "failed",
  evidenceKind: "provider_result",
  rawErrorInfo: "usageLimitExceeded",
  capacityExhausted: false,
};
const memory = NodeSqliteClient.layer({ filename: ":memory:" });
const upstream = runMigrations({ toMigrationInclusive: migrationManifest.at(-1)![0] });

const seed = Effect.fn("CodexFailoverEvidence.test.seed")(function* (
  binding: Evidence.CodexFailoverEvidenceInput = input,
  ordinal = 1,
) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO orchestration_v2_projection_runs
    (run_id, thread_id, ordinal, provider, provider_instance_id, provider_thread_id,
      status, requested_at, payload_json)
    VALUES (${binding.runId}, ${binding.threadId}, ${ordinal}, 'codex',
      ${binding.providerInstanceId}, ${binding.providerThreadId}, 'running', ${binding.occurredAt}, '{}')`;
  yield* sql`INSERT INTO orchestration_v2_projection_run_attempts
    (attempt_id, thread_id, run_id, attempt_ordinal, root_node_id, provider,
      provider_instance_id, provider_thread_id, status, payload_json)
    VALUES (${binding.attemptId}, ${binding.threadId}, ${binding.runId}, 1, 'node:evidence',
      'codex', ${binding.providerInstanceId}, ${binding.providerThreadId}, 'running', '{}')`;
});

const setup = Effect.gen(function* () {
  yield* upstream;
  yield* migration;
  yield* seed();
  return yield* Evidence.make;
});
const rows = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql`SELECT * FROM jones_codex_failover_evidence ORDER BY provider_turn_id`;
});

it.effect("records eligible native usage and HTTP 503 before a provider-turn row exists", () =>
  Effect.gen(function* () {
    const recorder = yield* setup;
    yield* recorder.record(input);
    yield* recorder.record({
      ...input,
      providerTurnId: ProviderTurnId.make("turn:unavailable"),
      rawErrorInfo: { responseStreamDisconnected: { httpStatusCode: 503 } },
    });
    assert.deepEqual(yield* rows, [
      {
        provider_turn_id: input.providerTurnId,
        thread_id: input.threadId,
        run_id: input.runId,
        attempt_id: input.attemptId,
        provider_thread_id: input.providerThreadId,
        provider_instance_id: input.providerInstanceId,
        disposition: "usage_limit",
        occurred_at: input.occurredAt,
      },
      {
        provider_turn_id: "turn:unavailable",
        thread_id: input.threadId,
        run_id: input.runId,
        attempt_id: input.attemptId,
        provider_thread_id: input.providerThreadId,
        provider_instance_id: input.providerInstanceId,
        disposition: "unavailable",
        occurred_at: input.occurredAt,
      },
    ]);
    const sql = yield* SqlClient.SqlClient;
    assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_projection_provider_turns`, []);
    assert.deepEqual(yield* sql`PRAGMA foreign_key_list(jones_codex_failover_evidence)`, []);
  }).pipe(Effect.provide(memory)),
);

it.effect(
  "capacity exhaustion supplies unavailable only when native classification is absent",
  () =>
    Effect.gen(function* () {
      const recorder = yield* setup;
      yield* recorder.record({ ...input, rawErrorInfo: null, capacityExhausted: true });
      yield* recorder.record({
        ...input,
        providerTurnId: ProviderTurnId.make("turn:usage"),
        capacityExhausted: true,
      });
      assert.deepEqual(
        (yield* rows).map((row) => row.disposition),
        ["unavailable", "usage_limit"],
      );
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "ignores local failures, nonfailed terminals, authentication and ambiguous failures",
  () =>
    Effect.gen(function* () {
      const recorder = yield* setup;
      const negatives: ReadonlyArray<Partial<Evidence.CodexFailoverEvidenceInput>> = [
        { evidenceKind: "local_failure", capacityExhausted: true },
        { status: "completed", capacityExhausted: true },
        { status: "interrupted", capacityExhausted: true },
        { status: "cancelled" },
        { status: "inProgress" },
        { rawErrorInfo: "unauthorized" },
        { rawErrorInfo: "badRequest" },
        { rawErrorInfo: "other" },
        { rawErrorInfo: null },
        { rawErrorInfo: undefined },
        { rawErrorInfo: { responseTooManyFailedAttempts: {} } },
        ...[400, 401, 403, 404].map((httpStatusCode) => ({
          rawErrorInfo: { httpConnectionFailed: { httpStatusCode } },
        })),
      ];
      for (const negative of negatives) yield* recorder.record({ ...input, ...negative });
      assert.deepEqual(yield* rows, []);
    }).pipe(Effect.provide(memory)),
);

it.effect("preserves the first logical-turn binding, disposition and timestamp on duplicates", () =>
  Effect.gen(function* () {
    const recorder = yield* setup;
    yield* recorder.record(input);
    const first = yield* rows;
    yield* recorder.record({ ...input, rawErrorInfo: "serverOverloaded" });
    const other = {
      ...input,
      threadId: ThreadId.make("thread:other"),
      runId: RunId.make("run:other"),
      attemptId: RunAttemptId.make("attempt:other"),
      providerThreadId: ProviderThreadId.make("provider-thread:other"),
      providerInstanceId: ProviderInstanceId.make("codex-other"),
      occurredAt: "2026-10-26T20:01:00.000Z",
      rawErrorInfo: "serverOverloaded" as const,
    };
    yield* seed(other);
    yield* recorder.record(other);
    assert.deepEqual(yield* rows, first);
  }).pipe(Effect.provide(memory)),
);

it.effect("requires matching upstream run and attempt bindings", () =>
  Effect.gen(function* () {
    const recorder = yield* setup;
    const sql = yield* SqlClient.SqlClient;
    for (const mismatch of [
      { runId: RunId.make("run:missing") },
      { attemptId: RunAttemptId.make("attempt:missing") },
      { threadId: ThreadId.make("thread:wrong") },
      { providerThreadId: ProviderThreadId.make("provider-thread:wrong") },
      { providerInstanceId: ProviderInstanceId.make("codex-wrong") },
    ])
      yield* recorder.record({ ...input, ...mismatch });
    // A valid run must not hide a mismatched attempt (and vice versa).
    yield* sql`UPDATE orchestration_v2_projection_run_attempts SET thread_id = 'thread:wrong'
      WHERE attempt_id = ${input.attemptId}`;
    yield* recorder.record(input);
    yield* sql`UPDATE orchestration_v2_projection_run_attempts SET thread_id = ${input.threadId},
      run_id = 'run:wrong' WHERE attempt_id = ${input.attemptId}`;
    yield* recorder.record(input);
    yield* sql`UPDATE orchestration_v2_projection_run_attempts SET run_id = ${input.runId},
      provider_thread_id = 'provider-thread:wrong' WHERE attempt_id = ${input.attemptId}`;
    yield* recorder.record(input);
    yield* sql`UPDATE orchestration_v2_projection_run_attempts SET provider_thread_id = ${input.providerThreadId},
      provider_instance_id = 'codex-wrong' WHERE attempt_id = ${input.attemptId}`;
    yield* recorder.record(input);
    assert.deepEqual(yield* rows, []);
  }).pipe(Effect.provide(memory)),
);

it.effect("absent Jones table is harmless and is observed when created later", () =>
  Effect.gen(function* () {
    yield* upstream;
    yield* seed();
    const recorder = yield* Evidence.make;
    assert.isUndefined(yield* recorder.record(input));
    const sql = yield* SqlClient.SqlClient;
    assert.deepEqual(
      yield* sql`SELECT name FROM sqlite_master
      WHERE name = 'jones_codex_failover_evidence'`,
      [],
    );
    yield* migration;
    yield* recorder.record(input);
    assert.lengthOf(yield* rows, 1);
  }).pipe(Effect.provide(memory)),
);

it.effect("write failure rolls back, never escapes, and permits a later successful record", () =>
  Effect.gen(function* () {
    const recorder = yield* setup;
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TRIGGER jones_evidence_test_failure AFTER INSERT ON jones_codex_failover_evidence
      BEGIN SELECT RAISE(ABORT, 'synthetic failure'); END`;
    assert.isUndefined(yield* recorder.record(input));
    assert.deepEqual(yield* rows, []);
    yield* sql`DROP TRIGGER jones_evidence_test_failure`;
    yield* recorder.record(input);
    assert.lengthOf(yield* rows, 1);
  }).pipe(Effect.provide(memory)),
);

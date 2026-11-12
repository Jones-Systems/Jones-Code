import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/sql/SqlClient";
import migration from "./129_JonesCodexFailover.ts";

const memory = NodeSqliteClient.layer({ filename: ":memory:" });
const tables = [
  "jones_codex_failover_evidence",
  "jones_codex_failover_decisions",
  "jones_codex_failover_steps",
  "jones_codex_failover_queued_runs",
] as const;

it.effect(
  "production migration creates only four sparse Jones tables with immutable evidence columns and no foreign keys",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* migration;
      assert.deepEqual(
        (yield* sql<{ name: string }>`SELECT name FROM sqlite_master
      WHERE type = 'table' ORDER BY name`).map((row) => row.name),
        [...tables].sort(),
      );
      for (const table of tables) {
        assert.deepEqual(yield* sql.unsafe(`PRAGMA foreign_key_list(${table})`), []);
      }
      assert.deepEqual(
        (yield* sql<{ name: string }>`PRAGMA table_info(jones_codex_failover_evidence)`).map(
          (row) => row.name,
        ),
        [
          "provider_turn_id",
          "thread_id",
          "run_id",
          "attempt_id",
          "provider_thread_id",
          "provider_instance_id",
          "disposition",
          "occurred_at",
        ],
      );
      const indexes = yield* sql<{ name: string }>`SELECT name FROM sqlite_master
      WHERE type = 'index' AND name NOT LIKE 'sqlite_%' ORDER BY name`;
      assert.deepEqual(
        indexes.map((row) => row.name),
        [
          "jones_codex_failover_decisions_deferred_idx",
          "jones_codex_failover_evidence_occurred_idx",
          "jones_codex_failover_evidence_thread_idx",
          "jones_codex_failover_queued_runs_thread_idx",
          "jones_codex_failover_steps_chain_idx",
          "jones_codex_failover_steps_open_idx",
          "jones_codex_failover_steps_reset_idx",
        ],
      );
    }).pipe(Effect.provide(memory)),
);

it.effect(
  "production constraints reject inconsistent decisions, invalid dispositions/JSON/statuses and duplicate continuation identities",
  () =>
    Effect.gen(function* () {
      yield* migration;
      const sql = yield* SqlClient.SqlClient;
      yield* sql`INSERT INTO jones_codex_failover_evidence VALUES
      ('turn', 'thread', 'run', 'attempt', 'provider-thread', 'codex:a', 'usage_limit', '2026-10-26T20:00:00.000Z')`;
      yield* sql`INSERT INTO jones_codex_failover_decisions VALUES ('turn', 'thread', 'deferred', NULL, 'now')`;
      yield* sql`INSERT INTO jones_codex_failover_steps VALUES
      ('switch', 'thread', 'run', 'run', 'turn', 'codex:a', 'usage_limit', NULL, 'codex:b',
       '{}', '{}', 'continue', 'message', 'switched', 'now', 'now')`;
      yield* sql`INSERT INTO jones_codex_failover_queued_runs VALUES ('queued', 'thread', 'now', NULL, NULL, 'now')`;
      for (const statement of [
        "UPDATE jones_codex_failover_evidence SET disposition = 'other'",
        "UPDATE jones_codex_failover_evidence SET run_id = NULL",
        "UPDATE jones_codex_failover_decisions SET state = 'recorded'",
        "UPDATE jones_codex_failover_decisions SET state = 'decided'",
        "UPDATE jones_codex_failover_decisions SET decision = 'switched'",
        "UPDATE jones_codex_failover_decisions SET state = 'decided', decision = ''",
        "UPDATE jones_codex_failover_steps SET status = 'unknown'",
        "UPDATE jones_codex_failover_steps SET source_failure = 'unknown'",
        "UPDATE jones_codex_failover_steps SET saved_selection_json = '{'",
        "UPDATE jones_codex_failover_steps SET continuation_selection_json = '{'",
        "UPDATE jones_codex_failover_queued_runs SET override_selection_json = '{}'",
        "UPDATE jones_codex_failover_queued_runs SET override_switch_command_id = 'switch'",
        "UPDATE jones_codex_failover_queued_runs SET override_selection_json = '{', override_switch_command_id = 'switch'",
        "INSERT INTO jones_codex_failover_evidence SELECT * FROM jones_codex_failover_evidence",
        "INSERT INTO jones_codex_failover_decisions SELECT * FROM jones_codex_failover_decisions",
        "INSERT INTO jones_codex_failover_queued_runs SELECT * FROM jones_codex_failover_queued_runs",
      ])
        assert.isTrue(Exit.isFailure(yield* Effect.exit(sql.unsafe(statement))), statement);
      for (const [origin, command, message] of [
        ["run", "other-command", "other-message"],
        ["other-run", "continue", "other-message"],
        ["other-run", "other-command", "message"],
      ]) {
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(sql`INSERT INTO jones_codex_failover_steps
        SELECT 'other-switch', thread_id, chain_root_run_id, ${origin}, origin_provider_turn_id,
        source_instance_id, source_failure, source_reset_at, target_instance_id, saved_selection_json,
        continuation_selection_json, ${command}, ${message}, status, created_at, updated_at
        FROM jones_codex_failover_steps WHERE switch_command_id = 'switch'`),
          ),
        );
      }
      yield* sql`UPDATE jones_codex_failover_decisions SET state = 'decided', decision = 'switched'`;
      yield* sql`UPDATE jones_codex_failover_queued_runs SET override_selection_json = '{}', override_switch_command_id = 'switch'`;
      assert.lengthOf(yield* sql`SELECT * FROM jones_codex_failover_evidence`, 1);
      assert.lengthOf(yield* sql`SELECT * FROM jones_codex_failover_steps`, 1);
    }).pipe(Effect.provide(memory)),
);

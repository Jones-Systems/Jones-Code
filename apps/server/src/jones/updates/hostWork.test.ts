import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import * as Sqlite from "../../persistence/Sqlite.ts";
import { readHostWork } from "./hostWork.ts";

const at = "2026-10-20T00:00:00.000Z";

it.layer(Sqlite.layerMemory)("host work snapshot", (it) => {
  it.effect("counts only work a restart would cut and classifies pending requests", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      expect(yield* readHostWork).toEqual({
        activeRuns: 0,
        unheldQueuedRuns: 0,
        processEffects: 0,
        backgroundTasks: 0,
        pendingRequests: [],
      });
      const run = (id: string, thread: string, ordinal: number, status: string, payload = {}) =>
        sql`
          INSERT INTO orchestration_v2_projection_runs (
            run_id, thread_id, ordinal, provider, provider_thread_id, status, requested_at,
            completed_at, payload_json
          ) VALUES (
            ${id}, ${thread}, ${ordinal}, 'codex', NULL, ${status}, ${at}, NULL,
            ${JSON.stringify(payload)}
          )
        `;
      yield* run("run:running", "thread:a", 1, "running");
      yield* run("run:done", "thread:a", 2, "completed");
      yield* run("run:queued", "thread:a", 3, "queued");
      yield* run("run:held", "thread:a", 4, "queued", { queueHeld: true });
      // Waiting on a pending request: the request decides, not the run.
      yield* run("run:asking", "thread:b", 1, "waiting");
      // Waiting with nothing pending: counted as running work.
      yield* run("run:stuck", "thread:c", 1, "waiting");
      const request = (id: string, thread: string, kind: string, status: string, type?: string) =>
        sql`
          INSERT INTO orchestration_v2_projection_runtime_requests (
            runtime_request_id, thread_id, node_id, provider_turn_id, kind, status, created_at,
            resolved_at, payload_json
          ) VALUES (
            ${id}, ${thread}, 'node', NULL, ${kind}, ${status}, ${at}, NULL,
            ${JSON.stringify(type === undefined ? {} : { responseCapability: { type } })}
          )
        `;
      yield* request("request:question", "thread:b", "user_input", "pending", "message");
      yield* request(
        "request:approval",
        "thread:b",
        "command_execution_approval",
        "pending",
        "live",
      );
      yield* request("request:old", "thread:c", "user_input", "resolved", "live");
      yield* request("request:unknown", "thread:d", "file_read_approval", "pending");
      const providerThread = (id: string, status: string, tasks: number) =>
        sql`
          INSERT INTO orchestration_v2_projection_provider_threads (
            provider_thread_id, thread_id, owner_node_id, provider, provider_session_id, status,
            first_run_ordinal, last_run_ordinal, updated_at, payload_json
          ) VALUES (
            ${id}, 'thread:a', NULL, 'codex', NULL, ${status}, NULL, NULL, ${at},
            ${JSON.stringify({ pendingBackgroundTasks: Array.from({ length: tasks }, (_, i) => ({ id: i })) })}
          )
        `;
      yield* providerThread("provider:live", "idle", 2);
      yield* providerThread("provider:closed", "closed", 1);
      yield* providerThread("provider:quiet", "active", 0);
      const effect = (id: string, type: string, status: string) =>
        sql`
          INSERT INTO orchestration_v2_effect_outbox (
            effect_id, command_id, thread_id, effect_type, payload_json, status, attempt_count,
            available_at, created_at, updated_at, completed_at
          ) VALUES (
            ${id}, ${`command:${id}`}, 'thread:a', ${type}, '{}', ${status}, 0, ${at}, ${at},
            ${at}, NULL
          )
        `;
      yield* effect("effect:turn", "provider-turn.start", "pending");
      yield* effect("effect:respond", "runtime-request.respond", "running");
      yield* effect("effect:done", "provider-turn.start", "succeeded");
      yield* effect("effect:title", "thread-title.generate", "pending");

      const work = yield* readHostWork;
      expect(work).toMatchObject({
        activeRuns: 2,
        unheldQueuedRuns: 1,
        processEffects: 2,
        backgroundTasks: 1,
      });
      expect(work.pendingRequests.toSorted((a, b) => a.kind.localeCompare(b.kind))).toEqual([
        { kind: "command_execution_approval", capability: "live" },
        { kind: "file_read_approval", capability: null },
        { kind: "user_input", capability: "message" },
      ]);
    }),
  );
});

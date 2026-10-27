import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import { PROCESS_BOUND_EFFECT_TYPES } from "../../orchestration-v2/EffectOutbox.ts";
import type { HostWorkSnapshot } from "./followerPolicy.ts";

/**
 * Reads the work a restart would cut, read-only, from the projections and the effect outbox.
 * It runs only when an automatic activation is otherwise due, so its status scans are bounded to
 * one per follower pass. A waiting run counts only when no pending request explains its wait; a
 * pending request is classified by its response capability instead.
 */
export const readHostWork = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const [counts] = yield* sql<{
    readonly active_runs: number;
    readonly unheld_queued_runs: number;
    readonly process_effects: number;
    readonly background_tasks: number;
  }>`
    SELECT
      (SELECT COUNT(*) FROM orchestration_v2_projection_runs AS run
        WHERE run.status IN ('preparing', 'starting', 'running')
          OR (run.status = 'waiting' AND NOT EXISTS (
            SELECT 1 FROM orchestration_v2_projection_runtime_requests AS request
            WHERE request.thread_id = run.thread_id AND request.status = 'pending'))
      ) AS active_runs,
      (SELECT COUNT(*) FROM orchestration_v2_projection_runs
        WHERE status = 'queued'
          AND COALESCE(json_extract(payload_json, '$.queueHeld'), 0) = 0
      ) AS unheld_queued_runs,
      (SELECT COUNT(*) FROM orchestration_v2_effect_outbox
        WHERE status IN ('pending', 'running')
          AND effect_type IN ${sql.in(PROCESS_BOUND_EFFECT_TYPES)}
      ) AS process_effects,
      (SELECT COUNT(*) FROM orchestration_v2_projection_provider_threads
        WHERE status IN ('idle', 'active')
          AND COALESCE(json_array_length(payload_json, '$.pendingBackgroundTasks'), 0) > 0
      ) AS background_tasks
  `;
  const requests = yield* sql<{ readonly kind: string; readonly capability: string | null }>`
    SELECT kind, json_extract(payload_json, '$.responseCapability.type') AS capability
    FROM orchestration_v2_projection_runtime_requests
    WHERE status = 'pending'
  `;
  return {
    activeRuns: counts?.active_runs ?? 0,
    unheldQueuedRuns: counts?.unheld_queued_runs ?? 0,
    processEffects: counts?.process_effects ?? 0,
    backgroundTasks: counts?.background_tasks ?? 0,
    pendingRequests: requests.map((request) => ({
      kind: request.kind,
      capability: typeof request.capability === "string" ? request.capability : null,
    })),
  } satisfies HostWorkSnapshot;
});

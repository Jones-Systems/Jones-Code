import type { RunId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

// Preserve the live query's JSON filtering and failure behavior. Decoding every
// effect for the thread would make unrelated malformed payloads fail preparation.
export const findPendingServerUpdateContinuation = (threadId: ThreadId, runId: RunId) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    return yield* sql<{ effect_id: string }>`SELECT effect_id FROM orchestration_v2_effect_outbox
        WHERE thread_id = ${threadId} AND status = 'pending'
          AND command_id GLOB 'command:server-update-prepare:*'
          AND effect_type = 'provider-runtime.continue'
          AND json_extract(payload_json, '$.sourceRunId') = ${runId}
          AND json_extract(payload_json, '$.preparedForRestart') = 1 LIMIT 1`;
  });

import type { ThreadId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import * as EventSink from "./EventSink.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ServerSettings from "../serverSettings.ts";
import { randomUuidV4 } from "./RandomUuid.ts";
import { findPendingServerUpdateContinuation } from "./ServerUpdateContinuationPreparation.ts";
import {
  serverUpdateContinuationEffect,
  serverUpdateContinuationRun,
} from "./ServerUpdateContinuationPolicy.ts";

const mark = (optedInOnly: boolean) =>
  Effect.gen(function* () {
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const sink = yield* EventSink.EventSinkV2;
    const sql = yield* SqlClient.SqlClient;
    const settings = yield* ServerSettings.ServerSettingsService;
    const preferences = optedInOnly ? yield* settings.getSettings : null;
    // A passive outbox record survives process loss without issuing a live prompt.
    // The transaction also rolls back every new marker if preparation fails.
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        const marked: ThreadId[] = [];
        for (const threadId of yield* projections.getRecoveryThreadIds("runtime")) {
          const projection = yield* projections.getRuntimeRecoveryProjection(threadId);
          const run = serverUpdateContinuationRun(projection, preferences);
          if (run === undefined) continue;
          const existing = yield* findPendingServerUpdateContinuation(threadId, run.id);
          if (existing.length > 0) {
            if (!optedInOnly) {
              yield* sql`UPDATE orchestration_v2_effect_outbox
                SET payload_json = json_set(payload_json, '$.continueWithoutPreference', json('true'))
                WHERE effect_id = ${existing[0]!.effect_id} AND status = 'pending'`;
            }
            marked.push(threadId);
            continue;
          }
          const preparationId = yield* randomUuidV4;
          const effect = serverUpdateContinuationEffect({
            preparationId,
            threadId,
            sourceRunId: run.id,
            optedInOnly,
          });
          yield* sink.writeWithEffects({
            commandId: effect.commandId,
            events: [],
            effects: [effect],
          });
          marked.push(threadId);
        }
        return marked;
      }),
    );
  });

export const markRunningProviderSessionsForContinuation = mark(false);
export const markOptedInProviderSessionsForContinuation = mark(true);

export const clearProviderSessionContinuationMarkers = (threadIds: ReadonlyArray<ThreadId>) =>
  Effect.gen(function* () {
    if (threadIds.length === 0) return;
    const sql = yield* SqlClient.SqlClient;
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    const now = DateTime.formatIso(yield* DateTime.now);
    const cancelled = yield* sql<{ effect_id: string }>`UPDATE orchestration_v2_effect_outbox
    SET status = 'cancelled', updated_at = ${now}, completed_at = ${now},
      last_error = 'Server update preparation was cancelled.'
    WHERE thread_id IN ${sql.in(threadIds)} AND status = 'pending'
      AND command_id GLOB 'command:server-update-prepare:*'
      AND effect_type = 'provider-runtime.continue'
      AND json_extract(payload_json, '$.preparedForRestart') = 1
    RETURNING effect_id`;
    yield* outbox.signalCancellations(cancelled.map((row) => row.effect_id));
  });

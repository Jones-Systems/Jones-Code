import { CommandId, type ThreadId } from "@t3tools/contracts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as EventSink from "./EventSink.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ServerSettings from "../serverSettings.ts";
import { randomUuidV4 } from "./RandomUuid.ts";
import { restartContinuationRun } from "./RestartContinuation.ts";

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
          if (
            preferences !== null &&
            !resolveProjectSettings(preferences, projection.thread.projectId).settings
              .continueThreadsAfterServerUpdate
          )
            continue;
          const run = restartContinuationRun(projection);
          if (run === undefined || run.status !== "running") continue;
          const existing = yield* sql<{
            effect_id: string;
          }>`SELECT effect_id FROM orchestration_v2_effect_outbox
        WHERE thread_id = ${threadId} AND status = 'pending'
          AND command_id GLOB 'command:server-update-prepare:*'
          AND effect_type = 'provider-runtime.continue'
          AND json_extract(payload_json, '$.sourceRunId') = ${run.id}
          AND json_extract(payload_json, '$.preparedForRestart') = 1 LIMIT 1`;
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
          const commandId = CommandId.make(`command:server-update-prepare:${preparationId}`);
          yield* sink.writeWithEffects({
            commandId,
            events: [],
            effects: [
              {
                id: `effect:server-update-continuation:${preparationId}`,
                commandId,
                threadId,
                request: {
                  type: "provider-runtime.continue",
                  sourceRunId: run.id,
                  preparedForRestart: true,
                  continueWithoutPreference: !optedInOnly,
                },
              },
            ],
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

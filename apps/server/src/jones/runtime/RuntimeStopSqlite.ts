import {
  CommandId,
  ThreadId,
  RunId,
  AuthSessionId,
  ServerAuthSessionMethod,
  CurrentRuntimeStopTarget,
  StopCurrentThreadRuntimeInput,
  ProviderRuntimeBinding,
  ProviderRuntimeEvidenceCapture,
  type OrchestrationV2ProviderThread,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/sql/SqlClient";
import { nativeCreationCanonicalJson } from "../nativeCreation/NativeCreationPreparation.ts";
import {
  makeJonesTablePresence,
  type JonesTablePresence,
} from "../persistence/JonesTablePresence.ts";
export class RuntimeStopError extends Schema.TaggedError<RuntimeStopError>()("RuntimeStopError", {
  reason: Schema.String,
  cause: Schema.optional(Schema.Defect()),
}) {}
export const RuntimeStopIdentity = Schema.Struct({
  request: StopCurrentThreadRuntimeInput,
  actor: Schema.Struct({
    sessionId: AuthSessionId,
    subject: Schema.String,
    method: ServerAuthSessionMethod,
  }),
  actorDigest: Schema.NonEmptyString,
  affectedRunIds: Schema.Array(RunId),
});
export type RuntimeStopIdentity = typeof RuntimeStopIdentity.Type;
export interface RuntimeStopCommitContext {
  readonly identity: RuntimeStopIdentity;
  readonly revalidate: Effect.Effect<void, RuntimeStopError>;
}
// These methods use EventSink's existing transaction and command receipt owner.
export const makeRuntimeStopMethods = (
  sql: SqlClient.SqlClient,
  presence: JonesTablePresence = makeJonesTablePresence(sql),
) => {
  const available = presence("jones_runtime_stop_intents");
  const read = Effect.fnUntraced(function* (commandId: CommandId) {
    if (!(yield* available)) return null;
    const rows = yield* sql<{
      identity_json: string;
    }>`SELECT identity_json FROM jones_runtime_stop_intents WHERE command_id=${commandId}`;
    if (rows.length === 0) return null;
    if (rows.length !== 1)
      return yield* new RuntimeStopError({ reason: "ambiguous_stop_identity" });
    return yield* Schema.decodeEffect(Schema.fromJsonString(RuntimeStopIdentity))(
      rows[0]!.identity_json,
    ).pipe(Effect.mapError(() => new RuntimeStopError({ reason: "invalid_stop_identity" })));
  });
  const assertIdentity = Effect.fnUntraced(function* (
    commandId: CommandId,
    context?: RuntimeStopCommitContext,
  ) {
    const prior = yield* read(commandId);
    if (
      prior !== null &&
      (!context ||
        nativeCreationCanonicalJson(prior) !== nativeCreationCanonicalJson(context.identity))
    )
      return yield* new RuntimeStopError({ reason: "stop_command_identity_conflict" });
    if (context && !(yield* available))
      return yield* new RuntimeStopError({ reason: "runtime_stop_storage_unavailable" });
    if (context && context.identity.request.commandId !== commandId)
      return yield* new RuntimeStopError({ reason: "stop_command_identity_conflict" });
  });
  const record = Effect.fnUntraced(function* (context: RuntimeStopCommitContext) {
    const { identity } = context;
    yield* assertIdentity(identity.request.commandId, context);
    yield* context.revalidate;
    if ((yield* read(identity.request.commandId)) !== null) return;
    const liveRuns = yield* sql<{
      run_id: string;
    }>`SELECT run_id FROM orchestration_v2_projection_runs WHERE thread_id=${identity.request.threadId} AND json_extract(payload_json,'$.status') IN ('queued','starting') ORDER BY run_id`;
    if (
      nativeCreationCanonicalJson(liveRuns.map((row) => row.run_id)) !==
      nativeCreationCanonicalJson([...identity.affectedRunIds].sort())
    )
      return yield* new RuntimeStopError({ reason: "runtime_stop_queued_basis_changed" });
    yield* sql`INSERT INTO jones_runtime_stop_intents(command_id,thread_id,identity_json) VALUES (${identity.request.commandId},${identity.request.threadId},${nativeCreationCanonicalJson(identity)})`;
    for (const runId of identity.affectedRunIds)
      yield* sql`INSERT INTO jones_runtime_stop_fences(command_id,thread_id,run_id,provider_thread_id,runtime_generation) VALUES (${identity.request.commandId},${identity.request.threadId},${runId},${identity.request.target.binding.providerThreadId},${identity.request.target.binding.runtimeGeneration})`;
  });
  const assertStartAllowed = Effect.fnUntraced(function* (input: {
    readonly threadId: ThreadId;
    readonly runId: RunId;
    readonly providerThreadId?: string;
    readonly runtimeGeneration?: string;
  }) {
    if (!(yield* available)) return;
    const rows =
      yield* sql`SELECT command_id FROM jones_runtime_stop_fences WHERE thread_id=${input.threadId} AND run_id=${input.runId} LIMIT 1`;
    if (rows.length !== 0)
      return yield* new RuntimeStopError({ reason: "queued_start_crosses_captured_runtime_stop" });
    if (input.providerThreadId !== undefined) {
      const lineage =
        yield* sql`SELECT command_id FROM jones_runtime_stop_intents WHERE thread_id=${input.threadId} AND json_extract(identity_json,'$.request.target.binding.providerThreadId')=${input.providerThreadId} AND (${input.runtimeGeneration ?? null} IS NULL OR json_extract(identity_json,'$.request.target.binding.runtimeGeneration')=${input.runtimeGeneration ?? null}) LIMIT 1`;
      if (lineage.length !== 0)
        return yield* new RuntimeStopError({
          reason: "queued_start_crosses_captured_runtime_lineage",
        });
    }
  });
  const readState = Effect.fnUntraced(function* (commandId: CommandId) {
    const identity = yield* read(commandId);
    if (identity === null) return null;
    const rows = yield* sql<{
      phase: string;
      result: string;
    }>`SELECT phase,result FROM jones_runtime_stop_observations WHERE command_id=${commandId}`;
    return {
      identity,
      status: rows.some((row) => row.phase === "completed" && row.result === "stopped")
        ? ("stopped" as const)
        : rows.length > 0
          ? ("unknown" as const)
          : ("accepted" as const),
    };
  });
  const start = Effect.fnUntraced(function* (
    commandId: CommandId,
    target: typeof CurrentRuntimeStopTarget.Type,
    revalidate: Effect.Effect<void, RuntimeStopError>,
  ) {
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        const state = yield* readState(commandId);
        if (
          state === null ||
          nativeCreationCanonicalJson(state.identity.request.target) !==
            nativeCreationCanonicalJson(target)
        )
          return yield* new RuntimeStopError({ reason: "stop_target_identity_conflict" });
        if (state.status !== "accepted") return false;
        yield* revalidate;
        const rows =
          yield* sql`INSERT OR IGNORE INTO jones_runtime_stop_observations(command_id,phase,result) VALUES (${commandId},'started','unknown') RETURNING command_id`;
        return rows.length === 1;
      }),
    );
  });
  const complete = Effect.fnUntraced(function* (
    commandId: CommandId,
    result: "unknown" | "stopped",
  ) {
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        const started =
          yield* sql`SELECT command_id FROM jones_runtime_stop_observations WHERE command_id=${commandId} AND phase='started'`;
        if (started.length !== 1)
          return yield* new RuntimeStopError({ reason: "stop_completion_without_start" });
        const existing = yield* sql<{
          result: string;
        }>`SELECT result FROM jones_runtime_stop_observations WHERE command_id=${commandId} AND phase='completed'`;
        if (existing.length) {
          if (existing[0]!.result !== result)
            return yield* new RuntimeStopError({ reason: "stop_completion_conflict" });
          return;
        }
        yield* sql`INSERT INTO jones_runtime_stop_observations(command_id,phase,result) VALUES (${commandId},'completed',${result})`;
      }),
    );
  });
  const hasStoppedRuntime = Effect.fnUntraced(
    function* (binding: ProviderRuntimeBinding) {
      if (!(yield* available)) return false;
      const rows = yield* sql`
      SELECT intent.command_id
      FROM jones_runtime_stop_intents AS intent
      JOIN jones_runtime_stop_observations AS observation ON observation.command_id=intent.command_id
      WHERE observation.phase='completed' AND observation.result='stopped'
        AND intent.thread_id=${binding.threadId}
        AND json_extract(intent.identity_json,'$.request.target.binding.threadId')=${binding.threadId}
        AND json_extract(intent.identity_json,'$.request.target.binding.providerThreadId')=${binding.providerThreadId}
        AND json_extract(intent.identity_json,'$.request.target.binding.providerSessionId')=${binding.providerSessionId}
        AND json_extract(intent.identity_json,'$.request.target.binding.providerInstanceId')=${binding.providerInstanceId}
        AND json_extract(intent.identity_json,'$.request.target.binding.driver')=${binding.driver}
        AND json_extract(intent.identity_json,'$.request.target.binding.nativeThreadId')=${binding.nativeThreadId}
        AND json_extract(intent.identity_json,'$.request.target.binding.runtimeGeneration')=${binding.runtimeGeneration}
      LIMIT 1
      `;
      return rows.length === 1;
    },
    Effect.mapError(
      (cause) => new RuntimeStopError({ reason: "runtime_stop_readback_unavailable", cause }),
    ),
  );
  const readStoppedRuntime = Effect.fnUntraced(
    function* (provider: OrchestrationV2ProviderThread) {
      if (provider.runtimeIdentity?.runtimeGeneration !== undefined) return null;
      const identity = yield* Schema.decodeUnknownEffect(ProviderRuntimeEvidenceCapture)({
        threadId: provider.appThreadId,
        providerThreadId: provider.id,
        providerSessionId: provider.providerSessionId,
        providerInstanceId: provider.providerInstanceId,
        driver: provider.driver,
        nativeThreadId: provider.nativeThreadRef?.nativeId,
      }).pipe(
        Effect.mapError(
          (cause) => new RuntimeStopError({ reason: "runtime_stop_readback_identity_incomplete", cause }),
        ),
      );
      if (provider.nativeThreadRef?.driver !== identity.driver)
        return yield* new RuntimeStopError({ reason: "runtime_stop_readback_native_driver_conflict" });
      if (!(yield* available)) return null;
      // Normalize all binding fields before DISTINCT: separate commands for one binding are one candidate.
      const rows = yield* sql<{ binding_json: string }>`
        SELECT DISTINCT json_object(
          'threadId',json_extract(intent.identity_json,'$.request.target.binding.threadId'),
          'providerThreadId',json_extract(intent.identity_json,'$.request.target.binding.providerThreadId'),
          'providerSessionId',json_extract(intent.identity_json,'$.request.target.binding.providerSessionId'),
          'providerInstanceId',json_extract(intent.identity_json,'$.request.target.binding.providerInstanceId'),
          'driver',json_extract(intent.identity_json,'$.request.target.binding.driver'),
          'nativeThreadId',json_extract(intent.identity_json,'$.request.target.binding.nativeThreadId'),
          'runtimeGeneration',json_extract(intent.identity_json,'$.request.target.binding.runtimeGeneration')
        ) AS binding_json
        FROM jones_runtime_stop_intents AS intent
        JOIN jones_runtime_stop_observations AS observation ON observation.command_id=intent.command_id
        WHERE observation.phase='completed' AND observation.result='stopped'
          AND intent.thread_id=${identity.threadId}
          AND json_extract(intent.identity_json,'$.request.threadId')=${identity.threadId}
          AND json_extract(intent.identity_json,'$.request.target.binding.threadId')=${identity.threadId}
          AND json_extract(intent.identity_json,'$.request.target.binding.providerThreadId')=${identity.providerThreadId}
          AND json_extract(intent.identity_json,'$.request.target.binding.providerSessionId')=${identity.providerSessionId}
          AND json_extract(intent.identity_json,'$.request.target.binding.providerInstanceId')=${identity.providerInstanceId}
          AND json_extract(intent.identity_json,'$.request.target.binding.driver')=${identity.driver}
          AND json_extract(intent.identity_json,'$.request.target.binding.nativeThreadId')=${identity.nativeThreadId}
        LIMIT 2
      `;
      const bindings = new Map<string, ProviderRuntimeBinding>();
      for (const row of rows) {
        const binding = yield* Schema.decodeEffect(Schema.fromJsonString(ProviderRuntimeBinding))(
          row.binding_json,
        ).pipe(
          Effect.mapError(
            (cause) => new RuntimeStopError({ reason: "runtime_stop_readback_binding_invalid", cause }),
          ),
        );
        if (
          binding.threadId !== identity.threadId ||
          binding.providerThreadId !== identity.providerThreadId ||
          binding.providerSessionId !== identity.providerSessionId ||
          binding.providerInstanceId !== identity.providerInstanceId ||
          binding.driver !== identity.driver ||
          binding.nativeThreadId !== identity.nativeThreadId
        )
          return yield* new RuntimeStopError({ reason: "runtime_stop_readback_binding_conflict" });
        bindings.set(nativeCreationCanonicalJson(binding), binding);
      }
      if (bindings.size > 1)
        return yield* new RuntimeStopError({ reason: "runtime_stop_readback_ambiguous" });
      const binding: ProviderRuntimeBinding | undefined = bindings.values().next().value;
      return binding !== undefined && (yield* hasStoppedRuntime(binding)) ? binding : null;
    },
    Effect.mapError((cause) =>
      Schema.is(RuntimeStopError)(cause)
        ? cause
        : new RuntimeStopError({ reason: "runtime_stop_readback_unavailable", cause }),
    ),
  );
  return {
    assertIdentity,
    record,
    readState,
    start,
    complete,
    assertStartAllowed,
    hasStoppedRuntime,
    readStoppedRuntime,
  };
};

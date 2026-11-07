import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type { OrchestrationEffectV2 } from "../../orchestration-v2/EffectOutbox.ts";
import { decodeOrchestrationEffectPayloadV2 } from "../../orchestration-v2/EffectOutbox.ts";
import {
  canonical,
  digest,
  RuntimeUseError,
  RuntimeUseOutboxClaim,
  runtimeUseError,
} from "./RuntimeUseProtocol.ts";

class CapturedRuntimeUseClaim extends Context.Service<
  CapturedRuntimeUseClaim,
  {
    readonly claim: RuntimeUseOutboxClaim;
  }
>()("t3/jones/cleanup/RuntimeUseClaim/CapturedRuntimeUseClaim") {}
const decodeClaim = Schema.decodeUnknownEffect(RuntimeUseOutboxClaim, {
  onExcessProperty: "error",
});
const relation = (request: OrchestrationEffectV2["request"]) => ({
  runId: "runId" in request && typeof request.runId === "string" ? request.runId : null,
  attemptId:
    "runAttemptId" in request && typeof request.runAttemptId === "string"
      ? request.runAttemptId
      : "attemptId" in request && typeof request.attemptId === "string"
        ? request.attemptId
        : null,
});

// Called at the actual worker boundary. These are EXPECTED caller values, not a
// lookup of whichever owner happens to be in the outbox when an adapter starts.
export const withRuntimeUseClaim = <A, E, R>(
  effect: OrchestrationEffectV2,
  workerId: string,
  execution: Effect.Effect<A, E, R>,
) => {
  const request = effect.request;
  const { runId, attemptId } = relation(request);
  const nativeReference = effect.nativeCreationExecutionReference ?? null;
  return execution.pipe(
    Effect.provideService(CapturedRuntimeUseClaim, {
      claim: {
        schema: "jones.workspace-runtime-outbox-claim/v1",
        effectId: effect.id,
        commandId: effect.commandId,
        threadId: effect.threadId,
        workerId,
        attemptCount: effect.attemptCount,
        claimedAt: effect.updatedAt,
        runId,
        attemptId,
        requestSha256: digest({ request, nativeReference }),
        nativeReference,
      },
    }),
  );
};
export const currentRuntimeUseClaim = Effect.serviceOption(CapturedRuntimeUseClaim).pipe(
  Effect.map(Option.map((value) => value.claim)),
);

export const validateRuntimeUseClaim = Effect.fn("RuntimeUseClaim.validate")(function* (
  expected: RuntimeUseOutboxClaim,
) {
  const claim = yield* decodeClaim(expected);
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{
    readonly effect_id: string;
    readonly command_id: string;
    readonly thread_id: string;
    readonly status: string;
    readonly lease_owner: string | null;
    readonly attempt_count: number;
    readonly updated_at: string;
    readonly payload_json: string;
  }>`SELECT effect_id,command_id,thread_id,status,lease_owner,attempt_count,updated_at,payload_json
    FROM orchestration_v2_effect_outbox WHERE effect_id=${claim.effectId}`;
  const row = rows[0];
  if (
    row === undefined ||
    row.status !== "running" ||
    row.effect_id !== claim.effectId ||
    row.command_id !== claim.commandId ||
    row.thread_id !== claim.threadId ||
    row.lease_owner !== claim.workerId ||
    row.attempt_count !== claim.attemptCount ||
    row.updated_at !== claim.claimedAt
  )
    return yield* new RuntimeUseError({ reason: "runtime_use_expected_outbox_claim_changed" });
  const payload = yield* decodeOrchestrationEffectPayloadV2(row.payload_json);
  const nativeReference =
    "nativeCreationExecutionReference" in payload
      ? (payload.nativeCreationExecutionReference ?? null)
      : null;
  const actual = relation(payload.request);
  if (
    digest({ request: payload.request, nativeReference }) !== claim.requestSha256 ||
    canonical(nativeReference) !== canonical(claim.nativeReference) ||
    actual.runId !== claim.runId ||
    actual.attemptId !== claim.attemptId
  )
    return yield* new RuntimeUseError({ reason: "runtime_use_original_outbox_reference_changed" });
}, Effect.mapError(runtimeUseError));

import { ThreadId, RunId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import {
  NativeCreationExecutionReferenceV2,
  NativeCreationWholeOperationEvidence,
} from "./NativeCreationExecutionTypes.ts";
import {
  decodeOrchestrationEffectPayloadV2,
  NativeOrchestrationEffectPayloadV2,
  OrchestrationEffectRequestV2,
} from "../../orchestration-v2/EffectOutbox.ts";
import { executeNativeProviderEffect } from "./NativeCreationProviderExecution.ts";

const reference = {
  version: 2,
  claimId: "synthetic-claim",
  stageCommandId: "synthetic-command",
  effectId: "synthetic-effect",
  stage: "native_command",
};
it.effect("retains ordinary payload compatibility and native authority lineage", () =>
  Effect.gen(function* () {
    const request = yield* Schema.decodeUnknownEffect(OrchestrationEffectRequestV2)({
      type: "provider-turn.start",
      runId: "synthetic-run",
    });
    assert.deepEqual(
      yield* decodeOrchestrationEffectPayloadV2(
        yield* Schema.encodeEffect(Schema.fromJsonString(OrchestrationEffectRequestV2))(request),
      ),
      {
        request,
      },
    );
    const envelope = yield* Schema.decodeUnknownEffect(NativeOrchestrationEffectPayloadV2)({
      request,
      nativeCreationExecutionReference: reference,
    });
    assert.deepEqual(
      yield* decodeOrchestrationEffectPayloadV2(
        yield* Schema.encodeEffect(Schema.fromJsonString(NativeOrchestrationEffectPayloadV2))(
          envelope,
        ),
      ),
      envelope,
    );
  }),
);
it.effect("does not downgrade malformed native envelopes to ordinary requests", () =>
  Effect.gen(function* () {
    for (const payload of [
      { request: { type: "provider-turn.start", runId: "synthetic-run" } },
      {
        type: "provider-turn.start",
        runId: "synthetic-run",
        nativeCreationExecutionReference: null,
      },
      {
        request: { type: "provider-turn.start", runId: "synthetic-run" },
        nativeCreationExecutionReference: { ...reference, permission: true },
      },
    ]) {
      const exit = yield* decodeOrchestrationEffectPayloadV2(
        yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown))(payload),
      ).pipe(Effect.exit);
      assert.isTrue(exit._tag === "Failure");
    }
  }),
);
it("references and evidence reject excess wire authority", () => {
  assert.throws(() =>
    Schema.decodeUnknownSync(NativeCreationExecutionReferenceV2)({ ...reference, grant: true }),
  );
  assert.throws(() =>
    Schema.decodeUnknownSync(NativeCreationWholeOperationEvidence)({
      version: 1,
      outcome: "confirmed_success",
      coverage: "final_rpc",
    }),
  );
});
it.effect("an unbound whole-operation executor cannot invoke ordinary provider start", () =>
  Effect.gen(function* () {
    const decodedReference = yield* Schema.decodeUnknownEffect(NativeCreationExecutionReferenceV2)(
      reference,
    );
    const threadId = yield* Schema.decodeUnknownEffect(ThreadId)("synthetic-thread");
    const runId = yield* Schema.decodeUnknownEffect(RunId)("synthetic-run");
    const rejection = yield* executeNativeProviderEffect({
      id: "synthetic-effect",
      commandId: decodedReference.stageCommandId,
      threadId,
      request: {
        type: "provider-turn.start",
        runId,
      },
      nativeCreationExecutionReference: decodedReference,
      status: "running",
      attemptCount: 1,
      leaseOwner: "synthetic-worker",
      leaseExpiresAt: "2099-01-01T00:00:00Z",
      availableAt: "2026-10-07T00:00:00Z",
      createdAt: "2026-10-07T00:00:00Z",
      updatedAt: "2026-10-07T00:00:00Z",
      completedAt: null,
      lastError: null,
    }).pipe(Effect.flip);
    assert.strictEqual(rejection._tag, "NativeCreationProviderExecutionError");
  }),
);

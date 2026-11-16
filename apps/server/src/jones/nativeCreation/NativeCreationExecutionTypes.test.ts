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

const decodeEffectOrchestrationEffectRequestV2 = Schema.decodeEffect(OrchestrationEffectRequestV2);
const encodeEffectFromJsonStringOrchestrationEffectRequestV2 = Schema.encodeEffect(
  Schema.fromJsonString(OrchestrationEffectRequestV2),
);
const decodeUnknownEffectNativeOrchestrationEffectPayloadV2 = Schema.decodeUnknownEffect(
  NativeOrchestrationEffectPayloadV2,
);
const encodeEffectFromJsonStringNativeOrchestrationEffectPayloadV2 = Schema.encodeEffect(
  Schema.fromJsonString(NativeOrchestrationEffectPayloadV2),
);

it.effect("retains ordinary payload compatibility and native authority lineage", () =>
  Effect.gen(function* () {
    const request = yield* decodeEffectOrchestrationEffectRequestV2({
      type: "provider-turn.start",
      runId: "synthetic-run",
    });
    assert.deepEqual(
      yield* decodeOrchestrationEffectPayloadV2(
        yield* encodeEffectFromJsonStringOrchestrationEffectRequestV2(request),
      ),
      {
        request,
      },
    );
    const envelope = yield* decodeUnknownEffectNativeOrchestrationEffectPayloadV2({
      request,
      nativeCreationExecutionReference: reference,
    });
    assert.deepEqual(
      yield* decodeOrchestrationEffectPayloadV2(
        yield* encodeEffectFromJsonStringNativeOrchestrationEffectPayloadV2(envelope),
      ),
      envelope,
    );
  }),
);

const encodeEffectFromJsonStringUnknown = Schema.encodeEffect(
  Schema.fromJsonString(Schema.Unknown),
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
        yield* encodeEffectFromJsonStringUnknown(payload),
      ).pipe(Effect.exit);
      assert.isTrue(exit._tag === "Failure");
    }
  }),
);

const decodeUnknownSyncNativeCreationExecutionReferenceV2 = Schema.decodeUnknownSync(
  NativeCreationExecutionReferenceV2,
);
const decodeUnknownSyncNativeCreationWholeOperationEvidence = Schema.decodeUnknownSync(
  NativeCreationWholeOperationEvidence,
);

it("references and evidence reject excess wire authority", () => {
  assert.throws(() =>
    decodeUnknownSyncNativeCreationExecutionReferenceV2({ ...reference, grant: true }),
  );
  assert.throws(() =>
    decodeUnknownSyncNativeCreationWholeOperationEvidence({
      version: 1,
      outcome: "confirmed_success",
      coverage: "final_rpc",
    }),
  );
});

const decodeUnknownEffectNativeCreationExecutionReferenceV2 = Schema.decodeUnknownEffect(
  NativeCreationExecutionReferenceV2,
);
const decodeEffectThreadId = Schema.decodeEffect(ThreadId);
const decodeEffectRunId = Schema.decodeEffect(RunId);

it.effect("an unbound whole-operation executor cannot invoke ordinary provider start", () =>
  Effect.gen(function* () {
    const decodedReference =
      yield* decodeUnknownEffectNativeCreationExecutionReferenceV2(reference);
    const threadId = yield* decodeEffectThreadId("synthetic-thread");
    const runId = yield* decodeEffectRunId("synthetic-run");
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

      availableAt: "2026-10-07T00:00:00Z",
      createdAt: "2026-10-07T00:00:00Z",
      updatedAt: "2026-10-07T00:00:00Z",
      completedAt: null,
      lastError: null,
    }).pipe(Effect.flip);
    assert.strictEqual(rejection._tag, "NativeCreationProviderExecutionError");
  }),
);

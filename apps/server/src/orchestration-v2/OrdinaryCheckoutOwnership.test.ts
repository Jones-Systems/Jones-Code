import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Cause from "effect/Cause";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";

import {
  OrdinaryCheckoutExecutionExecutorV1,
  OrdinaryCheckoutExecutionRefV1,
  OrdinaryCheckoutUseV1,
  decodeOrdinaryCheckoutExecutionRefV1,
  makeOrdinaryCheckoutExecutionRefV1,
  ordinaryCheckoutExecutionAssociationIdV1,
  ordinaryCheckoutOutboxOperationIdV1,
} from "./OrdinaryCheckoutOwnership.ts";

const timestamp = "2026-10-03T00:00:00.000Z";
const reference = { version: 1, admissionId: "a".repeat(64), admissionSha256: "b".repeat(64) };
const source = {
  kind: "outbox",
  link: {
    version: 1,
    effectId: "effect:execution-fixture",
    commandId: "command:execution-fixture",
    threadId: "thread:execution-fixture",
    requestSha256: "c".repeat(64),
    admission: reference,
    recordedAt: timestamp,
  },
  workerId: "worker:execution-fixture",
  expectedAttempt: 1,
  leaseExpiresAt: "2026-10-03T00:05:00.000Z",
};
const originalUse = Schema.decodeUnknownSync(OrdinaryCheckoutUseV1, { onExcessProperty: "error" })({
  version: 1,
  kind: "ordinary_checkout_use",
  operationId: "effect:execution-fixture:ordinary-checkout:attempt:1",
  admission: reference,
  source,
  lease: {
    resourcePath: "/fixture/checkout",
    leaseId: "lease:execution-fixture",
    ownerThreadId: "thread:execution-fixture",
    ownerIncarnation: "fixture-original-birth",
    branch: "fixture-branch",
    acquiredAtMs: 1,
    renewedAtMs: 1,
    expiresAtMs: 300001,
  },
});
const claimExecutor = Schema.decodeUnknownSync(OrdinaryCheckoutExecutionExecutorV1, {
  onExcessProperty: "error",
})({
  kind: "actual_outbox_claim",
  source,
});
const claimRef = makeOrdinaryCheckoutExecutionRefV1({ originalUse, executor: claimExecutor });
const managed = {
  kind: "captured_managed_run",
  captureId: "capture:execution-fixture",
  run: {
    runId: "run:execution-fixture",
    runAttemptId: "attempt:execution-fixture",
    nodeId: "node:execution-fixture",
    messageId: "message:execution-fixture",
  },
  checkpointScopeId: "scope:execution-fixture",
  driver: "codex",
  binding: {
    threadId: "thread:execution-fixture",
    providerThreadId: "provider-thread:execution-fixture",
    providerSessionId: "provider-session:execution-fixture",
    instanceId: "fixture-codex",
  },
};

it.effect(
  "execution reference identity uses canonical encoded immutable seed excluding its own association ID",
  () =>
    Effect.gen(function* () {
      assert.equal(
        claimRef.associationId,
        "2c6a1fd6a68018d0f8ce3ee62bfc467347938bb5dcde66494e22daf4029044ff",
      );
      assert.equal(
        claimRef.associationId,
        ordinaryCheckoutExecutionAssociationIdV1({
          version: 1,
          originalUse,
          executor: claimExecutor,
        }),
      );
      const encoded = yield* Schema.encodeEffect(OrdinaryCheckoutExecutionRefV1)(claimRef).pipe(
        Effect.orDie,
      );
      assert.deepEqual(decodeOrdinaryCheckoutExecutionRefV1(encoded), claimRef);
      const reordered = {
        executor: encoded.executor,
        associationId: encoded.associationId,
        originalUse: encoded.originalUse,
        version: encoded.version,
      };
      assert.deepEqual(decodeOrdinaryCheckoutExecutionRefV1(reordered), claimRef);
      assert.deepEqual(
        makeOrdinaryCheckoutExecutionRefV1({ originalUse, executor: claimExecutor }),
        claimRef,
      );
    }),
);

it.effect(
  "managed and real prepared executor variants preserve absent provider evidence and original prepared source",
  () =>
    Effect.gen(function* () {
      const executor = yield* Schema.decodeUnknownEffect(OrdinaryCheckoutExecutionExecutorV1, {
        onExcessProperty: "error",
      })(managed).pipe(Effect.orDie);
      const ref = makeOrdinaryCheckoutExecutionRefV1({ originalUse, executor });
      const encoded = yield* Schema.encodeEffect(OrdinaryCheckoutExecutionRefV1)(ref).pipe(
        Effect.orDie,
      );
      assert.notEqual(ref.associationId, claimRef.associationId);
      assert.deepEqual(decodeOrdinaryCheckoutExecutionRefV1(encoded), ref);
      for (const key of [
        "runtimeGeneration",
        "nativeThreadId",
        "evidenceRevision",
        "providerTurnId",
      ])
        assert.isFalse(Object.hasOwn(encoded.executor, key));
      for (const prepared of [
        { kind: "prepared_run", admission: reference, preparation: managed.run },
        {
          kind: "prepared_launch",
          admission: reference,
          preparationCommandId: "command:prepared-fixture",
          preparationEvent: {
            eventId: "event:prepared-fixture",
            sequence: 1,
            threadId: "thread:execution-fixture",
            commandId: "command:prepared-fixture",
            eventType: "thread.created",
          },
          applicationBirth: {
            kind: "application_v2_thread_birth",
            threadId: "thread:execution-fixture",
            eventId: "event:prepared-fixture",
            sequence: 1,
          },
          projectId: "project:execution-fixture",
          canonicalProjectRoot: "/fixture/repo",
          canonicalCheckoutPath: "/fixture/checkout",
          branch: "fixture-branch",
        },
      ]) {
        const preparedUse = yield* Schema.decodeUnknownEffect(OrdinaryCheckoutUseV1, {
          onExcessProperty: "error",
        })({
          ...(yield* Schema.encodeEffect(OrdinaryCheckoutUseV1)(originalUse).pipe(Effect.orDie)),
          source: prepared,
        }).pipe(Effect.orDie);
        const producer = yield* Schema.decodeUnknownEffect(OrdinaryCheckoutExecutionExecutorV1, {
          onExcessProperty: "error",
        })({
          kind: "actual_prepared_producer",
          producerId: "producer:prepared-fixture",
          source: prepared,
        }).pipe(Effect.orDie);
        const preparedRef = makeOrdinaryCheckoutExecutionRefV1({
          originalUse: preparedUse,
          executor: producer,
        });
        assert.deepEqual(
          decodeOrdinaryCheckoutExecutionRefV1(
            yield* Schema.encodeEffect(OrdinaryCheckoutExecutionRefV1)(preparedRef).pipe(
              Effect.orDie,
            ),
          ),
          preparedRef,
        );
      }
    }),
);

it.effect("changed immutable use or requesting claim cannot reuse an old association digest", () =>
  Effect.gen(function* () {
    const raw = yield* Schema.encodeEffect(OrdinaryCheckoutExecutionRefV1)(claimRef).pipe(
      Effect.orDie,
    );
    if (raw.executor.kind !== "actual_outbox_claim")
      throw new Error("Expected actual claim fixture");
    for (const changed of [
      { ...raw, associationId: "d".repeat(64) },
      { ...raw, originalUse: { ...raw.originalUse, operationId: "other-operation" } },
      {
        ...raw,
        originalUse: {
          ...raw.originalUse,
          admission: { ...raw.originalUse.admission, admissionSha256: "d".repeat(64) },
        },
      },
      {
        ...raw,
        originalUse: {
          ...raw.originalUse,
          lease: { ...raw.originalUse.lease, resourcePath: "/fixture/changed" },
        },
      },
      {
        ...raw,
        originalUse: {
          ...raw.originalUse,
          lease: { ...raw.originalUse.lease, leaseId: "lease:changed" },
        },
      },
      {
        ...raw,
        originalUse: {
          ...raw.originalUse,
          lease: { ...raw.originalUse.lease, ownerIncarnation: "birth:changed" },
        },
      },
      {
        ...raw,
        executor: {
          ...raw.executor,
          source: { ...raw.executor.source, workerId: "worker:changed" },
        },
      },
      {
        ...raw,
        executor: { ...raw.executor, source: { ...raw.executor.source, expectedAttempt: 2 } },
      },
      {
        ...raw,
        executor: {
          ...raw.executor,
          source: { ...raw.executor.source, leaseExpiresAt: "2026-10-03T00:06:00.000Z" },
        },
      },
    ])
      assert.throws(() => decodeOrdinaryCheckoutExecutionRefV1(changed));
  }),
);

it.effect(
  "closed executor codec rejects substitute proof booleans, invented kinds and invalid optional evidence",
  () =>
    Effect.gen(function* () {
      const decode = Schema.decodeUnknownEffect(OrdinaryCheckoutExecutionExecutorV1, {
        onExcessProperty: "error",
      });
      for (const invalid of [
        { ...managed, kind: "current_thread_lookup" },
        { ...managed, isCurrent: true },
        { ...managed, captureId: "" },
        { ...managed, evidenceRevision: 0 },
        { ...managed, evidenceRevision: 1.5 },
        { ...managed, runtimeGeneration: "" },
        { ...managed, nativeThreadId: null },
        { ...managed, providerTurnId: null },
        { kind: "actual_outbox_claim", source: { ...source, expectedAttempt: 0 } },
        {
          kind: "actual_prepared_producer",
          producerId: "producer",
          source: { ...source, kind: "prepared_run" },
        },
      ]) {
        const exit = yield* decode(invalid).pipe(Effect.orDie, Effect.exit);
        assert.isTrue(Exit.isFailure(exit));
        if (Exit.isFailure(exit)) {
          assert.isTrue(Cause.hasDies(exit.cause));
          assert.isTrue(Schema.isSchemaError(Cause.squash(exit.cause)));
        }
      }
      assert.throws(() =>
        decodeOrdinaryCheckoutExecutionRefV1({
          ...Schema.encodeSync(OrdinaryCheckoutExecutionRefV1)(claimRef),
          authoritative: true,
        }),
      );
    }),
);

it.effect(
  "new outbox operations bind positive actual attempts while historical operation IDs remain unchanged reads",
  () =>
    Effect.gen(function* () {
      assert.equal(
        ordinaryCheckoutOutboxOperationIdV1("effect:execution-fixture", 1),
        "effect:execution-fixture:ordinary-checkout:attempt:1",
      );
      assert.equal(
        ordinaryCheckoutOutboxOperationIdV1("effect:execution-fixture", 2),
        "effect:execution-fixture:ordinary-checkout:attempt:2",
      );
      for (const attempt of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY])
        assert.throws(() =>
          ordinaryCheckoutOutboxOperationIdV1("effect:execution-fixture", attempt),
        );
      for (const id of ["", " ", " effect:execution-fixture", "effect:execution-fixture "])
        assert.throws(() => ordinaryCheckoutOutboxOperationIdV1(id, 1));
      const historical = yield* Schema.decodeUnknownEffect(OrdinaryCheckoutUseV1, {
        onExcessProperty: "error",
      })({
        ...(yield* Schema.encodeEffect(OrdinaryCheckoutUseV1)(originalUse).pipe(Effect.orDie)),
        operationId: "effect:execution-fixture:ordinary-checkout",
      }).pipe(Effect.orDie);
      assert.equal(historical.operationId, "effect:execution-fixture:ordinary-checkout");
    }),
);

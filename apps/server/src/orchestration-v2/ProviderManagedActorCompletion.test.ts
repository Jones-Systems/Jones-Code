import { assert, describe, it } from "@effect/vitest";
import { CheckpointScopeId, ProviderThreadId } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";

import type { ProviderAdapterV2SessionRuntime } from "./ProviderAdapter.ts";
import {
  OrdinaryCheckoutAdmissionV1,
  OrdinaryCheckoutCaptureV1,
  OrdinaryCheckoutExecutionExecutorV1,
  OrdinaryCheckoutExecutionRefV1,
  OrdinaryCheckoutUseV1,
  makeOrdinaryCheckoutExecutionRefV1,
  ordinaryApplicationIncarnationV1,
  ordinaryCheckoutAdmissionIdV1,
  ordinaryCheckoutAdmissionRefV1,
  ordinaryCheckoutCommandDigestV1,
} from "./OrdinaryCheckoutOwnership.ts";
import { nativeCreationCanonicalJson, nativeCreationSha256 } from "./NativeCreationPreparation.ts";
import {
  ProviderManagedActorClosureV1,
  ProviderManagedActorCompletionError,
  ProviderManagedActorSourceV1,
  prepareProviderManagedActorRun,
  readProviderManagedActorClosure,
  readProviderManagedActorExecution,
  registerProviderManagedActorProducer,
  validateIssuedProviderManagedActorClosure,
  withProviderManagedActorExecution,
  type ProviderManagedActorAdmissionV1,
  type ProviderManagedActorClosureReadV1,
  type ProviderManagedActorIssuerV1,
  type ProviderManagedActorProducerGuardsV1,
  type ProviderManagedActorRunReaderV1,
} from "./ProviderManagedActorCompletion.ts";

const UnknownFromJsonString = Schema.fromJsonString(Schema.Unknown);
const at = "2026-10-03T00:00:00.000Z";
let fixtureOrdinal = 0;
function fixture() {
  const suffix = String(++fixtureOrdinal);
  const threadId = `thread:${suffix}`;
  const commandId = `command:${suffix}`;
  const birth = {
    kind: "application_v2_thread_birth" as const,
    threadId,
    eventId: `event:${suffix}`,
    sequence: 1,
  };
  const command = { type: "message.send", commandId, threadId };
  const capture = Schema.decodeUnknownSync(OrdinaryCheckoutCaptureV1)({
    version: 1,
    commandId,
    commandType: command.type,
    canonicalCommand: command,
    commandDigest: ordinaryCheckoutCommandDigestV1(command),
    origin: { kind: "command" },
    threadId,
    applicationBirth: birth,
    projectId: `project:${suffix}`,
    canonicalProjectRoot: "/fixture/repo",
    canonicalCheckoutPath: "/fixture/checkout",
    branch: "fixture-branch",
    lease: {
      resourcePath: "/fixture/checkout",
      leaseId: `lease:${suffix}`,
      ownerThreadId: threadId,
      ownerIncarnation: ordinaryApplicationIncarnationV1(
        Schema.decodeUnknownSync(OrdinaryCheckoutCaptureV1.fields.applicationBirth)(birth),
      ),
      branch: "fixture-branch",
      acquiredAtMs: 1,
      renewedAtMs: 1,
      expiresAtMs: 300001,
    },
  });
  const admission = Schema.decodeUnknownSync(OrdinaryCheckoutAdmissionV1)({
    version: 1,
    admissionId: ordinaryCheckoutAdmissionIdV1(capture),
    capture: Schema.encodeSync(OrdinaryCheckoutCaptureV1)(capture),
    receipt: {
      commandId,
      threadId,
      commandType: command.type,
      acceptedAt: at,
      resultSequence: 1,
      status: "accepted",
      error: null,
    },
    eventBasis: [
      { eventId: birth.eventId, sequence: 1, threadId, commandId, eventType: "message.accepted" },
    ],
    run: {
      runId: `run:${suffix}`,
      runAttemptId: `attempt:${suffix}`,
      nodeId: `node:${suffix}`,
      messageId: `message:${suffix}`,
    },
    recordedAt: at,
  });
  const admissionRef = ordinaryCheckoutAdmissionRefV1(admission);
  const source = {
    kind: "outbox",
    link: {
      version: 1,
      effectId: `effect:${suffix}`,
      commandId,
      threadId,
      requestSha256: "c".repeat(64),
      admission: admissionRef,
      recordedAt: at,
    },
    workerId: `worker:${suffix}`,
    expectedAttempt: 1,
    leaseExpiresAt: "2026-10-03T00:05:00.000Z",
  };
  const originalUse = Schema.decodeUnknownSync(OrdinaryCheckoutUseV1)({
    version: 1,
    kind: "ordinary_checkout_use",
    operationId: `effect:${suffix}:ordinary-checkout:attempt:1`,
    admission: admissionRef,
    source,
    lease: capture.lease,
  });
  const startExecution = makeOrdinaryCheckoutExecutionRefV1({
    originalUse,
    executor: Schema.decodeUnknownSync(OrdinaryCheckoutExecutionExecutorV1)({
      kind: "actual_outbox_claim",
      source,
    }),
  });
  const actualSource = Schema.decodeUnknownSync(ProviderManagedActorSourceV1)({
    sourceId: `source:${suffix}`,
    driver: "codex",
    instanceId: "fixture-codex",
    providerSessionId: `session:${suffix}`,
    providerThreadId: `provider-thread:${suffix}`,
    threadId,
    nativeThreadId: `native-thread:${suffix}`,
  });
  const runtime = {
    driver: actualSource.driver,
    instanceId: actualSource.instanceId,
    providerSessionId: actualSource.providerSessionId,
  } as ProviderAdapterV2SessionRuntime;
  const offer: ProviderManagedActorAdmissionV1 = {
    startExecution,
    admission,
    checkpointScopeId: CheckpointScopeId.make(`scope:${suffix}`),
    providerThreadId: actualSource.providerThreadId,
  };
  const managedExecutor = Schema.decodeUnknownSync(OrdinaryCheckoutExecutionExecutorV1)({
    kind: "captured_managed_run",
    captureId: `capture:${suffix}`,
    run: admission.run,
    checkpointScopeId: offer.checkpointScopeId,
    driver: actualSource.driver,
    binding: {
      threadId,
      providerThreadId: actualSource.providerThreadId,
      providerSessionId: actualSource.providerSessionId,
      instanceId: actualSource.instanceId,
    },
    nativeThreadId: actualSource.nativeThreadId,
  });
  const managedRef = makeOrdinaryCheckoutExecutionRefV1({ originalUse, executor: managedExecutor });
  return { runtime, offer, actualSource, managedRef };
}

function prepare<E = never>(
  f: ReturnType<typeof fixture>,
  guards: ProviderManagedActorProducerGuardsV1<E> = {
    revalidateMutation: Effect.void,
    revalidateCompletion: Effect.void,
  },
) {
  return Effect.gen(function* () {
    let issuer!: ProviderManagedActorIssuerV1;
    registerProviderManagedActorProducer(f.runtime, (input) =>
      Effect.sync(() => {
        issuer = input.issuer;
        return guards;
      }),
    );
    const reader = yield* prepareProviderManagedActorRun(f.runtime, f.offer);
    yield* Effect.addFinalizer(() => reader.release);
    return { issuer, reader };
  });
}

const decodeClosure = Schema.decodeUnknownSync(ProviderManagedActorClosureV1, {
  onExcessProperty: "error",
});
const closed = (value: ProviderManagedActorClosureReadV1) => {
  assert.equal(value.status, "closed");
  if (value.status !== "closed") throw new Error("Expected the original issued closure");
  return value.observation;
};
const failedReason = (exit: Exit.Exit<unknown, ProviderManagedActorCompletionError>) => {
  assert.isTrue(Exit.isFailure(exit));
};
function endRoot(f: ReturnType<typeof fixture>, issuer: ProviderManagedActorIssuerV1) {
  return Effect.gen(function* () {
    const actor = yield* issuer.admitActor({
      kind: "foreground",
      actualSource: f.actualSource,
      completionMode: "native_endpoint",
    });
    yield* issuer.markActorEntered(actor);
    yield* issuer.recordNativeEndpoint(actor, {
      kind: "native_endpoint",
      endpoint: "turn/completed",
      nativeThreadId: f.actualSource.nativeThreadId!,
      outcome: "completed",
      observedAt: at,
    });
    return actor;
  });
}
function activate(
  f: ReturnType<typeof fixture>,
  reader: ProviderManagedActorRunReaderV1,
  issuer: ProviderManagedActorIssuerV1,
) {
  return reader.bindManagedExecution(f.managedRef).pipe(Effect.andThen(issuer.seal));
}

describe("private managed actor completion", () => {
  it.effect("requires the actual registered runtime and rejects a second issuer", () =>
    Effect.gen(function* () {
      const f = fixture();
      failedReason(yield* prepareProviderManagedActorRun(f.runtime, f.offer).pipe(Effect.exit));
      const factory = () =>
        Effect.succeed({ revalidateMutation: Effect.void, revalidateCompletion: Effect.void });
      registerProviderManagedActorProducer(f.runtime, factory);
      assert.throws(
        () => registerProviderManagedActorProducer(f.runtime, factory),
        ProviderManagedActorCompletionError,
      );
    }),
  );

  it.effect("keeps one prepared ticket across input spreads and stable lease renewal", () =>
    Effect.gen(function* () {
      const f = fixture();
      const encoded = yield* Schema.encodeEffect(OrdinaryCheckoutExecutionRefV1)(
        f.offer.startExecution,
      );
      const renewedUse = yield* Schema.decodeUnknownEffect(OrdinaryCheckoutUseV1)({
        ...encoded.originalUse,
        lease: { ...encoded.originalUse.lease, renewedAtMs: 2, expiresAtMs: 300002 },
      });
      const renewed = makeOrdinaryCheckoutExecutionRefV1({
        originalUse: renewedUse,
        executor: f.offer.startExecution.executor,
      });
      const g = { ...f, offer: { ...f.offer, startExecution: renewed } };
      const { reader } = yield* prepare(g);
      assert.strictEqual(yield* prepareProviderManagedActorRun(f.runtime, { ...g.offer }), reader);
      failedReason(yield* prepareProviderManagedActorRun(f.runtime, f.offer).pipe(Effect.exit));
    }),
  );

  it.effect(
    "rejects unresolved admission, foreign original claim and conflicting checkpoint scope",
    () =>
      Effect.gen(function* () {
        const f = fixture();
        const { reader } = yield* prepare(f);
        failedReason(
          yield* prepareProviderManagedActorRun(f.runtime, {
            ...f.offer,
            admission: { ...f.offer.admission, run: null },
          }).pipe(Effect.exit),
        );
        failedReason(
          yield* prepareProviderManagedActorRun(f.runtime, {
            ...f.offer,
            startExecution: fixture().offer.startExecution,
          }).pipe(Effect.exit),
        );
        failedReason(
          yield* prepareProviderManagedActorRun(f.runtime, {
            ...f.offer,
            checkpointScopeId: CheckpointScopeId.make("other-scope"),
          }).pipe(Effect.exit),
        );
        assert.equal((yield* reader.readClosure).status, "pending");
      }),
  );

  it.effect(
    "issues normal completion without inventing a generation and rejects JSON-shaped observations",
    () =>
      Effect.gen(function* () {
        const f = fixture();
        const { reader, issuer } = yield* prepare(f);
        assert.isUndefined(yield* issuer.readManagedExecution);
        yield* endRoot(f, issuer);
        assert.equal((yield* reader.readClosure).status, "pending");
        yield* activate(f, reader, issuer);
        assert.deepEqual(yield* issuer.readManagedExecution, f.managedRef);
        assert.notDeepEqual(yield* issuer.readManagedExecution, f.offer.startExecution);
        const observation = closed(yield* readProviderManagedActorClosure(f.managedRef));
        assert.isFalse(
          Object.hasOwn(observation.descriptor.actors[0]!.source, "runtimeGeneration"),
        );
        assert.isTrue(Object.isFrozen(observation));
        assert.isTrue(Object.isFrozen(observation.descriptor.actors[0]!.source));
        const proof = validateIssuedProviderManagedActorClosure(observation, f.managedRef);
        assert.isNotNull(proof);
        yield* proof!.revalidateIssued;
        assert.isNull(validateIssuedProviderManagedActorClosure({ ...observation }, f.managedRef));
        const observationJson = yield* Schema.encodeEffect(UnknownFromJsonString)(observation);
        const copiedObservation =
          yield* Schema.decodeUnknownEffect(UnknownFromJsonString)(observationJson);
        assert.isNull(validateIssuedProviderManagedActorClosure(copiedObservation, f.managedRef));
        assert.isNull(validateIssuedProviderManagedActorClosure(observation, fixture().managedRef));
        assert.strictEqual(closed(yield* reader.readClosure), observation);
        assert.isUndefined((reader as unknown as Record<string, unknown>).recordNativeEndpoint);
      }),
  );

  it.effect(
    "retains an early native endpoint until its actual matching ACK and committed managed binding",
    () =>
      Effect.gen(function* () {
        const f = fixture();
        const { issuer, reader } = yield* prepare(f);
        const actor = yield* issuer.admitActor({
          kind: "native_request",
          actualSource: f.actualSource,
          completionMode: "native_endpoint",
        });
        yield* issuer.markActorEntered(actor);
        yield* issuer.recordNativeEndpoint(actor, {
          kind: "native_endpoint",
          endpoint: "turn/completed",
          nativeTurnId: "native-turn",
          outcome: "completed",
          observedAt: at,
        });
        yield* issuer.seal;
        const entered = yield* Deferred.make<void>();
        const waiter = yield* Effect.gen(function* () {
          yield* Deferred.succeed(entered, undefined);
          return yield* reader.awaitNativeClosure;
        }).pipe(Effect.forkChild);
        yield* Deferred.await(entered);
        yield* reader.bindManagedExecution(f.managedRef);
        assert.equal((yield* reader.readClosure).status, "pending");
        assert.isUndefined(waiter.pollUnsafe());
        yield* issuer.bindActorNativeIdentity(actor, { nativeTurnId: "native-turn" });
        const observation = closed(yield* Fiber.join(waiter));
        assert.equal(observation.descriptor.actors[0]!.source.nativeTurnId, "native-turn");
      }),
  );

  it.effect("does not join an early endpoint to an unrelated native ACK", () =>
    Effect.gen(function* () {
      const f = fixture();
      const { issuer, reader } = yield* prepare(f);
      const actor = yield* issuer.admitActor({
        kind: "foreground",
        actualSource: f.actualSource,
        completionMode: "native_endpoint",
      });
      yield* issuer.markActorEntered(actor);
      yield* issuer.recordNativeEndpoint(actor, {
        kind: "native_endpoint",
        endpoint: "turn/completed",
        nativeTurnId: "early-turn",
        outcome: "completed",
        observedAt: at,
      });
      failedReason(
        yield* issuer
          .bindActorNativeIdentity(actor, { nativeTurnId: "different-turn" })
          .pipe(Effect.exit),
      );
      assert.deepEqual(yield* reader.readClosure, {
        status: "unknown",
        reason: "endpoint_source_mismatch",
      });
    }),
  );

  it.effect(
    "qualifies a genuinely completed original binding before activation without issuing a closure",
    () =>
      Effect.gen(function* () {
        const f = fixture();
        let mutationCurrent = true;
        const { issuer, reader } = yield* prepare(f, {
          revalidateMutation: Effect.suspend(() =>
            mutationCurrent ? Effect.void : Effect.fail("original source stopped"),
          ),
          revalidateCompletion: Effect.void,
        });
        yield* endRoot(f, issuer);
        yield* issuer.seal;
        mutationCurrent = false;
        yield* reader.revalidateCompletionBinding;
        assert.equal((yield* reader.readClosure).status, "pending");
        assert.equal((yield* readProviderManagedActorClosure(f.managedRef)).status, "unknown");
        yield* reader.bindManagedExecution(f.managedRef);
        assert.equal((yield* reader.readClosure).status, "closed");
      }),
  );

  it.effect("does not qualify pending, empty or unknown membership for early activation", () =>
    Effect.gen(function* () {
      const f = fixture();
      const { issuer, reader } = yield* prepare(f);
      failedReason(yield* reader.revalidateCompletionBinding.pipe(Effect.exit));
      const actor = yield* issuer.admitActor({
        kind: "foreground",
        actualSource: f.actualSource,
        completionMode: "native_endpoint",
      });
      yield* issuer.markActorEntered(actor);
      yield* issuer.seal;
      failedReason(yield* reader.revalidateCompletionBinding.pipe(Effect.exit));
      yield* issuer.retainUnknown(actor, "native_outcome_unknown");
      failedReason(yield* reader.revalidateCompletionBinding.pipe(Effect.exit));
      const empty = fixture();
      const prepared = yield* prepare(empty);
      yield* prepared.issuer.seal;
      failedReason(yield* prepared.reader.revalidateCompletionBinding.pipe(Effect.exit));
    }),
  );

  it.effect("rejects foreign historical completion binding before any committed ref exists", () =>
    Effect.gen(function* () {
      const f = fixture();
      let original = true;
      const { issuer, reader } = yield* prepare(f, {
        revalidateMutation: Effect.void,
        revalidateCompletion: Effect.suspend(() =>
          original ? Effect.void : Effect.fail("captured source replaced"),
        ),
      });
      yield* endRoot(f, issuer);
      yield* issuer.seal;
      original = false;
      failedReason(yield* reader.revalidateCompletionBinding.pipe(Effect.exit));
      assert.deepEqual(yield* reader.readClosure, {
        status: "unknown",
        reason: "completion_capture_changed",
      });
    }),
  );

  it.effect(
    "root completion retains an admitted child and closes that root's spawning permission",
    () =>
      Effect.gen(function* () {
        const f = fixture();
        const { reader, issuer } = yield* prepare(f);
        const root = yield* issuer.admitActor({
          kind: "foreground",
          actualSource: f.actualSource,
          completionMode: "native_endpoint",
        });
        const child = yield* issuer.admitActor({
          parent: root,
          kind: "background_command",
          actualSource: { ...f.actualSource, sourceId: "child-source", nativeTaskId: "child-task" },
          completionMode: "native_endpoint",
        });
        yield* issuer.markActorEntered(root);
        yield* issuer.markActorEntered(child);
        yield* issuer.recordNativeEndpoint(root, {
          kind: "native_endpoint",
          endpoint: "turn/completed",
          nativeThreadId: f.actualSource.nativeThreadId!,
          outcome: "completed",
          observedAt: at,
        });
        failedReason(
          yield* issuer
            .admitActor({
              parent: root,
              kind: "subagent",
              actualSource: f.actualSource,
              completionMode: "native_endpoint",
            })
            .pipe(Effect.exit),
        );
        yield* activate(f, reader, issuer);
        assert.equal((yield* reader.readClosure).status, "pending");
        yield* issuer.recordNativeEndpoint(child, {
          kind: "native_endpoint",
          endpoint: "command/ended",
          nativeTaskId: "child-task",
          outcome: "completed",
          observedAt: at,
        });
        assert.equal(closed(yield* reader.readClosure).descriptor.actors.length, 2);
      }),
  );

  it.effect(
    "awaits the real registered mutation task and callback joins after a native endpoint",
    () =>
      Effect.gen(function* () {
        const f = fixture();
        const { reader, issuer } = yield* prepare(f);
        const releaseTask = yield* Deferred.make<void>();
        const releaseCallback = yield* Deferred.make<void>();
        const task = yield* Deferred.await(releaseTask).pipe(Effect.forkChild);
        const callback = yield* Deferred.await(releaseCallback).pipe(Effect.forkChild);
        const actor = yield* issuer.admitActor({
          kind: "native_request",
          actualSource: f.actualSource,
          completionMode: "native_endpoint_and_task_join",
        });
        yield* issuer.requireTaskJoin(actor, { taskId: "request-task", fiber: task });
        yield* issuer.requireTaskJoin(actor, { taskId: "callback-chain", fiber: callback });
        yield* issuer.markActorEntered(actor);
        yield* issuer.recordNativeEndpoint(actor, {
          kind: "native_endpoint",
          endpoint: "prompt/result",
          nativeThreadId: f.actualSource.nativeThreadId!,
          outcome: "completed",
          observedAt: at,
        });
        yield* activate(f, reader, issuer);
        const requestJoin = yield* issuer.joinTask(actor, "request-task").pipe(Effect.forkChild);
        const callbackJoin = yield* issuer.joinTask(actor, "callback-chain").pipe(Effect.forkChild);
        assert.equal((yield* reader.readClosure).status, "pending");
        yield* Deferred.succeed(releaseTask, undefined);
        yield* Fiber.join(requestJoin);
        assert.equal((yield* reader.readClosure).status, "pending");
        yield* Deferred.succeed(releaseCallback, undefined);
        yield* Fiber.join(callbackJoin);
        const evidence = closed(yield* reader.awaitNativeClosure).descriptor.actors[0]!.endEvidence;
        assert.equal(evidence.kind, "endpoint_and_task_joins");
        if (evidence.kind === "endpoint_and_task_joins")
          assert.deepEqual(
            evidence.tasks.map((item) => item.taskId),
            ["request-task", "callback-chain"],
          );
        assert.isFalse(Object.isFrozen(task));
      }),
  );

  it.effect(
    "records failed and interrupted actual task exits without substituting native success",
    () =>
      Effect.gen(function* () {
        for (const interrupted of [false, true]) {
          const f = fixture();
          const { issuer, reader } = yield* prepare(f);
          const task = yield* (
            interrupted ? Effect.never : Effect.fail("synthetic task failure")
          ).pipe(Effect.forkChild);
          const actor = yield* issuer.admitActor({
            kind: "client_terminal",
            actualSource: f.actualSource,
            completionMode: "task_join",
          });
          yield* issuer.requireTaskJoin(actor, { taskId: "terminal-child", fiber: task });
          yield* issuer.markActorEntered(actor);
          if (interrupted) yield* Fiber.interrupt(task);
          yield* issuer.joinTask(actor, "terminal-child");
          yield* activate(f, reader, issuer);
          const evidence = closed(yield* reader.readClosure).descriptor.actors[0]!.endEvidence;
          assert.equal(evidence.kind, "task_joins");
          if (evidence.kind === "task_joins")
            assert.equal(evidence.tasks[0]!.outcome, interrupted ? "interrupted" : "failed");
        }
      }),
  );

  it.effect("keeps a dispatched retry actor pending when only its timer is cancelled", () =>
    Effect.gen(function* () {
      const f = fixture();
      const { reader, issuer } = yield* prepare(f);
      const timer = yield* Effect.never.pipe(Effect.forkChild);
      const retry = yield* issuer.admitActor({
        kind: "retry",
        actualSource: f.actualSource,
        completionMode: "native_endpoint_and_task_join",
      });
      yield* issuer.requireTaskJoin(retry, { taskId: "retry-timer", fiber: timer });
      yield* issuer.markActorEntered(retry);
      yield* Fiber.interrupt(timer);
      yield* issuer.joinTask(retry, "retry-timer");
      yield* activate(f, reader, issuer);
      assert.equal((yield* reader.readClosure).status, "pending");
      yield* issuer.retainUnknown(retry, "dispatched_retry_outcome_unknown");
      assert.equal((yield* reader.readClosure).status, "unknown");
    }),
  );

  it.effect("serializes actor admission with cohort sealing before external entry", () =>
    Effect.gen(function* () {
      const f = fixture();
      const guardEntered = yield* Deferred.make<void>();
      const guardRelease = yield* Deferred.make<void>();
      let blockAdmission = false;
      const mutationGuard = Effect.suspend(() =>
        blockAdmission
          ? Deferred.succeed(guardEntered, undefined).pipe(
              Effect.andThen(Deferred.await(guardRelease)),
            )
          : Effect.void,
      );
      const { reader, issuer } = yield* prepare(f, {
        revalidateMutation: mutationGuard,
        revalidateCompletion: Effect.void,
      });
      blockAdmission = true;
      const admission = yield* issuer
        .admitActor({
          kind: "foreground",
          actualSource: f.actualSource,
          completionMode: "known_no_entry",
        })
        .pipe(Effect.forkChild);
      yield* Deferred.await(guardEntered);
      const seal = yield* issuer.seal.pipe(Effect.forkChild);
      assert.isUndefined(seal.pollUnsafe());
      blockAdmission = false;
      yield* Deferred.succeed(guardRelease, undefined);
      const actor = yield* Fiber.join(admission);
      yield* Fiber.join(seal);
      failedReason(yield* issuer.markActorEntered(actor).pipe(Effect.exit));
      yield* issuer.recordNativeEndpoint(actor, {
        kind: "known_no_entry",
        outcome: "no_effect",
        observedAt: at,
      });
      yield* reader.bindManagedExecution(f.managedRef);
      assert.equal(closed(yield* reader.readClosure).descriptor.actors.length, 1);
    }),
  );

  it.effect(
    "accepts historical completed binding while fencing foreign replacement and new entry",
    () =>
      Effect.gen(function* () {
        const f = fixture();
        let mutationCurrent = true;
        let originalSourceRetained = true;
        const { issuer, reader } = yield* prepare(f, {
          revalidateMutation: Effect.suspend(() =>
            mutationCurrent ? Effect.void : Effect.fail("source stopped"),
          ),
          revalidateCompletion: Effect.suspend(() =>
            originalSourceRetained ? Effect.void : Effect.fail("foreign replacement"),
          ),
        });
        yield* endRoot(f, issuer);
        yield* activate(f, reader, issuer);
        mutationCurrent = false;
        const observation = closed(yield* reader.readClosure);
        yield* validateIssuedProviderManagedActorClosure(observation, f.managedRef)!
          .revalidateIssued;
        failedReason(
          yield* issuer
            .admitActor({
              kind: "continuation",
              actualSource: f.actualSource,
              completionMode: "native_endpoint",
            })
            .pipe(Effect.exit),
        );
        originalSourceRetained = false;
        assert.equal((yield* reader.readClosure).status, "unknown");
        assert.isTrue(
          Exit.isFailure(
            yield* validateIssuedProviderManagedActorClosure(
              observation,
              f.managedRef,
            )!.revalidateIssued.pipe(Effect.exit),
          ),
        );
      }),
  );

  it.effect("treats unknown actor evidence as sticky and never closes an empty actor set", () =>
    Effect.gen(function* () {
      const f = fixture();
      const { issuer, reader } = yield* prepare(f);
      yield* activate(f, reader, issuer);
      assert.deepEqual(yield* reader.readClosure, {
        status: "unknown",
        reason: "actor_set_missing",
      });
      const g = fixture();
      const prepared = yield* prepare(g);
      const actor = yield* endRoot(g, prepared.issuer);
      yield* prepared.issuer.retainUnknown(actor, "native_loss");
      yield* activate(g, prepared.reader, prepared.issuer);
      assert.deepEqual(yield* prepared.reader.readClosure, {
        status: "unknown",
        reason: "native_loss",
      });
    }),
  );

  it.effect(
    "distinguishes known no entry from a dispatched request and validates captured actor sources",
    () =>
      Effect.gen(function* () {
        const f = fixture();
        const { issuer, reader } = yield* prepare(f);
        failedReason(
          yield* issuer
            .admitActor({
              kind: "foreground",
              actualSource: {
                ...f.actualSource,
                providerThreadId: ProviderThreadId.make("foreign-thread"),
              },
              completionMode: "native_endpoint",
            })
            .pipe(Effect.exit),
        );
        const actor = yield* issuer.admitActor({
          kind: "foreground",
          actualSource: f.actualSource,
          completionMode: "known_no_entry",
        });
        yield* issuer.markActorEntered(actor);
        failedReason(
          yield* issuer
            .recordNativeEndpoint(actor, {
              kind: "known_no_entry",
              outcome: "no_effect",
              observedAt: at,
            })
            .pipe(Effect.exit),
        );
        yield* activate(f, reader, issuer);
        assert.equal((yield* reader.readClosure).status, "pending");
      }),
  );

  it.effect("retains identical duplicate endpoints but fences conflicting outcomes", () =>
    Effect.gen(function* () {
      const f = fixture();
      const { reader, issuer } = yield* prepare(f);
      const actor = yield* endRoot(f, issuer);
      yield* issuer.recordNativeEndpoint(actor, {
        kind: "native_endpoint",
        endpoint: "turn/completed",
        nativeThreadId: f.actualSource.nativeThreadId!,
        outcome: "completed",
        observedAt: "2026-10-03T00:00:01.000Z",
      });
      yield* activate(f, reader, issuer);
      const first = closed(yield* reader.readClosure);
      failedReason(
        yield* issuer
          .recordNativeEndpoint(actor, {
            kind: "native_endpoint",
            endpoint: "turn/completed",
            nativeThreadId: f.actualSource.nativeThreadId!,
            outcome: "failed",
            observedAt: at,
          })
          .pipe(Effect.exit),
      );
      assert.deepEqual(yield* reader.readClosure, {
        status: "unknown",
        reason: "endpoint_conflict",
      });
      assert.isTrue(
        Exit.isFailure(
          yield* validateIssuedProviderManagedActorClosure(
            first,
            f.managedRef,
          )!.revalidateIssued.pipe(Effect.exit),
        ),
      );
    }),
  );

  it.effect(
    "rejects a foreign managed ref and actual checkpoint scope without registering them",
    () =>
      Effect.gen(function* () {
        const f = fixture();
        const { issuer, reader } = yield* prepare(f);
        yield* endRoot(f, issuer);
        const foreign = fixture().managedRef;
        failedReason(yield* reader.bindManagedExecution(foreign).pipe(Effect.exit));
        assert.equal((yield* readProviderManagedActorClosure(foreign)).status, "unknown");
        if (f.managedRef.executor.kind !== "captured_managed_run")
          throw new Error("Expected captured fixture");
        const wrongScope = makeOrdinaryCheckoutExecutionRefV1({
          originalUse: f.managedRef.originalUse,
          executor: {
            ...f.managedRef.executor,
            checkpointScopeId: CheckpointScopeId.make("foreign-scope"),
          },
        });
        failedReason(yield* reader.bindManagedExecution(wrongScope).pipe(Effect.exit));
        yield* activate(f, reader, issuer);
        assert.equal((yield* reader.readClosure).status, "closed");
      }),
  );

  it.effect(
    "strict closure decoding rejects changed digests, actor trees and source bindings",
    () =>
      Effect.gen(function* () {
        const f = fixture();
        const { issuer, reader } = yield* prepare(f);
        yield* endRoot(f, issuer);
        yield* activate(f, reader, issuer);
        const encoded = yield* Schema.encodeEffect(ProviderManagedActorClosureV1)(
          closed(yield* reader.readClosure).descriptor,
        );
        const actor = encoded.actors[0]!;
        const malformedActors = [
          [actor, actor],
          [{ ...actor, parentActorId: "missing-parent" }],
          [{ ...actor, parentActorId: actor.actorId }],
          [{ ...actor, source: { ...actor.source, providerThreadId: "foreign-provider-thread" } }],
        ];
        const decode = decodeClosure;
        assert.throws(() => decode({ ...encoded, actorSetSha256: "d".repeat(64) }));
        for (const actors of malformedActors)
          assert.throws(() =>
            decode({
              ...encoded,
              actors,
              membershipOrdinal: 20,
              actorSetSha256: nativeCreationSha256(nativeCreationCanonicalJson(actors)),
            }),
          );
        assert.throws(() => decode({ ...encoded, finished: true }));
        assert.deepEqual(decode(encoded), closed(yield* reader.readClosure).descriptor);
      }),
  );

  it.effect(
    "release cleans only this ticket and prevents operation reuse without issuing completion",
    () =>
      Effect.gen(function* () {
        const f = fixture();
        const g = fixture();
        const one = yield* prepare(f);
        const two = yield* prepare(g);
        yield* one.reader.bindManagedExecution(f.managedRef);
        yield* one.reader.release;
        assert.isUndefined(yield* one.issuer.readManagedExecution);
        assert.deepEqual(yield* one.reader.readClosure, {
          status: "unknown",
          reason: "ticket_released",
        });
        assert.equal((yield* readProviderManagedActorClosure(f.managedRef)).status, "unknown");
        failedReason(yield* one.reader.bindManagedExecution(f.managedRef).pipe(Effect.exit));
        failedReason(yield* prepareProviderManagedActorRun(f.runtime, f.offer).pipe(Effect.exit));
        yield* endRoot(g, two.issuer);
        yield* activate(g, two.reader, two.issuer);
        assert.equal((yield* two.reader.readClosure).status, "closed");
      }),
  );

  it.effect(
    "carries only the explicit private execution ref and leaves nonordinary work unchanged",
    () =>
      Effect.gen(function* () {
        const f = fixture();
        assert.isUndefined(yield* readProviderManagedActorExecution);
        const ref = yield* withProviderManagedActorExecution(
          f.managedRef,
          readProviderManagedActorExecution,
        );
        assert.deepEqual(ref, f.managedRef);
        assert.isTrue(Object.isFrozen(ref));
        assert.isUndefined(yield* readProviderManagedActorExecution);
      }),
  );
});

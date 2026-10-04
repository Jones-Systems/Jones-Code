import { assert, it } from "@effect/vitest";
import {
  AuthSessionId,
  MessageId,
  OrchestrationV2ImportedHistoryReviewBasis,
  CheckpointId,
  CheckpointScopeId,
  CommandId,
  EventId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ProjectId,
  ThreadId,
  type OrchestrationV2AppThread,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import type * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";

import * as CheckpointRollbackService from "./CheckpointRollbackService.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import * as EffectWorker from "./EffectWorker.ts";
import {
  ProviderNativeOperationUnknownError,
  nativeEffectEvidenceFromCause,
} from "./ProviderFailure.ts";
import * as NativeCreationAuthority from "./NativeCreationAuthority.ts";
import { NativeCreationRepository } from "../persistence/Services/NativeCreationRepository.ts";
import * as ResourceCleanupService from "./ResourceCleanupService.ts";
import * as RunFinalizationService from "./RunFinalizationService.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as ProviderTurnControlService from "./ProviderTurnControlService.ts";
import * as ProviderTurnStartService from "./ProviderTurnStartService.ts";
import * as RuntimeRequestService from "./RuntimeRequestService.ts";
import * as ThreadTitleRegenerationService from "./ThreadTitleRegenerationService.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";
import * as ServerSettings from "../serverSettings.ts";

const threadId = ThreadId.make("thread:effect-worker-restart");
const oldSessionId = ProviderSessionId.make("provider-session:effect-worker-restart:old");
const replacementSessionId = ProviderSessionId.make(
  "provider-session:effect-worker-restart:replacement",
);
const providerThreadId = ProviderThreadId.make("provider-thread:effect-worker-restart");
const providerTurnId = ProviderTurnId.make("provider-turn:effect-worker-restart");
const attemptId = RunAttemptId.make("run-attempt:effect-worker-restart");
const runId = RunId.make("run:effect-worker-restart");

function restartEffect(
  now: DateTime.Utc,
  sessionTransition: NonNullable<
    Extract<
      EffectOutbox.OrchestrationEffectV2["request"],
      { readonly type: "provider-turn.restart" }
    >["sessionTransition"]
  >,
): EffectOutbox.OrchestrationEffectV2 {
  const timestamp = DateTime.formatIso(now);
  return {
    id: `effect:restart:${sessionTransition.type}`,
    commandId: CommandId.make(`command:restart:${sessionTransition.type}`),
    threadId,
    request: {
      type: "provider-turn.restart",
      providerSessionId: oldSessionId,
      providerThreadId,
      providerTurnId,
      interruptedAttemptId: attemptId,
      runId,
      sessionTransition,
    },
    status: "running",
    attemptCount: 1,
    availableAt: timestamp,
    leaseOwner: "test-worker",
    leaseExpiresAt: timestamp,
    createdAt: timestamp,
    updatedAt: timestamp,
    completedAt: null,
    lastError: null,
  };
}

it.effect("passes rollback outbox identity to checkpoint execution", () =>
  Effect.gen(function* () {
    const events = yield* Ref.make<ReadonlyArray<string>>([]);
    const captured = yield* Ref.make<
      ReadonlyArray<
        Parameters<CheckpointRollbackService.CheckpointRollbackServiceV2Shape["execute"]>[0]
      >
    >([]);
    const now = yield* DateTime.now;
    const request = {
      type: "provider-thread.rollback" as const,
      providerThreadId,
      checkpointId: CheckpointId.make("checkpoint:rollback:actual-source"),
      scopeId: CheckpointScopeId.make("scope:rollback:actual-source"),
      restoreFiles: false,
    };
    const effect: EffectOutbox.OrchestrationEffectV2 = {
      ...restartEffect(now, { type: "detach" }),
      id: "effect:rollback:actual-source",
      commandId: CommandId.make("command:rollback:actual-source"),
      request,
    };
    yield* EffectWorker.OrchestrationEffectExecutorV2.pipe(
      Effect.flatMap((executor) => executor.execute(effect)),
      Effect.provide(
        makeExecutorLayer({
          events,
          checkpointRollback: (input) => Ref.update(captured, (values) => [...values, input]),
        }),
      ),
    );
    assert.deepEqual(yield* Ref.get(captured), [
      {
        threadId,
        providerThreadId,
        checkpointId: request.checkpointId,
        scopeId: request.scopeId,
        restoreFiles: false,
        sourceEffect: { effectId: effect.id, commandId: effect.commandId },
      },
    ]);
    assert.isEmpty(yield* Ref.get(events));
  }),
);

it.effect("forwards the actual complete restart request to its qualified control boundary", () =>
  Effect.gen(function* () {
    const events = yield* Ref.make<ReadonlyArray<string>>([]);
    const effect = restartEffect(yield* DateTime.now, { type: "detach" });
    const seen = yield* Ref.make<
      | Parameters<
          ProviderTurnControlService.ProviderTurnControlServiceV2Shape["interruptAndAwaitTerminal"]
        >[0]
      | null
    >(null);
    yield* EffectWorker.OrchestrationEffectExecutorV2.pipe(
      Effect.flatMap((executor) => executor.execute(effect)),
      Effect.provide(
        makeExecutorLayer({
          events,
          interruptAndAwaitTerminal: (request) => Ref.set(seen, request),
        }),
      ),
    );
    const actual = (yield* Ref.get(seen))!;
    assert.strictEqual(actual.ordinaryCheckoutRestartRequest, effect.request);
    assert.equal(actual.threadId, effect.threadId);
    assert.equal(
      actual.interruptedAttemptId,
      effect.request.type === "provider-turn.restart"
        ? effect.request.interruptedAttemptId
        : undefined,
    );
    assert.deepEqual(yield* Ref.get(events), ["detach", "start"]);
  }),
);

it.effect(
  "settles an ordinary skipped checkpoint without requiring a native start confirmation",
  () =>
    Effect.gen(function* () {
      const events = yield* Ref.make<ReadonlyArray<string>>([]);
      const now = yield* DateTime.now;
      const effect: EffectOutbox.OrchestrationEffectV2 = {
        ...restartEffect(now, { type: "detach" }),
        id: "effect:ordinary-skipped-checkpoint",
        commandId: CommandId.make("command:ordinary-skipped-checkpoint"),
        request: {
          type: "checkpoint.capture",
          runId,
          scopeId: CheckpointScopeId.make("scope:ordinary-skipped-checkpoint"),
        },
      };
      const record = (value: string) => Ref.update(events, (values) => [...values, value]);
      const outbox = Layer.mock(EffectOutbox.EffectOutboxV2)({
        claimNext: () => Effect.succeed(Option.some(effect)),
        get: () => Effect.succeed(Option.some(effect)),
        awaitCancellation: () => Effect.never,
        clearCancellation: () => Effect.void,
        succeed: () => record("ordinary-completed").pipe(Effect.as(true)),
        holdUnknown: () => record("native-start-confirmation-hold").pipe(Effect.as(true)),
      });
      const executor = makeExecutorLayer({ events, outboxLayer: outbox });
      const layer = EffectWorker.layerWithOptions({ workerId: effect.leaseOwner! }).pipe(
        Layer.provide(Layer.merge(outbox, executor)),
      );
      assert.isTrue(
        yield* EffectWorker.OrchestrationEffectWorkerV2.pipe(
          Effect.flatMap((worker) => worker.runOnce),
          Effect.provide(layer),
        ),
      );
      assert.deepEqual(yield* Ref.get(events), ["ordinary-completed"]);
    }),
);

for (const verified of [true, false]) {
  it.effect(
    `accepts an already committed ordinary claim only through its exact settlement readback (${verified})`,
    () =>
      Effect.gen(function* () {
        const events = yield* Ref.make<ReadonlyArray<string>>([]);
        const record = (value: string) => Ref.update(events, (values) => [...values, value]);
        const effect: EffectOutbox.OrchestrationEffectV2 = {
          ...restartEffect(yield* DateTime.now, { type: "detach" }),
          id: "effect:ordinary-committed-checkpoint",
          commandId: CommandId.make("command:ordinary-committed-checkpoint"),
          request: {
            type: "checkpoint.capture",
            runId,
            scopeId: CheckpointScopeId.make("scope:ordinary-committed-checkpoint"),
          },
        };
        const outbox = Layer.mock(EffectOutbox.EffectOutboxV2)({
          claimNext: () => Effect.succeed(Option.some(effect)),
          get: () => Effect.succeed(Option.some(effect)),
          awaitCancellation: () => Effect.never,
          clearCancellation: () => Effect.void,
          succeed: () => record("second-completion").pipe(Effect.as(false)),
        });
        const executor = Layer.succeed(
          EffectWorker.OrchestrationEffectExecutorV2,
          EffectWorker.OrchestrationEffectExecutorV2.of({
            execute: () =>
              Effect.succeed({
                status: "ordinary_claim_settled" as const,
                effectId: effect.id,
                commandId: effect.commandId,
                threadId,
                workerId: effect.leaseOwner!,
                expectedAttempt: effect.attemptCount,
                revalidate: record("qualified-readback").pipe(Effect.as(verified)),
              }),
          }),
        );
        const layer = EffectWorker.layerWithOptions({ workerId: effect.leaseOwner! }).pipe(
          Layer.provide(Layer.merge(outbox, executor)),
        );
        const result = yield* EffectWorker.OrchestrationEffectWorkerV2.pipe(
          Effect.flatMap((worker) => worker.runOnce),
          Effect.provide(layer),
          Effect.result,
        );
        assert.strictEqual(result._tag, verified ? "Success" : "Failure");
        if (result._tag === "Success") assert.isTrue(result.success);
        assert.deepEqual(yield* Ref.get(events), ["qualified-readback"]);
      }),
  );
}

for (const mode of ["ordinary", "imported_missing", "read_failure"] as const) {
  it.effect(
    `prepares attachment inventory for the original prune birth before reading its basis (${mode})`,
    () =>
      Effect.gen(function* () {
        const events = yield* Ref.make<ReadonlyArray<string>>([]);
        const now = yield* DateTime.now;
        const commandId = CommandId.make(`command:prune-inventory:${mode}`);
        const birth = {
          kind: "application_v2_thread_birth" as const,
          threadId,
          eventId: EventId.make("prune-original-birth"),
          sequence: 7,
        };
        const effect: EffectOutbox.OrchestrationEffectV2 = {
          ...restartEffect(now, { type: "detach" }),
          id: `effect:${commandId}:attachment.cleanup:prune`,
          commandId,
          leaseExpiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 5 })),
          request: { type: "attachment.cleanup", attachmentIds: [] },
          attachmentNamespaceCleanup: {
            version: 1,
            mode: "prune_thread",
            ownerBirth: birth,
            triggerEventId: EventId.make("prune-rollback-completed"),
            rollbackEffectId: `effect:${commandId}:rollback`,
          },
        };
        const task: EventSink.AttachmentNamespaceCleanupTaskV1 = {
          version: 1,
          effectId: effect.id,
          commandId,
          threadId,
          reference: effect.attachmentNamespaceCleanup!,
          triggerSequence: 8,
          bindingSha256: "e".repeat(64),
        };
        const basis: EventSink.QualifiedAttachmentNamespaceCleanupBasisV1 = {
          status: "ready",
          task,
          claim: {
            workerId: effect.leaseOwner!,
            expectedAttempt: effect.attemptCount,
            leaseExpiresAt: effect.leaseExpiresAt!,
          },
          basisEventSequence: 8,
          retainedRelativePaths: ["prune-thread/retained-image.png"],
        };
        const latest =
          yield* Ref.make<EventSink.AttachmentNamespaceCleanupRecordedObservationV1 | null>(null);
        const record = (event: string) => Ref.update(events, (values) => [...values, event]);
        const sink = Layer.mock(EventSink.EventSinkV2)({
          readAttachmentNamespaceCleanupTask: () => Effect.succeed(task),
          readAttachmentNamespaceCleanupObservation: () => Ref.get(latest),
          readAttachmentNamespaceCleanupBasis: (input) =>
            record("basis").pipe(
              Effect.as(
                mode === "imported_missing"
                  ? {
                      status: "unavailable" as const,
                      effectId: effect.id,
                      reason: "original_imported_inventory_unavailable",
                    }
                  : basis,
              ),
            ),
        });
        const outbox = Layer.mock(EffectOutbox.EffectOutboxV2)({
          get: () => Effect.succeed(Option.some(effect)),
        });
        const executorLayer = makeExecutorLayer({
          events,
          eventSinkLayer: sink,
          outboxLayer: outbox,
          ensureApplicationAttachmentInventory: (input) =>
            Effect.gen(function* () {
              assert.deepEqual(input, { threadId: task.threadId, expectedBirth: birth });
              assert.strictEqual(input.expectedBirth, birth);
              yield* record("inventory");
              if (mode === "read_failure")
                return yield* Effect.die("synthetic original inventory read failed");
              return { status: "unavailable" as const, reason: "source_not_materialized" };
            }),
          resourceCleanup: {
            cleanupAttachmentNamespace: (actual) =>
              Effect.gen(function* () {
                assert.deepEqual(actual, basis);
                yield* record("namespace-wrapper");
                const observation: EventSink.AttachmentNamespaceCleanupObservationV1 = {
                  version: 1,
                  producer: "attachment_namespace",
                  effectId: effect.id,
                  bindingSha256: task.bindingSha256,
                  workerId: basis.claim.workerId,
                  expectedAttempt: basis.claim.expectedAttempt,
                  basisEventSequence: basis.basisEventSequence,
                  configuredRoot: "/synthetic-attachments",
                  namespaceSegment: "prune-thread",
                  observedAt: DateTime.formatIso(now),
                  outcome: {
                    status: "unknown",
                    removedPaths: [],
                    reason: "synthetic retained invocation",
                  },
                };
                yield* Ref.set(latest, { ordinal: 0, task, basis, observation, status: "unknown" });
                return {
                  status: "unknown" as const,
                  basis,
                  observation,
                  record: { status: "unknown" as const, effectId: effect.id, ordinal: 0 },
                  anchorOrdinal: 0,
                };
              }),
          },
        });
        const result = yield* EffectWorker.OrchestrationEffectExecutorV2.pipe(
          Effect.flatMap((executor) => executor.execute(effect)),
          Effect.provide(executorLayer),
        );
        assert.equal(
          result !== undefined && "status" in result ? result.status : undefined,
          "attachment_namespace_retained",
        );
        assert.deepEqual(
          yield* Ref.get(events),
          mode === "ordinary"
            ? ["inventory", "basis", "namespace-wrapper"]
            : mode === "imported_missing"
              ? ["inventory", "basis"]
              : ["inventory"],
        );
      }),
  );
}

for (const mode of [
  "completed",
  "lost_response",
  "already_completed",
  "unknown",
  "unavailable",
] as const) {
  it.effect(
    `executes attachment namespace cleanup through its qualified wrapper and durable readback (${mode})`,
    () =>
      Effect.gen(function* () {
        const events = yield* Ref.make<ReadonlyArray<string>>([]);
        const now = yield* DateTime.now;
        const timestamp = DateTime.formatIso(now);
        const commandId = CommandId.make(`command:namespace:${mode}`);
        const effect: EffectOutbox.OrchestrationEffectV2 = {
          ...restartEffect(now, { type: "detach" }),
          id: `effect:${commandId}:attachment.cleanup`,
          commandId,
          leaseOwner: "namespace-worker",
          leaseExpiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 5 })),
          request: { type: "attachment.cleanup", attachmentIds: [] },
          attachmentNamespaceCleanup: {
            version: 1,
            mode: "delete_thread",
            ownerBirth: {
              kind: "application_v2_thread_birth",
              threadId,
              eventId: EventId.make("namespace-original-birth"),
              sequence: 1,
            },
            triggerEventId: EventId.make("namespace-deletion"),
          },
        };
        const task: EventSink.AttachmentNamespaceCleanupTaskV1 = {
          version: 1,
          effectId: effect.id,
          commandId,
          threadId,
          reference: effect.attachmentNamespaceCleanup!,
          triggerSequence: 2,
          bindingSha256: "a".repeat(64),
        };
        const basis: EventSink.QualifiedAttachmentNamespaceCleanupBasisV1 = {
          status: "ready",
          task,
          claim: {
            workerId: "namespace-worker",
            expectedAttempt: 1,
            leaseExpiresAt: effect.leaseExpiresAt!,
          },
          basisEventSequence: 2,
          retainedRelativePaths: [],
        };
        const observation: EventSink.AttachmentNamespaceCleanupObservationV1 = {
          version: 1,
          producer: "attachment_namespace",
          effectId: effect.id,
          bindingSha256: task.bindingSha256,
          workerId: "namespace-worker",
          expectedAttempt: 1,
          basisEventSequence: 2,
          configuredRoot: "/synthetic-attachments",
          namespaceSegment: "synthetic-thread",
          observedAt: timestamp,
          outcome: {
            status: "completed",
            matchingPaths: [],
            removedPaths: [],
            retainedPaths: [],
            rootAbsent: true,
          },
        };
        const current = yield* Ref.make(effect);
        const latest =
          yield* Ref.make<EventSink.AttachmentNamespaceCleanupRecordedObservationV1 | null>(null);
        const commit = Effect.gen(function* () {
          yield* Ref.set(latest, { ordinal: 1, task, basis, observation, status: "completed" });
          yield* Ref.set(current, {
            ...effect,
            status: "succeeded",
            completedAt: timestamp,
            leaseOwner: null,
            leaseExpiresAt: null,
          });
        });
        if (mode === "already_completed") yield* commit;
        const outboxLayer = Layer.mock(EffectOutbox.EffectOutboxV2)({
          get: () => Ref.get(current).pipe(Effect.map(Option.some)),
          listHeldByThreadId: () => Effect.succeed([]),
          holdResourceCleanupUnknown: () =>
            Ref.update(events, (values) => [...values, "generic-hold"]).pipe(Effect.as(false)),
        });
        const eventSinkLayer = Layer.mock(EventSink.EventSinkV2)({
          readDeletionCleanupTask: () => Effect.succeed(null),
          readAttachmentNamespaceCleanupTask: () => Effect.succeed(task),
          readAttachmentNamespaceCleanupObservation: () => Ref.get(latest),
          readAttachmentNamespaceCleanupBasis: () =>
            Effect.succeed(
              mode === "unavailable"
                ? { status: "unavailable", effectId: effect.id, reason: "synthetic claim changed" }
                : basis,
            ),
          recordAttachmentNamespaceCleanupObservation: () =>
            Ref.update(events, (values) => [...values, "duplicate-record"]).pipe(
              Effect.as({ status: "stale" as const, effectId: effect.id, ordinal: null }),
            ),
        });
        const executorLayer = makeExecutorLayer({
          events,
          outboxLayer,
          eventSinkLayer,
          resourceCleanup: {
            cleanupAttachmentNamespace: (actual) =>
              Effect.gen(function* () {
                assert.deepEqual(actual, basis);
                yield* Ref.update(events, (values) => [...values, "namespace-wrapper"]);
                if (mode === "unknown") {
                  const unknown = {
                    ...observation,
                    outcome: {
                      status: "unknown" as const,
                      removedPaths: [],
                      reason: "synthetic partial observation",
                    },
                  };
                  yield* Ref.set(latest, {
                    ordinal: 0,
                    task,
                    basis,
                    observation: unknown,
                    status: "unknown",
                  });
                  return {
                    status: "unknown" as const,
                    basis,
                    observation: unknown,
                    record: { status: "unknown" as const, effectId: effect.id, ordinal: 0 },
                    anchorOrdinal: 0,
                  };
                }
                yield* commit;
                return mode === "lost_response"
                  ? {
                      status: "unknown" as const,
                      basis,
                      observation: null,
                      record: null,
                      anchorOrdinal: 0,
                    }
                  : {
                      status: "completed" as const,
                      basis,
                      observation,
                      record: { status: "completed" as const, effectId: effect.id, ordinal: 1 },
                      anchorOrdinal: 0,
                    };
              }),
          },
        });
        const executed = yield* EffectWorker.OrchestrationEffectExecutorV2.pipe(
          Effect.flatMap((executor) => executor.execute(effect)),
          Effect.exit,
          Effect.provide(executorLayer),
        );
        assert.deepEqual(
          yield* Ref.get(events),
          mode === "already_completed" || mode === "unavailable" ? [] : ["namespace-wrapper"],
        );
        assert.isTrue(Exit.isSuccess(executed));
        if (Exit.isFailure(executed)) return;
        const result = executed.value;
        assert.equal(
          result !== undefined && "status" in result ? result.status : undefined,
          mode === "unknown" || mode === "unavailable"
            ? "attachment_namespace_retained"
            : "cleanup_completed",
        );
        if (result !== undefined && "revalidate" in result) {
          const revalidated = yield* Effect.exit(result.revalidate);
          assert.isTrue(Exit.isSuccess(revalidated));
          if (Exit.isSuccess(revalidated)) assert.equal(revalidated.value, mode !== "unavailable");
        }
      }),
  );
}

for (const mode of ["claimed", "denied", "same_attempt_replay"] as const) {
  it.effect(
    mode === "same_attempt_replay"
      ? "reconciles original namespace UNKNOWN without dispatching filesystem cleanup again"
      : `resumes attachment namespace cleanup through the daemon's qualified same-effect claim (${mode})`,
    () =>
      Effect.gen(function* () {
        const events = yield* Ref.make<ReadonlyArray<string>>([]);
        const now = yield* DateTime.now;
        const timestamp = DateTime.formatIso(now);
        const workerId = "namespace-retry-worker";
        const commandId = CommandId.make(`command:namespace-retry:${mode}`);
        const effect: EffectOutbox.OrchestrationEffectV2 = {
          ...restartEffect(now, { type: "detach" }),
          id: `effect:${commandId}:attachment.cleanup`,
          commandId,
          leaseOwner: workerId,
          attemptCount: 2,
          leaseExpiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 5 })),
          request: { type: "attachment.cleanup", attachmentIds: [] },
          attachmentNamespaceCleanup: {
            version: 1,
            mode: "delete_thread",
            ownerBirth: {
              kind: "application_v2_thread_birth",
              threadId,
              eventId: EventId.make("namespace-retry-birth"),
              sequence: 1,
            },
            triggerEventId: EventId.make("namespace-retry-delete"),
          },
        };
        const task: EventSink.AttachmentNamespaceCleanupTaskV1 = {
          version: 1,
          effectId: effect.id,
          commandId,
          threadId,
          reference: effect.attachmentNamespaceCleanup!,
          triggerSequence: 2,
          bindingSha256: "b".repeat(64),
        };
        const basis: EventSink.QualifiedAttachmentNamespaceCleanupBasisV1 = {
          status: "ready",
          task,
          claim: { workerId, expectedAttempt: 2, leaseExpiresAt: effect.leaseExpiresAt! },
          basisEventSequence: 2,
          retainedRelativePaths: [],
        };
        const unknown: EventSink.AttachmentNamespaceCleanupObservationV1 = {
          version: 1,
          producer: "attachment_namespace",
          effectId: effect.id,
          bindingSha256: task.bindingSha256,
          workerId,
          expectedAttempt: 2,
          basisEventSequence: 2,
          configuredRoot: "/synthetic-attachments",
          namespaceSegment: "synthetic-thread",
          observedAt: timestamp,
          outcome: { status: "unknown", removedPaths: [], reason: "synthetic partial scan" },
        };
        const priorBasis = {
          ...basis,
          claim: { ...basis.claim, workerId: "namespace-prior-worker", expectedAttempt: 1 },
        };
        const prior = { ...unknown, workerId: priorBasis.claim.workerId, expectedAttempt: 1 };
        const latest = yield* Ref.make<EventSink.AttachmentNamespaceCleanupRecordedObservationV1>({
          ordinal: 0,
          task,
          basis: mode === "same_attempt_replay" ? basis : priorBasis,
          observation: mode === "same_attempt_replay" ? unknown : prior,
          status: "unknown",
        });
        const current = yield* Ref.make<EffectOutbox.OrchestrationEffectV2>(
          mode === "same_attempt_replay"
            ? effect
            : {
                ...effect,
                status: "pending",
                attemptCount: 1,
                leaseOwner: null,
                leaseExpiresAt: null,
              },
        );
        const record = (value: string) => Ref.update(events, (values) => [...values, value]);
        const outboxLayer = Layer.mock(EffectOutbox.EffectOutboxV2)({
          claimNext: () => record("ordinary-empty").pipe(Effect.as(Option.none())),
          listAttachmentNamespaceCleanupRetryCandidates: (input) =>
            Effect.gen(function* () {
              assert.deepEqual(input, { limit: 16 });
              yield* record("candidate-facts");
              return [
                { effectId: effect.id, bindingSha256: task.bindingSha256, observationOrdinal: 0 },
              ];
            }),
          get: () => Ref.get(current).pipe(Effect.map(Option.some)),
          awaitCancellation: () => Effect.never,
          clearCancellation: () => Effect.void,
          listHeldByThreadId: () => Effect.succeed([]),
          succeed: () => record("generic-succeed").pipe(Effect.as(false)),
          retry: () => record("generic-retry").pipe(Effect.as(false)),
          fail: () => record("generic-fail").pipe(Effect.as(false)),
          holdResourceCleanupUnknown: () => record("generic-hold").pipe(Effect.as(false)),
        });
        const eventSinkLayer = Layer.mock(EventSink.EventSinkV2)({
          claimAttachmentNamespaceCleanup: (input) =>
            Effect.gen(function* () {
              assert.deepEqual(input, {
                effectId: effect.id,
                workerId,
                leaseDurationMs: 30_000,
                expectedBindingSha256: task.bindingSha256,
                expectedObservationOrdinal: 0,
              });
              yield* record("private-claim");
              if (mode !== "denied") yield* Ref.set(current, effect);
              return mode === "denied" ? null : effect;
            }),
          readAttachmentNamespaceCleanupTask: () => Effect.succeed(task),
          readAttachmentNamespaceCleanupObservation: () => Ref.get(latest),
          readAttachmentNamespaceCleanupBasis: () => Effect.succeed(basis),
          recordAttachmentNamespaceCleanupObservation: () =>
            record("duplicate-record").pipe(
              Effect.as({ status: "stale" as const, effectId: effect.id, ordinal: null }),
            ),
        });
        const executor = makeExecutorLayer({
          events,
          outboxLayer,
          eventSinkLayer,
          resourceCleanup: {
            cleanupAttachmentNamespace: (actual) =>
              Effect.gen(function* () {
                assert.deepEqual(actual, basis);
                yield* record("namespace-wrapper");
                yield* Ref.set(latest, {
                  ordinal: 1,
                  task,
                  basis,
                  observation: unknown,
                  status: "unknown",
                });
                return {
                  status: "unknown" as const,
                  basis,
                  observation: unknown,
                  record: { status: "unknown" as const, effectId: effect.id, ordinal: 1 },
                  anchorOrdinal: 1,
                };
              }),
          },
        });
        if (mode === "same_attempt_replay") {
          const result = yield* EffectWorker.OrchestrationEffectExecutorV2.pipe(
            Effect.flatMap((service) => service.execute(effect)),
            Effect.provide(executor),
          );
          assert.equal(
            result !== undefined && "status" in result ? result.status : undefined,
            "attachment_namespace_retained",
          );
          if (result !== undefined && "revalidate" in result) {
            const revalidated = yield* Effect.exit(result.revalidate);
            assert.isTrue(Exit.isSuccess(revalidated));
            if (Exit.isSuccess(revalidated)) assert.isTrue(revalidated.value);
          }
          assert.isEmpty(yield* Ref.get(events));
          return;
        }
        const layer = EffectWorker.layerWithOptions({ workerId }).pipe(
          Layer.provide(Layer.merge(executor, outboxLayer)),
        );
        const worked = yield* EffectWorker.OrchestrationEffectWorkerV2.pipe(
          Effect.flatMap((worker) => worker.runOnce),
          Effect.provide(layer),
        );
        assert.equal(worked, mode !== "denied");
        assert.deepEqual(
          yield* Ref.get(events),
          mode === "claimed"
            ? ["ordinary-empty", "candidate-facts", "private-claim", "namespace-wrapper"]
            : ["ordinary-empty", "candidate-facts", "private-claim"],
        );
        assert.equal((yield* Ref.get(current)).id, effect.id);
        assert.equal((yield* Ref.get(current)).attemptCount, mode === "denied" ? 1 : 2);
        assert.equal((yield* Ref.get(current)).status, mode === "denied" ? "pending" : "running");
      }),
  );
}

it.effect(
  "does not generically settle namespace cleanup without factual completion or retention",
  () =>
    Effect.gen(function* () {
      const events = yield* Ref.make<ReadonlyArray<string>>([]);
      const now = yield* DateTime.now;
      const effect: EffectOutbox.OrchestrationEffectV2 = {
        ...restartEffect(now, { type: "detach" }),
        request: { type: "attachment.cleanup", attachmentIds: [] },
        attachmentNamespaceCleanup: {
          version: 1,
          mode: "delete_thread",
          ownerBirth: {
            kind: "application_v2_thread_birth",
            threadId,
            eventId: EventId.make("namespace-unproven-birth"),
            sequence: 1,
          },
          triggerEventId: EventId.make("namespace-unproven-delete"),
        },
      };
      const record = (value: string) =>
        Ref.update(events, (values) => [...values, value]).pipe(Effect.as(false));
      const outbox = Layer.mock(EffectOutbox.EffectOutboxV2)({
        claimNext: () => Effect.succeed(Option.some(effect)),
        get: () => Effect.succeed(Option.some(effect)),
        awaitCancellation: () => Effect.never,
        clearCancellation: () => Effect.void,
        succeed: () => record("succeed"),
        retry: () => record("retry"),
        fail: () => record("fail"),
        holdUnknown: () => record("native-hold"),
        holdResourceCleanupUnknown: () => record("resource-hold"),
      });
      const executor = Layer.succeed(
        EffectWorker.OrchestrationEffectExecutorV2,
        EffectWorker.OrchestrationEffectExecutorV2.of({ execute: () => Effect.void }),
      );
      const exit = yield* EffectWorker.OrchestrationEffectWorkerV2.pipe(
        Effect.flatMap((worker) => worker.runOnce),
        Effect.exit,
        Effect.provide(
          EffectWorker.layerWithOptions({ workerId: "test-worker" }).pipe(
            Layer.provide(Layer.merge(outbox, executor)),
          ),
        ),
      );
      assert.isTrue(Exit.isFailure(exit));
      assert.isEmpty(yield* Ref.get(events));
    }),
);

function makeExecutorLayer(input: {
  readonly events: Ref.Ref<ReadonlyArray<string>>;
  readonly failFirstStart?: Ref.Ref<boolean>;
  readonly issueExecution?: NativeCreationAuthority.NativeCreationAuthority["Service"]["issueExecution"];
  readonly readImportedHistoryStartChoice?: EventSink.EventSinkV2["Service"]["readImportedHistoryStartChoice"];
  readonly readNativeCommandFacts?: EventSink.EventSinkV2["Service"]["readNativeCommandFacts"];
  readonly readClaimedQueuedRunStart?: EventSink.EventSinkV2["Service"]["readClaimedQueuedRunStart"];
  readonly readLeaseCleanupTask?: EventSink.EventSinkV2["Service"]["readLeaseCleanupTask"];
  readonly readDeletionCleanupTask?: EventSink.EventSinkV2["Service"]["readDeletionCleanupTask"];
  readonly readDeletionCleanupTaskOutcome?: EventSink.EventSinkV2["Service"]["readDeletionCleanupTaskOutcome"];
  readonly recordLeaseCleanupTaskOutcome?: EventSink.EventSinkV2["Service"]["recordLeaseCleanupTaskOutcome"];
  readonly resourceCleanup?: Partial<
    Context.Service.Shape<typeof ResourceCleanupService.ResourceCleanupService>
  >;
  readonly checkpointRollback?: CheckpointRollbackService.CheckpointRollbackServiceV2Shape["execute"];
  readonly interruptAndAwaitTerminal?: ProviderTurnControlService.ProviderTurnControlServiceV2Shape["interruptAndAwaitTerminal"];
  readonly ensureApplicationAttachmentInventory?: ThreadManagementService.ThreadManagementService["Service"]["ensureApplicationAttachmentInventory"];
  readonly readCurrentThreadRuntimeStopIntent?: EventSink.EventSinkV2["Service"]["readCurrentThreadRuntimeStopIntent"];
  readonly readCurrentProviderRuntimeOwner?: EventSink.EventSinkV2["Service"]["readCurrentProviderRuntimeOwner"];
  readonly stopPinnedRuntime?: ProviderSessionManager.ProviderSessionManagerV2Shape["stopPinnedRuntime"];
  readonly outboxLayer?: Layer.Layer<EffectOutbox.EffectOutboxV2>;
  readonly eventSinkLayer?: Layer.Layer<EventSink.EventSinkV2>;
}) {
  const record = (event: string) => Ref.update(input.events, (events) => [...events, event]);
  const dependencies = Layer.mergeAll(
    Layer.succeed(
      ProviderTurnControlService.ProviderTurnControlServiceV2,
      ProviderTurnControlService.ProviderTurnControlServiceV2.of({
        interrupt: () => Effect.void,
        steer: () => Effect.void,
        interruptAndAwaitTerminal:
          input.interruptAndAwaitTerminal ??
          ((request) =>
            record(
              request.replacementProviderSessionId === undefined
                ? "interrupt"
                : `interrupt:${request.replacementProviderSessionId}`,
            )),
      }),
    ),
    Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
      shutdown: Effect.void,
      open: () => Effect.die("unused open"),
      get: () => Effect.succeed(Option.none()),
      observeThreadRuntime: () => Effect.succeed({ status: "unknown", reason: "unused" }),
      close: () => Effect.void,
      closeInstance: () => Effect.void,
      release: () => record("release"),
      detach: () => record("detach"),
      stopPinnedRuntime:
        input.stopPinnedRuntime ??
        (() => Effect.succeed({ status: "unknown", reason: "managed_stop_unavailable" })),
    }),
    Layer.succeed(
      ProviderTurnStartService.ProviderTurnStartServiceV2,
      ProviderTurnStartService.ProviderTurnStartServiceV2.of({
        prepareImportedHistoryStart: () => Effect.die("unused imported choice preparation"),
        start: () =>
          Effect.gen(function* () {
            yield* record("start");
            if (
              input.failFirstStart !== undefined &&
              (yield* Ref.getAndSet(input.failFirstStart, false))
            ) {
              return yield* new ProviderTurnStartService.ProviderTurnStartError({
                runId,
                cause: "simulated first start failure",
              });
            }
          }),
      }),
    ),
    Layer.succeed(
      RunFinalizationService.RunFinalizationService,
      RunFinalizationService.RunFinalizationService.of({
        finalize: () => Effect.succeed({ version: 1, kind: "skipped", reason: "settled" }),
      }),
    ),
    Layer.succeed(
      CheckpointRollbackService.CheckpointRollbackServiceV2,
      CheckpointRollbackService.CheckpointRollbackServiceV2.of({
        execute: input.checkpointRollback ?? (() => Effect.void),
      }),
    ),
    Layer.succeed(
      RuntimeRequestService.RuntimeRequestServiceV2,
      RuntimeRequestService.RuntimeRequestServiceV2.of({ respond: () => Effect.void }),
    ),
    Layer.succeed(
      ThreadTitleRegenerationService.ThreadTitleRegenerationService,
      ThreadTitleRegenerationService.ThreadTitleRegenerationService.of({
        execute: () => Effect.void,
      }),
    ),
  );
  return EffectWorker.executorLayer.pipe(
    Layer.provide(
      Layer.mergeAll(
        dependencies,
        Layer.mock(ResourceCleanupService.ResourceCleanupService)({
          cleanupTerminals: () => record("terminal-legacy"),
          cleanupAttachments: () => record("attachment-legacy"),
          ...input.resourceCleanup,
        }),
        Layer.mock(NativeCreationAuthority.NativeCreationAuthority)({
          issueExecution:
            input.issueExecution ?? (() => Effect.die("unused native execution issuance")),
        }),
        Layer.mock(NativeCreationRepository)({}),
        input.eventSinkLayer ??
          Layer.mock(EventSink.EventSinkV2)({
            readOrdinaryCheckoutEffectLink: () => Effect.succeed(null),
            readNativeCommandFacts:
              input.readNativeCommandFacts ??
              (() =>
                Effect.succeed({
                  events: [],
                  eventMetadataOverflow: false,
                  commitSnapshot: { records: { start_reservations: [] } },
                } as never)),
            readClaimedQueuedRunStart:
              input.readClaimedQueuedRunStart ??
              (() => Effect.die("unused claimed queued start proof")),
            readImportedHistoryStartChoice:
              input.readImportedHistoryStartChoice ?? (() => Effect.succeed(null)),
            readLeaseCleanupTask: input.readLeaseCleanupTask ?? (() => Effect.succeed(null)),
            readDeletionCleanupTask:
              input.readDeletionCleanupTask ??
              input.readLeaseCleanupTask ??
              (() => Effect.succeed(null)),
            readDeletionCleanupTaskOutcome:
              input.readDeletionCleanupTaskOutcome ?? (() => Effect.succeed(null)),
            ...(input.recordLeaseCleanupTaskOutcome === undefined
              ? {}
              : { recordLeaseCleanupTaskOutcome: input.recordLeaseCleanupTaskOutcome }),
            readCurrentThreadRuntimeStopIntent:
              input.readCurrentThreadRuntimeStopIntent ?? (() => Effect.succeed(null)),
            ...(input.readCurrentProviderRuntimeOwner === undefined
              ? {}
              : { readCurrentProviderRuntimeOwner: input.readCurrentProviderRuntimeOwner }),
          }),
        input.outboxLayer ?? Layer.mock(EffectOutbox.EffectOutboxV2)({}),
        Layer.mock(ThreadManagementService.ThreadManagementService)({
          ...(input.ensureApplicationAttachmentInventory === undefined
            ? {}
            : {
                ensureApplicationAttachmentInventory: input.ensureApplicationAttachmentInventory,
              }),
        }),
        ServerSettings.layerTest(),
      ),
    ),
  );
}

it.effect("holds unknown native outcomes on retryable and final attempts", () =>
  Effect.gen(function* () {
    for (const attemptCount of [1, 5]) {
      const timestamp = DateTime.formatIso(yield* DateTime.now);
      const claimed: EffectOutbox.OrchestrationEffectV2 = {
        id: `effect:unknown:${attemptCount}`,
        commandId: CommandId.make(`command:unknown:${attemptCount}`),
        threadId,
        request: { type: "provider-turn.start", runId },
        status: "running",
        attemptCount,
        availableAt: timestamp,
        createdAt: timestamp,
        updatedAt: timestamp,
        leaseOwner: "unknown-worker",
        leaseExpiresAt: timestamp,
        completedAt: null,
        lastError: null,
      };
      const evidence = {
        operationId: `operation:unknown:${attemptCount}`,
        operation: "start_turn" as const,
        threadId,
        providerSessionId: oldSessionId,
        providerThreadId,
        attemptId,
        runtimeGeneration: "current-generation",
        outcome: "unknown" as const,
      };
      const holds = yield* Ref.make<
        ReadonlyArray<Parameters<EffectOutbox.EffectOutboxV2["Service"]["holdUnknown"]>[0]>
      >([]);
      const settlements = yield* Ref.make(0);
      const settle = () => Ref.update(settlements, (count) => count + 1).pipe(Effect.as(true));
      const layer = EffectWorker.layerWithOptions({
        workerId: "unknown-worker",
        maxAttempts: 5,
      }).pipe(
        Layer.provide(
          Layer.mergeAll(
            Layer.mock(EffectOutbox.EffectOutboxV2)({
              claimNext: () => Effect.succeed(Option.some(claimed)),
              get: () => Effect.succeed(Option.some(claimed)),
              awaitCancellation: () => Effect.never,
              clearCancellation: () => Effect.void,
              holdUnknown: (input) =>
                Ref.update(holds, (values) => [...values, input]).pipe(Effect.as(true)),
              succeed: settle,
              fail: settle,
              retry: settle,
            }),
            Layer.mock(EffectWorker.OrchestrationEffectExecutorV2)({
              execute: () =>
                Effect.fail(
                  new EffectWorker.OrchestrationEffectExecutionError({
                    effectId: claimed.id,
                    effectType: claimed.request.type,
                    cause: new ProviderNativeOperationUnknownError({ nativeEffect: evidence }),
                  }),
                ),
            }),
          ),
        ),
      );
      yield* EffectWorker.OrchestrationEffectWorkerV2.pipe(
        Effect.flatMap((worker) => worker.runOnce),
        Effect.provide(layer),
      );
      assert.equal(yield* Ref.get(settlements), 0);
      assert.deepEqual(yield* Ref.get(holds), [
        {
          effectId: claimed.id,
          workerId: "unknown-worker",
          expectedAttempt: attemptCount,
          operationId: evidence.operationId,
          evidence,
        },
      ]);
    }
  }),
);

it.effect("does not settle a native start without its durable ACK confirmation", () =>
  Effect.gen(function* () {
    const now = yield* DateTime.now;
    const workerId = "native-confirmation-worker";
    const effectId = `effect:command:native-confirmation:provider-turn.start:${runId}`;
    const claimed: EffectOutbox.OrchestrationEffectV2 = {
      ...restartEffect(now, { type: "detach" }),
      id: effectId,
      commandId: CommandId.make("command:native-confirmation"),
      request: { type: "provider-turn.start", runId },
      status: "running",
      attemptCount: 1,
      leaseOwner: workerId,
      nativeCreationExecutionReference: {
        version: 2,
        claimId: "claim:native-confirmation",
        stageCommandId: CommandId.make("command:native-confirmation"),
        effectId,
        stage: "native_command",
      },
    };
    const settlements = yield* Ref.make(0);
    const holds = yield* Ref.make<
      ReadonlyArray<Parameters<EffectOutbox.EffectOutboxV2["Service"]["holdUnknown"]>[0]>
    >([]);
    const settle = () => Ref.update(settlements, (value) => value + 1).pipe(Effect.as(true));
    const workerLayer = EffectWorker.layerWithOptions({ workerId }).pipe(
      Layer.provide(
        Layer.mergeAll(
          Layer.mock(EffectOutbox.EffectOutboxV2)({
            claimNext: () => Effect.succeed(Option.some(claimed)),
            get: () => Effect.succeed(Option.some(claimed)),
            awaitCancellation: () => Effect.never,
            clearCancellation: () => Effect.void,
            holdUnknown: (input) =>
              Ref.update(holds, (values) => [...values, input]).pipe(Effect.as(true)),
            succeed: settle,
            retry: settle,
            fail: settle,
          }),
          Layer.mock(EffectWorker.OrchestrationEffectExecutorV2)({ execute: () => Effect.void }),
        ),
      ),
    );
    const exit = yield* EffectWorker.OrchestrationEffectWorkerV2.pipe(
      Effect.flatMap((worker) => worker.runOnce),
      Effect.provide(workerLayer),
      Effect.exit,
    );
    assert.isTrue(Exit.isSuccess(exit));
    assert.equal(yield* Ref.get(settlements), 0);
    assert.deepEqual(yield* Ref.get(holds), [
      {
        effectId,
        workerId,
        expectedAttempt: claimed.attemptCount,
        operationId: effectId,
        evidence: { operationId: effectId, operation: "start_turn", threadId, outcome: "unknown" },
      },
    ]);
  }),
);

it.effect("holds unresolved fresh native execution issuance without starting the provider", () =>
  Effect.gen(function* () {
    const events = yield* Ref.make<ReadonlyArray<string>>([]);
    const now = yield* DateTime.now;
    const effectId = `effect:command:native-release:provider-turn.start:${runId}`;
    const effect: EffectOutbox.OrchestrationEffectV2 = {
      ...restartEffect(now, { type: "detach" }),
      id: effectId,
      commandId: CommandId.make("command:native-release"),
      request: { type: "provider-turn.start", runId },
      nativeCreationExecutionReference: {
        version: 2,
        claimId: "claim:native-release",
        stageCommandId: CommandId.make("command:native-release"),
        effectId,
        stage: "native_command",
      },
    };
    const exit = yield* EffectWorker.OrchestrationEffectExecutorV2.pipe(
      Effect.flatMap((executor) => executor.execute(effect, { willRetry: true })),
      Effect.exit,
      Effect.provide(
        makeExecutorLayer({
          events,
          issueExecution: () =>
            Ref.update(events, (values) => [...values, "issue"]).pipe(
              Effect.andThen(
                Effect.fail(
                  new NativeCreationAuthority.NativeCreationAuthorityError({
                    code: "unresolved_claim",
                    message: "The fresh stage start could not be confirmed.",
                  }),
                ),
              ),
            ),
        }),
      ),
    );
    assert.isTrue(Exit.isFailure(exit));
    assert.deepEqual(yield* Ref.get(events), ["issue"]);
    if (Exit.isFailure(exit)) {
      assert.deepEqual(nativeEffectEvidenceFromCause(exit.cause), {
        operationId: effectId,
        operation: "start_turn",
        threadId,
        outcome: "unknown",
      });
    }
  }),
);

it.effect(
  "holds an accepted imported-history choice before setup when its preparation facade is absent",
  () =>
    Effect.gen(function* () {
      const events = yield* Ref.make<ReadonlyArray<string>>([]);
      const now = yield* DateTime.now;
      const commandId = CommandId.make("command:imported-choice");
      const effect: EffectOutbox.OrchestrationEffectV2 = {
        ...restartEffect(now, { type: "detach" }),
        id: `effect:${commandId}:provider-turn.start:${runId}`,
        commandId,
        request: { type: "provider-turn.start", runId },
      };
      const exit = yield* EffectWorker.OrchestrationEffectExecutorV2.pipe(
        Effect.flatMap((executor) => executor.execute(effect)),
        Effect.exit,
        Effect.provide(
          makeExecutorLayer({
            events,
            readImportedHistoryStartChoice: (input) => {
              assert.deepEqual(input, { threadId, commandId });
              return Effect.succeed({
                commandId,
                threadId,
                runId,
                effectId: effect.id,
                receipt: { status: "accepted" },
              } as EventSink.ImportedHistoryStartOutcomeV2);
            },
          }),
        ),
      );
      assert.isTrue(Exit.isFailure(exit));
      assert.deepEqual(yield* Ref.get(events), []);
      if (Exit.isFailure(exit))
        assert.deepEqual(nativeEffectEvidenceFromCause(exit.cause), {
          operationId: effect.id,
          operation: "start_turn",
          threadId,
          outcome: "unknown",
        });
    }),
);

it.effect(
  "parks a non-head imported-history delivery without starting or settling native work",
  () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const workerId = "imported-history-head-worker";
      const commandId = CommandId.make("command:imported-history-head-wait");
      const claimed: EffectOutbox.OrchestrationEffectV2 = {
        ...restartEffect(now, { type: "detach" }),
        id: `effect:${commandId}:provider-turn.start:${runId}`,
        commandId,
        request: { type: "provider-turn.start", runId },
        status: "running",
        attemptCount: 1,
        leaseOwner: workerId,
        leaseExpiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 1 })),
      };
      const current = yield* Ref.make(claimed);
      const events = yield* Ref.make<ReadonlyArray<string>>([]);
      const parks = yield* Ref.make(0);
      const holds = yield* Ref.make(0);
      const settlements = yield* Ref.make(0);
      const settle = () => Ref.update(settlements, (count) => count + 1).pipe(Effect.as(true));
      const commandDigest = "a".repeat(64);
      const outboxLayer = Layer.mock(EffectOutbox.EffectOutboxV2)({
        claimNext: () => Effect.succeed(Option.some(claimed)),
        get: () => Ref.get(current).pipe(Effect.map(Option.some)),
        awaitCancellation: () => Effect.never,
        clearCancellation: () => Effect.void,
        holdUnknown: () => Ref.update(holds, (count) => count + 1).pipe(Effect.as(true)),
        succeed: settle,
        retry: settle,
        fail: settle,
        parkImportedHistoryDelivery: (input) =>
          Effect.gen(function* () {
            assert.deepEqual(input, {
              effectId: claimed.id,
              commandId,
              threadId,
              runId,
              runAttemptId: attemptId,
              workerId,
              expectedAttempt: 1,
            });
            yield* Ref.update(parks, (count) => count + 1);
            yield* Ref.set(current, {
              ...claimed,
              status: "pending",
              attemptCount: 0,
              leaseOwner: null,
              leaseExpiresAt: null,
              lastError: "imported-history.waiting-for-head/v1",
            });
            return {
              status: "parked" as const,
              effectId: claimed.id,
              commandId,
              threadId,
              runId,
              runAttemptId: attemptId,
              commandDigest,
              schedulingAttempt: 0,
            };
          }),
      });
      const preparation = Layer.succeed(EffectWorker.ImportedHistoryStartExecutionPreparation, {
        prepare: () =>
          Effect.succeed({ status: "rejected" as const, reason: "queued_delivery_not_first" }),
      });
      const executor = makeExecutorLayer({
        events,
        outboxLayer,
        readImportedHistoryStartChoice: () =>
          Effect.succeed({
            commandId,
            threadId,
            runId,
            effectId: claimed.id,
            commandDigest,
            actorSessionId: AuthSessionId.make("session:imported-history-head-wait"),
            messageId: MessageId.make("message:imported-history-head-wait"),
            rejectionReason: null,
            receipt: {
              commandId,
              threadId,
              commandType: "thread.imported-history.start",
              acceptedAt: now,
              resultSequence: 1,
              status: "accepted",
              error: null,
            },
            command: {
              type: "thread.imported-history.start",
              commandId,
              threadId,
              reviewedBasis: OrchestrationV2ImportedHistoryReviewBasis.make("review:head-wait"),
              delivery: {
                type: "queued_run",
                runId,
                messageId: MessageId.make("message:imported-history-head-wait"),
              },
            },
            basis: {
              snapshot: { records: { run_attempts: [{ run_id: runId, attempt_id: attemptId }] } },
            },
          } satisfies EventSink.ImportedHistoryStartOutcomeV2),
      }).pipe(Layer.provide(preparation));
      yield* EffectWorker.OrchestrationEffectWorkerV2.pipe(
        Effect.flatMap((worker) => worker.runOnce),
        Effect.provide(
          EffectWorker.layerWithOptions({ workerId }).pipe(
            Layer.provide(Layer.merge(executor, outboxLayer)),
          ),
        ),
      );
      assert.equal(yield* Ref.get(parks), 1);
      assert.deepEqual(yield* Ref.get(events), []);
      assert.equal(yield* Ref.get(holds), 0);
      assert.equal(yield* Ref.get(settlements), 0);
      const parked = yield* Ref.get(current);
      assert.equal(parked.status, "pending");
      assert.equal(parked.attemptCount, 0);
      assert.isNull(parked.leaseOwner);
      assert.isNull(parked.leaseExpiresAt);
      assert.deepEqual(parked.request, claimed.request);
    }),
);

it.effect(
  "holds an ordinary reserved queued start when its current claimed source proof is missing",
  () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const commandId = CommandId.make(`command:system:start-queued:${runId}`);
      const effect: EffectOutbox.OrchestrationEffectV2 = {
        ...restartEffect(now, { type: "detach" }),
        id: `effect:${commandId}:provider-turn.start:${runId}`,
        commandId,
        request: { type: "provider-turn.start", runId },
        leaseExpiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 1 })),
      };
      const events = yield* Ref.make<ReadonlyArray<string>>([]);
      const reads = yield* Ref.make(0);
      const exit = yield* EffectWorker.OrchestrationEffectExecutorV2.pipe(
        Effect.flatMap((executor) => executor.execute(effect)),
        Effect.exit,
        Effect.provide(
          makeExecutorLayer({
            events,
            readNativeCommandFacts: (input) => {
              assert.deepEqual(input, { threadId, commandId });
              return Effect.succeed({
                events: [],
                eventMetadataOverflow: false,
                commitSnapshot: {
                  records: {
                    start_reservations: [
                      {
                        effect_id: effect.id,
                        command_id: commandId,
                        thread_id: threadId,
                        run_id: runId,
                      },
                    ],
                  },
                },
              } as never);
            },
            readClaimedQueuedRunStart: (input) => {
              assert.deepEqual(input, {
                effectId: effect.id,
                threadId,
                runId,
                workerId: effect.leaseOwner,
                expectedAttempt: effect.attemptCount,
              });
              return Ref.update(reads, (count) => count + 1).pipe(Effect.as(null));
            },
          }),
        ),
      );
      assert.deepEqual(yield* Ref.get(events), []);
      assert.equal(yield* Ref.get(reads), 1);
      assert.isTrue(Exit.isFailure(exit));
      if (Exit.isFailure(exit))
        assert.deepEqual(nativeEffectEvidenceFromCause(exit.cause), {
          operationId: effect.id,
          operation: "start_turn",
          threadId,
          outcome: "unknown",
        });
    }),
);

it.effect("holds an ordinary start when its reservation facts cannot be read", () =>
  Effect.gen(function* () {
    const now = yield* DateTime.now;
    const commandId = CommandId.make("command:unavailable-start-reservations");
    const effect: EffectOutbox.OrchestrationEffectV2 = {
      ...restartEffect(now, { type: "detach" }),
      commandId,
      id: `effect:${commandId}:provider-turn.start:${runId}`,
      request: { type: "provider-turn.start", runId },
      leaseExpiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 1 })),
    };
    const events = yield* Ref.make<ReadonlyArray<string>>([]);
    const exit = yield* EffectWorker.OrchestrationEffectExecutorV2.pipe(
      Effect.flatMap((executor) => executor.execute(effect)),
      Effect.exit,
      Effect.provide(
        makeExecutorLayer({
          events,
          readNativeCommandFacts: () =>
            Effect.fail(
              new EventSink.EventSinkWriteError({
                eventCount: 0,
                cause: "reservation read unavailable",
              }),
            ),
        }),
      ),
    );
    assert.deepEqual(yield* Ref.get(events), []);
    assert.isTrue(Exit.isFailure(exit));
    if (Exit.isFailure(exit))
      assert.deepEqual(nativeEffectEvidenceFromCause(exit.cause), {
        operationId: effect.id,
        operation: "start_turn",
        threadId,
        outcome: "unknown",
      });
  }),
);

for (const matchingEvidence of [true, false]) {
  it.effect(
    `${matchingEvidence ? "recognizes" : "rejects"} an existing provider cleanup hold with ${matchingEvidence ? "matching" : "different"} pinned evidence`,
    () =>
      Effect.gen(function* () {
        const now = yield* DateTime.now;
        const timestamp = DateTime.formatIso(now);
        const workerId = "cleanup-held-worker";
        const commandId = CommandId.make("command:cleanup-already-held");
        const effect: EffectOutbox.OrchestrationEffectV2 = {
          ...restartEffect(now, { type: "detach" }),
          commandId,
          leaseOwner: workerId,
          id: `effect:${commandId}:provider-session.detach:${oldSessionId}`,
          request: { type: "provider-session.detach", providerSessionId: oldSessionId },
          leaseExpiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 1 })),
        };
        const evidence = {
          operationId: effect.id,
          operation: "close_session" as const,
          threadId,
          providerSessionId: oldSessionId,
          providerThreadId,
          instanceId: ProviderInstanceId.make("codex"),
          runtimeGeneration: "cleanup-held-generation",
          outcome: "unknown" as const,
        };
        const settlements = yield* Ref.make(0);
        const settle = () => Ref.update(settlements, (count) => count + 1).pipe(Effect.as(true));
        const exit = yield* EffectWorker.OrchestrationEffectWorkerV2.pipe(
          Effect.flatMap((worker) => worker.runOnce),
          Effect.exit,
          Effect.provide(
            EffectWorker.layerWithOptions({ workerId }).pipe(
              Layer.provide(
                Layer.mergeAll(
                  Layer.mock(EffectWorker.OrchestrationEffectExecutorV2)({
                    execute: () =>
                      Effect.fail(
                        new EffectWorker.OrchestrationEffectExecutionError({
                          effectId: effect.id,
                          effectType: effect.request.type,
                          cause: new ProviderNativeOperationUnknownError({
                            nativeEffect: evidence,
                          }),
                        }),
                      ),
                  }),
                  Layer.mock(EffectOutbox.EffectOutboxV2)({
                    claimNext: () => Effect.succeed(Option.some(effect)),
                    get: () => Effect.succeed(Option.some(effect)),
                    awaitCancellation: () => Effect.never,
                    clearCancellation: () => Effect.void,
                    holdUnknown: () => Effect.succeed(false),
                    listHeldByThreadId: (heldThreadId) => {
                      assert.equal(heldThreadId, threadId);
                      return Effect.succeed([
                        {
                          effectId: effect.id,
                          threadId,
                          workerId,
                          operationId: evidence.operationId,
                          expectedAttempt: effect.attemptCount,
                          heldAt: timestamp,
                          evidence: matchingEvidence
                            ? evidence
                            : { ...evidence, runtimeGeneration: "another-incarnation" },
                        },
                      ]);
                    },
                    succeed: settle,
                    retry: settle,
                    fail: settle,
                  }),
                ),
              ),
            ),
          ),
        );
        assert.equal(yield* Ref.get(settlements), 0);
        assert.equal(Exit.isSuccess(exit), matchingEvidence);
        if (Exit.isSuccess(exit)) assert.isTrue(exit.value);
      }),
  );
}

it.effect("holds an accepted targeted stop when its pinned managed cleanup remains unknown", () =>
  Effect.gen(function* () {
    const now = yield* DateTime.now;
    const commandId = CommandId.make("command:targeted-stop-no-drain");
    const workerId = "targeted-stop-worker";
    const claimed: EffectOutbox.OrchestrationEffectV2 = {
      ...restartEffect(now, { type: "detach" }),
      id: `effect:${commandId}:provider-session.detach:${oldSessionId}`,
      commandId,
      request: { type: "provider-session.detach", providerSessionId: oldSessionId },
      leaseOwner: workerId,
      leaseExpiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 1 })),
    };
    const binding = {
      threadId,
      providerThreadId,
      providerSessionId: oldSessionId,
      instanceId: ProviderInstanceId.make("codex"),
      driver: ProviderDriverKind.make("codex"),
      nativeThreadId: "targeted-stop-native",
      runtimeGeneration: "targeted-stop-generation",
    };
    const events = yield* Ref.make<ReadonlyArray<string>>([]);
    const holds = yield* Ref.make<
      ReadonlyArray<Parameters<EffectOutbox.EffectOutboxV2["Service"]["holdUnknown"]>[0]>
    >([]);
    const settlements = yield* Ref.make(0);
    const settle = () => Ref.update(settlements, (value) => value + 1).pipe(Effect.as(true));
    const outboxLayer = Layer.mock(EffectOutbox.EffectOutboxV2)({
      claimNext: () => Effect.succeed(Option.some(claimed)),
      get: () => Effect.succeed(Option.some(claimed)),
      awaitCancellation: () => Effect.never,
      clearCancellation: () => Effect.void,
      holdUnknown: (input) =>
        Ref.update(holds, (values) => [...values, input]).pipe(Effect.as(true)),
      succeed: settle,
      retry: settle,
      fail: settle,
    });
    const executor = makeExecutorLayer({
      events,
      outboxLayer,
      readCurrentProviderRuntimeOwner: () =>
        Effect.succeed({
          binding,
          evidenceRevision: 2,
          observation: null,
          registeredAt: DateTime.formatIso(now),
        }),
      readCurrentThreadRuntimeStopIntent: (input) => {
        assert.deepEqual(input, { threadId, commandId });
        return Effect.succeed({
          commandId,
          threadId,
          incarnation: {
            kind: "application_v2_thread_birth",
            threadId,
            eventId: EventId.make("targeted-stop-birth"),
            sequence: 1,
          },
          canonicalRequestDigest: "targeted-stop-request",
          actorBindingDigest: "targeted-stop-actor",
          targetBinding: binding,
          targetEvidenceRevision: 2,
          stopEventId: EventId.make("targeted-stop-request-event"),
          stopEventSequence: 2,
          affectedRunIds: [],
          queuedBases: [],
        });
      },
    });
    yield* EffectWorker.OrchestrationEffectWorkerV2.pipe(
      Effect.flatMap((worker) => worker.runOnce),
      Effect.provide(
        EffectWorker.layerWithOptions({ workerId }).pipe(
          Layer.provide(Layer.mergeAll(executor, outboxLayer)),
        ),
      ),
    );
    assert.deepEqual(yield* Ref.get(events), []);
    assert.equal(yield* Ref.get(settlements), 0);
    const retained = yield* Ref.get(holds);
    assert.lengthOf(retained, 1);
    assert.equal(retained[0]!.effectId, claimed.id);
    assert.equal(retained[0]!.expectedAttempt, claimed.attemptCount);
    assert.equal(retained[0]!.evidence.outcome, "unknown");
    assert.equal(retained[0]!.evidence.runtimeGeneration, binding.runtimeGeneration);
  }),
);

it.effect(
  "completes targeted stop only after exact pinned managed cleanup and its owned claim readback",
  () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const commandId = CommandId.make("command:targeted-stop-managed");
      const workerId = "targeted-stop-managed-worker";
      const claimed: EffectOutbox.OrchestrationEffectV2 = {
        ...restartEffect(now, { type: "detach" }),
        id: `effect:${commandId}:provider-session.detach:${oldSessionId}`,
        commandId,
        request: { type: "provider-session.detach", providerSessionId: oldSessionId },
        leaseOwner: workerId,
        leaseExpiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 1 })),
      };
      const binding = {
        threadId,
        providerThreadId,
        providerSessionId: oldSessionId,
        instanceId: ProviderInstanceId.make("codex"),
        driver: ProviderDriverKind.make("codex"),
        nativeThreadId: "managed-stop-native",
        runtimeGeneration: "managed-stop-generation",
      };
      const events = yield* Ref.make<ReadonlyArray<string>>([]);
      const settlements = yield* Ref.make(0);
      const holds = yield* Ref.make(0);
      const outboxLayer = Layer.mock(EffectOutbox.EffectOutboxV2)({
        claimNext: () => Effect.succeed(Option.some(claimed)),
        get: () => Effect.succeed(Option.some(claimed)),
        awaitCancellation: () => Effect.never,
        clearCancellation: () => Effect.void,
        holdUnknown: () => Ref.update(holds, (count) => count + 1).pipe(Effect.as(true)),
        succeed: () => Ref.update(settlements, (count) => count + 1).pipe(Effect.as(true)),
        retry: () => Effect.die("Managed stop must not retry"),
        fail: () => Effect.die("Managed stop must not fail"),
      });
      const executor = makeExecutorLayer({
        events,
        outboxLayer,
        readCurrentProviderRuntimeOwner: () =>
          Effect.succeed({
            binding,
            evidenceRevision: 2,
            observation: null,
            registeredAt: DateTime.formatIso(now),
          }),
        readCurrentThreadRuntimeStopIntent: () =>
          Effect.succeed({
            commandId,
            threadId,
            incarnation: {
              kind: "application_v2_thread_birth",
              threadId,
              eventId: EventId.make("managed-stop-birth"),
              sequence: 1,
            },
            canonicalRequestDigest: "managed-stop-request",
            actorBindingDigest: "managed-stop-actor",
            targetBinding: binding,
            targetEvidenceRevision: 2,
            stopEventId: EventId.make("managed-stop-request-event"),
            stopEventSequence: 2,
            affectedRunIds: [],
            queuedBases: [],
          }),
        stopPinnedRuntime: (input) => {
          assert.deepEqual(input, {
            operationId: claimed.id,
            binding,
            expectedEvidenceRevision: 2,
          });
          return Ref.update(events, (values) => [...values, "pinned-managed-stop"]).pipe(
            Effect.as({
              status: "stopped" as const,
              operationId: claimed.id,
              binding,
              cancelledPendingStart: false,
              interruptedProviderTurnIds: [],
              readback: { threadAttached: false as const },
            }),
          );
        },
      });
      yield* EffectWorker.OrchestrationEffectWorkerV2.pipe(
        Effect.flatMap((worker) => worker.runOnce),
        Effect.provide(
          EffectWorker.layerWithOptions({ workerId }).pipe(
            Layer.provide(Layer.mergeAll(executor, outboxLayer)),
          ),
        ),
      );
      assert.deepEqual(yield* Ref.get(events), ["pinned-managed-stop"]);
      assert.equal(yield* Ref.get(settlements), 1);
      assert.equal(yield* Ref.get(holds), 0);
    }),
);

it.effect(
  "holds a pinned deleted-owner provider cleanup after an unconfirmed managed stop without ordinary detach",
  () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const timestamp = DateTime.formatIso(now);
      const commandId = CommandId.make("command:delete-pinned-provider-owner");
      const effect: EffectOutbox.OrchestrationEffectV2 = {
        ...restartEffect(now, { type: "detach" }),
        id: `effect:${commandId}:provider-session.detach:${oldSessionId}`,
        commandId,
        request: { type: "provider-session.detach", providerSessionId: oldSessionId },
        leaseExpiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 1 })),
      };
      const ownerBirth = {
        kind: "application_v2_thread_birth" as const,
        threadId,
        eventId: EventId.make("event:deleted-provider-owner-birth"),
        sequence: 1,
      };
      const binding = {
        threadId,
        providerThreadId,
        providerSessionId: oldSessionId,
        instanceId: ProviderInstanceId.make("codex"),
        driver: ProviderDriverKind.make("codex"),
        nativeThreadId: "deleted-owner-native",
        runtimeGeneration: "deleted-owner-generation",
      };
      const subject = {
        version: 2 as const,
        effectId: effect.id,
        threadId,
        ownerBirth,
        lease: {
          resourcePath: "/workspace/deleted-provider-owner",
          leaseId: "deleted-provider-lease",
          ownerThreadId: threadId,
          ownerIncarnation: yield* Schema.encodeEffect(
            Schema.fromJsonString(Schema.Tuple([Schema.String, EventId, Schema.Number])),
          )(["t3.orchestration-v2.thread-birth/v1", ownerBirth.eventId, ownerBirth.sequence]),
          branch: "deleted-provider-branch",
          acquiredAtMs: 1,
          renewedAtMs: 2,
          expiresAtMs: 3,
        },
        deletion: { commandId, eventId: EventId.make("event:deleted-provider-owner"), sequence: 2 },
        task: { kind: "provider" as const, expectedBinding: binding, evidenceRevision: 4 },
      };
      const task = {
        ...subject,
        bindingSha256: EventSink.leaseCleanupTaskBindingDigestV2(subject),
        recordedAt: timestamp,
      };
      const events = yield* Ref.make<ReadonlyArray<string>>([]);
      const outcomes = yield* Ref.make<
        ReadonlyArray<
          Parameters<EventSink.EventSinkV2["Service"]["recordLeaseCleanupTaskOutcome"]>[0]
        >
      >([]);
      const holds = yield* Ref.make<ReadonlyArray<EffectOutbox.UnknownEffectHoldV2>>([]);
      const latest = yield* Ref.make<EventSink.DeletionCleanupTaskOutcomeRowV1 | null>(null);
      const outboxLayer = Layer.mock(EffectOutbox.EffectOutboxV2)({
        get: () => Effect.succeed(Option.some(effect)),
        listHeldByThreadId: () => Ref.get(holds),
        holdUnknown: (input) =>
          Ref.set(holds, [{ ...input, threadId, heldAt: timestamp }]).pipe(Effect.as(true)),
      });
      const eventSinkLayer = Layer.mock(EventSink.EventSinkV2)({
        readDeletionCleanupTask: () => Effect.succeed(task),
        readDeletionCleanupTaskOutcome: () => Ref.get(latest),
        recordLeaseCleanupTaskOutcome: (input) =>
          Ref.update(outcomes, (values) => [...values, input]).pipe(
            Effect.andThen(
              Ref.set(latest, {
                ordinal: 0,
                outcome: input.outcome,
                correlation: {
                  workerId: input.workerId,
                  expectedAttempt: input.expectedAttempt,
                  bindingSha256: task.bindingSha256,
                  evidence: input.evidence,
                },
                recordedAt: timestamp,
              }),
            ),
            Effect.as(input.outcome),
          ),
        recordObservedDeletionCleanupOutcome: (input) => {
          assert.isTrue(
            "kind" in input.observation && input.observation.kind === "managed_provider",
          );
          if ("kind" in input.observation && input.observation.kind === "managed_provider")
            assert.deepEqual(input.observation.result, {
              status: "unknown",
              reason: "managed_stop_unavailable",
            });
          return Effect.succeed({
            ordinal: 1,
            bindingSha256: task.bindingSha256,
            outcome: { taskId: effect.id, result: null, effect: "unknown" },
            evidence: {
              version: 1,
              schema: "t3.deletion-cleanup-observation/v1",
              producer: "managed_provider",
              observation: input.observation,
              coveredHolds: input.coveredHolds,
            },
          });
        },
      });
      const exit = yield* EffectWorker.OrchestrationEffectExecutorV2.pipe(
        Effect.flatMap((executor) => executor.execute(effect)),
        Effect.exit,
        Effect.provide(makeExecutorLayer({ events, outboxLayer, eventSinkLayer })),
      );
      assert.deepEqual(yield* Ref.get(events), []);
      assert.deepEqual(yield* Ref.get(outcomes), [
        {
          effectId: effect.id,
          workerId: effect.leaseOwner!,
          expectedAttempt: effect.attemptCount,
          outcome: { taskId: effect.id, result: null, effect: "unknown" },
          evidence: {
            operationId: effect.id,
            operation: "close_session",
            threadId,
            providerSessionId: oldSessionId,
            providerThreadId,
            instanceId: binding.instanceId,
            runtimeGeneration: binding.runtimeGeneration,
            outcome: "unknown",
          },
        },
      ]);
      assert.isTrue(Exit.isFailure(exit));
      if (Exit.isFailure(exit))
        assert.deepEqual(nativeEffectEvidenceFromCause(exit.cause), {
          operationId: effect.id,
          operation: "close_session",
          threadId,
          providerSessionId: oldSessionId,
          providerThreadId,
          instanceId: binding.instanceId,
          runtimeGeneration: binding.runtimeGeneration,
          outcome: "unknown",
        });
    }),
);

for (const mode of ["leased", "unleased", "existing_hold", "prehold_unavailable"] as const) {
  it.effect(
    `executes pinned provider deletion with ${mode} only through its durable intent and qualified completion`,
    () =>
      Effect.gen(function* () {
        const now = yield* DateTime.now;
        const timestamp = DateTime.formatIso(now);
        const workerId = "managed-provider-deletion-worker";
        const commandId = CommandId.make(`command:managed-provider-deletion:${mode}`);
        const effect: EffectOutbox.OrchestrationEffectV2 = {
          ...restartEffect(now, { type: "detach" }),
          commandId,
          id: `effect:${commandId}:provider-session.detach:${oldSessionId}`,
          request: { type: "provider-session.detach", providerSessionId: oldSessionId },
          leaseOwner: workerId,
          leaseExpiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 1 })),
        };
        const ownerBirth = {
          kind: "application_v2_thread_birth" as const,
          threadId,
          eventId: EventId.make("managed-provider-deletion-birth"),
          sequence: 1,
        };
        const binding = {
          threadId,
          providerThreadId,
          providerSessionId: oldSessionId,
          instanceId: ProviderInstanceId.make("codex"),
          driver: ProviderDriverKind.make("codex"),
          nativeThreadId: mode === "unleased" ? null : "managed-provider-deletion-native",
          runtimeGeneration: "managed-provider-deletion-generation",
        };
        const common = {
          effectId: effect.id,
          threadId,
          ownerBirth,
          deletion: {
            commandId,
            eventId: EventId.make("managed-provider-deletion-event"),
            sequence: 2,
          },
          task: { kind: "provider" as const, expectedBinding: binding, evidenceRevision: 4 },
        };
        const subject =
          mode === "unleased"
            ? {
                version: 1 as const,
                ...common,
                leaseInventory: {
                  status: "absent" as const,
                  resourcePath: "/workspace/managed-provider-deletion",
                },
              }
            : {
                version: 2 as const,
                ...common,
                lease: {
                  resourcePath: "/workspace/managed-provider-deletion",
                  leaseId: "managed-provider-deletion-lease",
                  ownerThreadId: threadId,
                  ownerIncarnation: yield* Schema.encodeEffect(
                    Schema.fromJsonString(Schema.Tuple([Schema.String, EventId, Schema.Number])),
                  )([
                    "t3.orchestration-v2.thread-birth/v1",
                    ownerBirth.eventId,
                    ownerBirth.sequence,
                  ]),
                  branch: "managed-provider-deletion",
                  acquiredAtMs: 1,
                  renewedAtMs: 2,
                  expiresAtMs: 3,
                },
              };
        const task = {
          ...subject,
          bindingSha256: EventSink.deletionCleanupTaskBindingDigestV1(subject),
          recordedAt: timestamp,
        };
        const evidence = {
          operationId: effect.id,
          operation: "close_session" as const,
          threadId,
          providerSessionId: binding.providerSessionId,
          providerThreadId,
          instanceId: binding.instanceId,
          runtimeGeneration: binding.runtimeGeneration,
          outcome: "unknown" as const,
        };
        const originalHold = {
          effectId: effect.id,
          threadId,
          workerId,
          operationId: effect.id,
          expectedAttempt: effect.attemptCount,
          heldAt: timestamp,
          evidence,
        };
        const events = yield* Ref.make<ReadonlyArray<string>>([]);
        const order = yield* Ref.make<ReadonlyArray<string>>([]);
        const current = yield* Ref.make(effect);
        const holds = yield* Ref.make<ReadonlyArray<EffectOutbox.UnknownEffectHoldV2>>(
          mode === "existing_hold" ? [originalHold] : [],
        );
        const latest = yield* Ref.make<EventSink.DeletionCleanupTaskOutcomeRowV1 | null>(null);
        const record = (name: string) => Ref.update(order, (values) => [...values, name]);
        const outboxLayer = Layer.mock(EffectOutbox.EffectOutboxV2)({
          claimNext: () => Effect.succeed(Option.some(effect)),
          get: () => Ref.get(current).pipe(Effect.map(Option.some)),
          listHeldByThreadId: () => Ref.get(holds),
          awaitCancellation: () => Effect.never,
          clearCancellation: () => Effect.void,
          holdUnknown: (input) =>
            Effect.gen(function* () {
              assert.deepEqual(input, {
                effectId: effect.id,
                workerId,
                operationId: effect.id,
                expectedAttempt: effect.attemptCount,
                evidence,
              });
              if (mode === "existing_hold" || mode === "prehold_unavailable") return false;
              if ((yield* Ref.get(holds)).length > 0) return false;
              yield* Ref.set(holds, [originalHold]);
              yield* record("prehold");
              return true;
            }),
          succeed: () => Effect.die("Qualified cleanup must not call ordinary succeed"),
          retry: () => Effect.die("Unknown cleanup must not retry"),
          fail: () => Effect.die("Unknown cleanup must not fail"),
        });
        const eventSinkLayer = Layer.mock(EventSink.EventSinkV2)({
          readDeletionCleanupTask: () => Effect.succeed(task),
          readDeletionCleanupTaskOutcome: () => Ref.get(latest),
          recordLeaseCleanupTaskOutcome: (input) =>
            Effect.gen(function* () {
              assert.deepEqual(yield* Ref.get(holds), [originalHold]);
              assert.deepEqual(input.evidence, evidence);
              yield* record("intent");
              yield* Ref.set(latest, {
                ordinal: 0,
                outcome: input.outcome,
                correlation: {
                  workerId,
                  expectedAttempt: effect.attemptCount,
                  bindingSha256: task.bindingSha256,
                  evidence: input.evidence,
                },
                recordedAt: timestamp,
              });
              return input.outcome;
            }),
          recordObservedDeletionCleanupOutcome: (input) =>
            Effect.gen(function* () {
              assert.equal(input.expectedLatestOrdinal, 0);
              assert.equal(input.bindingSha256, task.bindingSha256);
              assert.deepEqual(input.coveredHolds, [originalHold]);
              assert.isTrue(
                "kind" in input.observation && input.observation.kind === "managed_provider",
              );
              if ("kind" in input.observation && input.observation.kind === "managed_provider") {
                assert.deepEqual(input.observation.binding, binding);
                assert.equal(input.observation.evidenceRevision, 4);
                assert.equal(input.observation.nativeOperation.operationId, effect.id);
                assert.equal(input.observation.result.status, "stopped");
              }
              const proof: EventSink.ObservedDeletionCleanupOutcomeV1 = {
                ordinal: 1,
                bindingSha256: task.bindingSha256,
                outcome: { taskId: effect.id, result: "succeeded", effect: "confirmed" },
                evidence: {
                  version: 1,
                  schema: "t3.deletion-cleanup-observation/v1",
                  producer: "managed_provider",
                  observation: input.observation,
                  coveredHolds: input.coveredHolds,
                },
              };
              yield* record("observed");
              yield* Ref.set(latest, {
                ordinal: 1,
                outcome: proof.outcome,
                correlation: {
                  workerId,
                  expectedAttempt: effect.attemptCount,
                  bindingSha256: task.bindingSha256,
                  evidence: proof.evidence,
                },
                recordedAt: timestamp,
              });
              return proof;
            }),
          completeObservedDeletionCleanup: (input) =>
            Effect.gen(function* () {
              assert.deepEqual(input, {
                effectId: effect.id,
                bindingSha256: task.bindingSha256,
                expectedLatestOrdinal: 1,
              });
              assert.equal((yield* Ref.get(latest))?.outcome.result, "succeeded");
              yield* record("complete");
              yield* Ref.set(current, {
                ...effect,
                status: "succeeded",
                completedAt: timestamp,
                leaseOwner: null,
                leaseExpiresAt: null,
              });
              return true;
            }),
        });
        const executor = makeExecutorLayer({
          events,
          outboxLayer,
          eventSinkLayer,
          stopPinnedRuntime: (input) =>
            Effect.gen(function* () {
              assert.deepEqual(yield* Ref.get(order), ["prehold", "intent"]);
              assert.deepEqual(input, {
                operationId: effect.id,
                binding,
                expectedEvidenceRevision: 4,
                deletionBindingSha256: task.bindingSha256,
              });
              yield* record("stop");
              return {
                status: "stopped" as const,
                operationId: effect.id,
                binding,
                cancelledPendingStart: false,
                interruptedProviderTurnIds: [],
                readback: { threadAttached: false as const },
              };
            }),
        });
        const exit = yield* EffectWorker.OrchestrationEffectWorkerV2.pipe(
          Effect.flatMap((worker) => worker.runOnce),
          Effect.exit,
          Effect.provide(
            EffectWorker.layerWithOptions({ workerId }).pipe(
              Layer.provide(Layer.merge(executor, outboxLayer)),
            ),
          ),
        );
        assert.deepEqual(yield* Ref.get(events), []);
        if (mode === "leased" || mode === "unleased") {
          assert.isTrue(Exit.isSuccess(exit));
          assert.deepEqual(yield* Ref.get(order), [
            "prehold",
            "intent",
            "stop",
            "observed",
            "complete",
          ]);
          assert.equal((yield* Ref.get(current)).status, "succeeded");
          assert.deepEqual(yield* Ref.get(holds), [originalHold]);
        } else {
          assert.equal(Exit.isSuccess(exit), mode === "existing_hold");
          assert.deepEqual(yield* Ref.get(order), []);
          assert.equal((yield* Ref.get(current)).status, "running");
        }
      }),
  );
}

it.effect(
  "preserves ordinary terminal cleanup from an actual accepted archive receipt and owned SQL claim",
  () => {
    const database = SqlitePersistenceMemory;
    const stores = Layer.merge(EventStore.layer, ProjectionStore.layer).pipe(
      Layer.provide(database),
    );
    const outboxProvided = EffectOutbox.layer.pipe(Layer.provide(database));
    const sinkProvided = EventSink.layer.pipe(Layer.provide(Layer.merge(stores, database)));
    return Effect.gen(function* () {
      const sink = yield* EventSink.EventSinkV2;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const now = yield* DateTime.now;
      const instanceId = ProviderInstanceId.make("codex");
      const commandId = CommandId.make("command:ordinary-archive-cleanup");
      const appThread: OrchestrationV2AppThread = {
        createdBy: "user",
        creationSource: "web",
        id: threadId,
        projectId: ProjectId.make("project:ordinary-archive-cleanup"),
        title: "Ordinary archive cleanup",
        providerInstanceId: instanceId,
        modelSelection: { instanceId, model: "gpt-5-codex" },
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        activeProviderThreadId: null,
        lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
        archivedAt: null,
        settledOverride: null,
        settledAt: null,
        lastVisitedAt: null,
        deletedAt: null,
      };
      yield* sink.write({
        events: [
          {
            id: EventId.make("event:ordinary-archive-birth"),
            threadId,
            type: "thread.created",
            occurredAt: now,
            payload: appThread,
          },
        ],
      });
      const accepted = yield* sink.commitCommand({
        commandId,
        threadId,
        commandType: "thread.archive",
        acceptedAt: now,
        events: [
          {
            id: EventId.make("event:ordinary-archive-accepted"),
            threadId,
            type: "thread.archived",
            occurredAt: now,
            payload: { ...appThread, archivedAt: now },
          },
        ],
        effects: [
          {
            id: `effect:${commandId}:terminal.cleanup`,
            commandId,
            threadId,
            request: { type: "terminal.cleanup" },
          },
        ],
      });
      assert.isTrue(accepted.committed);
      const events = yield* Ref.make<ReadonlyArray<string>>([]);
      const outboxLayer = Layer.succeed(EffectOutbox.EffectOutboxV2, outbox);
      const executor = makeExecutorLayer({
        events,
        outboxLayer,
        eventSinkLayer: Layer.succeed(EventSink.EventSinkV2, sink),
      });
      const worked = yield* EffectWorker.OrchestrationEffectWorkerV2.pipe(
        Effect.flatMap((worker) => worker.runOnce),
        Effect.provide(
          EffectWorker.layerWithOptions({ workerId: "ordinary-archive-worker" }).pipe(
            Layer.provide(Layer.merge(executor, outboxLayer)),
          ),
        ),
      );
      assert.isTrue(worked);
      assert.deepEqual(yield* Ref.get(events), ["terminal-legacy"]);
      const effects = yield* outbox.listByCommandId(commandId);
      assert.equal(effects.length, 1);
      assert.equal(effects[0]!.status, "succeeded");
      assert.deepEqual(yield* outbox.listHeldByThreadId(threadId), []);
    }).pipe(Effect.provide(Layer.merge(sinkProvided, outboxProvided)));
  },
);

for (const kind of ["terminal", "attachment"] as const) {
  it.effect(
    `holds pinned ${kind} cleanup through the owned callee without legacy cleanup or terminal settlement`,
    () =>
      Effect.gen(function* () {
        const now = yield* DateTime.now;
        const timestamp = DateTime.formatIso(now);
        const workerId = `owned-${kind}-cleanup-worker`;
        const commandId = CommandId.make(`command:owned-${kind}-cleanup`);
        const effect: EffectOutbox.OrchestrationEffectV2 = {
          ...restartEffect(now, { type: "detach" }),
          commandId,
          leaseOwner: workerId,
          id: `effect:${commandId}:${kind}.cleanup`,
          request:
            kind === "terminal"
              ? { type: "terminal.cleanup" }
              : { type: "attachment.cleanup", attachmentIds: ["owned-attachment"] },
          leaseExpiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 1 })),
        };
        const ownerBirth = {
          kind: "application_v2_thread_birth" as const,
          threadId,
          eventId: EventId.make(`event:owned-${kind}-birth`),
          sequence: 1,
        };
        const subject = {
          version: 2 as const,
          effectId: effect.id,
          threadId,
          ownerBirth,
          lease: {
            resourcePath: "/workspace/owned-resource-cleanup",
            leaseId: `owned-${kind}-lease`,
            ownerThreadId: threadId,
            ownerIncarnation: yield* Schema.encodeEffect(
              Schema.fromJsonString(Schema.Tuple([Schema.String, EventId, Schema.Number])),
            )(["t3.orchestration-v2.thread-birth/v1", ownerBirth.eventId, ownerBirth.sequence]),
            branch: "owned-resource-branch",
            acquiredAtMs: 1,
            renewedAtMs: 2,
            expiresAtMs: 3,
          },
          deletion: {
            commandId,
            eventId: EventId.make(`event:owned-${kind}-deleted`),
            sequence: 2,
          },
          task:
            kind === "terminal"
              ? {
                  kind,
                  capture: {
                    managerId: "original-terminal-manager",
                    threadId,
                    ownerBirth,
                    status: "captured" as const,
                    managedTargetsOnly: true as const,
                    targets: [
                      {
                        threadId,
                        terminalId: "original-terminal",
                        handleId: "original-handle",
                        ownerBirth,
                      },
                    ],
                  },
                }
              : { kind, attachmentIds: ["owned-attachment"] },
        };
        const task = {
          ...subject,
          bindingSha256: EventSink.leaseCleanupTaskBindingDigestV2(subject),
          recordedAt: timestamp,
        };
        const events = yield* Ref.make<ReadonlyArray<string>>([]);
        const recorded = yield* Ref.make<
          ReadonlyArray<
            Parameters<EventSink.EventSinkV2["Service"]["recordLeaseCleanupTaskOutcome"]>[0]
          >
        >([]);
        const held = yield* Ref.make<ReadonlyArray<EffectOutbox.UnknownEffectHoldV2>>([]);
        const preholds = yield* Ref.make(0);
        const settlements = yield* Ref.make(0);
        const settle = () => Ref.update(settlements, (value) => value + 1).pipe(Effect.as(true));
        const outboxLayer = Layer.mock(EffectOutbox.EffectOutboxV2)({
          claimNext: () => Effect.succeed(Option.some(effect)),
          get: () => Effect.succeed(Option.some(effect)),
          awaitCancellation: () => Effect.never,
          clearCancellation: () => Effect.void,
          listHeldByThreadId: () => Ref.get(held),
          succeed: settle,
          retry: settle,
          fail: settle,
          holdResourceCleanupUnknown: (input) =>
            Effect.gen(function* () {
              assert.deepEqual(input, {
                effectId: effect.id,
                workerId,
                expectedAttempt: effect.attemptCount,
                evidence: {
                  version: 1,
                  kind: "resource_cleanup",
                  operationId: effect.id,
                  threadId,
                  taskKind: kind,
                  bindingSha256: task.bindingSha256,
                  outcome: "unknown",
                },
              });
              yield* Ref.update(preholds, (value) => value + 1);
              if ((yield* Ref.get(held)).length !== 0) return false;
              yield* Ref.set(held, [
                {
                  effectId: effect.id,
                  threadId,
                  workerId,
                  operationId: effect.id,
                  expectedAttempt: effect.attemptCount,
                  heldAt: timestamp,
                  evidence: input.evidence,
                },
              ]);
              return true;
            }),
        });
        const outcome = { taskId: effect.id, result: null, effect: "unknown" as const };
        const evidence = { reason: `${kind}_quiescence_unavailable` };
        const owned = (
          binding: EventSink.DeletionCleanupTaskBindingV1,
          correlation: ResourceCleanupService.OwnedResourceCleanupCorrelationV1,
        ) => {
          assert.deepEqual(binding, task);
          assert.deepEqual(correlation, { workerId, expectedAttempt: effect.attemptCount });
          return Effect.gen(function* () {
            assert.equal(yield* Ref.get(preholds), 1);
            assert.equal((yield* Ref.get(held)).length, 1);
            yield* Ref.update(events, (values) => [...values, `${kind}-owned`]);
            return { outcome, evidence };
          });
        };
        const executor = makeExecutorLayer({
          events,
          outboxLayer,
          readLeaseCleanupTask: () => Effect.succeed(task),
          resourceCleanup:
            kind === "terminal"
              ? { cleanupOwnedTerminals: owned }
              : { cleanupOwnedAttachments: owned },
          recordLeaseCleanupTaskOutcome: (input) =>
            Effect.gen(function* () {
              yield* Ref.update(recorded, (values) => [...values, input]);
              yield* Ref.set(held, [
                {
                  effectId: effect.id,
                  threadId,
                  workerId,
                  operationId: effect.id,
                  expectedAttempt: effect.attemptCount,
                  heldAt: timestamp,
                  evidence: {
                    version: 1,
                    kind: "resource_cleanup",
                    operationId: effect.id,
                    threadId,
                    taskKind: kind,
                    bindingSha256: task.bindingSha256,
                    outcome: "unknown",
                  },
                },
              ]);
              return input.outcome;
            }),
        });
        yield* EffectWorker.OrchestrationEffectWorkerV2.pipe(
          Effect.flatMap((worker) => worker.runOnce),
          Effect.provide(
            EffectWorker.layerWithOptions({ workerId }).pipe(
              Layer.provide(Layer.merge(executor, outboxLayer)),
            ),
          ),
        );
        assert.deepEqual(yield* Ref.get(events), [`${kind}-owned`]);
        assert.equal(yield* Ref.get(settlements), 0);
        assert.deepEqual(yield* Ref.get(recorded), [
          {
            effectId: effect.id,
            workerId,
            expectedAttempt: effect.attemptCount,
            outcome,
            evidence,
          },
        ]);
        yield* EffectWorker.OrchestrationEffectExecutorV2.pipe(
          Effect.flatMap((service) => service.execute(effect)),
          Effect.provide(executor),
        );
        assert.equal(yield* Ref.get(preholds), 2);
        assert.deepEqual(yield* Ref.get(events), [`${kind}-owned`]);
        assert.equal((yield* Ref.get(recorded)).length, 1);
        const rejectedOutbox = Layer.mock(EffectOutbox.EffectOutboxV2)({
          get: () => Effect.succeed(Option.some(effect)),
          listHeldByThreadId: () => Effect.succeed([]),
          holdResourceCleanupUnknown: () => Effect.succeed(false),
        });
        const rejectedExecutor = makeExecutorLayer({
          events,
          outboxLayer: rejectedOutbox,
          readLeaseCleanupTask: () => Effect.succeed(task),
          resourceCleanup:
            kind === "terminal"
              ? { cleanupOwnedTerminals: owned }
              : { cleanupOwnedAttachments: owned },
        });
        const rejected = yield* EffectWorker.OrchestrationEffectExecutorV2.pipe(
          Effect.flatMap((service) => service.execute(effect)),
          Effect.exit,
          Effect.provide(rejectedExecutor),
        );
        assert.isTrue(Exit.isFailure(rejected));
        assert.deepEqual(yield* Ref.get(events), [`${kind}-owned`]);
      }),
  );

  for (const taskRead of ["absent", "unavailable"] as const) {
    it.effect(`holds unbound ${kind} cleanup when its task binding is ${taskRead}`, () =>
      Effect.gen(function* () {
        const now = yield* DateTime.now;
        const timestamp = DateTime.formatIso(now);
        const workerId = `unbound-${kind}-cleanup-worker`;
        const commandId = CommandId.make(`command:unbound-${kind}-cleanup`);
        const effect: EffectOutbox.OrchestrationEffectV2 = {
          ...restartEffect(now, { type: "detach" }),
          commandId,
          leaseOwner: workerId,
          id: `effect:${commandId}:${kind}.cleanup`,
          request:
            kind === "terminal"
              ? { type: "terminal.cleanup" }
              : { type: "attachment.cleanup", attachmentIds: ["original-attachment"] },
          leaseExpiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 1 })),
        };
        const evidence: EffectOutbox.ResourceCleanupUnknownEvidenceV1 = {
          version: 1,
          kind: "resource_cleanup",
          operationId: effect.id,
          threadId,
          taskKind: kind,
          bindingSha256: null,
          reason: "task_binding_unavailable",
          outcome: "unknown",
        };
        const events = yield* Ref.make<ReadonlyArray<string>>([]);
        const held = yield* Ref.make<ReadonlyArray<EffectOutbox.UnknownEffectHoldV2>>([]);
        const holds = yield* Ref.make(0);
        const settlements = yield* Ref.make(0);
        const settle = () => Ref.update(settlements, (value) => value + 1).pipe(Effect.as(true));
        const outboxLayer = Layer.mock(EffectOutbox.EffectOutboxV2)({
          claimNext: () => Effect.succeed(Option.some(effect)),
          get: () => Effect.succeed(Option.some(effect)),
          awaitCancellation: () => Effect.never,
          clearCancellation: () => Effect.void,
          listHeldByThreadId: () => Ref.get(held),
          succeed: settle,
          retry: settle,
          fail: settle,
          holdResourceCleanupUnknown: (input) =>
            Effect.gen(function* () {
              assert.deepEqual(input, {
                effectId: effect.id,
                workerId,
                expectedAttempt: effect.attemptCount,
                evidence,
              });
              yield* Ref.update(holds, (value) => value + 1);
              yield* Ref.set(held, [
                {
                  effectId: effect.id,
                  threadId,
                  workerId,
                  operationId: effect.id,
                  expectedAttempt: effect.attemptCount,
                  heldAt: timestamp,
                  evidence,
                },
              ]);
              return true;
            }),
        });
        const executor = makeExecutorLayer({
          events,
          outboxLayer,
          readLeaseCleanupTask: () =>
            taskRead === "absent"
              ? Effect.succeed(null)
              : Effect.fail(
                  new EventSink.EventSinkWriteError({
                    eventCount: 0,
                    cause: "cleanup binding unavailable",
                  }),
                ),
        });
        yield* EffectWorker.OrchestrationEffectWorkerV2.pipe(
          Effect.flatMap((worker) => worker.runOnce),
          Effect.provide(
            EffectWorker.layerWithOptions({ workerId }).pipe(
              Layer.provide(Layer.merge(executor, outboxLayer)),
            ),
          ),
        );
        assert.deepEqual(yield* Ref.get(events), []);
        assert.equal(yield* Ref.get(holds), 1);
        assert.equal(yield* Ref.get(settlements), 0);
      }),
    );
  }
}

for (const terminalStatus of ["closed", "observed_absent", "unknown"] as const) {
  it.effect(
    `qualifies ${terminalStatus} managed terminal cleanup from the original absent-lease capture`,
    () =>
      Effect.gen(function* () {
        const now = yield* DateTime.now;
        const timestamp = DateTime.formatIso(now);
        const workerId = "managed-terminal-cleanup-worker";
        const commandId = CommandId.make(`command:managed-terminal:${terminalStatus}`);
        const effect: EffectOutbox.OrchestrationEffectV2 = {
          ...restartEffect(now, { type: "detach" }),
          commandId,
          id: `effect:${commandId}:terminal.cleanup`,
          request: { type: "terminal.cleanup" },
          leaseOwner: workerId,
          leaseExpiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 1 })),
        };
        const ownerBirth = {
          kind: "application_v2_thread_birth" as const,
          threadId,
          eventId: EventId.make("managed-terminal-original-birth"),
          sequence: 1,
        };
        const capture = {
          managerId: "managed-terminal-original-manager",
          threadId,
          ownerBirth,
          status: "captured" as const,
          managedTargetsOnly: true as const,
          targets:
            terminalStatus === "observed_absent"
              ? []
              : [
                  {
                    threadId,
                    terminalId: "managed-terminal-original-target",
                    handleId: "managed-terminal-original-handle",
                    ownerBirth,
                  },
                ],
        };
        const subject = {
          version: 1 as const,
          effectId: effect.id,
          threadId,
          ownerBirth,
          leaseInventory: {
            status: "absent" as const,
            resourcePath: "/workspace/managed-terminal-cleanup",
          },
          deletion: { commandId, eventId: EventId.make("managed-terminal-deletion"), sequence: 2 },
          task: { kind: "terminal" as const, capture },
        };
        const task = {
          ...subject,
          bindingSha256: EventSink.deletionCleanupTaskBindingDigestV1(subject),
          recordedAt: timestamp,
        };
        const events = yield* Ref.make<ReadonlyArray<string>>([]);
        const order = yield* Ref.make<ReadonlyArray<string>>([]);
        const current = yield* Ref.make(effect);
        const holds = yield* Ref.make<ReadonlyArray<EffectOutbox.UnknownEffectHoldV2>>([]);
        const latest = yield* Ref.make<EventSink.DeletionCleanupTaskOutcomeRowV1 | null>(null);
        const record = (name: string) => Ref.update(order, (values) => [...values, name]);
        const outboxLayer = Layer.mock(EffectOutbox.EffectOutboxV2)({
          claimNext: () => Effect.succeed(Option.some(effect)),
          get: () => Ref.get(current).pipe(Effect.map(Option.some)),
          listHeldByThreadId: () => Ref.get(holds),
          awaitCancellation: () => Effect.never,
          clearCancellation: () => Effect.void,
          holdResourceCleanupUnknown: (input) =>
            Effect.gen(function* () {
              assert.equal(input.evidence.bindingSha256, task.bindingSha256);
              if ((yield* Ref.get(holds)).length > 0) return false;
              yield* Ref.set(holds, [
                {
                  effectId: effect.id,
                  threadId,
                  workerId,
                  operationId: effect.id,
                  expectedAttempt: effect.attemptCount,
                  heldAt: timestamp,
                  evidence: input.evidence,
                },
              ]);
              yield* record("prehold");
              return true;
            }),
          succeed: () => Effect.die("Managed terminal cleanup must not call generic succeed"),
          retry: () => Effect.die("Managed terminal cleanup must not retry"),
          fail: () => Effect.die("Managed terminal cleanup must not fail"),
        });
        const eventSinkLayer = Layer.mock(EventSink.EventSinkV2)({
          readDeletionCleanupTask: () => Effect.succeed(task),
          readDeletionCleanupTaskOutcome: () => Ref.get(latest),
          recordObservedDeletionCleanupOutcome: (input) =>
            Effect.gen(function* () {
              assert.equal(input.expectedLatestOrdinal, -1);
              assert.deepEqual(input.coveredHolds, yield* Ref.get(holds));
              assert.isTrue(
                "kind" in input.observation && input.observation.kind === "managed_terminal",
              );
              if ("kind" in input.observation && input.observation.kind === "managed_terminal") {
                assert.deepEqual(input.observation.capture, capture);
                assert.equal(input.observation.result.status, terminalStatus);
                assert.equal(input.observation.workerId, workerId);
                assert.equal(input.observation.expectedAttempt, effect.attemptCount);
              }
              const proof: EventSink.ObservedDeletionCleanupOutcomeV1 = {
                ordinal: 0,
                bindingSha256: task.bindingSha256,
                outcome: {
                  taskId: effect.id,
                  result: terminalStatus === "unknown" ? null : "succeeded",
                  effect:
                    terminalStatus === "unknown"
                      ? "unknown"
                      : terminalStatus === "observed_absent"
                        ? "absent"
                        : "confirmed",
                },
                evidence: {
                  version: 1,
                  schema: "t3.deletion-cleanup-observation/v1",
                  producer: "managed_terminal",
                  observation: input.observation,
                  coveredHolds: input.coveredHolds,
                },
              };
              yield* record("observed");
              yield* Ref.set(latest, {
                ordinal: 0,
                outcome: proof.outcome,
                correlation: {
                  workerId,
                  expectedAttempt: effect.attemptCount,
                  bindingSha256: task.bindingSha256,
                  evidence: proof.evidence,
                },
                recordedAt: timestamp,
              });
              return proof;
            }),
          completeObservedDeletionCleanup: (input) =>
            Effect.gen(function* () {
              assert.deepEqual(input, {
                effectId: effect.id,
                bindingSha256: task.bindingSha256,
                expectedLatestOrdinal: 0,
              });
              assert.equal((yield* Ref.get(latest))?.outcome.result, "succeeded");
              yield* record("complete");
              yield* Ref.set(current, {
                ...effect,
                status: "succeeded",
                completedAt: timestamp,
                leaseOwner: null,
                leaseExpiresAt: null,
              });
              return true;
            }),
        });
        const executor = makeExecutorLayer({
          events,
          outboxLayer,
          eventSinkLayer,
          resourceCleanup: {
            cleanupOwnedTerminals: (binding, correlation) =>
              Effect.gen(function* () {
                assert.deepEqual(binding, task);
                assert.deepEqual(correlation, { workerId, expectedAttempt: effect.attemptCount });
                assert.deepEqual(yield* Ref.get(order), ["prehold"]);
                yield* record("close");
                return {
                  outcome: { taskId: effect.id, result: null, effect: "unknown" as const },
                  evidence: {},
                  observation: {
                    version: 1 as const,
                    kind: "managed_terminal" as const,
                    effectId: effect.id,
                    bindingSha256: task.bindingSha256,
                    workerId,
                    expectedAttempt: effect.attemptCount,
                    capture,
                    result: {
                      status: terminalStatus,
                      managedTargetsOnly: true as const,
                      processExitObserved: terminalStatus === "closed",
                      descendantsQuiescence: "unavailable" as const,
                      futureWakeClosure: "unavailable" as const,
                    },
                    observedAt: timestamp,
                  },
                };
              }),
          },
        });
        yield* EffectWorker.OrchestrationEffectWorkerV2.pipe(
          Effect.flatMap((worker) => worker.runOnce),
          Effect.provide(
            EffectWorker.layerWithOptions({ workerId }).pipe(
              Layer.provide(Layer.merge(executor, outboxLayer)),
            ),
          ),
        );
        assert.deepEqual(yield* Ref.get(events), []);
        assert.deepEqual(
          yield* Ref.get(order),
          terminalStatus === "unknown"
            ? ["prehold", "close", "observed"]
            : ["prehold", "close", "observed", "complete"],
        );
        assert.equal(
          (yield* Ref.get(current)).status,
          terminalStatus === "unknown" ? "running" : "succeeded",
        );
        assert.equal((yield* Ref.get(holds)).length, 1);
      }),
  );
}

for (const mode of ["captured", "empty", "prior_unbound", "prepare_unavailable"] as const) {
  it.effect(
    `prepares the original deleted terminal roster ${mode} before its first bound hold`,
    () =>
      Effect.gen(function* () {
        const now = yield* DateTime.now;
        const timestamp = DateTime.formatIso(now);
        const workerId = "terminal-roster-preparation-worker";
        const commandId = CommandId.make(`command:terminal-roster:${mode}`);
        const effect: EffectOutbox.OrchestrationEffectV2 = {
          ...restartEffect(now, { type: "detach" }),
          commandId,
          id: `effect:${commandId}:terminal.cleanup`,
          request: { type: "terminal.cleanup" },
          leaseOwner: workerId,
          leaseExpiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 1 })),
        };
        const ownerBirth = {
          kind: "application_v2_thread_birth" as const,
          threadId,
          eventId: EventId.make("terminal-roster-original-birth"),
          sequence: 1,
        };
        const deletion = {
          commandId,
          eventId: EventId.make("terminal-roster-deletion"),
          sequence: 2,
        };
        const capture = {
          managerId: "terminal-roster-original-manager",
          threadId,
          ownerBirth,
          status: "captured" as const,
          managedTargetsOnly: true as const,
          targets:
            mode === "empty"
              ? []
              : [
                  {
                    threadId,
                    ownerBirth,
                    terminalId: "terminal-roster-original-target",
                    handleId: "terminal-roster-original-handle",
                  },
                ],
        };
        const subject = {
          version: 1 as const,
          effectId: effect.id,
          threadId,
          ownerBirth,
          deletion,
          leaseInventory: { status: "absent" as const, resourcePath: "/workspace/terminal-roster" },
          task: { kind: "terminal" as const, capture },
        };
        const task = {
          ...subject,
          bindingSha256: EventSink.deletionCleanupTaskBindingDigestV1(subject),
          recordedAt: timestamp,
        };
        const original: EventSink.ThreadDeletionCommandRecordV1 = {
          command: { type: "thread.delete", commandId, threadId },
          commandDigest: "a".repeat(64),
          ownerBirth,
          deletion,
          recordedAt: timestamp,
          inventory: {
            worktree: {
              projectId: ProjectId.make("terminal-roster-project"),
              path: "/workspace/terminal-roster",
              branch: null,
            },
            projectRoot: "/workspace",
            leaseInventory: { status: "absent" },
            prerequisiteEffectIds: [effect.id],
            captureStatus: "captured",
            reason: null,
          },
        };
        const events = yield* Ref.make<ReadonlyArray<string>>([]);
        const order = yield* Ref.make<ReadonlyArray<string>>([]);
        const record = (name: string) => Ref.update(order, (values) => [...values, name]);
        const stored = yield* Ref.make<EventSink.DeletionCleanupTaskBindingV1 | null>(null);
        const unbound: EffectOutbox.ResourceCleanupUnknownEvidenceV1 = {
          version: 1,
          kind: "resource_cleanup",
          operationId: effect.id,
          threadId,
          taskKind: "terminal",
          bindingSha256: null,
          reason: "task_binding_unavailable",
          outcome: "unknown",
        };
        const holds = yield* Ref.make<ReadonlyArray<EffectOutbox.UnknownEffectHoldV2>>(
          mode === "prior_unbound"
            ? [
                {
                  effectId: effect.id,
                  threadId,
                  workerId,
                  operationId: effect.id,
                  expectedAttempt: effect.attemptCount,
                  heldAt: timestamp,
                  evidence: unbound,
                },
              ]
            : [],
        );
        const outboxLayer = Layer.mock(EffectOutbox.EffectOutboxV2)({
          claimNext: () => Effect.succeed(Option.some(effect)),
          get: () => Effect.succeed(Option.some(effect)),
          listHeldByThreadId: () => Ref.get(holds),
          awaitCancellation: () => Effect.never,
          clearCancellation: () => Effect.void,
          holdResourceCleanupUnknown: (input) =>
            Effect.gen(function* () {
              assert.equal(
                input.evidence.bindingSha256,
                mode === "captured" || mode === "empty" ? task.bindingSha256 : null,
              );
              if ((yield* Ref.get(holds)).length > 0) return false;
              yield* record("hold");
              yield* Ref.set(holds, [
                {
                  effectId: effect.id,
                  threadId,
                  workerId,
                  operationId: effect.id,
                  expectedAttempt: effect.attemptCount,
                  heldAt: timestamp,
                  evidence: input.evidence,
                },
              ]);
              return true;
            }),
          succeed: () => Effect.die("Unknown roster cleanup must not succeed"),
          retry: () => Effect.die("Unknown roster cleanup must not retry"),
          fail: () => Effect.die("Unknown roster cleanup must not fail"),
        });
        const eventSinkLayer = Layer.mock(EventSink.EventSinkV2)({
          readDeletionCleanupTask: () => Ref.get(stored),
          readDeletionCleanupTaskOutcome: () => Effect.succeed(null),
          readThreadDeletionCommand: (id) =>
            Effect.gen(function* () {
              assert.equal(id, commandId);
              yield* record("original");
              return original;
            }),
          prepareDeletionCleanupTaskBindings: (input) =>
            Effect.gen(function* () {
              assert.deepEqual(input, { commandId, terminalCapture: capture });
              yield* record("prepare");
              if (mode === "prepare_unavailable")
                return yield* new EventSink.EventSinkWriteError({
                  eventCount: 0,
                  cause: "preparation unavailable",
                });
              yield* Ref.set(stored, task);
              return [task];
            }),
          readNativeCommandFacts: () => Effect.succeed({ eventMetadataOverflow: true } as never),
          readApplicationBirthRecord: () => Effect.succeed(null),
          recordLeaseCleanupTaskOutcome: (input) =>
            Effect.gen(function* () {
              assert.equal(input.effectId, effect.id);
              assert.deepEqual(input.outcome, {
                taskId: effect.id,
                result: null,
                effect: "unknown",
              });
              yield* record("outcome");
              return input.outcome;
            }),
        });
        const executor = makeExecutorLayer({
          events,
          outboxLayer,
          eventSinkLayer,
          resourceCleanup: {
            captureOwnedTerminalTargets: (birth) =>
              Effect.gen(function* () {
                assert.deepEqual(birth, ownerBirth);
                assert.deepEqual(yield* Ref.get(holds), []);
                yield* record("capture");
                return capture;
              }),
            cleanupOwnedTerminals: (binding, correlation) =>
              Effect.gen(function* () {
                assert.deepEqual(binding, task);
                assert.deepEqual(correlation, { workerId, expectedAttempt: effect.attemptCount });
                assert.deepEqual(yield* Ref.get(order), ["original", "capture", "prepare", "hold"]);
                yield* record("close");
                return {
                  outcome: { taskId: effect.id, result: null, effect: "unknown" as const },
                  evidence: { reason: "fixture_native_close_unconfirmed" },
                };
              }),
          },
        });
        assert.isTrue(
          yield* EffectWorker.OrchestrationEffectWorkerV2.pipe(
            Effect.flatMap((worker) => worker.runOnce),
            Effect.provide(
              EffectWorker.layerWithOptions({ workerId }).pipe(
                Layer.provide(Layer.merge(executor, outboxLayer)),
              ),
            ),
          ),
        );
        assert.deepEqual(yield* Ref.get(events), []);
        assert.deepEqual(
          yield* Ref.get(order),
          mode === "prior_unbound"
            ? []
            : mode === "prepare_unavailable"
              ? ["original", "capture", "prepare", "hold"]
              : ["original", "capture", "prepare", "hold", "close", "outcome"],
        );
        assert.equal((yield* Ref.get(holds)).length, 1);
      }),
  );
}

for (const mode of [
  "completed",
  "retained",
  "unretired",
  "missing",
  "unavailable",
  "prior_unbound",
  "prior_bound",
] as const) {
  it.effect(`verifies worktree cleanup ${mode} without generic settlement or replay`, () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const timestamp = DateTime.formatIso(now);
      const workerId = "owned-worktree-cleanup-worker";
      const commandId = CommandId.make(`command:owned-worktree:${mode}`);
      const effect: EffectOutbox.OrchestrationEffectV2 = {
        ...restartEffect(now, { type: "detach" }),
        commandId,
        id: EventSink.deletionWorktreeEffectIdV1(commandId, threadId),
        request: { type: "worktree.cleanup" },
        leaseOwner: workerId,
        leaseExpiresAt: DateTime.formatIso(DateTime.add(now, { minutes: 1 })),
      };
      const projectId = ProjectId.make("owned-worktree-project");
      const consent = {
        projectId,
        path: "/workspace/owned-worktree",
        branch: "owned-worktree-branch",
        force: true as const,
      };
      const subject = {
        version: 1 as const,
        effectId: effect.id,
        threadId,
        leaseInventory: { status: "absent" as const },
        ownerBirth: {
          kind: "application_v2_thread_birth" as const,
          threadId,
          eventId: EventId.make("owned-worktree-birth"),
          sequence: 1,
        },
        deletion: { commandId, eventId: EventId.make("owned-worktree-delete"), sequence: 2 },
        task: {
          kind: "worktree" as const,
          canonicalCommand: {
            type: "thread.delete" as const,
            commandId,
            threadId,
            worktreeRemoval: consent,
          },
          commandDigest: "a".repeat(64),
          consent,
          worktree: { projectId, path: consent.path, branch: consent.branch },
          projectRoot: "/workspace",
          prerequisiteEffectIds: [],
          captureStatus: "captured" as const,
          reason: null,
        },
      };
      const task = {
        ...subject,
        bindingSha256: EventSink.deletionWorktreeTaskBindingDigestV1(subject),
        recordedAt: timestamp,
      };
      const start: EventSink.DeletionWorktreeRemovalStartV1 = {
        schema: "t3.deletion-worktree-removal-start/v1",
        effectId: effect.id,
        bindingSha256: task.bindingSha256,
        workerId,
        expectedAttempt: effect.attemptCount,
        target: {
          projectId,
          projectRoot: "/workspace",
          path: consent.path,
          branch: consent.branch,
          force: true,
        },
        startedAt: timestamp,
      };
      const readback = {
        registration: {
          status: "complete" as const,
          projectRoot: "/workspace",
          gitCommonDirectory: "/workspace/.git",
          entries: [],
        },
        filesystem: { status: "absent" as const, path: consent.path },
      };
      const observation: EventSink.DeletionCleanupObservationV1 = {
        version: 1,
        start,
        startOrdinal: 0,
        operation: { kind: "already_absent", completion: "not_invoked" },
        before: readback,
        after: readback,
        observedAt: timestamp,
      };
      const outcome: EventSink.LeaseCleanupTaskOutcomeV2 = {
        taskId: effect.id,
        result: "succeeded",
        effect: "absent",
      };
      const row: EventSink.DeletionCleanupTaskOutcomeRowV1 = {
        ordinal: 1,
        outcome,
        correlation: {
          workerId,
          expectedAttempt: effect.attemptCount,
          bindingSha256: task.bindingSha256,
          evidence: {
            version: 1,
            schema: "t3.deletion-cleanup-observation/v1",
            producer: "worktree",
            observation,
            coveredHolds: [],
          },
        },
        recordedAt: timestamp,
      };
      const events = yield* Ref.make<ReadonlyArray<string>>([]);
      const current = yield* Ref.make(effect);
      const bound: EffectOutbox.ResourceCleanupUnknownEvidenceV1 = {
        version: 1,
        kind: "resource_cleanup",
        operationId: effect.id,
        threadId,
        taskKind: "worktree",
        bindingSha256: task.bindingSha256,
        outcome: "unknown",
      };
      const unbound: EffectOutbox.ResourceCleanupUnknownEvidenceV1 = {
        ...bound,
        bindingSha256: null,
        reason: "task_binding_unavailable",
      };
      const makeHold = (
        evidence: EffectOutbox.ResourceCleanupUnknownEvidenceV1,
      ): EffectOutbox.UnknownEffectHoldV2 => ({
        effectId: effect.id,
        threadId,
        workerId,
        operationId: effect.id,
        expectedAttempt: effect.attemptCount,
        heldAt: timestamp,
        evidence,
      });
      const holds = yield* Ref.make<ReadonlyArray<EffectOutbox.UnknownEffectHoldV2>>(
        mode === "prior_unbound"
          ? [makeHold(unbound)]
          : mode === "prior_bound"
            ? [makeHold(bound)]
            : [],
      );
      const calls = yield* Ref.make(0);
      const outboxLayer = Layer.mock(EffectOutbox.EffectOutboxV2)({
        claimNext: () => Effect.succeed(Option.some(effect)),
        get: () => Ref.get(current).pipe(Effect.map(Option.some)),
        listHeldByThreadId: () => Ref.get(holds),
        awaitCancellation: () => Effect.never,
        clearCancellation: () => Effect.void,
        holdResourceCleanupUnknown: (input) =>
          Effect.gen(function* () {
            assert.equal(input.effectId, effect.id);
            assert.equal(input.workerId, workerId);
            assert.equal(input.expectedAttempt, effect.attemptCount);
            assert.deepEqual(
              input.evidence,
              mode === "missing" || mode === "unavailable" ? unbound : bound,
            );
            if ((yield* Ref.get(holds)).length > 0) return false;
            yield* Ref.set(holds, [makeHold(input.evidence)]);
            return true;
          }),
        succeed: () => Effect.die("Worktree cleanup must not call generic succeed"),
        retry: () => Effect.die("Worktree cleanup must not retry"),
        fail: () => Effect.die("Worktree cleanup must not fail"),
      });
      const eventSinkLayer = Layer.mock(EventSink.EventSinkV2)({
        readDeletionWorktreeTask: () =>
          mode === "unavailable"
            ? Effect.fail(
                new EventSink.EventSinkWriteError({ eventCount: 0, cause: "task unavailable" }),
              )
            : Effect.succeed(mode === "missing" ? null : task),
        readDeletionCleanupTaskOutcome: () => Effect.succeed(row),
        readDeletionWorktreeExecutionBasis: () =>
          Ref.get(current).pipe(
            Effect.map((value): EventSink.DeletionWorktreeExecutionBasisV1 => ({
              binding: task,
              effect: value,
              currentLease: "absent",
              inventoryComplete: true,
              prerequisitesReady: true,
              prerequisites: [],
              latestOutcome: { ordinal: 1, outcome },
              start: mode === "prior_bound" ? null : { ordinal: 0, evidence: start },
              held: false,
              reason: null,
              admission: {
                operationId: effect.id,
                path: consent.path,
                kind: "worktree_removal",
                subject: {
                  bindingSha256: task.bindingSha256,
                  effectId: effect.id,
                  threadId,
                  commandId,
                },
                state: mode === "unretired" ? "completed" : "released",
                startedAt: timestamp,
                outcome: {
                  schema: "t3.deletion-worktree-retirement/v1",
                  effectId: effect.id,
                  bindingSha256: task.bindingSha256,
                  qualifiedOrdinal: 1,
                  originalLeaseInventory: task.leaseInventory,
                  finalizedAt: timestamp,
                },
                recordedAt: timestamp,
                updatedAt: timestamp,
              },
            })),
          ),
      });
      const executor = makeExecutorLayer({
        events,
        outboxLayer,
        eventSinkLayer,
        resourceCleanup: {
          cleanupOwnedWorktree: (input) =>
            Effect.gen(function* () {
              assert.deepEqual(input, {
                effectId: effect.id,
                bindingSha256: task.bindingSha256,
                workerId,
                expectedAttempt: effect.attemptCount,
              });
              assert.deepEqual(yield* Ref.get(holds), []);
              yield* Ref.update(calls, (count) => count + 1);
              if (mode === "completed")
                yield* Ref.set(current, {
                  ...effect,
                  status: "succeeded",
                  completedAt: timestamp,
                  leaseOwner: null,
                  leaseExpiresAt: null,
                });
              return {
                status: mode === "retained" ? ("retained" as const) : ("completed" as const),
                effectId: effect.id,
                bindingSha256: task.bindingSha256,
                expectedLatestOrdinal: mode === "retained" ? null : 1,
                reason: mode === "retained" ? "prerequisites_unavailable" : null,
              };
            }),
        },
      });
      assert.isTrue(
        yield* EffectWorker.OrchestrationEffectWorkerV2.pipe(
          Effect.flatMap((worker) => worker.runOnce),
          Effect.provide(
            EffectWorker.layerWithOptions({ workerId }).pipe(
              Layer.provide(Layer.merge(executor, outboxLayer)),
            ),
          ),
        ),
      );
      assert.equal(
        yield* Ref.get(calls),
        ["completed", "retained", "unretired"].includes(mode) ? 1 : 0,
      );
      assert.equal(
        (yield* Ref.get(current)).status,
        mode === "completed" ? "succeeded" : "running",
      );
      assert.equal((yield* Ref.get(holds)).length, mode === "completed" ? 0 : 1);
      assert.deepEqual(yield* Ref.get(events), []);
    }),
  );
}

it("does not retry pure interrupt races where the turn is already gone", () => {
  assert.isTrue(
    EffectWorker.isNonRetryableProviderTurnControlFailure(
      "provider-turn.interrupt",
      "ProviderAdapterInterruptError: ... ACP provider turn provider-turn:x is not active",
    ),
  );
  assert.isTrue(
    EffectWorker.isNonRetryableProviderTurnControlFailure(
      "provider-turn.interrupt",
      "Provider session provider-session:x is not active.",
    ),
  );
  // Restart is compound (interrupt + detach + start). Do not swallow start failures.
  assert.isFalse(
    EffectWorker.isNonRetryableProviderTurnControlFailure(
      "provider-turn.restart",
      "Provider session provider-session:x is not active.",
    ),
  );
  assert.isFalse(
    EffectWorker.isNonRetryableProviderTurnControlFailure(
      "provider-turn.start",
      "Provider session provider-session:x is not active.",
    ),
  );
  assert.isFalse(
    EffectWorker.isNonRetryableProviderTurnControlFailure(
      "provider-turn.interrupt",
      "ACP hard teardown failed unexpectedly; the session is poisoned",
    ),
  );
});

it.effect("requeues a claim when a pre-execution worker check fails", () =>
  Effect.gen(function* () {
    const now = DateTime.formatIso(yield* DateTime.now);
    const effectId = "effect:worker-pre-execution-failure";
    const workerId = "worker-pre-execution-failure";
    const claimedEffect: EffectOutbox.OrchestrationEffectV2 = {
      id: effectId,
      commandId: CommandId.make("command:worker-pre-execution-failure"),
      threadId: ThreadId.make("thread:worker-pre-execution-failure"),
      request: { type: "terminal.cleanup" },
      status: "running",
      attemptCount: 1,
      availableAt: now,
      leaseOwner: workerId,
      leaseExpiresAt: now,
      createdAt: now,
      updatedAt: now,
      completedAt: null,
      lastError: null,
    };
    const retries = yield* Ref.make<
      ReadonlyArray<{
        readonly effectId: string;
        readonly workerId: string;
        readonly error: string;
        readonly delayMs: number;
      }>
    >([]);
    const executionCount = yield* Ref.make(0);
    const outboxLayer = Layer.mock(EffectOutbox.EffectOutboxV2)({
      claimNext: () => Effect.succeed(Option.some(claimedEffect)),
      get: () =>
        Effect.fail(
          new EffectOutbox.EffectOutboxError({
            operation: "get",
            effectId,
            cause: "simulated cancellation-state read failure",
          }),
        ),
      retry: (input) =>
        Ref.update(retries, (existing) => [...existing, input]).pipe(Effect.as(true)),
    });
    const executorLayer = Layer.succeed(
      EffectWorker.OrchestrationEffectExecutorV2,
      EffectWorker.OrchestrationEffectExecutorV2.of({
        execute: () => Ref.update(executionCount, (count) => count + 1),
      }),
    );
    const workerLayer = EffectWorker.layerWithOptions({ workerId }).pipe(
      Layer.provide(Layer.merge(outboxLayer, executorLayer)),
    );

    const exit = yield* EffectWorker.OrchestrationEffectWorkerV2.pipe(
      Effect.flatMap((worker) => worker.runOnce),
      Effect.provide(workerLayer),
      Effect.exit,
    );

    assert.isTrue(Exit.isFailure(exit));
    if (Exit.isFailure(exit)) {
      assert.include(Cause.pretty(exit.cause), "simulated cancellation-state read failure");
    }
    assert.equal(yield* Ref.get(executionCount), 0);
    const retry = (yield* Ref.get(retries))[0];
    assert.isDefined(retry);
    assert.equal(retry.effectId, effectId);
    assert.equal(retry.workerId, workerId);
    assert.equal(retry.delayMs, 0);
    assert.include(retry.error, "simulated cancellation-state read failure");
  }),
);

it.effect("arms cancellation before the durable pre-execution check", () =>
  Effect.gen(function* () {
    const now = DateTime.formatIso(yield* DateTime.now);
    const effectId = "effect:worker-cancellation-registration-race";
    const workerId = "worker-cancellation-registration-race";
    const claimedEffect: EffectOutbox.OrchestrationEffectV2 = {
      id: effectId,
      commandId: CommandId.make("command:worker-cancellation-registration-race"),
      threadId: ThreadId.make("thread:worker-cancellation-registration-race"),
      request: { type: "terminal.cleanup" },
      status: "running",
      attemptCount: 1,
      availableAt: now,
      leaseOwner: workerId,
      leaseExpiresAt: now,
      createdAt: now,
      updatedAt: now,
      completedAt: null,
      lastError: null,
    };
    const signal = yield* Deferred.make<void>();
    let cancellationArmed = false;
    const executionCount = yield* Ref.make(0);
    const settlementCount = yield* Ref.make(0);
    const outboxLayer = Layer.mock(EffectOutbox.EffectOutboxV2)({
      claimNext: () => Effect.succeed(Option.some(claimedEffect)),
      awaitCancellation: () => {
        cancellationArmed = true;
        return Deferred.await(signal);
      },
      get: () =>
        Effect.gen(function* () {
          // Model a cancellation commit immediately after this durable read
          // took its snapshot. Its process-local signal is only delivered when
          // the worker registered the waiter before starting the read.
          if (cancellationArmed) {
            yield* Deferred.succeed(signal, undefined);
          }
          return Option.some(claimedEffect);
        }),
      clearCancellation: () => Effect.void,
      succeed: () => Ref.update(settlementCount, (count) => count + 1).pipe(Effect.as(true)),
    });
    const executorLayer = Layer.succeed(
      EffectWorker.OrchestrationEffectExecutorV2,
      EffectWorker.OrchestrationEffectExecutorV2.of({
        execute: () =>
          Effect.yieldNow.pipe(Effect.andThen(Ref.update(executionCount, (count) => count + 1))),
      }),
    );
    const workerLayer = EffectWorker.layerWithOptions({ workerId }).pipe(
      Layer.provide(Layer.merge(outboxLayer, executorLayer)),
    );

    const exit = yield* EffectWorker.OrchestrationEffectWorkerV2.pipe(
      Effect.flatMap((worker) => worker.runOnce),
      Effect.provide(workerLayer),
      Effect.exit,
    );

    if (Exit.isFailure(exit)) {
      assert.fail(Cause.pretty(exit.cause));
    }
    assert.equal(yield* Ref.get(executionCount), 0);
    assert.equal(yield* Ref.get(settlementCount), 0);
  }),
);

it.effect("terminalizes a process-bound claim when success settlement fails", () =>
  Effect.gen(function* () {
    const now = DateTime.formatIso(yield* DateTime.now);
    const effectId = "effect:worker-process-bound-settlement-failure";
    const workerId = "worker-process-bound-settlement-failure";
    const claimedEffect: EffectOutbox.OrchestrationEffectV2 = {
      id: effectId,
      commandId: CommandId.make("command:worker-process-bound-settlement-failure"),
      threadId: ThreadId.make("thread:worker-process-bound-settlement-failure"),
      request: {
        type: "provider-turn.start",
        runId: RunId.make("run:worker-process-bound-settlement-failure"),
      },
      status: "running",
      attemptCount: 1,
      availableAt: now,
      leaseOwner: workerId,
      leaseExpiresAt: now,
      createdAt: now,
      updatedAt: now,
      completedAt: null,
      lastError: null,
    };
    const retries = yield* Ref.make(0);
    const terminalErrors = yield* Ref.make<ReadonlyArray<string>>([]);
    const executionCount = yield* Ref.make(0);
    const outboxLayer = Layer.mock(EffectOutbox.EffectOutboxV2)({
      claimNext: () => Effect.succeed(Option.some(claimedEffect)),
      get: () => Effect.succeed(Option.some(claimedEffect)),
      awaitCancellation: () => Effect.never,
      clearCancellation: () => Effect.void,
      succeed: () =>
        Effect.fail(
          new EffectOutbox.EffectOutboxError({
            operation: "succeed",
            effectId,
            cause: "simulated success settlement failure",
          }),
        ),
      retry: () => Ref.update(retries, (count) => count + 1).pipe(Effect.as(true)),
      fail: ({ error }) =>
        Ref.update(terminalErrors, (existing) => [...existing, error]).pipe(Effect.as(true)),
    });
    const executorLayer = Layer.succeed(
      EffectWorker.OrchestrationEffectExecutorV2,
      EffectWorker.OrchestrationEffectExecutorV2.of({
        execute: () => Ref.update(executionCount, (count) => count + 1),
      }),
    );
    const workerLayer = EffectWorker.layerWithOptions({ workerId }).pipe(
      Layer.provide(Layer.merge(outboxLayer, executorLayer)),
    );

    const exit = yield* EffectWorker.OrchestrationEffectWorkerV2.pipe(
      Effect.flatMap((worker) => worker.runOnce),
      Effect.provide(workerLayer),
      Effect.exit,
    );

    assert.isTrue(Exit.isFailure(exit));
    assert.equal(yield* Ref.get(executionCount), 1);
    assert.equal(yield* Ref.get(retries), 0);
    const terminalError = (yield* Ref.get(terminalErrors))[0];
    assert.isDefined(terminalError);
    assert.include(terminalError, "after execution started");
    assert.include(terminalError, "simulated success settlement failure");
  }),
);

it.effect("requeues a replay-safe claim when success settlement fails", () =>
  Effect.gen(function* () {
    const now = DateTime.formatIso(yield* DateTime.now);
    const effectId = "effect:worker-replay-safe-settlement-failure";
    const workerId = "worker-replay-safe-settlement-failure";
    const claimedEffect: EffectOutbox.OrchestrationEffectV2 = {
      id: effectId,
      commandId: CommandId.make("command:worker-replay-safe-settlement-failure"),
      threadId: ThreadId.make("thread:worker-replay-safe-settlement-failure"),
      request: { type: "terminal.cleanup" },
      status: "running",
      attemptCount: 1,
      availableAt: now,
      leaseOwner: workerId,
      leaseExpiresAt: now,
      createdAt: now,
      updatedAt: now,
      completedAt: null,
      lastError: null,
    };
    const retries = yield* Ref.make(0);
    const terminalizations = yield* Ref.make(0);
    const outboxLayer = Layer.mock(EffectOutbox.EffectOutboxV2)({
      claimNext: () => Effect.succeed(Option.some(claimedEffect)),
      get: () => Effect.succeed(Option.some(claimedEffect)),
      awaitCancellation: () => Effect.never,
      clearCancellation: () => Effect.void,
      succeed: () =>
        Effect.fail(
          new EffectOutbox.EffectOutboxError({
            operation: "succeed",
            effectId,
            cause: "simulated replay-safe settlement failure",
          }),
        ),
      retry: () => Ref.update(retries, (count) => count + 1).pipe(Effect.as(true)),
      fail: () => Ref.update(terminalizations, (count) => count + 1).pipe(Effect.as(true)),
    });
    const executorLayer = Layer.succeed(
      EffectWorker.OrchestrationEffectExecutorV2,
      EffectWorker.OrchestrationEffectExecutorV2.of({ execute: () => Effect.void }),
    );
    const workerLayer = EffectWorker.layerWithOptions({ workerId }).pipe(
      Layer.provide(Layer.merge(outboxLayer, executorLayer)),
    );

    const exit = yield* EffectWorker.OrchestrationEffectWorkerV2.pipe(
      Effect.flatMap((worker) => worker.runOnce),
      Effect.provide(workerLayer),
      Effect.exit,
    );

    assert.isTrue(Exit.isFailure(exit));
    assert.equal(yield* Ref.get(retries), 1);
    assert.equal(yield* Ref.get(terminalizations), 0);
  }),
);

it.effect("keeps a process-bound executor failure retryable when retry settlement fails", () =>
  Effect.gen(function* () {
    const now = DateTime.formatIso(yield* DateTime.now);
    const effectId = "effect:worker-process-bound-retry-settlement-failure";
    const workerId = "worker-process-bound-retry-settlement-failure";
    const claimedEffect: EffectOutbox.OrchestrationEffectV2 = {
      id: effectId,
      commandId: CommandId.make("command:worker-process-bound-retry-settlement-failure"),
      threadId: ThreadId.make("thread:worker-process-bound-retry-settlement-failure"),
      request: {
        type: "provider-turn.start",
        runId: RunId.make("run:worker-process-bound-retry-settlement-failure"),
      },
      status: "running",
      attemptCount: 1,
      availableAt: now,
      leaseOwner: workerId,
      leaseExpiresAt: now,
      createdAt: now,
      updatedAt: now,
      completedAt: null,
      lastError: null,
    };
    const retryAttempts = yield* Ref.make(0);
    const terminalizations = yield* Ref.make(0);
    const outboxLayer = Layer.mock(EffectOutbox.EffectOutboxV2)({
      claimNext: () => Effect.succeed(Option.some(claimedEffect)),
      get: () => Effect.succeed(Option.some(claimedEffect)),
      awaitCancellation: () => Effect.never,
      clearCancellation: () => Effect.void,
      retry: () =>
        Ref.updateAndGet(retryAttempts, (count) => count + 1).pipe(
          Effect.flatMap((attempt) =>
            attempt === 1
              ? Effect.fail(
                  new EffectOutbox.EffectOutboxError({
                    operation: "retry",
                    effectId,
                    cause: "simulated retry settlement failure",
                  }),
                )
              : Effect.succeed(true),
          ),
        ),
      fail: () => Ref.update(terminalizations, (count) => count + 1).pipe(Effect.as(true)),
    });
    const executorLayer = Layer.succeed(
      EffectWorker.OrchestrationEffectExecutorV2,
      EffectWorker.OrchestrationEffectExecutorV2.of({
        execute: () =>
          Effect.fail(
            new EffectWorker.OrchestrationEffectExecutionError({
              effectId,
              effectType: claimedEffect.request.type,
              cause: "simulated provider execution failure",
            }),
          ),
      }),
    );
    const workerLayer = EffectWorker.layerWithOptions({ workerId }).pipe(
      Layer.provide(Layer.merge(outboxLayer, executorLayer)),
    );

    const exit = yield* EffectWorker.OrchestrationEffectWorkerV2.pipe(
      Effect.flatMap((worker) => worker.runOnce),
      Effect.provide(workerLayer),
      Effect.exit,
    );

    assert.isTrue(Exit.isFailure(exit));
    assert.equal(yield* Ref.get(retryAttempts), 2);
    assert.equal(yield* Ref.get(terminalizations), 0);
  }),
);

it.effect("keeps a max-attempt replay-safe failure terminal when fail settlement fails", () =>
  Effect.gen(function* () {
    const now = DateTime.formatIso(yield* DateTime.now);
    const effectId = "effect:worker-replay-safe-terminal-settlement-failure";
    const workerId = "worker-replay-safe-terminal-settlement-failure";
    const claimedEffect: EffectOutbox.OrchestrationEffectV2 = {
      id: effectId,
      commandId: CommandId.make("command:worker-replay-safe-terminal-settlement-failure"),
      threadId: ThreadId.make("thread:worker-replay-safe-terminal-settlement-failure"),
      request: { type: "terminal.cleanup" },
      status: "running",
      attemptCount: 5,
      availableAt: now,
      leaseOwner: workerId,
      leaseExpiresAt: now,
      createdAt: now,
      updatedAt: now,
      completedAt: null,
      lastError: null,
    };
    const failAttempts = yield* Ref.make(0);
    const retries = yield* Ref.make(0);
    const outboxLayer = Layer.mock(EffectOutbox.EffectOutboxV2)({
      claimNext: () => Effect.succeed(Option.some(claimedEffect)),
      get: () => Effect.succeed(Option.some(claimedEffect)),
      awaitCancellation: () => Effect.never,
      clearCancellation: () => Effect.void,
      fail: () =>
        Ref.updateAndGet(failAttempts, (count) => count + 1).pipe(
          Effect.flatMap((attempt) =>
            attempt === 1
              ? Effect.fail(
                  new EffectOutbox.EffectOutboxError({
                    operation: "fail",
                    effectId,
                    cause: "simulated terminal settlement failure",
                  }),
                )
              : Effect.succeed(true),
          ),
        ),
      retry: () => Ref.update(retries, (count) => count + 1).pipe(Effect.as(true)),
    });
    const executorLayer = Layer.succeed(
      EffectWorker.OrchestrationEffectExecutorV2,
      EffectWorker.OrchestrationEffectExecutorV2.of({
        execute: () =>
          Effect.fail(
            new EffectWorker.OrchestrationEffectExecutionError({
              effectId,
              effectType: claimedEffect.request.type,
              cause: "simulated terminal cleanup failure",
            }),
          ),
      }),
    );
    const workerLayer = EffectWorker.layerWithOptions({ workerId, maxAttempts: 5 }).pipe(
      Layer.provide(Layer.merge(outboxLayer, executorLayer)),
    );

    const exit = yield* EffectWorker.OrchestrationEffectWorkerV2.pipe(
      Effect.flatMap((worker) => worker.runOnce),
      Effect.provide(workerLayer),
      Effect.exit,
    );

    assert.isTrue(Exit.isFailure(exit));
    assert.equal(yield* Ref.get(failAttempts), 2);
    assert.equal(yield* Ref.get(retries), 0);
  }),
);

it.effect("uses durable deadlines, notifications, and a slow liveness poll", () =>
  Effect.gen(function* () {
    const attempts = yield* Ref.make(0);
    const available = yield* Queue.unbounded<void>();
    const now = yield* DateTime.now;
    const nextClaimableAt = yield* Ref.make<Option.Option<DateTime.Utc>>(
      Option.some(DateTime.add(now, { milliseconds: 100 })),
    );
    const worker = EffectWorker.OrchestrationEffectWorkerV2.of({
      awaitWork: Queue.take(available),
      runRecoveryOnce: Effect.succeed(false),
      runOnce: Effect.gen(function* () {
        const count = yield* Ref.updateAndGet(attempts, (current) => current + 1);
        if (count === 2) {
          yield* Ref.set(nextClaimableAt, Option.some(DateTime.add(now, { milliseconds: 5_000 })));
        }
        if (count === 3) yield* Ref.set(nextClaimableAt, Option.none());
        return false;
      }),
      nextClaimableAt: Ref.get(nextClaimableAt),
      drain: () => Effect.succeed(0),
    });
    const awaitAttempts = Effect.fnUntraced(function* (expected: number) {
      while ((yield* Ref.get(attempts)) < expected) {
        yield* Effect.yieldNow;
      }
    });

    yield* EffectWorker.runDaemonWithOptions({
      concurrency: 1,
      livenessPollIntervalMs: 1_000,
    }).pipe(
      Effect.provideService(EffectWorker.OrchestrationEffectWorkerV2, worker),
      Effect.forkScoped,
    );

    yield* awaitAttempts(1);
    yield* TestClock.adjust("99 millis");
    assert.equal(yield* Ref.get(attempts), 1);

    yield* TestClock.adjust("1 millis");
    yield* awaitAttempts(2);
    yield* TestClock.adjust("999 millis");
    assert.equal(yield* Ref.get(attempts), 2);

    yield* Queue.offer(available, undefined);
    yield* awaitAttempts(3);
    yield* TestClock.adjust("999 millis");
    assert.equal(yield* Ref.get(attempts), 3);

    yield* TestClock.adjust("1 millis");
    yield* awaitAttempts(4);
  }).pipe(Effect.provide(TestClock.layer())),
);

it.effect("does not hot-loop when a claim fails", () =>
  Effect.gen(function* () {
    const attempts = yield* Ref.make(0);
    const now = yield* DateTime.now;
    const worker = EffectWorker.OrchestrationEffectWorkerV2.of({
      awaitWork: Effect.never,
      runRecoveryOnce: Effect.succeed(false),
      runOnce: Ref.update(attempts, (count) => count + 1).pipe(
        Effect.andThen(
          new EffectWorker.OrchestrationEffectWorkerError({
            operation: "claim",
            cause: "simulated database failure",
          }),
        ),
      ),
      nextClaimableAt: Effect.succeed(Option.some(now)),
      drain: () => Effect.succeed(0),
    });

    yield* EffectWorker.runDaemonWithOptions({
      concurrency: 1,
      livenessPollIntervalMs: 1_000,
    }).pipe(
      Effect.provideService(EffectWorker.OrchestrationEffectWorkerV2, worker),
      Effect.forkScoped,
    );

    while ((yield* Ref.get(attempts)) < 1) yield* Effect.yieldNow;
    yield* TestClock.adjust("999 millis");
    assert.equal(yield* Ref.get(attempts), 1);
    yield* TestClock.adjust("1 millis");
    while ((yield* Ref.get(attempts)) < 2) yield* Effect.yieldNow;
  }).pipe(Effect.provide(TestClock.layer())),
);

it.effect("backs off briefly when a due deadline loses a claim race", () =>
  Effect.gen(function* () {
    const attempts = yield* Ref.make(0);
    const now = yield* DateTime.now;
    const worker = EffectWorker.OrchestrationEffectWorkerV2.of({
      awaitWork: Effect.never,
      runRecoveryOnce: Effect.succeed(false),
      runOnce: Ref.update(attempts, (count) => count + 1).pipe(Effect.as(false)),
      nextClaimableAt: Effect.succeed(Option.some(now)),
      drain: () => Effect.succeed(0),
    });

    yield* EffectWorker.runDaemonWithOptions({
      concurrency: 1,
      livenessPollIntervalMs: 1_000,
    }).pipe(
      Effect.provideService(EffectWorker.OrchestrationEffectWorkerV2, worker),
      Effect.forkScoped,
    );

    while ((yield* Ref.get(attempts)) < 1) yield* Effect.yieldNow;
    yield* TestClock.adjust("24 millis");
    assert.equal(yield* Ref.get(attempts), 1);
    yield* TestClock.adjust("1 millis");
    while ((yield* Ref.get(attempts)) < 2) yield* Effect.yieldNow;
  }).pipe(Effect.provide(TestClock.layer())),
);

it.effect("safely retries after replacement cleanup succeeds and start fails", () =>
  Effect.gen(function* () {
    const now = yield* DateTime.now;
    const events = yield* Ref.make<ReadonlyArray<string>>([]);
    const failFirstStart = yield* Ref.make(true);
    const effect = restartEffect(now, {
      type: "replace",
      replacementProviderSessionId: replacementSessionId,
    });
    const layer = makeExecutorLayer({ events, failFirstStart });

    const first = yield* Effect.gen(function* () {
      const executor = yield* EffectWorker.OrchestrationEffectExecutorV2;
      return yield* Effect.exit(executor.execute(effect));
    }).pipe(Effect.provide(layer));
    assert.isTrue(Exit.isFailure(first));

    yield* Effect.gen(function* () {
      const executor = yield* EffectWorker.OrchestrationEffectExecutorV2;
      yield* executor.execute(effect);
    }).pipe(Effect.provide(layer));

    assert.deepEqual(yield* Ref.get(events), [
      `interrupt:${replacementSessionId}`,
      "detach",
      "start",
      `interrupt:${replacementSessionId}`,
      "detach",
      "start",
    ]);
  }),
);

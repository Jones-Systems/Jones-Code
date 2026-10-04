import { assert, it, vi } from "@effect/vitest";
import {
  CheckpointId,
  CommandId,
  CheckpointRef,
  CheckpointScopeId,
  EventId,
  MessageId,
  NodeId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Run,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import * as CheckpointStore from "../checkpointing/CheckpointStore.ts";
import { VcsProcessTimeoutError } from "@t3tools/contracts";
import * as CheckpointService from "./CheckpointService.ts";
import * as CheckpointCaptureService from "./CheckpointCaptureService.ts";
import * as EventSink from "./EventSink.ts";
import * as IdAllocator from "./IdAllocator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as OrdinaryCheckout from "./OrdinaryCheckoutOwnership.ts";
import * as RunFinalization from "./RunFinalizationService.ts";

const ProjectionStoreTestLayer = Layer.mergeAll(
  ProjectionStore.layer.pipe(Layer.provideMerge(SqlitePersistenceMemory)),
  SqlitePersistenceMemory,
);

const threadId = ThreadId.make("thread:checkpoint-capture-delegated");
const projectId = ProjectId.make("project:checkpoint-capture-delegated");
const runId = RunId.make("run:checkpoint-capture-delegated");
const scopeId = CheckpointScopeId.make("scope:checkpoint-capture-delegated");
const rootNodeId = NodeId.make("node:checkpoint-capture-delegated-root");
const taskId = NodeId.make("node:checkpoint-capture-task");
const deliveryMessageId = MessageId.make("message:checkpoint-capture-delivery");
const providerThreadId = ProviderThreadId.make("provider-thread:checkpoint-capture-delegated");
const providerInstanceId = ProviderInstanceId.make("codex");
const driver = ProviderDriverKind.make("codex");
const modelSelection = {
  instanceId: providerInstanceId,
  model: "gpt-5.4",
} as const;

function ordinaryCaptureFixture(now: DateTime.Utc) {
  const commandId = CommandId.make("command:ordinary-capture");
  const birth = {
    kind: "application_v2_thread_birth" as const,
    threadId,
    eventId: EventId.make("event:ordinary-capture-birth"),
    sequence: 1,
  };
  const lease = {
    resourcePath: "/repo",
    leaseId: "lease:ordinary-capture",
    ownerThreadId: threadId,
    ownerIncarnation: OrdinaryCheckout.ordinaryApplicationIncarnationV1(birth),
    branch: null,
    acquiredAtMs: 1,
    renewedAtMs: 1,
    expiresAtMs: 9999999999999,
  };
  const canonicalCommand = { commandId, threadId, type: "run.start" };
  const capture = {
    version: 1 as const,
    commandId,
    commandType: canonicalCommand.type,
    canonicalCommand,
    commandDigest: OrdinaryCheckout.ordinaryCheckoutCommandDigestV1(canonicalCommand),
    origin: { kind: "command" as const },
    threadId,
    applicationBirth: birth,
    projectId,
    canonicalProjectRoot: "/repo",
    canonicalCheckoutPath: "/repo",
    branch: null,
    lease,
  };
  const admission: OrdinaryCheckout.OrdinaryCheckoutAdmissionV1 = {
    version: 1,
    admissionId: OrdinaryCheckout.ordinaryCheckoutAdmissionIdV1(capture),
    capture,
    receipt: {
      commandId,
      threadId,
      commandType: capture.commandType,
      acceptedAt: now,
      resultSequence: 2,
      status: "accepted",
      error: null,
    },
    eventBasis: [
      {
        eventId: EventId.make("event:ordinary-capture-run"),
        sequence: 2,
        threadId,
        commandId,
        eventType: "run.created",
      },
    ],
    run: {
      runId,
      runAttemptId: "attempt:ordinary-capture",
      nodeId: rootNodeId,
      messageId: MessageId.make("message:ordinary-capture"),
    },
    recordedAt: now,
  };
  const reference = OrdinaryCheckout.ordinaryCheckoutAdmissionRefV1(admission);
  const use: OrdinaryCheckout.OrdinaryCheckoutUseV1 = {
    version: 1,
    kind: "ordinary_checkout_use",
    operationId: "operation:ordinary-capture",
    admission: reference,
    lease,
    source: {
      kind: "outbox",
      workerId: "worker:ordinary-capture",
      expectedAttempt: 1,
      leaseExpiresAt: DateTime.add(now, { hours: 1 }),
      link: {
        version: 1,
        effectId: "effect:ordinary-capture",
        commandId,
        threadId,
        requestSha256: "b".repeat(64),
        admission: reference,
        recordedAt: now,
      },
    },
  };
  return { admission, use };
}

it.layer(ProjectionStoreTestLayer)("CheckpointCaptureServiceV2", (it) => {
  it.effect.each([
    { refLookupFails: false, ordinary: "none" },
    { refLookupFails: true, ordinary: "none" },
    { refLookupFails: false, ordinary: "allowed" },
    { refLookupFails: false, ordinary: "stale_admission" },
    { refLookupFails: false, ordinary: "claim_mismatch" },
    { refLookupFails: false, ordinary: "unknown_use" },
    { refLookupFails: false, ordinary: "lifetime" },
    { refLookupFails: false, ordinary: "capture-error" },
    { refLookupFails: false, ordinary: "missing" },
    { refLookupFails: false, ordinary: "deferred-commit" },
    { refLookupFails: false, ordinary: "rolled-back-commit" },
    { refLookupFails: false, ordinary: "no-stored-event" },
    { refLookupFails: false, ordinary: "cached-commit" },
    { refLookupFails: false, ordinary: "refresh-error" },
    { refLookupFails: false, ordinary: "basis-ready" },
    { refLookupFails: false, ordinary: "basis-stale" },
    { refLookupFails: false, ordinary: "basis-cached" },
    { refLookupFails: false, ordinary: "basis-mutated" },
    { refLookupFails: false, ordinary: "basis-wrong-run" },
    { refLookupFails: false, ordinary: "basis-settled" },
  ] as const)(
    "captures without decoding history or losing newer delegated completion, %j",
    ({ refLookupFails, ordinary }) =>
      Effect.gen(function* () {
        const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
        const now = yield* DateTime.now;
        const later = DateTime.add(now, { seconds: 1 });
        const ordinaryFixture = ordinaryCaptureFixture(now);
        const use = ordinary === "none" ? undefined : ordinaryFixture.use;
        const ownershipError = new OrdinaryCheckout.OrdinaryCheckoutOwnershipError({
          reason:
            ordinary === "stale_admission" || ordinary === "claim_mismatch"
              ? ordinary
              : "unknown_use",
          threadId,
          path: "/repo",
          message: "Durable ownership revalidation rejected checkpoint entry.",
        });
        const nativeCapture = vi.fn(() =>
          ordinary === "capture-error"
            ? Effect.fail(
                new VcsProcessTimeoutError({
                  operation: "test.capture",
                  command: "git",
                  cwd: "/repo",
                  timeoutMs: 30000,
                }),
              )
            : Effect.void,
        );
        if (ordinaryFixture.use.source.kind !== "outbox")
          throw new Error("Fixture requires an outbox source.");
        const hasLifetimeExecution =
          ordinary.startsWith("basis-") ||
          [
            "lifetime",
            "capture-error",
            "missing",
            "deferred-commit",
            "rolled-back-commit",
            "no-stored-event",
            "cached-commit",
            "refresh-error",
          ].includes(ordinary);
        const execution = hasLifetimeExecution
          ? OrdinaryCheckout.makeOrdinaryCheckoutExecutionRefV1({
              originalUse: ordinaryFixture.use,
              executor: {
                kind: "actual_outbox_claim",
                source: {
                  ...ordinaryFixture.use.source,
                  workerId: "worker:joined-capture",
                  expectedAttempt: 2,
                  link: { ...ordinaryFixture.use.source.link, effectId: "effect:joined-capture" },
                },
              },
            })
          : undefined;
        const basis: EventSink.OrdinaryFinalCheckpointCompletionBasisV1 | undefined =
          ordinary.startsWith("basis-") && execution !== undefined
            ? {
                version: 1,
                schema: "t3.ordinary-final-checkpoint-basis/v1",
                checkpointExecution: execution,
                effectId: "effect:joined-capture",
                runId: ordinary === "basis-wrong-run" ? RunId.make("run:foreign-capture") : runId,
                scopeId,
                joinOrdinal: 2,
                managedRetirements: [
                  {
                    managedExecution: OrdinaryCheckout.makeOrdinaryCheckoutExecutionRefV1({
                      originalUse: ordinaryFixture.use,
                      executor: {
                        kind: "captured_managed_run",
                        captureId: "capture:retired-native",
                        run: ordinaryFixture.admission.run!,
                        checkpointScopeId: scopeId,
                        driver,
                        binding: {
                          threadId,
                          providerThreadId,
                          instanceId: providerInstanceId,
                          providerSessionId: ProviderSessionId.make("session:retired-native"),
                        },
                      },
                    }),
                    retirementOrdinal: 3,
                    closureSha256: "d".repeat(64),
                  },
                ],
              }
            : undefined;
        const revalidateBasis = vi.fn(
          (actual: EventSink.OrdinaryFinalCheckpointCompletionBasisV1) =>
            Effect.suspend(() => {
              assert.strictEqual(actual, basis);
              return ordinary === "basis-stale"
                ? Effect.fail(ownershipError)
                : Effect.succeed(actual);
            }),
        );
        const publications: Array<Effect.Effect<void>> = [];
        const revalidateExecution = vi.fn(
          (actual: OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1) => {
            assert.strictEqual(actual, execution);
            return Effect.succeed(actual);
          },
        );
        const revalidate = vi.fn((actual: OrdinaryCheckout.OrdinaryCheckoutUseV1) => {
          assert.strictEqual(actual, use);
          assert.isUndefined(
            execution,
            "Transferred executors cannot reuse the original claimant.",
          );
          return ordinary === "allowed"
            ? Effect.succeed({
                subject: {
                  schema: "t3.ordinary-checkout-use/v1" as const,
                  use: actual,
                  source: { projectWorkspaceRoot: "/repo", worktreePath: null },
                },
                state: "started" as const,
                startedAt: DateTime.formatIso(now),
              })
            : Effect.fail(ownershipError);
        });
        const usesActualCheckpointService = refLookupFails || use !== undefined;

        const staleDelegatedCompletion = {
          disposition: "open" as const,
          nextGeneration: 1,
          delivery: null,
        };
        const newerCohort = {
          disposition: "open" as const,
          nextGeneration: 2,
          delivery: {
            generation: 1,
            messageId: deliveryMessageId,
            taskIds: [taskId],
          },
        };
        const staleRun: OrchestrationV2Run = {
          id: runId,
          threadId,
          ordinal: 1,
          providerInstanceId,
          modelSelection,
          providerThreadId,
          userMessageId: MessageId.make("message:checkpoint-capture-user"),
          rootNodeId,
          activeAttemptId: null,
          status: ordinary === "basis-settled" ? "completed" : "waiting",
          requestedAt: now,
          startedAt: now,
          completedAt: null,
          checkpointId:
            ordinary === "basis-settled" ? CheckpointId.make("checkpoint:old-snapshot") : null,
          contextHandoffId: null,
          // Snapshot taken before a concurrent cohort advanced during capture work.
          delegatedCompletion: staleDelegatedCompletion,
        };
        const rootNode = {
          id: rootNodeId,
          threadId,
          runId,
          parentNodeId: null,
          rootNodeId,
          kind: "root_turn" as const,
          status: "waiting" as const,
          countsForRun: true,
          providerThreadId,
          providerTurnId: null,
          nativeItemRef: null,
          runtimeRequestId: null,
          checkpointScopeId: scopeId,
          startedAt: now,
          completedAt: null,
        };
        const scope = {
          id: scopeId,
          threadId,
          runId,
          nodeId: rootNodeId,
          parentScopeId: null,
          providerThreadId,
          kind: "root_run" as const,
          ordinalWithinParent: 0,
          advancesAppRunCount: true,
          cwd: "/repo",
          createdAt: now,
        };
        const providerThread = {
          id: providerThreadId,
          driver,
          providerInstanceId,
          providerSessionId: null,
          appThreadId: threadId,
          ownerNodeId: rootNodeId,
          nativeThreadRef: null,
          nativeConversationHeadRef: null,
          status: "idle" as const,
          firstRunOrdinal: 1,
          lastRunOrdinal: 1,
          handoffIds: [],
          forkedFrom: null,
          createdAt: now,
          updatedAt: now,
        };
        const readyBaseline = {
          id: CheckpointId.make("checkpoint:baseline-0"),
          threadId,
          scopeId,
          runId: null,
          nodeId: rootNodeId,
          parentCheckpointId: null,
          ordinalWithinScope: 0,
          appRunOrdinal: null,
          ref: CheckpointRef.make("checkpoint-ref:baseline-0"),
          status: "ready" as const,
          files: [],
          capturedAt: now,
        };
        const captured = {
          id: CheckpointId.make("checkpoint:captured-1"),
          threadId,
          scopeId,
          runId,
          nodeId: rootNodeId,
          parentCheckpointId: readyBaseline.id,
          ordinalWithinScope: 1,
          appRunOrdinal: 1,
          ref: CheckpointRef.make("checkpoint-ref:captured-1"),
          status: "ready" as const,
          files: [],
          capturedAt: now,
        };

        yield* projectionStore.apply({
          id: EventId.make("event:checkpoint-capture:thread"),
          type: "thread.created",
          threadId,
          occurredAt: now,
          payload: {
            createdBy: "user",
            creationSource: "web",
            id: threadId,
            projectId,
            title: "Checkpoint capture delegated completion",
            providerInstanceId,
            modelSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: null,
            activeProviderThreadId: null,
            lineage: {
              parentThreadId: null,
              relationshipToParent: null,
              rootThreadId: threadId,
            },
            forkedFrom: null,
            createdAt: now,
            updatedAt: now,
            archivedAt: null,
            settledOverride: null,
            settledAt: null,
            lastVisitedAt: null,
            deletedAt: null,
          },
        });
        yield* projectionStore.apply({
          id: EventId.make("event:checkpoint-capture:run-stale"),
          type: "run.updated",
          threadId,
          runId,
          nodeId: rootNodeId,
          providerInstanceId,
          occurredAt: now,
          payload: { ...staleRun, status: "waiting", checkpointId: null },
        });
        yield* projectionStore.apply({
          id: EventId.make("event:checkpoint-capture:node"),
          type: "node.updated",
          threadId,
          runId,
          nodeId: rootNodeId,
          providerInstanceId,
          occurredAt: now,
          payload: rootNode,
        });
        yield* projectionStore.apply({
          id: EventId.make("event:checkpoint-capture:provider-thread"),
          type: "provider-thread.updated",
          threadId,
          nodeId: rootNodeId,
          driver,
          providerInstanceId,
          occurredAt: now,
          payload: providerThread,
        });
        yield* projectionStore.apply({
          id: EventId.make("event:checkpoint-capture:scope"),
          type: "checkpoint-scope.created",
          threadId,
          runId,
          nodeId: rootNodeId,
          occurredAt: now,
          payload: scope,
        });
        yield* projectionStore.apply({
          id: EventId.make("event:checkpoint-capture:baseline"),
          type: "checkpoint.captured",
          threadId,
          nodeId: rootNodeId,
          driver,
          providerInstanceId,
          occurredAt: now,
          payload: readyBaseline,
        });
        const sql = yield* SqlClient.SqlClient;
        yield* sql`INSERT OR REPLACE INTO orchestration_v2_projection_turn_items
          (turn_item_id, thread_id, run_id, node_id, provider_thread_id, provider_turn_id,
            parent_item_id, ordinal, type, status, updated_at, payload_json)
          VALUES ('obsolete-history', ${threadId}, ${runId}, ${rootNodeId}, NULL, NULL,
            NULL, 1, 'assistant_message', 'completed', ${DateTime.formatIso(now)}, '{"obsolete":true}')`;
        yield* sql`UPDATE orchestration_v2_projection_checkpoints
          SET payload_json = json_set(payload_json, '$.files', 'obsolete file summary')
          WHERE checkpoint_id = ${readyBaseline.id}`;
        assert.equal(
          (yield* Effect.exit(projectionStore.getThreadProjection(threadId)))._tag,
          "Failure",
        );

        const committed = yield* Ref.make<ReadonlyArray<OrchestrationV2DomainEvent>>([]);
        const captureLayer = CheckpointCaptureService.layer.pipe(
          Layer.provide(
            Layer.mergeAll(
              IdAllocator.layer,
              usesActualCheckpointService
                ? CheckpointService.layer.pipe(
                    Layer.provide(
                      Layer.mergeAll(
                        IdAllocator.layer,
                        Layer.mock(CheckpointStore.CheckpointStore)({
                          isGitRepository: () => Effect.succeed(ordinary !== "missing"),
                          captureCheckpoint: nativeCapture,
                          hasCheckpointRef: () =>
                            refLookupFails
                              ? Effect.fail(
                                  new VcsProcessTimeoutError({
                                    operation: "test.hasCheckpointRef",
                                    command: "git",
                                    cwd: "/repo",
                                    timeoutMs: 30000,
                                  }),
                                )
                              : Effect.succeed(false),
                        }),
                      ),
                    ),
                  )
                : Layer.mock(CheckpointService.CheckpointServiceV2)({
                    materializeBaselineCheckpoint: () =>
                      Effect.die(
                        "baseline materialization must be skipped when ordinal 0 is ready",
                      ),
                    capture: () => Effect.succeed(captured),
                  }),
              Layer.mock(EventSink.EventSinkV2)({
                readOrdinaryCheckoutAdmissionForRun: () =>
                  Effect.succeed(ordinaryFixture.admission),
                revalidateOrdinaryCheckoutUse: revalidate,
                revalidateOrdinaryCheckoutExecution: revalidateExecution,
                revalidateOrdinaryFinalCheckpointBasis: revalidateBasis,
                withTransaction: (effect) => effect,
                onCommit: (effect) =>
                  ordinary === "deferred-commit" || ordinary === "rolled-back-commit"
                    ? Effect.sync(() => {
                        publications.push(effect);
                      })
                    : effect,
                commitCommand: (input) => {
                  if (execution !== undefined) {
                    assert.strictEqual(input.ordinaryCheckoutExecution, execution);
                    assert.strictEqual(input.ordinaryCheckoutUse, use);
                    assert.strictEqual(input.ordinaryFinalCheckpointBasis, basis);
                  }
                  return Ref.set(committed, input.events).pipe(
                    Effect.as({
                      receipt: {
                        commandId: input.commandId,
                        threadId: input.threadId,
                        commandType: input.commandType,
                        acceptedAt: input.acceptedAt,
                        resultSequence: input.events.length,
                        status: "accepted" as const,
                        error: null,
                      },
                      committed: ordinary !== "cached-commit" && ordinary !== "basis-cached",
                      cancelledEffectCount: 0,
                      storedEvents:
                        ordinary === "no-stored-event"
                          ? []
                          : input.events.map((event, index) => ({
                              commandId: input.commandId,
                              sequence: index + 1,
                              event,
                            })),
                    }),
                  );
                },
              }),
            ),
          ),
        );

        yield* Effect.gen(function* () {
          const service = yield* CheckpointCaptureService.CheckpointCaptureServiceV2;
          const incomplete = yield* service
            .execute({ threadId, runId, scopeId: CheckpointScopeId.make("missing-scope") })
            .pipe(Effect.flip);
          assert.instanceOf(incomplete, CheckpointCaptureService.CheckpointCaptureExecutionError);
          if (ordinary === "basis-settled") {
            yield* projectionStore.apply({
              id: EventId.make("event:checkpoint-capture:run-settled"),
              type: "run.updated",
              threadId,
              runId,
              nodeId: rootNodeId,
              providerInstanceId,
              occurredAt: now,
              payload: staleRun,
            });
          }
          // Capture reads the scenario's run while the projection still holds the stale cohort.
          const operation = service.execute({
            threadId,
            runId,
            scopeId,
            ...(use === undefined ? {} : { ordinaryCheckoutUse: use }),
            ...(execution === undefined ? {} : { ordinaryCheckoutExecution: execution }),
            ...(basis === undefined ? {} : { ordinaryFinalCheckpointBasis: basis }),
          });
          if (ordinary === "basis-wrong-run" || ordinary === "basis-settled") {
            const actual = yield* operation.pipe(Effect.flip);
            assert.instanceOf(actual, OrdinaryCheckout.OrdinaryCheckoutOwnershipError);
            if (Schema.is(OrdinaryCheckout.OrdinaryCheckoutOwnershipError)(actual))
              assert.equal(
                actual.reason,
                ordinary === "basis-wrong-run" ? "claim_mismatch" : "unknown_use",
              );
            assert.equal(nativeCapture.mock.calls.length, 0);
            assert.deepEqual(yield* Ref.get(committed), []);
            assert.isNull(
              CheckpointCaptureService.readIssuedCheckpointCaptureObservationForExecution(
                execution!,
              ),
            );
            return;
          }
          if (
            ["stale_admission", "claim_mismatch", "unknown_use", "basis-stale"].includes(ordinary)
          ) {
            const actual = yield* operation.pipe(Effect.flip);
            assert.strictEqual(actual, ownershipError);
            assert.equal(nativeCapture.mock.calls.length, 0);
            assert.deepEqual(yield* Ref.get(committed), []);
            return;
          }
          const observation = yield* operation;
          assert.equal(observation.kind, "captured");
          if (observation.kind !== "captured") return;
          if (ordinary === "allowed") {
            assert.equal(revalidate.mock.calls.length, 1);
            assert.equal(nativeCapture.mock.calls.length, 1);
          }

          const events = yield* Ref.get(committed);
          assert.equal(observation.commit.receipt.status, "accepted");
          const expectedStatus =
            ordinary === "capture-error" ? "error" : ordinary === "missing" ? "missing" : "ready";
          assert.equal(observation.checkpoint.status, expectedStatus);
          if (
            [
              "capture-error",
              "missing",
              "no-stored-event",
              "cached-commit",
              "basis-cached",
              "deferred-commit",
              "rolled-back-commit",
            ].includes(ordinary)
          ) {
            assert.isNull(
              CheckpointCaptureService.readIssuedCheckpointCaptureObservation(observation),
            );
          } else {
            assert.strictEqual(
              CheckpointCaptureService.readIssuedCheckpointCaptureObservation(observation),
              observation,
            );
            assert.isNull(
              CheckpointCaptureService.readIssuedCheckpointCaptureObservation({ ...observation }),
            );
            assert.isNull(
              CheckpointCaptureService.readIssuedCheckpointCaptureObservation(
                yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown))(observation).pipe(
                  Effect.flatMap(Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown))),
                  Effect.orDie,
                ),
              ),
            );
          }
          if (ordinary === "deferred-commit") {
            assert.equal(publications.length, 1);
            yield* publications[0]!;
            assert.strictEqual(
              CheckpointCaptureService.readIssuedCheckpointCaptureObservation(observation),
              observation,
            );
          }
          if (ordinary === "rolled-back-commit") {
            assert.equal(publications.length, 1);
            publications.length = 0;
            assert.isNull(
              CheckpointCaptureService.readIssuedCheckpointCaptureObservation(observation),
            );
          }
          if (execution !== undefined) {
            assert.strictEqual(observation.ordinaryCheckoutExecution, execution);
            if (
              [
                "lifetime",
                "refresh-error",
                "deferred-commit",
                "basis-ready",
                "basis-mutated",
              ].includes(ordinary)
            ) {
              assert.strictEqual(
                CheckpointCaptureService.readIssuedCheckpointCaptureObservationForExecution(
                  execution,
                ),
                observation,
              );
              assert.isNull(
                CheckpointCaptureService.readIssuedCheckpointCaptureObservationForExecution({
                  ...execution,
                }),
              );
            } else
              assert.isNull(
                CheckpointCaptureService.readIssuedCheckpointCaptureObservationForExecution(
                  execution,
                ),
              );
          }
          if (basis !== undefined) {
            assert.strictEqual(observation.ordinaryFinalCheckpointBasis, basis);
            assert.equal(nativeCapture.mock.calls.length, 1);
            assert.equal(
              revalidateBasis.mock.calls.length,
              3,
              "Capture entry and both Service entry/after-lock checks must validate the actual basis.",
            );
            if (ordinary === "basis-mutated") {
              Object.assign(basis, { joinOrdinal: basis.joinOrdinal + 1 });
              assert.isNull(
                CheckpointCaptureService.readIssuedCheckpointCaptureObservation(observation),
              );
              assert.isNull(
                CheckpointCaptureService.readIssuedCheckpointCaptureObservationForExecution(
                  execution!,
                ),
              );
            }
          }
          if (ordinary === "refresh-error" && execution !== undefined) {
            const refreshError = new RunFinalization.RunFinalizationRefreshError({
              cwd: "/repo",
              cause: "Read-only refresh failed after durable capture.",
            });
            const refresh = vi.fn(() => Effect.fail(refreshError));
            const finalizationLayer = RunFinalization.layer.pipe(
              Layer.provide(
                Layer.mergeAll(
                  Layer.mock(CheckpointCaptureService.CheckpointCaptureServiceV2)({
                    execute: () => Effect.succeed(observation),
                  }),
                  Layer.mock(ProjectionStore.ProjectionStoreV2)({
                    getCheckpointContext: () =>
                      Effect.succeed({
                        runs: [staleRun],
                        checkpointScopes: [scope],
                        checkpoints: [],
                      }),
                  }),
                  Layer.succeed(RunFinalization.RunFinalizationObserver, {
                    refresh,
                    refreshAfterTurn: () => Effect.void,
                  }),
                ),
              ),
            );
            const failed = yield* Effect.gen(function* () {
              const finalizer = yield* RunFinalization.RunFinalizationService;
              return yield* finalizer
                .finalize({ threadId, runId, scopeId, ordinaryCheckoutExecution: execution })
                .pipe(Effect.flip);
            }).pipe(Effect.provide(finalizationLayer));
            assert.isTrue(
              Schema.is(RunFinalization.RunFinalizationError)(failed) &&
                failed.cause === refreshError,
            );
            assert.equal(refresh.mock.calls.length, 1);
            assert.strictEqual(
              CheckpointCaptureService.readIssuedCheckpointCaptureObservationForExecution(
                execution,
              ),
              observation,
              "An unrelated later refresh error must preserve the actual durable physical result for its original invocation.",
            );
          }
          if (ordinary === "lifetime") {
            Object.assign(observation.commit, { cancelledEffectCount: 1 });
            assert.isNull(
              CheckpointCaptureService.readIssuedCheckpointCaptureObservation(observation),
              "Mutated receipt/event result facts cannot keep the original issuer proof.",
            );
            assert.isNull(
              CheckpointCaptureService.readIssuedCheckpointCaptureObservationForExecution(
                execution!,
              ),
            );
          }
          const runUpdated = events.find((event) => event.type === "run.updated");
          assert.isDefined(runUpdated);
          if (runUpdated?.type !== "run.updated") {
            return;
          }
          assert.equal(runUpdated.payload.status, "completed");
          const capturedEvent = events.find((event) => event.type === "checkpoint.captured");
          assert.equal(
            runUpdated.payload.checkpointId,
            usesActualCheckpointService ? capturedEvent?.payload.id : captured.id,
          );
          if (usesActualCheckpointService && capturedEvent?.type === "checkpoint.captured") {
            assert.equal(capturedEvent.payload.status, expectedStatus);
            assert.deepEqual(capturedEvent.payload.files, []);
          }
          assert.isUndefined(
            runUpdated.payload.delegatedCompletion,
            "checkpoint capture must omit delegatedCompletion so ProjectionStore can keep a newer cohort",
          );

          // Race: a newer cohort lands on the projection after capture read the stale
          // waiting run and before the capture command's run.updated is applied.
          yield* projectionStore.apply({
            id: EventId.make("event:checkpoint-capture:newer-cohort"),
            type: "run.updated",
            threadId,
            runId,
            nodeId: rootNodeId,
            providerInstanceId,
            occurredAt: later,
            payload: {
              ...staleRun,
              delegatedCompletion: newerCohort,
            },
          });

          // Apply the real capture-emitted run.updated through ProjectionStore.
          yield* projectionStore.apply(runUpdated);

          const projectedRun = (yield* projectionStore.getCheckpointCaptureContext(threadId, {
            runId,
            scopeId,
          })).run;
          assert.isDefined(projectedRun);
          assert.equal(projectedRun?.status, "completed");
          assert.equal(projectedRun?.checkpointId, runUpdated.payload.checkpointId);
          assert.deepEqual(projectedRun?.delegatedCompletion, newerCohort);
          assert.equal(projectedRun?.delegatedCompletion?.delivery?.messageId, deliveryMessageId);
          assert.deepEqual(projectedRun?.delegatedCompletion?.delivery?.taskIds, [taskId]);
          // The persisted completion is the at-least-once capture receipt.
          yield* Ref.set(committed, []);
          const skipped = yield* service.execute({ threadId, runId, scopeId });
          assert.deepEqual(skipped, { version: 1, kind: "skipped", reason: "settled" });
          assert.isNull(CheckpointCaptureService.readIssuedCheckpointCaptureObservation(skipped));
          assert.deepEqual(yield* Ref.get(committed), []);
        }).pipe(Effect.provide(captureLayer));
      }),
  );

  // Capture and rollback share the thread's effect lane, so a rollback can only
  // commit before a capture runs: while it waits out a retry, or when a restart
  // requeues it behind a pending rollback. It must not revive the run.
  it.effect("does not capture a stopped run that a rollback already discarded", () =>
    Effect.gen(function* () {
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      const stoppedThreadId = ThreadId.make("thread:checkpoint-capture-rolled-back");
      const stoppedRunId = RunId.make("run:checkpoint-capture-rolled-back");
      yield* projectionStore.apply({
        id: EventId.make("event:checkpoint-capture-rolled-back:thread"),
        type: "thread.created",
        threadId: stoppedThreadId,
        occurredAt: now,
        payload: {
          createdBy: "user",
          creationSource: "web",
          id: stoppedThreadId,
          projectId,
          title: "Checkpoint capture after rollback",
          providerInstanceId,
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          activeProviderThreadId: null,
          lineage: {
            parentThreadId: null,
            relationshipToParent: null,
            rootThreadId: stoppedThreadId,
          },
          forkedFrom: null,
          createdAt: now,
          updatedAt: now,
          archivedAt: null,
          settledOverride: null,
          settledAt: null,
          lastVisitedAt: null,
          deletedAt: null,
        },
      });
      const stoppedRun: OrchestrationV2Run = {
        id: stoppedRunId,
        threadId: stoppedThreadId,
        ordinal: 2,
        providerInstanceId,
        modelSelection,
        providerThreadId,
        userMessageId: MessageId.make("message:checkpoint-capture-rolled-back"),
        rootNodeId,
        activeAttemptId: null,
        status: "interrupted",
        requestedAt: now,
        startedAt: now,
        completedAt: now,
        checkpointId: null,
        contextHandoffId: null,
      };
      // The rollback's write, which landed before the capture ran.
      yield* projectionStore.apply({
        id: EventId.make("event:checkpoint-capture-rolled-back:run"),
        type: "run.updated",
        threadId: stoppedThreadId,
        runId: stoppedRunId,
        providerInstanceId,
        occurredAt: now,
        payload: { ...stoppedRun, status: "rolled_back" },
      });

      const committed = yield* Ref.make<ReadonlyArray<OrchestrationV2DomainEvent>>([]);
      const captureLayer = CheckpointCaptureService.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            IdAllocator.layer,
            Layer.mock(CheckpointService.CheckpointServiceV2)({
              materializeBaselineCheckpoint: () => Effect.die("a discarded run has no baseline"),
              capture: () => Effect.die("a discarded run must not be captured"),
            }),
            Layer.mock(EventSink.EventSinkV2)({
              withTransaction: (effect) => effect,
              onCommit: (effect) => effect,
              commitCommand: (input) =>
                Ref.set(committed, input.events).pipe(Effect.as({ committed: true } as never)),
            }),
          ),
        ),
      );

      // Settles without retrying, and commits nothing that would revive the run.
      yield* CheckpointCaptureService.CheckpointCaptureServiceV2.pipe(
        Effect.flatMap((service) =>
          service.execute({ threadId: stoppedThreadId, runId: stoppedRunId, scopeId }),
        ),
        Effect.provide(captureLayer),
      );

      assert.deepEqual(yield* Ref.get(committed), []);
      const projected = yield* projectionStore.getCheckpointCaptureContext(stoppedThreadId, {
        runId: stoppedRunId,
        scopeId,
      });
      assert.equal(projected.run?.status, "rolled_back");
      assert.isNull(projected.run?.checkpointId ?? null);
    }),
  );

  // A cancelled run's checkpoint is the rollback point for the message after
  // it, so capture records it without reporting the run as completed.
  it.effect("records the checkpoint of a cancelled run and keeps it cancelled", () =>
    Effect.gen(function* () {
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      const cancelledAt = DateTime.add(now, { seconds: 1 });
      const cancelledThreadId = ThreadId.make("thread:checkpoint-capture-cancelled");
      const cancelledRunId = RunId.make("run:checkpoint-capture-cancelled");
      const cancelledScopeId = CheckpointScopeId.make("scope:checkpoint-capture-cancelled");
      const cancelledRootNodeId = NodeId.make("node:checkpoint-capture-cancelled-root");
      const cancelledProviderThreadId = ProviderThreadId.make(
        "provider-thread:checkpoint-capture-cancelled",
      );
      yield* projectionStore.apply({
        id: EventId.make("event:checkpoint-capture-cancelled:thread"),
        type: "thread.created",
        threadId: cancelledThreadId,
        occurredAt: now,
        payload: {
          createdBy: "user",
          creationSource: "web",
          id: cancelledThreadId,
          projectId,
          title: "Checkpoint capture after cancel",
          providerInstanceId,
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          activeProviderThreadId: null,
          lineage: {
            parentThreadId: null,
            relationshipToParent: null,
            rootThreadId: cancelledThreadId,
          },
          forkedFrom: null,
          createdAt: now,
          updatedAt: now,
          archivedAt: null,
          settledOverride: null,
          settledAt: null,
          lastVisitedAt: null,
          deletedAt: null,
        },
      });
      const runningRun: OrchestrationV2Run = {
        id: cancelledRunId,
        threadId: cancelledThreadId,
        ordinal: 1,
        providerInstanceId,
        modelSelection,
        providerThreadId: cancelledProviderThreadId,
        userMessageId: MessageId.make("message:checkpoint-capture-cancelled"),
        rootNodeId: cancelledRootNodeId,
        activeAttemptId: null,
        status: "running",
        requestedAt: now,
        startedAt: now,
        completedAt: null,
        checkpointId: null,
        contextHandoffId: null,
      };
      const runningRootNode = {
        id: cancelledRootNodeId,
        threadId: cancelledThreadId,
        runId: cancelledRunId,
        parentNodeId: null,
        rootNodeId: cancelledRootNodeId,
        kind: "root_turn" as const,
        status: "running" as const,
        countsForRun: true,
        providerThreadId: cancelledProviderThreadId,
        providerTurnId: null,
        nativeItemRef: null,
        runtimeRequestId: null,
        checkpointScopeId: cancelledScopeId,
        startedAt: now,
        completedAt: null,
      };
      const scope = {
        id: cancelledScopeId,
        threadId: cancelledThreadId,
        runId: cancelledRunId,
        nodeId: cancelledRootNodeId,
        parentScopeId: null,
        providerThreadId: cancelledProviderThreadId,
        kind: "root_run" as const,
        ordinalWithinParent: 0,
        advancesAppRunCount: true,
        cwd: "/repo",
        createdAt: now,
      };
      yield* projectionStore.apply({
        id: EventId.make("event:checkpoint-capture-cancelled:provider-thread"),
        type: "provider-thread.updated",
        threadId: cancelledThreadId,
        nodeId: cancelledRootNodeId,
        driver,
        providerInstanceId,
        occurredAt: now,
        payload: {
          id: cancelledProviderThreadId,
          driver,
          providerInstanceId,
          providerSessionId: null,
          appThreadId: cancelledThreadId,
          ownerNodeId: cancelledRootNodeId,
          nativeThreadRef: null,
          nativeConversationHeadRef: null,
          status: "idle",
          firstRunOrdinal: 1,
          lastRunOrdinal: 1,
          handoffIds: [],
          forkedFrom: null,
          createdAt: now,
          updatedAt: now,
        },
      });
      yield* projectionStore.apply({
        id: EventId.make("event:checkpoint-capture-cancelled:scope"),
        type: "checkpoint-scope.created",
        threadId: cancelledThreadId,
        runId: cancelledRunId,
        nodeId: cancelledRootNodeId,
        occurredAt: now,
        payload: scope,
      });
      yield* projectionStore.apply({
        id: EventId.make("event:checkpoint-capture-cancelled:baseline"),
        type: "checkpoint.captured",
        threadId: cancelledThreadId,
        nodeId: cancelledRootNodeId,
        driver,
        providerInstanceId,
        occurredAt: now,
        payload: {
          id: CheckpointId.make("checkpoint:cancelled-baseline-0"),
          threadId: cancelledThreadId,
          scopeId: cancelledScopeId,
          runId: null,
          nodeId: cancelledRootNodeId,
          parentCheckpointId: null,
          ordinalWithinScope: 0,
          appRunOrdinal: null,
          ref: CheckpointRef.make("checkpoint-ref:cancelled-baseline-0"),
          status: "ready",
          files: [],
          capturedAt: now,
        },
      });
      // The turn runs, then finalizes as cancelled the way RunExecutionService
      // writes it, which also enqueues this capture.
      for (const [status, completedAt] of [
        ["running", null],
        ["cancelled", cancelledAt],
      ] as const) {
        yield* projectionStore.apply({
          id: EventId.make(`event:checkpoint-capture-cancelled:run-${status}`),
          type: "run.updated",
          threadId: cancelledThreadId,
          runId: cancelledRunId,
          nodeId: cancelledRootNodeId,
          providerInstanceId,
          occurredAt: completedAt ?? now,
          payload: { ...runningRun, status, completedAt },
        });
        yield* projectionStore.apply({
          id: EventId.make(`event:checkpoint-capture-cancelled:node-${status}`),
          type: "node.updated",
          threadId: cancelledThreadId,
          runId: cancelledRunId,
          nodeId: cancelledRootNodeId,
          providerInstanceId,
          occurredAt: completedAt ?? now,
          payload: { ...runningRootNode, status, completedAt },
        });
      }

      const captured = {
        id: CheckpointId.make("checkpoint:cancelled-captured-1"),
        threadId: cancelledThreadId,
        scopeId: cancelledScopeId,
        runId: cancelledRunId,
        nodeId: cancelledRootNodeId,
        parentCheckpointId: CheckpointId.make("checkpoint:cancelled-baseline-0"),
        ordinalWithinScope: 1,
        appRunOrdinal: 1,
        ref: CheckpointRef.make("checkpoint-ref:cancelled-captured-1"),
        status: "ready" as const,
        files: [],
        capturedAt: cancelledAt,
      };
      const commits = yield* Ref.make(0);
      const captureLayer = CheckpointCaptureService.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            IdAllocator.layer,
            Layer.mock(CheckpointService.CheckpointServiceV2)({
              materializeBaselineCheckpoint: () =>
                Effect.die("baseline materialization must be skipped when ordinal 0 is ready"),
              capture: () => Effect.succeed(captured),
            }),
            // Commit straight into the projection so the test reads what a
            // client would see after the capture lands.
            Layer.mock(EventSink.EventSinkV2)({
              withTransaction: (effect) => effect,
              onCommit: (effect) => effect,
              commitCommand: (input) =>
                Effect.forEach(input.events, (event) => projectionStore.apply(event)).pipe(
                  Effect.andThen(Ref.update(commits, (count) => count + 1)),
                  Effect.as({ committed: true } as never),
                  Effect.orDie,
                ),
            }),
          ),
        ),
      );

      yield* Effect.gen(function* () {
        const service = yield* CheckpointCaptureService.CheckpointCaptureServiceV2;
        yield* service.execute({
          threadId: cancelledThreadId,
          runId: cancelledRunId,
          scopeId: cancelledScopeId,
        });
        // A redelivered effect finds the recorded checkpoint and commits nothing.
        yield* service.execute({
          threadId: cancelledThreadId,
          runId: cancelledRunId,
          scopeId: cancelledScopeId,
        });
      }).pipe(Effect.provide(captureLayer));

      assert.equal(yield* Ref.get(commits), 1);
      const projected = yield* projectionStore.getCheckpointCaptureContext(cancelledThreadId, {
        runId: cancelledRunId,
        scopeId: cancelledScopeId,
      });
      assert.equal(projected.run?.status, "cancelled");
      assert.equal(projected.run?.checkpointId, captured.id);
      const completedAt = projected.run?.completedAt;
      assert.equal(
        completedAt ? DateTime.formatIso(completedAt) : completedAt,
        DateTime.formatIso(cancelledAt),
      );
      assert.equal(projected.rootNode?.status, "cancelled");
      assert.deepEqual([...projected.readyCheckpointOrdinals].toSorted(), [0, 1]);
    }),
  );
});

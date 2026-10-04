import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import {
  CheckpointScopeId,
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type RunId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EffectWorker from "./EffectWorker.ts";
import * as EventSink from "./EventSink.ts";
import * as Orchestrator from "./Orchestrator.ts";
import { ordinaryCheckoutOutboxOperationIdV1 } from "./OrdinaryCheckoutOwnership.ts";
import * as ProjectStore from "./ProjectStore.ts";
import {
  ProviderAdapterOpenSessionError,
  type ProviderAdapterV2Shape,
  type ProviderAdapterV2SessionRuntime,
} from "./ProviderAdapter.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import { makeOrchestratorV2ReplayLayerWithRegistry } from "./testkit/ProviderReplayHarness.ts";

it.effect(
  "resumes queued B exactly once after A fails before provider open with known_no_effect",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const workspace = yield* fs.makeTempDirectoryScoped({
        directory: process.env.TMPDIR ?? process.cwd(),
        prefix: "provider-start-queue-",
      });
      const driver = ProviderDriverKind.make("codex");
      const modelSelection = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" };
      const threadId = ThreadId.make("known-no-effect-queue");
      const projectId = ProjectId.make("known-no-effect-queue-project");
      const commandA = CommandId.make("known-no-effect-queue:A");
      const commandB = CommandId.make("known-no-effect-queue:B");
      let openAttempts = 0;
      let openedSessions = 0;
      const startedRunIds: RunId[] = [];
      const adapter: ProviderAdapterV2Shape = {
        instanceId: modelSelection.instanceId,
        driver,
        getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
        planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
        openSession: (input) =>
          Effect.gen(function* () {
            openAttempts += 1;
            assert.isDefined(input.nativeOperation);
            if (openAttempts === 1) {
              return yield* new ProviderAdapterOpenSessionError({
                driver,
                providerSessionId: input.providerSessionId,
                nativeEffect: { ...input.nativeOperation!, outcome: "known_no_effect" },
                cause: "Synthetic pre-open failure; no native session was opened.",
              });
            }
            openedSessions += 1;
            const now = yield* DateTime.now;
            const runtimeGeneration = input.nativeOperation?.runtimeGeneration;
            return {
              instanceId: modelSelection.instanceId,
              driver,
              providerSessionId: input.providerSessionId,
              ...(runtimeGeneration === undefined ? {} : { runtimeGeneration }),
              providerSession: {
                id: input.providerSessionId,
                driver,
                providerInstanceId: modelSelection.instanceId,
                status: "ready",
                cwd: workspace,
                model: modelSelection.model,
                capabilities: CodexProviderCapabilitiesV2,
                createdAt: now,
                updatedAt: now,
                lastError: null,
              },
              events: Stream.never,
              ensureThread: (request) =>
                Effect.succeed({
                  ...request.existingProviderThread!,
                  nativeThreadRef: {
                    driver,
                    nativeId: "synthetic-queued-thread",
                    strength: "strong",
                  },
                }),
              resumeThread: (request) => Effect.succeed(request.providerThread),
              startTurn: (request) =>
                Effect.sync(() => {
                  startedRunIds.push(request.runId);
                }),
              steerTurn: () => Effect.die("Unexpected steer in queued start fixture"),
              interruptTurn: () => Effect.void,
              unloadThread: () => Effect.void,
              respondToRuntimeRequest: () =>
                Effect.die("Unexpected runtime request in queued start fixture"),
              readThreadSnapshot: () =>
                Effect.die("Unexpected history read in queued start fixture"),
              rollbackThread: () => Effect.die("Unexpected rollback in queued start fixture"),
              forkThread: () => Effect.die("Unexpected fork in queued start fixture"),
            } satisfies ProviderAdapterV2SessionRuntime;
          }),
      };
      const database = SqlitePersistenceMemory;
      const replay = makeOrchestratorV2ReplayLayerWithRegistry(
        { name: "known-no-effect-queue", runtimePolicyOverride: { cwd: workspace } },
        ProviderAdapterRegistry.makeLayer([adapter]),
        { databaseLayer: database, runEffectWorker: false },
      );
      const worker = EffectWorker.layerWithOptions({
        workerId: "known-no-effect-queue-worker",
        maxAttempts: 1,
      }).pipe(Layer.provide(replay));
      const layer = Layer.mergeAll(
        replay,
        worker,
        ProjectStore.layer.pipe(Layer.provide(database)),
        database,
      );
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const projects = yield* ProjectStore.ProjectStoreV2;
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const sink = yield* EventSink.EventSinkV2;
        const outbox = yield* EffectOutbox.EffectOutboxV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const now = DateTime.formatIso(yield* DateTime.now);
        yield* projects.apply({
          sequence: 0,
          eventId: EventId.make("known-no-effect-queue-project-created"),
          aggregateKind: "project",
          aggregateId: projectId,
          occurredAt: now,
          commandId: null,
          causationEventId: null,
          correlationId: null,
          metadata: {},
          type: "project.created",
          payload: {
            projectId,
            title: "Queue pre-open failure",
            workspaceRoot: workspace,
            defaultModelSelection: modelSelection,
            scripts: [],
            createdAt: now,
            updatedAt: now,
          },
        });
        yield* orchestrator.dispatch({
          type: "thread.create",
          commandId: CommandId.make("known-no-effect-queue:create"),
          threadId,
          projectId,
          title: "Queue pre-open failure",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: workspace,
          createdBy: "user",
          creationSource: "web",
        });
        for (const commandId of [commandA, commandB]) {
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId,
            threadId,
            messageId: MessageId.make(commandId),
            text: commandId === commandA ? "A" : "B",
            attachments: [],
            modelSelection,
            dispatchMode: {
              type: commandId === commandA ? "start_immediately" : "queue_after_active",
            },
            createdBy: "user",
            creationSource: "web",
          });
        }
        const before = yield* orchestrator.getThreadProjection(threadId);
        const runA = before.runs.find((run) => run.userMessageId === MessageId.make(commandA))!;
        const runB = before.runs.find((run) => run.userMessageId === MessageId.make(commandB))!;
        assert.equal(runA.status, "starting");
        assert.equal(runB.status, "queued");
        const effectsA = yield* outbox.listByCommandId(commandA);
        const startA = effectsA.find((effect) => effect.request.type === "provider-turn.start")!;
        assert.isDefined(startA);
        let firstWorkerExit:
          | Exit.Exit<boolean, EffectWorker.OrchestrationEffectWorkerError>
          | undefined;
        for (const _ of effectsA) {
          firstWorkerExit = yield* worker.runOnce.pipe(Effect.exit);
          if (openAttempts > 0 || Exit.isFailure(firstWorkerExit)) break;
        }
        const afterFailure = yield* orchestrator.getThreadProjection(threadId);
        const claim = Option.getOrThrow(yield* outbox.get(startA.id));
        const use = yield* sink.readOrdinaryCheckoutUse(
          ordinaryCheckoutOutboxOperationIdV1(startA.id, claim.attemptCount),
        );
        const history =
          use === null
            ? null
            : yield* sink.readOrdinaryCheckoutExecutionAssociations(use.subject.use);
        const facts = yield* sink.readNativeCommandFacts({ commandId: commandA, threadId });
        const leases =
          yield* sql`SELECT * FROM worktree_ownership_leases WHERE resource_path = ${workspace}`;
        const holds = yield* outbox.listHeldByThreadId(threadId);
        const beforeResume = { openAttempts, openedSessions, startedRunIds: [...startedRunIds] };
        const resumeExit = yield* orchestrator
          .dispatch({
            type: "queue.resume",
            commandId: CommandId.make("known-no-effect-queue:resume"),
            threadId,
          })
          .pipe(Effect.exit);
        const diagnostic = yield* Schema.encodeEffect(
          Schema.fromJsonString(Schema.Unknown, { space: 2 }),
        )({
          firstWorkerExit:
            firstWorkerExit === undefined
              ? null
              : Exit.isFailure(firstWorkerExit)
                ? Cause.pretty(firstWorkerExit.cause)
                : firstWorkerExit.value,
          runs: afterFailure.runs,
          attempts: afterFailure.attempts,
          nodes: afterFailure.nodes,
          claim,
          use,
          history,
          leases,
          runtimeOwner: facts.commitSnapshot.records.runtime_evidence,
          holds,
          beforeResume,
          resumeExit: Exit.isFailure(resumeExit)
            ? Cause.pretty(resumeExit.cause)
            : resumeExit.value,
        }).pipe(Effect.orDie);
        assert.isDefined(firstWorkerExit, diagnostic);
        assert.isTrue(Exit.isSuccess(firstWorkerExit!), diagnostic);
        assert.equal(
          afterFailure.runs.find((run) => run.id === runA.id)?.status,
          "failed",
          diagnostic,
        );
        assert.equal(
          afterFailure.attempts.find((attempt) => attempt.id === runA.activeAttemptId)?.status,
          "failed",
          diagnostic,
        );
        assert.equal(
          afterFailure.nodes.find((node) => node.id === runA.rootNodeId)?.status,
          "failed",
          diagnostic,
        );
        assert.deepEqual(
          beforeResume,
          { openAttempts: 1, openedSessions: 0, startedRunIds: [] },
          diagnostic,
        );
        assert.deepEqual(facts.commitSnapshot.records.runtime_evidence, [], diagnostic);
        assert.deepEqual(holds, [], diagnostic);
        assert.isTrue(Exit.isSuccess(resumeExit), diagnostic);
        const pending = yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox
        WHERE thread_id = ${threadId} AND status = 'pending'`;
        for (const _ of pending) yield* worker.runOnce;
        assert.deepEqual(startedRunIds, [runB.id]);
        assert.equal(openAttempts, 2);
        assert.equal(openedSessions, 1);
        yield* orchestrator.dispatch({
          type: "queue.resume",
          commandId: CommandId.make("known-no-effect-queue:resume-again"),
          threadId,
        });
        yield* worker.runOnce;
        assert.deepEqual(startedRunIds, [runB.id]);
        assert.equal(openAttempts, 2);
      }).pipe(Effect.provide(layer));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

for (const variant of [
  "held",
  "claim",
  "lease",
  "birth",
  "runtime",
  "scope",
  "unknown",
  "stop",
  "pending-checkpoint",
  "forged",
  "idempotent",
] as const) {
  it.effect(
    `failed-before-open settlement preserves actual authority and explicit queue hold: ${variant}`,
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const workspace = yield* fs.makeTempDirectoryScoped({
          directory: process.env.TMPDIR ?? process.cwd(),
          prefix: "provider-start-queue-",
        });
        const driver = ProviderDriverKind.make("codex");
        const modelSelection = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" };
        const threadId = ThreadId.make("known-no-effect-queue");
        const projectId = ProjectId.make("known-no-effect-queue-project");
        const commandA = CommandId.make("known-no-effect-queue:A");
        const commandB = CommandId.make("known-no-effect-queue:B");
        let openAttempts = 0;
        let openedSessions = 0;
        const startedRunIds: RunId[] = [];
        let beforeFailure: Effect.Effect<void> = Effect.void;
        let attemptedSessionId: import("@t3tools/contracts").ProviderSessionId | undefined;
        const adapter: ProviderAdapterV2Shape = {
          instanceId: modelSelection.instanceId,
          driver,
          getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
          planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
          openSession: (input) =>
            Effect.gen(function* () {
              openAttempts += 1;
              attemptedSessionId = input.providerSessionId;
              yield* beforeFailure;
              return yield* new ProviderAdapterOpenSessionError({
                driver,
                providerSessionId: input.providerSessionId,
                nativeEffect: { ...input.nativeOperation!, outcome: "known_no_effect" },
                cause: "Synthetic pre-open failure",
              });
            }),
        };
        const database = SqlitePersistenceMemory;
        const replay = makeOrchestratorV2ReplayLayerWithRegistry(
          { name: "known-no-effect-queue", runtimePolicyOverride: { cwd: workspace } },
          ProviderAdapterRegistry.makeLayer([adapter]),
          { databaseLayer: database, runEffectWorker: false },
        );
        const worker = EffectWorker.layerWithOptions({
          workerId: "known-no-effect-queue-worker",
          maxAttempts: 1,
        }).pipe(Layer.provide(replay));
        const layer = Layer.mergeAll(
          replay,
          worker,
          ProjectStore.layer.pipe(Layer.provide(database)),
          database,
        );
        yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          const projects = yield* ProjectStore.ProjectStoreV2;
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const sink = yield* EventSink.EventSinkV2;
          const outbox = yield* EffectOutbox.EffectOutboxV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const now = DateTime.formatIso(yield* DateTime.now);
          yield* projects.apply({
            sequence: 0,
            eventId: EventId.make("known-no-effect-queue-project-created"),
            aggregateKind: "project",
            aggregateId: projectId,
            occurredAt: now,
            commandId: null,
            causationEventId: null,
            correlationId: null,
            metadata: {},
            type: "project.created",
            payload: {
              projectId,
              title: "Queue pre-open failure",
              workspaceRoot: workspace,
              defaultModelSelection: modelSelection,
              scripts: [],
              createdAt: now,
              updatedAt: now,
            },
          });
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("known-no-effect-queue:create"),
            threadId,
            projectId,
            title: "Queue pre-open failure",
            modelSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: workspace,
            createdBy: "user",
            creationSource: "web",
          });
          for (const commandId of [commandA, commandB]) {
            yield* orchestrator.dispatch({
              type: "message.dispatch",
              commandId,
              threadId,
              messageId: MessageId.make(commandId),
              text: commandId === commandA ? "A" : "B",
              attachments: [],
              modelSelection,
              dispatchMode: {
                type: commandId === commandA ? "start_immediately" : "queue_after_active",
              },
              createdBy: "user",
              creationSource: "web",
            });
          }
          const before = yield* orchestrator.getThreadProjection(threadId);
          const runA = before.runs.find((run) => run.userMessageId === MessageId.make(commandA))!;
          const runB = before.runs.find((run) => run.userMessageId === MessageId.make(commandB))!;
          assert.equal(runA.status, "starting");
          assert.equal(runB.status, "queued");
          const effectsA = yield* outbox.listByCommandId(commandA);
          const startA = effectsA.find((effect) => effect.request.type === "provider-turn.start")!;
          assert.isDefined(startA);
          const settleSpy = yield* Effect.acquireRelease(
            Effect.sync(() => vi.spyOn(sink, "settleOrdinaryCheckoutStartFailedBeforeOpen")),
            (spy) => Effect.sync(() => spy.mockRestore()),
          );
          // Tampering failures stay defects at the provider-open boundary.
          beforeFailure = Effect.gen(function* () {
            const current = Option.getOrThrow(yield* outbox.get(startA.id));
            if (variant === "claim")
              yield* sql`UPDATE orchestration_v2_effect_outbox SET lease_owner = 'foreign-worker' WHERE effect_id = ${startA.id}`;
            if (variant === "lease")
              yield* sql`UPDATE worktree_ownership_leases SET lease_id = 'foreign-lease' WHERE resource_path = ${workspace}`;
            if (variant === "birth")
              yield* sink.write({
                events: [
                  {
                    id: EventId.make("queue-negative-replaced-birth"),
                    type: "thread.created",
                    threadId,
                    occurredAt: yield* DateTime.now,
                    payload: before.thread,
                  },
                ],
              });
            if (variant === "scope")
              yield* sink.write({
                events: [
                  {
                    id: EventId.make("queue-negative-unrelated-scope"),
                    type: "checkpoint-scope.created",
                    threadId,
                    occurredAt: yield* DateTime.now,
                    payload: {
                      ...before.checkpointScopes[0]!,
                      id: CheckpointScopeId.make("queue-negative-unrelated-scope"),
                    },
                  },
                ],
              });
            if (variant === "unknown")
              yield* outbox.holdUnknown({
                effectId: startA.id,
                workerId: current.leaseOwner!,
                expectedAttempt: current.attemptCount,
                operationId: startA.id,
                evidence: {
                  operationId: startA.id,
                  operation: "start_turn",
                  threadId,
                  outcome: "unknown",
                },
              });
            if (variant === "pending-checkpoint") {
              const use = (yield* sink.readOrdinaryCheckoutUse(
                ordinaryCheckoutOutboxOperationIdV1(startA.id, current.attemptCount),
              ))!;
              const history = yield* sink.readOrdinaryCheckoutExecutionAssociations(
                use.subject.use,
              );
              yield* sink.writeWithEffects({
                events: [],
                effects: [
                  {
                    id: "queue-negative-linked-checkpoint",
                    commandId: commandA,
                    threadId,
                    request: {
                      type: "checkpoint.capture",
                      runId: runA.id,
                      scopeId: before.nodes.find((node) => node.id === runA.rootNodeId)!
                        .checkpointScopeId!,
                    },
                  },
                ],
                ordinaryCheckoutEffects: [
                  {
                    admission: use.subject.use.admission,
                    runId: runA.id,
                    source: use.subject.source,
                    joinedUse: use.subject.use,
                    ordinaryCheckoutExecution: history.participants[0]!.ref,
                  },
                ],
              });
            }
            if (variant === "stop")
              yield* orchestrator.dispatch({
                type: "run.interrupt",
                commandId: CommandId.make("queue-negative-stop"),
                threadId,
                runId: runA.id,
              });
            if (variant === "runtime") {
              yield* sink.write({
                events: [
                  {
                    id: EventId.make("queue-negative-prior-runtime"),
                    type: "provider-session.attached",
                    threadId,
                    occurredAt: yield* DateTime.now,
                    payload: {
                      id: attemptedSessionId!,
                      driver,
                      providerInstanceId: modelSelection.instanceId,
                      status: "stopped",
                      cwd: workspace,
                      model: modelSelection.model,
                      capabilities: CodexProviderCapabilitiesV2,
                      createdAt: yield* DateTime.now,
                      updatedAt: yield* DateTime.now,
                      lastError: null,
                    },
                  },
                ],
              });
            }
          }).pipe(Effect.orDie);
          let firstWorkerExit:
            | Exit.Exit<boolean, EffectWorker.OrchestrationEffectWorkerError>
            | undefined;
          for (const _ of effectsA) {
            firstWorkerExit = yield* worker.runOnce.pipe(Effect.exit);
            if (openAttempts > 0 || Exit.isFailure(firstWorkerExit)) break;
          }
          const rejected = !["held", "forged", "idempotent"].includes(variant);
          assert.isDefined(firstWorkerExit);
          if (!rejected)
            assert.isTrue(
              Exit.isSuccess(firstWorkerExit!),
              Exit.isFailure(firstWorkerExit!)
                ? Cause.pretty(firstWorkerExit!.cause)
                : "failed start must settle",
            );
          const claim = Option.getOrThrow(yield* outbox.get(startA.id));
          const operationId = ordinaryCheckoutOutboxOperationIdV1(startA.id, claim.attemptCount);
          const uses = yield* sql<{
            readonly state: string;
          }>`SELECT state FROM orchestration_v2_worktree_path_admissions WHERE operation_id = ${operationId}`;
          assert.lengthOf(uses, 1);
          assert.equal(uses[0]!.state === "released", !rejected);
          if (rejected && variant !== "pending-checkpoint")
            assert.notEqual(claim.status, "succeeded");
          if (variant === "pending-checkpoint") assert.equal(claim.status, "succeeded");
          assert.equal(openAttempts, 1);
          assert.equal(openedSessions, 0);
          assert.deepEqual(startedRunIds, []);
          if (rejected) {
            yield* orchestrator
              .dispatch({
                type: "queue.resume",
                commandId: CommandId.make("queue-negative-resume"),
                threadId,
              })
              .pipe(Effect.exit);
            assert.equal(
              (yield* orchestrator.getThreadProjection(threadId)).runs.find(
                (run) => run.id === runB.id,
              )?.status,
              "queued",
            );
            assert.equal(openAttempts, 1);
          } else {
            yield* orchestrator.resumeQueuedRuns;
            const held = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
              (run) => run.id === runB.id,
            )!;
            assert.equal(held.status, "queued");
            assert.isTrue(held.queueHeld);
            assert.equal(claim.status, "succeeded");
            assert.lengthOf(settleSpy.mock.calls, 1);
            const original = settleSpy.mock.calls[0]![0];
            const history = yield* sink.readOrdinaryCheckoutExecutionAssociations(
              original.ref.originalUse,
            );
            if (variant === "forged") {
              const copy = { ...original.observation };
              assert.isTrue(
                Exit.isFailure(
                  yield* sink
                    .settleOrdinaryCheckoutStartFailedBeforeOpen({ ...original, observation: copy })
                    .pipe(Effect.exit),
                ),
              );
            }
            if (variant === "idempotent") {
              yield* sink.settleOrdinaryCheckoutStartFailedBeforeOpen(original);
              yield* sink.settleOrdinaryCheckoutStartFailedBeforeOpen(original);
            }
            assert.deepEqual(
              yield* sink.readOrdinaryCheckoutExecutionAssociations(original.ref.originalUse),
              history,
            );
          }
        }).pipe(Effect.provide(layer));
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
}

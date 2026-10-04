import { assert, it, vi } from "@effect/vitest";
import {
  MessageId,
  NodeId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProjectId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  RuntimeRequestId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as SqlError from "effect/unstable/sql/SqlError";

import * as EffectWorker from "./EffectWorker.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EventSink from "./EventSink.ts";
import * as IdAllocator from "./IdAllocator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderRuntimeRecovery from "./ProviderRuntimeRecoveryService.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as ServerSettings from "../serverSettings.ts";

for (const respectProjectPreference of [true, false]) {
  it.effect(
    `reuses the exact dormant source across both ${respectProjectPreference ? "opted-in" : "forced"} preparations and releases after opt-out`,
    () =>
      Effect.gen(function* () {
        const now = DateTime.formatIso(yield* DateTime.now);
        const threadId = ThreadId.make(`repeat-preparation-${respectProjectPreference}`);
        const binding: EventSink.ProviderBindingExpectationV2 = {
          threadId,
          providerThreadId: ProviderThreadId.make(
            `repeat-provider-thread-${respectProjectPreference}`,
          ),
          providerSessionId: ProviderSessionId.make(`repeat-session-${respectProjectPreference}`),
          instanceId: ProviderInstanceId.make("codex"),
          driver: ProviderDriverKind.make("codex"),
          nativeThreadId: "repeat-native-thread",
          runtimeGeneration: "actual-repeat-generation",
        };
        const runId = RunId.make(`repeat-run-${respectProjectPreference}`);
        const attemptId = RunAttemptId.make(`repeat-attempt-${respectProjectPreference}`);
        const projectId = ProjectId.make("repeat-project");
        const projection = {
          thread: {
            id: threadId,
            projectId,
            providerInstanceId: binding.instanceId,
            activeProviderThreadId: binding.providerThreadId,
            modelSelection: { instanceId: binding.instanceId },
            archivedAt: null,
            deletedAt: null,
          },
          runs: [
            {
              id: runId,
              activeAttemptId: attemptId,
              providerThreadId: binding.providerThreadId,
              providerInstanceId: binding.instanceId,
              status: "completed",
              ordinal: 1,
            },
          ],
          attempts: [{ id: attemptId, runId, providerThreadId: binding.providerThreadId }],
          providerThreads: [
            {
              id: binding.providerThreadId,
              appThreadId: threadId,
              ownerNodeId: null,
              providerSessionId: binding.providerSessionId,
              providerInstanceId: binding.instanceId,
              driver: binding.driver,
              nativeThreadRef: {
                driver: binding.driver,
                nativeId: binding.nativeThreadId,
                strength: "strong",
              },
              status: "idle",
            },
          ],
          providerSessions: [
            {
              id: binding.providerSessionId,
              providerInstanceId: binding.instanceId,
              driver: binding.driver,
              status: "ready",
            },
          ],
          providerTurns: [],
          nodes: [],
          subagents: [],
          messages: [],
          runtimeRequests: [],
          turnItems: [],
        } as unknown as ProjectionStore.ProjectionRuntimeRecoveryState;
        let preferences = yield* ServerSettings.ServerSettingsService.pipe(
          Effect.flatMap((service) => service.getSettings),
          Effect.provide(ServerSettings.layerTest({ continueThreadsAfterServerUpdate: true })),
        );
        let marker: EventSink.RestartContinuationMarkerV2 | null = null;
        let observing = 0;
        let allocations = 0;
        let releases = 0;
        let afterTrial = false;
        const layer = ProviderRuntimeRecovery.layer.pipe(
          Layer.provide(
            Layer.mergeAll(
              Layer.mock(ServerSettings.ServerSettingsService)({
                getSettings: Effect.sync(() => preferences),
              }),
              IdAllocator.layer,
              Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
                observeCurrentThreadRuntime: () =>
                  Effect.sync(() => {
                    observing++;
                    return {
                      status: "monitoring" as const,
                      observedAt: now,
                      binding: {
                        threadId,
                        providerThreadId: binding.providerThreadId,
                        providerSessionId: binding.providerSessionId,
                        instanceId: binding.instanceId,
                        runtimeGeneration: "actual-repeat-generation",
                        nativeThreadId: "repeat-native-thread",
                      },
                    };
                  }),
              }),
              Layer.mock(ProjectionStore.ProjectionStoreV2)({
                getRecoveryThreadIds: () => Effect.sync(() => (afterTrial ? [] : [threadId])),
                getRuntimeRecoveryProjection: () => Effect.succeed(projection),
              }),
              Layer.mock(EventSink.EventSinkV2)({
                readProviderRuntimeEvidence: () =>
                  Effect.succeed({
                    binding,
                    evidenceRevision: 3,
                    observation: null,
                    registeredAt: now,
                  }),
                findDormantRestartContinuation: (input) =>
                  Effect.sync(() => {
                    assert.deepEqual(input, {
                      threadId,
                      projectId,
                      sourceRunId: runId,
                      sourceRunAttemptId: attemptId,
                      expectedBinding: binding,
                      expectedEvidenceRevision: 3,
                    });
                    return marker;
                  }),
                prepareRestartContinuation: (input) =>
                  Effect.gen(function* () {
                    allocations++;
                    const markerId =
                      typeof input.markerId === "string" ? input.markerId : yield* input.markerId;
                    marker = {
                      markerId,
                      threadId,
                      projectId,
                      sourceRunId: runId,
                      sourceRunAttemptId: attemptId,
                      binding,
                      evidenceRevision: 3,
                      createdAt: now,
                    };
                    return marker;
                  }),
                readLegacyContinuationDisposition: () => Effect.succeed(null),
                readNativeCommandFacts: ({ commandId }) =>
                  Effect.succeed({ commitSnapshot: { threadId, commandId } } as never),
                releaseRestartContinuation: (input) =>
                  Effect.gen(function* () {
                    assert.strictEqual(input.marker, marker);
                    // The store reports any failed post-trial revalidation as unqualified.
                    yield* input.revalidateAfterTrial.pipe(
                      Effect.mapError(
                        () =>
                          new EventSink.RestartContinuationMarkerError({
                            reason: "qualification_unavailable",
                          }),
                      ),
                    );
                    releases++;
                    return true;
                  }),
                writeWithEffects: () =>
                  Effect.die("Preparing a marker must not dispatch a runnable continuation."),
              }),
              Layer.mock(EffectOutbox.EffectOutboxV2)({
                listHeldByThreadId: () => Effect.succeed([]),
                reconcileAfterProcessLoss: Effect.succeed({ requeued: 0, cancelled: 0 }),
                reconcileAfterProcessLossExcluding: () =>
                  Effect.succeed({ requeued: 0, cancelled: 0 }),
              }),
            ),
          ),
        );
        yield* Effect.gen(function* () {
          const recovery = yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService;
          const first = yield* recovery.prepareForServerUpdate({ respectProjectPreference });
          const second = yield* recovery.prepareForServerUpdate({ respectProjectPreference });
          assert.lengthOf(first, 1);
          assert.lengthOf(second, 1);
          assert.strictEqual(second[0], first[0]);
          assert.equal(observing, 1);
          assert.equal(allocations, 1);
          preferences = { ...preferences, continueThreadsAfterServerUpdate: false };
          afterTrial = true;
          const released = yield* recovery.reconcileAfterStartupTrial({
            continuationMarkers: second,
          });
          assert.deepEqual(released.releasedContinuationMarkerIds, [first[0]!.markerId]);
          assert.equal(releases, 1);
        }).pipe(Effect.provide(layer));
      }),
  );
}

for (const changed of [false, true]) {
  it.effect(
    `post-trial continuation ${changed ? "holds a changed source without synthesizing a fresh run" : "rechecks the exact source inside release"}`,
    () =>
      Effect.gen(function* () {
        const now = yield* DateTime.now;
        const threadId = ThreadId.make(`post-trial-source-${changed}`);
        const binding: EventSink.ProviderBindingExpectationV2 = {
          threadId,
          providerThreadId: ProviderThreadId.make(`post-trial-provider-thread-${changed}`),
          providerSessionId: ProviderSessionId.make(`post-trial-session-${changed}`),
          instanceId: ProviderInstanceId.make("codex"),
          driver: ProviderDriverKind.make("codex"),
          nativeThreadId: "post-trial-native-thread",
          runtimeGeneration: "prior-actual-incarnation",
        };
        const marker: EventSink.RestartContinuationMarkerV2 = {
          markerId: `post-trial-marker-${changed}`,
          threadId,
          projectId: ProjectId.make("post-trial-project"),
          sourceRunId: RunId.make("post-trial-run"),
          sourceRunAttemptId: RunAttemptId.make("post-trial-attempt"),
          binding,
          evidenceRevision: 3,
          createdAt: DateTime.formatIso(now),
        };
        const projection = {
          thread: {
            id: threadId,
            projectId: marker.projectId,
            activeProviderThreadId: binding.providerThreadId,
            modelSelection: { instanceId: binding.instanceId },
            archivedAt: null,
            deletedAt: null,
          },
          runs: [
            {
              id: marker.sourceRunId,
              activeAttemptId: marker.sourceRunAttemptId,
              providerThreadId: binding.providerThreadId,
              providerInstanceId: binding.instanceId,
              status: "completed",
              ordinal: 1,
            },
          ],
          attempts: [
            {
              id: marker.sourceRunAttemptId,
              runId: marker.sourceRunId,
              providerThreadId: binding.providerThreadId,
            },
          ],
          providerThreads: [
            {
              id: binding.providerThreadId,
              appThreadId: threadId,
              providerSessionId: binding.providerSessionId,
              providerInstanceId: binding.instanceId,
              driver: binding.driver,
              nativeThreadRef: { nativeId: binding.nativeThreadId },
              status: "idle",
            },
          ],
          nodes: [],
          subagents: [],
          messages: [],
          providerSessions: [],
          providerTurns: [],
          runtimeRequests: [],
          turnItems: [],
        } as unknown as ProjectionStore.ProjectionRuntimeRecoveryState;
        const snapshot = {
          commandId: `command:restart-continuation:${marker.markerId}`,
          threadId,
        } as unknown as EventSink.NativeCommandTargetSnapshotV2;
        let projectionReads = 0;
        let releases = 0;
        let factsReads = 0;
        const layer = ProviderRuntimeRecovery.layer.pipe(
          Layer.provide(
            Layer.mergeAll(
              ServerSettings.layerTest({ continueThreadsAfterServerUpdate: true }),
              IdAllocator.layer,
              Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({}),
              Layer.mock(ProjectionStore.ProjectionStoreV2)({
                getRecoveryThreadIds: () => Effect.succeed([]),
                getRuntimeRecoveryProjection: () =>
                  Effect.sync(() => {
                    projectionReads++;
                    return projection;
                  }),
              }),
              Layer.mock(EventSink.EventSinkV2)({
                readProviderRuntimeEvidence: () =>
                  Effect.succeed({
                    binding,
                    evidenceRevision: changed ? 4 : 3,
                    observation: null,
                    registeredAt: DateTime.formatIso(now),
                  }),
                readLegacyContinuationDisposition: () => Effect.succeed(null),
                readNativeCommandFacts: () =>
                  Effect.sync(() => {
                    factsReads++;
                    return { commitSnapshot: snapshot } as never;
                  }),
                releaseRestartContinuation: (input) =>
                  Effect.gen(function* () {
                    releases++;
                    assert.strictEqual(input.marker, marker);
                    assert.strictEqual(input.currentSnapshot, snapshot);
                    // The store reports any failed post-trial revalidation as unqualified.
                    yield* input.revalidateAfterTrial.pipe(
                      Effect.mapError(
                        () =>
                          new EventSink.RestartContinuationMarkerError({
                            reason: "qualification_unavailable",
                          }),
                      ),
                    );
                    return true;
                  }),
                commitCommand: () =>
                  Effect.die(
                    "Post-trial marker qualification must not synthesize a replacement source.",
                  ),
              }),
              Layer.mock(EffectOutbox.EffectOutboxV2)({
                listHeldByThreadId: () => Effect.succeed([]),
                reconcileAfterProcessLoss: Effect.succeed({ requeued: 0, cancelled: 0 }),
              }),
            ),
          ),
        );
        yield* Effect.gen(function* () {
          const result =
            yield* (yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService).reconcileAfterStartupTrial(
              { continuationMarkers: [marker] },
            );
          assert.deepEqual(result.releasedContinuationMarkerIds, changed ? [] : [marker.markerId]);
          assert.equal(result.heldContinuationMarkers.length, changed ? 1 : 0);
          if (changed) assert.strictEqual(result.heldContinuationMarkers[0]?.marker, marker);
          assert.equal(releases, changed ? 0 : 1);
          assert.equal(factsReads, changed ? 0 : 1);
          assert.equal(projectionReads, changed ? 1 : 2);
        }).pipe(Effect.provide(layer));
      }),
  );
}

for (const transient of [true, false]) {
  it.effect(
    `bounds ${transient ? "transient" : "deterministic"} recovery reads while excluding failed threads from outbox reconciliation`,
    () => {
      const failed = ThreadId.make(`failed-recovery-read-${transient}`);
      const later = ThreadId.make(`later-recovery-read-${transient}`);
      let failedReads = 0;
      let laterReads = 0;
      let excluded: ReadonlyArray<ThreadId> = [];
      const error = new ProjectionStore.ProjectionStoreReadError({
        threadId: failed,
        cause: transient
          ? new SqlError.SqlError({
              reason: new SqlError.LockTimeoutError({
                cause: { code: "SQLITE_BUSY" },
                operation: "read",
              }),
            })
          : new SqlError.SqlError({
              reason: new SqlError.SqlSyntaxError({ cause: null, operation: "read" }),
            }),
      });
      const layer = ProviderRuntimeRecovery.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            ServerSettings.layerTest(),
            Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({}),
            Layer.mock(ProjectionStore.ProjectionStoreV2)({
              getRecoveryThreadIds: () => Effect.succeed([failed, later]),
              getRuntimeRecoveryProjection: (threadId) =>
                Effect.suspend(() => {
                  if (threadId === failed) {
                    failedReads++;
                    return Effect.fail(error);
                  }
                  laterReads++;
                  return Effect.succeed({
                    thread: { id: later },
                    runs: [],
                    attempts: [],
                    nodes: [],
                    subagents: [],
                    messages: [],
                    providerSessions: [],
                    providerThreads: [],
                    providerTurns: [],
                    runtimeRequests: [],
                    turnItems: [],
                  } as unknown as ProjectionStore.ProjectionRuntimeRecoveryState);
                }),
            }),
            Layer.mock(EventSink.EventSinkV2)({
              commitCommand: () => Effect.die("A failed read must not mutate its projection."),
            }),
            IdAllocator.layer,
            Layer.mock(EffectOutbox.EffectOutboxV2)({
              listHeldByThreadId: () => Effect.succeed([]),
              cancelUnsettled: () => Effect.succeed([]),
              signalCancellations: () => Effect.void,
              reconcileAfterProcessLoss: Effect.die("Failed threads require scoped exclusions."),
              reconcileAfterProcessLossExcluding: ({ excludeThreadIds }) =>
                Effect.sync(() => {
                  excluded = excludeThreadIds;
                  return { requeued: 2, cancelled: 3 };
                }),
            }),
          ),
        ),
      );
      return Effect.gen(function* () {
        const result = yield* (yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService)
          .recover;
        assert.equal(failedReads, transient ? 2 : 1);
        assert.equal(laterReads, 1);
        assert.deepEqual(result.failedThreadIds, [failed]);
        assert.deepEqual(excluded, [failed]);
        assert.equal(result.requeuedEffects, 2);
        assert.equal(result.retiredEffects, 3);
      }).pipe(Effect.provide(layer));
    },
  );
}

it.effect("does not retry or suppress a recovery read defect", () => {
  const threadId = ThreadId.make("defective-recovery-read");
  let reads = 0;
  const layer = ProviderRuntimeRecovery.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        ServerSettings.layerTest(),
        Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({}),
        Layer.mock(ProjectionStore.ProjectionStoreV2)({
          getRecoveryThreadIds: () => Effect.succeed([threadId]),
          getRuntimeRecoveryProjection: () =>
            Effect.suspend(() => {
              reads++;
              return Effect.die("Synthetic read defect");
            }),
        }),
        Layer.mock(EventSink.EventSinkV2)({}),
        Layer.mock(EffectOutbox.EffectOutboxV2)({}),
        IdAllocator.layer,
      ),
    ),
  );
  return Effect.gen(function* () {
    const result =
      yield* (yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService).recover.pipe(
        Effect.exit,
      );
    assert.isTrue(Exit.isFailure(result));
    assert.equal(reads, 1);
  }).pipe(Effect.provide(layer));
});

it.effect(
  "preserves a thread with an unknown native operation through process-loss recovery",
  () => {
    const threadId = ThreadId.make("unknown-native-operation-recovery");
    const projection = {
      thread: { id: threadId },
      runs: [{ id: RunId.make("held-run"), status: "running" }],
      providerSessions: [{ id: ProviderSessionId.make("held-session"), status: "running" }],
      runtimeRequests: [],
      providerThreads: [],
      providerTurns: [],
      nodes: [],
      attempts: [],
      subagents: [],
      messages: [],
      turnItems: [],
    } as unknown as ProjectionStore.ProjectionRuntimeRecoveryState;
    const layer = ProviderRuntimeRecovery.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          ServerSettings.layerTest(),
          Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({}),
          Layer.mock(ProjectionStore.ProjectionStoreV2)({
            getRecoveryThreadIds: () => Effect.succeed([threadId]),
            getRuntimeRecoveryProjection: () => Effect.succeed(projection),
          }),
          Layer.mock(EventSink.EventSinkV2)({
            commitCommand: () =>
              Effect.die("Unknown native operations must preserve the projection."),
          }),
          IdAllocator.layer,
          Layer.mock(EffectOutbox.EffectOutboxV2)({
            listHeldByThreadId: () =>
              Effect.succeed([
                {
                  effectId: "held-effect",
                  threadId,
                  workerId: "prior-worker",
                  operationId: "unknown-operation",
                  evidence: {
                    operationId: "unknown-operation",
                    operation: "start_turn",
                    outcome: "unknown",
                  },
                  expectedAttempt: 5,
                  heldAt: "2026-10-03T12:00:00Z",
                },
              ]),
            cancelUnsettled: () =>
              Effect.die("Unknown native operations must preserve their lane hold."),
            reconcileAfterProcessLoss: Effect.succeed({ requeued: 0, cancelled: 0 }),
          }),
        ),
      ),
    );
    return Effect.gen(function* () {
      const result = yield* (yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService).recover;
      assert.equal(result.terminalizedRuns, 0);
      assert.equal(result.stoppedSessions, 0);
      assert.equal(result.retiredEffects, 0);
    }).pipe(Effect.provide(layer));
  },
);

it.effect("gathers dormant startup preparation without touching projections or outbox", () =>
  Effect.gen(function* () {
    const reconciliations = yield* Ref.make(0);
    const layer = ProviderRuntimeRecovery.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(
        Layer.mergeAll(
          Layer.mock(ProjectionStore.ProjectionStoreV2)({
            getRecoveryThreadIds: () => Effect.succeed([]),
          }),
          Layer.mock(EventSink.EventSinkV2)({
            readDormantRestartContinuations: Effect.succeed([]),
          }),
          IdAllocator.layer,
          Layer.mock(EffectOutbox.EffectOutboxV2)({
            listHeldByThreadId: () => Effect.succeed([]),
            reconcileAfterProcessLoss: Ref.update(reconciliations, (count) => count + 1).pipe(
              Effect.as({ requeued: 2, cancelled: 3 }),
            ),
          }),
        ),
      ),
    );
    const summary = yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService.pipe(
      Effect.flatMap((service) => service.stageStartupRecovery),
      Effect.provide(layer),
    );
    assert.deepEqual(summary, { continuationMarkers: [] });
    assert.equal(yield* Ref.get(reconciliations), 0);
  }),
);

it.effect("leaves durable effects for the worker after runtime reconciliation", () =>
  Effect.gen(function* () {
    const runs = yield* Ref.make(0);
    const layer = ProviderRuntimeRecovery.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(
        Layer.mergeAll(
          Layer.mock(ProjectionStore.ProjectionStoreV2)({
            getRecoveryThreadIds: () => Effect.succeed([]),
          }),
          Layer.mock(EventSink.EventSinkV2)({}),
          IdAllocator.layer,
          Layer.mock(EffectWorker.OrchestrationEffectWorkerV2)({
            runRecoveryOnce: Ref.getAndUpdate(runs, (count) => count + 1).pipe(
              Effect.map((count) => count < 2),
            ),
          }),
          Layer.mock(EffectOutbox.EffectOutboxV2)({
            listHeldByThreadId: () => Effect.succeed([]),
            reconcileAfterProcessLoss: Effect.succeed({ requeued: 0, cancelled: 0 }),
          }),
        ),
      ),
    );
    const summary = yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService.pipe(
      Effect.flatMap((recovery) => recovery.recover),
      Effect.provide(layer),
    );
    assert.deepEqual(summary, {
      terminalizedRuns: 0,
      stoppedSessions: 0,
      closedRequests: 0,
      retiredEffects: 0,
      requeuedEffects: 0,
      failedThreadIds: [],
    });
    assert.equal(yield* Ref.get(runs), 0);
  }),
);

it.effect("reads recovery projections only for threads that need runtime recovery", () => {
  const settledThreadIds = Array.from({ length: 1_000 }, (_, index) =>
    ThreadId.make(`thread_recovery_settled_${index}`),
  );
  const recoveryThreadId = ThreadId.make("thread_recovery_candidate");
  const projectionReads = vi.fn<(threadId: ThreadId) => void>();
  const layer = ProviderRuntimeRecovery.layer.pipe(
    Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
    Layer.provide(ServerSettings.layerTest()),
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(ProjectionStore.ProjectionStoreV2)({
          getShellSnapshot: () =>
            Effect.succeed({
              schemaVersion: 2,
              snapshotSequence: 0,
              threads: [...settledThreadIds, recoveryThreadId].map((id) => ({ id })),
              archivedThreads: [],
            } as never),
          getRecoveryThreadIds: () => Effect.succeed([recoveryThreadId]),
          getRuntimeRecoveryProjection: (threadId) => {
            projectionReads(threadId);
            return Effect.succeed({
              thread: { id: threadId },
              runtimeRequests: [],
              providerSessions: [],
              providerThreads: [],
              providerTurns: [],
              runs: [],
              attempts: [],
              nodes: [],
              subagents: [],
              messages: [],
              turnItems: [],
            } as unknown as OrchestrationV2ThreadProjection);
          },
        }),
        Layer.mock(EventSink.EventSinkV2)({}),
        IdAllocator.layer,
        Layer.mock(EffectWorker.OrchestrationEffectWorkerV2)({
          runRecoveryOnce: Effect.succeed(false),
        }),
        Layer.mock(EffectOutbox.EffectOutboxV2)({
          listHeldByThreadId: () => Effect.succeed([]),
          cancelUnsettled: () => Effect.succeed([]),
          signalCancellations: () => Effect.void,
          reconcileAfterProcessLoss: Effect.succeed({ requeued: 0, cancelled: 0 }),
        }),
      ),
    ),
  );

  return Effect.gen(function* () {
    yield* (yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService).recover;
    assert.deepEqual(
      projectionReads.mock.calls.map(([threadId]) => threadId),
      [recoveryThreadId],
    );
  }).pipe(Effect.provide(layer));
});

it.effect("expires orphaned runtime requests before command readiness", () => {
  const threadId = ThreadId.make("thread_recovery_requests");
  let committedInput: Parameters<EventSink.EventSinkV2["Service"]["commitCommand"]>[0] | null =
    null;
  const committed = vi.fn(
    (input: Parameters<EventSink.EventSinkV2["Service"]["commitCommand"]>[0]) => {
      committedInput = input;
      return Effect.succeed({ committed: true, cancelledEffectCount: 0 } as never);
    },
  );
  const projection = {
    thread: { id: threadId },
    runtimeRequests: [
      {
        id: RuntimeRequestId.make("request_orphaned"),
        nodeId: NodeId.make("node_orphaned"),
        status: "pending",
        responseCapability: { type: "not_resumable", reason: "old process" },
      },
    ],
    providerSessions: [],
    providerThreads: [],
    runs: [],
    nodes: [],
  } as unknown as OrchestrationV2ThreadProjection;
  const layer = ProviderRuntimeRecovery.layer.pipe(
    Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
    Layer.provide(ServerSettings.layerTest()),
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(ProjectionStore.ProjectionStoreV2)({
          getRecoveryThreadIds: () => Effect.succeed([threadId]),
          getRuntimeRecoveryProjection: () => Effect.succeed(projection),
        }),
        Layer.mock(EventSink.EventSinkV2)({ commitCommand: committed }),
        IdAllocator.layer,
        Layer.mock(EffectWorker.OrchestrationEffectWorkerV2)({
          runRecoveryOnce: Effect.succeed(false),
        }),
        Layer.mock(EffectOutbox.EffectOutboxV2)({
          listHeldByThreadId: () => Effect.succeed([]),
          reconcileAfterProcessLoss: Effect.succeed({ requeued: 0, cancelled: 0 }),
        }),
      ),
    ),
  );
  return Effect.gen(function* () {
    yield* (yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService).recover;
    const command = committedInput;
    assert.isNotNull(command);
    if (command === null) return;
    assert.equal(command?.events[0]?.type, "runtime-request.updated");
    if (command?.events[0]?.type === "runtime-request.updated") {
      assert.equal(command.events[0].payload.status, "expired");
      assert.equal(command.events[0].payload.responseCapability.type, "not_resumable");
    }
  }).pipe(Effect.provide(layer));
});

it.effect("preserves async questions across startup and shutdown", () => {
  const threadId = ThreadId.make("async-recovery-thread");
  const nodeId = NodeId.make("async-recovery-node");
  const projection = {
    thread: { id: threadId },
    runtimeRequests: [
      {
        id: RuntimeRequestId.make("async-recovery-request"),
        nodeId,
        status: "pending",
        responseCapability: { type: "message" },
      },
    ],
    providerSessions: [],
    providerThreads: [],
    runs: [],
    nodes: [],
    turnItems: [],
  } as unknown as OrchestrationV2ThreadProjection;
  const commitCommand = vi.fn(() => Effect.die("an async question needs no process-loss write"));
  const layer = ProviderRuntimeRecovery.layer.pipe(
    Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
    Layer.provide(ServerSettings.layerTest()),
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(ProjectionStore.ProjectionStoreV2)({
          getRecoveryThreadIds: () => Effect.succeed([threadId]),
          getRuntimeRecoveryProjection: () => Effect.succeed(projection),
        }),
        Layer.mock(EventSink.EventSinkV2)({ commitCommand }),
        IdAllocator.layer,
        Layer.mock(EffectWorker.OrchestrationEffectWorkerV2)({
          runRecoveryOnce: Effect.succeed(false),
        }),
        Layer.mock(EffectOutbox.EffectOutboxV2)({
          listHeldByThreadId: () => Effect.succeed([]),
          reconcileAfterProcessLoss: Effect.succeed({ requeued: 0, cancelled: 0 }),
          cancelUnsettled: () => Effect.succeed([]),
          signalCancellations: () => Effect.void,
        }),
      ),
    ),
  );
  return Effect.gen(function* () {
    const recovery = yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService;
    assert.equal((yield* recovery.reconcile("startup")).closedRequests, 0);
    assert.equal((yield* recovery.reconcile("startup")).closedRequests, 0);
    assert.isFalse(commitCommand.mock.calls.length > 0);
  }).pipe(Effect.provide(layer));
});

it.effect("uses the same reconciliation path to cancel runtime requests during shutdown", () => {
  const threadId = ThreadId.make("thread_shutdown_requests");
  let committedInput: Parameters<EventSink.EventSinkV2["Service"]["commitCommand"]>[0] | null =
    null;
  const projection = {
    thread: { id: threadId },
    runtimeRequests: [
      {
        id: RuntimeRequestId.make("request_shutdown"),
        nodeId: NodeId.make("node_shutdown"),
        status: "pending",
        responseCapability: { type: "live" },
      },
    ],
    providerSessions: [],
    providerThreads: [],
    runs: [],
    nodes: [],
  } as unknown as OrchestrationV2ThreadProjection;
  const layer = ProviderRuntimeRecovery.layer.pipe(
    Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
    Layer.provide(ServerSettings.layerTest()),
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(ProjectionStore.ProjectionStoreV2)({
          getRecoveryThreadIds: () => Effect.succeed([threadId]),
          getRuntimeRecoveryProjection: () => Effect.succeed(projection),
        }),
        Layer.mock(EventSink.EventSinkV2)({
          commitCommand: (input) => {
            committedInput = input;
            return Effect.succeed({ committed: true, cancelledEffectCount: 1 } as never);
          },
        }),
        IdAllocator.layer,
        Layer.mock(EffectWorker.OrchestrationEffectWorkerV2)({
          runRecoveryOnce: Effect.succeed(false),
        }),
        Layer.mock(EffectOutbox.EffectOutboxV2)({
          listHeldByThreadId: () => Effect.succeed([]),
          reconcileAfterProcessLoss: Effect.succeed({ requeued: 0, cancelled: 0 }),
        }),
      ),
    ),
  );

  return Effect.gen(function* () {
    const summary =
      yield* (yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService).reconcile("shutdown");
    assert.equal(summary.closedRequests, 1);
    assert.equal(summary.retiredEffects, 1);
    const requestEvent = committedInput?.events[0];
    assert.equal(requestEvent?.type, "runtime-request.updated");
    if (requestEvent?.type === "runtime-request.updated") {
      assert.equal(requestEvent.payload.status, "cancelled");
      assert.equal(requestEvent.payload.responseCapability.type, "not_resumable");
      if (requestEvent.payload.responseCapability.type === "not_resumable") {
        assert.match(requestEvent.payload.responseCapability.reason, /shut down/);
      }
    }
  }).pipe(Effect.provide(layer));
});

it.effect(
  "preserves a replayable waiting run while cancelling its process-bound background work",
  () => {
    const threadId = ThreadId.make("thread_waiting_checkpoint");
    const runId = RunId.make("run_waiting_checkpoint");
    const providerThreadId = ProviderThreadId.make("provider_thread_waiting_checkpoint");
    const providerInstanceId = ProviderInstanceId.make("claude");
    const backgroundItemId = TurnItemId.make("turn_item_waiting_checkpoint");
    let committedInput: Parameters<EventSink.EventSinkV2["Service"]["commitCommand"]>[0] | null =
      null;
    const projection = {
      thread: { id: threadId },
      runtimeRequests: [],
      providerSessions: [],
      providerThreads: [
        {
          id: providerThreadId,
          driver: ProviderDriverKind.make("claude"),
          providerInstanceId,
          status: "idle",
          pendingBackgroundTasks: [
            { taskId: String(backgroundItemId), description: "Finish background task" },
          ],
        },
      ],
      runs: [{ id: runId, status: "waiting", providerInstanceId, providerThreadId }],
      attempts: [],
      nodes: [],
      subagents: [],
      messages: [],
      turnItems: [
        {
          id: backgroundItemId,
          runId,
          nodeId: null,
          providerThreadId,
          type: "command_execution",
          status: "running",
        },
      ],
    } as unknown as OrchestrationV2ThreadProjection;
    const layer = ProviderRuntimeRecovery.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(
        Layer.mergeAll(
          Layer.mock(ProjectionStore.ProjectionStoreV2)({
            getRecoveryThreadIds: () => Effect.succeed([threadId]),
            getRuntimeRecoveryProjection: () => Effect.succeed(projection),
          }),
          Layer.mock(EventSink.EventSinkV2)({
            commitCommand: (input) => {
              committedInput = input;
              return Effect.succeed({
                committed: true,
                cancelledEffectCount: 0,
              } as never);
            },
          }),
          IdAllocator.layer,
          Layer.mock(EffectWorker.OrchestrationEffectWorkerV2)({
            runRecoveryOnce: Effect.succeed(false),
          }),
          Layer.mock(EffectOutbox.EffectOutboxV2)({
            listHeldByThreadId: () => Effect.succeed([]),
            listByCommandId: () =>
              Effect.succeed([
                {
                  request: { type: "checkpoint.capture", runId },
                  status: "running",
                },
              ] as never),
            reconcileAfterProcessLoss: Effect.succeed({ requeued: 1, cancelled: 0 }),
          }),
        ),
      ),
    );

    return Effect.gen(function* () {
      const summary =
        yield* (yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService).reconcile("startup");
      assert.equal(summary.terminalizedRuns, 0);
      assert.equal(summary.requeuedEffects, 1);

      const command = committedInput;
      assert.isNotNull(command);
      if (command === null) return;
      // The run stays waiting for its checkpoint; it only records the work
      // the restart cancelled so the next provider turn can be told.
      assert.isFalse(command.events.some((event) => event.type === "run.updated"));
      const recorded = command.events.flatMap((event) =>
        event.type === "run.background-work-cancelled" ? [event.payload] : [],
      );
      // The roster entry is the same task as the item, so it is listed once.
      assert.deepEqual(
        recorded.map((entry) => ({
          runId: entry.runId,
          kinds: entry.restartCancelledBackgroundWork.map((work) => work.kind),
        })),
        [{ runId, kinds: ["shell"] }],
      );
      assert.isTrue(
        command.events.some(
          (event) =>
            event.type === "turn-item.updated" &&
            event.payload.id === backgroundItemId &&
            event.payload.status === "cancelled",
        ),
      );
      assert.isTrue(
        command.events.some(
          (event) =>
            event.type === "provider-thread.updated" &&
            event.payload.id === providerThreadId &&
            event.payload.pendingBackgroundTasks?.length === 0,
        ),
      );
    }).pipe(Effect.provide(layer));
  },
);

it.effect(
  "records cancelled work on the provider thread that lost it after a provider switch",
  () => {
    const threadId = ThreadId.make("thread_switch_note");
    const claudeRunId = RunId.make("run_switch_note_claude");
    const codexRunId = RunId.make("run_switch_note_codex");
    const claudeThreadId = ProviderThreadId.make("provider_thread_switch_note_claude");
    const codexThreadId = ProviderThreadId.make("provider_thread_switch_note_codex");
    const claude = ProviderInstanceId.make("claude");
    const codex = ProviderInstanceId.make("codex");
    let committedInput: Parameters<EventSink.EventSinkV2["Service"]["commitCommand"]>[0] | null =
      null;
    // Claude launched a background subagent, then the thread switched to Codex.
    const projection = {
      thread: { id: threadId },
      runtimeRequests: [],
      providerSessions: [],
      providerThreads: [
        {
          id: claudeThreadId,
          ownerNodeId: null,
          driver: ProviderDriverKind.make("claudeAgent"),
          providerInstanceId: claude,
          status: "idle",
          pendingBackgroundTasks: [{ taskId: "task-claude", description: "Watch the build" }],
        },
        {
          id: codexThreadId,
          ownerNodeId: null,
          driver: ProviderDriverKind.make("codex"),
          providerInstanceId: codex,
          status: "idle",
        },
      ],
      providerTurns: [],
      runs: [
        {
          id: claudeRunId,
          ordinal: 1,
          status: "completed",
          providerInstanceId: claude,
          providerThreadId: claudeThreadId,
        },
        {
          id: codexRunId,
          ordinal: 2,
          status: "completed",
          providerInstanceId: codex,
          providerThreadId: codexThreadId,
        },
      ],
      attempts: [],
      nodes: [],
      subagents: [],
      messages: [],
      turnItems: [
        {
          id: TurnItemId.make("turn_item_switch_note_subagent"),
          runId: claudeRunId,
          nodeId: null,
          providerThreadId: claudeThreadId,
          nativeItemRef: null,
          type: "subagent",
          title: "Background subagent",
          status: "running",
        },
      ],
    } as unknown as OrchestrationV2ThreadProjection;
    const layer = ProviderRuntimeRecovery.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(
        Layer.mergeAll(
          Layer.mock(ProjectionStore.ProjectionStoreV2)({
            getRecoveryThreadIds: () => Effect.succeed([threadId]),
            getRuntimeRecoveryProjection: () => Effect.succeed(projection),
          }),
          Layer.mock(EventSink.EventSinkV2)({
            commitCommand: (input) => {
              committedInput = input;
              return Effect.succeed({ committed: true, cancelledEffectCount: 0 } as never);
            },
          }),
          IdAllocator.layer,
          Layer.mock(EffectWorker.OrchestrationEffectWorkerV2)({
            runRecoveryOnce: Effect.succeed(false),
          }),
          Layer.mock(EffectOutbox.EffectOutboxV2)({
            listHeldByThreadId: () => Effect.succeed([]),
            reconcileAfterProcessLoss: Effect.succeed({ requeued: 0, cancelled: 0 }),
          }),
        ),
      ),
    );

    return Effect.gen(function* () {
      yield* (yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService).reconcile("startup");
      const recorded = (committedInput?.events ?? []).flatMap((event) =>
        event.type === "run.background-work-cancelled" ? [event.payload] : [],
      );
      // The Codex run is later, but only Claude's turns may be told about it.
      assert.deepEqual(
        recorded.map((entry) => ({
          runId: entry.runId,
          kinds: entry.restartCancelledBackgroundWork.map((work) => work.kind),
        })),
        [{ runId: claudeRunId, kinds: ["subagent", "task"] }],
      );
    }).pipe(Effect.provide(layer));
  },
);

it.effect("cancels a stale waiting run when no checkpoint capture can finish it", () => {
  const threadId = ThreadId.make("thread_stale_waiting");
  const runId = RunId.make("run_stale_waiting");
  let committedInput: Parameters<EventSink.EventSinkV2["Service"]["commitCommand"]>[0] | null =
    null;
  const projection = {
    thread: { id: threadId },
    runtimeRequests: [],
    providerSessions: [],
    providerThreads: [],
    providerTurns: [],
    runs: [
      {
        id: runId,
        status: "waiting",
        providerInstanceId: ProviderInstanceId.make("codex"),
      },
    ],
    attempts: [],
    nodes: [],
    subagents: [],
    messages: [],
    turnItems: [],
  } as unknown as OrchestrationV2ThreadProjection;
  const layer = ProviderRuntimeRecovery.layer.pipe(
    Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
    Layer.provide(ServerSettings.layerTest()),
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(ProjectionStore.ProjectionStoreV2)({
          getRecoveryThreadIds: () => Effect.succeed([threadId]),
          getRuntimeRecoveryProjection: () => Effect.succeed(projection),
        }),
        Layer.mock(EventSink.EventSinkV2)({
          commitCommand: (input) => {
            committedInput = input;
            return Effect.succeed({ committed: true, cancelledEffectCount: 0 } as never);
          },
        }),
        IdAllocator.layer,
        Layer.mock(EffectWorker.OrchestrationEffectWorkerV2)({
          runRecoveryOnce: Effect.succeed(false),
        }),
        Layer.mock(EffectOutbox.EffectOutboxV2)({
          listHeldByThreadId: () => Effect.succeed([]),
          listByCommandId: () => Effect.succeed([]),
          reconcileAfterProcessLoss: Effect.succeed({ requeued: 0, cancelled: 0 }),
        }),
      ),
    ),
  );

  return Effect.gen(function* () {
    const summary =
      yield* (yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService).reconcile("startup");
    assert.equal(summary.terminalizedRuns, 1);
    const runEvent = committedInput?.events.find((event) => event.type === "run.updated");
    assert.equal(runEvent?.type === "run.updated" ? runEvent.payload.status : null, "cancelled");
  }).pipe(Effect.provide(layer));
});

it.effect("holds accepted queued work without cancelling its execution state after restart", () => {
  const threadId = ThreadId.make("thread_queued_restart");
  const runId = RunId.make("run_queued_restart");
  const attemptId = RunAttemptId.make("attempt_queued_restart");
  const rootNodeId = NodeId.make("node_queued_restart");
  let committedInput: Parameters<EventSink.EventSinkV2["Service"]["commitCommand"]>[0] | null =
    null;
  const projection = {
    thread: { id: threadId },
    runtimeRequests: [],
    providerSessions: [],
    providerThreads: [],
    providerTurns: [],
    runs: [
      {
        id: runId,
        status: "queued",
        queuePosition: 1,
        providerInstanceId: ProviderInstanceId.make("codex"),
      },
    ],
    attempts: [
      {
        id: attemptId,
        runId,
        rootNodeId,
        status: "pending",
      },
    ],
    nodes: [
      {
        id: rootNodeId,
        runId,
        status: "pending",
      },
    ],
    subagents: [],
    messages: [],
    turnItems: [],
  } as unknown as OrchestrationV2ThreadProjection;
  const layer = ProviderRuntimeRecovery.layer.pipe(
    Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
    Layer.provide(ServerSettings.layerTest()),
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(ProjectionStore.ProjectionStoreV2)({
          getRecoveryThreadIds: () => Effect.succeed([threadId]),
          getRuntimeRecoveryProjection: () => Effect.succeed(projection),
        }),
        Layer.mock(EventSink.EventSinkV2)({
          commitCommand: (input) => {
            committedInput = input;
            return Effect.succeed({ committed: true, cancelledEffectCount: 0 } as never);
          },
        }),
        IdAllocator.layer,
        Layer.mock(EffectWorker.OrchestrationEffectWorkerV2)({
          runRecoveryOnce: Effect.succeed(false),
        }),
        Layer.mock(EffectOutbox.EffectOutboxV2)({
          listHeldByThreadId: () => Effect.succeed([]),
          reconcileAfterProcessLoss: Effect.succeed({ requeued: 0, cancelled: 0 }),
        }),
      ),
    ),
  );

  return Effect.gen(function* () {
    const summary =
      yield* (yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService).reconcile("startup");
    assert.equal(summary.terminalizedRuns, 0);
    const command = committedInput;
    assert.isNotNull(command);
    if (command === null) return;
    const runEvent = command.events.find((event) => event.type === "run.updated");
    const attemptEvent = command.events.find((event) => event.type === "run-attempt.updated");
    const nodeEvent = command.events.find((event) => event.type === "node.updated");
    assert.equal(runEvent?.type === "run.updated" ? runEvent.payload.status : null, "queued");
    assert.equal(runEvent?.type === "run.updated" ? runEvent.payload.queuePosition : null, 1);
    assert.equal(runEvent?.type === "run.updated" ? runEvent.payload.queueHeld : false, true);
    assert.isUndefined(attemptEvent);
    assert.isUndefined(nodeEvent);
  }).pipe(Effect.provide(layer));
});

it.effect(
  "cancels the complete in-flight subtree and stops its persisted session without reopening it",
  () => {
    const threadId = ThreadId.make("thread_recovery_cancel");
    const runId = RunId.make("run_recovery_cancel");
    const attemptId = RunAttemptId.make("attempt_recovery_cancel");
    const rootNodeId = NodeId.make("node_recovery_cancel");
    const providerThreadId = ProviderThreadId.make("provider_thread_recovery_cancel");
    const providerTurnId = ProviderTurnId.make("provider_turn_recovery_cancel");
    const providerSessionId = ProviderSessionId.make("provider_session_recovery_cancel");
    let committedInput: Parameters<EventSink.EventSinkV2["Service"]["commitCommand"]>[0] | null =
      null;
    const projection = {
      thread: { id: threadId },
      runtimeRequests: [],
      providerSessions: [
        {
          id: providerSessionId,
          driver: ProviderDriverKind.make("codex"),
          providerInstanceId: ProviderInstanceId.make("codex"),
          status: "ready",
        },
      ],
      providerThreads: [
        {
          id: providerThreadId,
          driver: ProviderDriverKind.make("codex"),
          providerInstanceId: ProviderInstanceId.make("codex"),
          status: "active",
        },
      ],
      providerTurns: [
        {
          id: providerTurnId,
          runAttemptId: attemptId,
          nodeId: rootNodeId,
          status: "running",
        },
      ],
      runs: [
        {
          id: runId,
          status: "starting",
          providerThreadId,
          providerInstanceId: ProviderInstanceId.make("codex"),
        },
      ],
      attempts: [
        {
          id: attemptId,
          runId,
          rootNodeId,
          status: "running",
        },
      ],
      nodes: [{ id: rootNodeId, runId, status: "running" }],
      subagents: [],
      messages: [{ id: MessageId.make("message_recovery_cancel"), runId, streaming: true }],
      turnItems: [
        {
          id: TurnItemId.make("turn_item_recovery_cancel"),
          runId,
          nodeId: rootNodeId,
          status: "running",
        },
      ],
    } as unknown as OrchestrationV2ThreadProjection;
    const layer = ProviderRuntimeRecovery.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(
        Layer.mergeAll(
          Layer.mock(ProjectionStore.ProjectionStoreV2)({
            getRecoveryThreadIds: () => Effect.succeed([threadId]),
            getRuntimeRecoveryProjection: () => Effect.succeed(projection),
          }),
          Layer.mock(EventSink.EventSinkV2)({
            commitCommand: (input) => {
              committedInput = input;
              return Effect.succeed({ committed: true, cancelledEffectCount: 2 } as never);
            },
          }),
          IdAllocator.layer,
          Layer.mock(EffectWorker.OrchestrationEffectWorkerV2)({
            runRecoveryOnce: Effect.succeed(false),
          }),
          Layer.mock(EffectOutbox.EffectOutboxV2)({
            listHeldByThreadId: () => Effect.succeed([]),
            reconcileAfterProcessLoss: Effect.succeed({ requeued: 0, cancelled: 0 }),
          }),
        ),
      ),
    );

    return Effect.gen(function* () {
      const summary = yield* (yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService)
        .recover;
      assert.equal(summary.terminalizedRuns, 1);
      assert.equal(summary.stoppedSessions, 1);
      assert.equal(summary.retiredEffects, 2);
      assert.deepEqual(committedInput?.cancelUnsettledEffects?.effectTypes, [
        "worktree.cleanup",
        "provider-turn.start",
        "provider-turn.interrupt",
        "provider-turn.steer",
        "provider-turn.restart",
        "runtime-request.respond",
      ]);
      const events = committedInput?.events ?? [];
      assert.deepEqual(
        events.map((event) => [
          event.type,
          "status" in event.payload ? event.payload.status : null,
        ]),
        [
          ["run.updated", "cancelled"],
          ["run-attempt.updated", "cancelled"],
          ["node.updated", "cancelled"],
          ["provider-turn.updated", "cancelled"],
          ["message.updated", null],
          ["turn-item.updated", "cancelled"],
          ["provider-thread.updated", "idle"],
          ["provider-session.updated", "stopped"],
        ],
      );
      const messageEvent = events.find((event) => event.type === "message.updated");
      assert.isFalse(messageEvent?.type === "message.updated" && messageEvent.payload.streaming);
    }).pipe(Effect.provide(layer));
  },
);

it.effect(
  "clears persisted pendingBackgroundTasks and terminalizes stale background items on settled runs",
  () => {
    const threadId = ThreadId.make("thread_recovery_background");
    const settledRunId = RunId.make("run_recovery_background_settled");
    const activeRunId = RunId.make("run_recovery_background_active");
    const activeAttemptId = RunAttemptId.make("attempt_recovery_background_active");
    const activeRootNodeId = NodeId.make("node_recovery_background_active");
    const idleProviderThreadId = ProviderThreadId.make("provider_thread_recovery_background_idle");
    const activeProviderThreadId = ProviderThreadId.make(
      "provider_thread_recovery_background_active",
    );
    const secondaryProviderThreadId = ProviderThreadId.make(
      "provider_thread_recovery_background_secondary",
    );
    const providerSessionId = ProviderSessionId.make("provider_session_recovery_background");
    const settledStaleItemId = TurnItemId.make("turn_item_recovery_background_stale");
    const activeRunItemId = TurnItemId.make("turn_item_recovery_background_active");
    const nullRunCommandItemId = TurnItemId.make("turn_item_recovery_background_null_run");
    const nullRunSubagentItemId = TurnItemId.make("turn_item_recovery_background_null_subagent");
    const claudeInstanceId = ProviderInstanceId.make("claude");
    const secondaryInstanceId = ProviderInstanceId.make("claude-secondary");
    const subagentInstanceId = ProviderInstanceId.make("claude-subagent");
    let committedInput: Parameters<EventSink.EventSinkV2["Service"]["commitCommand"]>[0] | null =
      null;
    const projection = {
      thread: { id: threadId },
      runtimeRequests: [],
      providerSessions: [
        {
          id: providerSessionId,
          driver: ProviderDriverKind.make("claude"),
          providerInstanceId: claudeInstanceId,
          status: "ready",
        },
      ],
      providerThreads: [
        {
          id: idleProviderThreadId,
          driver: ProviderDriverKind.make("claude"),
          // Index-0 is intentionally a different instance so misattribution
          // to providerThreads[0] fails the assertions below.
          providerInstanceId: claudeInstanceId,
          status: "idle",
          pendingBackgroundTasks: [{ taskId: "bg-settled", description: "sleep 30" }],
        },
        {
          id: activeProviderThreadId,
          driver: ProviderDriverKind.make("claude"),
          providerInstanceId: claudeInstanceId,
          status: "active",
          pendingBackgroundTasks: [{ taskId: "bg-active", description: "npm test" }],
        },
        {
          id: secondaryProviderThreadId,
          driver: ProviderDriverKind.make("claude"),
          providerInstanceId: secondaryInstanceId,
          status: "idle",
          pendingBackgroundTasks: [],
        },
      ],
      providerTurns: [],
      runs: [
        {
          id: settledRunId,
          status: "completed",
          providerInstanceId: claudeInstanceId,
        },
        {
          id: activeRunId,
          status: "running",
          providerInstanceId: claudeInstanceId,
        },
      ],
      attempts: [
        {
          id: activeAttemptId,
          runId: activeRunId,
          rootNodeId: activeRootNodeId,
          status: "running",
        },
      ],
      nodes: [{ id: activeRootNodeId, runId: activeRunId, status: "running" }],
      subagents: [],
      messages: [],
      turnItems: [
        {
          id: settledStaleItemId,
          runId: settledRunId,
          nodeId: null,
          providerThreadId: idleProviderThreadId,
          type: "command_execution",
          status: "running",
        },
        {
          id: activeRunItemId,
          runId: activeRunId,
          nodeId: activeRootNodeId,
          providerThreadId: activeProviderThreadId,
          type: "dynamic_tool",
          status: "running",
        },
        {
          // Missing run: must attribute via providerThreadId, not index 0.
          id: nullRunCommandItemId,
          runId: null,
          nodeId: null,
          providerThreadId: secondaryProviderThreadId,
          type: "command_execution",
          status: "running",
        },
        {
          // Missing run with a real matching provider thread whose instance
          // differs from the subagent's own: own providerInstanceId must win.
          id: nullRunSubagentItemId,
          runId: null,
          nodeId: null,
          providerThreadId: secondaryProviderThreadId,
          type: "subagent",
          status: "running",
          providerInstanceId: subagentInstanceId,
        },
      ],
    } as unknown as OrchestrationV2ThreadProjection;
    const layer = ProviderRuntimeRecovery.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(
        Layer.mergeAll(
          Layer.mock(ProjectionStore.ProjectionStoreV2)({
            getRecoveryThreadIds: () => Effect.succeed([threadId]),
            getRuntimeRecoveryProjection: () => Effect.succeed(projection),
          }),
          Layer.mock(EventSink.EventSinkV2)({
            commitCommand: (input) => {
              committedInput = input;
              return Effect.succeed({ committed: true, cancelledEffectCount: 0 } as never);
            },
          }),
          IdAllocator.layer,
          Layer.mock(EffectWorker.OrchestrationEffectWorkerV2)({
            runRecoveryOnce: Effect.succeed(false),
          }),
          Layer.mock(EffectOutbox.EffectOutboxV2)({
            listHeldByThreadId: () => Effect.succeed([]),
            listByCommandId: () => Effect.succeed([]),
            reconcileAfterProcessLoss: Effect.succeed({ requeued: 0, cancelled: 0 }),
          }),
        ),
      ),
    );

    return Effect.gen(function* () {
      const summary =
        yield* (yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService).reconcile("startup");
      assert.equal(summary.terminalizedRuns, 1);
      const events = committedInput?.events ?? [];

      const turnItemCancels = events.filter(
        (event) => event.type === "turn-item.updated" && event.payload.status === "cancelled",
      );
      // Active-run item + settled-run stale + null-run command + null-run subagent.
      assert.equal(turnItemCancels.length, 4);
      assert.deepEqual(
        turnItemCancels
          .map((event) => event.type === "turn-item.updated" && event.payload.id)
          .sort(),
        [activeRunItemId, nullRunCommandItemId, nullRunSubagentItemId, settledStaleItemId].sort(),
      );

      const cancelById = (id: TurnItemId) =>
        turnItemCancels.find(
          (event) => event.type === "turn-item.updated" && event.payload.id === id,
        );
      assert.equal(cancelById(nullRunCommandItemId)?.providerInstanceId, secondaryInstanceId);
      // Subagent own instance wins over the matching thread's secondary instance.
      assert.notEqual(subagentInstanceId, secondaryInstanceId);
      assert.equal(cancelById(nullRunSubagentItemId)?.providerInstanceId, subagentInstanceId);
      // Settled-run item still prefers the run's provider instance when present.
      assert.equal(cancelById(settledStaleItemId)?.providerInstanceId, claudeInstanceId);

      const providerThreadEvents = events.filter(
        (event) => event.type === "provider-thread.updated",
      );
      // Only threads with active status or nonempty rosters are rewritten.
      assert.equal(providerThreadEvents.length, 2);
      for (const event of providerThreadEvents) {
        if (event.type !== "provider-thread.updated") continue;
        assert.deepEqual(event.payload.pendingBackgroundTasks ?? [], []);
      }
      const idleThreadEvent = providerThreadEvents.find(
        (event) =>
          event.type === "provider-thread.updated" && event.payload.id === idleProviderThreadId,
      );
      assert.equal(
        idleThreadEvent?.type === "provider-thread.updated" ? idleThreadEvent.payload.status : null,
        "idle",
      );
      const activeThreadEvent = providerThreadEvents.find(
        (event) =>
          event.type === "provider-thread.updated" && event.payload.id === activeProviderThreadId,
      );
      assert.equal(
        activeThreadEvent?.type === "provider-thread.updated"
          ? activeThreadEvent.payload.status
          : null,
        "idle",
      );
    }).pipe(Effect.provide(layer));
  },
);

it.effect(
  "terminalizes a leftover nonpersistent dynamic_tool on a settled run after process loss",
  () => {
    const threadId = ThreadId.make("thread_recovery_orphan_wait");
    const settledRunId = RunId.make("run_recovery_orphan_wait_settled");
    const providerThreadId = ProviderThreadId.make("provider_thread_recovery_orphan_wait");
    const orphanWaitItemId = TurnItemId.make(
      "turn-item:provider:codex:native-item:exec-4669f3bb-78c9-4af1-b44e-daa340d2c538",
    );
    const persistentMonitorItemId = TurnItemId.make(
      "turn-item:provider:codex:native-item:exec-persistent-monitor",
    );
    const orphanWaitNodeId = NodeId.make("node_recovery_orphan_wait");
    const persistentMonitorNodeId = NodeId.make("node_recovery_persistent_monitor");
    const codexInstanceId = ProviderInstanceId.make("codex");
    let committedInput: Parameters<EventSink.EventSinkV2["Service"]["commitCommand"]>[0] | null =
      null;
    const projection = {
      thread: { id: threadId, providerInstanceId: codexInstanceId },
      runtimeRequests: [],
      providerSessions: [],
      providerThreads: [
        {
          id: providerThreadId,
          driver: ProviderDriverKind.make("codex"),
          providerInstanceId: codexInstanceId,
          status: "idle",
          pendingBackgroundTasks: [],
        },
      ],
      providerTurns: [],
      runs: [{ id: settledRunId, status: "completed", providerInstanceId: codexInstanceId }],
      attempts: [],
      nodes: [
        { id: orphanWaitNodeId, runId: settledRunId, status: "running", kind: "tool_call" },
        {
          id: persistentMonitorNodeId,
          runId: settledRunId,
          status: "running",
          kind: "tool_call",
        },
      ],
      subagents: [],
      messages: [],
      turnItems: [
        {
          id: orphanWaitItemId,
          runId: settledRunId,
          nodeId: orphanWaitNodeId,
          providerThreadId,
          type: "dynamic_tool",
          status: "running",
          toolName: "t3-code.t3_thread_wait",
          input: {
            threadId:
              "thread:delegated-task:command%3Amcp%3Aaafffab1-e811-458a-ae83-558e542c61ff%3Adelegate-task%3Areview-mobile-reconnect-opus-20260815",
            timeoutMs: 30000,
          },
        },
        {
          id: persistentMonitorItemId,
          runId: settledRunId,
          nodeId: persistentMonitorNodeId,
          providerThreadId,
          type: "dynamic_tool",
          status: "running",
          toolName: "grok.monitor",
          input: { persistent: true, command: "tail -f" },
        },
      ],
    } as unknown as OrchestrationV2ThreadProjection;
    const layer = ProviderRuntimeRecovery.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(
        Layer.mergeAll(
          Layer.mock(ProjectionStore.ProjectionStoreV2)({
            getRecoveryThreadIds: () => Effect.succeed([threadId]),
            getRuntimeRecoveryProjection: () => Effect.succeed(projection),
          }),
          Layer.mock(EventSink.EventSinkV2)({
            commitCommand: (input) => {
              committedInput = input;
              return Effect.succeed({ committed: true, cancelledEffectCount: 0 } as never);
            },
          }),
          IdAllocator.layer,
          Layer.mock(EffectWorker.OrchestrationEffectWorkerV2)({
            runRecoveryOnce: Effect.succeed(false),
          }),
          Layer.mock(EffectOutbox.EffectOutboxV2)({
            listHeldByThreadId: () => Effect.succeed([]),
            listByCommandId: () => Effect.succeed([]),
            reconcileAfterProcessLoss: Effect.succeed({ requeued: 0, cancelled: 0 }),
          }),
        ),
      ),
    );

    return Effect.gen(function* () {
      const summary =
        yield* (yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService).reconcile("startup");
      assert.equal(summary.terminalizedRuns, 0);
      const turnItemCancels = (committedInput?.events ?? []).filter(
        (event) => event.type === "turn-item.updated" && event.payload.status === "cancelled",
      );
      // Process loss means the provider is gone, so even a persistent monitor
      // cannot still be alive. The leftover wait and the monitor both close.
      assert.equal(turnItemCancels.length, 2);
      assert.deepEqual(
        turnItemCancels
          .map((event) => event.type === "turn-item.updated" && event.payload.id)
          .sort(),
        [orphanWaitItemId, persistentMonitorItemId].sort(),
      );
      const nodeCancels = (committedInput?.events ?? []).filter(
        (event) => event.type === "node.updated" && event.payload.status === "cancelled",
      );
      assert.equal(nodeCancels.length, 2);
      assert.deepEqual(
        nodeCancels.map((event) => event.type === "node.updated" && event.payload.id).sort(),
        [orphanWaitNodeId, persistentMonitorNodeId].sort(),
      );
    }).pipe(Effect.provide(layer));
  },
);

it.effect(
  "terminalizes the linked subagent and node for a stale subagent item on a settled run",
  () => {
    const threadId = ThreadId.make("thread_recovery_subagent");
    const settledRunId = RunId.make("run_recovery_subagent_settled");
    const providerThreadId = ProviderThreadId.make("provider_thread_recovery_subagent");
    const staleSubagentNodeId = NodeId.make("node_recovery_subagent_stale");
    const doneSubagentNodeId = NodeId.make("node_recovery_subagent_done");
    const staleItemId = TurnItemId.make("turn_item_recovery_subagent_stale");
    const doneItemId = TurnItemId.make("turn_item_recovery_subagent_done");
    const claudeInstanceId = ProviderInstanceId.make("claude");
    let committedInput: Parameters<EventSink.EventSinkV2["Service"]["commitCommand"]>[0] | null =
      null;
    const projection = {
      thread: { id: threadId },
      runtimeRequests: [],
      providerSessions: [],
      providerThreads: [
        {
          id: providerThreadId,
          driver: ProviderDriverKind.make("claude"),
          providerInstanceId: claudeInstanceId,
          status: "idle",
          pendingBackgroundTasks: [],
        },
      ],
      providerTurns: [],
      // Settled run: the stale-item loop owns it, not the nonterminal loop.
      runs: [{ id: settledRunId, status: "completed", providerInstanceId: claudeInstanceId }],
      attempts: [],
      nodes: [
        { id: staleSubagentNodeId, runId: settledRunId, status: "running" },
        { id: doneSubagentNodeId, runId: settledRunId, status: "completed" },
      ],
      subagents: [
        {
          id: staleSubagentNodeId,
          runId: settledRunId,
          driver: ProviderDriverKind.make("claude"),
          providerInstanceId: claudeInstanceId,
          status: "running",
        },
        {
          // Already finished with a real result: must never be overwritten.
          id: doneSubagentNodeId,
          runId: settledRunId,
          driver: ProviderDriverKind.make("claude"),
          providerInstanceId: claudeInstanceId,
          status: "completed",
          result: "done",
        },
      ],
      messages: [],
      turnItems: [
        {
          id: staleItemId,
          runId: settledRunId,
          nodeId: staleSubagentNodeId,
          providerThreadId,
          type: "subagent",
          status: "running",
          subagentId: staleSubagentNodeId,
          providerInstanceId: claudeInstanceId,
        },
        {
          id: doneItemId,
          runId: settledRunId,
          nodeId: doneSubagentNodeId,
          providerThreadId,
          type: "subagent",
          status: "completed",
          subagentId: doneSubagentNodeId,
          providerInstanceId: claudeInstanceId,
        },
      ],
    } as unknown as OrchestrationV2ThreadProjection;
    const layer = ProviderRuntimeRecovery.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(
        Layer.mergeAll(
          Layer.mock(ProjectionStore.ProjectionStoreV2)({
            getRecoveryThreadIds: () => Effect.succeed([threadId]),
            getRuntimeRecoveryProjection: () => Effect.succeed(projection),
          }),
          Layer.mock(EventSink.EventSinkV2)({
            commitCommand: (input) => {
              committedInput = input;
              return Effect.succeed({ committed: true, cancelledEffectCount: 0 } as never);
            },
          }),
          IdAllocator.layer,
          Layer.mock(EffectWorker.OrchestrationEffectWorkerV2)({
            runRecoveryOnce: Effect.succeed(false),
          }),
          Layer.mock(EffectOutbox.EffectOutboxV2)({
            listHeldByThreadId: () => Effect.succeed([]),
            listByCommandId: () => Effect.succeed([]),
            reconcileAfterProcessLoss: Effect.succeed({ requeued: 0, cancelled: 0 }),
          }),
        ),
      ),
    );

    return Effect.gen(function* () {
      yield* (yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService).reconcile("startup");
      const events = committedInput?.events ?? [];

      // Only the nonterminal subagent item is cancelled.
      const turnItemCancels = events.filter(
        (event) => event.type === "turn-item.updated" && event.payload.status === "cancelled",
      );
      assert.equal(turnItemCancels.length, 1);

      // The linked subagent entity is terminalized alongside its turn item.
      const subagentCancels = events.filter((event) => event.type === "subagent.updated");
      assert.equal(subagentCancels.length, 1);
      const subagentCancel = subagentCancels[0];
      assert.equal(
        subagentCancel?.type === "subagent.updated" ? subagentCancel.payload.id : null,
        staleSubagentNodeId,
      );
      assert.equal(
        subagentCancel?.type === "subagent.updated" ? subagentCancel.payload.status : null,
        "cancelled",
      );

      // So is its execution node, which no live process can terminalize.
      const nodeCancels = events.filter((event) => event.type === "node.updated");
      assert.equal(nodeCancels.length, 1);
      const nodeCancel = nodeCancels[0];
      assert.equal(
        nodeCancel?.type === "node.updated" ? nodeCancel.payload.id : null,
        staleSubagentNodeId,
      );

      // The already-completed subagent and node are left untouched.
      assert.isFalse(
        events.some(
          (event) => event.type === "subagent.updated" && event.payload.id === doneSubagentNodeId,
        ),
      );
      assert.isFalse(
        events.some(
          (event) => event.type === "node.updated" && event.payload.id === doneSubagentNodeId,
        ),
      );
    }).pipe(Effect.provide(layer));
  },
);

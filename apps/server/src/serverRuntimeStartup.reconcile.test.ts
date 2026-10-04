import { assert, it } from "@effect/vitest";
import {
  ProjectId, ProviderDriverKind, ProviderInstanceId, ProviderSessionId, ProviderThreadId,
  RunAttemptId, RunId, ThreadId, ServerSettingsError,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as SqlError from "effect/unstable/sql/SqlError";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as ServerSettings from "./serverSettings.ts";
import * as Startup from "./serverRuntimeStartup.ts";
import * as Recovery from "./orchestration-v2/ProviderRuntimeRecoveryService.ts";
import * as EventSink from "./orchestration-v2/EventSink.ts";
import * as ProjectionStore from "./orchestration-v2/ProjectionStore.ts";
import * as ProviderSessions from "./orchestration-v2/ProviderSessionManager.ts";
import * as EffectOutbox from "./orchestration-v2/EffectOutbox.ts";
import * as IdAllocator from "./orchestration-v2/IdAllocator.ts";

const instanceId = ProviderInstanceId.make("codex");
const driver = ProviderDriverKind.make("codex");
const timestamp = "2026-01-01T00:00:00.000Z";
const source = (id: string) => {
  const threadId = ThreadId.make(id);
  const providerThreadId = ProviderThreadId.make(`provider-${id}`);
  const providerSessionId = ProviderSessionId.make(`session-${id}`);
  const runId = RunId.make(`run-${id}`);
  const attemptId = RunAttemptId.make(`attempt-${id}`);
  const binding = {
    threadId, providerThreadId, providerSessionId, instanceId, driver,
    nativeThreadId: `native-${id}`, runtimeGeneration: `generation-${id}`,
  } satisfies EventSink.ProviderBindingExpectationV2;
  const projection = {
    thread: { id: threadId, projectId: ProjectId.make(id), providerInstanceId: instanceId, archivedAt: null, deletedAt: null },
    runs: [{ id: runId, ordinal: 1, status: "running", providerInstanceId: instanceId, providerThreadId, activeAttemptId: attemptId }],
    providerThreads: [{ id: providerThreadId, appThreadId: threadId, ownerNodeId: null, providerInstanceId: instanceId,
      providerSessionId, driver, status: "active", nativeThreadRef: { nativeId: binding.nativeThreadId, strength: "strong", driver } }],
    providerSessions: [{ id: providerSessionId, driver, providerInstanceId: instanceId, status: "running" }],
    providerTurns: [{ providerThreadId, runAttemptId: attemptId, status: "running" }],
    attempts: [], nodes: [], turnItems: [], subagents: [], runtimeRequests: [], messages: [],
  } as unknown as ProjectionStore.ProjectionRuntimeRecoveryState;
  const marker: EventSink.RestartContinuationMarkerV2 = {
    markerId: `marker-${id}`, threadId, projectId: projection.thread.projectId, sourceRunId: runId,
    sourceRunAttemptId: attemptId, binding, evidenceRevision: 7, createdAt: timestamp,
  };
  return { threadId, projection, binding, marker };
};

const recoveryLayer = (input: {
  readonly settings?: Layer.Layer<ServerSettings.ServerSettingsService, ServerSettingsError>;
  readonly projections?: Partial<ProjectionStore.ProjectionStoreV2["Service"]>;
  readonly events?: Partial<EventSink.EventSinkV2["Service"]>;
  readonly sessions?: Partial<ProviderSessions.ProviderSessionManagerV2["Service"]>;
  readonly outbox?: Partial<EffectOutbox.EffectOutboxV2["Service"]>;
}) => Recovery.layer.pipe(Layer.provide(Layer.mergeAll(
  input.settings ?? ServerSettings.layerTest(),
  Layer.mock(ProjectionStore.ProjectionStoreV2)({
    getRecoveryThreadIds: () => Effect.die("preparation must not scan projections"),
    getRuntimeRecoveryProjection: () => Effect.die("preparation must not read runtime state"),
    ...input.projections,
  }),
  Layer.mock(EventSink.EventSinkV2)({
    readDormantRestartContinuations: Effect.die("unexpected marker inventory read"),
    readProviderRuntimeEvidence: () => Effect.succeed(null),
    findDormantRestartContinuation: () => Effect.succeed(null),
    prepareRestartContinuation: () => Effect.die("unexpected marker mutation"),
    clearRestartContinuation: () => Effect.die("unexpected marker clear"),
    ...input.events,
  }),
  Layer.mock(ProviderSessions.ProviderSessionManagerV2)({
    observeCurrentThreadRuntime: () => Effect.die("unexpected provider observation"),
    ...input.sessions,
  }),
  Layer.mock(EffectOutbox.EffectOutboxV2)({
    reconcileAfterProcessLoss: Effect.die("staging must not reconcile the outbox"),
    enqueue: () => Effect.die("staging must not enqueue provider work"),
    ...input.outbox,
  }),
  IdAllocator.layer,
)));

it.effect("desktop preparation with default-off continuation writes no resume markers", () =>
  Effect.gen(function* () {
    assert.deepEqual(yield* Startup.markOptedInProviderSessionsForContinuation, []);
  }).pipe(Effect.provide(recoveryLayer({
    projections: { getRecoveryThreadIds: () => Effect.succeed([]) },
  }))),
);

it.effect("desktop preparation refuses unreadable continuation preferences before touching sessions", () =>
  Effect.gen(function* () {
    const result = yield* Startup.markOptedInProviderSessionsForContinuation.pipe(Effect.exit);
    assert.isTrue(Exit.isFailure(result));
  }).pipe(Effect.provide(recoveryLayer({ settings: Layer.mock(ServerSettings.ServerSettingsService)({
    getSettings: Effect.fail(new ServerSettingsError({ settingsPath: "/fixture/settings.json", operation: "read-file", cause: "preferences unavailable" })),
  }) }))),
);

it.effect.each([false, true])(
  "desktop preparation marks only effectively opted-in projects when environment continuation is %s",
  (environmentOptIn) => {
    const values = [source("inherited"), source("enabled"), source("disabled")];
    const prepared: Array<EventSink.RestartContinuationMarkerV2> = [];
    const layer = recoveryLayer({
      settings: ServerSettings.layerTest({
        continueThreadsAfterServerUpdate: environmentOptIn,
        projectSettingsOverrides: { [ProjectId.make("enabled")]: { continueThreadsAfterServerUpdate: true }, [ProjectId.make("disabled")]: { continueThreadsAfterServerUpdate: false } },
      }),
      projections: {
        getRecoveryThreadIds: () => Effect.succeed(values.map((value) => value.threadId)),
        getRuntimeRecoveryProjection: (id) => Effect.succeed(values.find((value) => value.threadId === id)!.projection),
      },
      sessions: { observeCurrentThreadRuntime: (id) => Effect.succeed({
        status: "busy", binding: values.find((value) => value.threadId === id)!.binding, observedAt: timestamp,
      }) },
      events: {
        readProviderRuntimeEvidence: (id) => Effect.succeed({ binding: values.find((value) => value.threadId === id)!.binding,
          evidenceRevision: 7, observation: null, registeredAt: timestamp }),
        prepareRestartContinuation: (input) => Effect.gen(function* () {
          const value = values.find((value) => value.threadId === input.threadId)!;
          assert.deepEqual(input.expectedBinding, value.marker.binding);
          assert.equal(input.expectedEvidenceRevision, value.marker.evidenceRevision);
          const markerId = typeof input.markerId === "string" ? input.markerId : yield* input.markerId;
          value.marker = { ...value.marker, markerId };
          prepared.push(value.marker);
          return value.marker;
        }),
      },
    });
    return Effect.gen(function* () {
      const markers = yield* Startup.markOptedInProviderSessionsForContinuation;
      const expected = environmentOptIn ? [values[0]!.marker, values[1]!.marker] : [values[1]!.marker];
      assert.deepEqual(markers, expected);
      assert.deepEqual(prepared, expected);
    }).pipe(Effect.provide(layer));
  },
);

it.effect("startup stages immutable dormant markers without projection or outbox mutation", () => {
  const markers = [source("dormant").marker];
  return Effect.gen(function* () {
    const stage = yield* (yield* Recovery.ProviderRuntimeRecoveryService).stageStartupRecovery;
    assert.strictEqual(stage.continuationMarkers, markers);
  }).pipe(Effect.provide(recoveryLayer({ events: { readDormantRestartContinuations: Effect.succeed(markers) } })));
});

it.effect("clear delegates the captured full marker reference without thread-wide mutation", () => {
  const marker = source("captured").marker;
  const cleared: Array<EventSink.RestartContinuationMarkerV2> = [];
  return Effect.gen(function* () {
    yield* Startup.clearProviderSessionContinuationMarkers([marker]);
    assert.strictEqual(cleared[0], marker);
  }).pipe(Effect.provide(recoveryLayer({ events: {
    clearRestartContinuation: (value) => Effect.sync(() => { cleared.push(value); return true; }),
  } })));
});

it.effect.each(["archived", "deleted", "missing native ref", "stopped session", "wrong instance", "no live turn"] as const)(
  "does not prepare an interrupted source with %s",
  (reason) => {
    const value = source(`excluded-${reason}`);
    const projection: ProjectionStore.ProjectionRuntimeRecoveryState = {
      ...value.projection,
      thread: {
        ...value.projection.thread,
        archivedAt: reason === "archived" ? DateTime.makeUnsafe(timestamp) : null,
        deletedAt: reason === "deleted" ? DateTime.makeUnsafe(timestamp) : null,
        providerInstanceId: reason === "wrong instance" ? ProviderInstanceId.make("other") : instanceId,
      },
      providerThreads: value.projection.providerThreads.map((thread) => ({
        ...thread, nativeThreadRef: reason === "missing native ref" ? null : thread.nativeThreadRef,
      })),
      providerSessions: value.projection.providerSessions.map((session) => ({
        ...session, status: reason === "stopped session" ? "stopped" as const : session.status,
      })),
      providerTurns: value.projection.providerTurns.map((turn) => ({
        ...turn, status: reason === "no live turn" ? "completed" as const : turn.status,
      })),
    };
    return Effect.gen(function* () {
      assert.deepEqual(yield* Startup.markRunningProviderSessionsForContinuation, []);
    }).pipe(Effect.provide(recoveryLayer({
      projections: { getRecoveryThreadIds: () => Effect.succeed([value.threadId]), getRuntimeRecoveryProjection: () => Effect.succeed(projection) },
      sessions: { observeCurrentThreadRuntime: () => Effect.succeed({ status: "busy", binding: value.binding, observedAt: timestamp }) },
    })));
  },
);

it.effect.each(["runtime_not_resident", "runtime_binding_unavailable", "native_observation_incomplete"] as const)(
  "update preparation never substitutes persisted running state for %s",
  (reason) => {
    const value = source(`unknown-${reason}`);
    return Effect.gen(function* () {
      const result = yield* Startup.markRunningProviderSessionsForContinuation.pipe(Effect.exit);
      if (reason === "runtime_not_resident") {
        assert.isTrue(Exit.isSuccess(result));
        if (Exit.isSuccess(result)) assert.deepEqual(result.value, []);
      } else {
        assert.isTrue(Exit.isFailure(result));
      }
    }).pipe(Effect.provide(recoveryLayer({
      projections: { getRecoveryThreadIds: () => Effect.succeed([value.threadId]), getRuntimeRecoveryProjection: () => Effect.succeed(value.projection) },
      sessions: { observeCurrentThreadRuntime: () => Effect.succeed({ status: "unknown", reason }) },
    })));
  },
);

it.effect("startup reports an actual dormant marker inventory failure without enumerating providers", () => {
  const failure = new EventSink.EventSinkWriteError({ eventCount: 0, cause: "marker inventory unavailable" });
  return Effect.gen(function* () {
    const error = yield* (yield* Recovery.ProviderRuntimeRecoveryService).stageStartupRecovery.pipe(Effect.flip);
    assert.equal(error.operation, "read-projections");
    assert.strictEqual(error.cause, failure);
  }).pipe(Effect.provide(recoveryLayer({ events: { readDormantRestartContinuations: Effect.fail(failure) } })));
});

const emptyRecoveryProjection = (id: string): ProjectionStore.ProjectionRuntimeRecoveryState => ({
  ...source(id).projection, runs: [], providerThreads: [], providerSessions: [], providerTurns: [],
});
const idleRecoveryOutbox = {
  listHeldByThreadId: () => Effect.succeed([]),
  cancelUnsettled: () => Effect.succeed([]),
  signalCancellations: () => Effect.void,
  reconcileAfterProcessLoss: Effect.succeed({ requeued: 0, cancelled: 0 }),
  reconcileAfterProcessLossExcluding: () => Effect.succeed({ requeued: 0, cancelled: 0 }),
} satisfies Partial<EffectOutbox.EffectOutboxV2["Service"]>;

it.effect("retries a transient projection recovery read before completing startup", () => {
  const threadId = ThreadId.make("transient-recovery-read");
  let reads = 0;
  return Effect.gen(function* () {
    const result = yield* (yield* Recovery.ProviderRuntimeRecoveryService).recover.pipe(Effect.exit);
    assert.isTrue(Exit.isSuccess(result));
    if (Exit.isSuccess(result)) assert.deepEqual(result.value.failedThreadIds, []);
    assert.equal(reads, 2);
  }).pipe(Effect.provide(recoveryLayer({
    projections: {
      getRecoveryThreadIds: () => Effect.succeed([threadId]),
      getRuntimeRecoveryProjection: () => Effect.suspend(() => ++reads === 1
        ? Effect.fail(new ProjectionStore.ProjectionStoreReadError({ threadId, cause: new SqlError.SqlError({ reason: new SqlError.LockTimeoutError({
            cause: { code: "SQLITE_BUSY" }, operation: "read", message: "Synthetic transient read lock",
          }) }) }))
        : Effect.succeed(emptyRecoveryProjection(threadId))),
    },
    outbox: idleRecoveryOutbox,
  })));
});

it.effect("continues recovering later threads after a persistent projection read failure", () => {
  const failed = ThreadId.make("persistent-recovery-read");
  const later = ThreadId.make("later-recovery-read");
  let laterReads = 0;
  let unrelatedOutboxProgress = 0;
  let exclusions: ReadonlyArray<ThreadId> | undefined;
  return Effect.gen(function* () {
    const result = yield* (yield* Recovery.ProviderRuntimeRecoveryService).recover.pipe(Effect.exit);
    assert.isTrue(Exit.isSuccess(result));
    if (Exit.isSuccess(result)) {
      assert.deepEqual(result.value.failedThreadIds, [failed]);
      assert.equal(result.value.requeuedEffects, 1);
      assert.equal(result.value.retiredEffects, 0);
    }
    assert.equal(laterReads, 1);
    assert.deepEqual(exclusions, [failed]);
    assert.equal(unrelatedOutboxProgress, 1);
  }).pipe(Effect.provide(recoveryLayer({
    projections: {
      getRecoveryThreadIds: () => Effect.succeed([failed, later]),
      getRuntimeRecoveryProjection: (id) => id === failed
        ? Effect.fail(new ProjectionStore.ProjectionStoreReadError({ threadId: id, cause: "persistent read failure" }))
        : Effect.sync(() => { laterReads += 1; return emptyRecoveryProjection(id); }),
    },
    outbox: {
      ...idleRecoveryOutbox,
      reconcileAfterProcessLoss: Effect.die("failed thread requires scoped outbox reconciliation"),
      reconcileAfterProcessLossExcluding: ({ excludeThreadIds }) => Effect.sync(() => {
        exclusions = excludeThreadIds;
        assert.deepEqual(excludeThreadIds, [failed]);
        unrelatedOutboxProgress += 1;
        return { requeued: 1, cancelled: 0 };
      }),
    },
  })));
});

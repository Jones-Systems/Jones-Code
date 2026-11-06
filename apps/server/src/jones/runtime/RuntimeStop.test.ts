import * as RestartPreparation from "../orchestration/RestartPreparation.ts";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  ThreadId,
  RunId,
  ProjectId,
  ProviderInstanceId,
  ProviderDriverKind,
  ProviderSessionId,
  ProviderThreadId,
  EventId,
  AuthSessionId,
  EnvironmentAuthenticatedPrincipal,
  type StopCurrentThreadRuntimeInput,
  type ProviderRuntimeBinding,
  type OrchestrationV2ProviderThread,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Layer from "effect/Layer";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Stream from "effect/Stream";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import { ProviderAdapterCloseSessionError } from "@t3tools/provider-core/server/ProviderAdapter";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as AuthSessions from "../../persistence/AuthSessions.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as ProviderSessions from "../../orchestration-v2/ProviderSessionManager.ts";
import * as RuntimeStop from "./RuntimeStop.ts";
import * as StopStore from "./RuntimeStopSqlite.ts";
const stores = Layer.mergeAll(EventStore.layer, ProjectionStore.layer, AuthSessions.layer).pipe(
  Layer.provideMerge(SqlitePersistence.layerMemory),
);
const eventLayer = EventSink.layer.pipe(Layer.provideMerge(stores));
const testLayer = RestartPreparation.layer.pipe(Layer.provideMerge(eventLayer));
const fixture = Effect.fnUntraced(function* (retained = false) {
  const sink = yield* EventSink.EventSinkV2;
  const sessions = yield* AuthSessions.AuthSessionRepository;
  const threadId = ThreadId.make("thread:runtime-stop");
  const runId = RunId.make("run:runtime-stop");
  const now = yield* DateTime.now;
  const binding: ProviderRuntimeBinding = {
    threadId,
    providerThreadId: ProviderThreadId.make("provider-thread:stop"),
    providerSessionId: ProviderSessionId.make("session:stop"),
    providerInstanceId: ProviderInstanceId.make("codex"),
    driver: ProviderDriverKind.make("codex"),
    nativeThreadId: "native:stop",
    runtimeGeneration: "physical-generation:stop",
  };
  const provider: OrchestrationV2ProviderThread = {
    id: binding.providerThreadId,
    appThreadId: binding.threadId,
    providerSessionId: binding.providerSessionId,
    providerInstanceId: binding.providerInstanceId,
    driver: binding.driver,
    ownerNodeId: null,
    nativeThreadRef: { driver: binding.driver, nativeId: binding.nativeThreadId, strength: "strong" },
    nativeConversationHeadRef: null,
    status: "not_loaded",
    firstRunOrdinal: null,
    lastRunOrdinal: null,
    handoffIds: [],
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
    pendingBackgroundTasks: [],
    contextUsage: null,
    nativeMetadata: null,
    goal: null,
    runtimeIdentity: {
      requested: {
        providerInstanceId: binding.providerInstanceId,
        providerDriver: binding.driver,
        model: "synthetic",
        serviceTier: null,
      },
      observed: {
        backend: { status: "unknown" },
        account: { status: "unknown" },
        model: { status: "unknown" },
        serviceTier: { status: "unknown" },
      },
    },
  };
  yield* sink.write({
    events: [
      {
        id: EventId.make("event:stop:birth"),
        type: "thread.created",
        threadId,
        occurredAt: now,
        payload: {
          id: threadId,
          projectId: ProjectId.make("project:stop"),
          title: "Synthetic stop",
          providerInstanceId: binding.providerInstanceId,
          modelSelection: { instanceId: binding.providerInstanceId, model: "synthetic" },
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
          createdBy: "user",
          creationSource: "web",
        },
      },
    ],
  });
  yield* sink.write({
    events: [
      {
        id: EventId.make("event:stop:session"),
        type: "provider-session.attached",
        threadId,
        driver: binding.driver,
        providerInstanceId: binding.providerInstanceId,
        occurredAt: now,
        payload: {
          id: binding.providerSessionId,
          driver: binding.driver,
          providerInstanceId: binding.providerInstanceId,
          status: "ready",
          cwd: "/synthetic/runtime-stop",
          model: null,
          capabilities: CodexProviderCapabilitiesV2,
          createdAt: now,
          updatedAt: now,
          lastError: null,
        },
      },
    ],
  });
  const principal = {
    sessionId: AuthSessionId.make("actor:stop"),
    subject: "Synthetic stop actor",
    method: "browser-session-cookie" as const,
    scopes: new Set<"orchestration:operate" | "orchestration:read">([
      "orchestration:operate",
      "orchestration:read",
    ]),
  };
  yield* sessions.create({
    sessionId: principal.sessionId,
    subject: principal.subject,
    method: principal.method,
    scopes: ["orchestration:operate", "orchestration:read"],
    client: {
      label: null,
      ipAddress: null,
      userAgent: null,
      deviceType: "bot",
      os: null,
      browser: null,
    },
    issuedAt: now,
    expiresAt: DateTime.add(now, { hours: 1 }),
  });
  const state = {
    current: true,
    invocations: 0,
    unknown: false,
    available: true,
    wait: false,
    held: false,
    reconciled: [] as ProviderRuntimeBinding[],
    retirementStarted: false,
    observationFailures: 0,
    interruptObservation: false,
    observations: 0,
    retirementBinding: binding,
    cleanupCalls: 0,
  };
  const started = yield* Deferred.make<void>();
  const completion = yield* Deferred.make<void>();
  const captured = {
    binding,
    evidenceRevision: 1,
    isCurrent: Effect.sync(() => state.current),
    stop: Effect.gen(function* () {
      state.invocations++;
      state.retirementStarted = true;
      yield* Deferred.succeed(started, undefined);
      if (state.wait) yield* Deferred.await(completion);
      if (state.unknown)
        return yield* new StopStore.RuntimeStopError({ reason: "synthetic_lost_stop" });
    }),
    ...(retained
      ? {
          retirement: {
            binding,
            started: Effect.sync(() => state.retirementStarted),
            awaitRetired: Effect.gen(function* () {
              state.observations++;
              if (state.interruptObservation) {
                state.interruptObservation = false;
                return yield* Effect.interrupt;
              }
              if (state.observationFailures > 0) {
                state.observationFailures--;
                return yield* new ProviderAdapterCloseSessionError({
                  driver: binding.driver,
                  providerSessionId: binding.providerSessionId,
                  cause: "synthetic_observation_interruption",
                });
              }
              yield* Deferred.await(completion);
              state.cleanupCalls++;
              return { binding: state.retirementBinding, evidence: "producer_exited" as const };
            }),
          },
        }
      : {}),
  };
  const manager = Layer.succeed(ProviderSessions.ProviderSessionManagerV2, {
    isMcpCallerAttached: () => Effect.succeed(false),
    isLive: () => Effect.die("Runtime stop must use the captured runtime binding"),
    shutdown: Effect.void,
    open: () => Effect.die("unexpected_open"),
    get: () => Effect.succeedNone,
    close: () => Effect.die("unexpected_close"),
    closeInstance: () => Effect.die("unexpected_close_instance"),
    release: () => Effect.die("unexpected_release"),
    detach: () => Effect.die("unexpected_ordinary_detach"),
    captureCurrentThreadRuntimeStop: () =>
      Effect.succeed(
        state.available
          ? {
              ...captured,
              stop: captured.stop.pipe(
                Effect.mapError(
                  (cause) =>
                    new ProviderAdapterCloseSessionError({
                      driver: binding.driver,
                      providerSessionId: binding.providerSessionId,
                      cause,
                    }),
                ),
              ),
            }
          : null,
      ),
  });
  const service = RuntimeStop.layer.pipe(
    Layer.provide(manager),
    Layer.provide(
      Layer.mock(RestartPreparation.RestartPreparation)({
        captureHeldRuntimeStop: () =>
          Effect.succeed(
            state.held
              ? {
                  ...captured,
                  stop: captured.stop.pipe(
                    Effect.mapError(
                      (cause) =>
                        new ProviderAdapterCloseSessionError({
                          driver: binding.driver,
                          providerSessionId: binding.providerSessionId,
                          cause,
                        }),
                    ),
                  ),
                }
              : null,
          ),
        resolveAfterTeardown: (resolved) =>
          Effect.sync(() => {
            state.reconciled.push(resolved);
          }),
      }),
    ),
  );
  const input: StopCurrentThreadRuntimeInput = {
    commandId: CommandId.make("command:runtime-stop"),
    threadId,
    target: { binding, evidenceRevision: 1 },
  };
  return { sink, sessions, principal, state, input, runId, service, started, completion, provider };
});
const recordStoppedFixture = Effect.fnUntraced(function* (input: {
  readonly commandId: CommandId;
  readonly binding: ProviderRuntimeBinding;
  readonly actor: StopStore.RuntimeStopIdentity["actor"];
  readonly result: "accepted" | "started" | "unknown" | "stopped";
}) {
  const sql = yield* SqlClient.SqlClient;
  const request = {
    commandId: input.commandId,
    threadId: input.binding.threadId,
    target: { binding: input.binding, evidenceRevision: 1 },
  };
  const identityJson = yield* Schema.encodeEffect(
    Schema.fromJsonString(StopStore.RuntimeStopIdentity),
  )({ request, actor: input.actor, actorDigest: "synthetic-readback", affectedRunIds: [] });
  yield* sql`INSERT INTO orchestration_command_receipts(command_id,aggregate_kind,aggregate_id,command_type,accepted_at,result_sequence,status,error) VALUES (${input.commandId},'thread',${input.binding.threadId},'provider-session.detach','2026-10-07T12:00:00Z',1,'accepted',NULL)`;
  yield* sql`INSERT INTO jones_runtime_stop_intents VALUES (${input.commandId},${input.binding.threadId},${identityJson})`;
  const methods = StopStore.makeRuntimeStopMethods(sql);
  if (input.result !== "accepted") yield* methods.start(input.commandId, request.target, Effect.void);
  if (input.result === "unknown" || input.result === "stopped")
    yield* methods.complete(input.commandId, input.result);
});
it.effect(
  "missing physical capture rejects with no command receipt, fence or provider effect",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      f.state.available = false;
      yield* Effect.gen(function* () {
        const owner = yield* RuntimeStop.CurrentRuntimeStop;
        assert.strictEqual(
          (yield* owner
            .stop(f.input)
            .pipe(
              Effect.provideService(EnvironmentAuthenticatedPrincipal, f.principal),
              Effect.result,
            ))._tag,
          "Failure",
        );
        assert.strictEqual(f.state.invocations, 0);
        const sql = yield* SqlClient.SqlClient;
        assert.deepEqual(yield* sql`SELECT * FROM jones_runtime_stop_intents`, []);
        assert.deepEqual(yield* sql`SELECT * FROM orchestration_command_receipts`, []);
      }).pipe(Effect.provide(f.service));
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
);
it.effect(
  "accepted stop shares actual command receipt/outbox, duplicate observes and conflicting actor request rejects",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* Effect.gen(function* () {
        const owner = yield* RuntimeStop.CurrentRuntimeStop;
        const request = owner
          .stop(f.input)
          .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, f.principal));
        const first = yield* request;
        const second = yield* request;
        assert.deepEqual(first, second);
        assert.strictEqual(f.state.invocations, 0);
        const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
        assert.strictEqual(
          (yield* projectionStore.getThreadRecords(f.input.threadId, ["providerSessions"]))
            .providerSessions.length,
          1,
        );
        const replay = yield* (yield* EventStore.EventStoreV2)
          .readByCommandId({ commandId: f.input.commandId })
          .pipe(Stream.runCollect);
        assert.strictEqual(replay[0]?.event.type, "provider-session.detach-requested");
        const sql = yield* SqlClient.SqlClient;
        assert.strictEqual((yield* sql`SELECT * FROM orchestration_command_receipts`).length, 1);
        assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_command_receipts`, []);
        assert.strictEqual((yield* sql`SELECT * FROM jones_runtime_stop_intents`).length, 1);
        yield* owner.execute(f.input.commandId);
        assert.strictEqual(f.state.invocations, 1);
        yield* owner.execute(f.input.commandId);
        assert.strictEqual(f.state.invocations, 1);
        const observed = yield* request;
        assert.strictEqual(observed.status, "stopped");
        assert.strictEqual(observed.backgroundCoverage, "partial");
        assert.strictEqual(
          (yield* owner
            .stop({ ...f.input, target: { ...f.input.target, evidenceRevision: 2 } })
            .pipe(
              Effect.provideService(EnvironmentAuthenticatedPrincipal, f.principal),
              Effect.result,
            ))._tag,
          "Failure",
        );
      }).pipe(Effect.provide(f.service));
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
);
it.effect(
  "replacement before execution denies stop, unknown completion never invokes replacement or repeats original",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* Effect.gen(function* () {
        const owner = yield* RuntimeStop.CurrentRuntimeStop;
        yield* owner
          .stop(f.input)
          .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, f.principal));
        f.state.current = false;
        assert.strictEqual(
          (yield* owner.execute(f.input.commandId).pipe(Effect.result))._tag,
          "Failure",
        );
        assert.strictEqual(f.state.invocations, 0);
        f.state.current = true;
        f.state.unknown = true;
        assert.strictEqual(
          (yield* owner.execute(f.input.commandId).pipe(Effect.result))._tag,
          "Failure",
        );
        assert.strictEqual(f.state.invocations, 1);
        f.state.unknown = false;
        assert.strictEqual(
          (yield* owner.execute(f.input.commandId).pipe(Effect.result))._tag,
          "Failure",
        );
        assert.strictEqual(f.state.invocations, 1);
        assert.strictEqual((yield* f.sink.readRuntimeStop!(f.input.commandId))?.status, "unknown");
      }).pipe(Effect.provide(f.service));
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
);
it.effect(
  "captured queued run fences remain blocked after unknown stop and owner start is compare-and-swap",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const sql = yield* SqlClient.SqlClient;
      const methods = StopStore.makeRuntimeStopMethods(sql);
      yield* sql`INSERT INTO orchestration_command_receipts(command_id,aggregate_kind,aggregate_id,command_type,accepted_at,result_sequence,status,error) VALUES (${f.input.commandId},'thread',${f.input.threadId},'provider-session.detach','2026-10-07T12:00:00Z',1,'accepted',NULL)`;
      const identityJson = yield* Schema.encodeEffect(
        Schema.fromJsonString(StopStore.RuntimeStopIdentity),
      )({
        request: f.input,
        actor: {
          sessionId: f.principal.sessionId,
          subject: f.principal.subject,
          method: f.principal.method,
        },
        actorDigest: "fixture-digest",
        affectedRunIds: [f.runId],
      });
      yield* sql`INSERT INTO jones_runtime_stop_intents VALUES (${f.input.commandId},${f.input.threadId},${identityJson})`;
      yield* sql`INSERT INTO jones_runtime_stop_fences VALUES (${f.input.commandId},${f.input.threadId},${f.runId},${f.input.target.binding.providerThreadId},${f.input.target.binding.runtimeGeneration})`;
      assert.strictEqual(
        (yield* f.sink.assertRuntimeStopStartAllowed!({
          threadId: f.input.threadId,
          runId: f.runId,
        }).pipe(Effect.result))._tag,
        "Failure",
      );
      const winners = yield* Effect.all(
        [
          methods.start(f.input.commandId, f.input.target, Effect.void),
          methods.start(f.input.commandId, f.input.target, Effect.void),
        ],
        { concurrency: 2 },
      );
      assert.strictEqual(winners.filter(Boolean).length, 1);
      yield* methods.complete(f.input.commandId, "unknown");
      assert.strictEqual(
        (yield* f.sink.assertRuntimeStopStartAllowed!({
          threadId: f.input.threadId,
          runId: f.runId,
        }).pipe(Effect.result))._tag,
        "Failure",
      );
      assert.isFalse(yield* methods.start(f.input.commandId, f.input.target, Effect.void));
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
);

it.effect("revoked ordinary actor cannot invoke an accepted physical stop", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    yield* Effect.gen(function* () {
      const owner = yield* RuntimeStop.CurrentRuntimeStop;
      yield* owner
        .stop(f.input)
        .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, f.principal));
      yield* f.sessions.revoke({
        sessionId: f.principal.sessionId,
        revokedAt: yield* DateTime.now,
      });
      assert.strictEqual(
        (yield* owner.execute(f.input.commandId).pipe(Effect.result))._tag,
        "Failure",
      );
      assert.strictEqual(f.state.invocations, 0);
    }).pipe(Effect.provide(f.service));
  }).pipe(Effect.scoped, Effect.provide(testLayer)),
);

it.effect("cancellation after captured invocation retains unknown without replay", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    f.state.wait = true;
    yield* Effect.gen(function* () {
      const owner = yield* RuntimeStop.CurrentRuntimeStop;
      yield* owner
        .stop(f.input)
        .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, f.principal));
      const fiber = yield* owner.execute(f.input.commandId).pipe(Effect.forkChild);
      yield* Deferred.await(f.started);
      yield* Fiber.interrupt(fiber);
      assert.strictEqual(f.state.invocations, 1);
      assert.strictEqual((yield* f.sink.readRuntimeStop!(f.input.commandId))?.status, "unknown");
      f.state.wait = false;
      assert.strictEqual(
        (yield* owner.execute(f.input.commandId).pipe(Effect.result))._tag,
        "Failure",
      );
      assert.strictEqual(f.state.invocations, 1);
    }).pipe(Effect.provide(f.service));
  }).pipe(Effect.scoped, Effect.provide(testLayer)),
);

it.effect(
  "read target qualifies capture without effects and a read-only principal cannot stop",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* Effect.gen(function* () {
        const owner = yield* RuntimeStop.CurrentRuntimeStop;
        const principal = {
          ...f.principal,
          scopes: new Set<"orchestration:read">(["orchestration:read"]),
        };
        const read = owner
          .readTarget(f.input.threadId)
          .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, principal));
        const target = yield* read;
        assert.strictEqual(target.status, "available");
        if (target.status === "available") assert.deepEqual(target.target, f.input.target);
        assert.strictEqual(
          (yield* owner
            .stop(f.input)
            .pipe(
              Effect.provideService(EnvironmentAuthenticatedPrincipal, principal),
              Effect.result,
            ))._tag,
          "Failure",
        );
        f.state.available = false;
        assert.strictEqual((yield* read).status, "unavailable");
        const sql = yield* SqlClient.SqlClient;
        assert.deepEqual(yield* sql`SELECT * FROM orchestration_command_receipts`, []);
        assert.strictEqual(f.state.invocations, 0);
      }).pipe(Effect.provide(f.service));
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
);
it.effect("a stop with no queued runs still blocks later starts in its captured lineage", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    yield* Effect.gen(function* () {
      const owner = yield* RuntimeStop.CurrentRuntimeStop;
      yield* owner
        .stop(f.input)
        .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, f.principal));
      const later = {
        threadId: f.input.threadId,
        runId: RunId.make("run:later"),
        providerThreadId: f.input.target.binding.providerThreadId,
      };
      assert.strictEqual(
        (yield* f.sink.assertRuntimeStopStartAllowed!({
          ...later,
          runtimeGeneration: f.input.target.binding.runtimeGeneration,
        }).pipe(Effect.result))._tag,
        "Failure",
      );
      assert.strictEqual(
        (yield* f.sink.assertRuntimeStopStartAllowed!(later).pipe(Effect.result))._tag,
        "Failure",
      );
      yield* f.sink.assertRuntimeStopStartAllowed!({
        ...later,
        runtimeGeneration: "physical:proved-replacement",
      });
      assert.strictEqual(f.state.invocations, 0);
    }).pipe(Effect.provide(f.service));
  }).pipe(Effect.scoped, Effect.provide(testLayer)),
);

it.effect(
  "observation of a missing command returns null without acceptance or physical capture",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      f.state.available = false;
      yield* Effect.gen(function* () {
        const owner = yield* RuntimeStop.CurrentRuntimeStop;
        assert.strictEqual(
          yield* owner
            .observe(f.input)
            .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, f.principal)),
          null,
        );
        const sql = yield* SqlClient.SqlClient;
        assert.deepEqual(yield* sql`SELECT * FROM orchestration_command_receipts`, []);
        assert.deepEqual(yield* sql`SELECT * FROM jones_runtime_stop_intents`, []);
        assert.strictEqual(f.state.invocations, 0);
      }).pipe(Effect.provide(f.service));
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
);
it.effect(
  "observation follows original receipt after replacement and rejects changed actor or target without replay",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* Effect.gen(function* () {
        const owner = yield* RuntimeStop.CurrentRuntimeStop;
        yield* owner
          .stop(f.input)
          .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, f.principal));
        f.state.available = false;
        f.state.current = false;
        const read = owner
          .observe(f.input)
          .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, f.principal));
        assert.strictEqual((yield* read)?.status, "accepted");
        assert.strictEqual(
          (yield* owner
            .observe({ ...f.input, target: { ...f.input.target, evidenceRevision: 2 } })
            .pipe(
              Effect.provideService(EnvironmentAuthenticatedPrincipal, f.principal),
              Effect.result,
            ))._tag,
          "Failure",
        );
        const alternate = { ...f.principal, sessionId: AuthSessionId.make("actor:other") };
        const now = yield* DateTime.now;
        yield* f.sessions.create({
          sessionId: alternate.sessionId,
          subject: alternate.subject,
          method: alternate.method,
          scopes: ["orchestration:read", "orchestration:operate"],
          client: {
            label: null,
            ipAddress: null,
            userAgent: null,
            deviceType: "bot",
            os: null,
            browser: null,
          },
          issuedAt: now,
          expiresAt: DateTime.add(now, { hours: 1 }),
        });
        assert.strictEqual(
          (yield* owner
            .observe(f.input)
            .pipe(
              Effect.provideService(EnvironmentAuthenticatedPrincipal, alternate),
              Effect.result,
            ))._tag,
          "Failure",
        );
        const sql = yield* SqlClient.SqlClient;
        assert.strictEqual((yield* sql`SELECT * FROM orchestration_command_receipts`).length, 1);
        assert.deepEqual(yield* sql`SELECT * FROM jones_runtime_stop_observations`, []);
        assert.strictEqual(f.state.invocations, 0);
      }).pipe(Effect.provide(f.service));
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
);

it.effect.each([false, true])(
  "a captured old restart runtime resolves its hold only after verified stop (unknown=%s)",
  (unknown) =>
    Effect.gen(function* () {
      const f = yield* fixture();
      f.state.available = false;
      f.state.held = true;
      f.state.unknown = unknown;
      yield* Effect.gen(function* () {
        const owner = yield* RuntimeStop.CurrentRuntimeStop;
        const target = yield* owner
          .readTarget(f.input.threadId)
          .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, f.principal));
        assert.equal(target.status, "available");
        yield* owner
          .stop(f.input)
          .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, f.principal));
        const result = yield* owner.execute(f.input.commandId).pipe(Effect.result);
        assert.equal(result._tag, unknown ? "Failure" : "Success");
        assert.deepEqual(f.state.reconciled, unknown ? [] : [f.input.target.binding]);
        assert.equal(f.state.invocations, 1);
      }).pipe(Effect.provide(f.service));
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
);

it.effect("a slow retained Stop outlives its deadline, joins duplicates and completes after exact cleanup", () =>
  Effect.gen(function* () {
    const f = yield* fixture(true);
    f.state.wait = true;
    yield* Effect.gen(function* () {
      const owner = yield* RuntimeStop.CurrentRuntimeStop;
      yield* owner.stop(f.input).pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, f.principal));
      const first = yield* owner.execute(f.input.commandId).pipe(Effect.result, Effect.forkChild);
      yield* Deferred.await(f.started);
      yield* TestClock.adjust(RuntimeStop.RUNTIME_STOP_WAIT_TIMEOUT_MS);
      assert.equal((yield* Fiber.join(first))._tag, "Failure");
      const sql = yield* SqlClient.SqlClient;
      assert.deepEqual(yield* sql`SELECT phase,result FROM jones_runtime_stop_observations`, [
        { phase: "started", result: "unknown" },
      ]);
      f.state.current = false;
      f.state.available = false;
      const duplicate = yield* owner.execute(f.input.commandId).pipe(Effect.forkChild);
      const observation = yield* owner.observe(f.input).pipe(
        Effect.provideService(EnvironmentAuthenticatedPrincipal, f.principal),
        Effect.forkChild,
      );
      yield* Fiber.interrupt(duplicate);
      assert.equal(f.state.invocations, 1);
      assert.deepEqual(f.state.reconciled, []);
      yield* Deferred.succeed(f.completion, undefined);
      assert.equal((yield* Fiber.join(observation))?.status, "stopped");
      yield* owner.execute(f.input.commandId);
      assert.equal(f.state.invocations, 1);
      assert.equal(f.state.cleanupCalls, 1);
      assert.deepEqual(f.state.reconciled, [f.input.target.binding]);
      assert.isTrue(yield* StopStore.makeRuntimeStopMethods(sql).hasStoppedRuntime(f.input.target.binding));
    }).pipe(Effect.provide(f.service));
  }).pipe(Effect.scoped, Effect.provide(testLayer)),
);

it.effect.each(["failure", "interruption"] as const)("canceling a Stop waiter and an observation %s never stop the successor", (observation) =>
  Effect.gen(function* () {
    const f = yield* fixture(true);
    f.state.observationFailures = observation === "failure" ? 1 : 0;
    f.state.interruptObservation = observation === "interruption";
    yield* Effect.gen(function* () {
      const owner = yield* RuntimeStop.CurrentRuntimeStop;
      yield* owner.stop(f.input).pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, f.principal));
      const waiter = yield* owner.execute(f.input.commandId).pipe(Effect.forkChild);
      yield* Deferred.await(f.started);
      yield* Fiber.interrupt(waiter);
      f.state.current = false;
      assert.equal((yield* f.sink.readRuntimeStop!(f.input.commandId))?.status, "unknown");
      const sql = yield* SqlClient.SqlClient;
      assert.deepEqual(yield* sql`SELECT phase FROM jones_runtime_stop_observations WHERE phase='completed'`, []);
      yield* Deferred.succeed(f.completion, undefined);
      yield* TestClock.adjust("1 second");
      yield* owner.execute(f.input.commandId);
      assert.equal(f.state.invocations, 1);
      assert.equal(f.state.observations, 2);
      assert.equal(f.state.cleanupCalls, 1);
      assert.equal((yield* f.sink.readRuntimeStop!(f.input.commandId))?.status, "stopped");
    }).pipe(Effect.provide(f.service));
  }).pipe(Effect.scoped, Effect.provide(testLayer)),
);

it.effect.each(["runtimeGeneration", "providerSessionId", "nativeThreadId"] as const)(
  "a retained retirement receipt for the wrong %s cannot complete or release a hold",
  (key) => Effect.gen(function* () {
    const f = yield* fixture(true);
    f.state.retirementBinding = { ...f.input.target.binding, [key]: "replacement" } as ProviderRuntimeBinding;
    yield* Effect.gen(function* () {
      const owner = yield* RuntimeStop.CurrentRuntimeStop;
      yield* owner.stop(f.input).pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, f.principal));
      const waiter = yield* owner.execute(f.input.commandId).pipe(Effect.result, Effect.forkChild);
      yield* Deferred.await(f.started);
      f.state.current = false;
      yield* Deferred.succeed(f.completion, undefined);
      assert.equal((yield* Fiber.join(waiter))._tag, "Failure");
      assert.equal((yield* owner.execute(f.input.commandId).pipe(Effect.result))._tag, "Failure");
      assert.equal(f.state.invocations, 1);
      assert.deepEqual(f.state.reconciled, []);
      const sql = yield* SqlClient.SqlClient;
      assert.deepEqual(yield* sql`SELECT phase,result FROM jones_runtime_stop_observations`, [
        { phase: "started", result: "unknown" },
      ]);
    }).pipe(Effect.provide(f.service));
  }).pipe(Effect.scoped, Effect.provide(testLayer)),
);

it.effect.each([false, true])("a lost retained capture stays unknown without a second physical Stop (completed=%s)", (completed) =>
  Effect.gen(function* () {
    const f = yield* fixture(true);
    yield* Effect.gen(function* () {
      const owner = yield* RuntimeStop.CurrentRuntimeStop;
      yield* owner.stop(f.input).pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, f.principal));
    }).pipe(Effect.provide(f.service));
    const sql = yield* SqlClient.SqlClient;
    const methods = StopStore.makeRuntimeStopMethods(sql);
    yield* methods.start(f.input.commandId, f.input.target, Effect.void);
    if (completed) yield* methods.complete(f.input.commandId, "unknown");
    f.state.current = false;
    yield* Effect.gen(function* () {
      const owner = yield* RuntimeStop.CurrentRuntimeStop;
      assert.equal((yield* owner.execute(f.input.commandId).pipe(Effect.result))._tag, "Failure");
      assert.equal((yield* owner.observe(f.input).pipe(
        Effect.provideService(EnvironmentAuthenticatedPrincipal, f.principal),
      ))?.status, "unknown");
      assert.equal(f.state.invocations, 0);
    }).pipe(Effect.provide(f.service));
    if (completed)
      assert.equal((yield* methods.complete(f.input.commandId, "stopped").pipe(Effect.result))._tag, "Failure");
    assert.isFalse(yield* methods.hasStoppedRuntime(f.input.target.binding));
  }).pipe(Effect.scoped, Effect.provide(testLayer)),
);

it.effect("completed stopped readback requires every captured binding field and an affirmative terminal receipt", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    yield* Effect.gen(function* () {
      const owner = yield* RuntimeStop.CurrentRuntimeStop;
      yield* owner.stop(f.input).pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, f.principal));
      const sql = yield* SqlClient.SqlClient;
      const methods = StopStore.makeRuntimeStopMethods(sql);
      assert.isFalse(yield* methods.hasStoppedRuntime(f.input.target.binding));
      yield* methods.start(f.input.commandId, f.input.target, Effect.void);
      assert.isFalse(yield* methods.hasStoppedRuntime(f.input.target.binding));
      yield* methods.complete(f.input.commandId, "stopped");
      assert.isTrue(yield* methods.hasStoppedRuntime(f.input.target.binding));
      for (const key of Object.keys(f.input.target.binding) as Array<keyof ProviderRuntimeBinding>)
        assert.isFalse(yield* methods.hasStoppedRuntime({
          ...f.input.target.binding,
          [key]: `${f.input.target.binding[key]}:wrong`,
        } as ProviderRuntimeBinding));
    }).pipe(Effect.provide(f.service));
  }).pipe(Effect.scoped, Effect.provide(testLayer)),
);

it.effect.each(["accepted", "started", "unknown", "stopped"] as const)(
  "generation-less runtime readback requires completed/stopped proof (receipt=%s)",
  (result) =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const binding = f.input.target.binding;
      yield* recordStoppedFixture({
        commandId: f.input.commandId,
        binding,
        actor: f.principal,
        result,
      });
      const methods = StopStore.makeRuntimeStopMethods(yield* SqlClient.SqlClient);
      assert.deepEqual(yield* methods.readStoppedRuntime(f.provider), result === "stopped" ? binding : null);
      assert.equal(yield* methods.hasStoppedRuntime(binding), result === "stopped");
      assert.equal(yield* methods.readStoppedRuntime({
        ...f.provider,
        runtimeIdentity: {
          ...f.provider.runtimeIdentity!,
          runtimeGeneration: "a-current-successor",
        },
      }), null);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
);

it.effect("generation-less readback compares every remaining identity field and rejects incomplete identity", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    yield* recordStoppedFixture({
      commandId: f.input.commandId,
      binding: f.input.target.binding,
      actor: f.principal,
      result: "stopped",
    });
    const methods = StopStore.makeRuntimeStopMethods(yield* SqlClient.SqlClient);
    const otherDriver = ProviderDriverKind.make("synthetic-other");
    const different: ReadonlyArray<OrchestrationV2ProviderThread> = [
      { ...f.provider, appThreadId: ThreadId.make("thread:other") },
      { ...f.provider, id: ProviderThreadId.make("provider-thread:other") },
      { ...f.provider, providerSessionId: ProviderSessionId.make("session:other") },
      { ...f.provider, providerInstanceId: ProviderInstanceId.make("instance:other") },
      { ...f.provider, nativeThreadRef: { ...f.provider.nativeThreadRef!, nativeId: "native:other" } },
      { ...f.provider, driver: otherDriver, nativeThreadRef: { ...f.provider.nativeThreadRef!, driver: otherDriver } },
    ];
    for (const provider of different) assert.equal(yield* methods.readStoppedRuntime(provider), null);
    const incomplete: ReadonlyArray<OrchestrationV2ProviderThread> = [
      { ...f.provider, appThreadId: null },
      { ...f.provider, providerSessionId: null },
      { ...f.provider, nativeThreadRef: null },
      { ...f.provider, nativeThreadRef: { ...f.provider.nativeThreadRef!, nativeId: "" } },
      { ...f.provider, nativeThreadRef: { ...f.provider.nativeThreadRef!, driver: otherDriver } },
    ];
    for (const provider of incomplete)
      assert.equal((yield* methods.readStoppedRuntime(provider).pipe(Effect.result))._tag, "Failure");
  }).pipe(Effect.scoped, Effect.provide(testLayer)),
);

it.effect("duplicate stopped commands count as one binding and two distinct generations are ambiguous", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    const methods = StopStore.makeRuntimeStopMethods(yield* SqlClient.SqlClient);
    assert.equal(yield* methods.readStoppedRuntime(f.provider), null);
    for (const suffix of ["original", "duplicate-a", "duplicate-b"]) {
      yield* recordStoppedFixture({
        commandId: CommandId.make(`command:stopped:${suffix}`),
        binding: f.input.target.binding,
        actor: f.principal,
        result: "stopped",
      });
    }
    assert.deepEqual(yield* methods.readStoppedRuntime(f.provider), f.input.target.binding);
    yield* recordStoppedFixture({
      commandId: CommandId.make("command:stopped:other-generation"),
      binding: { ...f.input.target.binding, runtimeGeneration: "another-stopped-generation" },
      actor: f.principal,
      result: "stopped",
    });
    const ambiguous = yield* methods.readStoppedRuntime(f.provider).pipe(Effect.result);
    assert.equal(ambiguous._tag, "Failure");
    if (ambiguous._tag === "Failure") assert.equal(ambiguous.failure.reason, "runtime_stop_readback_ambiguous");
  }).pipe(Effect.scoped, Effect.provide(testLayer)),
);

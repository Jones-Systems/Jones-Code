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
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Stream from "effect/Stream";
import * as Schema from "effect/Schema";
import { ProviderAdapterCloseSessionError } from "../../orchestration-v2/ProviderAdapter.ts";
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
const testLayer = EventSink.layer.pipe(Layer.provideMerge(stores));
const fixture = Effect.fnUntraced(function* () {
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
  const state = { current: true, invocations: 0, unknown: false, available: true, wait: false };
  const started = yield* Deferred.make<void>();
  const completion = yield* Deferred.make<void>();
  const captured = {
    binding,
    evidenceRevision: 1,
    isCurrent: Effect.sync(() => state.current),
    stop: Effect.gen(function* () {
      state.invocations++;
      yield* Deferred.succeed(started, undefined);
      if (state.wait) yield* Deferred.await(completion);
      if (state.unknown)
        return yield* new StopStore.RuntimeStopError({ reason: "synthetic_lost_stop" });
    }),
  };
  const manager = Layer.succeed(ProviderSessions.ProviderSessionManagerV2, {
    isMcpCallerAttached: () => Effect.succeed(false),
    shutdown: Effect.void,
    open: () => Effect.die("unexpected_open"),
    get: () => Effect.succeed(Option.none()),
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
  const service = RuntimeStop.layer.pipe(Layer.provide(manager));
  const input: StopCurrentThreadRuntimeInput = {
    commandId: CommandId.make("command:runtime-stop"),
    threadId,
    target: { binding, evidenceRevision: 1 },
  };
  return { sink, sessions, principal, state, input, runId, service, started, completion };
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

import { assert, it } from "@effect/vitest";
import { EnvironmentId, MessageId } from "@t3tools/contracts";
import * as Exit from "effect/Exit";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as Projection from "../../orchestration-v2/ProjectionStore.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import { makeDependencyStore } from "./DependencyStore.ts";
import { makeJonesTablePresence } from "../persistence/JonesTablePresence.ts";
import * as Changes from "./DependencyChanges.ts";
import { makeCommit } from "./Transactions.ts";
import * as Watch from "./RequestWatch.ts";
import * as Fixture from "./testkit.ts";

// Observation ports delegate to real EventSink streams and real SQL projections.
// Only provider-continuation completion is synthetic: no provider process is launched.
import * as Stream from "effect/Stream";

const observationPorts = (continuation?: { pending: () => Effect.Effect<boolean>; await: Effect.Effect<void> }) => Layer.unwrap(Effect.gen(function* () {
  const projection = yield* Projection.ProjectionStoreV2;
  const sink = yield* EventSink.EventSinkV2;
  return Layer.mock(Orchestrator.OrchestratorV2)({
    getThreadRecords: (threadId, fields, filter) => projection.getThreadRecords(threadId, fields, filter).pipe(Effect.mapError((cause) => new Orchestrator.OrchestratorProjectionError({ threadId, cause }))),
    getThreadEventSequence: (threadId) => sink.latestSequence({ threadId }).pipe(Effect.mapError((cause) => new Orchestrator.OrchestratorProjectionError({ threadId, cause }))),
    streamStoredEventsFrom: (input) => sink.stream(input).pipe(Stream.mapError((cause) => new Orchestrator.OrchestratorDomainEventStreamError({ cause }))),
    restartContinuationPending: continuation?.pending ?? (() => Effect.succeed(false)),
    awaitRestartContinuation: () => continuation?.await ?? Effect.void,
  });
}));
const layer = (continuation?: Parameters<typeof observationPorts>[0]) => Watch.layer.pipe(Layer.provideMerge(observationPorts(continuation)), Layer.provideMerge(Fixture.layer));

it.effect("subscribe then read catches an explicit early answer and wakes on a Jones-only answer change", () => Effect.gen(function* () {
  const f = yield* Fixture.seed;
  const watch = yield* Watch.RequestWatch;
  const changes = yield* Changes.DependencyChanges;
  const input = yield* f.send("observe");
  const initial = yield* watch.observe({ originEnvironmentId: "local", operationId: input.operationId, timeoutMs: 0 });
  const waiting = yield* watch.observe({ originEnvironmentId: "local", operationId: input.operationId, afterToken: initial.token, timeoutMs: Infinity }).pipe(Effect.forkScoped);
  yield* makeCommit(f.sql, f.store, changes)(f.store.answer(f.receiver.id, input.requestId, "fulfilled", "Explicit answer", f.receiverRun.id));
  const observed = yield* Fiber.join(waiting);
  assert.equal(observed.outcome, "fulfilled");
  assert.equal(observed.result, "Explicit answer");
  assert.equal((yield* watch.observe({ originEnvironmentId: "local", operationId: input.operationId, timeoutMs: 0 })).outcome, "fulfilled");
}).pipe(Effect.provide(layer())));

it.effect("active to pending continuation to eventless decline is observed without treating either as an answer", () => Effect.gen(function* () {
  const complete = yield* Deferred.make<void>();
  const subscribed = yield* Deferred.make<void>();
  let pending = false;
  const ports = {
    pending: () => Effect.sync(() => pending),
    await: Deferred.succeed(subscribed, undefined).pipe(Effect.andThen(Deferred.await(complete))),
  };
  yield* Effect.gen(function* () {
    const f = yield* Fixture.seed;
    const watch = yield* Watch.RequestWatch;
    const input = yield* f.send("continuation");
    const initial = yield* watch.observe({ originEnvironmentId: "local", operationId: input.operationId, timeoutMs: 0 });
    pending = true;
    yield* f.sink.write({ events: [f.event({ type: "run.updated", threadId: f.receiver.id, runId: f.receiverRun.id, payload: { ...f.receiverRun, status: "completed", completedAt: Fixture.time } })] });
    const held = yield* watch.observe({ originEnvironmentId: "local", operationId: input.operationId, timeoutMs: 0 });
    assert.equal(held.observation, "receiver_working");
    const waiting = yield* watch.observe({ originEnvironmentId: "local", operationId: input.operationId, afterToken: held.token, timeoutMs: Infinity }).pipe(Effect.forkScoped);
    yield* Deferred.await(subscribed);
    pending = false;
    yield* Deferred.succeed(complete, undefined);
    const observed = yield* Fiber.join(waiting);
    assert.equal(observed.observation, "receiver_idle_unanswered");
    assert.equal(observed.outcome, null);
    assert.equal(observed.state, "pending");
    assert.notEqual(observed.token, initial.token);
  }).pipe(Effect.provide(layer(ports)));
}));

it.effect("a lost direct response reconciles without an admission row, and fences reject late direct delivery", () => Effect.gen(function* () {
  const f = yield* Fixture.seed;
  const watch = yield* Watch.RequestWatch;
  const local = f.requestInput("remote-direct");
  const { localDependency: _local, ...remote } = { ...local, originKind: "linked" as const, originEnvironmentId: "linked-origin" };
  yield* f.send("remote-direct", f.receiverRun, remote);
  const observed = yield* watch.observe({ originEnvironmentId: "linked-origin", operationId: remote.operationId, fence: true, timeoutMs: 0 });
  assert.equal(observed.request?.messageId, remote.messageId);
  assert.equal(observed.admissionId, null);
  const missing = yield* watch.observe({ originEnvironmentId: "linked-origin", operationId: "lost-before-arrival", fence: true, timeoutMs: 0 });
  assert.equal(missing.outcome, "not_sent");
  const refused = yield* f.send("late-direct", f.receiverRun, { ...remote, requestId: "late-request", operationId: "lost-before-arrival", messageId: MessageId.make("late-message"), commandId: "late-command" }).pipe(Effect.exit);
  assert.isTrue(Exit.isFailure(refused));
}).pipe(Effect.provide(layer())));

it.effect("admission dispatching remains observed until its exact request binding exists; unrelated origins cannot read it", () => Effect.gen(function* () {
  const f = yield* Fixture.seed;
  const watch = yield* Watch.RequestWatch;
  const operation = "dispatch-gap";
  yield* f.sql`INSERT INTO jones_remote_admissions (id, client_id, idempotency_key, request_digest, binding_digest, envelope_json, status, command_id, created_at, updated_at, origin_environment_id, operation_id)
    VALUES ('admission-gap', 'session', 'key', 'digest', 'binding', '{}', 'dispatching', 'dispatch-command', '2026-10-26', '2026-10-26', 'linked-origin', ${operation})`;
  const dispatching = yield* watch.observe({ originEnvironmentId: "linked-origin", operationId: operation, fence: true, timeoutMs: 0 });
  assert.equal(dispatching.observation, "admission_dispatching");
  assert.equal(dispatching.outcome, null);
  assert.equal(dispatching.admissionId, "admission-gap");
  const stranger = yield* watch.observe({ originEnvironmentId: "other-origin", operationId: operation, timeoutMs: 0 });
  assert.equal(stranger.admissionId, null);
  const local = f.requestInput("gap-request");
  const { localDependency: _local, ...remote } = { ...local, originKind: "linked" as const, originEnvironmentId: "linked-origin", operationId: operation };
  yield* f.send("gap-request", f.receiverRun, remote);
  yield* f.sql.withTransaction(f.store.answer(f.receiver.id, remote.requestId, "fulfilled", "Recovered explicit result", f.receiverRun.id));
  const answered = yield* watch.observe({ originEnvironmentId: "linked-origin", operationId: operation, fence: true, timeoutMs: 0 });
  assert.equal(answered.outcome, "fulfilled");
  assert.equal(answered.request?.requestId, remote.requestId);
}).pipe(Effect.provide(layer())));

it.effect("restart intent retains the exact original forwarding key, namespace and thread; unresolved states remain duties", () => Effect.gen(function* () {
  const f = yield* Fixture.seed;
  const input = { dependencyId: "restart-dependency", operationId: "restart-operation", waiterThreadId: f.waiter.id, waiterRunId: f.waiterRun.id, waiterGeneration: 0,
    targetEnvironmentId: "peer", targetThreadId: f.receiver.id, targetLabel: "Peer receiver", forwardedRequestKey: "fwd:original-key", requestDigest: "original-digest",
    scope: { environmentId: EnvironmentId.make("local"), requestNamespace: "original-namespace", thread: { threadId: f.waiter.id, providerSessionId: "original-session", providerInstanceId: f.waiter.providerInstanceId } },
  };
  yield* f.sql.withTransaction(f.store.prepare(input));
  const sql = yield* SqlClient.SqlClient;
  const recovered = makeDependencyStore(sql, makeJonesTablePresence(sql));
  assert.deepEqual((yield* recovered.get(input.dependencyId))?.scope, input.scope);
  assert.equal((yield* recovered.get(input.dependencyId))?.forwardedRequestKey, input.forwardedRequestKey);
  assert.equal((yield* recovered.duties(f.waiter.id)).outgoing, 1);
  yield* sql.withTransaction(recovered.observe(input.dependencyId, 0, "unreachable"));
  assert.equal((yield* recovered.get(input.dependencyId))?.outcome, null);
  yield* sql.withTransaction(recovered.resolve(input.dependencyId, 0, "unknown", "Observation ended; remote work may continue."));
  assert.equal((yield* recovered.get(input.dependencyId))?.state, "resolved");
  assert.equal((yield* recovered.duties(f.waiter.id)).outgoing, 1);
}).pipe(Effect.provide(layer())));

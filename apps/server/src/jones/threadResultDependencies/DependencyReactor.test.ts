import { assert, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { EnvironmentId, OrchestratorMcpFailure } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Queue from "effect/Queue";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as Forwarding from "../../peer/PeerForwarding.ts";
import { DependencyWakeCommit } from "./DependencyCommit.ts";
import * as Changes from "./DependencyChanges.ts";
import * as Reactor from "./DependencyReactor.ts";
import * as Watch from "./RequestWatch.ts";
import { makeCommit } from "./Transactions.ts";
import * as Fixture from "./testkit.ts";

// Exercise the real follower, SQL facts and EventSink. The peer transport and
// provider-launch boundary are ports; neither launches a process in this test.
const ports = (
  f: Effect.Success<typeof Fixture.seed>,
  acquisition?: Orchestrator.OrchestratorV2Shape["acquireDomainEvents"],
) =>
  Layer.mock(Orchestrator.OrchestratorV2)({
    getThreadRecords: (threadId, fields, filter) =>
      f.projection
        .getThreadRecords(threadId, fields, filter)
        .pipe(
          Effect.mapError(
            (cause) => new Orchestrator.OrchestratorProjectionError({ threadId, cause }),
          ),
        ),
    getThreadEventSequence: (threadId) =>
      f.sink
        .latestSequence({ threadId })
        .pipe(
          Effect.mapError(
            (cause) => new Orchestrator.OrchestratorProjectionError({ threadId, cause }),
          ),
        ),
    streamStoredEventsFrom: (input) =>
      f.sink
        .stream(input)
        .pipe(
          Stream.mapError(
            (cause) => new Orchestrator.OrchestratorDomainEventStreamError({ cause }),
          ),
        ),
    acquireDomainEvents:
      acquisition ??
      f.sink.latestSequence().pipe(
        Effect.map((afterSequence) =>
          f.sink.stream({ afterSequence }).pipe(
            Stream.map((stored) => stored.event),
            Stream.mapError(
              (cause) => new Orchestrator.OrchestratorDomainEventStreamError({ cause }),
            ),
          ),
        ),
        Effect.mapError((cause) => new Orchestrator.OrchestratorDomainEventStreamError({ cause })),
      ),
    streamDomainEvents: f.sink.stream({ afterSequence: 0 }).pipe(
      Stream.map((stored) => stored.event),
      Stream.mapError((cause) => new Orchestrator.OrchestratorDomainEventStreamError({ cause })),
    ),
    restartContinuationPending: () => Effect.succeed(false),
    awaitRestartContinuation: () => Effect.void,
    finalizeDelegatedTask: () => Effect.void,
    dispatch: (command) =>
      Effect.gen(function* () {
        if (command.type !== "message.dispatch" || command.notification === undefined)
          return yield* Effect.die("Unexpected reactor dispatch");
        const wake = yield* DependencyWakeCommit;
        const run = f.run(`processing:${command.messageId}`, command.threadId, "queued", 2);
        const message = {
          ...f.message(command.messageId, run),
          text: command.text,
          notification: command.notification,
        };
        const committed = yield* f.sink.commitCommand({
          commandId: command.commandId,
          commandType: command.type,
          threadId: command.threadId,
          acceptedAt: Fixture.time,
          dependencyWake: wake,
          events: [
            f.event({
              type: "run.created",
              threadId: command.threadId,
              runId: run.id,
              payload: run,
            }),
            f.event({
              type: "message.updated",
              threadId: command.threadId,
              runId: run.id,
              payload: message,
            }),
          ],
          effects: [],
        });
        return { sequence: committed.receipt.resultSequence, storedEvents: committed.storedEvents };
      }).pipe(
        Effect.mapError(
          (cause) =>
            new Orchestrator.OrchestratorDispatchError({
              commandId: command.commandId,
              commandType: command.type,
              cause,
            }),
        ),
      ),
  });
const prepare = (f: Effect.Success<typeof Fixture.seed>) =>
  f.sql.withTransaction(
    f.store.prepare({
      dependencyId: "recovered-remote",
      operationId: "original-operation",
      waiterThreadId: f.waiter.id,
      waiterRunId: f.waiterRun.id,
      targetEnvironmentId: "peer",
      targetThreadId: f.receiver.id,
      targetLabel: "Remote receiver",
      forwardedRequestKey: "fwd:original-key",
      requestDigest: "original-digest",
      waiterGeneration: 0,
      scope: {
        environmentId: EnvironmentId.make("local"),
        requestNamespace: "original-namespace",
        thread: {
          threadId: f.waiter.id,
          providerSessionId: "original-session",
          providerInstanceId: f.waiter.providerInstanceId,
        },
      },
    }),
  );

it.effect(
  "restart reconciles only by observation; revoked authority yields one unknown wake and no remote cancellation",
  () =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const changes = yield* Changes.DependencyChanges;
      yield* prepare(f);
      const subscription = yield* changes.subscribe;
      const calls: string[] = [];
      const peer = Layer.mock(Forwarding.PeerForwarding)({
        call: (scope, tool, _environmentId, input) =>
          Effect.suspend(() => {
            calls.push(tool.name);
            assert.equal(scope.requestNamespace, "original-namespace");
            assert.deepEqual<unknown>(input, {
              operationId: "original-operation",
              fence: true,
              timeoutMs: 50_000,
            });
            return Effect.fail(
              new OrchestratorMcpFailure({ code: "capability_denied", message: "Link revoked" }),
            );
          }),
      });
      yield* Effect.gen(function* () {
        const reactor = yield* Reactor.DependencyReactor;
        yield* reactor.start();
        const delivered = yield* Stream.fromSubscription(subscription).pipe(
          Stream.mapEffect(() => f.store.get("recovered-remote")),
          Stream.filter((dependency) => dependency?.state === "delivered"),
          Stream.runHead,
        );
        assert.isTrue(Option.isSome(delivered));
        assert.deepEqual(calls, ["t3_thread_request_observe"]);
        const dependency = (yield* f.store.get("recovered-remote"))!;
        // Delivery is committed before change publication. Reconciliation may
        // interrupt the completed follower before its dispatch port returns.
        const messages = (yield* f.projection.getThreadRecords(f.waiter.id, ["messages"])).messages;
        assert.lengthOf(messages, 1);
        assert.equal(messages[0]?.id, dependency.wakeMessageId);
        assert.equal(messages[0]?.notification?.source.kind, "background_task");
        assert.equal(messages[0]?.notification?.outcome, "unknown");
        assert.include(messages[0]?.notification?.detail, "Remote work may continue");
        assert.equal(dependency.outcome, "unknown");
        assert.equal(dependency.forwardedRequestKey, "fwd:original-key");
        assert.equal((yield* f.store.duties(f.waiter.id)).outgoing, 1); // Result processing is still queued.
      }).pipe(
        Effect.provide(
          Reactor.layer.pipe(
            Layer.provide(Watch.layer),
            Layer.provide(ports(f)),
            Layer.provide(peer),
            Layer.provide(NodeCrypto.layer),
          ),
        ),
      );
    }).pipe(Effect.provide(Fixture.layer)),
);

it.effect(
  "temporary unreachability stays a visible duty and local release never calls remote Stop",
  () =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const changes = yield* Changes.DependencyChanges;
      yield* prepare(f);
      const subscription = yield* changes.subscribe;
      const calls: string[] = [];
      const peer = Layer.mock(Forwarding.PeerForwarding)({
        call: (_scope, tool) =>
          Effect.suspend(() => {
            calls.push(tool.name);
            return Effect.fail(
              new OrchestratorMcpFailure({ code: "orchestration_error", message: "Unreachable" }),
            );
          }),
      });
      yield* Effect.gen(function* () {
        const reactor = yield* Reactor.DependencyReactor;
        yield* reactor.start();
        yield* Stream.fromSubscription(subscription).pipe(
          Stream.mapEffect(() => f.store.get("recovered-remote")),
          Stream.filter((dependency) => dependency?.observation === "unreachable"),
          Stream.runHead,
        );
        assert.equal((yield* f.store.duties(f.waiter.id)).outgoing, 1);
        assert.deepEqual(
          (yield* f.projection.getThreadRecords(f.waiter.id, ["messages"])).messages,
          [],
        );
        yield* makeCommit(
          f.sql,
          f.store,
          changes,
        )(f.store.release(f.waiter.id, "recovered-remote"));
        assert.equal((yield* f.store.get("recovered-remote"))?.state, "released");
        assert.isTrue(calls.every((tool) => tool === "t3_thread_request_observe"));
      }).pipe(
        Effect.provide(
          Reactor.layer.pipe(
            Layer.provide(Watch.layer),
            Layer.provide(ports(f)),
            Layer.provide(peer),
            Layer.provide(NodeCrypto.layer),
          ),
        ),
      );
    }).pipe(Effect.provide(Fixture.layer)),
);

it.effect(
  "captures the committed start cursor and catches a real event before stream admission",
  () =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const before = yield* f.sink.latestSequence();
      const gate = yield* Deferred.make<void>();
      const finalized = yield* Queue.unbounded<typeof f.waiter.id>();
      const cursors: unknown[] = [];
      const observed = Layer.effect(
        Orchestrator.OrchestratorV2,
        Effect.gen(function* () {
          const original = yield* Orchestrator.OrchestratorV2;
          return {
            ...original,
            acquireDomainEvents: f.sink.latestSequence().pipe(
              Effect.map((afterSequence) => {
                cursors.push({ afterSequence });
                return Stream.unwrap(
                  Deferred.await(gate).pipe(
                    Effect.as(
                      f.sink.stream({ afterSequence }).pipe(
                        Stream.map((stored) => stored.event),
                        Stream.mapError(
                          (cause) => new Orchestrator.OrchestratorDomainEventStreamError({ cause }),
                        ),
                      ),
                    ),
                  ),
                );
              }),
              Effect.mapError(
                (cause) => new Orchestrator.OrchestratorDomainEventStreamError({ cause }),
              ),
            ),
            finalizeDelegatedTask: (id: typeof f.waiter.id) =>
              Queue.offer(finalized, id).pipe(Effect.asVoid),
          };
        }),
      ).pipe(Layer.provide(ports(f)));
      yield* Effect.gen(function* () {
        const reactor = yield* Reactor.DependencyReactor;
        yield* reactor.start();
        assert.deepEqual(cursors, [{ afterSequence: before }]);
        yield* f.sink.write({
          events: [
            f.event({
              type: "run.updated",
              threadId: f.waiter.id,
              runId: f.waiterRun.id,
              payload: f.waiterRun,
            }),
          ],
        });
        assert.equal(yield* Queue.size(finalized), 0); // Actual source is still held at its admission gate.
        yield* Deferred.succeed(gate, undefined);
        assert.equal(yield* Queue.take(finalized), f.waiter.id);
      }).pipe(
        Effect.provide(
          Reactor.layer.pipe(
            Layer.provide(Watch.layer),
            Layer.provide(observed),
            Layer.provide(
              Layer.mock(Forwarding.PeerForwarding)({
                call: () => Effect.die("Local event must not call a peer"),
              }),
            ),
            Layer.provide(NodeCrypto.layer),
          ),
        ),
      );
    }).pipe(Effect.provide(Fixture.layer)),
);

it.effect("failed baseline acquisition refuses startup before admitting subscribers", () =>
  Effect.gen(function* () {
    const f = yield* Fixture.seed;
    const changes = yield* Changes.DependencyChanges;
    const failure = new Orchestrator.OrchestratorDomainEventStreamError({
      cause: "synthetic baseline read failure",
    });
    let subscriptions = 0;
    const observedChanges = Layer.succeed(Changes.DependencyChanges, {
      ...changes,
      subscribe: changes.subscribe.pipe(
        Effect.tap(() =>
          Effect.sync(() => {
            subscriptions++;
          }),
        ),
      ),
    });
    yield* Effect.gen(function* () {
      const reactor = yield* Reactor.DependencyReactor;
      const result = yield* reactor.start().pipe(Effect.result);
      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") assert.strictEqual(result.failure, failure);
      assert.equal(subscriptions, 0);
    }).pipe(
      Effect.provide(
        Reactor.layer.pipe(
          Layer.provide(Watch.layer),
          Layer.provide(ports(f, Effect.fail(failure))),
          Layer.provide(observedChanges),
          Layer.provide(NodeCrypto.layer),
          Layer.provide(
            Layer.mock(Forwarding.PeerForwarding)({
              call: () => Effect.die("Failed acquisition must not admit a follower"),
            }),
          ),
        ),
      ),
    );
  }).pipe(Effect.provide(Fixture.layer)),
);

it.effect("the acquired domain consumer drains when its owning startup scope closes", () =>
  Effect.gen(function* () {
    const f = yield* Fixture.seed;
    const entered = yield* Deferred.make<void>();
    const withheld = yield* Deferred.make<void>();
    let acquisitions = 0;
    let drained = false;
    const acquisition = Effect.sync(() => {
      acquisitions++;
    }).pipe(
      Effect.andThen(f.sink.latestSequence()),
      Effect.map((afterSequence) =>
        Stream.unwrap(
          Deferred.succeed(entered, undefined).pipe(
            Effect.andThen(Deferred.await(withheld)),
            Effect.as(
              f.sink.stream({ afterSequence }).pipe(
                Stream.map((stored) => stored.event),
                Stream.mapError(
                  (cause) => new Orchestrator.OrchestratorDomainEventStreamError({ cause }),
                ),
              ),
            ),
          ),
        ).pipe(
          Stream.ensuring(
            Effect.sync(() => {
              drained = true;
            }),
          ),
        ),
      ),
      Effect.mapError((cause) => new Orchestrator.OrchestratorDomainEventStreamError({ cause })),
    );
    yield* Effect.scoped(
      Effect.gen(function* () {
        const reactor = yield* Reactor.DependencyReactor;
        yield* reactor.start();
        yield* Deferred.await(entered);
        assert.equal(acquisitions, 1);
        assert.isFalse(drained);
      }),
    ).pipe(
      Effect.provide(
        Reactor.layer.pipe(
          Layer.provide(Watch.layer),
          Layer.provide(ports(f, acquisition)),
          Layer.provide(NodeCrypto.layer),
          Layer.provide(
            Layer.mock(Forwarding.PeerForwarding)({
              call: () => Effect.die("This local subscription must not call a peer"),
            }),
          ),
        ),
      ),
    );
    assert.isTrue(drained);
  }).pipe(Effect.provide(Fixture.layer)),
);

// Let ready fibers and in-memory SQL settle without advancing the virtual clock.
const settle = Effect.forEach(Array.from({ length: 300 }), () => Effect.yieldNow, {
  discard: true,
});
const answered = {
  token: "done",
  state: "answered",
  request: null,
  admissionId: null,
  observation: "receiver_working",
  outcome: "fulfilled",
  result: "Done",
} as never;

it.effect("paces a peer failure that carries no delay of its own with local backoff", () =>
  Effect.gen(function* () {
    const f = yield* Fixture.seed;
    yield* prepare(f);
    let calls = 0;
    const peer = Layer.mock(Forwarding.PeerForwarding)({
      call: () =>
        Effect.suspend(() => {
          calls++;
          return Effect.fail(
            new OrchestratorMcpFailure({ code: "provider_unavailable", message: "Down" }),
          );
        }),
    });
    yield* Effect.gen(function* () {
      const reactor = yield* Reactor.DependencyReactor;
      yield* reactor.start();
      yield* settle;
      assert.equal(calls, 1);
      yield* TestClock.adjust(999);
      yield* settle;
      assert.equal(calls, 1);
      yield* TestClock.adjust(1);
      yield* settle;
      assert.equal(calls, 2);
      yield* TestClock.adjust(1_999);
      yield* settle;
      assert.equal(calls, 2);
      yield* TestClock.adjust(1);
      yield* settle;
      assert.equal(calls, 3);
    }).pipe(
      Effect.provide(
        Reactor.layer.pipe(
          Layer.provide(Watch.layer),
          Layer.provide(ports(f)),
          Layer.provide(peer),
          Layer.provide(NodeCrypto.layer),
        ),
      ),
    );
  }).pipe(Effect.provide(Fixture.layer)),
);

it.effect("a follower that fails unexpectedly retries instead of waiting for another change", () =>
  Effect.gen(function* () {
    const f = yield* Fixture.seed;
    const changes = yield* Changes.DependencyChanges;
    yield* prepare(f);
    const subscription = yield* changes.subscribe;
    let calls = 0;
    const peer = Layer.mock(Forwarding.PeerForwarding)({
      call: () =>
        Effect.suspend(() => {
          calls++;
          return calls === 1 ? Effect.die("Synthetic follower defect") : Effect.succeed(answered);
        }),
    });
    yield* Effect.gen(function* () {
      const reactor = yield* Reactor.DependencyReactor;
      yield* reactor.start();
      yield* settle;
      assert.equal(calls, 1);
      yield* TestClock.adjust(1_000);
      const delivered = yield* Stream.fromSubscription(subscription).pipe(
        Stream.mapEffect(() => f.store.get("recovered-remote")),
        Stream.filter((dependency) => dependency?.state === "delivered"),
        Stream.runHead,
      );
      assert.isTrue(Option.isSome(delivered));
      assert.equal(calls, 2);
    }).pipe(
      Effect.provide(
        Reactor.layer.pipe(
          Layer.provide(Watch.layer),
          Layer.provide(ports(f)),
          Layer.provide(peer),
          Layer.provide(NodeCrypto.layer),
        ),
      ),
    );
  }).pipe(Effect.provide(Fixture.layer)),
);

it.effect("a domain-event subscriber that stops is reacquired after a delay", () =>
  Effect.gen(function* () {
    const f = yield* Fixture.seed;
    const reacquired = yield* Deferred.make<void>();
    let acquisitions = 0;
    const acquisition: Orchestrator.OrchestratorV2Shape["acquireDomainEvents"] = Effect.suspend(
      () => {
        acquisitions++;
        if (acquisitions === 1)
          return Effect.succeed(
            Stream.fail(
              new Orchestrator.OrchestratorDomainEventStreamError({
                cause: "synthetic stream loss",
              }),
            ),
          );
        return Deferred.succeed(reacquired, undefined).pipe(Effect.as(Stream.never));
      },
    );
    yield* Effect.gen(function* () {
      const reactor = yield* Reactor.DependencyReactor;
      yield* reactor.start();
      yield* settle;
      assert.equal(acquisitions, 1);
      yield* TestClock.adjust(999);
      yield* settle;
      assert.equal(acquisitions, 1);
      yield* TestClock.adjust(1);
      yield* Deferred.await(reacquired);
      assert.equal(acquisitions, 2);
    }).pipe(
      Effect.provide(
        Reactor.layer.pipe(
          Layer.provide(Watch.layer),
          Layer.provide(ports(f, acquisition)),
          Layer.provide(NodeCrypto.layer),
          Layer.provide(
            Layer.mock(Forwarding.PeerForwarding)({
              call: () => Effect.die("No dependency is open in this test"),
            }),
          ),
        ),
      ),
    );
  }).pipe(Effect.provide(Fixture.layer)),
);

it.effect("retention runs again while the reactor stays started", () =>
  Effect.gen(function* () {
    const f = yield* Fixture.seed;
    yield* Effect.gen(function* () {
      const reactor = yield* Reactor.DependencyReactor;
      yield* reactor.start(); // The startup pass finds nothing to retire.
      yield* prepare(f);
      yield* f.sql.withTransaction(f.store.release(f.waiter.id, "recovered-remote"));
      // Older than the replay window, so the next pass may retire it.
      yield* f.sql`UPDATE jones_thread_result_dependencies SET changed_sequence = -5000
        WHERE dependency_id = 'recovered-remote'`;
      yield* TestClock.adjust(60 * 60 * 1_000 - 1);
      yield* settle;
      assert.equal((yield* f.store.get("recovered-remote"))?.state, "released");
      yield* TestClock.adjust(1);
      yield* settle;
      assert.isUndefined(yield* f.store.get("recovered-remote"));
    }).pipe(
      Effect.provide(
        Reactor.layer.pipe(
          Layer.provide(Watch.layer),
          Layer.provide(ports(f)),
          Layer.provide(NodeCrypto.layer),
          Layer.provide(
            Layer.mock(Forwarding.PeerForwarding)({
              call: () => Effect.die("A released dependency must not call a peer"),
            }),
          ),
        ),
      ),
    );
  }).pipe(Effect.provide(Fixture.layer)),
);

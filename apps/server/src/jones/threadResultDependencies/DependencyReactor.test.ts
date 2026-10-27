import { assert, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { EnvironmentId, OrchestratorMcpFailure } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
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
const ports = (f: Effect.Success<typeof Fixture.seed>) =>
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

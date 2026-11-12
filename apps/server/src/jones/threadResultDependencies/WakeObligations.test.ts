import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  RunAttemptId,
  NodeId,
  ProviderThreadId,
  ProviderTurnId,
  ProviderDriverKind,
  OrchestrationV2RunAttempt,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as Forwarding from "../../peer/PeerForwarding.ts";
import * as Reactor from "./DependencyReactor.ts";
import * as Watch from "./RequestWatch.ts";
import * as StartRefusals from "../startRefusal/Store.ts";
import { makeDependencyStore } from "./DependencyStore.ts";
import { pruneEventLogLikeUpstream } from "../persistence/prunedEventLog.testkit.ts";
import { makeJonesTablePresence } from "../persistence/JonesTablePresence.ts";
import { roster } from "./Projection.ts";
import { notification } from "./Wake.ts";
import { wakeEligible } from "./WakeObligations.ts";
import * as Fixture from "./testkit.ts";

const decodeAttempt = Schema.decodeEffect(OrchestrationV2RunAttempt);

const admit = Effect.fnUntraced(function* (
  f: Effect.Success<typeof Fixture.seed>,
  key = "held",
  metadata = true,
) {
  const request = yield* f.send(key);
  yield* f.sql.withTransaction(
    f.store.answer(f.receiver.id, request.requestId, "fulfilled", "PEER_UNIQUE_ANSWER", null),
  );
  const dependency = (yield* f.store.get(`dependency:${key}`))!;
  const run = {
    ...f.run(`wake:${key}`, f.waiter.id, "starting", 2),
    startedAt: null,
    activeAttemptId: RunAttemptId.make(`attempt:${key}`),
    rootNodeId: NodeId.make(`root:${key}`),
  };
  const attempt = yield* decodeAttempt({
    id: run.activeAttemptId,
    runId: run.id,
    attemptOrdinal: 1,
    rootNodeId: run.rootNodeId,
    providerInstanceId: "codex",
    providerThreadId: "provider:peer",
    providerTurnId: null,
    providerSettlement: null,
    reason: "initial",
    status: "pending",
    startedAt: null,
    completedAt: null,
  });
  const message = {
    ...f.message(`wake-message:${key}`, run),
    ...(metadata ? { notification: notification(dependency) } : {}),
  };
  const commit = {
    dependencyId: dependency.dependencyId,
    generation: dependency.generation,
    waiterThreadId: f.waiter.id,
    messageId: message.id,
  };
  yield* f.sink.commitCommand({
    commandId: CommandId.make(`admit:${key}`),
    commandType: "message.dispatch",
    threadId: f.waiter.id,
    acceptedAt: Fixture.time,
    dependencyWake: commit,
    effects: [],
    events: [
      f.event({
        type: "run.created",
        threadId: f.waiter.id,
        runId: run.id,
        payload: { ...run, userMessageId: message.id },
      }),
      f.event({
        type: "run-attempt.created",
        threadId: f.waiter.id,
        runId: run.id,
        payload: attempt,
      }),
      f.event({ type: "message.updated", threadId: f.waiter.id, runId: run.id, payload: message }),
    ],
  });
  const refuse = (activeAttemptId = attempt.id) =>
    f.sink.writeIfRunCurrent({
      threadId: f.waiter.id,
      runId: run.id,
      activeAttemptId,
      expectedStatus: "starting",
      startRefusalReason: "control_held",
      effects: [],
      events: [
        f.event({
          type: "run.updated",
          threadId: f.waiter.id,
          runId: run.id,
          payload: {
            ...run,
            userMessageId: message.id,
            status: "failed",
            completedAt: Fixture.time,
          },
        }),
        f.event({
          type: "run-attempt.updated",
          threadId: f.waiter.id,
          runId: run.id,
          payload: { ...attempt, status: "failed", completedAt: Fixture.time },
        }),
      ],
    });
  return { request, dependency, run, attempt, message, commit, refuse };
});
const advance = Effect.fnUntraced(function* (f: Effect.Success<typeof Fixture.seed>) {
  yield* f.sql`UPDATE jones_thread_result_requests SET changed_sequence = -2000`;
  yield* f.sql`UPDATE jones_thread_result_dependencies SET changed_sequence = -2000`;
});
const witness = Effect.fnUntraced(function* (
  f: Effect.Success<typeof Fixture.seed>,
  original: Effect.Success<ReturnType<typeof admit>>,
  key = "external",
  native = true,
  invalid?: "unmatched" | "other_thread" | "held_run" | "unsupported",
  publish = true,
) {
  const run = {
    ...f.run(
      key,
      f.waiter.id,
      "running",
      (yield* f.sql<{
        ordinal: number;
      }>`SELECT MAX(ordinal) + 1 AS ordinal FROM orchestration_v2_projection_runs WHERE thread_id = ${f.waiter.id}`)[0]!
        .ordinal,
    ),
    activeAttemptId: RunAttemptId.make(`attempt:${key}`),
    rootNodeId: NodeId.make(`root:${key}`),
  };
  const attempt = {
    ...original.attempt,
    id: run.activeAttemptId,
    runId: run.id,
    rootNodeId: run.rootNodeId,
    status: "running" as const,
    startedAt: Fixture.time,
  };
  yield* f.sink.write({
    events: [
      f.event({ type: "run.created", threadId: f.waiter.id, runId: run.id, payload: run }),
      f.event({
        type: "run-attempt.created",
        threadId: f.waiter.id,
        runId: run.id,
        payload: attempt,
      }),
    ],
  });
  const event = f.event({
    type: "provider-turn.updated",
    threadId: f.waiter.id,
    runId: run.id,
    driver: ProviderDriverKind.make("codex"),
    payload: {
      id: ProviderTurnId.make(`turn:${key}`),
      providerThreadId: ProviderThreadId.make("provider:peer"),
      runAttemptId: attempt.id,
      nodeId: attempt.rootNodeId,
      ordinal: (yield* f.sql<{
        ordinal: number;
      }>`SELECT COALESCE(MAX(ordinal),0) + 1 AS ordinal FROM orchestration_v2_projection_provider_turns`)[0]!
        .ordinal,
      nativeTurnRef: native
        ? { driver: ProviderDriverKind.make("codex"), nativeId: key, strength: "strong" }
        : null,
      status: "running",
      startedAt: Fixture.time,
      completedAt: null,
    },
  });
  if (event.type !== "provider-turn.updated")
    return yield* Effect.die("Unexpected witness fixture");
  const candidate =
    invalid === undefined
      ? event
      : {
          ...event,
          threadId: invalid === "other_thread" ? f.receiver.id : event.threadId,
          runId: invalid === "held_run" ? original.run.id : event.runId,
          driver: ProviderDriverKind.make(invalid === "unsupported" ? "claude-code" : "codex"),
          payload: {
            ...event.payload,
            runAttemptId:
              invalid === "unmatched"
                ? RunAttemptId.make("unknown-attempt")
                : event.payload.runAttemptId,
          },
        };
  if (publish) yield* f.sink.write({ events: [candidate] });
  return candidate;
});

it.effect(
  "refused wake retains exactly one duty and answer, including prune before startup reconcile",
  () =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const wake = yield* admit(f);
      yield* wake.refuse();
      const held = (yield* f.store.get(wake.dependency.dependencyId))!;
      assert.equal(held.state, "resolved");
      assert.equal(held.wakeHold, "held");
      assert.equal(held.generation, 1);
      assert.equal((yield* f.store.duties(f.waiter.id)).outgoing, 1);
      assert.match(roster([held])[0]!.description!, /Result held/);
      assert.match(
        (yield* f.projection.getThreadShell(f.waiter.id))?.pendingBackgroundTasks?.[0]
          ?.description ?? "",
        /Result held/,
      );
      yield* advance(f);
      yield* f.sql.withTransaction(f.store.prune);
      const restarted = makeDependencyStore(f.sql, makeJonesTablePresence(f.sql));
      assert.equal((yield* restarted.requests(f.receiver.id))[0]?.result, "PEER_UNIQUE_ANSWER");
      assert.isFalse(wakeEligible((yield* restarted.get(held.dependencyId))!));
      assert.lengthOf(yield* f.sql`SELECT * FROM jones_peer_wake_bindings`, 1);
      const settlement = yield* f.sink
        .write({
          events: [
            f.event({
              type: "thread.settled",
              threadId: f.waiter.id,
              payload: { ...f.waiter, settledAt: Fixture.time },
            }),
          ],
        })
        .pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(settlement));
      assert.equal((yield* restarted.get(held.dependencyId))?.wakeHold, "held");
    }).pipe(Effect.provide(Fixture.layer)),
);

it.effect(
  "a pruned attempt birth denies accepted-start release and retains the held answer after restart",
  () =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const wake = yield* admit(f);
      yield* wake.refuse();
      const event = yield* witness(f, wake, "pruned-birth", true, undefined, false);
      const birth = (yield* f.sql<{ sequence: number }>`SELECT sequence FROM orchestration_events
      WHERE event_type = 'run-attempt.created' AND json_extract(payload_json, '$.id') = ${event.payload.runAttemptId}`)[0]!;
      const attemptBefore = yield* f.sql`SELECT * FROM orchestration_v2_projection_run_attempts
      WHERE attempt_id = ${event.payload.runAttemptId}`;
      assert.lengthOf(attemptBefore, 1);
      assert.equal(attemptBefore[0]?.run_id, event.runId);
      assert.equal(attemptBefore[0]?.provider_thread_id, event.payload.providerThreadId);
      assert.equal(attemptBefore[0]?.root_node_id, event.payload.nodeId);
      const head = yield* f.sink.write({
        events: [
          f.event({
            type: "thread.metadata-updated",
            threadId: f.waiter.id,
            payload: f.waiter,
          }),
        ],
      });
      assert.include(yield* pruneEventLogLikeUpstream(birth.sequence), birth.sequence);
      assert.lengthOf(
        yield* f.sql`SELECT * FROM orchestration_events WHERE sequence = ${birth.sequence}`,
        0,
      );
      assert.deepEqual(
        yield* f.sql`SELECT * FROM orchestration_v2_projection_run_attempts
      WHERE attempt_id = ${event.payload.runAttemptId}`,
        attemptBefore,
      );
      assert.lengthOf(
        yield* f.sql`SELECT * FROM orchestration_events WHERE event_id = ${head[0]!.event.id}`,
        1,
      );
      yield* f.sink.write({ events: [event] });
      assert.lengthOf(yield* f.store.wakes.armedBy(event.id), 0);
      yield* advance(f);
      yield* f.sql.withTransaction(f.store.prune);
      const restarted = makeDependencyStore(f.sql, makeJonesTablePresence(f.sql));
      const held = (yield* restarted.get(wake.dependency.dependencyId))!;
      assert.equal(held.state, "resolved");
      assert.equal(held.wakeHold, "held");
      assert.equal(held.generation, 1);
      assert.equal((yield* restarted.requests(f.receiver.id))[0]?.result, "PEER_UNIQUE_ANSWER");
      assert.equal((yield* restarted.duties(f.waiter.id)).outgoing, 1);
      assert.deepEqual((yield* restarted.dependencies()).filter(wakeEligible), []);
      const calls: string[] = [];
      const unexpected = (name: string) =>
        Effect.suspend(() => {
          calls.push(name);
          return Effect.die(`Unexpected held-duty ${name}`);
        });
      const ports = Layer.mergeAll(
        Layer.mock(Orchestrator.OrchestratorV2)({
          acquireDomainEvents: Effect.succeed(Stream.never),
          dispatch: () => unexpected("dispatch"),
          finalizeDelegatedTask: () => unexpected("finalize"),
        }),
        Layer.mock(Watch.RequestWatch)({ observe: () => unexpected("observe") }),
        Layer.mock(Forwarding.PeerForwarding)({ call: () => unexpected("forward") }),
        NodeCrypto.layer,
      );
      for (let restart = 0; restart < 2; restart++) {
        yield* Effect.scoped(
          Effect.gen(function* () {
            yield* (yield* Reactor.DependencyReactor).start();
            assert.deepEqual(calls, []);
            assert.equal((yield* restarted.get(held.dependencyId))?.wakeHold, "held");
            assert.equal((yield* restarted.duties(f.waiter.id)).outgoing, 1);
            assert.equal(
              (yield* restarted.requests(f.receiver.id))[0]?.result,
              "PEER_UNIQUE_ANSWER",
            );
          }).pipe(Effect.provide(Reactor.layer.pipe(Layer.provide(ports)))),
        );
      }

      assert.lengthOf(yield* restarted.wakes.armedBy(event.id), 0);
      assert.isTrue(
        Exit.isFailure(
          yield* f.sql
            .withTransaction(restarted.admitWake({ ...wake.commit, generation: held.generation }))
            .pipe(Effect.exit),
        ),
      );
      assert.equal((yield* restarted.get(held.dependencyId))?.wakeHold, "held");
      assert.lengthOf(yield* f.sql`SELECT * FROM jones_peer_wake_holds`, 1);
    }).pipe(Effect.provide(Fixture.layer)),
);

it.effect(
  "queued wake request survives prune before classification without relying on a binding",
  () =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const wake = yield* admit(f);
      yield* f.sink.write({
        events: [
          f.event({
            type: "run.updated",
            threadId: f.waiter.id,
            runId: wake.run.id,
            payload: { ...wake.run, userMessageId: wake.message.id, status: "queued" },
          }),
        ],
      });
      yield* f.sql`DELETE FROM jones_peer_wake_bindings`;
      yield* advance(f);
      yield* f.sql.withTransaction(f.store.prune);
      assert.equal((yield* f.store.requests(f.receiver.id))[0]?.result, "PEER_UNIQUE_ANSWER");
      yield* wake.refuse();
      assert.equal((yield* f.store.get(wake.dependency.dependencyId))?.state, "delivered");
    }).pipe(Effect.provide(Fixture.layer)),
);

it.effect.each([
  "legacy",
  "crossed",
  "missing",
  "extra",
  "epoch",
  "message",
  "generation",
  "stale",
  "rollback",
] as const)("%s evidence never holds or replays", (mode) =>
  Effect.gen(function* () {
    const f = yield* Fixture.seed;
    const wake = yield* admit(f, "negative", mode !== "legacy");
    if (mode === "crossed")
      yield* (yield* StartRefusals.StartRefusals).cross({
        threadId: f.waiter.id,
        runId: wake.run.id,
        attemptId: wake.attempt.id,
      });
    if (mode === "missing") yield* f.sql`DELETE FROM jones_peer_wake_bindings`;
    if (mode === "extra")
      yield* f.sink.write({
        events: [
          f.event({
            type: "run-attempt.created",
            threadId: f.waiter.id,
            runId: wake.run.id,
            payload: { ...wake.attempt, id: RunAttemptId.make("extra"), attemptOrdinal: 2 },
          }),
        ],
      });
    if (mode === "epoch") yield* f.sql.withTransaction(f.store.release(f.waiter.id));
    if (mode === "message")
      yield* f.sql`UPDATE jones_thread_result_dependencies SET wake_message_id = 'other'`;
    if (mode === "generation")
      yield* f.sql`UPDATE jones_thread_result_dependencies SET generation = generation + 1`;
    if (mode === "rollback")
      yield* f.sql`CREATE TEMP TRIGGER reject_hold BEFORE INSERT ON jones_peer_wake_holds BEGIN SELECT RAISE(ABORT,'rollback'); END`;
    const result = yield* wake
      .refuse(mode === "stale" ? RunAttemptId.make("stale") : wake.attempt.id)
      .pipe(Effect.exit);
    if (mode === "rollback") assert.isTrue(Exit.isFailure(result));
    assert.lengthOf(yield* f.sql`SELECT * FROM jones_peer_wake_holds`, 0);
  }).pipe(Effect.provide(Fixture.layer)),
);

it.effect(
  "only a newer accepted native start arms once; probe failure advances the witness fence",
  () =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const wake = yield* admit(f);
      yield* wake.refuse();
      yield* wake.refuse();
      assert.equal((yield* f.store.get(wake.dependency.dependencyId))?.generation, 1);
      const nullEvent = yield* witness(f, wake, "null", false);
      assert.lengthOf(yield* f.store.wakes.armedBy(nullEvent.id), 0);
      const event = yield* witness(f, wake);
      const holds = yield* f.store.wakes.armedBy(event.id);
      assert.lengthOf(holds, 1);
      assert.isFalse(yield* f.store.wakes.finishArming(holds[0]!, false));
      yield* f.sink.write({
        events: [
          {
            ...event,
            id: f.event({
              type: "run.updated",
              threadId: f.waiter.id,
              runId: wake.run.id,
              payload: wake.run,
            }).id,
          },
        ],
      });
      assert.equal((yield* f.store.get(wake.dependency.dependencyId))?.wakeHold, "held");
      const newer = yield* witness(f, wake, "newer");
      const armed = (yield* f.store.wakes.armedBy(newer.id))[0]!;
      assert.isTrue(yield* f.store.wakes.finishArming(armed, true));
      assert.isFalse(yield* f.store.wakes.finishArming(armed, true));
      assert.isTrue(wakeEligible((yield* f.store.get(wake.dependency.dependencyId))!));
    }).pipe(Effect.provide(Fixture.layer)),
);

it.effect(
  "individual delivered release is false; held release voids and fences arm/consume/admit without affecting another duty",
  () =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const wake = yield* admit(f);
      assert.isFalse(
        yield* f.sql.withTransaction(f.store.release(f.waiter.id, wake.dependency.dependencyId)),
      );
      yield* wake.refuse();
      yield* f.send("other");
      const event = yield* witness(f, wake);
      const hold = (yield* f.store.wakes.armedBy(event.id))[0]!;
      assert.isTrue(
        yield* f.sql.withTransaction(f.store.release(f.waiter.id, wake.dependency.dependencyId)),
      );
      assert.isFalse(yield* f.store.wakes.finishArming(hold, true));
      assert.isTrue(
        Exit.isFailure(
          yield* f.sql
            .withTransaction(f.store.admitWake({ ...wake.commit, generation: 1 }))
            .pipe(Effect.exit),
        ),
      );
      assert.equal((yield* f.store.get(wake.dependency.dependencyId))?.state, "released");
      assert.equal((yield* f.store.get("dependency:other"))?.state, "awaiting");
      assert.equal((yield* f.store.duties(f.waiter.id)).outgoing, 1);
    }).pipe(Effect.provide(Fixture.layer)),
);

it.effect.each(["stop", "archive", "delete"] as const)(
  "%s atomically voids a hold; reopening and late callbacks cannot revive it",
  (mode) =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const wake = yield* admit(f);
      yield* wake.refuse();
      const event = yield* witness(f, wake);
      const hold = (yield* f.store.wakes.armedBy(event.id))[0]!;
      yield* f.sink.commitCommand({
        commandId: CommandId.make(`lifecycle:${mode}`),
        threadId: f.waiter.id,
        commandType: mode === "stop" ? "thread.stop" : `thread.${mode}`,
        acceptedAt: Fixture.time,
        effects: [],
        events:
          mode === "stop"
            ? [
                f.event({
                  type: "run.updated",
                  threadId: f.waiter.id,
                  runId: f.waiterRun.id,
                  payload: f.waiterRun,
                }),
              ]
            : [
                f.event({
                  type: mode === "archive" ? "thread.archived" : "thread.deleted",
                  threadId: f.waiter.id,
                  payload: {
                    ...f.waiter,
                    archivedAt: mode === "archive" ? Fixture.time : null,
                    deletedAt: mode === "delete" ? Fixture.time : null,
                  },
                }),
              ],
      });
      assert.equal((yield* f.store.get(wake.dependency.dependencyId))?.state, "released");
      assert.equal((yield* f.sql`SELECT state FROM jones_peer_wake_holds`)[0]?.state, "void");
      assert.isFalse(yield* f.store.wakes.finishArming(hold, true));
      yield* f.sink.write({
        events: [
          f.event({ type: "thread.unarchived", threadId: f.waiter.id, payload: f.waiter }),
          f.event({ type: "thread.unsettled", threadId: f.waiter.id, payload: f.waiter }),
        ],
      });
      yield* witness(f, wake, "late");
      yield* wake.refuse();
      assert.equal((yield* f.store.get(wake.dependency.dependencyId))?.state, "released");
      assert.equal((yield* f.store.duties(f.waiter.id)).outgoing, 0);
    }).pipe(Effect.provide(Fixture.layer)),
);

it.effect("waiter hold/release/prune never deletes linked-origin answers or fences", () =>
  Effect.gen(function* () {
    const f = yield* Fixture.seed;
    const input = {
      ...f.requestInput("linked"),
      originKind: "linked" as const,
      localDependency: undefined,
    };
    const { localDependency: _local, ...linked } = input;
    yield* f.send("linked", f.receiverRun, linked);
    yield* f.sql.withTransaction(
      f.store.answer(f.receiver.id, linked.requestId, "fulfilled", "LINKED_DURABLE_ANSWER", null),
    );
    yield* f.sql.withTransaction(f.store.fence("linked-origin", "linked-operation"));
    const wake = yield* admit(f);
    yield* wake.refuse();
    yield* f.sql.withTransaction(f.store.release(f.waiter.id));
    yield* advance(f);
    yield* f.sql.withTransaction(f.store.prune);
    assert.equal(
      (yield* f.store.requests(f.receiver.id)).find((r) => r.requestId === linked.requestId)
        ?.result,
      "LINKED_DURABLE_ANSWER",
    );
    assert.isTrue(yield* f.store.fenced("linked-origin", "linked-operation"));
    assert.lengthOf(yield* f.sql`SELECT * FROM jones_peer_wake_bindings`, 0);
    assert.lengthOf(yield* f.sql`SELECT * FROM jones_peer_wake_holds`, 0);
  }).pipe(Effect.provide(Fixture.layer)),
);

it.effect.each(["unmatched", "other_thread", "held_run", "unsupported"] as const)(
  "%s native witness never arms a hold",
  (mode) =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const wake = yield* admit(f);
      yield* wake.refuse();
      const event = yield* witness(f, wake, "invalid", true, mode);
      assert.lengthOf(yield* f.store.wakes.armedBy(event.id), 0);
      assert.equal((yield* f.store.get(wake.dependency.dependencyId))?.wakeHold, "held");
    }).pipe(Effect.provide(Fixture.layer)),
);

it.effect("an attempt born before the hold and running projection alone cannot release it", () =>
  Effect.gen(function* () {
    const f = yield* Fixture.seed;
    const wake = yield* admit(f);
    const old = yield* witness(f, wake, "old", false);
    yield* wake.refuse();
    const event = {
      ...old,
      id: f.event({
        type: "run.updated",
        threadId: f.waiter.id,
        runId: wake.run.id,
        payload: wake.run,
      }).id,
      payload: {
        ...old.payload,
        nativeTurnRef: {
          driver: ProviderDriverKind.make("codex"),
          nativeId: "late-native",
          strength: "strong" as const,
        },
      },
    };
    yield* f.sink.write({ events: [event] });
    assert.equal((yield* f.store.get(wake.dependency.dependencyId))?.wakeHold, "held");
    yield* f.sink.write({
      events: [
        f.event({
          type: "run.updated",
          threadId: f.waiter.id,
          runId: wake.run.id,
          payload: { ...wake.run, status: "running" },
        }),
      ],
    });
    assert.equal((yield* f.store.get(wake.dependency.dependencyId))?.wakeHold, "held");
  }).pipe(Effect.provide(Fixture.layer)),
);

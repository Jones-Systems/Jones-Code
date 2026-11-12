import { assert, it } from "@effect/vitest";
import { CommandId, EnvironmentId, MessageId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Changes from "./DependencyChanges.ts";
import { roster } from "./Projection.ts";
import { notification } from "./Wake.ts";
import * as Fixture from "./testkit.ts";

const deliverQueuedWake = Effect.fnUntraced(function* (
  f: Effect.Success<typeof Fixture.seed>,
  key: string,
) {
  const input = yield* f.send(key);
  yield* f.sql.withTransaction(
    f.store.answer(f.receiver.id, input.requestId, "fulfilled", "Delivered result", null),
  );
  const dependency = (yield* f.store.get(`dependency:${key}`))!;
  const run = f.run(`${key}-wake`, f.waiter.id, "queued", 2);
  const wake = { ...f.message(`${key}-wake-message`, run), notification: notification(dependency) };
  yield* f.sink.commitCommand({
    commandId: CommandId.make(`accept:${key}-wake`),
    threadId: f.waiter.id,
    commandType: "message.dispatch",
    acceptedAt: Fixture.time,
    dependencyWake: {
      dependencyId: dependency.dependencyId,
      generation: dependency.generation,
      waiterThreadId: f.waiter.id,
      messageId: wake.id,
    },
    events: [
      f.event({
        type: "run.created",
        threadId: f.waiter.id,
        runId: run.id,
        payload: { ...run, userMessageId: wake.id },
      }),
      f.event({ type: "message.updated", threadId: f.waiter.id, runId: run.id, payload: wake }),
    ],
    effects: [],
  });
  return { dependency, run: { ...run, userMessageId: wake.id }, wake };
});

it.effect(
  "a completed foreground retains explicit result waiting; active work suppresses only presentation",
  () =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      yield* f.send("first");
      const waiting = yield* f.projection.getThreadShell(f.waiter.id);
      assert.equal(waiting?.status, "completed");
      assert.equal(
        waiting?.pendingBackgroundTasks?.[0]?.taskId,
        "jones-dependency:dependency:first",
      );
      assert.deepEqual(yield* f.store.duties(f.waiter.id), { outgoing: 1, owed: 0 });
      yield* f.sink.write({
        events: [
          f.event({
            type: "run.updated",
            threadId: f.waiter.id,
            runId: f.waiterRun.id,
            payload: { ...f.waiterRun, status: "running", completedAt: null },
          }),
        ],
      });
      assert.deepEqual(
        (yield* f.projection.getThreadShell(f.waiter.id))?.pendingBackgroundTasks,
        [],
      );
      assert.equal((yield* f.store.duties(f.waiter.id)).outgoing, 1);
    }).pipe(Effect.provide(Fixture.layer)),
);

it.effect("receiver completion, ordinary INFO and ordinary replies do not answer a request", () =>
  Effect.gen(function* () {
    const f = yield* Fixture.seed;
    yield* f.send("unanswered");
    yield* f.sink.write({
      events: [
        f.event({
          type: "run.updated",
          threadId: f.receiver.id,
          runId: f.receiverRun.id,
          payload: { ...f.receiverRun, status: "completed", completedAt: Fixture.time },
        }),
        f.event({
          type: "message.updated",
          threadId: f.receiver.id,
          runId: f.receiverRun.id,
          payload: {
            ...f.message("ordinary-reply", f.receiverRun, "assistant"),
            text: "INFO: here is a reply",
          },
        }),
      ],
    });
    assert.equal((yield* f.store.requests(f.receiver.id))[0]?.state, "pending");
    assert.equal((yield* f.store.get("dependency:unanswered"))?.state, "awaiting");
    assert.deepEqual(yield* f.store.duties(f.receiver.id), { outgoing: 0, owed: 1 });
  }).pipe(Effect.provide(Fixture.layer)),
);

it.effect(
  "two requests sharing one run answer independently, including an early answer and a later-run answer",
  () =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const first = yield* f.send("one");
      const second = yield* f.send("two");
      yield* f.sql.withTransaction(
        f.store.answer(
          f.receiver.id,
          first.requestId,
          "fulfilled",
          "First result",
          f.receiverRun.id,
        ),
      );
      assert.equal((yield* f.store.get("dependency:one"))?.state, "resolved");
      assert.equal((yield* f.store.get("dependency:two"))?.state, "awaiting");
      assert.equal(
        (yield* f.store.requests(f.receiver.id)).filter((request) => request.state === "pending")
          .length,
        1,
      );
      const later = f.run("later-run", f.receiver.id, "completed", 2);
      yield* f.sink.write({
        events: [
          f.event({
            type: "run.created",
            threadId: f.receiver.id,
            runId: later.id,
            payload: later,
          }),
        ],
      });
      yield* f.sql.withTransaction(
        f.store.answer(
          f.receiver.id,
          second.requestId,
          "declined",
          "Cannot supply the second result",
          later.id,
        ),
      );
      assert.equal(
        (yield* f.store.requests(f.receiver.id)).find(
          (request) => request.requestId === second.requestId,
        )?.resolvedByRunId,
        later.id,
      );
      assert.equal((yield* f.store.get("dependency:two"))?.outcome, "declined");
      assert.equal((yield* f.store.duties(f.waiter.id)).outgoing, 2); // Resolved, awaiting delivery.
    }).pipe(Effect.provide(Fixture.layer)),
);

it.effect(
  "only the receiver answers; identical retries succeed and a changed answer conflicts",
  () =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const input = yield* f.send("answer");
      assert.isTrue(
        Exit.isFailure(
          yield* f.sql
            .withTransaction(
              f.store.answer(f.waiter.id, input.requestId, "fulfilled", "spoofed", null),
            )
            .pipe(Effect.exit),
        ),
      );
      yield* f.sql.withTransaction(
        f.store.answer(f.receiver.id, input.requestId, "fulfilled", "Explicit result", null),
      );
      assert.equal(
        (yield* f.sql.withTransaction(
          f.store.answer(f.receiver.id, input.requestId, "fulfilled", "Explicit result", null),
        )).requester,
        "waiting",
      );
      const conflict = yield* f.sql
        .withTransaction(
          f.store.answer(f.receiver.id, input.requestId, "declined", "different", null),
        )
        .pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(conflict));
      assert.equal((yield* f.store.get("dependency:answer"))?.result, "Explicit result");
    }).pipe(Effect.provide(Fixture.layer)),
);

it.effect(
  "queue promotion follows message identity and explicit queue cancellation withdraws only its request",
  () =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const queued = f.run("queued", f.receiver.id, "queued", 2);
      yield* f.sink.write({
        events: [
          f.event({
            type: "run.created",
            threadId: f.receiver.id,
            runId: queued.id,
            payload: queued,
          }),
        ],
      });
      const promoted = yield* f.send("promoted", queued);
      const cancelled = yield* f.send("cancelled", queued);
      yield* f.sink.commitCommand({
        commandId: CommandId.make("promote"),
        threadId: f.receiver.id,
        commandType: "queued-message.promote-to-steer",
        acceptedAt: Fixture.time,
        events: [
          f.event({
            type: "run.updated",
            threadId: f.receiver.id,
            runId: queued.id,
            payload: { ...queued, status: "cancelled", completedAt: Fixture.time },
          }),
          f.event({
            type: "message.updated",
            threadId: f.receiver.id,
            runId: f.receiverRun.id,
            payload: f.message(promoted.messageId),
          }),
        ],
        effects: [],
      });
      assert.equal(
        (yield* f.store.requests(f.receiver.id)).find(
          (request) => request.requestId === promoted.requestId,
        )?.state,
        "pending",
      );
      yield* f.sink.commitCommand({
        commandId: CommandId.make("cancel-queue"),
        threadId: f.receiver.id,
        commandType: "queued-run.cancel",
        acceptedAt: Fixture.time,
        events: [
          f.event({
            type: "run.updated",
            threadId: f.receiver.id,
            runId: queued.id,
            payload: {
              ...queued,
              userMessageId: cancelled.messageId,
              status: "cancelled",
              completedAt: Fixture.time,
            },
          }),
        ],
        effects: [],
      });
      assert.equal(
        (yield* f.store.requests(f.receiver.id)).find(
          (request) => request.requestId === cancelled.requestId,
        )?.state,
        "withdrawn",
      );
      assert.equal(
        (yield* f.store.requests(f.receiver.id)).find(
          (request) => request.requestId === promoted.requestId,
        )?.state,
        "pending",
      );
    }).pipe(Effect.provide(Fixture.layer)),
);

it.effect("release invalidates late bind, answer and wake; the target remains active", () =>
  Effect.gen(function* () {
    const f = yield* Fixture.seed;
    const input = yield* f.send("released");
    yield* f.sql.withTransaction(f.store.release(f.waiter.id, "dependency:released"));
    const answer = yield* f.sql.withTransaction(
      f.store.answer(f.receiver.id, input.requestId, "fulfilled", "Late result", f.receiverRun.id),
    );
    assert.equal(answer.requester, "released");
    yield* f.sql.withTransaction(
      f.store.observe("dependency:released", 0, "receiver_working", answer.request),
    );
    yield* f.sql.withTransaction(
      f.store.resolve("dependency:released", 0, "fulfilled", "Late result"),
    );
    assert.equal((yield* f.store.get("dependency:released"))?.state, "released");
    assert.equal(
      (yield* f.projection.getThreadRecords(f.receiver.id, ["runs"])).runs[0]?.status,
      "running",
    );
    assert.isTrue(
      Exit.isFailure(
        yield* f.sql
          .withTransaction(
            f.store.admitWake({
              dependencyId: "dependency:released",
              generation: 0,
              waiterThreadId: f.waiter.id,
              messageId: MessageId.make("stale"),
            }),
          )
          .pipe(Effect.exit),
      ),
    );
  }).pipe(Effect.provide(Fixture.layer)),
);

it.effect(
  "Stop's epoch rejects a local dispatch still in flight and rolls back message plus receipt",
  () =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const stale = f.requestInput("after-stop");
      yield* f.sql.withTransaction(f.store.release(f.waiter.id));
      assert.isTrue(
        Exit.isFailure(yield* f.send("after-stop", f.receiverRun, stale).pipe(Effect.exit)),
      );
      assert.deepEqual(
        yield* f.sql`SELECT * FROM orchestration_v2_projection_messages WHERE message_id = ${stale.messageId}`,
        [],
      );
      assert.deepEqual(
        yield* f.sql`SELECT * FROM orchestration_command_receipts WHERE command_id = ${stale.commandId}`,
        [],
      );
      assert.deepEqual(yield* f.store.dependencies(f.waiter.id), []);
    }).pipe(Effect.provide(Fixture.layer)),
);

it.effect(
  "wake acceptance is generation-guarded and remains a duty through queue promotion until its own run terminates",
  () =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const input = yield* f.send("wake");
      yield* f.sql.withTransaction(
        f.store.answer(f.receiver.id, input.requestId, "fulfilled", "Bounded answer", null),
      );
      const dependency = (yield* f.store.get("dependency:wake"))!;
      const wake = f.message("wake-message", f.waiterRun);
      const note = notification(dependency);
      assert.equal(note.source.kind, "background_task");
      const wakeRun = f.run("wake-run", f.waiter.id, "queued", 2);
      yield* f.sink.commitCommand({
        commandId: CommandId.make("wake-accept"),
        threadId: f.waiter.id,
        commandType: "message.dispatch",
        acceptedAt: Fixture.time,
        dependencyWake: {
          dependencyId: dependency.dependencyId,
          generation: dependency.generation,
          waiterThreadId: f.waiter.id,
          messageId: wake.id,
        },
        events: [
          f.event({
            type: "run.created",
            threadId: f.waiter.id,
            runId: wakeRun.id,
            payload: wakeRun,
          }),
          f.event({
            type: "message.updated",
            threadId: f.waiter.id,
            runId: wakeRun.id,
            payload: { ...wake, runId: wakeRun.id, notification: note },
          }),
        ],
        effects: [],
      });
      assert.equal((yield* f.store.get(dependency.dependencyId))?.state, "delivered");
      assert.equal((yield* f.store.duties(f.waiter.id)).outgoing, 1);
      assert.deepEqual(roster(yield* f.store.dependencies(f.waiter.id)), []);
      yield* f.sink.write({
        events: [
          f.event({
            type: "run.updated",
            threadId: f.waiter.id,
            runId: wakeRun.id,
            payload: { ...wakeRun, status: "completed", completedAt: Fixture.time },
          }),
        ],
      });
      assert.equal((yield* f.store.duties(f.waiter.id)).outgoing, 0);
    }).pipe(Effect.provide(Fixture.layer)),
);

it.effect(
  "archive disposes owed requests and releases its outgoing waits in the accepting transaction",
  () =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      yield* f.send("archive");
      yield* f.sink.commitCommand({
        commandId: CommandId.make("archive"),
        threadId: f.receiver.id,
        commandType: "thread.archive",
        acceptedAt: Fixture.time,
        events: [
          f.event({
            type: "thread.archived",
            threadId: f.receiver.id,
            payload: { ...f.receiver, archivedAt: Fixture.time },
          }),
        ],
        effects: [],
      });
      assert.equal((yield* f.store.get("dependency:archive"))?.outcome, "dropped");
      assert.equal((yield* f.store.requests(f.receiver.id))[0]?.state, "dropped");
    }).pipe(Effect.provide(Fixture.layer)),
);

it.effect("an informational send creates no result duty or owed request", () =>
  Effect.gen(function* () {
    const f = yield* Fixture.seed;
    yield* f.sink.commitCommand({
      commandId: CommandId.make("ordinary-send"),
      threadId: f.receiver.id,
      commandType: "message.dispatch",
      acceptedAt: Fixture.time,
      events: [
        f.event({
          type: "message.updated",
          threadId: f.receiver.id,
          runId: f.receiverRun.id,
          payload: f.message("ordinary-message"),
        }),
      ],
      effects: [],
    });
    assert.deepEqual(yield* f.store.dependencies(f.waiter.id), []);
    assert.deepEqual(yield* f.store.requests(f.receiver.id), []);
    assert.deepEqual((yield* f.projection.getThreadShell(f.waiter.id))?.pendingBackgroundTasks, []);
  }).pipe(Effect.provide(Fixture.layer)),
);

it.effect(
  "rolling back a run drops all requests steered into it, without touching an answered request",
  () =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const first = yield* f.send("rollback-one");
      const second = yield* f.send("rollback-two");
      const answered = yield* f.send("rollback-answered");
      yield* f.sql.withTransaction(
        f.store.answer(f.receiver.id, answered.requestId, "fulfilled", "Already answered", null),
      );
      yield* f.sink.write({
        events: [
          f.event({
            type: "run.updated",
            threadId: f.receiver.id,
            runId: f.receiverRun.id,
            payload: { ...f.receiverRun, status: "rolled_back", completedAt: Fixture.time },
          }),
        ],
      });
      const requests = yield* f.store.requests(f.receiver.id);
      assert.equal(
        requests.find((request) => request.requestId === first.requestId)?.state,
        "dropped",
      );
      assert.equal(
        requests.find((request) => request.requestId === second.requestId)?.state,
        "dropped",
      );
      assert.equal(
        requests.find((request) => request.requestId === answered.requestId)?.state,
        "fulfilled",
      );
    }).pipe(Effect.provide(Fixture.layer)),
);

it.effect("a Stop winning the wake race rolls back the entire stale wake command", () =>
  Effect.gen(function* () {
    const f = yield* Fixture.seed;
    const input = yield* f.send("stale-wake");
    yield* f.sql.withTransaction(
      f.store.answer(f.receiver.id, input.requestId, "fulfilled", "Result", null),
    );
    yield* f.sink.commitCommand({
      commandId: CommandId.make("winning-stop"),
      threadId: f.waiter.id,
      commandType: "thread.stop",
      acceptedAt: Fixture.time,
      events: [
        f.event({ type: "thread.metadata-updated", threadId: f.waiter.id, payload: f.waiter }),
      ],
      effects: [],
    });
    const run = f.run("stale-wake-run", f.waiter.id, "queued", 2);
    const wake = f.message("stale-wake-message", run);
    const rejected = yield* f.sink
      .commitCommand({
        commandId: CommandId.make("losing-wake"),
        threadId: f.waiter.id,
        commandType: "message.dispatch",
        acceptedAt: Fixture.time,
        dependencyWake: {
          dependencyId: "dependency:stale-wake",
          generation: 0,
          waiterThreadId: f.waiter.id,
          messageId: wake.id,
        },
        events: [
          f.event({ type: "run.created", threadId: f.waiter.id, runId: run.id, payload: run }),
          f.event({ type: "message.updated", threadId: f.waiter.id, runId: run.id, payload: wake }),
        ],
        effects: [],
      })
      .pipe(Effect.exit);
    assert.isTrue(Exit.isFailure(rejected));
    assert.deepEqual(
      yield* f.sql`SELECT * FROM orchestration_v2_projection_messages WHERE message_id = ${wake.id}`,
      [],
    );
    assert.deepEqual(
      yield* f.sql`SELECT * FROM orchestration_v2_projection_runs WHERE run_id = ${run.id}`,
      [],
    );
    assert.deepEqual(
      yield* f.sql`SELECT * FROM orchestration_command_receipts WHERE command_id = 'losing-wake'`,
      [],
    );
    assert.equal((yield* f.store.get("dependency:stale-wake"))?.state, "released");
  }).pipe(Effect.provide(Fixture.layer)),
);

it.effect.each([
  "exact queued wake",
  "missing wake id",
  "missing message",
  "missing run",
  "foreign message",
  "foreign run",
  "live run",
] as const)("settlement duty planning requires an exact queued wake binding: %s", (binding) =>
  Effect.gen(function* () {
    const f = yield* Fixture.seed;
    const { dependency, run, wake } = yield* deliverQueuedWake(f, "planned-wake");
    const unrelated = f.run("unrelated-queued-run", f.waiter.id, "queued", 3);
    yield* f.sink.write({
      events: [
        f.event({
          type: "run.created",
          threadId: f.waiter.id,
          runId: unrelated.id,
          payload: unrelated,
        }),
      ],
    });
    switch (binding) {
      case "missing wake id":
        yield* f.sql`UPDATE jones_thread_result_dependencies SET wake_message_id = NULL WHERE dependency_id = ${dependency.dependencyId}`;
        break;
      case "missing message":
        yield* f.sql`DELETE FROM orchestration_v2_projection_messages WHERE message_id = ${wake.id}`;
        break;
      case "missing run":
        yield* f.sql`DELETE FROM orchestration_v2_projection_runs WHERE run_id = ${run.id}`;
        break;
      case "foreign message":
        yield* f.sql`UPDATE orchestration_v2_projection_messages SET thread_id = ${f.receiver.id} WHERE message_id = ${wake.id}`;
        break;
      case "foreign run":
        yield* f.sql`UPDATE orchestration_v2_projection_runs SET thread_id = ${f.receiver.id} WHERE run_id = ${run.id}`;
        break;
      case "live run":
        yield* f.sql`UPDATE orchestration_v2_projection_runs SET status = 'running' WHERE run_id = ${run.id}`;
        break;
    }
    const before =
      yield* f.sql`SELECT * FROM orchestration_v2_projection_runs WHERE run_id = ${run.id}`;
    assert.equal(
      (yield* f.store.dutiesAfterQueuedWakeCancellations(f.waiter.id, new Set())).outgoing,
      1,
    );
    assert.equal(
      (yield* f.store.dutiesAfterQueuedWakeCancellations(f.waiter.id, new Set([unrelated.id])))
        .outgoing,
      1,
    );
    assert.equal(
      (yield* f.store.dutiesAfterQueuedWakeCancellations(f.waiter.id, new Set([run.id]))).outgoing,
      binding === "exact queued wake" ? 0 : 1,
    );
    assert.equal((yield* f.store.duties(f.waiter.id)).outgoing, 1);
    assert.equal((yield* f.store.get(dependency.dependencyId))?.state, "delivered");
    assert.deepEqual(
      yield* f.sql`SELECT * FROM orchestration_v2_projection_runs WHERE run_id = ${run.id}`,
      before,
    );
  }).pipe(Effect.provide(Fixture.layer)),
);

it.effect("planned wake cancellations retain sending, awaiting and resolved duties", () =>
  Effect.gen(function* () {
    const f = yield* Fixture.seed;
    const { run } = yield* deliverQueuedWake(f, "cancelled-wake");
    yield* f.send("other-awaiting");
    const resolved = yield* f.send("other-resolved");
    yield* f.sql.withTransaction(
      f.store.answer(f.receiver.id, resolved.requestId, "fulfilled", "Not yet delivered", null),
    );
    yield* f.sql.withTransaction(
      f.store.prepare({
        dependencyId: "other-sending",
        operationId: "other-sending-operation",
        waiterThreadId: f.waiter.id,
        waiterRunId: f.waiterRun.id,
        targetEnvironmentId: "peer",
        targetThreadId: f.receiver.id,
        targetLabel: "Remote receiver",
        forwardedRequestKey: "planned-send-key",
        requestDigest: "planned-send-digest",
        waiterGeneration: 0,
        scope: {
          environmentId: EnvironmentId.make("local"),
          requestNamespace: "planned-cancellations",
          thread: {
            threadId: f.waiter.id,
            providerSessionId: "planned-session",
            providerInstanceId: f.waiter.providerInstanceId,
          },
        },
      }),
    );
    assert.deepEqual(
      yield* f.store.dutiesAfterQueuedWakeCancellations(f.waiter.id, new Set([run.id])),
      { outgoing: 3, owed: 0 },
    );
    assert.equal((yield* f.store.duties(f.waiter.id)).outgoing, 4);
  }).pipe(Effect.provide(Fixture.layer)),
);

it.effect("a dependency opened after queued-wake cancellation planning rolls back settlement", () =>
  Effect.gen(function* () {
    const f = yield* Fixture.seed;
    const changes = yield* Changes.DependencyChanges;
    const { dependency, run } = yield* deliverQueuedWake(f, "settle-planned-wake");
    const events = [
      f.event({
        type: "run.updated",
        threadId: f.waiter.id,
        runId: run.id,
        payload: { ...run, status: "cancelled", completedAt: Fixture.time },
      }),
      f.event({
        type: "thread.settled",
        threadId: f.waiter.id,
        payload: { ...f.waiter, settledAt: Fixture.time },
      }),
    ];
    assert.equal(
      (yield* f.store.dutiesAfterQueuedWakeCancellations(f.waiter.id, new Set([run.id]))).outgoing,
      0,
    );
    // Another thread accepts a dependency after the planner has staged the cancellation.
    yield* f.send("settle-race");
    const sequence = yield* f.store.sequence;
    const generation = yield* f.store.waiterGeneration(f.waiter.id);
    const publishedGeneration = changes.generation(f.waiter.id);
    const commandId = CommandId.make("stale-settle-plan");
    const rejected = yield* f.sink
      .commitCommand({
        commandId,
        threadId: f.waiter.id,
        commandType: "thread.settle",
        acceptedAt: Fixture.time,
        events,
        effects: [
          {
            id: "stale-settle-cleanup",
            commandId,
            threadId: f.waiter.id,
            request: { type: "terminal.cleanup" },
          },
        ],
      })
      .pipe(Effect.exit);
    assert.isTrue(Exit.isFailure(rejected));
    assert.equal((yield* f.projection.getThread(f.waiter.id)).settledAt, null);
    assert.equal((yield* f.store.get("dependency:settle-race"))?.state, "awaiting");
    assert.equal((yield* f.store.get(dependency.dependencyId))?.state, "delivered");
    assert.equal(
      (yield* f.projection.getThreadRecords(f.waiter.id, ["runs"])).runs.find(
        (candidate) => candidate.id === run.id,
      )?.status,
      "queued",
    );
    assert.equal(yield* f.store.waiterGeneration(f.waiter.id), generation);
    assert.equal(changes.generation(f.waiter.id), publishedGeneration);
    assert.equal(yield* f.store.sequence, sequence);
    assert.deepEqual(
      yield* f.sql`SELECT * FROM orchestration_command_receipts WHERE command_id = ${commandId}`,
      [],
    );
    assert.deepEqual(
      yield* f.sql`SELECT * FROM orchestration_v2_effect_outbox WHERE command_id = ${commandId}`,
      [],
    );
  }).pipe(Effect.provide(Fixture.layer)),
);

it.effect(
  "an observation cannot rebind a dependency to a different operation, message or receiver",
  () =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const input = yield* f.send("binding");
      const request = (yield* f.store.requests(f.receiver.id)).find(
        (request) => request.requestId === input.requestId,
      )!;
      for (const unrelated of [
        { ...request, operationId: "other-operation" },
        { ...request, messageId: MessageId.make("other-message") },
        { ...request, targetThreadId: f.waiter.id },
        { ...request, originThreadId: f.receiver.id },
      ]) {
        assert.isTrue(
          Exit.isFailure(
            yield* f.sql
              .withTransaction(
                f.store.observe("dependency:binding", 0, "receiver_working", unrelated),
              )
              .pipe(Effect.exit),
          ),
        );
      }
      assert.equal((yield* f.store.get("dependency:binding"))?.requestId, input.requestId);
      assert.equal((yield* f.store.get("dependency:binding"))?.state, "awaiting");
    }).pipe(Effect.provide(Fixture.layer)),
);

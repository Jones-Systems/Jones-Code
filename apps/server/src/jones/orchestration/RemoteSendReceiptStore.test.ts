import { assert, it } from "@effect/vitest";
import { ThreadId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Locks from "../../orchestration-v2/ThreadCommandExecutor.ts";
import * as Fixture from "../threadResultDependencies/testkit.ts";
import { makeJonesTablePresence } from "../persistence/JonesTablePresence.ts";
import * as Receipts from "./RemoteSendReceiptStore.ts";

// Requires root-composed Jones migration 156. All source cases are UNRUN here.
const layer = Fixture.layer.pipe(Layer.provideMerge(Locks.layer));
const captureFor = (threadId: ThreadId): Receipts.SendCommitContext => ({
  commandId: Receipts.commandIdFor("authenticated-peer-origin", "original-send"),
  originNamespace: "authenticated-peer-origin", clientRequestId: "original-send",
  requestDigest: "a".repeat(64), threadId, messageId: Receipts.messageIdFor("authenticated-peer-origin", "original-send"),
  expectedThreadCreatedAt: DateTime.formatIso(Fixture.time), mode: "queue",
});
it.effect("retains the original exact receipt across new store instances and duplicate transactions", () =>
  Effect.gen(function* () {
    const f = yield* Fixture.seed;
    const locks = yield* Locks.ThreadCommandExecutor;
    const makeStore = () => Receipts.makeRemoteSendReceiptStore(f.sql, makeJonesTablePresence(f.sql));
    const store = makeStore();
    const capture = captureFor(f.receiver.id);
    const sent = { threadId: capture.threadId, messageId: capture.messageId, runId: f.receiverRun.id,
      status: "queued" as const, delivery: "queued" as const };
    const write = (status: "queued" | "completed") => locks.withLock(capture.threadId,
      f.sql.withTransaction(store.record(capture, { ...sent, status })));
    const results = yield* Effect.all([write("queued"), write("completed")], { concurrency: 2 });
    assert.deepEqual(results[0], results[1]);
    assert.equal((yield* f.sql`SELECT command_id FROM jones_remote_send_receipts`).length, 1);
    const restartedStore = makeStore();
    assert.deepEqual(yield* restartedStore.lookup(capture), { outcome: "accepted", sent: results[0] });
    for (const query of [{ ...capture, originNamespace: "other-origin" },
      { ...capture, threadId: ThreadId.make("other-thread") }, { ...capture, requestDigest: "b".repeat(64) },
      { ...capture, expectedThreadCreatedAt: "different-birth" }])
      assert.deepEqual(yield* restartedStore.lookup(query), { outcome: "unknown", sent: null });
    assert.isTrue(Exit.isFailure(yield* f.sql.withTransaction(store.record({ ...capture,
      requestDigest: "b".repeat(64) }, sent)).pipe(Effect.exit)));
    assert.deepEqual(yield* store.lookup(capture), { outcome: "accepted", sent: results[0] });
  }).pipe(Effect.provide(layer)),
);

it.effect("rolls back receipt writes, rejects malformed results, and keeps absent or pruned outcomes unknown", () =>
  Effect.gen(function* () {
    const f = yield* Fixture.seed;
    const store = Receipts.makeRemoteSendReceiptStore(f.sql, makeJonesTablePresence(f.sql));
    const capture = captureFor(f.receiver.id);
    const sent = { threadId: capture.threadId, messageId: capture.messageId, runId: f.receiverRun.id,
      status: "queued" as const, delivery: "queued" as const };
    assert.deepEqual(yield* store.lookup(capture), { outcome: "unknown", sent: null });
    assert.isTrue(Exit.isFailure(yield* store.record(capture, sent).pipe(Effect.exit)));
    const rolledBack = yield* f.sql.withTransaction(store.record(capture, sent).pipe(
      Effect.andThen(Effect.fail("abort outer transaction")),
    )).pipe(Effect.exit);
    assert.isTrue(Exit.isFailure(rolledBack));
    assert.deepEqual(yield* store.lookup(capture), { outcome: "unknown", sent: null });
    assert.isTrue(Exit.isFailure(yield* f.sql.withTransaction(store.record(capture,
      { ...sent, secret: "must not persist" } as typeof sent)).pipe(Effect.exit)));
    assert.isTrue(Exit.isFailure(yield* f.sql.withTransaction(store.record({ ...capture,
      requestDigest: "not-sha256" }, sent)).pipe(Effect.exit)));
    yield* f.sql.withTransaction(store.record(capture, sent));
    assert.equal(yield* store.pruneBefore(DateTime.makeUnsafe("9999-01-01T00:00:00.000Z"), 1), 1);
    assert.deepEqual(yield* store.lookup(capture), { outcome: "unknown", sent: null });
  }).pipe(Effect.provide(layer)),
);

it.effect("records accepted remote SEND before outbox publication and rolls the entire acceptance back on failure", () =>
  Effect.gen(function* () {
    const f = yield* Fixture.seed;
    const store = Receipts.makeRemoteSendReceiptStore(f.sql, makeJonesTablePresence(f.sql));
    const capture = captureFor(f.receiver.id);
    const run = { ...f.run("accepted-remote-run", f.receiver.id, "queued", 2), startedAt: null };
    const request = { ...f.requestInput("remote-accepted"), commandId: capture.commandId, messageId: capture.messageId };
    const input = { remoteSend: capture, commandId: capture.commandId, threadId: capture.threadId,
      commandType: "message.dispatch", acceptedAt: Fixture.time, resultRequest: request,
      events: [f.event({ type: "run.created", threadId: f.receiver.id, runId: run.id, payload: run }),
        f.event({ type: "message.updated", threadId: f.receiver.id, runId: run.id, payload: f.message(capture.messageId, run) })],
      effects: [{ id: "effect:remote-receipt-title", commandId: capture.commandId, threadId: capture.threadId,
        request: { type: "thread-title.generate" as const, kind: { type: "regenerate" as const } } }] };
    const before = yield* f.sink.latestSequence();
    const failed = yield* f.sink.withProviderBatch(f.sink.commitCommand(input).pipe(Effect.andThen(
      Effect.gen(function* () {
        assert.equal((yield* store.lookup(capture)).outcome, "accepted");
        assert.equal((yield* f.sql`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE command_id = ${capture.commandId}`).length, 1);
        return yield* Effect.fail("crash before outer commit/publication");
      }),
    )).pipe(Effect.exit);
    assert.isTrue(Exit.isFailure(failed));
    assert.equal(yield* f.sink.latestSequence(), before);
    assert.deepEqual(yield* store.lookup(capture), { outcome: "unknown", sent: null });
    assert.isUndefined(yield* f.store.get(request.localDependency!.dependencyId));
    assert.equal((yield* f.sql`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE command_id = ${capture.commandId}`).length, 0);
    const committed = yield* f.sink.commitCommand(input);
    const replayed = yield* f.sink.commitCommand(input);
    assert.isTrue(committed.committed);
    assert.isFalse(replayed.committed);
    const accepted = yield* store.lookup(capture);
    assert.equal(accepted.outcome, "accepted");
    if (accepted.outcome === "accepted") {
      assert.equal(accepted.sent.runId, run.id);
      assert.equal(accepted.sent.delivery, "queued");
      assert.equal(accepted.sent.status, "queued");
    }
    yield* f.sink.write({ events: [f.event({ type: "run.updated", threadId: run.threadId, runId: run.id,
      payload: { ...run, status: "cancelled", completedAt: Fixture.time } })] });
    assert.deepEqual(yield* store.lookup(capture), accepted);
    const changed = yield* f.sink.commitCommand({ ...input, remoteSend: { ...capture,
      expectedThreadCreatedAt: "replacement-birth" } }).pipe(Effect.exit);
    assert.isTrue(Exit.isFailure(changed));
  }).pipe(Effect.provide(layer)),
);

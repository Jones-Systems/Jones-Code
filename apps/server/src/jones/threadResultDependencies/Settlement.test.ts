import { assert, it } from "@effect/vitest";
import { CommandId, ProviderSessionId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { selfSettlementTerminalDisposition } from "../../orchestration-v2/SelfSettlement.ts";
import * as Fixture from "./testkit.ts";

it.effect("deferred self-settlement cancels for outgoing and owed duties after provider completion", () => Effect.gen(function* () {
  const f = yield* Fixture.seed;
  const withIntent = (records: Parameters<typeof selfSettlementTerminalDisposition>[0], runId: typeof f.waiterRun.id) => ({
    ...records, thread: { ...records.thread, selfSettlement: {
      commandId: CommandId.make(`self-settle:${runId}`), runId, mcpCredentialId: "synthetic-credential",
      providerSessionId: ProviderSessionId.make("synthetic-session"), providerInstanceId: f.waiter.providerInstanceId,
    } },
  });
  const waiter = withIntent(yield* f.projection.getThreadRecords(f.waiter.id, ["runs", "messages", "runtimeRequests", "attempts"]), f.waiterRun.id);
  assert.equal(selfSettlementTerminalDisposition(waiter, f.waiterRun.id), "settle");
  yield* f.send("self-settlement");
  assert.equal(selfSettlementTerminalDisposition(waiter, f.waiterRun.id, yield* f.store.duties(f.waiter.id)), "cancel");
  yield* f.sink.write({ events: [f.event({ type: "run.updated", threadId: f.receiver.id, runId: f.receiverRun.id, payload: { ...f.receiverRun, status: "completed", completedAt: Fixture.time } })] });
  const receiver = withIntent(yield* f.projection.getThreadRecords(f.receiver.id, ["runs", "messages", "runtimeRequests", "attempts"]), f.receiverRun.id);
  assert.equal(selfSettlementTerminalDisposition(receiver, f.receiverRun.id), "settle");
  assert.equal(selfSettlementTerminalDisposition(receiver, f.receiverRun.id, yield* f.store.duties(f.receiver.id)), "cancel");
}).pipe(Effect.provide(Fixture.layer)));

for (const disposition of ["deleted", "settled"] as const) it.effect(`${disposition} records receiver disposal together with its upstream cause`, () => Effect.gen(function* () {
  const f = yield* Fixture.seed;
  yield* f.send(disposition);
  yield* f.sink.write({ events: [f.event({ type: "run.updated", threadId: f.receiver.id, runId: f.receiverRun.id, payload: { ...f.receiverRun, status: "completed", completedAt: Fixture.time } })] });
  yield* f.sink.commitCommand({ commandId: CommandId.make(`dispose:${disposition}`), commandType: disposition === "deleted" ? "thread.delete" : "thread.settle", threadId: f.receiver.id, acceptedAt: Fixture.time,
    events: [f.event({ type: disposition === "deleted" ? "thread.deleted" : "thread.settled", threadId: f.receiver.id, payload: { ...f.receiver, deletedAt: disposition === "deleted" ? Fixture.time : null, settledAt: disposition === "settled" ? Fixture.time : null } })], effects: [] });
  assert.equal((yield* f.store.get(`dependency:${disposition}`))?.outcome, "dropped");
  assert.equal((yield* f.store.requests(f.receiver.id))[0]?.state, "dropped");
}).pipe(Effect.provide(Fixture.layer)));

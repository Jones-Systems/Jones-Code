import { assert, it } from "@effect/vitest";
import { CommandId, NodeId, ProviderThreadId, RunAttemptId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { CodexFailoverError } from "../codexFailover/CodexFailover.ts";
import * as Fixture from "../threadResultDependencies/testkit.ts";
import * as Refusals from "./Store.ts";

it.effect(
  "reads only exact scoped original acceptance and never turns crossed, missing or pruned proof into eligibility",
  () =>
    Effect.gen(function* () {
      const f = yield* Fixture.seed;
      const refusals = yield* Refusals.StartRefusals;
      const seed = (key: string, ordinal: number) =>
        Effect.gen(function* () {
          const attemptId = RunAttemptId.make(`attempt:${key}`);
          const providerThreadId = ProviderThreadId.make(`provider-thread:${key}`);
          const run = {
            ...f.run(key, f.receiver.id, "starting", ordinal),
            providerThreadId,
            activeAttemptId: attemptId,
            startedAt: null,
          };
          const attempt = {
            id: attemptId,
            runId: run.id,
            attemptOrdinal: 1,
            rootNodeId: NodeId.make(`node:${key}`),
            providerInstanceId: f.receiver.providerInstanceId,
            providerThreadId,
            providerTurnId: null,
            reason: "initial" as const,
            status: "pending" as const,
            startedAt: null,
            completedAt: null,
          };
          const identity = { threadId: f.receiver.id, runId: run.id, attemptId };
          // Synthetic scoped acceptance participant, using the same transaction as the original command receipt.
          yield* f.sink.commitCommand({
            commandId: CommandId.make(`accept:${key}`),
            threadId: f.receiver.id,
            commandType: "message.dispatch",
            acceptedAt: Fixture.time,
            events: [
              f.event({
                type: "run.created",
                threadId: f.receiver.id,
                runId: run.id,
                payload: run,
              }),
              f.event({
                type: "run-attempt.created",
                threadId: f.receiver.id,
                runId: run.id,
                payload: attempt,
              }),
            ],
            effects: [],
            codexFailover: {
              accept: () =>
                Effect.gen(function* () {
                  yield* refusals.establish(
                    identity,
                    "dependency_wake",
                    yield* f.sink.latestSequence(),
                  );
                }).pipe(
                  Effect.mapError(
                    (cause) => new CodexFailoverError({ reason: "state_unavailable", cause }),
                  ),
                ),
            },
          });
          const settle = f.sink.writeIfRunCurrent({
            threadId: f.receiver.id,
            runId: run.id,
            activeAttemptId: attemptId,
            expectedStatus: "starting",
            startRefusalReason: "control_held",
            events: [
              f.event({
                type: "run.updated",
                threadId: f.receiver.id,
                runId: run.id,
                payload: { ...run, status: "failed", completedAt: Fixture.time },
              }),
              f.event({
                type: "run-attempt.updated",
                threadId: f.receiver.id,
                runId: run.id,
                payload: { ...attempt, status: "failed", completedAt: Fixture.time },
              }),
            ],
            effects: [],
          });
          return { identity, settle };
        });
      const crossed = yield* seed("prefix-crossed", 2);
      assert.equal((yield* refusals.readPrefix(crossed.identity)).state, "eligible");
      yield* refusals.cross(crossed.identity);
      yield* crossed.settle;
      assert.equal((yield* refusals.readPrefix(crossed.identity)).state, "crossed");
      const refused = yield* seed("prefix-refused", 3);
      yield* refused.settle;
      const proof = yield* refusals.readPrefix(refused.identity);
      assert.equal(proof.state, "refused");
      if (proof.state === "refused") assert.equal(proof.reason, "control_held");
      assert.deepEqual(
        yield* refusals.readPrefix({
          threadId: f.receiver.id,
          runId: f.receiverRun.id,
          attemptId: RunAttemptId.make("unrecorded-ordinary-attempt"),
        }),
        { state: "unknown" },
      );
      // Isolated fixture retention simulates loss of original acceptance; no new receipt is fabricated.
      yield* f.sql`DELETE FROM orchestration_command_receipts WHERE command_id = 'accept:prefix-refused'`;
      assert.deepEqual(yield* refusals.readPrefix(refused.identity), { state: "unknown" });
    }).pipe(Effect.provide(Fixture.layer)),
);

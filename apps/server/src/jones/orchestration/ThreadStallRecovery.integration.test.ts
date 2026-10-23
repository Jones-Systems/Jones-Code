import { assert, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  ThreadId,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Scope from "effect/Scope";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";
import * as Sqlite from "../../persistence/Sqlite.ts";
import * as EffectOutbox from "../../orchestration-v2/EffectOutbox.ts";
import * as EffectWorker from "../../orchestration-v2/EffectWorker.ts";
import * as ProviderTurnControl from "../../orchestration-v2/ProviderTurnControlService.ts";
import * as ControlAdmission from "./ControlAdmission.ts";
import { boundedSteer, CODEX_STEER_TIMEOUT_MS } from "./LateSteer.ts";

const database = Sqlite.layerMemory;
const testLayer = Layer.mergeAll(
  database,
  EffectOutbox.layer.pipe(Layer.provide(database)),
  ControlAdmission.layer,
);

it.effect(
  "four real workers release unknown steers at the budget and another instance proceeds",
  () =>
    Effect.gen(function* () {
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const admission = yield* ControlAdmission.ControlAdmission;
      const producerScope = yield* Scope.Scope;
      const sent = yield* Queue.unbounded<string>();
      const failed = yield* Queue.unbounded<string>();
      const healthyFinished = yield* Deferred.make<void>();
      const late = yield* Effect.forEach([0, 1, 2, 3], () => Deferred.make<{ turnId: string }>());
      const dispatches = new Map<string, number>();
      const effects = Array.from({ length: 5 }, (_, index) => ({
        id: `effect:stall:${index}`,
        commandId: CommandId.make(`command:stall:${index}`),
        threadId: ThreadId.make(`thread:stall:${index}`),
        request: {
          providerSessionId: ProviderSessionId.make(`session:${index}`),
          providerThreadId: ProviderThreadId.make(`provider-thread:${index}`),
          providerTurnId: ProviderTurnId.make(`provider-turn:${index}`),
          messageId: MessageId.make(`message:${index}`),
          ...(index === 2
            ? {
                type: "provider-turn.steer-batch" as const,
                messageIds: [
                  MessageId.make(`message:${index}`),
                  MessageId.make("message:batch-second"),
                ],
              }
            : { type: "provider-turn.steer" as const }),
        },
      }));
      const executor = Layer.succeed(EffectWorker.OrchestrationEffectExecutorV2, {
        execute: (effect) =>
          Effect.gen(function* () {
            const index = effects.findIndex((row) => row.id === effect.id);
            const binding = {
              instanceId: index === 4 ? "healthy" : "held",
              providerSessionId: `session:${index}`,
              providerThreadId: `provider-thread:${index}`,
              providerTurnId: `provider-turn:${index}`,
              runtimeGeneration: "generation:fixture",
              nativeThreadId: `native-thread:${index}`,
              nativeTurnId: `native-turn:${index}`,
            };
            yield* boundedSteer({
              instanceId: binding.instanceId,
              prepare: Effect.succeed({
                binding,
                producerScope,
                send: Effect.sync(() => {
                  dispatches.set(effect.id, (dispatches.get(effect.id) ?? 0) + 1);
                }).pipe(
                  Effect.andThen(Queue.offer(sent, effect.id)),
                  Effect.andThen(
                    index === 4
                      ? Effect.succeed({ turnId: binding.nativeTurnId })
                      : Deferred.await(late[index]!),
                  ),
                ),
              }),
            });
            if (index === 4) yield* Deferred.succeed(healthyFinished, undefined);
          }).pipe(
            Effect.provideService(ControlAdmission.ControlAdmission, admission),
            Effect.mapError(
              (cause) =>
                new EffectWorker.OrchestrationEffectExecutionError({
                  effectId: effect.id,
                  effectType: effect.request.type,
                  cause: new ProviderTurnControl.ProviderTurnControlError({
                    threadId: effect.threadId,
                    operation: "steer",
                    providerTurnId: ProviderTurnId.make("wrapper-target"),
                    turnCompleted: true,
                    cause,
                  }),
                }),
            ),
          ),
        recordNonRetryableFailure: (effect) => Queue.offer(failed, effect.id).pipe(Effect.asVoid),
      });
      const workerLayer = EffectWorker.layer.pipe(Layer.provide(executor));
      yield* outbox.enqueue(effects);
      const daemon = yield* EffectWorker.runDaemon.pipe(
        Effect.provide(workerLayer),
        Effect.forkScoped,
      );
      const firstSends = yield* Effect.forEach([0, 1, 2, 3], () => Queue.take(sent));
      assert.sameMembers(
        firstSends,
        effects.slice(0, 4).map((row) => row.id),
      );
      assert.isTrue(Option.isNone(yield* Deferred.poll(healthyFinished)));
      yield* TestClock.adjust(CODEX_STEER_TIMEOUT_MS);
      yield* Deferred.await(healthyFinished);
      const failures = yield* Effect.forEach([0, 1, 2, 3], () => Queue.take(failed));
      assert.sameMembers(
        failures,
        effects.slice(0, 4).map((row) => row.id),
      );
      for (const effect of effects.slice(0, 4)) {
        const row = yield* outbox.get(effect.id);
        assert.isTrue(Option.isSome(row));
        if (Option.isSome(row)) {
          assert.equal(row.value.status, "failed");
          assert.equal(row.value.attemptCount, 1);
          assert.isNull(row.value.leaseOwner);
        }
        assert.equal(dispatches.get(effect.id), 1);
      }
      assert.equal(dispatches.get(effects[4]!.id), 1);
      assert.lengthOf(yield* admission.snapshot("held"), 4);
      yield* Deferred.succeed(late[0]!, { turnId: "native-turn:0" });
      yield* TestClock.adjust(0);
      assert.lengthOf(yield* admission.snapshot("held"), 3);
      yield* admission.assertOpen("held").pipe(Effect.flip);
      yield* TestClock.adjust(CODEX_STEER_TIMEOUT_MS * 2);
      assert.deepEqual(Array.from(dispatches.values()), [1, 1, 1, 1, 1]);
      yield* Fiber.interrupt(daemon);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
);

it.effect("lease loss prevents a stale worker from emitting a delivery explanation", () =>
  Effect.gen(function* () {
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    const producerScope = yield* Scope.Scope;
    const admission = yield* ControlAdmission.ControlAdmission;
    const sent = yield* Deferred.make<void>();
    const sql = yield* SqlClient.SqlClient;
    let explanations = 0;
    const effect = {
      id: "effect:lease-loss",
      commandId: CommandId.make("command:lease-loss"),
      threadId: ThreadId.make("thread:lease-loss"),
      request: {
        type: "provider-turn.steer" as const,
        providerSessionId: ProviderSessionId.make("session:loss"),
        providerThreadId: ProviderThreadId.make("provider-thread:loss"),
        providerTurnId: ProviderTurnId.make("provider-turn:loss"),
        messageId: MessageId.make("message:loss"),
      },
    };
    yield* outbox.enqueue([effect]);
    const executor = Layer.succeed(EffectWorker.OrchestrationEffectExecutorV2, {
      execute: () =>
        boundedSteer({
          instanceId: "lost",
          prepare: Effect.succeed({
            binding: {
              instanceId: "lost",
              providerSessionId: "session:loss",
              providerThreadId: "provider-thread:loss",
              providerTurnId: "provider-turn:loss",
              runtimeGeneration: "generation:loss",
              nativeThreadId: "native-thread:loss",
              nativeTurnId: "native-turn:loss",
            },
            producerScope,
            send: Deferred.succeed(sent, undefined).pipe(Effect.andThen(Effect.never)),
          }),
        }).pipe(
          Effect.provideService(ControlAdmission.ControlAdmission, admission),
          Effect.mapError(
            (cause) =>
              new EffectWorker.OrchestrationEffectExecutionError({
                effectId: effect.id,
                effectType: effect.request.type,
                cause,
              }),
          ),
        ),
      recordNonRetryableFailure: () =>
        Effect.sync(() => {
          explanations++;
        }),
    });
    const worker = yield* EffectWorker.OrchestrationEffectWorkerV2.pipe(
      Effect.flatMap((worker) => worker.runOnce),
      Effect.provide(
        EffectWorker.layerWithOptions({ workerId: "original", leaseDurationMs: 1 }).pipe(
          Layer.provide(executor),
        ),
      ),
      Effect.exit,
      Effect.forkScoped,
    );
    yield* Deferred.await(sent);
    yield* TestClock.adjust(1);
    yield* sql`UPDATE orchestration_v2_effect_outbox SET lease_owner = 'replacement' WHERE effect_id = ${effect.id} AND lease_owner = 'original'`;
    yield* TestClock.adjust(CODEX_STEER_TIMEOUT_MS);
    assert.equal((yield* Fiber.join(worker))._tag, "Failure");
    assert.equal(explanations, 0);
    const row = yield* outbox.get(effect.id);
    if (Option.isSome(row)) assert.equal(row.value.leaseOwner, "replacement");
  }).pipe(Effect.scoped, Effect.provide(testLayer)),
);

import { expect, it } from "@effect/vitest";
import { CommandId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Tracer from "effect/Tracer";
import { TestClock } from "effect/testing";

import * as EffectOutbox from "../../orchestration-v2/EffectOutbox.ts";
import * as EffectWorker from "../../orchestration-v2/EffectWorker.ts";

it.effect("attributes availability-to-claim wait before the durable cancellation read", () =>
  Effect.scoped(
    Effect.gen(function* () {
      yield* TestClock.setTime(0);
      const ended: Tracer.NativeSpan[] = [];
      const tracer = Tracer.make({
        span: (options) => {
          const span = new Tracer.NativeSpan(options);
          const end = span.end.bind(span);
          span.end = (time, exit) => {
            end(time, exit);
            ended.push(span);
          };
          return span;
        },
      });
      const effect: EffectOutbox.OrchestrationEffectV2 = {
        id: "effect:claim-timing",
        commandId: CommandId.make("command:claim-timing"),
        threadId: ThreadId.make("thread:claim-timing"),
        request: { type: "terminal.cleanup" },
        status: "running",
        attemptCount: 1,
        availableAt: "1970-01-01T00:00:00.005Z",
        leaseOwner: "claim-timing",
        leaseExpiresAt: "1970-01-01T00:01:40.000Z",
        createdAt: "1970-01-01T00:00:00.000Z",
        updatedAt: "1970-01-01T00:00:00.020Z",
        completedAt: null,
        lastError: null,
      };
      let executions = 0;
      let completions = 0;
      const outbox = Layer.mock(EffectOutbox.EffectOutboxV2)({
        claimNext: () => Effect.sleep(20).pipe(Effect.as(Option.some(effect))),
        get: () => Effect.sleep(50).pipe(Effect.as(Option.some(effect))),
        awaitCancellation: () => Effect.never,
        clearCancellation: () => Effect.void,
        succeed: () =>
          Effect.sync(() => {
            completions++;
            return true;
          }),
      });
      const executor = Layer.succeed(
        EffectWorker.OrchestrationEffectExecutorV2,
        EffectWorker.OrchestrationEffectExecutorV2.of({
          execute: () =>
            Effect.sync(() => {
              executions++;
            }),
        }),
      );
      const layer = EffectWorker.layerWithOptions({ workerId: "claim-timing" }).pipe(
        Layer.provide(Layer.merge(outbox, executor)),
      );
      const run = yield* EffectWorker.OrchestrationEffectWorkerV2.pipe(
        Effect.flatMap((worker) => worker.runOnce),
        Effect.provide(layer),
        Effect.withTracer(tracer),
        Effect.forkScoped,
      );
      yield* TestClock.adjust(70);
      expect(yield* Fiber.join(run)).toBe(true);
      expect(executions).toBe(1);
      expect(completions).toBe(1);
      const operation = ended.find((span) => span.name === "NativeOperation.withOperation");
      expect(operation).toBeDefined();
      expect(Object.fromEntries(operation!.attributes)).toMatchObject({
        "orchestration.effect.available_at_ms": 5,
        "orchestration.effect.claimed_at_ms": 20,
        "orchestration.effect.queue_wait_ms": 15,
      });
      expect(ended.filter((span) => span.name === "NativeOperation.withOperation")).toHaveLength(1);
    }),
  ),
);

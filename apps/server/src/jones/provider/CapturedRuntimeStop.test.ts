import { assert, it } from "@effect/vitest";
import { CommandId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import * as Outbox from "../../orchestration-v2/EffectOutbox.ts";
import * as EffectWorker from "../../orchestration-v2/EffectWorker.ts";
import * as Sqlite from "../../persistence/Sqlite.ts";
import * as RuntimeStop from "../runtime/RuntimeStop.ts";
import {
  CurrentEffectClaim,
  type EffectClaimLost,
  withActiveEffectClaim,
} from "../workspaceLifecycle/ActiveEffectClaim.ts";
import {
  currentRuntimeUseClaim,
  validateRuntimeUseClaim,
  withRuntimeUseClaim,
} from "../cleanup/RuntimeUseClaim.ts";
import * as CapturedRuntimeStop from "./CapturedRuntimeStop.ts";

it.effect(
  "retains the original Stop owner across build and inherits each actual invocation claim",
  () =>
    Effect.gen(function* () {
      const outbox = yield* Outbox.EffectOutboxV2;
      const sql = yield* SqlClient.SqlClient;
      const seen: string[] = [];
      let retainedCheck: Effect.Effect<void, EffectClaimLost> = Effect.die("Stop was not invoked");
      const stopLayer = Layer.mock(RuntimeStop.CurrentRuntimeStop)({
        execute: () =>
          Effect.gen(function* () {
            const current = yield* Effect.serviceOption(CurrentEffectClaim);
            if (Option.isNone(current)) return yield* Effect.die("Original worker claim missing");
            const claim = current.value;
            const use = yield* currentRuntimeUseClaim;
            assert.isTrue(Option.isSome(use));
            if (Option.isNone(use))
              return yield* Effect.die("Invocation runtime-use claim missing");
            yield* validateRuntimeUseClaim(use.value);
            yield* claim.use((actual) =>
              Effect.sync(() => {
                assert.equal(actual.id, use.value.effectId);
                seen.push(actual.id);
              }),
            );
            retainedCheck = claim.check;
          }).pipe(Effect.provideService(SqlClient.SqlClient, sql), Effect.orDie),
      });
      // This external executor port repeats the production detach branch's lookup.
      // The wrapper is built outside both real worker-claim scopes.
      const executorLayer = Layer.succeed(EffectWorker.OrchestrationEffectExecutorV2, {
        execute: (effect) =>
          Effect.gen(function* () {
            const stop = yield* Effect.serviceOption(RuntimeStop.CurrentRuntimeStop);
            if (Option.isNone(stop)) return yield* Effect.die("Original Stop owner missing");
            yield* stop.value.execute(effect.commandId);
          }).pipe(Effect.orDie),
      });
      const captured = yield* Layer.build(
        CapturedRuntimeStop.layer.pipe(Layer.provide(Layer.merge(executorLayer, stopLayer))),
      );
      const executor = yield* Effect.service(EffectWorker.OrchestrationEffectExecutorV2).pipe(
        Effect.provide(captured),
      );
      for (const id of ["first", "second"]) {
        yield* outbox.enqueue([
          {
            id,
            commandId: CommandId.make(`stop:${id}`),
            threadId: ThreadId.make("stop-thread"),
            request: { type: "terminal.cleanup" },
          },
        ]);
        const next = yield* outbox.claimNext({ workerId: "captured-stop-worker" });
        if (Option.isNone(next)) return yield* Effect.die("No actual outbox claim");
        const claim = next.value;
        yield* withActiveEffectClaim(outbox, claim, 1_000, false, () =>
          withRuntimeUseClaim(claim, "captured-stop-worker", executor.execute(claim)),
        );
        assert.equal((yield* Effect.exit(retainedCheck))._tag, "Failure");
        assert.isTrue(
          yield* outbox.succeed({ effectId: claim.id, workerId: "captured-stop-worker", claim }),
        );
      }
      assert.deepEqual(seen, ["first", "second"]);
    }).pipe(
      Effect.scoped,
      Effect.provide(Outbox.layer.pipe(Layer.provideMerge(Sqlite.layerMemory))),
    ),
);

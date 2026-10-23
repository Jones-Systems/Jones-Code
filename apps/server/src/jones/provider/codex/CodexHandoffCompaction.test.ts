import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import { makeCodexHandoffCompaction } from "./CodexHandoffCompaction.ts";

const binding = {
  runtimeGeneration: "generation",
  nativeThreadId: "native",
  runId: "run",
  attemptId: "attempt",
};
const usage = { usedTokens: 1_000, maxTokens: 200_000 };

it.effect("requires matched compaction, successful terminal, acknowledgement and fresh usage", () =>
  Effect.gen(function* () {
    const state = makeCodexHandoffCompaction();
    const request = yield* state.begin(binding);
    const waiting = yield* request.wait.pipe(Effect.forkChild);
    yield* request.acknowledge;
    yield* state.started(binding, "turn");
    yield* state.compacted(binding, "turn");
    yield* state.usage({ ...binding, runtimeGeneration: "retired" }, "turn", usage);
    yield* state.usage(binding, "other-turn", usage);
    assert.isUndefined(waiting.pollUnsafe());
    yield* state.usage(binding, "turn", usage);
    assert.isUndefined(waiting.pollUnsafe());
    yield* state.completed(binding, "turn", true);
    assert.deepEqual(yield* Fiber.join(waiting), usage);
    yield* request.release;
    assert.isFalse(state.blocked);
    assert.isTrue((yield* Effect.exit(state.begin(binding)))._tag === "Failure");
  }),
);

it.effect("accepts telemetry before item completion and after terminal", () =>
  Effect.gen(function* () {
    for (const before of [true, false]) {
      const state = makeCodexHandoffCompaction();
      const request = yield* state.begin(binding);
      yield* state.started(binding, "turn");
      if (before) yield* state.usage(binding, "turn", usage);
      yield* state.compacted(binding, "turn");
      yield* state.completed(binding, "turn", true);
      yield* request.acknowledge;
      if (!before) yield* state.usage(binding, "turn", usage);
      assert.deepEqual(yield* request.wait, usage);
    }
  }),
);

it.effect(
  "cancellation cannot resume input and only unambiguous terminal evidence releases the fence",
  () =>
    Effect.gen(function* () {
      for (const failure of ["stop", "failed", "ambiguous"] as const) {
        const state = makeCodexHandoffCompaction();
        const request = yield* state.begin(binding);
        yield* state.started(binding, "turn");
        if (failure === "stop") yield* state.cancel("cancelled");
        if (failure === "failed") yield* state.completed(binding, "turn", false);
        if (failure === "ambiguous") yield* state.started(binding, "other");
        yield* state.compacted(binding, "turn");
        yield* state.usage(binding, "turn", usage);
        yield* state.completed(binding, "turn", true);
        yield* request.acknowledge;
        yield* request.release;
        assert.isTrue((yield* Effect.exit(request.wait))._tag === "Failure");
        assert.equal(state.blocked, failure === "ambiguous");
      }
    }),
);

it.effect("late acknowledgement and terminal reconcile cancellation without reviving it", () =>
  Effect.gen(function* () {
    const state = makeCodexHandoffCompaction();
    const request = yield* state.begin(binding);
    yield* request.cancel;
    assert.isTrue(state.blocked);
    yield* state.started(binding, "late-turn");
    yield* state.completed(binding, "late-turn", true);
    assert.isTrue(state.blocked);
    yield* request.acknowledge;
    assert.isFalse(state.blocked);
    assert.equal((yield* Effect.exit(request.wait))._tag, "Failure");
    const next = yield* state.begin({ ...binding, attemptId: "next" });
    yield* request.cancel;
    yield* next.assertActive;
  }),
);

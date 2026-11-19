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
const compactionItem = { item: { id: "compact-item", type: "contextCompaction" } };
const observeStart = (state: ReturnType<typeof makeCodexHandoffCompaction>, nativeTurnId: string) =>
  state.observeActivity(binding, nativeTurnId, "turn/started", { turn: { id: nativeTurnId } });
const observeCompaction = (
  state: ReturnType<typeof makeCodexHandoffCompaction>,
  nativeTurnId: string,
) => state.observeActivity(binding, nativeTurnId, "item/started", compactionItem);

it.effect("attributes only the queued generation, thread, turn and compaction item", () =>
  Effect.gen(function* () {
    const state = makeCodexHandoffCompaction();
    const request = yield* state.begin(binding);
    const start = { turn: { id: "maintenance" } };
    assert.isFalse(state.observeActivity(binding, "maintenance", "turn/started", start));
    request.markDispatched();
    assert.isFalse(
      state.observeActivity(
        { ...binding, runtimeGeneration: "retired" },
        "maintenance",
        "turn/started",
        start,
      ),
    );
    assert.isFalse(
      state.observeActivity(
        { ...binding, nativeThreadId: "sibling" },
        "maintenance",
        "turn/started",
        start,
      ),
    );
    assert.isTrue(state.observeActivity(binding, "maintenance", "turn/started", start));
    assert.isUndefined(state.nativeTurnId(binding));
    const item = { item: { id: "compaction", type: "contextCompaction" } };
    assert.isTrue(state.observeActivity(binding, "maintenance", "item/started", item));
    assert.equal(state.nativeTurnId(binding), "maintenance");
    assert.isFalse(
      state.observeActivity(binding, "opposing", "turn/started", {
        turn: { id: "opposing" },
      }),
    );
    assert.isFalse(state.ownsTurn(binding, "opposing"));
    assert.isTrue(state.ownsTurn(binding, "maintenance"));
    yield* state.compacted(binding, "maintenance");
    yield* state.usage(binding, "maintenance", usage);
    yield* state.completed(binding, "maintenance", true);
    yield* request.acknowledge;
    const result = yield* request.wait.pipe(Effect.flip);
    assert.equal(result.reason, "ambiguous");
    assert.isTrue(state.blocked);
  }),
);

it.effect("requires matched compaction, successful terminal, acknowledgement and fresh usage", () =>
  Effect.gen(function* () {
    const state = makeCodexHandoffCompaction();
    const request = yield* state.begin(binding);
    const waiting = yield* request.wait.pipe(Effect.forkChild);
    request.markDispatched();
    yield* request.acknowledge;
    assert.isTrue(observeCompaction(state, "turn"));
    yield* state.compacted(binding, "turn");
    yield* state.usage({ ...binding, runtimeGeneration: "retired" }, "turn", usage);
    yield* state.usage(binding, "other-turn", usage);
    assert.isUndefined(waiting.pollUnsafe());
    yield* state.usage(binding, "turn", usage);
    assert.isUndefined(waiting.pollUnsafe());
    yield* state.completed(binding, "turn", true);
    assert.deepEqual(yield* Fiber.join(waiting), usage);
    assert.isFalse(yield* request.release(() => false));
    assert.isTrue(yield* request.release(() => true));
    assert.isFalse(state.blocked);
    assert.isTrue(state.observeActivity(binding, "turn", "item/completed", compactionItem));
    assert.isFalse(
      state.observeActivity(binding, "turn", "item/completed", {
        item: { id: "other-item", type: "contextCompaction" },
      }),
    );
    assert.isFalse(state.observeActivity(binding, "turn", "item/agentMessage/delta", {}));
    assert.isTrue((yield* Effect.exit(state.begin(binding)))._tag === "Failure");
  }),
);

it.effect("accepts telemetry before item completion and after terminal", () =>
  Effect.gen(function* () {
    for (const before of [true, false]) {
      const state = makeCodexHandoffCompaction();
      const request = yield* state.begin(binding);
      request.markDispatched();
      assert.isTrue(observeCompaction(state, "turn"));
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
        request.markDispatched();
        assert.isTrue(observeCompaction(state, "turn"));
        if (failure === "stop") yield* state.cancel("cancelled");
        if (failure === "failed") yield* state.completed(binding, "turn", false);
        if (failure === "ambiguous") assert.isFalse(observeStart(state, "other"));
        yield* state.compacted(binding, "turn");
        yield* state.usage(binding, "turn", usage);
        yield* state.completed(binding, "turn", true);
        yield* request.acknowledge;
        assert.isFalse(yield* request.release(() => true));
        assert.isTrue((yield* Effect.exit(request.wait))._tag === "Failure");
        assert.equal(state.blocked, failure === "ambiguous");
      }
    }),
);

it.effect("late acknowledgement and terminal reconcile cancellation without reviving it", () =>
  Effect.gen(function* () {
    const state = makeCodexHandoffCompaction();
    const request = yield* state.begin(binding);
    request.markDispatched();
    yield* request.cancel;
    assert.isTrue(state.blocked);
    assert.isTrue(observeCompaction(state, "late-turn"));
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

it.effect("keeps a cancelled provisional turn held without its compaction item", () =>
  Effect.gen(function* () {
    const state = makeCodexHandoffCompaction();
    const request = yield* state.begin(binding);
    request.markDispatched();
    assert.isTrue(observeStart(state, "provisional"));
    yield* request.cancel;
    yield* request.acknowledge;
    yield* state.completed(binding, "provisional", true);
    assert.isTrue(state.blocked);
    assert.isFalse(yield* request.release(() => true));
    assert.isUndefined(state.nativeTurnId(binding));
  }),
);

it.effect("restoration has an independent fence and late acknowledgement never revives input", () =>
  Effect.gen(function* () {
    const state = makeCodexHandoffCompaction();
    const request = yield* state.begin(binding);
    request.markDispatched();
    yield* request.acknowledge;
    assert.isTrue(observeCompaction(state, "turn"));
    yield* state.compacted(binding, "turn");
    yield* state.usage(binding, "turn", usage);
    yield* state.completed(binding, "turn", true);
    yield* request.wait;
    yield* request.beginRestoration;
    yield* request.cancel;
    assert.isFalse(yield* request.release(() => true));
    yield* state.completed(binding, "turn", true);
    assert.isTrue(state.blocked);
    assert.equal(
      (yield* Effect.exit(state.begin({ ...binding, attemptId: "blocked" })))._tag,
      "Failure",
    );
    yield* request.acknowledgeRestoration;
    assert.isFalse(state.blocked);
    assert.equal((yield* Effect.exit(request.assertActive))._tag, "Failure");
    const next = yield* state.begin({ ...binding, attemptId: "next" });
    yield* request.acknowledgeRestoration;
    assert.isTrue(state.blocked);
    yield* next.beginRestoration;
    yield* next.cancel;
    yield* state.retire(binding);
    assert.isFalse(state.blocked);
  }),
);

it.effect("releases only an unsent reservation while retaining its consumed attempt", () =>
  Effect.gen(function* () {
    const state = makeCodexHandoffCompaction();
    const request = yield* state.begin(binding);
    yield* request.cancelIfUndispatched;
    assert.isFalse(state.blocked);
    assert.equal((yield* Effect.exit(request.wait))._tag, "Failure");
    assert.isFalse(request.canDispatch());
    assert.equal((yield* Effect.exit(state.begin(binding)))._tag, "Failure");
    const next = yield* state.begin({ ...binding, attemptId: "fresh" });
    yield* request.cancelIfUndispatched;
    assert.isTrue(state.blocked);
    yield* next.assertActive;
    assert.isTrue(next.canDispatch());
    next.markDispatched();
  }),
);

it.effect.each(["dispatch", "acknowledgement", "native-start", "restoration"] as const)(
  "does not clear %s evidence through the unsent cancellation capability",
  (evidence) =>
    Effect.gen(function* () {
      const state = makeCodexHandoffCompaction();
      const request = yield* state.begin(binding);
      if (evidence === "dispatch") request.markDispatched();
      if (evidence === "acknowledgement") yield* request.acknowledge;
      if (evidence === "native-start") {
        request.markDispatched();
        assert.isTrue(observeStart(state, "native-turn"));
      }
      if (evidence === "restoration") yield* request.beginRestoration;
      yield* request.cancelIfUndispatched;
      assert.isTrue(state.blocked);
      assert.equal((yield* Effect.exit(request.wait))._tag, "Failure");
      assert.equal(
        (yield* Effect.exit(state.begin({ ...binding, attemptId: "fresh" })))._tag,
        "Failure",
      );
      if (evidence === "dispatch") {
        assert.isTrue(observeCompaction(state, "native-turn"));
        yield* state.completed(binding, "native-turn", true);
        assert.isTrue(state.blocked);
        yield* request.acknowledge;
        assert.isFalse(state.blocked);
        assert.equal((yield* Effect.exit(state.begin(binding)))._tag, "Failure");
      }
    }),
);

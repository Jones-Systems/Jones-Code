import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import {
  ProviderControlOutcomeUnknown,
  confirmProviderControlTerminalIngested,
  findProviderControlOutcomeUnknown,
  makeProviderControlTerminalDrain,
} from "./providerControlOutcome.ts";

it("finds the typed outcome through adapter/service causes without classifying strings", () => {
  const unknown = new ProviderControlOutcomeUnknown({
    driver: "cursor",
    providerSessionId: "synthetic-session",
    nativeThreadId: "synthetic-native-thread",
    operation: "interrupt",
  });
  assert.strictEqual(
    findProviderControlOutcomeUnknown(Cause.fail({ cause: { cause: unknown } })),
    unknown,
  );
  assert.strictEqual(
    findProviderControlOutcomeUnknown(Cause.combine(
      Cause.fail("unrelated"),
      Cause.die({ cause: unknown }),
    )),
    unknown,
  );
  assert.isUndefined(findProviderControlOutcomeUnknown(new Error(unknown.message)));
  const cycle: { cause?: unknown } = {};
  cycle.cause = cycle;
  assert.isUndefined(findProviderControlOutcomeUnknown(cycle));
});

it.effect("joins the exact raw terminal SQL receipt and the following guarded root-terminal handling", () =>
  Effect.gen(function* () {
    const drain = makeProviderControlTerminalDrain();
    const rawTerminal = { type: "provider_turn.updated" as const };
    const rootTerminal = { type: "turn.terminal" as const };
    const childTerminal = { type: "turn_item.updated" as const };
    drain.capture(rawTerminal);
    drain.capture(rootTerminal);
    drain.capture(childTerminal);
    const completed = yield* Deferred.make<void>();
    const waiting = yield* drain.awaitIngested.pipe(
      Effect.andThen(Deferred.succeed(completed, undefined)),
      Effect.forkChild,
    );
    confirmProviderControlTerminalIngested({ ...rawTerminal }, 1);
    confirmProviderControlTerminalIngested(rootTerminal, 0);
    confirmProviderControlTerminalIngested(rawTerminal, 0);
    yield* Effect.yieldNow;
    assert.isFalse(yield* Deferred.isDone(completed), "a rejected raw write is not persistence");
    confirmProviderControlTerminalIngested(rawTerminal, 1);
    yield* Effect.yieldNow;
    assert.isFalse(yield* Deferred.isDone(completed), "late child cleanup must also reach SQL");
    confirmProviderControlTerminalIngested(childTerminal, 1);
    yield* Fiber.join(waiting);
    yield* drain.awaitIngested;
    assert.isTrue(yield* Deferred.isDone(completed));
  }),
);

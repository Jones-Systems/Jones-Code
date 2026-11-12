import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import { retainHistoricalTerminalOnError } from "./HistoricalTerminalHistory.ts";
import { historyScenario, modes } from "./HistoricalTerminalHistory.testkit.ts";

it.effect.each([
  ...modes.map((mode) => ({ mode, lockFailures: 0 })),
  ...(
    [
      "current",
      "superseded-rebound-session",
      "wrong-generation",
      "ordinary-current-guard-race",
    ] as const
  ).flatMap((mode) => [2, 3].map((lockFailures) => ({ mode, lockFailures }))),
])("restart terminal history: $mode, locks: $lockFailures", historyScenario);

it.effect.each([false, true])(
  "preserves the original error when exact fallback is unavailable: %s",
  (fails) =>
    Effect.gen(function* () {
      const original = new Error("ordinary failure");
      const fallback = fails
        ? Effect.fail(new Error("historical failure"))
        : Effect.succeed({ handled: true, storedEvents: [] });
      const result = yield* retainHistoricalTerminalOnError(Effect.fail(original), fallback).pipe(
        Effect.exit,
      );
      assert.isTrue(Exit.isFailure(result));
      if (Exit.isFailure(result)) assert.strictEqual(Cause.squash(result.cause), original);
    }),
);

it.effect("propagates pure cancellation without offering historical recovery", () =>
  Effect.gen(function* () {
    let calls = 0;
    const result = yield* retainHistoricalTerminalOnError(
      Effect.interrupt,
      Effect.sync(() => {
        calls++;
        return { handled: true, persisted: true, storedEvents: [] };
      }),
    ).pipe(Effect.exit);
    assert.equal(calls, 0);
    assert.isTrue(Exit.isFailure(result));
    if (Exit.isFailure(result)) assert.isTrue(Cause.hasInterruptsOnly(result.cause));
  }),
);

it.effect("propagates cancellation of historical readback instead of resolving the original error", () =>
  Effect.gen(function* () {
    const result = yield* retainHistoricalTerminalOnError(
      Effect.fail(new Error("ordinary")),
      Effect.interrupt,
    ).pipe(Effect.exit);
    assert.isTrue(Exit.isFailure(result));
    if (Exit.isFailure(result)) assert.isTrue(Cause.hasInterruptsOnly(result.cause));
  }),
);

it.effect("ordinary history selection cannot resolve the original ingestion failure", () =>
  Effect.gen(function* () {
    const original = new Error("ordinary");
    const result = yield* retainHistoricalTerminalOnError(
      Effect.fail(original),
      Effect.succeed({ handled: false, storedEvents: [] }),
    ).pipe(Effect.exit);
    assert.isTrue(Exit.isFailure(result));
    if (Exit.isFailure(result)) assert.strictEqual(Cause.squash(result.cause), original);
  }),
);

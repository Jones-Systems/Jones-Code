import { assert, it } from "@effect/vitest";
import { ProviderSessionId } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Scope from "effect/Scope";
import { TestClock } from "effect/testing";
import { makeRetirementLedger, WriterRetirementError } from "./RetirementLedger.ts";
import {
  awaitRetirementForAdmission,
  awaitWriterRetirement,
  awaitWriterRetirements,
  RETIREMENT_ADMISSION_BUDGET_MS,
} from "./admissionWait.ts";

const fixture = Effect.gen(function* () {
  const record = makeRetirementLedger().retain({
    source: {},
    scope: yield* Scope.make(),
    providerSessionId: ProviderSessionId.make("shared-codex-session"),
    threadIds: new Set(),
    kind: "transfer",
    sessionWide: false,
    bindings: [],
  });
  const deadline = (yield* Clock.currentTimeMillis) + RETIREMENT_ADMISSION_BUDGET_MS;
  return { record, deadline };
});

it.effect("pending proof waits beyond ordinary retries and resumes the same operation once", () =>
  Effect.gen(function* () {
    const { record, deadline } = yield* fixture;
    let calls = 0;
    let starts = 0;
    let revalidations = 0;
    const waiting = yield* awaitRetirementForAdmission(
      Effect.sync(() => {
        calls++;
      }).pipe(Effect.andThen(awaitWriterRetirement(record, 10_000))),
      deadline,
      Effect.sync(() => {
        revalidations++;
      }),
    ).pipe(
      Effect.tap(() =>
        Effect.sync(() => {
          starts++;
        }),
      ),
      Effect.forkChild,
    );
    yield* TestClock.adjust("100 seconds");
    assert.isUndefined(waiting.pollUnsafe());
    assert.equal(starts, 0);
    assert.equal(calls, 1);
    assert.isAbove(revalidations, 0);
    yield* Deferred.succeed(record.completion, undefined);
    yield* Fiber.join(waiting);
    assert.equal(starts, 1);
    assert.equal(calls, 1);
  }),
);

it.effect("the wait expires after 110 seconds without discarding proof", () =>
  Effect.gen(function* () {
    const { record, deadline } = yield* fixture;
    const waiting = yield* awaitRetirementForAdmission(
      awaitWriterRetirement(record, 10_000),
      deadline,
      Effect.void,
    ).pipe(Effect.flip, Effect.forkChild);
    yield* TestClock.adjust("109 seconds");
    assert.isUndefined(waiting.pollUnsafe());
    yield* TestClock.adjust("1 second");
    assert.isDefined(waiting.pollUnsafe());
    const failure = yield* Fiber.join(waiting);
    assert.equal(failure._tag, "WriterRetirementWaitExpiredError");
    assert.include(failure.message, "shared-codex-session");
    assert.include(failure.message, "within 110 seconds");
    assert.include(failure.message, "Please retry");
    assert.notInclude(failure.message, "Failed to release");
    assert.isFalse(yield* Deferred.isDone(record.completion));
  }),
);

it.effect.each([false, true])(
  "real retirement failure remains immediate (after pending: %s)",
  (late) =>
    Effect.gen(function* () {
      const { record, deadline } = yield* fixture;
      const failure = new WriterRetirementError({
        providerSessionId: record.providerSessionId,
        cause: "proof unavailable",
      });
      if (!late) yield* Deferred.fail(record.completion, failure);
      const waiting = yield* awaitRetirementForAdmission(
        awaitWriterRetirement(record, 10_000),
        deadline,
        Effect.void,
      ).pipe(Effect.flip, Effect.forkChild);
      if (late) {
        yield* TestClock.adjust("1 minute");
        yield* Deferred.fail(record.completion, failure);
      }
      assert.strictEqual(yield* Fiber.join(waiting), failure);
    }),
);

it.effect(
  "cancellation interrupts admission while preserving the retained retirement observer",
  () =>
    Effect.gen(function* () {
      const { record, deadline } = yield* fixture;
      const waiting = yield* awaitRetirementForAdmission(
        awaitWriterRetirement(record, 10_000),
        deadline,
        Effect.void,
      ).pipe(Effect.forkChild);
      yield* TestClock.adjust("1 minute");
      yield* Fiber.interrupt(waiting);
      assert.isDefined(waiting.pollUnsafe());
      assert.isFalse(yield* Deferred.isDone(record.completion));
      yield* Deferred.succeed(record.completion, undefined);
    }),
);

it.effect("a cancelled or superseded run stops awaiting proof on its currentness fence", () =>
  Effect.gen(function* () {
    const { record, deadline } = yield* fixture;
    let current = true;
    const waiting = yield* awaitRetirementForAdmission(
      awaitWriterRetirement(record, 10_000),
      deadline,
      Effect.suspend(() => (current ? Effect.void : Effect.fail("stale attempt"))),
    ).pipe(Effect.flip, Effect.forkChild);
    yield* TestClock.adjust("1 minute");
    current = false;
    yield* TestClock.adjust("1 second");
    assert.equal(yield* Fiber.join(waiting), "stale attempt");
    assert.isFalse(yield* Deferred.isDone(record.completion));
  }),
);

it.effect("a multi-record conversation barrier cannot resume after only its first closure", () =>
  Effect.gen(function* () {
    const first = yield* fixture;
    const second = yield* fixture;
    const waiting = yield* awaitRetirementForAdmission(
      awaitWriterRetirements([first.record, second.record], 10_000),
      first.deadline,
      Effect.void,
    ).pipe(Effect.forkChild);
    yield* TestClock.adjust("1 minute");
    yield* Deferred.succeed(first.record.completion, undefined);
    yield* TestClock.adjust("40 seconds");
    assert.isUndefined(waiting.pollUnsafe());
    yield* Deferred.succeed(second.record.completion, undefined);
    yield* Fiber.join(waiting);
  }),
);

import { assert, it } from "@effect/vitest";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as TestClock from "effect/testing/TestClock";
import { ProviderControlOutcomeUnknown } from "@t3tools/provider-core/server/jones/providerControlOutcome";
import { makeClaudeQueryRetirement } from "./retirement.ts";

const failure = (cause?: unknown) =>
  new ProviderControlOutcomeUnknown({
    driver: "claude-code",
    providerSessionId: "synthetic-session",
    nativeThreadId: "synthetic-native-thread",
    operation: "interrupt",
    ...(cause === undefined ? {} : { cause }),
  });

it.effect.each(["rejection", "silence"] as const)(
  "closes once after cancel %s, releases the caller, and retains the late cleanup owner",
  (mode) => Effect.gen(function* () {
    const cancelGate = yield* Deferred.make<void>();
    const nativeExit = yield* Deferred.make<void>();
    const drain = yield* Deferred.make<void>();
    const closeStarted = yield* Deferred.make<void>();
    const retired = yield* Deferred.make<void>();
    yield* Effect.addFinalizer(() => Deferred.succeed(cancelGate, undefined));
    yield* Effect.addFinalizer(() => Deferred.succeed(nativeExit, undefined));
    yield* Effect.addFinalizer(() => Deferred.succeed(drain, undefined));
    let cancels = 0;
    let closes = 0;
    let fenced = false;
    const retirement = makeClaudeQueryRetirement({
      deadline: (yield* Clock.currentTimeMillis) + 10_000,
      cancel: Effect.sync(() => {
        cancels++;
      }).pipe(
        Effect.andThen(
          mode === "rejection" ? Effect.fail(new Error("cancel failed")) : Deferred.await(cancelGate),
        ),
      ),
      close: Effect.sync(() => {
        closes++;
      }).pipe(Effect.andThen(Deferred.succeed(closeStarted, undefined))),
      termination: Deferred.await(nativeExit),
      drain: Deferred.await(drain),
      retire: Deferred.succeed(retired, undefined).pipe(Effect.asVoid),
      failure,
      fence: () => {
        fenced = true;
      },
    });
    yield* retirement.start;
    const caller = yield* retirement.join.pipe(Effect.exit, Effect.forkChild);
    if (mode === "silence") yield* TestClock.adjust("2 seconds");
    yield* Deferred.await(closeStarted);
    assert.equal(cancels, 1);
    assert.equal(closes, 1);
    assert.isFalse(yield* Deferred.isDone(retired));
    yield* TestClock.adjust(mode === "silence" ? "8 seconds" : "10 seconds");
    const result = yield* Fiber.join(caller);
    assert.isTrue(Exit.isFailure(result));
    assert.isTrue(fenced);
    yield* retirement.start;
    const retry = yield* Effect.exit(retirement.join);
    assert.isTrue(Exit.isFailure(retry));
    assert.equal(cancels, 1);
    assert.equal(closes, 1);
    yield* Deferred.succeed(nativeExit, undefined);
    assert.isFalse(yield* Deferred.isDone(retired), "native exit alone does not drain terminals");
    yield* Deferred.succeed(drain, undefined);
    yield* Deferred.await(retired);
    assert.isTrue(fenced, "late cleanup does not reopen admission");
    assert.isTrue(Exit.isFailure(yield* Effect.exit(retirement.join)));
  }),
);

it.effect("caller interruption schedules captured close while cancellation is still unacknowledged", () =>
  Effect.gen(function* () {
    const cancelGate = yield* Deferred.make<void>();
    const closeStarted = yield* Deferred.make<void>();
    yield* Effect.addFinalizer(() => Deferred.succeed(cancelGate, undefined));
    let fenced = false;
    const retirement = makeClaudeQueryRetirement({
      deadline: (yield* Clock.currentTimeMillis) + 10_000,
      cancel: Deferred.await(cancelGate),
      close: Deferred.succeed(closeStarted, undefined).pipe(Effect.asVoid),
      termination: Effect.void,
      drain: Effect.void,
      retire: Effect.void,
      failure,
      fence: () => {
        fenced = true;
      },
    });
    yield* retirement.start;
    const joining = yield* retirement.join.pipe(Effect.forkChild);
    yield* Effect.yieldNow;
    yield* Fiber.interrupt(joining);
    yield* Deferred.await(closeStarted);
    assert.isTrue(fenced);
    assert.isTrue(Exit.isFailure(yield* Effect.exit(retirement.join)));
  }),
);

it.effect.each(["rejection", "uninterruptible silence"] as const)(
  "retains an unknown fence after close %s",
  (mode) => Effect.gen(function* () {
    const closeGate = yield* Deferred.make<void>();
    const closeStarted = yield* Deferred.make<void>();
    const retired = yield* Deferred.make<void>();
    yield* Effect.addFinalizer(() => Deferred.succeed(closeGate, undefined));
    let fenced = false;
    const retirement = makeClaudeQueryRetirement({
      deadline: (yield* Clock.currentTimeMillis) + 10_000,
      close: Deferred.succeed(closeStarted, undefined).pipe(
        Effect.andThen(
          mode === "rejection"
            ? Effect.fail(new Error("close failed"))
            : Effect.uninterruptible(Deferred.await(closeGate)),
        ),
      ),
      termination: Effect.void,
      drain: Effect.void,
      retire: Deferred.succeed(retired, undefined).pipe(Effect.asVoid),
      failure,
      fence: () => {
        fenced = true;
      },
    });
    yield* retirement.start;
    const caller = yield* retirement.join.pipe(Effect.exit, Effect.forkChild);
    yield* Deferred.await(closeStarted);
    if (mode === "uninterruptible silence") yield* TestClock.adjust("10 seconds");
    assert.isTrue(Exit.isFailure(yield* Fiber.join(caller)));
    assert.isTrue(fenced);
    assert.isFalse(yield* Deferred.isDone(retired));
  }),
);

it.effect.each(["success", "rejection"] as const)(
  "confirmed retirement adds no delay after cancel %s",
  (mode) => Effect.gen(function* () {
    const startedAt = yield* Clock.currentTimeMillis;
    let retired = false;
    const retirement = makeClaudeQueryRetirement({
      deadline: startedAt + 10_000,
      cancel: mode === "success" ? Effect.void : Effect.fail(new Error("cancel rejected")),
      close: Effect.void,
      termination: Effect.void,
      drain: Effect.void,
      retire: Effect.sync(() => {
        retired = true;
      }),
      failure,
      fence: () => {
        throw new Error("healthy retirement must not fence");
      },
    });
    yield* retirement.start;
    yield* retirement.join;
    assert.isTrue(retired);
    assert.equal(yield* Clock.currentTimeMillis, startedAt);
  }),
);

it.effect("the cancellation sub-budget cannot extend the remaining whole-call budget", () =>
  Effect.gen(function* () {
    const cancelGate = yield* Deferred.make<void>();
    const nativeExit = yield* Deferred.make<void>();
    const closeStarted = yield* Deferred.make<void>();
    yield* Effect.addFinalizer(() => Deferred.succeed(cancelGate, undefined));
    yield* Effect.addFinalizer(() => Deferred.succeed(nativeExit, undefined));
    let fenced = false;
    const retirement = makeClaudeQueryRetirement({
      deadline: (yield* Clock.currentTimeMillis) + 1_000,
      cancel: Deferred.await(cancelGate),
      close: Deferred.succeed(closeStarted, undefined).pipe(Effect.asVoid),
      termination: Deferred.await(nativeExit),
      drain: Effect.void,
      retire: Effect.void,
      failure,
      fence: () => {
        fenced = true;
      },
    });
    yield* retirement.start;
    const waiting = yield* retirement.join.pipe(Effect.exit, Effect.forkChild);
    yield* TestClock.adjust("1 second");
    assert.isTrue(Exit.isFailure(yield* Fiber.join(waiting)));
    assert.isTrue(yield* Deferred.isDone(closeStarted));
    assert.isTrue(fenced);
  }),
);

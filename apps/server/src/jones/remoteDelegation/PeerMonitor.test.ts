import { assert, it } from "@effect/vitest";
import { EnvironmentId, OrchestratorMcpFailure } from "@t3tools/contracts";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as TestClock from "effect/testing/TestClock";
import * as PeerMonitor from "./PeerMonitor.ts";

const peer = EnvironmentId.make("peer:one");
const unreachable = () =>
  new OrchestratorMcpFailure({ code: "orchestration_error", message: "Peer is unreachable." });
const settle = Effect.forEach([1, 2, 3, 4], () => Effect.yieldNow, { discard: true });

it.effect(
  "a long poll lets go of its permit, so a fifth caller waits for the lease, not the poll",
  () =>
    Effect.gen(function* () {
      const monitor = yield* PeerMonitor.make;
      const polls = yield* Effect.forEach([1, 2, 3, 4], (n) =>
        monitor(peer, Effect.sleep("50 seconds").pipe(Effect.as(`poll:${n}`))).pipe(
          Effect.forkChild,
        ),
      );
      yield* settle;
      const short = yield* monitor(peer, Effect.succeed("short")).pipe(Effect.forkChild);
      yield* settle;
      assert.isTrue(short.pollUnsafe() === undefined, "all four permits are leased");
      yield* TestClock.adjust(PeerMonitor.PERMIT_LEASE);
      // Fifty seconds would have been the wait if permits were held across the poll.
      assert.equal(yield* Fiber.join(short), "short");
      yield* TestClock.adjust("50 seconds");
      assert.deepEqual(yield* Effect.forEach(polls, Fiber.join), [
        "poll:1",
        "poll:2",
        "poll:3",
        "poll:4",
      ]);
    }),
);

it.effect("many long polls all start within a few leases instead of four per poll", () =>
  Effect.gen(function* () {
    const monitor = yield* PeerMonitor.make;
    let started = 0;
    const fibers = yield* Effect.forEach(
      Array.from({ length: 12 }, (_, index) => index),
      () =>
        monitor(
          peer,
          Effect.sync(() => started++).pipe(Effect.andThen(Effect.sleep("50 seconds"))),
        ).pipe(Effect.forkChild),
    );
    yield* settle;
    assert.equal(started, 4);
    // Twelve polls, four permits: three leases, where holding across the poll took 150 seconds.
    yield* TestClock.adjust(Duration.times(PeerMonitor.PERMIT_LEASE, 2));
    yield* settle;
    assert.equal(started, 12);
    yield* TestClock.adjust("50 seconds");
    yield* Effect.forEach(fibers, Fiber.join);
  }),
);

it.effect("ordinary calls stay capped at four at a time", () =>
  Effect.gen(function* () {
    const monitor = yield* PeerMonitor.make;
    let active = 0;
    let peak = 0;
    const call = monitor(
      peer,
      Effect.gen(function* () {
        active += 1;
        peak = Math.max(peak, active);
        yield* Effect.sleep("1 second");
        active -= 1;
      }),
    );
    const fibers = yield* Effect.forEach(
      Array.from({ length: 10 }, (_, index) => index),
      () => Effect.forkChild(call),
    );
    yield* settle;
    assert.equal(peak, 4);
    yield* TestClock.adjust("3 seconds");
    yield* Effect.forEach(fibers, Fiber.join);
    assert.equal(peak, 4);
    assert.equal(active, 0);
  }),
);

it.effect("an interrupted caller returns its permit", () =>
  Effect.gen(function* () {
    const monitor = yield* PeerMonitor.make;
    const holders = yield* Effect.forEach([1, 2, 3, 4], () =>
      monitor(peer, Effect.never).pipe(Effect.forkChild),
    );
    yield* settle;
    const queued = yield* monitor(peer, Effect.succeed("ran")).pipe(Effect.forkChild);
    yield* settle;
    assert.isTrue(queued.pollUnsafe() === undefined);
    yield* Fiber.interrupt(holders[0]!);
    assert.equal(yield* Fiber.join(queued), "ran");
    yield* Effect.forEach(holders.slice(1), Fiber.interrupt);
  }),
);

it.effect("an unreachable peer delays the next call, which does not hold a permit meanwhile", () =>
  Effect.gen(function* () {
    const monitor = yield* PeerMonitor.make;
    yield* monitor(peer, Effect.fail(unreachable())).pipe(Effect.ignore);
    let started = false;
    const next = yield* monitor(
      peer,
      Effect.sync(() => {
        started = true;
        return "reached";
      }),
    ).pipe(Effect.forkChild);
    yield* TestClock.adjust("1500 millis");
    assert.isFalse(started, "the delay is at least 1.6 seconds");
    // Four waiting callers would exhaust the permits if waiting held one; a fifth still runs
    // against another peer, whose state is separate.
    const other = EnvironmentId.make("peer:two");
    assert.equal(yield* monitor(other, Effect.succeed("other peer")), "other peer");
    yield* TestClock.adjust("1000 millis");
    assert.equal(yield* Fiber.join(next), "reached");
    assert.isTrue(started);
    // Success cleared the delay.
    assert.equal(yield* monitor(peer, Effect.succeed("immediate")), "immediate");
  }),
);

it.effect("only orchestration errors delay the peer", () =>
  Effect.gen(function* () {
    const monitor = yield* PeerMonitor.make;
    yield* monitor(
      peer,
      Effect.fail(new OrchestratorMcpFailure({ code: "invalid_request", message: "Refused." })),
    ).pipe(Effect.ignore);
    assert.equal(yield* monitor(peer, Effect.succeed("immediate")), "immediate");
  }),
);

it.effect("a caller that queued while another failed waits out the new delay before calling", () =>
  Effect.gen(function* () {
    const monitor = yield* PeerMonitor.make;
    const failing = yield* monitor(
      peer,
      Effect.sleep("100 millis").pipe(Effect.andThen(Effect.fail(unreachable()))),
    ).pipe(Effect.ignore, Effect.forkChild);
    const steady = yield* Effect.forEach([1, 2, 3], () =>
      monitor(peer, Effect.sleep("1 second")).pipe(Effect.forkChild),
    );
    yield* settle;
    let started = false;
    const queued = yield* monitor(
      peer,
      Effect.sync(() => {
        started = true;
        return "called";
      }),
    ).pipe(Effect.forkChild);
    yield* settle;
    assert.isFalse(started, "no permit is free yet");
    // The failure frees a permit at 100 ms. The queued caller takes it, sees the delay the
    // failure set, and gives it back instead of calling a peer that just failed.
    yield* TestClock.adjust("1 second");
    assert.isFalse(started);
    yield* TestClock.adjust("1600 millis");
    assert.equal(yield* Fiber.join(queued), "called");
    yield* Fiber.join(failing);
    yield* Effect.forEach(steady, Fiber.join);
  }),
);

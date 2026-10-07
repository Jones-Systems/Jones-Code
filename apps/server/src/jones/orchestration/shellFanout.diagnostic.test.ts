import { assert, it } from "@effect/vitest";
import * as NodeConsole from "node:console";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Stream from "effect/Stream";

import { bufferLiveStream } from "../../orchestration-v2/LiveStreamBudget.ts";

const stages = ["live", "grouped", "buffered", "merged"] as const;

it.live.each(stages)("shell failure propagates through %s", (stage) =>
  Effect.scoped(
    Effect.gen(function* () {
      const observations: Array<{ stage: string; cause: Cause.Cause<unknown> }> = [];
      const upstreamClosed = yield* Deferred.make<void>();
      const capture =
        (boundary: string) =>
        <A, E, R>(stream: Stream.Stream<A, E, R>): Stream.Stream<A, E, R> =>
          stream.pipe(
            Stream.onError((cause) =>
              Effect.sync(() => {
                observations.push({ stage: boundary, cause });
              }),
            ),
          );
      const live = Stream.succeed({ sequence: 1 }).pipe(
        Stream.mapEffect(() => Effect.die(new Error("synthetic upstream failure"))),
        capture("live"),
        Stream.ensuring(Deferred.succeed(upstreamClosed, undefined)),
      );
      const grouped = live.pipe(Stream.groupedWithin(512, "50 millis"), capture("grouped"));
      const buffered = bufferLiveStream(grouped).pipe(capture("buffered"));
      const merged = Stream.merge(buffered, Stream.never).pipe(capture("merged"));
      const stream =
        stage === "live"
          ? live
          : stage === "grouped"
            ? grouped
            : stage === "buffered"
              ? buffered
              : merged;
      const fiber = yield* Effect.forkChild(Effect.exit(Stream.runCollect(stream)));
      yield* Effect.addFinalizer(() => Fiber.interrupt(fiber).pipe(Effect.asVoid));
      const upstreamExit = yield* Effect.exit(
        Deferred.await(upstreamClosed).pipe(Effect.timeout("5 seconds")),
      );
      const downstreamExit = yield* Effect.exit(
        Fiber.join(fiber).pipe(Effect.timeout("5 seconds")),
      );
      // Keep raw reasons and nested exits: a timeout or parent interrupt is not upstream failure propagation.
      NodeConsole.dir({ stage, observations, upstreamExit, downstreamExit }, { depth: null });
      assert.isTrue(Exit.isSuccess(upstreamExit), "upstream finalizer must complete");
      assert.isTrue(
        Exit.isSuccess(downstreamExit),
        "downstream must terminate without parent cancellation",
      );
      if (Exit.isSuccess(downstreamExit)) {
        assert.isTrue(Exit.isFailure(downstreamExit.value));
        if (Exit.isFailure(downstreamExit.value)) {
          assert.isTrue(
            Cause.hasDies(downstreamExit.value.cause),
            "original synthetic defect must survive",
          );
        }
      }
    }),
  ),
);

// A terminal queue defect must not become a second failing scope finalizer that
// prevents the merged stream from notifying its waiting consumer.
it.live("failed live queue finalization preserves the defect and releases enrichment", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const failure = new Error("synthetic terminal queue defect");
      const failNow = yield* Deferred.make<void>();
      const upstreamClosed = yield* Deferred.make<void>();
      const enrichmentStarted = yield* Deferred.make<void>();
      let activeEnrichment = 0;
      let enrichmentFinalizations = 0;
      const source = Stream.fromEffect(
        Deferred.await(failNow).pipe(Effect.andThen(Effect.die(failure))),
      ).pipe(Stream.ensuring(Deferred.succeed(upstreamClosed, undefined)));
      const enrichment = Stream.unwrap(
        Effect.gen(function* () {
          activeEnrichment++;
          yield* Effect.addFinalizer(() =>
            Effect.sync(() => {
              activeEnrichment--;
              enrichmentFinalizations++;
            }),
          );
          yield* Deferred.succeed(enrichmentStarted, undefined);
          return Stream.never;
        }),
      );
      const fiber = yield* Effect.forkChild(
        Effect.exit(Stream.runCollect(Stream.merge(bufferLiveStream(source), enrichment))),
      );
      yield* Effect.addFinalizer(() => Fiber.interrupt(fiber).pipe(Effect.asVoid));
      yield* Deferred.await(enrichmentStarted).pipe(Effect.timeout("5 seconds"));
      yield* Deferred.succeed(failNow, undefined);
      yield* Deferred.await(upstreamClosed).pipe(Effect.timeout("5 seconds"));
      const result = yield* Fiber.join(fiber).pipe(Effect.timeout("5 seconds"));
      assert.isTrue(Exit.isFailure(result));
      if (Exit.isFailure(result)) {
        assert.lengthOf(result.cause.reasons, 1, "cleanup must not add another failure cause");
        const reason = result.cause.reasons[0]!;
        assert.strictEqual(reason._tag, "Die");
        if (reason._tag === "Die") assert.strictEqual(reason.defect, failure);
      }
      assert.strictEqual(activeEnrichment, 0);
      assert.strictEqual(enrichmentFinalizations, 1);
    }),
  ),
);

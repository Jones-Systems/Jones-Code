import type { JonesUpdateState } from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { observeJonesUpdateState } from "./jonesUpdates.ts";

const staged: JonesUpdateState = {
  source: "jones-actions",
  channel: "jones-main",
  phase: "staged",
  revision: 2,
  capability: { check: true, download: true, install: true },
  stagedHandle: "fixed-stage",
};
it.effect("starts observation after an initially disconnected mount prepares its connection", () =>
  Effect.gen(function* () {
    const connections = yield* SubscriptionRef.make(Option.none<string>());
    const disconnected = yield* Deferred.make<void>();
    let reads = 0;
    const observed = yield* observeJonesUpdateState(
      SubscriptionRef.changes(connections),
      (after) =>
        after === undefined
          ? Effect.sync(() => {
              reads++;
              return staged;
            })
          : Effect.never,
    ).pipe(
      Stream.tap((state) =>
        state === null ? Deferred.succeed(disconnected, undefined) : Effect.void,
      ),
      Stream.take(2),
      Stream.runCollect,
      Effect.forkChild({ startImmediately: true }),
    );
    yield* Deferred.await(disconnected);
    expect(reads).toBe(0);
    yield* SubscriptionRef.set(connections, Option.some("prepared"));
    expect(yield* Fiber.join(observed)).toEqual([null, staged]);
    expect(reads).toBe(1);
  }),
);
it.effect("hides Jones controls when an older host has no updater endpoint", () =>
  Effect.gen(function* () {
    const state = yield* observeJonesUpdateState(Stream.succeed(Option.some("older-host")), () =>
      Effect.fail("missing-endpoint"),
    ).pipe(Stream.runHead);
    expect(state).toEqual(Option.some(null));
  }),
);

it.effect("an ordinary Release host leaves Jones absent and ends HTTP observation", () =>
  Effect.gen(function* () {
    let reads = 0;
    const states = yield* observeJonesUpdateState(Stream.succeed(Option.some("release-host")), () =>
      Effect.sync(() => {
        reads++;
        return null;
      }),
    ).pipe(Stream.runCollect);
    expect(states).toEqual([null]);
    expect(reads).toBe(1);
  }),
);

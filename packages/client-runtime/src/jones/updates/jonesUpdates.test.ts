import type { JonesUpdateState } from "@t3tools/contracts/jones/jonesUpdates";
import { expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as Schema from "effect/Schema";
import { AsyncResult } from "effect/reactivity";
import { JonesUpdateState as JonesUpdateStateSchema } from "@t3tools/contracts/jones/jonesUpdates";
import { jonesUpdateActionError, jonesUpdatePresentation, observeJonesUpdateSnapshot, observeJonesUpdateState } from "./jonesUpdates.ts";

const decodeJonesUpdateState = Schema.decodeUnknownSync(JonesUpdateStateSchema);

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
      Effect.fail({ _tag: "RemoteEnvironmentAuthUndeclaredStatusError", status: 404 }),
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

it.effect("resumes mounted observation with a fresh cursor after connection replacement", () =>
  Effect.gen(function* () {
    const connections = yield* SubscriptionRef.make(Option.some("first"));
    const firstState = yield* Deferred.make<void>();
    const disconnected = yield* Deferred.make<void>();
    const cursors: Array<number | undefined> = [];
    const observed = yield* observeJonesUpdateState(
      SubscriptionRef.changes(connections),
      (after) => {
        cursors.push(after);
        return after === undefined ? Effect.succeed(staged) : Effect.never;
      },
    ).pipe(
      Stream.tap((state) =>
        state === null
          ? Deferred.succeed(disconnected, undefined)
          : Deferred.succeed(firstState, undefined),
      ),
      Stream.take(3),
      Stream.runCollect,
      Effect.forkChild({ startImmediately: true }),
    );
    yield* Deferred.await(firstState);
    yield* SubscriptionRef.set(connections, Option.none());
    yield* Deferred.await(disconnected);
    yield* SubscriptionRef.set(connections, Option.some("replacement"));
    expect(yield* Fiber.join(observed)).toEqual([staged, null, staged]);
    expect(cursors.filter((cursor) => cursor === undefined)).toHaveLength(2);
  }),
);

it.effect("long-polls by the returned revision until the host reports no Jones state", () =>
  Effect.gen(function* () {
    const cursors: Array<number | undefined> = [];
    const newer: JonesUpdateState = {
      ...staged,
      revision: 3,
      phase: "available",
      capability: { check: true, download: false, install: true },
    };
    const states = yield* observeJonesUpdateState(
      Stream.succeed(Option.some("prepared")),
      (after) => {
        cursors.push(after);
        return Effect.succeed(after === undefined ? staged : after === 2 ? newer : null);
      },
    ).pipe(Stream.runCollect);
    expect(cursors).toEqual([undefined, 2, 3]);
    expect(states).toEqual([staged, newer, null]);
  }),
);

it("keeps restart pending until a host outcome arrives and presents rollback reasons", () => {
  expect(
    jonesUpdatePresentation({ ...staged, phase: "installing", updateId: "native-id" }),
  ).toMatchObject({
    busy: true,
    message: "Installing — server restarting. Waiting for the launcher outcome.",
    outcomeMessage: undefined,
  });
  expect(
    jonesUpdatePresentation({
      ...staged,
      phase: "rolled-back",
      outcome: {
        status: "rolled-back",
        fromVersion: "1.0.0",
        targetVersion: "2.0.0",
        reason: "candidate older than database",
      },
    }),
  ).toMatchObject({
    busy: false,
    outcomeMessage: "Rolled back: 1.0.0 → 2.0.0 · candidate older than database",
  });
});

it("decodes older Jones update payloads without outcome metadata", () => {
  expect(decodeJonesUpdateState(staged)).toEqual(staged);
});

it.effect("retains the cursor and visible state across a transient GET failure, then recovers", () =>
  Effect.gen(function* () {
    const cursors: Array<number | undefined> = [];
    const delays: number[] = [];
    const states = yield* observeJonesUpdateSnapshot(Stream.succeed(Option.some("prepared")), (after) => {
      cursors.push(after);
      if (cursors.length === 1) return Effect.succeed(staged);
      if (cursors.length === 2) return Effect.fail({ _tag: "RemoteEnvironmentAuthTimeoutError" });
      return Effect.succeed({ ...staged, revision: 3 });
    }, (failure) => Effect.sync(() => { delays.push(failure); })).pipe(Stream.take(3), Stream.runCollect);
    expect(states.map((snapshot) => snapshot.freshness)).toEqual(["fresh", "stale", "fresh"]);
    expect(states[1]?.state).toEqual(staged);
    expect(cursors).toEqual([undefined, 2, 2]);
    expect(delays).toEqual([1]);
  }),
);

it.effect("connection replacement cancels a pending read retry and resets its cursor", () =>
  Effect.gen(function* () {
    const connections = yield* SubscriptionRef.make(Option.some("first"));
    const waiting = yield* Deferred.make<void>();
    const canceled = yield* Deferred.make<void>();
    let reads = 0;
    const cursors: Array<number | undefined> = [];
    const observer = yield* observeJonesUpdateSnapshot(SubscriptionRef.changes(connections), (after) => {
      reads++;
      cursors.push(after);
      return reads === 1 ? Effect.fail({ _tag: "RemoteEnvironmentAuthTimeoutError" }) : Effect.succeed(staged);
    }, () => Deferred.succeed(waiting, undefined).pipe(
      Effect.andThen(Effect.never), Effect.onInterrupt(() => Deferred.succeed(canceled, undefined)),
    )).pipe(Stream.take(2), Stream.runCollect, Effect.forkChild({ startImmediately: true }));
    yield* Deferred.await(waiting);
    yield* SubscriptionRef.set(connections, Option.some("replacement"));
    yield* Deferred.await(canceled);
    const results = yield* Fiber.join(observer);
    expect(results.map((snapshot) => snapshot.freshness)).toEqual(["stale", "fresh"]);
    expect(cursors).toEqual([undefined, undefined]);
  }),
);

it.effect("authorization refusal marks retained data stale without retrying", () =>
  Effect.gen(function* () {
    let reads = 0;
    let delays = 0;
    const results = yield* observeJonesUpdateSnapshot(Stream.succeed(Option.some("prepared")), () =>
      ++reads === 1 ? Effect.succeed(staged) : Effect.fail({ _tag: "EnvironmentAuthInvalidError" }),
      () => Effect.sync(() => { delays++; }),
    ).pipe(Stream.take(2), Stream.runCollect);
    expect(results[1]).toMatchObject({ state: staged, freshness: "stale" });
    expect(delays).toBe(0);
  }),
);

it("distinguishes fulfilled remote failures from accepted restart progress", () => {
  expect(jonesUpdateActionError(AsyncResult.success({ ...staged, phase: "blocked", message: "Source changed" })))
    .toBe("Source changed");
  expect(jonesUpdateActionError(AsyncResult.success({ ...staged, phase: "installing" }))).toBeNull();
});

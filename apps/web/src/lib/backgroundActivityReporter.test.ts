import { EnvironmentRegistry } from "@t3tools/client-runtime/connection";
import { type ClientActivityReportInput, EnvironmentId, WS_METHODS } from "@t3tools/contracts";
import { afterEach, beforeEach, describe, expect, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as TestClock from "effect/testing/TestClock";
import { vi } from "vite-plus/test";

import {
  layer,
  observeBackgroundActivitySubscription,
  retainedBackgroundScopes,
} from "./backgroundActivityReporter.ts";

// Expose the built report to the fake registry without creating an RPC session.
vi.mock("@t3tools/client-runtime/rpc", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@t3tools/client-runtime/rpc")>()),
  request: (_method: string, input: unknown) => Effect.succeed(input),
}));

describe("wasRecentlyInteracted", () => {
  it.effect("retains an observed subscription until its returned finalizer runs", () =>
    Effect.gen(function* () {
      const environmentId = EnvironmentId.make("environment-observation-test");
      const scope = { type: "vcs-status" as const, cwd: "/repo" };
      const releasePassive = yield* observeBackgroundActivitySubscription({
        environmentId,
        method: WS_METHODS.subscribeVcsStatus,
        input: { cwd: scope.cwd, includeRemote: false },
      });
      expect(retainedBackgroundScopes(environmentId)).toEqual([]);
      const release = yield* observeBackgroundActivitySubscription({
        environmentId,
        method: WS_METHODS.subscribeVcsStatus,
        input: { cwd: scope.cwd },
      });

      expect(retainedBackgroundScopes(environmentId)).toEqual([scope]);

      yield* releasePassive;
      expect(retainedBackgroundScopes(environmentId)).toEqual([scope]);

      yield* release;
      expect(retainedBackgroundScopes(environmentId)).toEqual([]);
    }),
  );

  it.effect("keeps delimiter-containing environment and scope values distinct", () =>
    Effect.gen(function* () {
      const firstEnvironmentId = EnvironmentId.make("a");
      const secondEnvironmentId = EnvironmentId.make("a:vcs-status:b");
      const releaseFirst = yield* observeBackgroundActivitySubscription({
        environmentId: firstEnvironmentId,
        method: WS_METHODS.subscribeVcsStatus,
        input: { cwd: "b:vcs-status:c" },
      });
      const releaseSecond = yield* observeBackgroundActivitySubscription({
        environmentId: secondEnvironmentId,
        method: WS_METHODS.subscribeVcsStatus,
        input: { cwd: "c" },
      });

      expect(retainedBackgroundScopes(firstEnvironmentId)).toEqual([
        { type: "vcs-status", cwd: "b:vcs-status:c" },
      ]);
      expect(retainedBackgroundScopes(secondEnvironmentId)).toEqual([
        { type: "vcs-status", cwd: "c" },
      ]);

      yield* Effect.all([releaseFirst, releaseSecond]);
    }),
  );
});

describe("layer", () => {
  beforeEach(() => {
    const target = new EventTarget();
    const listeners = {
      addEventListener: target.addEventListener.bind(target),
      removeEventListener: target.removeEventListener.bind(target),
      dispatchEvent: target.dispatchEvent.bind(target),
    };
    vi.stubGlobal("window", {
      ...listeners,
      localStorage: { getItem: () => "client", setItem: () => undefined },
    });
    vi.stubGlobal("document", { ...listeners, visibilityState: "visible", hasFocus: () => true });
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it.effect("T13: a stalled A does not delay B activity changes or cadence renewals", () =>
    Effect.gen(function* () {
      const a = EnvironmentId.make("stalled-environment");
      const b = EnvironmentId.make("healthy-environment");
      const entries = yield* SubscriptionRef.make(
        new Map([
          [a, {}],
          [b, {}],
        ]),
      );
      const reports: ClientActivityReportInput[] = [];
      let aCanceled = 0;
      const registry = EnvironmentRegistry.EnvironmentRegistry.of({
        entries,
        run: (environmentId: EnvironmentId, effect: Effect.Effect<ClientActivityReportInput>) =>
          Effect.gen(function* () {
            const report = yield* effect;
            reports.push(report);
            if (environmentId === a) {
              return yield* Effect.never.pipe(
                Effect.onInterrupt(() =>
                  Effect.sync(() => {
                    aCanceled += 1;
                  }),
                ),
              );
            }
          }),
      } as unknown as EnvironmentRegistry.EnvironmentRegistry["Service"]);

      yield* Layer.build(
        layer.pipe(Layer.provide(Layer.succeed(EnvironmentRegistry.EnvironmentRegistry, registry))),
      );
      yield* TestClock.adjust("1 second");
      expect(reports.filter((report) => report.environmentId === b)).toHaveLength(1);

      Object.assign(document, { visibilityState: "hidden" });
      window.dispatchEvent(new Event("blur"));
      yield* TestClock.adjust("250 millis");
      const changed = reports.filter((report) => report.environmentId === b);
      expect(changed).toHaveLength(2);
      expect(changed[1]).toMatchObject({ visible: false, appState: "background", ttlMs: 45_000 });
      expect(changed[1]?.scopes).toEqual([{ type: "provider-status" }]);
      expect(aCanceled).toBe(0);

      yield* TestClock.adjust("25 seconds");
      yield* TestClock.adjust("25 seconds");
      const renewals = reports.filter((report) => report.environmentId === b);
      expect(renewals).toHaveLength(4);
      const observed = renewals.map((report) => DateTime.toEpochMillis(report.observedAt));
      expect(observed[3]! - observed[2]!).toBe(25_000);
      expect(aCanceled).toBeGreaterThan(0);

      const beforeRemoval = aCanceled;
      // Removal must interrupt immediately, before the report trigger's debounce.
      window.dispatchEvent(new Event("focus"));
      yield* TestClock.adjust("250 millis");
      yield* SubscriptionRef.set(entries, new Map([[b, {}]]));
      yield* TestClock.adjust("1 millis");
      expect(aCanceled).toBe(beforeRemoval + 1);
    }).pipe(Effect.scoped),
  );

  // As when an environment's connection is torn down while its report is in flight.
  it.effect("keeps reporting after a report is interrupted", () =>
    Effect.gen(function* () {
      const entries = yield* SubscriptionRef.make(
        new Map([[EnvironmentId.make("environment-reporter-test"), {}]]),
      );
      let calls = 0;
      const registry = EnvironmentRegistry.EnvironmentRegistry.of({
        entries,
        run: () => {
          calls += 1;
          return calls === 1 ? Effect.failCause(Cause.interrupt()) : Effect.void;
        },
      } as unknown as EnvironmentRegistry.EnvironmentRegistry["Service"]);

      yield* Layer.build(
        layer.pipe(Layer.provide(Layer.succeed(EnvironmentRegistry.EnvironmentRegistry, registry))),
      );
      yield* TestClock.adjust("1 second");
      expect(calls).toBe(1);

      yield* TestClock.adjust("25 seconds");
      expect(calls).toBe(2);
    }).pipe(Effect.scoped),
  );
});

import { EnvironmentRegistry } from "@t3tools/client-runtime/connection";
import { type ClientActivityReportInput, EnvironmentId } from "@t3tools/contracts";
import { afterEach, beforeEach, describe, expect, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as TestClock from "effect/testing/TestClock";
import { vi } from "vite-plus/test";

import * as MobileStorage from "../persistence/mobile-storage";
import { layerReporter } from "./background-activity";

const native = vi.hoisted(() => ({
  listeners: new Set<(state: string) => void>(),
}));

vi.mock("react-native", () => ({
  AppState: {
    currentState: "active",
    addEventListener: (_event: string, listener: (state: string) => void) => {
      native.listeners.add(listener);
      return { remove: () => native.listeners.delete(listener) };
    },
  },
}));
vi.mock("expo-secure-store", () => ({}));

// Expose the built report to the fake registry without creating an RPC session.
vi.mock("@t3tools/client-runtime/rpc", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@t3tools/client-runtime/rpc")>()),
  request: (_method: string, input: unknown) => Effect.succeed(input),
}));

describe("mobile activity reporter", () => {
  beforeEach(() => native.listeners.clear());
  afterEach(() => expect(native.listeners.size).toBe(0));

  it.effect("T13: a stalled A does not delay B app-state changes or cadence renewals", () =>
    Effect.gen(function* () {
      const a = EnvironmentId.make("mobile-stalled");
      const b = EnvironmentId.make("mobile-healthy");
      const entries = yield* SubscriptionRef.make(
        new Map([
          [a, {}],
          [b, {}],
        ]),
      );
      const reports: ClientActivityReportInput[] = [];
      const canceled: EnvironmentId[] = [];
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
                    canceled.push(environmentId);
                  }),
                ),
              );
            }
          }),
      } as unknown as EnvironmentRegistry.EnvironmentRegistry["Service"]);
      const storage = MobileStorage.MobileStorage.of({
        loadOrCreateAgentAwarenessDeviceId: Effect.succeed("test-device"),
      } as unknown as MobileStorage.MobileStorage["Service"]);

      yield* Layer.build(
        layerReporter.pipe(
          Layer.provide(
            Layer.mergeAll(
              Layer.succeed(EnvironmentRegistry.EnvironmentRegistry, registry),
              Layer.succeed(MobileStorage.MobileStorage, storage),
            ),
          ),
        ),
      );
      yield* TestClock.adjust("1 second");
      expect(reports.filter((report) => report.environmentId === b)).toHaveLength(1);
      for (const listener of native.listeners) listener("background");
      yield* TestClock.adjust("250 millis");
      const changed = reports.filter((report) => report.environmentId === b);
      expect(changed).toHaveLength(2);
      expect(changed[1]).toMatchObject({
        clientId: "mobile-test-device",
        clientKind: "mobile",
        visible: false,
        focused: false,
        recentlyInteracted: false,
        appState: "background",
        ttlMs: 45_000,
        scopes: [{ type: "provider-status" }],
      });
      expect(canceled).toEqual([]);

      yield* TestClock.adjust("25 seconds");
      yield* TestClock.adjust("25 seconds");
      const renewals = reports.filter((report) => report.environmentId === b);
      expect(renewals).toHaveLength(4);
      const observed = renewals.map((report) => DateTime.toEpochMillis(report.observedAt));
      expect(observed[3]! - observed[2]!).toBe(25_000);
      expect(canceled.length).toBeGreaterThan(0);

      const beforeRemoval = canceled.length;
      yield* SubscriptionRef.set(entries, new Map([[b, {}]]));
      yield* TestClock.adjust("1 millis");
      expect(canceled).toHaveLength(beforeRemoval + 1);
    }).pipe(Effect.scoped),
  );
});

import { EnvironmentId } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as TestClock from "effect/testing/TestClock";
import * as HttpClient from "effect/http/HttpClient";

import type { ConnectionCatalogEntry, ConnectionRoute } from "../../connection/catalog.ts";
import * as Connectivity from "../../connection/connectivity.ts";
import * as ConnectionDriver from "../../connection/driver.ts";
import {
  ConnectionBlockedError,
  ConnectionTransientError,
  RelayConnectionTarget,
  SshConnectionTarget,
  type ConnectionAttemptError,
  type ConnectionTarget,
  type PreparedConnection,
  type SupervisorConnectionState,
} from "../../connection/model.ts";
import * as ConnectionResolver from "../../connection/resolver.ts";
import * as EnvironmentSupervisor from "../../connection/supervisor.ts";
import * as ConnectionWakeups from "../../connection/wakeups.ts";
import type { WsRpcProtocolClient } from "../../rpc/protocol.ts";
import * as RpcSession from "../../rpc/session.ts";

const environmentId = EnvironmentId.make("route-budget-test");
const preferred = new SshConnectionTarget({
  environmentId,
  label: "Route budget test",
  connectionId: "ssh:preferred",
});
const fallback = new RelayConnectionTarget({ environmentId, label: preferred.label });
const entry: ConnectionCatalogEntry = {
  target: preferred,
  profile: Option.none(),
  enabled: true,
  alternateRoutes: [{ target: fallback, profile: Option.none() }],
};

const awaitPhase = (
  state: SubscriptionRef.SubscriptionRef<SupervisorConnectionState>,
  phase: SupervisorConnectionState["phase"],
) =>
  SubscriptionRef.changes(state).pipe(
    Stream.filter((value) => value.phase === phase),
    Stream.runHead,
    Effect.map(Option.getOrThrow),
  );

const makeHarness = Effect.fn("RouteBudgetTest.makeHarness")(function* (options?: {
  readonly prepare?: (target: ConnectionTarget) => Effect.Effect<void, ConnectionAttemptError>;
  readonly open?: (target: ConnectionTarget) => Effect.Effect<void, ConnectionAttemptError>;
  readonly ready?: (target: ConnectionTarget) => Effect.Effect<void, ConnectionAttemptError>;
  readonly release?: (target: ConnectionTarget) => Effect.Effect<void>;
}) {
  const attempts: Array<ConnectionTarget> = [];
  const releases: Array<ConnectionTarget> = [];
  const opened: Array<ConnectionTarget> = [];
  const closed: Array<ConnectionTarget> = [];
  let active = 0;
  let maxActive = 0;
  const release = (target: ConnectionTarget) =>
    (options?.release?.(target) ?? Effect.void).pipe(
      Effect.andThen(
        Effect.sync(() => {
          releases.push(target);
          active -= 1;
        }),
      ),
    );
  const driver = yield* ConnectionDriver.make.pipe(
    Effect.provideService(
      ConnectionResolver.ConnectionResolver,
      ConnectionResolver.ConnectionResolver.of({
        prepare: (routeEntry) =>
          Effect.gen(function* () {
            const target = routeEntry.target;
            yield* Effect.sync(() => {
              attempts.push(target);
              active += 1;
              maxActive = Math.max(maxActive, active);
            });
            yield* (options?.prepare?.(target) ?? Effect.void).pipe(
              Effect.onExit((exit) => (Exit.isSuccess(exit) ? Effect.void : release(target))),
            );
            return {
              environmentId,
              label: target.label,
              target,
              httpBaseUrl: "https://route-budget.example.test",
              socketUrl: "wss://route-budget.example.test/ws",
              httpAuthorization: null,
            } satisfies PreparedConnection;
          }),
        prepareForUpdate: () => Effect.die(new Error("Update preparation is not used.")),
      }),
    ),
    Effect.provideService(
      RpcSession.RpcSessionFactory,
      RpcSession.RpcSessionFactory.of({
        connect: (prepared) =>
          Effect.gen(function* () {
            yield* (options?.open?.(prepared.target) ?? Effect.void).pipe(
              Effect.onExit((exit) =>
                Exit.isSuccess(exit) ? Effect.void : release(prepared.target),
              ),
            );
            return yield* Effect.acquireRelease(
              Effect.sync(() => {
                opened.push(prepared.target);
                return {
                  client: {} as WsRpcProtocolClient,
                  initialConfig: Effect.die(new Error("Initial config is not used.")),
                  subscribeServerConfig: () => Stream.die(new Error("Config stream is not used.")),
                  ready: options?.ready?.(prepared.target) ?? Effect.void,
                  probe: Effect.void,
                  closed: Effect.never,
                } satisfies RpcSession.RpcSession;
              }),
              () =>
                Effect.sync(() => void closed.push(prepared.target)).pipe(
                  Effect.andThen(release(prepared.target)),
                ),
            );
          }),
      }),
    ),
    Effect.provideService(
      HttpClient.HttpClient,
      HttpClient.make(() => Effect.die(new Error("These routes have no public descriptor URL."))),
    ),
  );

  const startSupervisor = (catalog = entry) =>
    EnvironmentSupervisor.make(catalog, { initiallyDesired: true }).pipe(
      Effect.provideService(ConnectionDriver.ConnectionDriver, driver),
      Effect.provideService(Connectivity.Connectivity, {
        status: Effect.succeed("online" as const),
        changes: Stream.never,
      }),
      Effect.provideService(ConnectionWakeups.ConnectionWakeups, { changes: Stream.never }),
    );

  return {
    startSupervisor,
    attempts,
    releases,
    opened,
    closed,
    active: () => active,
    maxActive: () => maxActive,
  };
});

describe("route attempt budgets through the connection driver", () => {
  it.effect("falls back from never-completing preparation after its cleanup", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const harness = yield* makeHarness({
        prepare: (target) =>
          target === preferred
            ? Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never))
            : Effect.void,
      });
      const supervisor = yield* harness.startSupervisor();
      yield* Deferred.await(started);
      yield* TestClock.adjust(14_999);
      expect(harness.attempts).toEqual([preferred]);
      yield* TestClock.adjust(1);
      expect(harness.attempts).toEqual([preferred, fallback]);
      yield* awaitPhase(supervisor.state, "connected");
      expect(harness.attempts).toEqual([preferred, fallback]);
      expect(harness.releases).toEqual([preferred]);
      expect(harness.opened).toEqual([fallback]);
      expect(harness.maxActive()).toBe(1);
      yield* supervisor.disconnect;
      yield* awaitPhase(supervisor.state, "available");
      expect(harness.releases).toEqual([preferred, fallback]);
      expect(harness.closed).toEqual([fallback]);
      expect(harness.active()).toBe(0);
    }).pipe(Effect.provide(TestClock.layer())),
  );

  it.effect("shares the establishment allowance between preparation and opening", () =>
    Effect.gen(function* () {
      const preparing = yield* Deferred.make<void>();
      const prepared = yield* Deferred.make<void>();
      const opening = yield* Deferred.make<void>();
      const harness = yield* makeHarness({
        prepare: (target) =>
          target === preferred
            ? Deferred.succeed(preparing, undefined).pipe(Effect.andThen(Deferred.await(prepared)))
            : Effect.void,
        open: (target) =>
          target === preferred
            ? Deferred.succeed(opening, undefined).pipe(Effect.andThen(Effect.never))
            : Effect.void,
      });
      const supervisor = yield* harness.startSupervisor();
      yield* Deferred.await(preparing);
      yield* TestClock.adjust("10 seconds");
      yield* Deferred.succeed(prepared, undefined);
      yield* Deferred.await(opening);
      yield* TestClock.adjust(4_999);
      expect(harness.attempts).toEqual([preferred]);
      yield* TestClock.adjust(1);
      expect(harness.attempts).toEqual([preferred, fallback]);
      yield* awaitPhase(supervisor.state, "connected");
      expect(harness.attempts).toEqual([preferred, fallback]);
      expect(harness.releases).toEqual([preferred]);
      expect(harness.maxActive()).toBe(1);
    }).pipe(Effect.provide(TestClock.layer())),
  );

  it.effect("gives a connected but never-ready route its full synchronization allowance", () =>
    Effect.gen(function* () {
      const preparing = yield* Deferred.make<void>();
      const prepared = yield* Deferred.make<void>();
      const synchronizing = yield* Deferred.make<void>();
      const closing = yield* Deferred.make<void>();
      const finishClose = yield* Deferred.make<void>();
      yield* Effect.gen(function* () {
        const harness = yield* makeHarness({
          prepare: (target) =>
            target === preferred
              ? Deferred.succeed(preparing, undefined).pipe(
                  Effect.andThen(Deferred.await(prepared)),
                )
              : Effect.void,
          ready: (target) =>
            target === preferred
              ? Deferred.succeed(synchronizing, undefined).pipe(Effect.andThen(Effect.never))
              : Effect.void,
          release: (target) =>
            target === preferred
              ? Deferred.succeed(closing, undefined).pipe(
                  Effect.andThen(Deferred.await(finishClose)),
                )
              : Effect.void,
        });
        const supervisor = yield* harness.startSupervisor();
        yield* Deferred.await(preparing);
        yield* TestClock.adjust("14 seconds");
        yield* Deferred.succeed(prepared, undefined);
        yield* Deferred.await(synchronizing);
        yield* TestClock.adjust(74_999);
        expect(harness.attempts).toEqual([preferred]);
        expect(harness.opened).toEqual([preferred]);
        expect(harness.closed).toEqual([]);
        yield* TestClock.adjust(1);
        expect(yield* Deferred.isDone(closing)).toBe(true);
        expect(harness.attempts).toEqual([preferred]);
        expect(harness.active()).toBe(1);
        yield* Deferred.succeed(finishClose, undefined);
        yield* awaitPhase(supervisor.state, "connected");
        expect(harness.attempts).toEqual([preferred, fallback]);
        expect(harness.releases).toEqual([preferred]);
        expect(harness.closed).toEqual([preferred]);
        expect(harness.maxActive()).toBe(1);
      }).pipe(Effect.ensuring(Deferred.succeed(finishClose, undefined)));
    }).pipe(Effect.provide(TestClock.layer())),
  );

  it.effect("leaves unused aggregate allowances available to the final route", () =>
    Effect.gen(function* () {
      const preparing = yield* Deferred.make<void>();
      const prepared = yield* Deferred.make<void>();
      const synchronizing = yield* Deferred.make<void>();
      const ready = yield* Deferred.make<void>();
      const harness = yield* makeHarness({
        prepare: (target) =>
          target === preferred
            ? Effect.fail(new ConnectionTransientError({ reason: "transport", detail: "Refused." }))
            : Deferred.succeed(preparing, undefined).pipe(Effect.andThen(Deferred.await(prepared))),
        ready: () =>
          Deferred.succeed(synchronizing, undefined).pipe(Effect.andThen(Deferred.await(ready))),
      });
      const supervisor = yield* harness.startSupervisor();
      yield* Deferred.await(preparing);
      yield* TestClock.adjust("16 seconds");
      expect(harness.attempts).toEqual([preferred, fallback]);
      expect((yield* SubscriptionRef.get(supervisor.state)).phase).toBe("connecting");
      yield* Deferred.succeed(prepared, undefined);
      yield* Deferred.await(synchronizing);
      yield* TestClock.adjust("76 seconds");
      expect(harness.releases).toEqual([preferred]);
      expect((yield* SubscriptionRef.get(supervisor.state)).phase).toBe("connecting");
      yield* Deferred.succeed(ready, undefined);
      yield* awaitPhase(supervisor.state, "connected");
      expect(harness.maxActive()).toBe(1);
    }).pipe(Effect.provide(TestClock.layer())),
  );

  it.effect("preserves a single slow synchronization until the aggregate deadline", () =>
    Effect.gen(function* () {
      const synchronizing = yield* Deferred.make<void>();
      const harness = yield* makeHarness({
        ready: () => Deferred.succeed(synchronizing, undefined).pipe(Effect.andThen(Effect.never)),
      });
      const supervisor = yield* harness.startSupervisor({ ...entry, alternateRoutes: [] });
      yield* Deferred.await(synchronizing);
      yield* TestClock.adjust("76 seconds");
      expect((yield* SubscriptionRef.get(supervisor.state)).phase).toBe("connecting");
      expect(harness.closed).toEqual([]);
      yield* TestClock.adjust("14 seconds");
      const state = yield* awaitPhase(supervisor.state, "backoff");
      expect(state.lastFailure).toMatchObject({
        _tag: "ConnectionTransientError",
        reason: "timeout",
      });
      expect(harness.attempts).toEqual([preferred]);
      expect(harness.closed).toEqual([preferred]);
      expect(harness.active()).toBe(0);
    }).pipe(Effect.provide(TestClock.layer())),
  );

  it.effect("interrupts a budgeted route without starting fallback on explicit disconnect", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const harness = yield* makeHarness({
        ready: () => Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never)),
      });
      const supervisor = yield* harness.startSupervisor();
      yield* Deferred.await(started);
      yield* supervisor.disconnect;
      yield* awaitPhase(supervisor.state, "available");
      yield* TestClock.adjust("2 minutes");
      expect(harness.attempts).toEqual([preferred]);
      expect(harness.closed).toEqual([preferred]);
      expect(harness.active()).toBe(0);
    }).pipe(Effect.provide(TestClock.layer())),
  );
});

describe("route budget ordering seam", () => {
  it.effect.each(
    (
      [
        ["silent", "answered", "silent"],
        ["answered", "silent", "silent"],
        ["silent", "silent", "silent"],
        ["answered", "answered", "answered"],
      ] as const
    ).map((checks) => ({ checks })),
  )("exempts only the final actual attempt for checks $checks", ({ checks }) =>
    Effect.gen(function* () {
      const third = new SshConnectionTarget({
        environmentId,
        label: preferred.label,
        connectionId: "ssh:third",
      });
      const routes: ReadonlyArray<ConnectionRoute> = [preferred, fallback, third].map((target) => ({
        target,
        profile: Option.none(),
      }));
      const attempted: Array<{
        readonly target: ConnectionTarget;
        readonly hasFallback: boolean;
      }> = [];
      const error = new ConnectionTransientError({ reason: "transport", detail: "Refused." });
      const result = yield* ConnectionDriver.connectOverRoutes(
        { ...entry, alternateRoutes: routes.slice(1) },
        (route) =>
          Effect.succeed(checks[routes.findIndex((saved) => saved.target === route.target)]!),
        (route, hasFallback) =>
          Effect.sync(() => attempted.push({ target: route.target, hasFallback })).pipe(
            Effect.andThen(Effect.fail(error)),
          ),
      ).pipe(Effect.flip);
      expect(result).toBe(error);
      const actualOrder = [
        ...routes.filter((_route, index) => checks[index] !== "silent"),
        ...routes.filter((_route, index) => checks[index] === "silent"),
      ];
      expect(attempted).toEqual(
        actualOrder.map((route, index) => ({ target: route.target, hasFallback: index < 2 })),
      );
    }),
  );

  it.effect("keeps unsupported-server failures terminal", () =>
    Effect.gen(function* () {
      const attempted: Array<ConnectionTarget> = [];
      const error = new ConnectionBlockedError({
        reason: "unsupported",
        detail: "Update required.",
      });
      const result = yield* ConnectionDriver.connectOverRoutes(
        entry,
        () => Effect.succeed("answered"),
        (route) =>
          Effect.sync(() => attempted.push(route.target)).pipe(Effect.andThen(Effect.fail(error))),
      ).pipe(Effect.flip);
      expect(result).toBe(error);
      expect(attempted).toEqual([preferred]);
    }),
  );
});

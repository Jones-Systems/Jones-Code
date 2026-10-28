import { EnvironmentId, ORCHESTRATION_PROTOCOL_VERSION } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as TestClock from "effect/testing/TestClock";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientResponse from "effect/http/HttpClientResponse";

import { BearerConnectionProfile, type ConnectionCatalogEntry } from "../../connection/catalog.ts";
import * as Connectivity from "../../connection/connectivity.ts";
import * as ConnectionDriver from "../../connection/driver.ts";
import {
  BearerConnectionTarget,
  ConnectionBlockedError,
  ConnectionTransientError,
  RelayConnectionTarget,
  SshConnectionTarget,
  type ConnectionAttemptError,
  type PreparedConnection,
} from "../../connection/model.ts";
import * as ConnectionResolver from "../../connection/resolver.ts";
import { connectionRoutes, entryWithRoutes } from "../../connection/routes.ts";
import * as EnvironmentSupervisor from "../../connection/supervisor.ts";
import * as ConnectionWakeups from "../../connection/wakeups.ts";
import { CloudSession, type CloudSessionIdentity } from "../../platform/capabilities.ts";
import type { WsRpcProtocolClient } from "../../rpc/protocol.ts";
import * as RpcSession from "../../rpc/session.ts";

const environmentId = EnvironmentId.make("connect-recovery-test");
const preferred = new RelayConnectionTarget({ environmentId, label: "Recovery test" });
const fallback = new SshConnectionTarget({
  environmentId,
  label: preferred.label,
  connectionId: "ssh:fallback",
});
const entry: ConnectionCatalogEntry = {
  target: preferred,
  profile: Option.none(),
  alternateRoutes: [{ target: fallback, profile: Option.none() }],
  enabled: true,
};
const unavailable = () =>
  new ConnectionTransientError({
    reason: "relay-unavailable",
    detail: "Synthetic Connect failure.",
  });

const drain = Effect.forEach(Array.from({ length: 60 }), () => Effect.yieldNow, { discard: true });

const makeHarness = Effect.fn("ConnectRecoveryTest.makeHarness")(function* (options?: {
  readonly http?: HttpClient.HttpClient;
}) {
  const identity = yield* Ref.make<Option.Option<CloudSessionIdentity>>(
    Option.some({ accountId: "account-a" }),
  );
  const wakeups = yield* SubscriptionRef.make(0);
  const lock = yield* Semaphore.make(1);
  const preflightStarted = yield* Deferred.make<void>();
  const consuming = yield* Deferred.make<void>();
  const consumeGate = yield* Deferred.make<void>();
  let authoritative: ConnectionCatalogEntry | undefined = entry;
  let restored = false;
  let holdConsumption = false;
  let ownerCalls = 0;
  let cancelled = 0;
  let nextPreparation: Effect.Effect<void, ConnectionAttemptError> = Effect.void;
  let mismatch = false;
  const prepared: Array<PreparedConnection> = [];
  const sockets: Array<PreparedConnection> = [];
  const released: Array<PreparedConnection> = [];
  const closures: Array<Deferred.Deferred<never, ConnectionTransientError>> = [];
  const driver = yield* ConnectionDriver.make.pipe(
    Effect.provideService(ConnectionResolver.ConnectionResolver, {
      prepare: (catalog) =>
        Effect.gen(function* () {
          if (catalog.target._tag !== "SshConnectionTarget") {
            if (!restored) return yield* unavailable();
            yield* Deferred.succeed(preflightStarted, undefined);
            yield* nextPreparation.pipe(
              Effect.onInterrupt(() =>
                Effect.sync(() => {
                  cancelled += 1;
                }),
              ),
            );
          }
          const result: PreparedConnection = {
            environmentId: mismatch ? EnvironmentId.make("wrong-environment") : environmentId,
            label: catalog.target.label,
            target: catalog.target,
            httpBaseUrl: "https://recovery.example.test/",
            socketUrl: `wss://recovery.example.test/one-use-${prepared.length}`,
            httpAuthorization: null,
          };
          prepared.push(result);
          return result;
        }),
      prepareForUpdate: () => Effect.die("Update preparation is outside this test."),
    }),
    Effect.provideService(RpcSession.RpcSessionFactory, {
      connect: (material) =>
        Effect.gen(function* () {
          const closed = yield* Deferred.make<never, ConnectionTransientError>();
          closures.push(closed);
          return yield* Effect.acquireRelease(
            Effect.sync(() => {
              sockets.push(material);
              return {
                client: {} as WsRpcProtocolClient,
                initialConfig: Effect.die("No learning in this test."),
                subscribeServerConfig: () => Stream.die("No config stream in this test."),
                ready: Effect.void,
                probe: Effect.void,
                closed: Deferred.await(closed),
              } satisfies RpcSession.RpcSession;
            }),
            () =>
              Effect.sync(() => {
                released.push(material);
              }),
          );
        }),
    }),
    Effect.provideService(
      HttpClient.HttpClient,
      options?.http ??
        HttpClient.make(() => Effect.die("Recovery must not make an extra descriptor request.")),
    ),
  );
  const start = (scope?: Scope.Scope) => {
    const make = EnvironmentSupervisor.make(entry, {
      initiallyDesired: true,
      betterRouteEntryOwner: {
        withCurrentEntry: (use) =>
          Effect.gen(function* () {
            ownerCalls += 1;
            if (holdConsumption && ownerCalls === 3) {
              yield* Deferred.succeed(consuming, undefined);
              yield* Deferred.await(consumeGate);
            }
            return yield* lock.withPermits(1)(use(() => authoritative));
          }),
      },
    }).pipe(
      Effect.provideService(ConnectionDriver.ConnectionDriver, driver),
      Effect.provideService(Connectivity.Connectivity, {
        status: Effect.succeed("online" as const),
        changes: Stream.never,
      }),
      Effect.provideService(ConnectionWakeups.ConnectionWakeups, {
        changes: SubscriptionRef.changes(wakeups).pipe(
          Stream.drop(1),
          Stream.map(() => "network-changed" as const),
        ),
      }),
      Effect.provideService(CloudSession, {
        identity: Ref.get(identity),
        clerkToken: Effect.die("No token read in this test."),
      }),
    );
    return scope === undefined ? make : Scope.provide(make, scope);
  };
  return {
    start,
    driver,
    identity,
    sockets,
    prepared,
    released,
    preflightStarted,
    consuming,
    cancelled: () => cancelled,
    ownerCalls: () => ownerCalls,
    restore: (options?: {
      preparation?: Effect.Effect<void, ConnectionAttemptError>;
      holdConsumption?: boolean;
      mismatch?: boolean;
    }) => {
      restored = true;
      nextPreparation = options?.preparation ?? Effect.void;
      holdConsumption = options?.holdConsumption ?? false;
      mismatch = options?.mismatch ?? false;
    },
    replaceEntry: (next: ConnectionCatalogEntry | undefined) =>
      lock.withPermits(1)(
        Effect.sync(() => {
          authoritative = next;
        }),
      ),
    wake: SubscriptionRef.update(wakeups, (n) => n + 1),
    releaseConsumption: Deferred.succeed(consumeGate, undefined),
    closeFallback: Effect.suspend(() => Deferred.fail(closures[0]!, unavailable())),
  };
});

type Harness = Effect.Success<ReturnType<typeof makeHarness>>;
const connected = (
  supervisor: EnvironmentSupervisor.EnvironmentSupervisor["Service"],
  generation = 1,
) =>
  SubscriptionRef.changes(supervisor.state).pipe(
    Stream.filter((state) => state.phase === "connected" && state.generation === generation),
    Stream.runHead,
    Effect.asVoid,
  );
const unchanged = (
  harness: Harness,
  supervisor: EnvironmentSupervisor.EnvironmentSupervisor["Service"],
  session: RpcSession.RpcSession,
) => {
  expect(Option.getOrThrow(SubscriptionRef.getUnsafe(supervisor.session))).toBe(session);
  expect(SubscriptionRef.getUnsafe(supervisor.state).phase).toBe("connected");
  expect(SubscriptionRef.getUnsafe(supervisor.state).generation).toBe(1);
  expect(Option.getOrThrow(SubscriptionRef.getUnsafe(supervisor.prepared)).target).toBe(fallback);
  expect(harness.sockets).toHaveLength(1);
  expect(harness.released).toHaveLength(0);
};

describe("Connect preferred route recovery through the production driver and supervisor", () => {
  it.effect("prepares restored unchecked Connect exactly once without opening a socket", () =>
    Effect.gen(function* () {
      const h = yield* makeHarness();
      h.restore();
      expect(yield* h.driver.preflight(entry, connectionRoutes(entry)[0]!)).toBe(true);
      expect(h.prepared).toHaveLength(1);
      expect(h.sockets).toHaveLength(0);
    }),
  );

  it.effect(
    "recovers unchecked Connect with fresh switch preparation and no validation socket or descriptor",
    () =>
      Effect.gen(function* () {
        const h = yield* makeHarness();
        const supervisor = yield* h.start();
        yield* connected(supervisor);
        h.restore({ holdConsumption: true });
        yield* h.wake;
        yield* Deferred.await(h.consuming);
        expect(h.prepared).toHaveLength(2);
        expect(h.sockets).toHaveLength(1);
        yield* h.releaseConsumption;
        yield* connected(supervisor, 2);
        expect(h.prepared).toHaveLength(3);
        expect(h.sockets[1]).toBe(h.prepared[2]);
        expect(h.sockets[1]).not.toBe(h.prepared[1]);
        expect(h.sockets[1]!.target).toBe(preferred);
        expect(h.released).toEqual([h.sockets[0]]);
      }),
  );

  it.effect.each(["failure", "hung", "mismatch", "incompatible"] as const)(
    "preserves the fallback when Connect preflight is %s",
    (failure) =>
      Effect.gen(function* () {
        const h = yield* makeHarness();
        const supervisor = yield* h.start();
        yield* connected(supervisor);
        const session = Option.getOrThrow(yield* SubscriptionRef.get(supervisor.session));
        h.restore({
          mismatch: failure === "mismatch",
          preparation:
            failure === "hung"
              ? Effect.never
              : failure === "failure"
                ? Effect.fail(unavailable())
                : failure === "incompatible"
                  ? Effect.fail(
                      new ConnectionBlockedError({
                        reason: "unsupported",
                        detail: "Synthetic incompatible descriptor.",
                      }),
                    )
                  : Effect.void,
        });
        yield* h.wake;
        yield* Deferred.await(h.preflightStarted);
        if (failure === "hung") yield* TestClock.adjust("15 seconds");
        yield* drain;
        unchanged(h, supervisor, session);
        if (failure === "hung") expect(h.cancelled()).toBe(1);
      }),
  );

  const mutations = [
    "account",
    "same-account-relogin",
    "catalog",
    "profile",
    "order",
    "route-removed",
    "remove-recreate",
    "disabled",
    "ineligible",
  ] as const;
  const recoveryChanges = mutations.flatMap((mutation) =>
    (["during preparation", "after enqueue"] as const).map((when) => ({ mutation, when })),
  );
  it.effect.each(recoveryChanges)(
    "rejects $mutation changes $when without changing fallback or preferred selection",
    ({ mutation, when }) =>
      Effect.gen(function* () {
        const gate = yield* Deferred.make<void>();
        const h = yield* makeHarness();
        const supervisor = yield* h.start();
        yield* connected(supervisor);
        const session = Option.getOrThrow(yield* SubscriptionRef.get(supervisor.session));
        h.restore({
          preparation: when === "during preparation" ? Deferred.await(gate) : Effect.void,
          holdConsumption: when === "after enqueue",
        });
        yield* h.wake;
        yield* Deferred.await(when === "during preparation" ? h.preflightStarted : h.consuming);
        if (mutation === "account" || mutation === "same-account-relogin") {
          yield* Ref.set(h.identity, Option.none());
          yield* Ref.set(
            h.identity,
            Option.some({ accountId: mutation === "account" ? "account-b" : "account-a" }),
          );
        } else {
          const routes = connectionRoutes(entry);
          const changed =
            mutation === "order"
              ? entryWithRoutes(entry, routes.toReversed())
              : mutation === "route-removed"
                ? entryWithRoutes(entry, [routes[1]!])
                : mutation === "disabled"
                  ? { ...entry, enabled: false }
                  : mutation === "ineligible"
                    ? { ...entry, unsupportedReason: "Synthetic incompatible catalog." }
                    : mutation === "profile"
                      ? {
                          ...entry,
                          alternateRoutes: [
                            {
                              target: fallback,
                              profile: Option.some(
                                new BearerConnectionProfile({
                                  connectionId: fallback.connectionId,
                                  environmentId,
                                  label: fallback.label,
                                  httpBaseUrl: "https://changed.example.test/",
                                  wsBaseUrl: "wss://changed.example.test/",
                                }),
                              ),
                            },
                          ],
                        }
                      : { ...entry };
          if (mutation === "remove-recreate") yield* h.replaceEntry(undefined);
          yield* h.replaceEntry(changed);
        }
        yield* Deferred.succeed(gate, undefined);
        yield* h.releaseConsumption;
        yield* drain;
        unchanged(h, supervisor, session);
        // A later independent replacement still follows catalog order, proving
        // the stale signal did not leave a hidden preferred-route override.
        h.restore({ preparation: Effect.fail(unavailable()) });
        yield* h.closeFallback;
        yield* drain;
        yield* TestClock.adjust("2 seconds");
        yield* connected(supervisor, 2);
        expect(h.sockets[1]!.target).toBe(fallback);
      }),
  );

  it.effect("keeps the account epoch valid across ordinary token refresh", () =>
    Effect.gen(function* () {
      const h = yield* makeHarness();
      const supervisor = yield* h.start();
      yield* connected(supervisor);
      h.restore({ holdConsumption: true });
      yield* h.wake;
      yield* Deferred.await(h.consuming);
      const sameIdentity = Option.getOrThrow(yield* Ref.get(h.identity));
      yield* Ref.set(h.identity, Option.some(sameIdentity));
      yield* h.releaseConsumption;
      yield* connected(supervisor, 2);
      expect(h.sockets[1]!.target).toBe(preferred);
    }),
  );

  it.effect("requires a present Connect identity", () =>
    Effect.gen(function* () {
      const h = yield* makeHarness();
      const supervisor = yield* h.start();
      yield* connected(supervisor);
      const session = Option.getOrThrow(yield* SubscriptionRef.get(supervisor.session));
      yield* Ref.set(h.identity, Option.none());
      h.restore();
      yield* h.wake;
      yield* drain;
      unchanged(h, supervisor, session);
      expect(h.prepared).toHaveLength(1);
    }),
  );

  it.effect.each(["during preparation", "after enqueue"] as const)(
    "does not apply an old lease observation to its replacement %s",
    (when) =>
      Effect.gen(function* () {
        const gate = yield* Deferred.make<void>();
        const h = yield* makeHarness();
        const supervisor = yield* h.start();
        yield* connected(supervisor);
        h.restore({
          preparation: when === "during preparation" ? Deferred.await(gate) : Effect.void,
          holdConsumption: when === "after enqueue",
        });
        yield* h.wake;
        yield* Deferred.await(when === "during preparation" ? h.preflightStarted : h.consuming);
        h.restore({ preparation: Effect.fail(unavailable()) });
        yield* h.closeFallback;
        yield* drain;
        yield* TestClock.adjust("2 seconds");
        yield* connected(supervisor, 2);
        const replacement = Option.getOrThrow(yield* SubscriptionRef.get(supervisor.session));
        yield* Deferred.succeed(gate, undefined);
        yield* h.releaseConsumption;
        yield* drain;
        expect(Option.getOrThrow(yield* SubscriptionRef.get(supervisor.session))).toBe(replacement);
        expect(SubscriptionRef.getUnsafe(supervisor.state).generation).toBe(2);
        expect(h.sockets).toHaveLength(2);
      }),
  );

  it.effect.each(["during preparation", "after enqueue"] as const)(
    "cancels old recovery when a supervisor is recreated with the same numeric generation %s",
    (when) =>
      Effect.gen(function* () {
        const h = yield* makeHarness();
        const parentScope = yield* Scope.Scope;
        const oldScope = yield* Scope.fork(parentScope);
        const old = yield* h.start(oldScope);
        yield* connected(old);
        h.restore({
          preparation: when === "during preparation" ? Effect.never : Effect.void,
          holdConsumption: when === "after enqueue",
        });
        yield* h.wake;
        yield* Deferred.await(when === "during preparation" ? h.preflightStarted : h.consuming);
        yield* Scope.close(oldScope, Exit.void);
        if (when === "during preparation") expect(h.cancelled()).toBe(1);
        h.restore({ preparation: Effect.fail(unavailable()) });
        const replacement = yield* h.start();
        yield* connected(replacement);
        yield* h.releaseConsumption;
        yield* drain;
        expect(SubscriptionRef.getUnsafe(old.state).generation).toBe(1);
        expect(SubscriptionRef.getUnsafe(replacement.state).generation).toBe(1);
        expect(h.sockets).toHaveLength(2);
        expect(Option.getOrThrow(yield* SubscriptionRef.get(replacement.prepared)).target).toBe(
          fallback,
        );
      }),
  );

  it.effect("preserves the five-minute cooldown after a failed actual switch", () =>
    Effect.gen(function* () {
      const h = yield* makeHarness();
      const supervisor = yield* h.start();
      yield* connected(supervisor);
      h.restore({ holdConsumption: true });
      yield* h.wake;
      yield* Deferred.await(h.consuming);
      h.restore({ preparation: Effect.fail(unavailable()) });
      yield* h.releaseConsumption;
      yield* connected(supervisor, 2);
      const preparedCount = h.prepared.length;
      h.restore();
      yield* h.wake;
      yield* drain;
      expect(h.prepared).toHaveLength(preparedCount);
      expect(h.sockets).toHaveLength(2);
      yield* TestClock.adjust("5 minutes");
      yield* h.wake;
      yield* connected(supervisor, 3);
      expect(h.sockets[2]!.target).toBe(preferred);
    }),
  );

  it.effect("retains scoped preflight cancellation", () =>
    Effect.gen(function* () {
      const h = yield* makeHarness();
      h.restore({ preparation: Effect.never });
      const fiber = yield* Effect.forkChild(h.driver.preflight(entry, connectionRoutes(entry)[0]!));
      yield* Deferred.await(h.preflightStarted);
      yield* Fiber.interrupt(fiber);
      expect(h.cancelled()).toBe(1);
      expect(h.sockets).toHaveLength(0);
    }),
  );

  it.effect("does not admit unchecked SSH or cleartext credential routes", () =>
    Effect.gen(function* () {
      const h = yield* makeHarness();
      h.restore();
      expect(yield* h.driver.preflight(entry, connectionRoutes(entry)[1]!)).toBe(false);
      const secureTarget = new BearerConnectionTarget({
        environmentId,
        label: preferred.label,
        connectionId: "paired",
      });
      const clearTarget = new BearerConnectionTarget({
        environmentId,
        label: preferred.label,
        connectionId: "learned:clear@paired",
      });
      const profile = (connectionId: string, httpBaseUrl: string) =>
        Option.some(
          new BearerConnectionProfile({
            connectionId,
            environmentId,
            label: preferred.label,
            httpBaseUrl,
            wsBaseUrl: "wss://synthetic.example.test/",
          }),
        );
      const clearEntry: ConnectionCatalogEntry = {
        target: secureTarget,
        profile: profile("paired", "https://secure.example.test/"),
        alternateRoutes: [
          {
            target: clearTarget,
            profile: profile(clearTarget.connectionId, "http://192.168.1.12/"),
          },
        ],
        enabled: true,
      };
      expect(yield* h.driver.preflight(clearEntry, connectionRoutes(clearEntry)[1]!)).toBe(false);
      expect(h.prepared).toHaveLength(0);
      expect(h.sockets).toHaveLength(0);
    }),
  );

  it.effect(
    "preserves silent refusal and answered preparation with one total preflight deadline",
    () =>
      Effect.gen(function* () {
        let descriptorId = EnvironmentId.make("different-environment");
        let requests = 0;
        const h = yield* makeHarness({
          http: HttpClient.make((request) =>
            Effect.sleep("2 seconds").pipe(
              Effect.andThen(
                Effect.sync(() => {
                  requests += 1;
                  return HttpClientResponse.fromWeb(
                    request,
                    Response.json({
                      environmentId: descriptorId,
                      label: preferred.label,
                      platform: { os: "linux", arch: "x64" },
                      serverVersion: "0.0.0-test",
                      orchestrationProtocolVersion: ORCHESTRATION_PROTOCOL_VERSION,
                      capabilities: { repositoryIdentity: true },
                    }),
                  );
                }),
              ),
            ),
          ),
        });
        const publicRoute = {
          target: new BearerConnectionTarget({
            environmentId,
            label: preferred.label,
            connectionId: "public",
          }),
          profile: Option.some(
            new BearerConnectionProfile({
              connectionId: "descriptor-only",
              environmentId,
              label: preferred.label,
              httpBaseUrl: "https://public.example.test/",
              wsBaseUrl: "wss://public.example.test/",
            }),
          ),
        };
        h.restore();
        const silent = yield* Effect.forkChild(h.driver.preflight(entry, publicRoute));
        yield* TestClock.adjust("2 seconds");
        expect(yield* Fiber.join(silent)).toBe(false);
        expect(h.prepared).toHaveLength(0);
        descriptorId = environmentId;
        const answered = yield* Effect.forkChild(h.driver.preflight(entry, publicRoute));
        yield* TestClock.adjust("2 seconds");
        expect(yield* Fiber.join(answered)).toBe(true);
        expect(h.prepared).toHaveLength(1);
        h.restore({ preparation: Effect.never });
        const bounded = yield* Effect.forkChild(h.driver.preflight(entry, publicRoute));
        yield* TestClock.adjust("2 seconds");
        yield* drain;
        yield* TestClock.adjust("13 seconds");
        expect(yield* Fiber.join(bounded)).toBe(false);
        expect(h.cancelled()).toBe(1);
        expect(requests).toBe(3);
        expect(h.sockets).toHaveLength(0);
      }),
  );
});

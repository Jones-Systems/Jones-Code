import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import type { ConnectionCatalogEntry, ConnectionRoute } from "../../connection/catalog.ts";
import type { EnvironmentConnectionLease, RouteCheck } from "../../connection/driver.ts";
import type { ConnectionAttemptError, PreparedConnection } from "../../connection/model.ts";
import {
  connectionRouteId,
  connectionRoutes,
  routeEntry,
  sendsTokenInClear,
  usableRoutes,
} from "../../connection/routes.ts";
import type { CloudSessionIdentity } from "../../platform/capabilities.ts";

export interface BetterRouteEntryOwner {
  readonly withCurrentEntry: <A>(
    use: (read: () => ConnectionCatalogEntry | undefined) => Effect.Effect<A>,
  ) => Effect.Effect<A>;
}

export interface BetterRouteObservation {
  readonly lease: EnvironmentConnectionLease;
  readonly entry: ConnectionCatalogEntry;
  readonly route: ConnectionRoute;
  readonly identity: Option.Option<CloudSessionIdentity>;
}

// ConnectionDriver.preflight owns the span for this operation.
export const preflightBetterRoute = Effect.fnUntraced(
  function* (
    entry: ConnectionCatalogEntry,
    route: ConnectionRoute,
    check: Effect.Effect<RouteCheck>,
    prepare: (
      entry: ConnectionCatalogEntry,
    ) => Effect.Effect<PreparedConnection, ConnectionAttemptError>,
  ) {
    if (sendsTokenInClear(connectionRoutes(entry), route)) return false;
    const result = yield* check;
    if (
      result !== "answered" &&
      !(result === "unchecked" && route.target._tag === "RelayConnectionTarget")
    )
      return false;
    const prepared = yield* prepare(routeEntry(entry, route));
    return prepared.environmentId === entry.target.environmentId;
  },
  Effect.orElseSucceed(() => false),
  Effect.timeoutOption("15 seconds"),
  Effect.map(Option.getOrElse(() => false)),
);

export function makeBetterRouteRecovery(input: {
  readonly owner: BetterRouteEntryOwner;
  readonly identity: Effect.Effect<Option.Option<CloudSessionIdentity>>;
  readonly readState: () => {
    readonly lease: EnvironmentConnectionLease | undefined;
    readonly desired: boolean;
    readonly cooldowns: ReadonlyMap<string, bigint>;
  };
  readonly preflight: (
    entry: ConnectionCatalogEntry,
    route: ConnectionRoute,
  ) => Effect.Effect<boolean>;
  readonly offer: (observation: BetterRouteObservation) => void;
  readonly select: (routeId: string) => void;
}) {
  const current = (
    observation: BetterRouteObservation,
    entry: ConnectionCatalogEntry | undefined,
    identity: Option.Option<CloudSessionIdentity>,
    now: bigint,
  ) => {
    const state = input.readState();
    if (
      state.lease !== observation.lease ||
      !state.desired ||
      entry === undefined ||
      entry !== observation.entry
    ) {
      return false;
    }
    if (
      !entry.enabled ||
      entry.unsupportedReason !== undefined ||
      entry.serverUpdateRequired === true
    )
      return false;
    if (Option.getOrUndefined(identity) !== Option.getOrUndefined(observation.identity)) {
      return false;
    }
    if (observation.route.target._tag === "RelayConnectionTarget" && Option.isNone(identity)) {
      return false;
    }
    const routes = usableRoutes(entry);
    const candidate = routes.findIndex(
      (route) =>
        route.target === observation.route.target && route.profile === observation.route.profile,
    );
    const fallback = routes.findIndex(
      (route) =>
        connectionRouteId(route.target) === connectionRouteId(observation.lease.prepared.target),
    );
    return (
      candidate >= 0 &&
      candidate < fallback &&
      (state.cooldowns.get(connectionRouteId(observation.route.target)) ?? 0n) <= now
    );
  };

  const commit = (observation: BetterRouteObservation, action: () => void) =>
    input.owner.withCurrentEntry((read) =>
      Effect.gen(function* () {
        const now = yield* Clock.monotonicTimeNanos;
        const identity = yield* input.identity;
        // Guard acquisition and identity reads finish before this synchronous
        // comparison/selection; the catalog lock never covers preflight I/O.
        return yield* Effect.sync(() => {
          if (!current(observation, read(), identity, now)) return false;
          action();
          return true;
        });
      }),
    );

  const check = Effect.fn("BetterRouteRecovery.check")(function* (
    lease: EnvironmentConnectionLease,
  ) {
    const captured = yield* input.owner.withCurrentEntry((read) =>
      Effect.gen(function* () {
        const identity = yield* input.identity;
        return yield* Effect.sync(() => ({ entry: read(), identity }));
      }),
    );
    const entry = captured.entry;
    if (entry === undefined) return;
    const now = yield* Clock.monotonicTimeNanos;
    const observations = usableRoutes(entry)
      .map((route): BetterRouteObservation => ({
        lease,
        entry,
        route,
        identity: captured.identity,
      }))
      .filter((observation) => current(observation, entry, captured.identity, now));
    const passed = yield* Effect.forEach(
      observations,
      (observation) => input.preflight(observation.entry, observation.route),
      { concurrency: "unbounded" },
    );
    const observation = observations[passed.indexOf(true)];
    if (observation !== undefined) yield* commit(observation, () => input.offer(observation));
  });

  const consume = (observation: BetterRouteObservation, lease: EnvironmentConnectionLease) =>
    lease !== observation.lease
      ? Effect.succeed(false)
      : commit(observation, () => input.select(connectionRouteId(observation.route.target)));

  return { check, consume };
}

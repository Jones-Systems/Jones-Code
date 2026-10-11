import type {
  JonesUpdateState,
  JonesUpdateDownloadInput,
  JonesUpdateInstallInput,
} from "@t3tools/contracts/jones/jonesUpdates";
import * as Schema from "effect/Schema";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Random from "effect/Random";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { AsyncResult, Atom } from "effect/reactivity";
import { HttpClientError, type HttpClient } from "effect/http";
import { EnvironmentRegistry } from "../../connection/registry.ts";
import { EnvironmentSupervisor } from "../../connection/supervisor.ts";
import { ManagedRelayDpopSigner } from "../../relay/managedRelay.ts";
import { RemoteEnvironmentAuthorization } from "../../authorization/service.ts";
import { environmentEndpointUrl } from "../../environment/endpoint.ts";
import { executeAuthenticatedEnvironmentHttpRequest } from "../../state/environmentHttpAuth.ts";
import {
  createEnvironmentCommand,
  isAtomCommandInterrupted,
  type AtomCommandResult,
  createEnvironmentSubscriptionAtomFamily,
} from "../../state/runtime.ts";

export class JonesUpdateDisconnectedError extends Schema.TaggedError<JonesUpdateDisconnectedError>()(
  "JonesUpdateDisconnectedError",
  {},
) {}

type JonesAction =
  | { action: "check" }
  | { action: "download"; input: JonesUpdateDownloadInput }
  | { action: "install"; input: JonesUpdateInstallInput };

const request = Effect.fn("clientRuntime.jonesUpdates.request")(function* (
  input: JonesAction | { action: "state"; after?: number },
) {
  const supervisor = yield* EnvironmentSupervisor;
  const prepared = yield* SubscriptionRef.get(supervisor.prepared);
  if (Option.isNone(prepared)) return yield* new JonesUpdateDisconnectedError({});
  const signer = yield* Effect.serviceOption(ManagedRelayDpopSigner);
  const remoteAuthorization = yield* Effect.serviceOption(RemoteEnvironmentAuthorization);
  const suffix = input.action === "state" ? "" : `/${input.action}`;
  return yield* executeAuthenticatedEnvironmentHttpRequest({
    prepared: prepared.value,
    signer,
    remoteAuthorization,
    group: "jonesUpdates",
    method: input.action === "state" ? "GET" : "POST",
    url: (base) =>
      environmentEndpointUrl(
        base,
        `/api/jones-updates${suffix}${input.action === "state" && input.after !== undefined ? `?after=${input.after}` : ""}`,
      ),
    timeoutMs: input.action === "state" ? 35_000 : 20 * 60_000,
    request: ({ client, headers }) => {
      switch (input.action) {
        case "state":
          return client.state({
            headers,
            query: input.after === undefined ? {} : { after: input.after },
          });
        case "check":
          return client.check({ headers });
        case "download":
          return client.download({ headers, payload: input.input });
        case "install":
          return client.install({ headers, payload: input.input });
      }
    },
  });
});

export interface JonesUpdateObservation {
  readonly state: JonesUpdateState | null;
  readonly freshness: "loading" | "fresh" | "stale" | "unsupported";
  readonly message?: string;
}

function readFailure(error: unknown): "unsupported" | "retry" | "wait" {
  if (typeof error !== "object" || error === null || !("_tag" in error)) return "wait";
  if (error._tag === "EnvironmentResourceNotFoundError") return "unsupported";
  if (error._tag === "RemoteEnvironmentAuthUndeclaredStatusError" && "status" in error) {
    if (error.status === 404) return "unsupported";
    if (typeof error.status === "number" &&
      (error.status === 408 || error.status === 429 || error.status >= 500)) return "retry";
  }
  if (error._tag === "RemoteEnvironmentAuthFetchError") {
    return "cause" in error && HttpClientError.isHttpClientError(error.cause) &&
      error.cause.response === undefined ? "retry" : "wait";
  }
  return ["RemoteEnvironmentAuthTimeoutError", "EnvironmentInternalError"]
    .includes(String(error._tag)) ? "retry" : "wait";
}

/** This subscription owns GET retries only; mutations and transport reconnection remain single-owner. */
export function observeJonesUpdateSnapshot<A, E, R>(
  connections: Stream.Stream<Option.Option<A>>,
  read: (after?: number) => Effect.Effect<JonesUpdateState | null, E, R>,
  retryDelay: (failure: number) => Effect.Effect<void> = (failure) =>
    Random.nextBetween(0.5, 1).pipe(
      Effect.flatMap((jitter) => Effect.sleep(Math.min(300_000, 1000 * 2 ** Math.min(failure - 1, 9)) * jitter)),
    ),
): Stream.Stream<JonesUpdateObservation, never, R> {
  return Stream.suspend(() => {
    let latest: JonesUpdateState | null = null;
    let latestIsFresh = false;
    type Cursor = { after?: number; failures: number; wait?: "retry" | "wait"; done?: boolean };
    return connections.pipe(Stream.switchMap((connection) => {
      if (Option.isNone(connection)) {
        latestIsFresh = false;
        return Stream.succeed<JonesUpdateObservation>({
        state: latest, freshness: latest === null ? "loading" : "stale",
        message: "Update status is waiting for a connection.",
      });
      }
      const replacingFreshConnection = latestIsFresh;
      latestIsFresh = false;
      const reads = Stream.unfold({ failures: 0 } as Cursor, (cursor): Effect.Effect<readonly [JonesUpdateObservation, Cursor] | undefined, never, R> =>
        Effect.gen(function* () {
          if (cursor.done) return undefined;
          if (cursor.wait === "wait") return yield* Effect.never;
          if (cursor.wait === "retry") yield* retryDelay(cursor.failures);
          return yield* read(cursor.after).pipe(
            Effect.map((state): readonly [JonesUpdateObservation, Cursor] => {
              latest = state;
              latestIsFresh = state !== null;
              return [{ state, freshness: state === null ? "unsupported" : "fresh" }, {
                ...(state?.revision === undefined ? {} : { after: state.revision }),
                failures: 0, done: state === null,
              }];
            }),
            Effect.catch((error) => {
              latestIsFresh = false;
              const disposition = readFailure(error);
              if (disposition === "unsupported") latest = null;
              return Effect.succeed<readonly [JonesUpdateObservation, Cursor]>([{
                state: latest,
                freshness: disposition === "unsupported" ? "unsupported" : "stale",
                ...(disposition === "unsupported" ? {} : { message: disposition === "retry"
                  ? "Update status is temporarily unavailable; retrying."
                  : "Update status is unavailable. Check this connection or sign in again." }),
              }, { ...cursor, failures: cursor.failures + 1,
                ...(disposition === "unsupported" ? { done: true } : { wait: disposition }),
              }]);
            }),
          );
        }),
      );
      return replacingFreshConnection ? Stream.succeed<JonesUpdateObservation>({
        state: latest, freshness: "stale", message: "Refreshing update status for the new connection.",
      }).pipe(Stream.concat(reads)) : reads;
    }));
  });
}

/** Compatibility projection for callers that only display host state. */
export function observeJonesUpdateState<A, E, R>(
  connections: Stream.Stream<Option.Option<A>>,
  read: (after?: number) => Effect.Effect<JonesUpdateState | null, E, R>,
) {
  return observeJonesUpdateSnapshot(connections, read).pipe(Stream.map((snapshot) => snapshot.freshness === "fresh" ? snapshot.state : null));
}

/** Observe host-owned state; mounting another client never starts another GitHub checker. */
export function createJonesUpdateAtoms<R, E>(
  runtime: Atom.AtomRuntime<EnvironmentRegistry | HttpClient.HttpClient | R, E>,
) {
  const subscription = createEnvironmentSubscriptionAtomFamily(runtime, {
    label: "environment:jones-updates",
    subscribe: () =>
      Stream.unwrap(
        EnvironmentSupervisor.pipe(
          Effect.map((supervisor) =>
            observeJonesUpdateSnapshot(SubscriptionRef.changes(supervisor.prepared), (after) =>
              request({ action: "state", ...(after === undefined ? {} : { after }) }),
            ),
          ),
        ),
      ),
  });
  const observation = Atom.family((environmentId: import("@t3tools/contracts").EnvironmentId) =>
    Atom.make((get): JonesUpdateObservation =>
      Option.getOrElse(AsyncResult.value(get(subscription({ environmentId, input: {} }))),
        () => ({ state: null, freshness: "loading" } as const)),
    ),
  );
  const value = Atom.family((environmentId: import("@t3tools/contracts").EnvironmentId) =>
    Atom.make((get) => get(observation(environmentId)).state),
  );
  const action = createEnvironmentCommand(runtime, {
    label: "environment:jones-update-action",
    concurrency: { mode: "serial", key: ({ environmentId }) => environmentId },
    execute: (input: JonesAction) => request(input),
  });
  return { value, observation, action };
}

export function jonesUpdatePresentation(state: JonesUpdateState) {
  const busy = ["checking", "downloading", "verifying", "preparing", "installing"].includes(
    state.phase,
  );
  const message =
    state.phase === "installing"
      ? "Installing — server restarting. Waiting for the launcher outcome."
      : (state.message ?? state.phase);
  const outcome = state.outcome;
  const outcomeMessage =
    outcome === undefined
      ? undefined
      : `${outcome.status === "committed" ? "Installed" : outcome.status === "rolled-back" ? "Rolled back" : "Blocked"}: ${outcome.fromVersion} → ${outcome.targetVersion}${outcome.reason ? ` · ${outcome.reason}` : ""}`;
  return { busy, message, outcomeMessage };
}

export function jonesUpdateActionError(result: AtomCommandResult<JonesUpdateState | null, unknown>): string | null {
  if (result._tag === "Failure") return isAtomCommandInterrupted(result) ? null
    : "The update request failed. Check the connection and try again.";
  const state = result.value;
  if (state === null) return "This host does not support Jones updates.";
  return ["error", "blocked", "rolled-back"].includes(state.phase)
    ? state.message ?? `The update is ${state.phase}.` : null;
}

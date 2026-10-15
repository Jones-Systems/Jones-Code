import type {
  JonesUpdateProvenance,
  JonesUpdateState,
  JonesUpdateDownloadInput,
  JonesUpdateInstallInput,
} from "@t3tools/contracts/jones/jonesUpdates";
import * as Schema from "effect/Schema";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { AsyncResult, Atom } from "effect/reactivity";
import type { HttpClient } from "effect/http";
import { EnvironmentRegistry } from "../../connection/registry.ts";
import { EnvironmentSupervisor } from "../../connection/supervisor.ts";
import { ManagedRelayDpopSigner } from "../../relay/managedRelay.ts";
import { RemoteEnvironmentAuthorization } from "../../authorization/service.ts";
import { executeAuthenticatedEnvironmentHttpRequest } from "../../state/environmentHttpAuth.ts";
import {
  createEnvironmentCommand,
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
  return yield* executeAuthenticatedEnvironmentHttpRequest({
    prepared: prepared.value,
    signer,
    remoteAuthorization,
    group: "jonesUpdates",
    method: input.action === "state" ? "GET" : "POST",
    url: (urls) =>
      input.action === "state"
        ? urls.state({ query: input.after === undefined ? {} : { after: input.after } })
        : urls[input.action](),
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

/** Wait for a prepared connection; an initial disconnected mount must not end observation. */
export function observeJonesUpdateState<A, E, R>(
  connections: Stream.Stream<Option.Option<A>>,
  read: (after?: number) => Effect.Effect<JonesUpdateState | null, E, R>,
) {
  return connections.pipe(
    Stream.switchMap((connection) =>
      Option.isNone(connection)
        ? Stream.succeed(null)
        : Stream.unfold(undefined as number | undefined, (after) =>
            read(after).pipe(Effect.map((state) => [state, state?.revision] as const)),
          ).pipe(
            Stream.takeUntil((state) => state === null),
            Stream.catch(() => Stream.succeed(null)),
          ),
    ),
  );
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
            observeJonesUpdateState(SubscriptionRef.changes(supervisor.prepared), (after) =>
              request({ action: "state", ...(after === undefined ? {} : { after }) }),
            ),
          ),
        ),
      ),
  });
  const value = Atom.family((environmentId: import("@t3tools/contracts").EnvironmentId) =>
    Atom.make((get): JonesUpdateState | null =>
      Option.getOrNull(AsyncResult.value(get(subscription({ environmentId, input: {} })))),
    ),
  );
  const action = createEnvironmentCommand(runtime, {
    label: "environment:jones-update-action",
    concurrency: { mode: "serial", key: ({ environmentId }) => environmentId },
    execute: (input: JonesAction) => request(input),
  });
  return { value, action };
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

/** GitHub's abbreviation; callers keep the full commit reachable. */
export function shortJonesSource(sha: string) {
  return sha.slice(0, 7);
}

/** Native update IDs can be full hashes; callers keep the full ID reachable. */
export function shortJonesUpdateId(id: string) {
  return id.length > 12 ? `${id.slice(0, 8)}…` : id;
}

/** The host's offered or staged build. Hosts that predate offered versions omit `version`. */
export function jonesCandidatePresentation(state: JonesUpdateState) {
  const provenance = state.provenance;
  if (
    provenance === undefined ||
    (state.stagedHandle === undefined &&
      state.phase !== "available" &&
      state.phase !== "downloading" &&
      state.phase !== "verifying")
  )
    return undefined;
  return {
    label:
      state.stagedHandle !== undefined
        ? "Downloaded"
        : state.phase === "available"
          ? "Available"
          : "Offered",
    version: provenance.version,
    sourceSha: provenance.sourceSha,
  };
}

export type JonesSourceComparison = "matches" | "different" | "unavailable";

const FULL_SOURCE_COMMIT = /^[a-f0-9]{40}$/;

/**
 * Compares a build with an installed source its host observed. Only full commits from the same
 * repository compare: each platform workflow builds one source under its own run, so versions
 * and run numbers neither establish nor rule out a match.
 */
export function compareJonesSource(
  installed: { readonly repository: string; readonly sha: string } | undefined,
  candidate: JonesUpdateProvenance | undefined,
): JonesSourceComparison {
  if (
    installed === undefined ||
    candidate === undefined ||
    installed.repository !== candidate.repository ||
    !FULL_SOURCE_COMMIT.test(installed.sha) ||
    !FULL_SOURCE_COMMIT.test(candidate.sourceSha)
  )
    return "unavailable";
  return installed.sha === candidate.sourceSha ? "matches" : "different";
}

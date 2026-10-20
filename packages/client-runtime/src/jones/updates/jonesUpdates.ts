import type { EnvironmentId } from "@t3tools/contracts";
import type {
  JonesUpdateProvenance,
  JonesUpdateState,
  JonesUpdateDownloadInput,
  JonesUpdateExactInput,
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

export type JonesAction =
  | { action: "check" }
  | { action: "download"; input: JonesUpdateDownloadInput }
  /** Hosts that predate exact selection answer 404; callers gate this on `capability.exactSource`. */
  | { action: "downloadExact"; input: JonesUpdateExactInput }
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
        case "downloadExact":
          return client.downloadExact({ headers, payload: input.input });
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
    if (
      typeof error.status === "number" &&
      (error.status === 408 || error.status === 429 || error.status >= 500)
    )
      return "retry";
  }
  if (error._tag === "RemoteEnvironmentAuthFetchError") {
    return "cause" in error &&
      HttpClientError.isHttpClientError(error.cause) &&
      error.cause.response === undefined
      ? "retry"
      : "wait";
  }
  return ["RemoteEnvironmentAuthTimeoutError", "EnvironmentInternalError"].includes(
    String(error._tag),
  )
    ? "retry"
    : "wait";
}

/** This subscription owns GET retries only; mutations and transport reconnection remain single-owner. */
export function observeJonesUpdateSnapshot<A, E, R>(
  connections: Stream.Stream<Option.Option<A>>,
  read: (after?: number) => Effect.Effect<JonesUpdateState | null, E, R>,
  retryDelay: (failure: number) => Effect.Effect<void> = (failure) =>
    Random.nextBetween(0.5, 1).pipe(
      Effect.flatMap((jitter) =>
        Effect.sleep(Math.min(300_000, 1000 * 2 ** Math.min(failure - 1, 9)) * jitter),
      ),
    ),
): Stream.Stream<JonesUpdateObservation, never, R> {
  return Stream.suspend(() => {
    let latest: JonesUpdateState | null = null;
    let latestIsFresh = false;
    type Cursor = { after?: number; failures: number; wait?: "retry" | "wait"; done?: boolean };
    return connections.pipe(
      Stream.switchMap((connection) => {
        if (Option.isNone(connection)) {
          latestIsFresh = false;
          return Stream.succeed<JonesUpdateObservation>({
            state: latest,
            freshness: latest === null ? "loading" : "stale",
            message: "Update status is waiting for a connection.",
          });
        }
        const replacingFreshConnection = latestIsFresh;
        latestIsFresh = false;
        const reads = Stream.unfold(
          { failures: 0 } as Cursor,
          (
            cursor,
          ): Effect.Effect<readonly [JonesUpdateObservation, Cursor] | undefined, never, R> =>
            Effect.gen(function* () {
              if (cursor.done) return undefined;
              if (cursor.wait === "wait") return yield* Effect.never;
              if (cursor.wait === "retry") yield* retryDelay(cursor.failures);
              return yield* read(cursor.after).pipe(
                Effect.map((state): readonly [JonesUpdateObservation, Cursor] => {
                  latest = state;
                  latestIsFresh = state !== null;
                  return [
                    { state, freshness: state === null ? "unsupported" : "fresh" },
                    {
                      ...(state?.revision === undefined ? {} : { after: state.revision }),
                      failures: 0,
                      done: state === null,
                    },
                  ];
                }),
                Effect.catch((error) => {
                  latestIsFresh = false;
                  const disposition = readFailure(error);
                  if (disposition === "unsupported") latest = null;
                  return Effect.succeed<readonly [JonesUpdateObservation, Cursor]>([
                    {
                      state: latest,
                      freshness: disposition === "unsupported" ? "unsupported" : "stale",
                      ...(disposition === "unsupported"
                        ? {}
                        : {
                            message:
                              disposition === "retry"
                                ? "Update status is temporarily unavailable; retrying."
                                : "Update status is unavailable. Check this connection or sign in again.",
                          }),
                    },
                    {
                      ...cursor,
                      failures: cursor.failures + 1,
                      ...(disposition === "unsupported" ? { done: true } : { wait: disposition }),
                    },
                  ]);
                }),
              );
            }),
        );
        return replacingFreshConnection
          ? Stream.succeed<JonesUpdateObservation>({
              state: latest,
              freshness: "stale",
              message: "Refreshing update status for the new connection.",
            }).pipe(Stream.concat(reads))
          : reads;
      }),
    );
  });
}

/** Compatibility projection for callers that only display host state. */
export function observeJonesUpdateState<A, E, R>(
  connections: Stream.Stream<Option.Option<A>>,
  read: (after?: number) => Effect.Effect<JonesUpdateState | null, E, R>,
) {
  return observeJonesUpdateSnapshot(connections, read).pipe(
    Stream.map((snapshot) => (snapshot.freshness === "fresh" ? snapshot.state : null)),
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
            observeJonesUpdateSnapshot(SubscriptionRef.changes(supervisor.prepared), (after) =>
              request({ action: "state", ...(after === undefined ? {} : { after }) }),
            ),
          ),
        ),
      ),
  });
  const observation = Atom.family((environmentId: import("@t3tools/contracts").EnvironmentId) =>
    Atom.make((get): JonesUpdateObservation =>
      Option.getOrElse(
        AsyncResult.value(get(subscription({ environmentId, input: {} }))),
        () => ({ state: null, freshness: "loading" }) as const,
      ),
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

export function jonesUpdateActionError(
  result: AtomCommandResult<JonesUpdateState | null, unknown>,
): string | null {
  if (result._tag === "Failure")
    return isAtomCommandInterrupted(result)
      ? null
      : "The update request failed. Check the connection and try again.";
  const state = result.value;
  if (state === null) return "This host does not support Jones updates.";
  return ["error", "blocked", "rolled-back"].includes(state.phase)
    ? (state.message ?? `The update is ${state.phase}.`)
    : null;
}

/** GitHub's abbreviation; callers keep the full commit reachable. */
export function shortJonesSource(sha: string) {
  return sha.slice(0, 7);
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

/** What one host can do toward this app's exact source commit. */
export type JonesExactSourceStep =
  | { readonly kind: "download-exact"; readonly targetSource: string }
  | { readonly kind: "download"; readonly input: JonesUpdateDownloadInput }
  | { readonly kind: "install"; readonly input: JonesUpdateInstallInput };

export type JonesExactSourceUnavailable =
  | "invalid-target"
  | "waiting"
  | "unsupported"
  | "no-permission"
  | "already-installed"
  | "busy"
  | "reconciling"
  | "staged-other"
  | "offered-other"
  | "unavailable";

export type JonesExactSourceDecision =
  | { readonly status: "ready"; readonly step: JonesExactSourceStep }
  | {
      readonly status: "unavailable";
      readonly reason: JonesExactSourceUnavailable;
      readonly message: string;
    };

export interface JonesExactSourceInput {
  readonly environmentId: EnvironmentId;
  readonly observation: JonesUpdateObservation;
  /** The running app's own source commit. Never a desktop-staged, offered or latest build. */
  readonly targetSource: string | undefined;
  readonly canOperate: boolean;
  /** An install request whose response was lost; it blocks re-submission until the host moves on. */
  readonly unresolvedInstall?:
    | {
        readonly stagedHandle: string;
        readonly revision: number | undefined;
      }
    | undefined;
}

const unavailable = (
  reason: JonesExactSourceUnavailable,
  message: string,
): JonesExactSourceDecision => ({ status: "unavailable", reason, message });

function capabilityRefusal(state: JonesUpdateState, what: "download" | "install") {
  return state.capability.reason === "bootstrap-required"
    ? "Installing needs a qualified launcher on this host first."
    : `This host cannot ${what} updates right now.`;
}

/**
 * Chooses the one next step that moves a host toward this app's exact source, or says why none
 * exists. Sources compare as full commits: platform artifacts of one commit carry different
 * versions, and equal versions can hide different commits. Hosts that cannot select a source
 * may only download an offer that already is the target.
 */
export function resolveJonesExactSourceAction(
  input: JonesExactSourceInput,
): JonesExactSourceDecision {
  const { observation, targetSource } = input;
  if (targetSource === undefined || !FULL_SOURCE_COMMIT.test(targetSource))
    return unavailable("invalid-target", "This app does not report a full source commit.");
  const state = observation.state;
  if (observation.freshness === "unsupported" || (observation.freshness === "fresh" && !state))
    return unavailable("unsupported", "This host does not support Jones updates.");
  if (observation.freshness !== "fresh" || state === null)
    return unavailable(
      "waiting",
      observation.message ?? "Waiting for this host's current update status.",
    );
  if (!input.canOperate)
    return unavailable("no-permission", "Your session on this host cannot change updates.");
  const short = shortJonesSource(targetSource);
  if (state.installedSource?.sha === targetSource)
    return unavailable("already-installed", `This host already runs source ${short}.`);
  if (jonesUpdatePresentation(state).busy)
    return unavailable("busy", jonesUpdatePresentation(state).message);
  const pending = input.unresolvedInstall;
  if (
    pending !== undefined &&
    state.stagedHandle === pending.stagedHandle &&
    state.revision === pending.revision
  )
    return unavailable(
      "reconciling",
      "The install request was sent but its outcome is unknown. Waiting for the host to report it; it will not be sent again.",
    );
  const provenance = state.provenance;
  if (state.stagedHandle !== undefined) {
    if (provenance === undefined)
      return unavailable(
        "staged-other",
        "This host has a downloaded update with no reported source, so it will not be installed from this notice.",
      );
    if (provenance.sourceSha !== targetSource)
      return unavailable(
        "staged-other",
        `This host already has source ${shortJonesSource(provenance.sourceSha)} downloaded. It will not be installed from this notice.`,
      );
    if (!state.capability.install || state.currentVersion === undefined)
      return unavailable("unavailable", capabilityRefusal(state, "install"));
    return {
      status: "ready",
      step: {
        kind: "install",
        input: {
          stagedHandle: state.stagedHandle,
          environmentId: input.environmentId,
          currentVersion: state.currentVersion,
        },
      },
    };
  }
  if (!state.capability.download)
    return unavailable("unavailable", capabilityRefusal(state, "download"));
  if (state.capability.exactSource === true)
    return { status: "ready", step: { kind: "download-exact", targetSource } };
  if (
    provenance !== undefined &&
    state.phase === "available" &&
    provenance.sourceSha === targetSource
  )
    return {
      status: "ready",
      step: {
        kind: "download",
        input: { artifactId: provenance.artifactId, sourceSha: provenance.sourceSha },
      },
    };
  return unavailable(
    "offered-other",
    provenance !== undefined && state.phase === "available"
      ? `This host offers source ${shortJonesSource(provenance.sourceSha)} and cannot download a specific source. Update the host from its own settings.`
      : `This host cannot download a specific source (${short}). Update the host from its own settings.`,
  );
}

/**
 * Judges a completed exact-source request. A host that stages another commit than the target
 * is a refusal for this notice even when the host itself reports success.
 */
export function jonesExactSourceResultError(
  step: JonesExactSourceStep,
  result: AtomCommandResult<JonesUpdateState | null, unknown>,
  targetSource: string,
): string | null {
  const failure = jonesUpdateActionError(result);
  if (failure !== null || result._tag !== "Success" || step.kind === "install") return failure;
  const state = result.value;
  if (state === null) return failure;
  if (
    state.stagedHandle !== undefined &&
    state.provenance !== undefined &&
    state.provenance.sourceSha !== targetSource
  )
    return `The host reports source ${shortJonesSource(state.provenance.sourceSha)} downloaded, not ${shortJonesSource(targetSource)}. It will not be installed from this notice.`;
  // Accepted work is under way or done; any other phase means the host did not take the download.
  return ["downloading", "verifying", "staged"].includes(state.phase)
    ? null
    : (state.message ?? `The host did not start the download (${state.phase}).`);
}

import {
  executeAuthenticatedEnvironmentHttpRequest,
  RemoteEnvironmentAuthorization,
  type EnvironmentHttpAuthHeaders,
} from "@t3tools/client-runtime/authorization";
import { EnvironmentSupervisor, type PreparedConnection } from "@t3tools/client-runtime/connection";
import * as ManagedRelay from "@t3tools/client-runtime/relay";
import type {
  makeEnvironmentHttpApiGroupClient,
  makeEnvironmentHttpApiUrlBuilder,
  RemoteEnvironmentRequestError,
} from "@t3tools/client-runtime/rpc";
import { createEnvironmentCommand, runAtomCommand } from "@t3tools/client-runtime/state/runtime";
import { EnvironmentId } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as SubscriptionRef from "effect/SubscriptionRef";
import type { HttpClient } from "effect/http";

import { appAtomRegistry } from "../../rpc/atomRegistry";

export type EnvironmentWorkstreamsClient = Effect.Success<
  ReturnType<typeof makeEnvironmentHttpApiGroupClient<"workstreams">>
>;
export type EnvironmentWorkstreamsUrls = ReturnType<
  typeof makeEnvironmentHttpApiUrlBuilder
>["workstreams"];

/** One authenticated Workstreams request against the named environment's own server. */
export type EnvironmentWorkstreamsRequest = <A, E>(
  environmentId: string,
  method: "GET" | "POST",
  url: (urls: EnvironmentWorkstreamsUrls) => string,
  run: (
    client: EnvironmentWorkstreamsClient,
    headers: EnvironmentHttpAuthHeaders,
  ) => Effect.Effect<A, E>,
  signal?: AbortSignal,
) => Promise<A>;

/** The environment has no prepared HTTP connection, so nothing was sent to it. */
class EnvironmentWorkstreamsUnavailableError extends Data.TaggedError(
  "EnvironmentWorkstreamsUnavailableError",
)<{ readonly message: string }> {}

type DpopSigner = Option.Option<ManagedRelay.ManagedRelay.ManagedRelayDpopSigner["Service"]>;
type Authorization = Option.Option<RemoteEnvironmentAuthorization["Service"]>;

interface RequestInput {
  readonly execute: (
    prepared: PreparedConnection,
    signer: DpopSigner,
    remoteAuthorization: Authorization,
  ) => Effect.Effect<unknown, RemoteEnvironmentRequestError, HttpClient.HttpClient>;
  readonly signal: AbortSignal | undefined;
}

type EnvironmentCommand = ReturnType<typeof makeCommand>;

const makeCommand = (
  connectionAtomRuntime: (typeof import("../../connection/runtime"))["connectionAtomRuntime"],
) =>
  createEnvironmentCommand(connectionAtomRuntime, {
    label: "web-workstreams:environment-request",
    execute: (input: RequestInput, _registry, environmentId) => {
      const aborted = Effect.callback<never>((resume) => {
        const abort = () => resume(Effect.interrupt);
        if (input.signal?.aborted) abort();
        else input.signal?.addEventListener("abort", abort, { once: true });
        return Effect.sync(() => input.signal?.removeEventListener("abort", abort));
      });
      return Effect.raceFirst(
        Effect.gen(function* () {
          const supervisor = yield* EnvironmentSupervisor.EnvironmentSupervisor;
          const prepared = yield* SubscriptionRef.get(supervisor.prepared);
          if (Option.isNone(prepared) || prepared.value.environmentId !== environmentId)
            return yield* new EnvironmentWorkstreamsUnavailableError({
              message: "The thread's environment connection is not ready.",
            });
          const signer = yield* Effect.serviceOption(
            ManagedRelay.ManagedRelay.ManagedRelayDpopSigner,
          );
          const remoteAuthorization = yield* Effect.serviceOption(RemoteEnvironmentAuthorization);
          return yield* input.execute(prepared.value, signer, remoteAuthorization);
        }),
        aborted,
      );
    },
  });

let command: Promise<EnvironmentCommand> | null = null;

/**
 * Loaded on first use so modules that only touch the primary environment do not construct the
 * connection runtime.
 */
const environmentCommand = () =>
  (command ??= import("../../connection/runtime").then(({ connectionAtomRuntime }) =>
    makeCommand(connectionAtomRuntime),
  ));

export const requestEnvironmentWorkstreams: EnvironmentWorkstreamsRequest = async <A, E>(
  environmentId: string,
  method: "GET" | "POST",
  url: (urls: EnvironmentWorkstreamsUrls) => string,
  run: (
    client: EnvironmentWorkstreamsClient,
    headers: EnvironmentHttpAuthHeaders,
  ) => Effect.Effect<A, E>,
  signal?: AbortSignal,
): Promise<A> => {
  signal?.throwIfAborted();
  const result = await runAtomCommand(
    appAtomRegistry,
    await environmentCommand(),
    {
      environmentId: EnvironmentId.make(environmentId),
      input: {
        // The shared helper owns credential renewal; this adapter adds no transport retry.
        execute: (prepared, signer, remoteAuthorization) =>
          executeAuthenticatedEnvironmentHttpRequest({
            prepared,
            signer,
            remoteAuthorization,
            method,
            group: "workstreams",
            timeoutMs: 15_000,
            url,
            request: ({ client, headers }) => run(client, headers),
          }),
        signal,
      },
    },
    { reportFailure: false, reportDefect: false },
  );
  signal?.throwIfAborted();
  if (result._tag === "Failure") throw Cause.squash(result.cause);
  return result.value as A;
};

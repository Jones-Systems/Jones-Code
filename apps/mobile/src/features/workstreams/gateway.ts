import type { PreparedConnection } from "@t3tools/client-runtime/connection";
import { ManagedRelay } from "@t3tools/client-runtime/relay";
import { makeEnvironmentHttpApiGroupClient } from "@t3tools/client-runtime/rpc";
import {
  executeAuthenticatedEnvironmentHttpRequest,
  RemoteEnvironmentAuthorization,
  type EnvironmentHttpAuthHeaders,
} from "@t3tools/client-runtime/authorization";
import * as Effect from "effect/Effect";

export type WorkstreamClient = Effect.Success<
  ReturnType<typeof makeEnvironmentHttpApiGroupClient<"workstreams">>
>;
export const workstreamRequest = <A, E>(
  prepared: PreparedConnection,
  method: "GET" | "POST",
  path: string,
  request: (client: WorkstreamClient, headers: EnvironmentHttpAuthHeaders) => Effect.Effect<A, E>,
) =>
  Effect.gen(function* () {
    const signer = yield* Effect.serviceOption(ManagedRelay.ManagedRelayDpopSigner);
    const remoteAuthorization = yield* Effect.serviceOption(RemoteEnvironmentAuthorization);
    return yield* executeAuthenticatedEnvironmentHttpRequest({
      prepared,
      signer,
      remoteAuthorization,
      method,
      group: "workstreams",
      timeoutMs: 15_000,
      url: (base) => `${base.replace(/\/$/, "")}/api/workstreams${path}`,
      request: ({ client, headers }) => request(client, headers),
    });
  });

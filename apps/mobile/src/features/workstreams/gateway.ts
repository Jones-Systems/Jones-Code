import type { PreparedConnection } from "@t3tools/client-runtime/connection";
import { ManagedRelay } from "@t3tools/client-runtime/relay";
import {
  makeEnvironmentHttpApiGroupClient,
  makeEnvironmentHttpApiUrlBuilder,
} from "@t3tools/client-runtime/rpc";
import { T3WorkstreamPageQuery } from "@t3tools/contracts";
import {
  executeAuthenticatedEnvironmentHttpRequest,
  RemoteEnvironmentAuthorization,
  type EnvironmentHttpAuthHeaders,
} from "@t3tools/client-runtime/authorization";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

export type WorkstreamClient = Effect.Success<
  ReturnType<typeof makeEnvironmentHttpApiGroupClient<"workstreams">>
>;
export type WorkstreamUrls = ReturnType<typeof makeEnvironmentHttpApiUrlBuilder>["workstreams"];
export type WorkstreamUrlSelector = (urls: WorkstreamUrls) => string;
const encodePageQuery = Schema.encodeSync(Schema.Struct(T3WorkstreamPageQuery));

/** GET pagination remains a contract payload, which the URL builder does not model as query. */
export const workstreamPageUrl = (
  endpointUrl: string,
  query: Parameters<WorkstreamClient["list"]>[0]["payload"],
): string => {
  const url = new URL(endpointUrl);
  for (const [key, value] of Object.entries(encodePageQuery(query))) {
    if (value !== undefined) url.searchParams.append(key, value);
  }
  return url.toString();
};

export const workstreamRequest = <A, E>(
  prepared: PreparedConnection,
  method: "GET" | "POST",
  url: WorkstreamUrlSelector,
  request: (client: WorkstreamClient, headers: EnvironmentHttpAuthHeaders) => Effect.Effect<A, E>,
) =>
  {
const runEffect = Effect.fn("MobileWorkstreamGateway.workstreamRequest")(function* () {
    const signer = yield* Effect.serviceOption(ManagedRelay.ManagedRelayDpopSigner);
    const remoteAuthorization = yield* Effect.serviceOption(RemoteEnvironmentAuthorization);
    return yield* executeAuthenticatedEnvironmentHttpRequest({
      prepared,
      signer,
      remoteAuthorization,
      method,
      group: "workstreams",
      timeoutMs: 15_000,
      url,
      request: ({ client, headers }) => request(client, headers),
    });
  });
return runEffect();
};

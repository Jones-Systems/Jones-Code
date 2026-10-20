import type { RemoteAdmissionDecision, RemoteAdmissionPolicyUpdate } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import type { ConnectionCatalogEntry } from "../../connection/catalog.ts";
import { ConnectionResolver } from "../../connection/resolver.ts";
import { connectionRoutes, routeEntry } from "../../connection/routes.ts";
import { RemoteEnvironmentAuthorization } from "../../authorization/service.ts";
import { ManagedRelayDpopSigner } from "../../relay/managedRelay.ts";
import { executeAuthenticatedEnvironmentHttpRequest } from "../../state/environmentHttpAuth.ts";

export type RemoteAdmissionRequest =
  | { readonly action: "snapshot" | "attention" }
  | { readonly action: "policy"; readonly input: RemoteAdmissionPolicyUpdate }
  | { readonly action: "decide"; readonly input: RemoteAdmissionDecision };

export const requestRemoteAdmission = Effect.fn("clientRuntime.remoteAdmission.request")(function* (
  entry: ConnectionCatalogEntry,
  input: RemoteAdmissionRequest,
) {
  const resolver = yield* ConnectionResolver;
  const { prepared } = yield* Effect.firstSuccessOf(
    connectionRoutes(entry).map((route) => resolver.prepareForUpdate(routeEntry(entry, route))),
  );
  const signer = yield* Effect.serviceOption(ManagedRelayDpopSigner);
  const remoteAuthorization = yield* Effect.serviceOption(RemoteEnvironmentAuthorization);
  return yield* executeAuthenticatedEnvironmentHttpRequest({
    prepared,
    signer,
    remoteAuthorization,
    group: "remoteAdmission",
    method:
      input.action === "snapshot" || input.action === "attention"
        ? "GET"
        : input.action === "policy"
          ? "PUT"
          : "POST",
    url: (urls) => urls[input.action](),
    timeoutMs: 45_000,
    request: ({ client, headers }) =>
      Effect.gen(function* () {
        switch (input.action) {
          case "attention":
            return yield* client.attention({ headers });
          case "snapshot":
            return yield* client.snapshot({ headers });
          case "policy":
            return yield* client.policy({ headers, payload: input.input });
          case "decide":
            return yield* client.decide({ headers, payload: input.input });
        }
      }),
  });
});

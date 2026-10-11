import type {
  JonesUpdateDownloadInput,
  JonesUpdateInstallInput,
} from "@t3tools/contracts/jones/jonesUpdates";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type { ConnectionCatalogEntry } from "../../connection/catalog.ts";
import { ConnectionResolver } from "../../connection/resolver.ts";
import { connectionRoutes, routeEntry } from "../../connection/routes.ts";
import { RemoteEnvironmentAuthorization } from "../../authorization/service.ts";
import { ManagedRelayDpopSigner } from "../../relay/managedRelay.ts";
import { environmentEndpointUrl } from "../../environment/endpoint.ts";
import { executeAuthenticatedEnvironmentHttpRequest } from "../../state/environmentHttpAuth.ts";

export type JonesUpdateBridgeInput =
  | { readonly action: "state"; readonly after?: number }
  | { readonly action: "check" }
  | { readonly action: "download"; readonly input: JonesUpdateDownloadInput }
  | { readonly action: "install"; readonly input: JonesUpdateInstallInput };

export class JonesUpdateBindingError extends Schema.TaggedError<JonesUpdateBindingError>()(
  "JonesUpdateBindingError",
  { reason: Schema.Literals(["environment-changed", "installation-changed"]) },
) {}

/** Authenticates only update HTTP requests; it never opens an orchestration socket. */
export const requestJonesUpdateWithDescriptor = Effect.fn(
  "clientRuntime.fleetUpdates.requestJonesUpdate",
)(function* (entry: ConnectionCatalogEntry, input: JonesUpdateBridgeInput) {
  const resolver = yield* ConnectionResolver;
  // Route fallback is read-only preparation. The selected mutation is sent once;
  // a lost response belongs to the caller's operation reconciliation.
  const { prepared, descriptor } = yield* Effect.firstSuccessOf(
    connectionRoutes(entry).map((route) => resolver.prepareForUpdate(routeEntry(entry, route))),
  );
  const environmentId = entry.target.environmentId;
  if (prepared.environmentId !== environmentId || descriptor.environmentId !== environmentId) {
    return yield* new JonesUpdateBindingError({ reason: "environment-changed" });
  }
  if (
    input.action === "install" &&
    (input.input.environmentId !== environmentId ||
      input.input.currentVersion !== descriptor.serverVersion)
  ) {
    return yield* new JonesUpdateBindingError({ reason: "installation-changed" });
  }
  const signer = yield* Effect.serviceOption(ManagedRelayDpopSigner);
  const remoteAuthorization = yield* Effect.serviceOption(RemoteEnvironmentAuthorization);
  const suffix = input.action === "state" ? "" : `/${input.action}`;
  const state = yield* executeAuthenticatedEnvironmentHttpRequest({
    prepared,
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
  return { state, descriptor };
});

export const requestJonesUpdate = (entry: ConnectionCatalogEntry, input: JonesUpdateBridgeInput) =>
  requestJonesUpdateWithDescriptor(entry, input).pipe(Effect.map((result) => result.state));

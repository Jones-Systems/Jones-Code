import type { FleetActivateInput, FleetEnrollmentInput, FleetStageInput, FleetRetireInput } from "@t3tools/contracts/jones/fleet-updates";
import { FLEET_UPDATES_HTTP_BASE } from "@t3tools/contracts/jones/fleet-updates";
import * as Effect from "effect/Effect";
import type { ConnectionCatalogEntry } from "../../connection/catalog.ts";
import { ConnectionResolver } from "../../connection/resolver.ts";
import { connectionRoutes, routeEntry } from "../../connection/routes.ts";
import { RemoteEnvironmentAuthorization } from "../../authorization/service.ts";
import { ManagedRelayDpopSigner } from "../../relay/managedRelay.ts";
import { environmentEndpointUrl } from "../../environment/endpoint.ts";
import { executeAuthenticatedEnvironmentHttpRequest } from "../../state/environmentHttpAuth.ts";
import { JonesUpdateBindingError } from "./updateBridge.ts";

export type FleetHostRequest =
  | { readonly action: "status"; readonly operationId?: string }
  | { readonly action: "enroll"; readonly input: FleetEnrollmentInput }
  | { readonly action: "stage"; readonly input: FleetStageInput }
  | { readonly action: "retire"; readonly input: FleetRetireInput }
  | { readonly action: "activate"; readonly input: FleetActivateInput };

/** Uses the narrow authenticated update transport even when orchestration is incompatible. */
export const requestFleetHost = Effect.fn("clientRuntime.fleetUpdates.requestFleetHost")(
  function* (entry: ConnectionCatalogEntry, input: FleetHostRequest) {
    const resolver = yield* ConnectionResolver;
    const { prepared, descriptor } = yield* Effect.firstSuccessOf(
      connectionRoutes(entry).map((route) => resolver.prepareForUpdate(routeEntry(entry, route))),
    );
    const environmentId = entry.target.environmentId;
    if (prepared.environmentId !== environmentId || descriptor.environmentId !== environmentId ||
        (input.action !== "status" && (input.action === "enroll" ? input.input.enrollment.environmentId : input.input.environmentId) !== environmentId)) {
      return yield* new JonesUpdateBindingError({ reason: "environment-changed" });
    }
    const signer = yield* Effect.serviceOption(ManagedRelayDpopSigner);
    const remoteAuthorization = yield* Effect.serviceOption(RemoteEnvironmentAuthorization);
    const suffix = input.action === "status" ? (input.operationId === undefined ? "" : `?operationId=${encodeURIComponent(input.operationId)}`)
      : input.action === "enroll" ? "/enrollment" : `/${input.action}`;
    return yield* executeAuthenticatedEnvironmentHttpRequest({
      prepared, signer, remoteAuthorization, group: "jonesFleetUpdates",
      method: input.action === "status" ? "GET" : input.action === "enroll" ? "PUT" : "POST",
      url: (base) => environmentEndpointUrl(base, `${FLEET_UPDATES_HTTP_BASE}${suffix}`),
      timeoutMs: input.action === "stage" ? 20 * 60_000 : 35_000,
      request: ({ client, headers }) => {
        switch (input.action) {
          case "status": return client.status({ headers, query: input.operationId === undefined ? {} : { operationId: input.operationId } });
          case "enroll": return client.enroll({ headers, payload: input.input });
          case "stage": return client.stage({ headers, payload: input.input });
          case "retire": return client.retire({ headers, payload: input.input });
          case "activate": return client.activate({ headers, payload: input.input });
        }
      },
    });
  },
);

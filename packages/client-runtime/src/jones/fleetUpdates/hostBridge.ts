import type {
  FleetActivateInput,
  FleetEnrollmentInput,
  FleetLeaderInput,
  FleetStageInput,
  FleetRetireInput,
  FleetPlanDirectiveInput,
  FleetReleasePlanDirectiveInput,
} from "@t3tools/contracts/jones/fleet-updates";
import * as Effect from "effect/Effect";
import type { ConnectionCatalogEntry } from "../../connection/catalog.ts";
import { ConnectionResolver } from "../../connection/resolver.ts";
import { connectionRoutes, routeEntry } from "../../connection/routes.ts";
import { RemoteEnvironmentAuthorization } from "../../authorization/service.ts";
import { ManagedRelayDpopSigner } from "../../relay/managedRelay.ts";
import { executeAuthenticatedEnvironmentHttpRequest } from "../../state/environmentHttpAuth.ts";
import type { JonesOperationBeforeSend } from "../updates/jonesUpdates.ts";
import { JonesUpdateBindingError } from "./updateBridge.ts";

export type FleetHostRequest =
  | { readonly action: "status"; readonly operationId?: string }
  | { readonly action: "enroll"; readonly input: FleetEnrollmentInput }
  | { readonly action: "stage"; readonly input: FleetStageInput }
  | { readonly action: "retire"; readonly input: FleetRetireInput }
  | { readonly action: "activate"; readonly input: FleetActivateInput }
  | { readonly action: "leader"; readonly input: FleetLeaderInput }
  | {
      readonly action: "releaseDirective";
      readonly input: FleetReleasePlanDirectiveInput;
      readonly beforeSend?: JonesOperationBeforeSend;
    }
  | {
      readonly action: "fenceDirective";
      readonly input: FleetPlanDirectiveInput;
      readonly beforeSend?: JonesOperationBeforeSend;
    }
  | {
      readonly action: "planDirective";
      readonly input: FleetPlanDirectiveInput;
      readonly beforeSend?: JonesOperationBeforeSend;
    };

/** Uses the narrow authenticated update transport even when orchestration is incompatible. */
export const requestFleetHost = Effect.fn("clientRuntime.fleetUpdates.requestFleetHost")(function* (
  entry: ConnectionCatalogEntry,
  input: FleetHostRequest,
) {
  const resolver = yield* ConnectionResolver;
  const { prepared, descriptor } = yield* Effect.firstSuccessOf(
    connectionRoutes(entry).map((route) => resolver.prepareForUpdate(routeEntry(entry, route))),
  );
  const environmentId = entry.target.environmentId;
  if (
    prepared.environmentId !== environmentId ||
    descriptor.environmentId !== environmentId ||
    (input.action !== "status" &&
      (input.action === "enroll"
        ? input.input.enrollment.environmentId
        : input.input.environmentId) !== environmentId)
  ) {
    return yield* new JonesUpdateBindingError({ reason: "environment-changed" });
  }
  const signer = yield* Effect.serviceOption(ManagedRelayDpopSigner);
  const remoteAuthorization = yield* Effect.serviceOption(RemoteEnvironmentAuthorization);
  const beforeSend = "beforeSend" in input ? input.beforeSend : undefined;
  return yield* executeAuthenticatedEnvironmentHttpRequest({
    prepared,
    signer,
    remoteAuthorization,
    group: "jonesFleetUpdates",
    method:
      input.action === "status"
        ? "GET"
        : input.action === "enroll" || input.action === "leader" || input.action === "planDirective"
          ? "PUT"
          : "POST",
    url: (urls) =>
      input.action === "status"
        ? urls.status({
            query: input.operationId === undefined ? {} : { operationId: input.operationId },
          })
        : urls[input.action](),
    timeoutMs: input.action === "stage" ? 20 * 60_000 : 35_000,
    request: Effect.fn("clientRuntime.fleetUpdates.sendAuthorizedRequest")(function* ({
      client,
      headers,
    }) {
      if (beforeSend !== undefined) {
        const permit = yield* Effect.tryPromise(async () => beforeSend()).pipe(
          Effect.orElseSucceed(() => false),
        );
        if (permit !== true)
          return yield* Effect.fail("Fleet directive dispatch failed its local check.");
      }
      switch (input.action) {
        case "status":
          return yield* client.status({
            headers,
            query: input.operationId === undefined ? {} : { operationId: input.operationId },
          });
        case "enroll":
          return yield* client.enroll({ headers, payload: input.input });
        case "stage":
          return yield* client.stage({ headers, payload: input.input });
        case "retire":
          return yield* client.retire({ headers, payload: input.input });
        case "activate":
          return yield* client.activate({ headers, payload: input.input });
        case "leader":
          return yield* client.leader({ headers, payload: input.input });
        case "releaseDirective":
          return yield* client.releaseDirective({ headers, payload: input.input });
        case "fenceDirective":
          return yield* client.fenceDirective({ headers, payload: input.input });
        case "planDirective":
          return yield* client.planDirective({ headers, payload: input.input });
      }
    }),
  });
});

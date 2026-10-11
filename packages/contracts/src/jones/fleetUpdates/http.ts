import * as Schema from "effect/Schema";
import * as HttpApiEndpoint from "effect/http-api/HttpApiEndpoint";
import * as HttpApiGroup from "effect/http-api/HttpApiGroup";
import * as HttpApiSchema from "effect/http-api/HttpApiSchema";
import type { makeJonesHttpGroups } from "../environmentHttpGroups.ts";
import {
  FLEET_UPDATES_HTTP_BASE, FleetActivateInput, FleetEnrollmentInput, FleetHostError,
  FleetHostStatus, FleetOperationId, FleetStageInput,
} from "./host.ts";

export function makeFleetUpdatesHttpGroup({
  OptionalBearerHeaders, EnvironmentAuthenticatedAuth, EnvironmentScopeRequiredError,
  EnvironmentInternalError,
}: Parameters<typeof makeJonesHttpGroups>[0]) {
  const common = {
    headers: OptionalBearerHeaders,
    error: [EnvironmentScopeRequiredError, EnvironmentInternalError, FleetHostError.pipe(HttpApiSchema.status(409))],
    success: FleetHostStatus,
  } as const;
  return HttpApiGroup.make("jonesFleetUpdates")
    .add(HttpApiEndpoint.get("status", FLEET_UPDATES_HTTP_BASE, {
      ...common, query: Schema.Struct({ operationId: Schema.optionalKey(FleetOperationId) }),
    }).middleware(EnvironmentAuthenticatedAuth))
    .add(HttpApiEndpoint.put("enroll", `${FLEET_UPDATES_HTTP_BASE}/enrollment`, {
      ...common, payload: FleetEnrollmentInput,
    }).middleware(EnvironmentAuthenticatedAuth))
    .add(HttpApiEndpoint.post("stage", `${FLEET_UPDATES_HTTP_BASE}/stage`, {
      ...common, payload: FleetStageInput,
    }).middleware(EnvironmentAuthenticatedAuth))
    .add(HttpApiEndpoint.post("activate", `${FLEET_UPDATES_HTTP_BASE}/activate`, {
      ...common, payload: FleetActivateInput,
    }).middleware(EnvironmentAuthenticatedAuth));
}

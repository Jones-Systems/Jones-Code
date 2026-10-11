import { AuthOrchestrationOperateScope, AuthOrchestrationReadScope, EnvironmentHttpApi } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import * as HttpEffect from "effect/http/HttpEffect";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import { requireEnvironmentScope } from "../../auth/http.ts";
import { FleetUpdates } from "./service.ts";

const noStore = HttpEffect.appendPreResponseHandler((_request, response) =>
  Effect.succeed(HttpServerResponse.setHeader(response, "cache-control", "no-store")),
);

export const fleetUpdatesHttpApiLayer = HttpApiBuilder.group(EnvironmentHttpApi, "jonesFleetUpdates", (handlers) =>
  Effect.gen(function* () {
    const fleet = yield* FleetUpdates;
    return handlers
      .handle("status", ({ query }) => Effect.gen(function* () {
        yield* requireEnvironmentScope(AuthOrchestrationReadScope);
        yield* noStore;
        return yield* fleet.status(query.operationId);
      }))
      .handle("enroll", ({ payload }) => Effect.gen(function* () {
        yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
        yield* noStore;
        return yield* fleet.enroll(payload);
      }))
      .handle("stage", ({ payload }) => Effect.gen(function* () {
        yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
        yield* noStore;
        return yield* fleet.stage(payload);
      }))
      .handle("activate", ({ payload }) => Effect.gen(function* () {
        yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
        yield* noStore;
        return yield* fleet.activate(payload);
      }));
  }),
);

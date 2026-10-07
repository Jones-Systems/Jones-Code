import {
  AuthOrchestrationReadScope,
  AuthOrchestrationOperateScope,
  EnvironmentHttpApi,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";
import * as HttpEffect from "effect/unstable/http/HttpEffect";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import { requireEnvironmentScope } from "../../auth/http.ts";
import { JonesUpdates } from "./service.ts";

const noStore = HttpEffect.appendPreResponseHandler((_request, response) =>
  Effect.succeed(HttpServerResponse.setHeader(response, "cache-control", "no-store")),
);

export const jonesUpdatesHttpApiLayer = HttpApiBuilder.group(
  EnvironmentHttpApi,
  "jonesUpdates",
  (handlers) =>
    Effect.gen(function* () {
      const updates = yield* JonesUpdates;
      return handlers
        .handle("prepareNative", ({ payload }) =>
          Effect.gen(function* () {
            yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
            yield* noStore;
            return yield* updates.prepareNative(payload);
          }),
        )
        .handle("state", ({ query }) =>
          Effect.gen(function* () {
            yield* requireEnvironmentScope(AuthOrchestrationReadScope);
            yield* noStore;
            return yield* updates.state(query.after);
          }),
        )
        .handle("check", () =>
          Effect.gen(function* () {
            yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
            yield* noStore;
            return yield* updates.check;
          }),
        )
        .handle("download", ({ payload }) =>
          Effect.gen(function* () {
            yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
            yield* noStore;
            return yield* updates.download(payload);
          }),
        )
        .handle("install", ({ payload }) =>
          Effect.gen(function* () {
            yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
            yield* noStore;
            return yield* updates.install(payload);
          }),
        );
    }),
);

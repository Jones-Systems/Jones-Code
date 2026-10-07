import { AuthOrchestrationReadScope, EnvironmentHttpApi } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";
import * as HttpEffect from "effect/unstable/http/HttpEffect";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import { requireEnvironmentScope } from "../../auth/http.ts";
import * as WorkQueueMetadataService from "./WorkQueueMetadataService.ts";

export const workQueueMetadataHttpApiLayer = HttpApiBuilder.group(
  EnvironmentHttpApi,
  "workQueueMetadata",
  Effect.fnUntraced(function* (handlers) {
    const service = yield* WorkQueueMetadataService.WorkQueueMetadataService;
    return handlers.handle(
      "snapshot",
      Effect.fn("workQueueMetadata.snapshot")(function* () {
        yield* requireEnvironmentScope(AuthOrchestrationReadScope);
        yield* HttpEffect.appendPreResponseHandler((_request, response) =>
          Effect.succeed(HttpServerResponse.setHeader(response, "cache-control", "no-store")),
        );
        return yield* service.snapshot;
      }),
    );
  }),
);

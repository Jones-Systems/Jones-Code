import { AuthOrchestrationReadScope, EnvironmentHttpBadRequestError } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import * as HttpEffect from "effect/http/HttpEffect";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";

import { failEnvironmentInternal, requireEnvironmentScope } from "../../../auth/http.ts";
import type { WorkstreamsRegistrationContext } from "./service.ts";

const invalidRequest = () =>
  new EnvironmentHttpBadRequestError({
    message: "workstreams_registration_context_invalid_request",
  });

export const createRegistrationContextHandler = (
  service: WorkstreamsRegistrationContext["Service"],
) =>
  Effect.fn("environment.workstreams.registrationContext")(function* () {
    yield* HttpEffect.appendPreResponseHandler((_request, response) =>
      Effect.succeed(
        HttpServerResponse.setHeaders(response, {
          "cache-control": "private, no-store",
          "x-content-type-options": "nosniff",
        }),
      ),
    );
    yield* requireEnvironmentScope(AuthOrchestrationReadScope);
    const request = yield* HttpServerRequest.HttpServerRequest;
    if (
      request.method !== "GET" ||
      request.originalUrl.includes("?") ||
      request.originalUrl.includes("#")
    ) {
      return yield* invalidRequest();
    }
    // The Web adapter reports an absent body as a stream error; native empty streams complete.
    if (!(request.source instanceof Request && request.source.body === null)) {
      yield* Stream.runForEach(request.stream, (chunk) =>
        chunk.byteLength === 0 ? Effect.void : Effect.fail(invalidRequest()),
      ).pipe(Effect.mapError(() => invalidRequest()));
    }
    const result = yield* service.read().pipe(
      Effect.catch((error) =>
        failEnvironmentInternal("internal_error", {
          operation: "registrationContext",
          reason: error.reason,
        }),
      ),
    );
    return result.context;
  });

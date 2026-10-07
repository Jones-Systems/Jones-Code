import { AuthOrchestrationReadScope, EnvironmentHttpApi } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";
import * as HttpEffect from "effect/unstable/http/HttpEffect";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";

import {
  annotateEnvironmentRequest,
  failEnvironmentInternal,
  requireEnvironmentScope,
} from "../auth/http.ts";
import * as HostStatus from "./HostStatus.ts";

export const hostStatusHttpApiLayer = HttpApiBuilder.group(
  EnvironmentHttpApi,
  "hostStatus",
  (handlers) =>
    Effect.gen(function* () {
      const hostStatus = yield* HostStatus.HostStatus;
      return handlers.handle(
        "snapshot",
        Effect.fn("environment.hostStatus.snapshot")(function* (args) {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          yield* requireEnvironmentScope(AuthOrchestrationReadScope);
          yield* HttpEffect.appendPreResponseHandler((_request, response) =>
            Effect.succeed(HttpServerResponse.setHeader(response, "cache-control", "no-store")),
          );
          return yield* hostStatus
            .snapshot()
            .pipe(Effect.catchDefect((cause) => failEnvironmentInternal("internal_error", cause)));
        }),
      );
    }),
);

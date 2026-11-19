import { EnvironmentAuthenticatedPrincipal, EnvironmentHttpApi } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import * as HttpEffect from "effect/http/HttpEffect";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import { failEnvironmentInternal } from "../../auth/http.ts";
import * as Requests from "./RequestManagement.ts";

const uncachedPrincipal = Effect.gen(function* () {
  yield* HttpEffect.appendPreResponseHandler((_request, response) =>
    Effect.succeed(HttpServerResponse.setHeader(response, "cache-control", "no-store")),
  );
  return yield* EnvironmentAuthenticatedPrincipal;
});

export const requestManagementHttpApiLayer = HttpApiBuilder.group(
  EnvironmentHttpApi,
  "requestManagement",
  Effect.fnUntraced(function* (handlers) {
    const requests = yield* Requests.RequestManagement;
    return handlers
      .handle("query", ({ payload }) =>
        uncachedPrincipal.pipe(
          Effect.flatMap((principal) => requests.ownerQuery(payload, principal)),
          Effect.catchTags({
            SqlError: (error) => failEnvironmentInternal("internal_error", error),
          }),
        ),
      )
      .handle("edit", ({ payload }) =>
        uncachedPrincipal.pipe(
          Effect.flatMap((principal) => requests.edit(payload, principal)),
          Effect.catchTags({
            SqlError: (error) => failEnvironmentInternal("internal_error", error),
          }),
        ),
      );
  }),
);

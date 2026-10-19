import { AuthOrchestrationReadScope, EnvironmentHttpApi } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import * as HttpEffect from "effect/http/HttpEffect";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import { failEnvironmentInternal, requireEnvironmentScope } from "../../auth/http.ts";
import * as SettlementOrigins from "./SettlementOrigins.ts";

export const settlementOriginsHttpApiLayer = HttpApiBuilder.group(
  EnvironmentHttpApi,
  "settlementOrigins",
  Effect.fnUntraced(function* (handlers) {
    const service = yield* SettlementOrigins.SettlementOrigins;
    return handlers.handle(
      "resolve",
      Effect.fn("settlementOrigins.resolve")(function* ({ payload }) {
        yield* requireEnvironmentScope(AuthOrchestrationReadScope);
        yield* HttpEffect.appendPreResponseHandler((_request, response) =>
          Effect.succeed(HttpServerResponse.setHeader(response, "cache-control", "no-store")),
        );
        return yield* service.resolve(payload).pipe(
          Effect.catchTags({
            SettlementOriginsReadError: (error) => failEnvironmentInternal("internal_error", error),
          }),
        );
      }),
    );
  }),
);

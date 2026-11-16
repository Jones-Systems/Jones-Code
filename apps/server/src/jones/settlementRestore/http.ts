import { AuthOrchestrationReadScope, EnvironmentHttpApi } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import * as HttpEffect from "effect/http/HttpEffect";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import { failEnvironmentInternal, requireEnvironmentScope } from "../../auth/http.ts";
import * as SettlementRestore from "./SettlementRestore.ts";

export const settlementRestoreHttpApiLayer = HttpApiBuilder.group(
  EnvironmentHttpApi,
  "settlementRestore",
  Effect.fnUntraced(function* (handlers) {
    const service = yield* SettlementRestore.SettlementRestore;
    return handlers.handle(
      "inspect",
      Effect.fn("settlementRestore.inspect")(function* ({ payload }) {
        yield* requireEnvironmentScope(AuthOrchestrationReadScope);
        yield* HttpEffect.appendPreResponseHandler((_request, response) =>
          Effect.succeed(HttpServerResponse.setHeader(response, "cache-control", "no-store")),
        );
        return yield* service.inspect(payload).pipe(
          Effect.catchTags({
            SettlementRestoreReadError: (error) => failEnvironmentInternal("internal_error", error),
          }),
        );
      }),
    );
  }),
);

import {
  AuthOrchestrationReadScope,
  AuthPreviewOperateScope,
  EnvironmentHttpApi,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import * as HttpEffect from "effect/http/HttpEffect";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import {
  failEnvironmentInternal,
  failEnvironmentScopeRequired,
  layerAuthenticatedAuth,
  requireEnvironmentScope,
} from "../../auth/http.ts";
import * as CompanionHostRegistry from "./CompanionHostRegistry.ts";
import * as RenderHostSelection from "./RenderHostSelection.ts";

export const previewCompanionHttpApiLayer = HttpApiBuilder.group(
  EnvironmentHttpApi,
  "previewCompanion",
  Effect.fnUntraced(function* (handlers) {
    const registry = yield* CompanionHostRegistry.CompanionHostRegistry;
    const selection = yield* RenderHostSelection.RenderHostSelection;
    const authorize = (write: boolean) =>
      Effect.gen(function* () {
        const principal = yield* requireEnvironmentScope(AuthOrchestrationReadScope);
        if (principal.subject === "mcp-client")
          return yield* failEnvironmentScopeRequired(AuthPreviewOperateScope);
        if (write) yield* requireEnvironmentScope(AuthPreviewOperateScope);
        yield* HttpEffect.appendPreResponseHandler((_request, response) =>
          Effect.succeed(HttpServerResponse.setHeader(response, "cache-control", "no-store")),
        );
      });
    return handlers
      .handle("hosts", () =>
        Effect.gen(function* () {
          yield* authorize(false);
          return {
            hosts: yield* registry.hosts,
            environmentDefault: yield* selection.environmentDefault,
          };
        }),
      )
      .handle("thread", ({ params }) =>
        Effect.gen(function* () {
          yield* authorize(false);
          return yield* selection.thread(params.threadId);
        }),
      )
      .handle("setDefault", ({ payload }) =>
        Effect.gen(function* () {
          yield* authorize(true);
          yield* selection
            .setDefault(payload.selection)
            .pipe(Effect.catch(() => failEnvironmentInternal("internal_error")));
        }),
      )
      .handle("setThread", ({ params, payload }) =>
        Effect.gen(function* () {
          yield* authorize(true);
          yield* selection
            .setThread(params.threadId, payload.selection)
            .pipe(Effect.catch(() => failEnvironmentInternal("internal_error")));
        }),
      );
  }),
);

class CompanionHttpApi extends HttpApi.make("environment").add(
  EnvironmentHttpApi.groups.previewCompanion,
) {}

/** Standalone assembly for the bounded HTTP harness; production provides the group to its shared API. */
export const routeLayer = HttpApiBuilder.layer(CompanionHttpApi).pipe(
  Layer.provide(previewCompanionHttpApiLayer),
  Layer.provide(layerAuthenticatedAuth),
);

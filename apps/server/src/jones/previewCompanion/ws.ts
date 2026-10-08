import * as NodeHttpServerRequest from "@effect/platform-node/NodeHttpServerRequest";
import {
  AuthOrchestrationReadScope,
  AuthPreviewOperateScope,
  PREVIEW_COMPANION_WS_PATH,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/http";
import * as Socket from "effect/socket/Socket";
import { authenticateMediaRequest } from "../../auth/http.ts";
import * as EnvironmentAuth from "../../auth/EnvironmentAuth.ts";
import * as CompanionHostRegistry from "./CompanionHostRegistry.ts";

export const routeLayer = HttpRouter.use((router) =>
  Effect.gen(function* () {
    const auth = yield* EnvironmentAuth.EnvironmentAuth;
    const registry = yield* CompanionHostRegistry.CompanionHostRegistry;
    const handler = Effect.scoped(
      Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        const session = yield* authenticateMediaRequest(AuthPreviewOperateScope);
        if (
          session.subject === "mcp-client" ||
          !session.scopes.includes(AuthOrchestrationReadScope)
        )
          return HttpServerResponse.empty({ status: 403 });
        if (request.headers.upgrade?.toLowerCase() !== "websocket")
          return HttpServerResponse.empty({ status: 400 });
        const incoming = NodeHttpServerRequest.toIncomingMessage(request);
        // Keep queued bytes visible to the registry's bound, outside a compression queue.
        delete incoming.headers["sec-websocket-extensions"];
        const socket = yield* request.upgrade;
        const reader = yield* socket.reader;
        const writer = yield* socket.writer;
        yield* registry.connect({
          read: reader.pull.pipe(
            Effect.mapError(
              (cause) => new CompanionHostRegistry.CompanionTransportError({ cause }),
            ),
          ),
          write: (message) =>
            writer
              .write(message)
              .pipe(
                Effect.mapError(
                  (cause) => new CompanionHostRegistry.CompanionTransportError({ cause }),
                ),
              ),
          close: (code, reason) =>
            writer.write(new Socket.CloseEvent(code, reason)).pipe(Effect.ignore),
          writableLength: () => incoming.socket.writableLength,
        });
        return HttpServerResponse.empty();
      }),
    ).pipe(
      Effect.provideService(EnvironmentAuth.EnvironmentAuth, auth),
      Effect.catch((error) =>
        Effect.succeed(
          HttpServerResponse.empty({
            status:
              error._tag === "EnvironmentScopeRequiredError"
                ? 403
                : error._tag === "EnvironmentAuthInvalidError"
                  ? 401
                  : 500,
          }),
        ),
      ),
    );
    yield* router.add("GET", PREVIEW_COMPANION_WS_PATH, handler);
  }),
);

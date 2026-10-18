import * as NodeHttpPlatform from "@effect/platform-node/NodeHttpPlatform";
import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  AuthOrchestrationReadScope,
  AuthPreviewOperateScope,
  AuthSessionId,
  type AuthEnvironmentScope,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpBody from "effect/http/HttpBody";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { HttpRouter, HttpServer } from "effect/http";
import * as EnvironmentAuth from "../../auth/EnvironmentAuth.ts";
import * as Registry from "./CompanionHostRegistry.ts";
import * as Selection from "./RenderHostSelection.ts";
import * as Http from "./http.ts";
import * as Ws from "./ws.ts";

it.live.each([
  { subject: "human", scopes: [AuthOrchestrationReadScope], expected: 403 },
  {
    subject: "mcp-client",
    scopes: [AuthPreviewOperateScope, AuthOrchestrationReadScope],
    expected: 403,
  },
  { subject: "human", scopes: [AuthPreviewOperateScope], expected: 403 },
  {
    subject: "human",
    scopes: [AuthPreviewOperateScope, AuthOrchestrationReadScope],
    expected: 204,
  },
])(
  "gates human selection before the service ($subject, $expected)",
  ({ subject, scopes, expected }) =>
    Effect.gen(function* () {
      let writes = 0;
      const auth = Layer.mock(EnvironmentAuth.EnvironmentAuth, {
        authenticateHttpRequest: () =>
          Effect.succeed({
            sessionId: AuthSessionId.make("test"),
            subject,
            method: "bearer-access-token",
            scopes: scopes as AuthEnvironmentScope[],
          }),
      });
      const services = yield* Layer.build(
        HttpRouter.serve(
          Http.routeLayer.pipe(
            Layer.provide(auth),
            Layer.provide(Layer.mock(Registry.CompanionHostRegistry, {})),
            Layer.provide(
              Layer.mock(Selection.RenderHostSelection, {
                setDefault: () =>
                  Effect.sync(() => {
                    writes++;
                  }),
              }),
            ),
            Layer.provide(NodeHttpPlatform.layer.pipe(Layer.provide(NodeServices.layer))),
          ),
          { disableListenLog: true },
        ).pipe(Layer.provideMerge(NodeHttpServer.layerTest)),
      );
      const origin = HttpServer.formatAddress(Context.get(services, HttpServer.HttpServer).address);
      const response = yield* HttpClient.put(`${origin}/api/jones/preview-companion/default`, {
        body: HttpBody.jsonUnsafe({ selection: { _tag: "server" } }),
      }).pipe(Effect.provide(FetchHttpClient.layer));
      expect(response.status).toBe(expected);
      expect(writes).toBe(expected === 204 ? 1 : 0);
    }).pipe(Effect.scoped),
);

it.live("does not upgrade or connect when the native authenticator rejects missing proof", () =>
  Effect.gen(function* () {
    let connects = 0;
    const services = yield* Layer.build(
      HttpRouter.serve(
        Ws.routeLayer.pipe(
          Layer.provide(
            Layer.mock(EnvironmentAuth.EnvironmentAuth, {
              authenticateWebSocketUpgrade: () =>
                Effect.fail(new EnvironmentAuth.ServerAuthMissingCredentialError({})),
            }),
          ),
          Layer.provide(
            Layer.mock(Registry.CompanionHostRegistry, {
              connect: () =>
                Effect.sync(() => {
                  connects++;
                }),
            }),
          ),
        ),
        { disableListenLog: true },
      ).pipe(Layer.provideMerge(NodeHttpServer.layerTest)),
    );
    const origin = HttpServer.formatAddress(Context.get(services, HttpServer.HttpServer).address);
    const response = yield* HttpClient.get(`${origin}/api/jones/preview-companion/ws`).pipe(
      Effect.provide(FetchHttpClient.layer),
    );
    expect(response.status).toBe(401);
    expect(connects).toBe(0);
  }).pipe(Effect.scoped),
);

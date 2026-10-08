import * as NodeHttpPlatform from "@effect/platform-node/NodeHttpPlatform";
import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import { AuthOrchestrationReadScope, AuthSessionId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { HttpRouter, HttpServer } from "effect/http";
import * as EnvironmentAuth from "../../auth/EnvironmentAuth.ts";
import * as ServerBrowser from "../../preview/ServerBrowser.ts";
import * as ServerBrowserStream from "../../preview/ServerBrowserStream.ts";
import { CompanionHostUnavailable } from "./CompanionHostRegistry.ts";

it.live("closes the real stream socket with 4504 and bounded host status", () =>
  Effect.gen(function* () {
    const hostId = "m".repeat(64);
    const auth = Layer.mock(EnvironmentAuth.EnvironmentAuth, {
      authenticateWebSocketUpgrade: () =>
        Effect.succeed({
          sessionId: AuthSessionId.make("test"),
          subject: "human",
          method: "bearer-access-token",
          scopes: [AuthOrchestrationReadScope],
        }),
    });
    const services = yield* Layer.build(
      HttpRouter.serve(
        ServerBrowserStream.routeLayer.pipe(
          Layer.provide(
            Layer.mock(ServerBrowser.ServerBrowser, {
              attachViewer: () =>
                Effect.fail(
                  new ServerBrowser.ServerBrowserLaunchError({
                    cause: new CompanionHostUnavailable({
                      hostId,
                      label: "😀".repeat(32),
                      state: "offline",
                    }),
                  }),
                ),
            }),
          ),
          Layer.provide(NodeHttpPlatform.layer.pipe(Layer.provideMerge(NodeServices.layer))),
        ),
        { disableListenLog: true },
      ).pipe(Layer.provideMerge(NodeHttpServer.layerTest), Layer.provide(auth)),
    );
    const origin = HttpServer.formatAddress(
      Context.get(services, HttpServer.HttpServer).address,
    ).replace(/^http/, "ws");
    const closed = Promise.withResolvers<CloseEvent>();
    const socket = yield* Effect.acquireRelease(
      Effect.sync(() => new WebSocket(`${origin}/api/preview-stream/ws?threadId=thread&tabId=tab`)),
      (socket) => Effect.sync(() => socket.close()),
    );
    socket.addEventListener("close", closed.resolve);
    socket.addEventListener("error", () => closed.reject(new Error("stream upgrade failed")));
    const result = yield* Effect.promise(() => closed.promise);
    expect(result.code).toBe(4504);
    expect(new TextEncoder().encode(result.reason).byteLength).toBeLessThanOrEqual(123);
    expect(JSON.parse(result.reason)).toMatchObject({ hostId, state: "offline" });
  }).pipe(Effect.scoped),
);

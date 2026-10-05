import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import { AuthOrchestrationReadScope, AuthSessionId, EnvironmentHttpApi } from "@t3tools/contracts";
import { HostProcessArchitecture, HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Etag from "effect/unstable/http/Etag";
import * as HttpPlatform from "effect/unstable/http/HttpPlatform";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import * as HttpApi from "effect/unstable/httpapi/HttpApi";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";
import { expect } from "vite-plus/test";

import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import { environmentAuthenticatedAuthLayer } from "../auth/http.ts";
import * as ServerConfig from "../config.ts";
import * as SelfUpdate from "../cloud/selfUpdate.ts";
import * as Launcher from "../cloud/serviceLauncherClient.ts";
import * as Startup from "../serverRuntimeStartup.ts";
import * as DesktopReceiver from "../resourceTelemetry/DesktopTelemetryReceiver.ts";
import { jonesUpdatesHttpApiLayer } from "./http.ts";
import * as JonesUpdates from "./service.ts";

class JonesUpdatesTestApi extends HttpApi.make("environment").add(
  EnvironmentHttpApi.groups.jonesUpdates,
) {}

const unusedService = <A>(): A =>
  new Proxy({} as object, {
    get(_target, key) {
      throw new Error(`Ordinary Release must not invoke native update operation ${String(key)}`);
    },
  }) as A;

it.layer(NodeServices.layer)("Jones update Release compatibility", (it) => {
  it.effect.each(["web", "desktop"] as const)(
    "ordinary Release %s host returns absent Jones state without displacing Release updates",
    (mode) =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "jones-release-http-" });
        const config = yield* ServerConfig.ServerConfig.pipe(
          Effect.provide(ServerConfig.layerTest(process.cwd(), baseDir)),
        );
        const authenticate: EnvironmentAuth.EnvironmentAuth["Service"]["authenticateHttpRequest"] =
          () =>
            Effect.succeed({
              sessionId: AuthSessionId.make("jones-release-http"),
              subject: "synthetic-release-user",
              method: "browser-session-cookie",
              scopes: [AuthOrchestrationReadScope],
            });
        const auth = new Proxy(unusedService<EnvironmentAuth.EnvironmentAuth["Service"]>(), {
          get(target, key) {
            return key === "authenticateHttpRequest" ? authenticate : Reflect.get(target, key);
          },
        });
        const updates = JonesUpdates.layer.pipe(
          Layer.provide(ServerConfig.layer({ ...config, mode })),
          Layer.provide(Layer.succeed(HostProcessPlatform, "linux")),
          Layer.provide(Layer.succeed(HostProcessArchitecture, "x64")),
          Layer.provide(
            Layer.succeed(
              SelfUpdate.ServerSelfUpdate,
              unusedService<SelfUpdate.ServerSelfUpdate["Service"]>(),
            ),
          ),
          Layer.provide(
            Layer.succeed(
              Startup.ServerRuntimeStartup,
              unusedService<Startup.ServerRuntimeStartup["Service"]>(),
            ),
          ),
          Layer.provide(
            Layer.succeed(
              DesktopReceiver.DesktopTelemetryReceiver,
              unusedService<DesktopReceiver.DesktopTelemetryReceiver["Service"]>(),
            ),
          ),
          Layer.provide(
            Layer.succeed(Launcher.ServiceLauncherClient, {
              managed: false,
              requestUpdate: () => Effect.die("Release state must not request Install"),
              prepareTrial: Effect.undefined,
            }),
          ),
        );
        const routes = HttpApiBuilder.layer(JonesUpdatesTestApi).pipe(
          Layer.provide(jonesUpdatesHttpApiLayer),
          Layer.provide(environmentAuthenticatedAuthLayer),
          Layer.provide(Layer.succeed(EnvironmentAuth.EnvironmentAuth, auth)),
          Layer.provide(updates),
          Layer.provideMerge(
            HttpPlatform.layer.pipe(
              Layer.provideMerge(NodeServices.layer),
              Layer.provideMerge(Etag.layerWeak),
            ),
          ),
          Layer.provide(NodeServices.layer),
        );
        yield* Effect.acquireUseRelease(
          Effect.sync(() => HttpRouter.toWebHandler(routes, { disableLogger: true })),
          (app) =>
            Effect.promise(async () => {
              const response = await app.handler(
                new Request("http://fixture/api/jones-updates", {
                  headers: { cookie: "synthetic-release-session=fixture" },
                }),
              );
              expect(response.status).toBe(200);
              expect(await response.json()).toBeNull();
              expect(response.headers.get("cache-control")).toBe("no-store");
            }),
          (app) => Effect.promise(() => app.dispose()),
        );
      }),
  );
});

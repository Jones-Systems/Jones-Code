import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import {
  AuthOrchestrationReadScope,
  AuthSessionId,
  EnvironmentAuthenticatedAuth,
  EnvironmentAuthenticatedPrincipal,
  EnvironmentHttpApi,
  EnvironmentId,
  type AuthEnvironmentScope,
  type JonesUpdateState,
} from "@t3tools/contracts";
import { HostProcessArchitecture, HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Etag from "effect/http/Etag";
import * as HttpPlatform from "effect/http/HttpPlatform";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpServer from "effect/http/HttpServer";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import { describe, expect, it as test } from "vite-plus/test";

import * as EnvironmentAuth from "../../auth/EnvironmentAuth.ts";
import * as AuthHttp from "../../auth/http.ts";
import * as ServerConfig from "../../config.ts";
import * as SelfUpdate from "../../cloud/selfUpdate.ts";
import * as Launcher from "../../cloud/serviceLauncherClient.ts";
import * as Startup from "../../serverRuntimeStartup.ts";
import * as DesktopReceiver from "../../resourceTelemetry/DesktopTelemetryReceiver.ts";
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
              requiresQualifiedTrialGate: false,
              prepareQualifiedTrial: () =>
                Effect.die("Release state must not prepare a qualified trial"),
              requestUpdate: () => Effect.die("Release state must not request Install"),
              prepareTrial: Effect.undefined,
            }),
          ),
        );
        const routes = HttpApiBuilder.layer(JonesUpdatesTestApi).pipe(
          Layer.provide(jonesUpdatesHttpApiLayer),
          Layer.provide(AuthHttp.layerAuthenticatedAuth),
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

function fixture(scopes: ReadonlyArray<AuthEnvironmentScope>) {
  const calls: unknown[] = [];
  const state: JonesUpdateState = {
    source: "jones-actions",
    channel: "jones-main",
    phase: "staged",
    capability: { check: true, download: true, install: false, reason: "bootstrap-required" },
    stagedHandle: "11111111-1111-4111-8111-111111111111",
    currentVersion: "0.0.0-preview.20261002.100",
    environmentId: EnvironmentId.make("synthetic-environment"),
  };
  const record = (action: string, input?: unknown) =>
    Effect.sync(() => {
      calls.push({ action, input });
      return state;
    });
  const service = Layer.succeed(JonesUpdates.JonesUpdates, {
    state: (after) => record("state", after),
    check: record("check"),
    download: (input) => record("download", input),
    prepareNative: (input) => record("prepareNative", input),
    install: (input) => record("install", input),
    stageExact: (input) => record("stageExact", input),
    installForOperation: (input) => record("installForOperation", input),
    reconcileOperation: (operationId) => Effect.succeed({ state: "absent" as const, operationId }),
  });
  const auth = Layer.succeed(EnvironmentAuthenticatedAuth, (effect) =>
    effect.pipe(
      Effect.provideService(EnvironmentAuthenticatedPrincipal, {
        sessionId: AuthSessionId.make("synthetic-session"),
        subject: "synthetic-user",
        method: "bearer-access-token",
        scopes: new Set(scopes),
      }),
    ),
  );
  const routes = HttpApiBuilder.layer(JonesUpdatesTestApi).pipe(
    Layer.provide(jonesUpdatesHttpApiLayer.pipe(Layer.provide(service))),
    Layer.provide(auth),
    Layer.provide(HttpServer.layerServices),
  );
  return { ...HttpRouter.toWebHandler(routes), calls, state };
}

describe("qualified update HTTP authority and decoding", () => {
  test("round trips a fixed download selection and exact install input with no-store", async () => {
    const app = fixture(["orchestration:read", "orchestration:operate"]);
    try {
      const download = { artifactId: 101, sourceSha: "a".repeat(40) };
      const install = {
        stagedHandle: app.state.stagedHandle,
        environmentId: app.state.environmentId,
        currentVersion: app.state.currentVersion,
        continueRunningThreads: true,
      };
      for (const [action, input] of [
        ["download", download],
        ["install", install],
      ] as const) {
        const response = await app.handler(
          new Request(`http://fixture/api/jones-updates/${action}`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify(input),
          }),
        );
        expect(response.status).toBe(200);
        expect(response.headers.get("cache-control")).toBe("no-store");
        expect(await response.json()).toEqual(app.state);
      }
      expect(app.calls).toEqual([
        { action: "download", input: download },
        { action: "install", input: install },
      ]);
    } finally {
      await app.dispose();
    }
  });

  test("rejects malformed install before calling the service", async () => {
    const app = fixture(["orchestration:operate"]);
    try {
      const response = await app.handler(
        new Request("http://fixture/api/jones-updates/install", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ stagedHandle: 123, environmentId: "synthetic-environment" }),
        }),
      );
      expect(response.status).toBe(400);
      expect(app.calls).toEqual([]);
    } finally {
      await app.dispose();
    }
  });

  test("requires operate scope for every mutation while allowing read-only state", async () => {
    const app = fixture(["orchestration:read"]);
    try {
      const input = {
        stagedHandle: app.state.stagedHandle,
        environmentId: app.state.environmentId,
        currentVersion: app.state.currentVersion,
      };
      for (const action of ["check", "download", "prepare-native", "install"]) {
        const response = await app.handler(
          new Request(`http://fixture/api/jones-updates/${action}`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify(
              action === "download" ? { artifactId: 101, sourceSha: "a".repeat(40) } : input,
            ),
          }),
        );
        expect(response.status).toBe(403);
      }
      expect(app.calls).toEqual([]);
      const response = await app.handler(new Request("http://fixture/api/jones-updates?after=2"));
      expect(response.status).toBe(200);
      expect(app.calls).toEqual([{ action: "state", input: 2 }]);
    } finally {
      await app.dispose();
    }
  });
});

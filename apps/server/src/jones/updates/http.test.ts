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
import * as HostProcess from "@t3tools/shared/HostProcess";
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
import * as UpdateAdmissionFence from "./UpdateAdmissionFence.ts";
import * as SourceRelation from "./sourceRelation.ts";

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
          Layer.provide(UpdateAdmissionFence.layer),
          Layer.provide(ServerConfig.layer({ ...config, mode })),
          Layer.provide(Layer.succeed(HostProcess.Platform, "linux")),
          Layer.provide(Layer.succeed(HostProcess.Architecture, "x64")),
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
          Layer.provide(
            jonesUpdatesHttpApiLayer.pipe(
              Layer.provide(
                Layer.succeed(
                  SourceRelation.JonesSourceRelation,
                  unusedService<SourceRelation.JonesSourceRelation["Service"]>(),
                ),
              ),
            ),
          ),
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
    fleetOperationsSupported: false,
    refreshPolicy: Effect.succeed(undefined),
    retireStagedOperation: () => Effect.succeed({ retired: false }),
    state: (after) => record("state", after),
    check: record("check"),
    download: (input) => record("download", input),
    downloadExact: (input) => record("downloadExact", input),
    prepareNative: (input) => record("prepareNative", input),
    install: (input) => record("install", input),
    stageExact: (input) => record("stageExact", input),
    installForOperation: (input) => record("installForOperation", input),
    reconcileOperation: (operationId) => Effect.succeed({ state: "absent" as const, operationId }),
  });
  const relation = Layer.succeed(SourceRelation.JonesSourceRelation, {
    relation: (pair) =>
      Effect.sync(() => {
        calls.push({ action: "sourceRelation", input: pair });
        return {
          repository: "Jones-Systems/Jones-Code" as const,
          ...pair,
          relation: "ahead" as const,
          observedAt: "2026-10-20T00:00:00.000Z",
        };
      }),
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
    Layer.provide(jonesUpdatesHttpApiLayer.pipe(Layer.provide(Layer.mergeAll(service, relation)))),
    Layer.provide(auth),
    Layer.provide(HttpServer.layerServices),
  );
  return { ...HttpRouter.toWebHandler(routes), calls, state };
}

describe("qualified update HTTP authority and decoding", () => {
  test("round trips fixed download, install, and native preparation attempt inputs with no-store", async () => {
    const app = fixture(["orchestration:read", "orchestration:operate"]);
    try {
      const download = { artifactId: 101, sourceSha: "a".repeat(40) };
      const downloadExact = { targetSource: "b".repeat(40) };
      const install = {
        stagedHandle: app.state.stagedHandle,
        environmentId: app.state.environmentId,
        currentVersion: app.state.currentVersion,
        continueRunningThreads: true,
      };
      const preparation = {
        stagedHandle: "b".repeat(64),
        transactionId: "c".repeat(64),
        environmentId: app.state.environmentId,
        currentVersion: app.state.currentVersion,
      };
      for (const [action, input] of [
        ["download", download],
        ["download-exact", downloadExact],
        ["install", install],
        ["prepare-native", preparation],
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
        { action: "downloadExact", input: downloadExact },
        { action: "install", input: install },
        { action: "prepareNative", input: preparation },
      ]);
    } finally {
      await app.dispose();
    }
  });

  test("rejects malformed install and missing preparation attempt before calling the service", async () => {
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
      const preparation = await app.handler(
        new Request("http://fixture/api/jones-updates/prepare-native", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            stagedHandle: "b".repeat(64),
            environmentId: app.state.environmentId,
            currentVersion: app.state.currentVersion,
          }),
        }),
      );
      expect(preparation.status).toBe(400);
      // A version string or abbreviated commit never selects an exact source.
      for (const targetSource of ["0.1.0-preview.20261019.1.1", "b".repeat(12), "B".repeat(40)]) {
        const exact = await app.handler(
          new Request("http://fixture/api/jones-updates/download-exact", {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ targetSource }),
          }),
        );
        expect(exact.status).toBe(400);
      }
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
      for (const action of ["check", "download", "download-exact", "prepare-native", "install"]) {
        const response = await app.handler(
          new Request(`http://fixture/api/jones-updates/${action}`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify(
              action === "download"
                ? { artifactId: 101, sourceSha: "a".repeat(40) }
                : action === "download-exact"
                  ? { targetSource: "a".repeat(40) }
                  : action === "prepare-native"
                    ? { ...input, stagedHandle: "b".repeat(64), transactionId: "c".repeat(64) }
                    : input,
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

  test("serves exact-pair source relation with read scope only and no-store", async () => {
    const base = "a".repeat(40),
      head = "b".repeat(40);
    const app = fixture(["orchestration:read"]);
    try {
      const response = await app.handler(
        new Request(`http://fixture/api/jones-updates/source-relation?base=${base}&head=${head}`),
      );
      expect(response.status).toBe(200);
      expect(response.headers.get("cache-control")).toBe("no-store");
      expect(await response.json()).toEqual({
        repository: "Jones-Systems/Jones-Code",
        base,
        head,
        relation: "ahead",
        observedAt: "2026-10-20T00:00:00.000Z",
      });
      expect(app.calls).toEqual([{ action: "sourceRelation", input: { base, head } }]);
    } finally {
      await app.dispose();
    }
  });

  test("rejects malformed or unauthorized source relation reads before calling the service", async () => {
    const base = "a".repeat(40);
    const app = fixture(["orchestration:read"]);
    try {
      for (const query of [
        `base=${base}`,
        `base=${base}&head=${base.slice(0, 7)}`,
        `base=${base}&head=${"B".repeat(40)}`,
        `base=${base}&head=repos%2Fother%2Frepo`,
      ]) {
        const response = await app.handler(
          new Request(`http://fixture/api/jones-updates/source-relation?${query}`),
        );
        expect(response.status).toBe(400);
      }
      expect(app.calls).toEqual([]);
    } finally {
      await app.dispose();
    }
    const unscoped = fixture([]);
    try {
      const response = await unscoped.handler(
        new Request(
          `http://fixture/api/jones-updates/source-relation?base=${base}&head=${"b".repeat(40)}`,
        ),
      );
      expect(response.status).toBe(403);
      expect(unscoped.calls).toEqual([]);
    } finally {
      await unscoped.dispose();
    }
  });
});

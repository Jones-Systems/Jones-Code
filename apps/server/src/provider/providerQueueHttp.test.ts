import { describe, expect, it } from "vite-plus/test";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as HttpApi from "effect/unstable/httpapi/HttpApi";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import * as HttpServer from "effect/unstable/http/HttpServer";
import {
  AuthSessionId,
  EnvironmentAuthenticatedAuth,
  EnvironmentAuthenticatedPrincipal,
  ProviderInstanceId,
  ProviderDriverKind,
  ProviderQueueHttpApi,
  type AuthEnvironmentScope,
} from "@t3tools/contracts";
import { makeProviderQueueHttpApiLayer } from "./providerQueueHttp.ts";
import { makeProviderQueue, ProviderQueueStorageError } from "./providerQueue.ts";

const api = HttpApi.make("environment").add(ProviderQueueHttpApi);
function fixture(scopes: AuthEnvironmentScope[], known = false) {
  let probes = 0;
  const queue = makeProviderQueue(
    {
      getProviders: Effect.succeed(
        known
          ? [
              {
                instanceId: ProviderInstanceId.make("known"),
                driver: ProviderDriverKind.make("codex"),
                displayName: "Known",
                enabled: true,
                installed: true,
                version: null,
                status: "ready" as const,
                auth: { status: "authenticated" as const },
                checkedAt: "2026-09-30T12:00:00.000Z",
                models: [],
                slashCommands: [],
                skills: [],
              },
            ]
          : [],
      ),
      refreshInstance: () =>
        Effect.sync(() => {
          probes++;
          return [];
        }),
    },
    {
      read: Effect.fail(new ProviderQueueStorageError({ code: "state_unavailable" })),
      write: () => Effect.fail(new ProviderQueueStorageError({ code: "state_unavailable" })),
    },
  );
  const auth = Layer.succeed(EnvironmentAuthenticatedAuth, (httpEffect) =>
    httpEffect.pipe(
      Effect.provideService(EnvironmentAuthenticatedPrincipal, {
        sessionId: AuthSessionId.make("fixture"),
        subject: "fixture",
        method: "bearer-access-token",
        scopes: new Set(scopes),
      }),
    ),
  );
  const routes = HttpApiBuilder.layer(api).pipe(
    Layer.provide(makeProviderQueueHttpApiLayer(queue)),
    Layer.provide(auth),
    Layer.provide(HttpServer.layerServices),
  );
  return { ...HttpRouter.toWebHandler(routes), probes: () => probes };
}

describe("provider queue HTTP boundaries", () => {
  it("requires admin refresh scope and read scope on both read routes", async () => {
    const f = fixture([]);
    try {
      for (const [method, path] of [
        ["GET", "inventory"],
        ["GET", "instances/missing/usage"],
        ["POST", "instances/missing/refresh"],
      ] as const) {
        const response = await f.handler(
          new Request(`http://localhost/api/provider-queue/${path}`, { method }),
          Context.empty(),
        );
        expect(response.status).toBe(403);
      }
      expect(f.probes()).toBe(0);
    } finally {
      await f.dispose();
    }
  });
  it("returns selected inventory and typed unknown-instance results without probing", async () => {
    const f = fixture(["orchestration:read", "access:write"]);
    try {
      const response = await f.handler(
        new Request("http://localhost/api/provider-queue/inventory"),
        Context.empty(),
      );
      expect(response.status).toBe(200);
      expect(await response.json()).toMatchObject({
        instances: [],
        evidence: "configured-provider-registry",
      });
      for (const [method, path] of [
        ["GET", "usage"],
        ["POST", "refresh"],
      ] as const) {
        const result = await f.handler(
          new Request(`http://localhost/api/provider-queue/instances/missing/${path}`, { method }),
          Context.empty(),
        );
        expect(result.status).toBe(200);
        expect(await result.json()).toMatchObject({ status: "unknown_instance", quota: null });
      }
      expect(f.probes()).toBe(0);
    } finally {
      await f.dispose();
    }
  });
  it("returns a closed unavailable result on storage failure without private exceptions", async () => {
    const f = fixture(["orchestration:read", "access:write"], true);
    try {
      for (const [method, path] of [
        ["GET", "usage"],
        ["POST", "refresh"],
      ] as const) {
        const response = await f.handler(
          new Request(`http://localhost/api/provider-queue/instances/known/${path}`, { method }),
          Context.empty(),
        );
        expect(response.status).toBe(200);
        expect(await response.json()).toEqual({
          instanceId: "known",
          status: "unavailable",
          nextRefreshAt: null,
          quota: null,
        });
      }
      expect(f.probes()).toBe(0);
    } finally {
      await f.dispose();
    }
  });
});

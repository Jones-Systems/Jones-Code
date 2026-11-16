import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  AuthOrchestrationReadScope,
  AuthSessionId,
  EnvironmentAuthenticatedAuth,
  EnvironmentAuthenticatedPrincipal,
  EnvironmentAuthInvalidError,
  EnvironmentHttpApi,
  type HostStatusSnapshot,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Etag from "effect/http/Etag";
import * as HttpPlatform from "effect/http/HttpPlatform";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import { expect, it, vi } from "vite-plus/test";

import * as HostStatus from "./HostStatus.ts";
import { hostStatusHttpApiLayer } from "./http.ts";

class TestApi extends HttpApi.make("environment").add(EnvironmentHttpApi.groups.hostStatus) {}
const unavailable: HostStatusSnapshot = {
  hosts: ["vps", "test", "mini", "home"].map((id) => ({
    id: id as HostStatusSnapshot["hosts"][number]["id"],
    status: "unavailable",
    reason: "not_configured",
  })),
};

// Synthetic authentication boundary only; these fixtures never open the auth
// store, read process configuration, or contact a collector.
const authLayer = Layer.succeed(EnvironmentAuthenticatedAuth, (handler) =>
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const credential = request.headers.authorization ?? request.headers.cookie;
    if (!credential) {
      return yield* new EnvironmentAuthInvalidError({
        code: "auth_invalid",
        reason: "missing_credential",
        traceId: "test",
      });
    }
    const allowed = credential === "Bearer fixture-allowed" || credential === "fixture=allowed";
    return yield* handler.pipe(
      Effect.provideService(EnvironmentAuthenticatedPrincipal, {
        sessionId: AuthSessionId.make("fixture-session"),
        subject: "fixture",
        method: request.headers.authorization ? "bearer-access-token" : "browser-session-cookie",
        scopes: new Set(allowed ? [AuthOrchestrationReadScope] : []),
      }),
    );
  }),
);

function makeApp(effect = Effect.succeed(unavailable)) {
  const snapshot = vi.fn(() => effect);
  const app = HttpRouter.toWebHandler(
    HttpApiBuilder.layer(TestApi).pipe(
      Layer.provide(hostStatusHttpApiLayer),
      Layer.provide(Layer.succeed(HostStatus.HostStatus, { snapshot })),
      Layer.provide(authLayer),
      Layer.provide(HttpPlatform.layer.pipe(Layer.provide(NodeServices.layer))),
      Layer.provide(Etag.layerWeak),
      Layer.provide(NodeServices.layer),
    ),
    { disableLogger: true },
  );
  return { app, snapshot };
}

it("requires authentication and read scope before invoking the host service", async () => {
  const { app, snapshot } = makeApp();
  try {
    const unauthenticated = await app.handler(new Request("http://fixture/api/host-status"));
    expect(unauthenticated.status).toBe(401);
    expect(await unauthenticated.json()).toMatchObject({ code: "auth_invalid" });
    for (const headers of [
      { authorization: "Bearer fixture-denied" },
      { cookie: "fixture=denied" },
    ]) {
      const denied = await app.handler(new Request("http://fixture/api/host-status", { headers }));
      expect(denied.status).toBe(403);
      expect(await denied.json()).toMatchObject({
        code: "insufficient_scope",
        requiredScope: AuthOrchestrationReadScope,
      });
    }
    expect(snapshot).not.toHaveBeenCalled();
  } finally {
    await app.dispose();
  }
});

it.each([{ authorization: "Bearer fixture-allowed" }, { cookie: "fixture=allowed" }])(
  "serves no-store, ordered unavailable data through authenticated cookie or bearer requests",
  async (headers) => {
    const { app, snapshot } = makeApp();
    try {
      const response = await app.handler(
        new Request(
          "http://fixture/api/host-status?url=https://untrusted.example&token=untrusted",
          { headers },
        ),
      );
      expect(response.status).toBe(200);
      expect(response.headers.get("cache-control")).toBe("no-store");
      expect(await response.json()).toEqual(unavailable);
      expect(snapshot).toHaveBeenCalledExactlyOnceWith();
    } finally {
      await app.dispose();
    }
  },
);

it("preserves available and unavailable states without exposing collector configuration", async () => {
  const result: HostStatusSnapshot = {
    hosts: [
      {
        id: "vps",
        status: "available",
        cpuUsagePercent: 24,
        logicalCpuCount: 8,
        occupiedMemoryBytes: 4 * 1024 ** 3,
        availableMemoryBytes: 6 * 1024 ** 3,
        totalMemoryBytes: 8 * 1024 ** 3,
        sampledAt: "2026-10-04T12:00:00.000Z",
      },
      { id: "test", status: "unavailable", reason: "stale" },
      { id: "mini", status: "unavailable", reason: "invalid_response" },
      { id: "home", status: "unavailable", reason: "upstream_unavailable" },
    ],
  };
  const { app } = makeApp(Effect.succeed(result));
  try {
    const response = await app.handler(
      new Request("http://fixture/api/host-status", {
        headers: { authorization: "Bearer fixture-allowed" },
      }),
    );
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual(result);
  } finally {
    await app.dispose();
  }
});

it("maps an unexpected service defect to the environment API error envelope", async () => {
  const { app } = makeApp(Effect.die(new Error("synthetic-private-detail")));
  try {
    const response = await app.handler(
      new Request("http://fixture/api/host-status", {
        headers: { authorization: "Bearer fixture-allowed" },
      }),
    );
    expect(response.status).toBe(500);
    expect(response.headers.get("cache-control")).toBe("no-store");
    const body = await response.json();
    expect(body).toMatchObject({ code: "internal_error", reason: "internal_error" });
    expect(JSON.stringify(body)).not.toContain("synthetic-private-detail");
  } finally {
    await app.dispose();
  }
});

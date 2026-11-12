import { describe, expect, it } from "vite-plus/test";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpServer from "effect/http/HttpServer";
import {
  AuthSessionId,
  EnvironmentAuthenticatedAuth,
  EnvironmentAuthenticatedPrincipal,
  EnvironmentAuthInvalidError,
  EnvironmentHttpApi,
} from "@t3tools/contracts";
import * as RetiredRoutes from "./workstreams/personal/http.ts";

function fixture(authenticated: boolean) {
  const auth = Layer.succeed(EnvironmentAuthenticatedAuth, (effect) =>
    authenticated
      ? effect.pipe(
          Effect.provideService(EnvironmentAuthenticatedPrincipal, {
            sessionId: AuthSessionId.make("legacy-http-fixture"),
            subject: "fixture",
            method: "bearer-access-token",
            scopes: new Set(["orchestration:read"] as const),
          }),
        )
      : Effect.fail(
          new EnvironmentAuthInvalidError({
            code: "auth_invalid",
            reason: "missing_credential",
            traceId: "fixture",
          }),
        ),
  );
  return HttpRouter.toWebHandler(
    HttpApiBuilder.layer(
      HttpApi.make("environment").add(EnvironmentHttpApi.groups.workstreams),
    ).pipe(
      Layer.provide(RetiredRoutes.layer),
      Layer.provide(auth),
      Layer.provide(HttpServer.layerServices),
    ),
  );
}

describe("retired Workstreams HTTP compatibility", () => {
  it.each(Object.values(EnvironmentHttpApi.groups.workstreams.endpoints))(
    "$method $path returns the exact unavailable failure for an old client",
    async (endpoint) => {
      const server = fixture(true);
      try {
        const path = endpoint.path.replace(/:[A-Za-z]+/g, "synthetic");
        const response = await server.handler(
          new Request("http://localhost" + path, {
            method: endpoint.method,
            ...(endpoint.method === "POST"
              ? {
                  headers: { "content-type": "application/json" },
                  body: JSON.stringify({ legacy: true }),
                }
              : {}),
          }),
        );
        expect(response.status).toBe(409);
        expect(await response.json()).toEqual({
          _tag: "EnvironmentHttpConflictError",
          message: "workstreams_not_configured",
        });
      } finally {
        await server.dispose();
      }
    },
  );
  it("keeps ordinary authentication ahead of unavailable responses", async () => {
    const server = fixture(false);
    try {
      expect((await server.handler(new Request("http://localhost/api/workstreams"))).status).toBe(
        401,
      );
    } finally {
      await server.dispose();
    }
  });
  it("does not intercept unrelated paths or removed native owner routes", async () => {
    const server = fixture(true);
    try {
      for (const path of [
        "/api/workstreams/native/v1/context",
        "/api/workstreams/native/owner/v1/operations",
        "/api/unrelated",
      ]) {
        expect((await server.handler(new Request("http://localhost" + path))).status).toBe(404);
      }
    } finally {
      await server.dispose();
    }
  });
});

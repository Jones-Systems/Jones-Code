import { describe, expect, it } from "vite-plus/test";
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
  EnvironmentAuthInvalidError,
  EnvironmentWorkQueueMetadataHttpApi,
  type AuthEnvironmentScope,
} from "@t3tools/contracts";
import { workQueueMetadataHttpApiLayer } from "./http.ts";
import * as Service from "./WorkQueueMetadataService.ts";

function fixture(scopes: AuthEnvironmentScope[] | null) {
  let reads = 0;
  const auth = Layer.succeed(EnvironmentAuthenticatedAuth, (effect) =>
    scopes === null
      ? Effect.fail(
          new EnvironmentAuthInvalidError({
            code: "auth_invalid",
            reason: "missing_credential",
            traceId: "fixture",
          }),
        )
      : effect.pipe(
          Effect.provideService(EnvironmentAuthenticatedPrincipal, {
            sessionId: AuthSessionId.make("fixture"),
            subject: "fixture",
            method: "bearer-access-token",
            scopes: new Set(scopes),
          }),
        ),
  );
  const service = Layer.succeed(Service.WorkQueueMetadataService, {
    snapshot: Effect.sync(() => {
      reads++;
      return { status: "unconfigured" as const, reason: "not_configured" as const };
    }),
  });
  const routes = HttpApiBuilder.layer(
    HttpApi.make("environment").add(EnvironmentWorkQueueMetadataHttpApi),
  ).pipe(
    Layer.provide(workQueueMetadataHttpApiLayer.pipe(Layer.provide(service))),
    Layer.provide(auth),
    Layer.provide(HttpServer.layerServices),
  );
  return { ...HttpRouter.toWebHandler(routes), reads: () => reads };
}

describe("queue metadata HTTP authority", () => {
  it.each([
    [null, 401],
    [[], 403],
    [["orchestration:read"], 200],
  ] as const)(
    "enforces authentication and read scope before reading: %j",
    async (scopes, status) => {
      const server = fixture(scopes === null ? null : [...scopes]);
      try {
        const result = await server.handler(
          new Request("http://localhost/api/work-queue/metadata"),
        );
        expect(result.status).toBe(status);
        expect(server.reads()).toBe(status === 200 ? 1 : 0);
        if (status === 200) {
          expect(await result.json()).toEqual({ status: "unconfigured", reason: "not_configured" });
          expect(result.headers.get("cache-control")).toBe("no-store");
        }
      } finally {
        await server.dispose();
      }
    },
  );
  it("has no mutation operation or caller-selected source", async () => {
    const server = fixture(["orchestration:read"]);
    try {
      const result = await server.handler(
        new Request("http://localhost/api/work-queue/metadata?path=/private&host_id=other"),
      );
      expect(result.status).toBe(200);
      expect(await result.json()).toEqual({ status: "unconfigured", reason: "not_configured" });
      expect(
        (
          await server.handler(
            new Request("http://localhost/api/work-queue/metadata", { method: "POST" }),
          )
        ).status,
      ).toBe(404);
      expect(server.reads()).toBe(1);
    } finally {
      await server.dispose();
    }
  });
});

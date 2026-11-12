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
  EnvironmentSettlementOriginsHttpApi,
  type AuthEnvironmentScope,
} from "@t3tools/contracts";
import { settlementOriginsHttpApiLayer } from "./http.ts";
import * as SettlementOrigins from "./SettlementOrigins.ts";

const item = { threadId: "thread:origin", settledAtMs: 1_700_000_000_123 };
const request = (items: readonly unknown[] = [item]) =>
  new Request("http://localhost/api/jones/settlement-origins", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ items }),
  });

function fixture(scopes: AuthEnvironmentScope[] | null, failRead = false) {
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
  const service = Layer.succeed(SettlementOrigins.SettlementOrigins, {
    recordClientSettle: () => Effect.die("Read endpoint must never record a client command"),
    resolve: ({ items }) =>
      Effect.suspend(() => {
        reads++;
        return failRead
          ? Effect.fail(
              new SettlementOrigins.SettlementOriginsReadError({
                cause: new Error("synthetic read failure"),
              }),
            )
          : Effect.succeed({
              items: items.map((query) => ({ ...query, origin: "other" as const })),
            });
      }),
  });
  const routes = HttpApiBuilder.layer(
    HttpApi.make("environment").add(EnvironmentSettlementOriginsHttpApi),
  ).pipe(
    Layer.provide(settlementOriginsHttpApiLayer.pipe(Layer.provide(service))),
    Layer.provide(auth),
    Layer.provide(HttpServer.layerServices),
  );
  return { ...HttpRouter.toWebHandler(routes), reads: () => reads };
}

describe("settlement origins HTTP", () => {
  it.each([
    [null, 401],
    [[], 403],
    [["orchestration:operate"], 403],
    [["orchestration:read"], 200],
  ] as const)("requires authentication and read scope: %j", async (scopes, status) => {
    const server = fixture(scopes === null ? null : [...scopes]);
    try {
      const response = await server.handler(request());
      expect(response.status).toBe(status);
      expect(server.reads()).toBe(status === 200 ? 1 : 0);
      if (status === 200) {
        expect(response.headers.get("cache-control")).toBe("no-store");
        expect(await response.json()).toEqual({ items: [{ ...item, origin: "other" }] });
      }
    } finally {
      await server.dispose();
    }
  });

  it("accepts 100 items and rejects oversized or malformed batches before service access", async () => {
    const server = fixture(["orchestration:read"]);
    try {
      expect((await server.handler(request(Array.from({ length: 100 }, () => item)))).status).toBe(
        200,
      );
      expect((await server.handler(request(Array.from({ length: 101 }, () => item)))).status).toBe(
        400,
      );
      expect((await server.handler(request([{ ...item, settledAtMs: -1 }]))).status).toBe(400);
      expect((await server.handler(request([{ ...item, settledAtMs: 1.5 }]))).status).toBe(400);
      expect(server.reads()).toBe(1);
    } finally {
      await server.dispose();
    }
  });

  it("returns a retryable no-store failure instead of claiming client origin on a read error", async () => {
    const server = fixture(["orchestration:read"], true);
    try {
      const response = await server.handler(request());
      expect(response.status).toBe(500);
      expect(response.headers.get("cache-control")).toBe("no-store");
      expect(await response.json()).toMatchObject({
        code: "internal_error",
        reason: "internal_error",
      });
      expect(server.reads()).toBe(1);
    } finally {
      await server.dispose();
    }
  });
});

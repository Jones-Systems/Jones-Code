import * as NodeServices from "@effect/platform-node/NodeServices";
import * as HttpPlatform from "effect/unstable/http/HttpPlatform";
import * as Etag from "effect/unstable/http/Etag";
import { afterEach, expect, it, vi } from "vite-plus/test";
import {
  AuthOrchestrationReadScope,
  AuthSessionId,
  EnvironmentAuthenticatedAuth,
  EnvironmentAuthenticatedPrincipal,
  EnvironmentAuthInvalidError,
  EnvironmentHttpApi,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as HttpApi from "effect/unstable/httpapi/HttpApi";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import { hostStatusHttpApiLayer } from "./http.ts";

class TestApi extends HttpApi.make("environment").add(EnvironmentHttpApi.groups.hostStatus) {}
const authLayer = Layer.succeed(EnvironmentAuthenticatedAuth, (handler) =>
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const credential = request.headers.authorization;
    if (!credential)
      return yield* new EnvironmentAuthInvalidError({
        code: "auth_invalid",
        reason: "missing_credential",
        traceId: "test",
      });
    return yield* handler.pipe(
      Effect.provideService(EnvironmentAuthenticatedPrincipal, {
        sessionId: AuthSessionId.make("test-session"),
        subject: "test",
        method: "bearer-access-token",
        scopes: new Set(credential === "Bearer allowed" ? [AuthOrchestrationReadScope] : []),
      }),
    );
  }),
);

afterEach(() => {
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
});

it("requires authentication and read scope, then serves a no-store snapshot ignoring upstream query input", async () => {
  for (const id of ["VPS", "TEST", "MINI", "HOME"]) vi.stubEnv(`T3CODE_NETDATA_${id}_URL`, "");
  const fetcher = vi.fn();
  vi.stubGlobal("fetch", fetcher);
  const app = HttpRouter.toWebHandler(
    HttpApiBuilder.layer(TestApi).pipe(
      Layer.provide(hostStatusHttpApiLayer),
      Layer.provide(authLayer),
      Layer.provide(HttpPlatform.layer.pipe(Layer.provide(NodeServices.layer))),
      Layer.provide(Etag.layerWeak),
      Layer.provide(NodeServices.layer),
    ),
    { disableLogger: true },
  );
  try {
    expect((await app.handler(new Request("http://local/api/host-status"))).status).toBe(401);
    expect(
      (
        await app.handler(
          new Request("http://local/api/host-status", {
            headers: { authorization: "Bearer denied" },
          }),
        )
      ).status,
    ).toBe(403);
    const response = await app.handler(
      new Request("http://local/api/host-status?url=https://untrusted.example", {
        headers: { authorization: "Bearer allowed" },
      }),
    );
    expect(response.status).toBe(200);
    expect(response.headers.get("cache-control")).toBe("no-store");
    expect(await response.json()).toEqual({
      hosts: ["vps", "test", "mini", "home"].map((id) => ({
        id,
        status: "unavailable",
        reason: "not_configured",
      })),
    });
    expect(fetcher).not.toHaveBeenCalled();
  } finally {
    await app.dispose();
  }
});

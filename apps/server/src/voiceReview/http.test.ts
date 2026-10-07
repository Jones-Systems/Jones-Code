import * as NodeServices from "@effect/platform-node/NodeServices";
import * as HttpPlatform from "effect/unstable/http/HttpPlatform";
import * as Etag from "effect/unstable/http/Etag";
import { expect, it, vi } from "vite-plus/test";
import {
  AuthSessionId,
  type AuthEnvironmentScope,
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
import { voiceReviewHttpApiLayer, voiceReviewResponseHeadersLayer } from "./http.ts";
import * as VoiceReview from "./bridge.ts";

class TestApi extends HttpApi.make("environment").add(EnvironmentHttpApi.groups.voiceReview) {}
const auth = Layer.succeed(EnvironmentAuthenticatedAuth, (handler) =>
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    if (!request.headers.authorization)
      return yield* new EnvironmentAuthInvalidError({
        code: "auth_invalid",
        reason: "missing_credential",
        traceId: "test",
      });
    return yield* handler.pipe(
      Effect.provideService(EnvironmentAuthenticatedPrincipal, {
        sessionId: AuthSessionId.make(
          request.headers.authorization === "Bearer owner" ? "owner" : "other",
        ),
        subject: "generic-subject",
        method: "bearer-access-token",
        scopes: new Set<AuthEnvironmentScope>(["orchestration:read", "orchestration:operate"]),
      }),
    );
  }),
);
const makeApp = (
  serviceLayer = VoiceReview.layer.pipe(Layer.provide(VoiceReview.dependenciesLayer)),
) =>
  HttpRouter.toWebHandler(
    HttpApiBuilder.layer(TestApi).pipe(
      Layer.provide(voiceReviewHttpApiLayer.pipe(Layer.provide(serviceLayer))),
      Layer.provide(auth),
      Layer.provide(voiceReviewResponseHeadersLayer),
      Layer.provide(HttpPlatform.layer.pipe(Layer.provide(NodeServices.layer))),
      Layer.provide(Etag.layerWeak),
      Layer.provide(NodeServices.layer),
    ),
    { disableLogger: true },
  );

it("serves closed unconfigured and session-denied responses with no-store, without broker access", async () => {
  const fetcher = vi.fn();
  vi.stubGlobal("fetch", fetcher);
  vi.stubEnv("T3CODE_VOICE_REVIEW_BROKER_URL", "");
  let app = makeApp();
  try {
    expect((await app.handler(new Request("http://local/api/voice-review/drafts"))).status).toBe(
      401,
    );
    const response = await app.handler(
      new Request("http://local/api/voice-review/drafts", {
        headers: { authorization: "Bearer owner" },
      }),
    );
    expect(response.status).toBe(503);
    expect(response.headers.get("cache-control")).toBe("no-store");
    expect(await response.json()).toEqual({ _tag: "VoiceReviewNotConfiguredError" });
    await app.dispose();
    vi.stubEnv("T3CODE_VOICE_REVIEW_BROKER_URL", "http://127.0.0.1:7000");
    vi.stubEnv("T3CODE_VOICE_REVIEW_REVIEWER_TOKEN_FILE", "/not-read");
    vi.stubEnv("T3CODE_VOICE_REVIEW_SOURCE_ID", "microphone");
    vi.stubEnv("T3CODE_VOICE_REVIEW_ALLOWED_SESSION_IDS", '["owner"]');
    app = makeApp();
    const denied = await app.handler(
      new Request("http://local/api/voice-review/drafts", {
        headers: { authorization: "Bearer other" },
      }),
    );
    expect(denied.status).toBe(403);
    expect(await denied.json()).toEqual({ _tag: "VoiceReviewForbiddenError" });
    expect(denied.headers.get("cache-control")).toBe("no-store");
    expect(fetcher).not.toHaveBeenCalled();
  } finally {
    await app.dispose();
    vi.unstubAllEnvs();
    vi.unstubAllGlobals();
  }
});
it("rejects strict mutation and list-window violations before credential or broker access", async () => {
  const fetcher = vi.fn();
  vi.stubGlobal("fetch", fetcher);
  const app = makeApp();
  try {
    for (const payload of [
      { expected_revision: true },
      { expected_revision: "1" },
      { expected_revision: 0 },
      { expected_revision: 1, broker_url: "https://evil.test" },
    ]) {
      const response = await app.handler(
        new Request("http://local/api/voice-review/drafts/capture/pause", {
          method: "POST",
          headers: { authorization: "Bearer owner", "content-type": "application/json" },
          body: JSON.stringify(payload),
        }),
      );
      expect(response.status).toBe(400);
      expect(response.headers.get("cache-control")).toBe("no-store");
    }
    expect(
      (
        await app.handler(
          new Request("http://local/api/voice-review/drafts?limit=201", {
            headers: { authorization: "Bearer owner" },
          }),
        )
      ).status,
    ).toBe(400);
    expect(fetcher).not.toHaveBeenCalled();
  } finally {
    await app.dispose();
    vi.unstubAllGlobals();
  }
});

it("delegates authenticated draft reads to the injected domain service", async () => {
  const fetcher = vi.fn();
  vi.stubGlobal("fetch", fetcher);
  const list = vi.fn(() => Effect.succeed({ server_now: "2026-10-02T00:00:00Z", drafts: [] }));
  const app = makeApp(
    Layer.succeed(VoiceReview.VoiceReview, {
      recent: () => Effect.die("unexpected recent"),
      registrySnapshot: () => Effect.die("unexpected registrySnapshot"),
      registryWorkstreams: () => Effect.die("unexpected registryWorkstreams"),
      registryEvents: () => Effect.die("unexpected registryEvents"),
      correctAssociation: () => Effect.die("unexpected correctAssociation"),
      correctLabel: () => Effect.die("unexpected correctLabel"),
      diagnostics: () => Effect.die("unexpected diagnostics"),
      list,
      get: () => Effect.die("unexpected get"),
      mutate: () => Effect.die("unexpected mutation"),
    }),
  );
  try {
    const response = await app.handler(
      new Request("http://local/api/voice-review/drafts?scope=recent&limit=7", {
        headers: { authorization: "Bearer owner" },
      }),
    );
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ server_now: "2026-10-02T00:00:00Z", drafts: [] });
    expect(list).toHaveBeenCalledWith(expect.objectContaining({ sessionId: "owner" }), "recent", 7);
    expect(fetcher).not.toHaveBeenCalled();
  } finally {
    await app.dispose();
    vi.unstubAllGlobals();
  }
});

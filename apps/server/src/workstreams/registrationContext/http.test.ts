import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it as effectIt } from "@effect/vitest";
import {
  AuthOrchestrationReadScope,
  AuthSessionId,
  EnvironmentAuthenticatedAuth,
  EnvironmentAuthenticatedPrincipal,
  EnvironmentAuthInvalidError,
  EnvironmentHttpBadRequestError,
  EnvironmentInternalError,
  EnvironmentScopeRequiredError,
  type AuthEnvironmentScope,
} from "@t3tools/contracts";
import { WorkstreamsRegistrationContextResponse } from "../../../../../packages/contracts/src/workstreamsRegistrationContext.ts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Etag from "effect/unstable/http/Etag";
import * as HttpPlatform from "effect/unstable/http/HttpPlatform";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import * as HttpApi from "effect/unstable/httpapi/HttpApi";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";
import * as HttpApiEndpoint from "effect/unstable/httpapi/HttpApiEndpoint";
import * as HttpApiGroup from "effect/unstable/httpapi/HttpApiGroup";
import { expect, it } from "vite-plus/test";

import { createRegistrationContextHandler } from "./http.ts";
import {
  makeRegistrationFixture,
  response,
  githubSource,
  fixtureTransportError,
} from "./testFixtures.ts";

class TestGroup extends HttpApiGroup.make("registrationContext").add(
  HttpApiEndpoint.get("context", "/api/workstreams/registration-context", {
    success: WorkstreamsRegistrationContextResponse,
    error: [
      EnvironmentScopeRequiredError,
      EnvironmentHttpBadRequestError,
      EnvironmentInternalError,
    ],
  }).middleware(EnvironmentAuthenticatedAuth),
) {}
class TestApi extends HttpApi.make("registration-context-test").add(TestGroup) {}

const authenticated = {
  sessionId: AuthSessionId.make("synthetic-browser-session"),
  subject: "synthetic-browser",
  method: "bearer-access-token" as const,
  scopes: new Set<AuthEnvironmentScope>([AuthOrchestrationReadScope]),
};
const authLayer = Layer.succeed(EnvironmentAuthenticatedAuth, (handler) =>
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const credential = request.headers.authorization;
    if (credential === undefined)
      return yield* new EnvironmentAuthInvalidError({
        code: "auth_invalid",
        reason: "missing_credential",
        traceId: "registration-context-test",
      });
    return yield* handler.pipe(
      Effect.provideService(EnvironmentAuthenticatedPrincipal, {
        ...authenticated,
        scopes:
          credential === "Bearer allowed" ? authenticated.scopes : new Set<AuthEnvironmentScope>(),
      }),
    );
  }),
);

const makeApp = (fixture: ReturnType<typeof makeRegistrationFixture>) =>
  HttpRouter.toWebHandler(
    HttpApiBuilder.layer(TestApi).pipe(
      Layer.provide(
        HttpApiBuilder.group(TestApi, "registrationContext", (handlers) =>
          Effect.succeed(
            handlers.handle("context", createRegistrationContextHandler(fixture.service)),
          ),
        ),
      ),
      Layer.provide(authLayer),
      Layer.provide(HttpPlatform.layer.pipe(Layer.provide(NodeServices.layer))),
      Layer.provide(Etag.layerWeak),
      Layer.provide(NodeServices.layer),
    ),
    { disableLogger: true },
  );
const readRequest = (suffix = "") =>
  new Request(`http://local/api/workstreams/registration-context${suffix}`, {
    headers: { authorization: "Bearer allowed" },
  });

it("authenticates and requires standard read scope before reading registration metadata", async () => {
  const fixture = makeRegistrationFixture();
  const app = makeApp(fixture);
  try {
    expect(
      (await app.handler(new Request("http://local/api/workstreams/registration-context"))).status,
    ).toBe(401);
    expect(
      (
        await app.handler(
          new Request("http://local/api/workstreams/registration-context", {
            headers: { authorization: "Bearer denied" },
          }),
        )
      ).status,
    ).toBe(403);
    expect(fixture.reads).toEqual({ registry: 0, authority: 0, build: 0 });
    const result = await app.handler(readRequest());
    expect(result.status).toBe(200);
    expect(result.headers.get("cache-control")).toBe("private, no-store");
    expect(result.headers.get("x-content-type-options")).toBe("nosniff");
    expect(await result.json()).toEqual(response);
    expect(fixture.reads).toEqual({ registry: 1, authority: 1, build: 1 });
  } finally {
    await app.dispose();
  }
});

it("rejects every query selector before contacting the registry", async () => {
  const fixture = makeRegistrationFixture();
  const app = makeApp(fixture);
  try {
    for (const query of [
      "?owner_id=other-owner",
      "?source_instance_id=other-source",
      "?url=https://private-origin.invalid",
      "?unknown=1",
    ]) {
      const result = await app.handler(readRequest(query));
      expect(result.status).toBe(400);
      expect(await result.json()).toEqual({
        _tag: "EnvironmentHttpBadRequestError",
        message: "workstreams_registration_context_invalid_request",
      });
      expect(result.headers.get("cache-control")).toBe("private, no-store");
    }
    expect(fixture.reads).toEqual({ registry: 0, authority: 0, build: 0 });
  } finally {
    await app.dispose();
  }
});

effectIt.effect(
  "rejects a GET body on the server request stream before reading registration metadata",
  () =>
    Effect.gen(function* () {
      const fixture = makeRegistrationFixture();
      const handler = createRegistrationContextHandler(fixture.service);
      const base = HttpServerRequest.fromWeb(
        new Request("http://local/api/workstreams/registration-context", {
          method: "POST",
          body: '{"source_instance_id":"caller-selected"}',
        }),
      );
      const request = new Proxy(base, {
        get: (target, key) => (key === "method" ? "GET" : Reflect.get(target, key)),
      });
      const error = yield* handler().pipe(
        Effect.provideService(HttpServerRequest.HttpServerRequest, request),
        Effect.provideService(EnvironmentAuthenticatedPrincipal, authenticated),
        Effect.flip,
      );
      assert.instanceOf(error, EnvironmentHttpBadRequestError);
      assert.deepEqual(fixture.reads, { registry: 0, authority: 0, build: 0 });
    }),
);

effectIt.effect("native empty request streams are accepted without weakening body rejection", () =>
  Effect.gen(function* () {
    const fixture = makeRegistrationFixture();
    const base = HttpServerRequest.fromWeb(readRequest());
    const request = new Proxy(base, {
      get: (target, key) =>
        key === "source" ? {} : key === "stream" ? Stream.empty : Reflect.get(target, key),
    });
    const result = yield* createRegistrationContextHandler(fixture.service)().pipe(
      Effect.provideService(HttpServerRequest.HttpServerRequest, request),
      Effect.provideService(EnvironmentAuthenticatedPrincipal, authenticated),
    );
    assert.deepEqual(result, response);
  }),
);

it("returns only the qualified closed response while GitHub remains available without native build metadata", async () => {
  const fixture = makeRegistrationFixture({ build: Effect.succeedNone });
  const app = makeApp(fixture);
  try {
    const result = await app.handler(readRequest());
    expect(result.status).toBe(200);
    expect(await result.json()).toEqual({ ...response, sources: [githubSource] });
    expect(result.headers.get("cache-control")).toBe("private, no-store");
  } finally {
    await app.dispose();
  }
});

it("upstream failures return fixed environment errors without origins, credentials, sessions or raw reports", async () => {
  const fixture = makeRegistrationFixture({
    readRegistrationContext: Effect.fail(
      fixtureTransportError("https://private-origin.invalid Bearer private-header private-session"),
    ),
  });
  const app = makeApp(fixture);
  try {
    const result = await app.handler(readRequest());
    expect(result.status).toBe(500);
    const text = await result.text();
    expect(Schema.decodeSync(Schema.fromJsonString(Schema.Unknown))(text)).toMatchObject({
      _tag: "EnvironmentInternalError",
      code: "internal_error",
      reason: "internal_error",
    });
    for (const secret of ["private-origin", "private-header", "private-session"])
      expect(text).not.toContain(secret);
    expect(result.headers.get("cache-control")).toBe("private, no-store");
  } finally {
    await app.dispose();
  }
});

import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  AuthSessionId,
  EnvironmentAuthenticatedAuth,
  EnvironmentAuthenticatedPrincipal,
  EnvironmentAuthInvalidError,
  EnvironmentHttpApi,
  WorkstreamsNativeAttestationRequest,
  WorkstreamsNativeContextResponse,
  WorkstreamsNativeAttestationResponse,
  WorkstreamsNativeSettlementResponse,
  type AuthEnvironmentScope,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Etag from "effect/http/Etag";
import * as HttpPlatform from "effect/http/HttpPlatform";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import { expect, it } from "vite-plus/test";

import { workstreamResponseHeadersLayer } from "../../../workstreams/http.ts";
import { NATIVE_PROVIDER_SCOPES } from "../nativeProvider/enrollment.ts";
import {
  binding,
  makeProviderFixture,
  requestText,
  attestationRequest,
} from "../nativeProvider/testFixtures.ts";
import { NativeWorkstreamsRuntime, nativeWorkstreamsHttpApiLayer } from "./native.ts";

class NativeTestApi extends HttpApi.make("environment").add(
  EnvironmentHttpApi.groups.workstreamsNative,
) {}

const authLayer = Layer.succeed(EnvironmentAuthenticatedAuth, (handler) =>
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const credential = request.headers.authorization;
    if (credential === undefined)
      return yield* new EnvironmentAuthInvalidError({
        code: "auth_invalid",
        reason: "missing_credential",
        traceId: "native-integration-test",
      });
    return yield* handler.pipe(
      Effect.provideService(EnvironmentAuthenticatedPrincipal, {
        sessionId: AuthSessionId.make(binding.session_id),
        subject:
          credential === "Bearer wrong-subject"
            ? "other-subject"
            : `workstreams-native:${binding.enrollment_id}`,
        method: credential === "Bearer cookie" ? "browser-session-cookie" : "bearer-access-token",
        scopes: new Set<AuthEnvironmentScope>(
          credential === "Bearer browser"
            ? ["orchestration:read"]
            : Object.values(NATIVE_PROVIDER_SCOPES),
        ),
      }),
    );
  }),
);

const makeApp = (enrolled = true) => {
  const fixture = makeProviderFixture();
  let lookups = 0;
  const app = HttpRouter.toWebHandler(
    HttpApiBuilder.layer(NativeTestApi).pipe(
      Layer.provide(nativeWorkstreamsHttpApiLayer),
      Layer.provide(
        Layer.succeed(NativeWorkstreamsRuntime, {
          provider: fixture.provider(),
          enrollments: {
            getBySessionId: () =>
              Effect.sync(() => {
                lookups += 1;
                return enrolled ? Option.some(binding) : Option.none();
              }),
          },
        }),
      ),
      Layer.provide(authLayer),
      Layer.provide(workstreamResponseHeadersLayer),
      Layer.provide(HttpPlatform.layer.pipe(Layer.provide(NodeServices.layer))),
      Layer.provide(Etag.layerWeak),
      Layer.provide(NodeServices.layer),
    ),
    { disableLogger: true },
  );
  return { ...app, fixture, lookups: () => lookups };
};

const request = (operation: string, credential = "Bearer enrolled", body?: string) =>
  new Request(`http://local/api/workstreams/native/v1/${operation}`, {
    method: operation === "context" || operation.startsWith("context?") ? "GET" : "POST",
    headers: { authorization: credential, "content-type": "application/json" },
    ...(body === undefined ? {} : { body }),
  });

it("authenticates and checks dedicated scope before consulting enrollment", async () => {
  const app = makeApp();
  try {
    expect(
      (await app.handler(new Request("http://local/api/workstreams/native/v1/context"))).status,
    ).toBe(401);
    expect((await app.handler(request("context", "Bearer browser"))).status).toBe(403);
    expect(app.lookups()).toBe(0);
    for (const credential of ["Bearer wrong-subject", "Bearer cookie"]) {
      const response = await app.handler(request("context", credential));
      expect(response.status).toBe(403);
      expect(
        Schema.decodeUnknownSync(WorkstreamsNativeContextResponse)(await response.json()),
      ).toEqual({
        protocol: "workstreams-t3-provider/1.0.0",
        state: "rejected",
        reason: "forbidden",
      });
    }
    expect(app.fixture.calls).toHaveLength(0);
  } finally {
    await app.dispose();
  }
});

it("fails closed when an authenticated dedicated session has no enrollment", async () => {
  const app = makeApp(false);
  try {
    for (const operation of ["context", "attestations", "settlements", "settlements/lookup"]) {
      const response = await app.handler(request(operation));
      expect(response.status).toBe(403);
    }
    expect(app.fixture.calls).toHaveLength(0);
    expect(app.fixture.attempts.size).toBe(0);
  } finally {
    await app.dispose();
  }
});

it("mounts all four closed native handlers and preserves the raw settlement digest", async () => {
  const app = makeApp();
  try {
    const context = await app.handler(request("context"));
    expect(context.status).toBe(200);
    expect(context.headers.get("cache-control")).toBe("private, no-store");
    expect(
      Schema.decodeUnknownSync(WorkstreamsNativeContextResponse)(await context.json()).state,
    ).toBe("ready");
    const text = Schema.encodeSync(Schema.fromJsonString(WorkstreamsNativeAttestationRequest))(
      attestationRequest,
    );
    const attestation = await app.handler(request("attestations", "Bearer enrolled", text));
    expect(
      Schema.decodeUnknownSync(WorkstreamsNativeAttestationResponse)(await attestation.json())
        .state,
    ).toBe("attested");
    const settled = await app.handler(request("settlements", "Bearer enrolled", requestText));
    expect(settled.status).toBe(200);
    const terminal = Schema.decodeUnknownSync(WorkstreamsNativeSettlementResponse)(
      await settled.json(),
    );
    expect(terminal.state).toBe("terminal");
    const lookup = await app.handler(request("settlements/lookup", "Bearer enrolled", requestText));
    expect(
      Schema.decodeUnknownSync(WorkstreamsNativeSettlementResponse)(await lookup.json()),
    ).toEqual(terminal);
    const changed = await app.handler(request("settlements", "Bearer enrolled", ` ${requestText}`));
    expect(changed.status).toBe(409);
    expect(
      Schema.decodeUnknownSync(WorkstreamsNativeSettlementResponse)(await changed.json()),
    ).toEqual({
      protocol: "workstreams-t3-provider/1.0.0",
      state: "rejected",
      reason: "idempotency_conflict",
    });
    expect(app.fixture.calls).toHaveLength(1);
  } finally {
    await app.dispose();
  }
});

it("rejects query selectors and excessive raw bodies without reserving a command", async () => {
  const app = makeApp();
  try {
    expect((await app.handler(request("context?source_instance_id=other"))).status).toBe(400);
    expect(
      (await app.handler(request("settlements?owner_id=other", "Bearer enrolled", requestText)))
        .status,
    ).toBe(400);
    expect(
      (await app.handler(request("settlements", "Bearer enrolled", "x".repeat(32_769)))).status,
    ).toBe(400);
    expect(app.fixture.calls).toHaveLength(0);
    expect(app.fixture.attempts.size).toBe(0);
  } finally {
    await app.dispose();
  }
});

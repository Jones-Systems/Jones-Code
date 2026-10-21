import { describe, expect, it } from "vite-plus/test";
import {
  AuthSessionId,
  AuthAdministrativeScopes,
  EnvironmentHttpApi,
  type ServerAuthSessionMethod,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpServer from "effect/http/HttpServer";
import * as AuthHttp from "../../auth/http.ts";
import * as EnvironmentAuth from "../../auth/EnvironmentAuth.ts";
import * as Intake from "./RemoteAdmissionIntake.ts";
import { remoteAdmissionHttpApiLayer } from "./http.ts";
import { encodeJson } from "./json.ts";

function fixture(subject: string, method: ServerAuthSessionMethod) {
  let decisions = 0;
  const auth = Layer.succeed(EnvironmentAuth.EnvironmentAuth, {
    authenticateHttpRequest: () =>
      Effect.succeed({
        sessionId: AuthSessionId.make("synthetic-session"),
        subject,
        method,
        scopes: AuthAdministrativeScopes,
      }),
  } as unknown as EnvironmentAuth.EnvironmentAuth["Service"]);
  const service = Layer.succeed(Intake.RemoteAdmissionIntake, {
    attention: () => Effect.succeed({ count: 0 }),
    snapshot: () =>
      Effect.succeed({
        eligible: true,
        policy: { enabled: false, revision: "initial", modelSelection: null },
        pending: [],
      }),
    decide: () => {
      decisions++;
      return Effect.fail(
        new Intake.RemoteAdmissionIntakeError({ cause: "synthetic invocation reached service" }),
      );
    },
  } as unknown as Intake.RemoteAdmissionIntake["Service"]);
  const routes = HttpApiBuilder.layer(
    HttpApi.make("environment").add(EnvironmentHttpApi.groups.remoteAdmission),
  ).pipe(
    Layer.provide(remoteAdmissionHttpApiLayer),
    Layer.provide(service),
    Layer.provide(AuthHttp.layerAuthenticatedAuth),
    Layer.provide(auth),
    Layer.provide(HttpServer.layerServices),
  );
  return { ...HttpRouter.toWebHandler(routes), decisions: () => decisions };
}

describe("remote admission HTTP owner boundary", () => {
  it.each([
    ["desktop-bootstrap", "browser-session-cookie", 200],
    ["administrative-bootstrap", "browser-session-cookie", 200],
    ["mcp-client", "bearer-access-token", 403],
    ["mcp-client", "browser-session-cookie", 403],
    ["reusable-dev-token-child", "browser-session-cookie", 200],
    ["agent", "bearer-access-token", 403],
    ["one-time-token", "browser-session-cookie", 200],
    ["desktop-bootstrap", "dpop-access-token", 403],
  ] as const)("uses authenticated session %s/%s", async (subject, method, expected) => {
    const server = fixture(subject, method);
    try {
      const response = await server.handler(
        new Request("http://localhost/api/jones/remote-admission"),
      );
      expect(response.status).toBe(expected);
      if (expected === 200) expect(response.headers.get("cache-control")).toBe("no-store");
      const decision = await server.handler(
        new Request("http://localhost/api/jones/remote-admission/decision", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: encodeJson({
            id: "synthetic-admission",
            bindingDigest: "a".repeat(64),
            decision: "approve",
            principal: { kind: "human-owner" },
          }),
        }),
      );
      expect(decision.status).toBe(expected === 200 ? 500 : 403);
      expect(server.decisions()).toBe(expected === 200 ? 1 : 0);
    } finally {
      await server.dispose();
    }
  });
});

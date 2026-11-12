import { describe, expect, it } from "vite-plus/test";
import {
  AuthSessionId,
  AuthAccessWriteScope,
  AuthSettingsWriteScope,
  type AuthEnvironmentScope,
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

function fixture(
  subject: string,
  method: ServerAuthSessionMethod,
  scopes: ReadonlyArray<AuthEnvironmentScope> = AuthAdministrativeScopes,
) {
  let decisions = 0;
  let policyUpdates = 0;
  const auth = Layer.succeed(EnvironmentAuth.EnvironmentAuth, {
    authenticateHttpRequest: () =>
      Effect.succeed({
        sessionId: AuthSessionId.make("synthetic-session"),
        subject,
        method,
        scopes,
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
    updatePolicy: () =>
      Effect.suspend(() => {
        policyUpdates++;
        return Effect.fail(
          new Intake.RemoteAdmissionIntakeError({ cause: "synthetic invocation reached service" }),
        );
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
  return {
    ...HttpRouter.toWebHandler(routes),
    decisions: () => decisions,
    policyUpdates: () => policyUpdates,
  };
}

describe("remote admission HTTP owner boundary", () => {
  it.each([
    ["desktop-bootstrap", "browser-session-cookie", 200],
    ["desktop-bootstrap", "bearer-access-token", 200],
    ["administrative-bootstrap", "browser-session-cookie", 200],
    ["mcp-client", "bearer-access-token", 403],
    ["mcp-client", "browser-session-cookie", 403],
    ["reusable-dev-token-child", "browser-session-cookie", 200],
    ["agent", "bearer-access-token", 403],
    ["cli-issued-session", "bearer-access-token", 403],
    ["one-time-token", "bearer-access-token", 403],
    ["administrative-bootstrap", "bearer-access-token", 403],
    ["reusable-dev-token-child", "bearer-access-token", 403],
    ["desktop-bootstrap-impostor", "bearer-access-token", 403],
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
      const attention = await server.handler(
        new Request("http://localhost/api/jones/remote-admission/attention"),
      );
      expect(attention.status).toBe(expected);
      const policy = await server.handler(
        new Request("http://localhost/api/jones/remote-admission/policy", {
          method: "PUT",
          headers: { "content-type": "application/json" },
          body: encodeJson({ enabled: false, expectedRevision: "initial", modelSelection: null }),
        }),
      );
      expect(policy.status).toBe(expected === 200 ? 500 : 403);
      expect(server.policyUpdates()).toBe(expected === 200 ? 1 : 0);
      const decision = await server.handler(
        new Request("http://localhost/api/jones/remote-admission/decision", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: encodeJson({
            id: "synthetic-admission",
            bindingDigest: "a".repeat(64),
            decision: "approve",
            principal: {
              kind: "human-owner",
              subject: "desktop-bootstrap",
              method: "bearer-access-token",
            },
          }),
        }),
      );
      expect(decision.status).toBe(expected === 200 ? 500 : 403);
      expect(server.decisions()).toBe(expected === 200 ? 1 : 0);
    } finally {
      await server.dispose();
    }
  });
  it.each([
    ["desktop-bootstrap", "bearer-access-token", [AuthAccessWriteScope]],
    ["desktop-bootstrap", "bearer-access-token", [AuthSettingsWriteScope]],
    ["desktop-bootstrap", "bearer-access-token", []],
    ["administrative-bootstrap", "browser-session-cookie", [AuthAccessWriteScope]],
    ["administrative-bootstrap", "browser-session-cookie", [AuthSettingsWriteScope]],
  ] as const)("requires both owner scopes for %s/%s with %j", async (subject, method, scopes) => {
    const server = fixture(subject, method, scopes);
    try {
      for (const path of ["", "/attention"]) {
        const response = await server.handler(
          new Request(`http://localhost/api/jones/remote-admission${path}`),
        );
        expect(response.status).toBe(403);
      }
      for (const [path, method, body] of [
        ["policy", "PUT", { enabled: false, expectedRevision: "initial", modelSelection: null }],
        [
          "decision",
          "POST",
          { id: "synthetic-admission", bindingDigest: "a".repeat(64), decision: "approve" },
        ],
      ] as const) {
        const response = await server.handler(
          new Request(`http://localhost/api/jones/remote-admission/${path}`, {
            method,
            headers: { "content-type": "application/json" },
            body: encodeJson(body),
          }),
        );
        expect(response.status).toBe(403);
      }
      expect(server.decisions()).toBe(0);
      expect(server.policyUpdates()).toBe(0);
    } finally {
      await server.dispose();
    }
  });
});

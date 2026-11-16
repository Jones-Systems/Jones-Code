import { describe, expect, it } from "vite-plus/test";
import {
  AuthSessionId,
  AuthAccessWriteScope,
  AuthSettingsWriteScope,
  AuthAdministrativeScopes,
  EnvironmentHttpApi,
  type AuthEnvironmentScope,
  type ServerAuthSessionMethod,
} from "@t3tools/contracts";
import { REMOTE_ADMISSION_REVIEW_VERSION } from "@t3tools/contracts/jones/remoteAdmissionReview";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpServer from "effect/http/HttpServer";
import * as AuthHttp from "../../auth/http.ts";
import * as EnvironmentAuth from "../../auth/EnvironmentAuth.ts";
import { RemoteAdmissionIntakeError } from "./RemoteAdmissionIntake.ts";
import { RemoteAdmissionReviewService } from "./ReviewService.ts";
import { remoteAdmissionReviewHttpApiLayer } from "./http.ts";
import { isRemoteAdmissionOwner } from "./owner.ts";
function fixture(
  subject: string,
  method: ServerAuthSessionMethod,
  scopes: ReadonlyArray<AuthEnvironmentScope>,
  supported = true,
) {
  const calls = { decision: 0, restore: 0, retry: 0, recheck: 0 };
  const auth = Layer.mock(EnvironmentAuth.EnvironmentAuth)({
    authenticateHttpRequest: () =>
      Effect.succeed({
        sessionId: AuthSessionId.make("synthetic-review-session"),
        subject,
        method,
        scopes,
      }),
  });
  const reached = (kind: keyof typeof calls) =>
    Effect.suspend(() => {
      calls[kind]++;
      return Effect.fail(
        new RemoteAdmissionIntakeError({ cause: "synthetic mutation service reached" }),
      );
    });
  const service = Layer.mock(RemoteAdmissionReviewService)({
    capabilities: (principal) =>
      Effect.succeed({
        version: REMOTE_ADMISSION_REVIEW_VERSION,
        features: {
          reviewV2: supported,
          grants: supported,
          conflicts: supported,
          changeFeed: supported,
          grantRestore: supported,
          blockedAudit: supported,
          blockedReview: supported,
          history: supported,
          launchRetry: supported,
          recoveredResult: supported,
          managedAccess: supported,
        },
        principal: { canDecide: isRemoteAdmissionOwner(principal) },
      }),
    decide: () => reached("decision"),
    restore: () => reached("restore"),
    retry: () => reached("retry"),
    recheck: () => reached("recheck"),
  });
  const routes = HttpApiBuilder.layer(
    HttpApi.make("environment").add(EnvironmentHttpApi.groups.remoteAdmissionReview),
  ).pipe(
    Layer.provide(remoteAdmissionReviewHttpApiLayer),
    Layer.provide(service),
    Layer.provide(AuthHttp.layerAuthenticatedAuth),
    Layer.provide(auth),
    Layer.provide(HttpServer.layerServices),
  );
  return { ...HttpRouter.toWebHandler(routes), calls };
}
const digest = "a".repeat(64);
const mutations = [
  ["decision", "/review/decision", { id: "admission", bindingDigest: digest, decision: "approve" }],
  ["decision", "/review/decision", { id: "admission", bindingDigest: digest, decision: "deny" }],
  [
    "restore",
    "/grants/restore",
    {
      grantId: "grant",
      expectedGeneration: 1,
      durationSeconds: 86400,
      commandKey: "original-command",
    },
  ],
  [
    "retry",
    "/review/retry",
    { id: "admission", bindingDigest: digest, commandKey: "retry-command" },
  ],
  ["recheck", "/review/recheck", { id: "admission", bindingDigest: digest }],
] as const;
const base = "http://localhost/api/jones/remote-admission";
describe("V2 review HTTP owner boundary", () => {
  it.each([
    ["mcp-client", "bearer-access-token", AuthAdministrativeScopes, false, true],
    ["desktop-bootstrap", "bearer-access-token", AuthAdministrativeScopes, true, true],
    ["administrative-bootstrap", "browser-session-cookie", AuthAdministrativeScopes, true, true],
    ["desktop-bootstrap", "browser-session-cookie", [AuthAccessWriteScope], false, true],
    ["desktop-bootstrap", "browser-session-cookie", AuthAdministrativeScopes, true, false],
  ] as const)(
    "capabilities stay separate from canDecide for %s/%s",
    async (subject, method, scopes, canDecide, supported) => {
      const server = fixture(subject, method, scopes, supported);
      try {
        const response = await server.handler(new Request(`${base}/capabilities`));
        expect(response.status).toBe(200);
        expect(await response.json()).toMatchObject({
          version: REMOTE_ADMISSION_REVIEW_VERSION,
          features: { reviewV2: supported },
          principal: { canDecide },
        });
        expect(server.calls).toEqual({ decision: 0, restore: 0, retry: 0, recheck: 0 });
      } finally {
        await server.dispose();
      }
    },
  );
  it.each([
    ["mcp-client", "bearer-access-token", AuthAdministrativeScopes],
    ["mcp-client", "browser-session-cookie", AuthAdministrativeScopes],
    ["desktop-bootstrap", "browser-session-cookie", [AuthAccessWriteScope]],
    ["desktop-bootstrap", "browser-session-cookie", [AuthSettingsWriteScope]],
    ["desktop-bootstrap", "bearer-access-token", []],
    ["desktop-bootstrap-impostor", "bearer-access-token", AuthAdministrativeScopes],
  ] as const)(
    "never reaches mutations for %s/%s without exact owner identity and both scopes",
    async (subject, method, scopes) => {
      const server = fixture(subject, method, scopes);
      try {
        for (const [, path, payload] of mutations) {
          const response = await server.handler(
            new Request(`${base}${path}`, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify(payload),
            }),
          );
          expect(response.status).toBe(403);
          expect(await response.json()).toMatchObject({ code: "owner_required" });
        }
        expect(server.calls).toEqual({ decision: 0, restore: 0, retry: 0, recheck: 0 });
      } finally {
        await server.dispose();
      }
    },
  );
  it.each(["browser-session-cookie", "bearer-access-token"] as const)(
    "exact owner %s reaches each requested mutation service",
    async (method) => {
      const server = fixture("desktop-bootstrap", method, AuthAdministrativeScopes);
      try {
        for (const [kind, path, payload] of mutations) {
          const before = server.calls[kind];
          const response = await server.handler(
            new Request(`${base}${path}`, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify(payload),
            }),
          );
          // The synthetic service deliberately fails after entry; this proves route reachability,
          // independently of domain mutation success, with no live receiver effect.
          expect(response.status).toBe(500);
          expect(server.calls[kind]).toBe(before + 1);
        }
        expect(server.calls).toEqual({ decision: 2, restore: 1, retry: 1, recheck: 1 });
      } finally {
        await server.dispose();
      }
    },
  );
});

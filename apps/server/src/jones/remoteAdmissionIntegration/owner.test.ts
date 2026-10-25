import * as NodeServices from "@effect/platform-node/NodeServices";
import { AuthAdministrativeScopes } from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as ServerConfig from "../../config.ts";
import * as ServerEnvironment from "../../environment/ServerEnvironment.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as EnvironmentAuth from "../../auth/EnvironmentAuth.ts";
import * as ServerSecretStore from "../../auth/ServerSecretStore.ts";
import { isRemoteAdmissionOwner } from "./owner.ts";

const bootstrapToken = "synthetic-desktop-bootstrap-token";
const config = Layer.effect(
  ServerConfig.ServerConfig,
  Effect.gen(function* () {
    const defaults = yield* ServerConfig.ServerConfig;
    return { ...defaults, desktopBootstrapToken: bootstrapToken };
  }),
).pipe(Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "remote-admission-owner-" })));
const authLayer = EnvironmentAuth.layer.pipe(
  Layer.provide(SqlitePersistence.layerMemory),
  Layer.provide(ServerSecretStore.layer),
  Layer.provide(ServerEnvironment.layerIdentity),
  Layer.provide(config),
  Layer.provide(NodeServices.layer),
);

it.effect(
  "binds desktop owner authority to the verified bootstrap session, not request claims",
  () =>
    Effect.gen(function* () {
      const auth = yield* EnvironmentAuth.EnvironmentAuth;
      const desktop = yield* auth.exchangeBootstrapCredentialForAccessToken(
        bootstrapToken,
        undefined,
        { deviceType: "desktop" },
      );
      const ordinary = yield* auth.issuePairingCredential({ scopes: AuthAdministrativeScopes });
      const paired = yield* auth.exchangeBootstrapCredentialForAccessToken(
        ordinary.credential,
        undefined,
        { deviceType: "desktop" },
      );
      const cli = yield* auth.issueSession();
      for (const [token, expectedSubject, owner] of [
        [desktop.access_token, "desktop-bootstrap", true],
        [paired.access_token, "one-time-token", false],
        [cli.token, "cli-issued-session", false],
      ] as const) {
        const session = yield* auth.authenticateHttpRequest(
          HttpServerRequest.fromWeb(
            new Request("http://localhost/api/jones/remote-admission", {
              headers: { authorization: `Bearer ${token}`, "x-subject": "desktop-bootstrap" },
            }),
          ),
        );
        expect(session.subject).toBe(expectedSubject);
        expect(session.method).toBe("bearer-access-token");
        expect(isRemoteAdmissionOwner({ ...session, scopes: new Set(session.scopes) })).toBe(owner);
      }
    }).pipe(Effect.provide(authLayer)),
);

// @effect-diagnostics nodeBuiltinImport:off -- One cleanup-owned synthetic T3 home is shared by the CLI and server layers.
import * as NodeFS from "node:fs";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  AuthSessionId,
  EnvironmentHttpApi,
  WorkstreamsNativeAttestationRequest,
  WorkstreamsNativeAttestationResponse,
  WorkstreamsNativeContextResponse,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Etag from "effect/http/Etag";
import * as HttpPlatform from "effect/http/HttpPlatform";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import * as EnvironmentAuth from "../../../auth/EnvironmentAuth.ts";
import * as ServerSecretStore from "../../../auth/ServerSecretStore.ts";
import * as SessionStore from "../../../auth/SessionStore.ts";
import * as AuthHttp from "../../../auth/http.ts";
import * as ServerConfig from "../../../config.ts";
import { readExistingNativeStoreAuthorityState } from "../../../environment/nativeStoreAuthorityPersistence.ts";
import * as ServerEnvironment from "../../../environment/ServerEnvironment.ts";
import * as AuthSessions from "../../../persistence/AuthSessions.ts";
import * as SqlitePersistence from "../../../persistence/Sqlite.ts";
import { workstreamResponseHeadersLayer } from "../../../workstreams/http.ts";
import { NativeProviderEnrollment } from "../nativeProvider/enrollment.ts";
import { makeWorkstreamsNativeProvider } from "../nativeProvider/service.ts";
import { attestationRequest, makeProviderFixture } from "../nativeProvider/testFixtures.ts";
import {
  NativeWorkstreamsRuntime,
  nativeWorkstreamsHttpApiLayer,
} from "../runtimeIntegration/native.ts";
import { prepareWorkstreamProviderRequest, runWorkstreamProviderCliOperation } from "./cli.ts";
import { makeNativeEnrollments } from "./service.ts";
import { syntheticBuild, syntheticT3Home } from "./testHome.ts";

vi.mock("../../../../package.json", () => ({
  default: {
    version: "0.0.0-preview.20261002.100",
    jonesSource: {
      repository: "Jones-Systems/Jones-Code",
      sha: "a".repeat(40),
      tree: "b".repeat(40),
    },
  },
}));

class NativeRouteApi extends HttpApi.make("environment").add(
  EnvironmentHttpApi.groups.workstreamsNative,
) {}

const decodeCredential = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.Struct({ authorization_header: Schema.String })),
);
const decodeContextResponse = Schema.decodeUnknownSync(WorkstreamsNativeContextResponse);
const decodeAttestationResponse = Schema.decodeUnknownSync(WorkstreamsNativeAttestationResponse);
const encodeAttestation = Schema.encodeSync(
  Schema.fromJsonString(WorkstreamsNativeAttestationRequest),
);

// The real server layers this test composes: persisted config, migrated SQLite and environment auth.
const serverAuthLayer = (baseDir: string) => {
  const config = ServerConfig.layerTest(process.cwd(), baseDir);
  return EnvironmentAuth.layer.pipe(
    Layer.provideMerge(SqlitePersistence.layerConfig),
    Layer.provide(ServerSecretStore.layer),
    Layer.provide(ServerEnvironment.layerIdentity),
    Layer.provideMerge(config),
    Layer.provideMerge(NodeServices.layer),
  );
};

// Live clock: the web handler runtime verifies session expiry against wall-clock time.
it.live(
  "an applied native bearer authenticates through environment auth and reaches only its enrolled native context",
  () =>
    Effect.gen(function* () {
      const home = yield* syntheticT3Home("workstreams-native-bearer-route-");
      // First server start: full migrations, WAL journal and the server-created signing key.
      yield* Effect.gen(function* () {
        yield* EnvironmentAuth.EnvironmentAuth;
      }).pipe(Effect.provide(serverAuthLayer(home.baseDir)), Effect.scoped);
      assert.strictEqual(NodeFS.existsSync(`${home.dbPath}-wal`), false);

      const requestFile = `${home.baseDir}/native-enrollment-request.json`;
      const credentialPath = `${home.baseDir}/userdata/workstreams-native-provider.json`;
      const prepared = yield* prepareWorkstreamProviderRequest({
        baseDir: home.baseDir,
        authorityDir: home.authorityDir,
        requestFile,
        ownerId: "synthetic-owner",
        principalId: "synthetic-principal",
        enrollmentId: "synthetic-route-enrollment",
        registryOrigin: "http://127.0.0.1:8787",
        lifetimeDays: 30,
      });
      const applied = yield* runWorkstreamProviderCliOperation({
        baseDir: home.baseDir,
        authorityDir: home.authorityDir,
        credentialPath,
        requestFile,
        operation: "apply",
        expectedPriorSha256: null,
      });
      assert.strictEqual(applied.state, "applied");
      const credentialBytes = NodeFS.readFileSync(credentialPath, "utf8");
      const authorization = decodeCredential(credentialBytes).authorization_header;
      // The control plane's provider credential locator accepts only this private single-key shape
      // (chatgpt-control-plane src/workstreams/adapters/credentials.ts).
      assert.strictEqual(NodeFS.statSync(credentialPath).mode & 0o7777, 0o600);
      assert.deepEqual(Object.keys(JSON.parse(credentialBytes) as object), [
        "authorization_header",
      ]);
      assert.match(authorization, /^Bearer [A-Za-z0-9._~+/-]+=*$/);
      assert.isAtMost(Buffer.byteLength(authorization), 3072);

      const authority = readExistingNativeStoreAuthorityState(home.authorityDir);
      const authorityPort = {
        readCurrent: Effect.succeed({
          environmentId: authority.environment_id,
          authorityNamespace: authority.authority_namespace,
          storeGeneration: authority.store_generation,
        }),
      };
      const providerFixture = makeProviderFixture();
      const nativeThreadId = attestationRequest.identity.native_id;
      yield* Effect.gen(function* () {
        const sessions = yield* AuthSessions.AuthSessionRepository;
        const sessionStore = yield* SessionStore.SessionStore;
        const enrollments = yield* makeNativeEnrollments({
          authority: authorityPort,
          build: Effect.succeed(Option.some(syntheticBuild)),
        }).pipe(Effect.provideService(AuthSessions.AuthSessionRepository, sessions));
        const runtime = NativeWorkstreamsRuntime.of({
          provider: makeWorkstreamsNativeProvider({
            ...providerFixture.ports,
            authority: authorityPort,
            build: Effect.succeed(Option.some(syntheticBuild)),
            threadExists: (threadId) => Effect.succeed(threadId === nativeThreadId),
          }),
          enrollments: NativeProviderEnrollment.of({
            getBySessionId: (sessionId) =>
              enrollments.getBySessionId(sessionId).pipe(Effect.orElseSucceed(() => Option.none())),
          }),
        });
        const environmentAuth = yield* EnvironmentAuth.EnvironmentAuth;
        const app = HttpRouter.toWebHandler(
          HttpApiBuilder.layer(NativeRouteApi).pipe(
            Layer.provide(nativeWorkstreamsHttpApiLayer),
            Layer.provide(Layer.succeed(NativeWorkstreamsRuntime, runtime)),
            Layer.provide(AuthHttp.layerAuthenticatedAuth),
            Layer.provide(Layer.succeed(EnvironmentAuth.EnvironmentAuth, environmentAuth)),
            Layer.provide(workstreamResponseHeadersLayer),
            Layer.provide(HttpPlatform.layer.pipe(Layer.provide(NodeServices.layer))),
            Layer.provide(Etag.layerWeak),
            Layer.provide(NodeServices.layer),
          ),
          { disableLogger: true },
        );
        // Exactly the control-plane adapter's request shape: the file's header, verbatim.
        const call = (operation: string, credential: string | null, body?: string) =>
          Effect.promise(() =>
            app.handler(
              new Request(`http://127.0.0.1:3774/api/workstreams/native/v1/${operation}`, {
                method: body === undefined ? "GET" : "POST",
                headers: {
                  accept: "application/json",
                  ...(credential === null ? {} : { authorization: credential }),
                  ...(body === undefined ? {} : { "content-type": "application/json" }),
                },
                ...(body === undefined ? {} : { body }),
              }),
            ),
          );
        yield* Effect.addFinalizer(() => Effect.promise(() => app.dispose()));

        const context = yield* call("context", authorization);
        assert.strictEqual(context.status, 200);
        const ready = decodeContextResponse(yield* Effect.promise(() => context.json()));
        assert.strictEqual(ready.state, "ready");
        if (ready.state === "ready") assert.deepEqual(ready.context, prepared.native_context);

        const attestation = yield* call(
          "attestations",
          authorization,
          encodeAttestation({
            ...attestationRequest,
            owner_id: prepared.native_context.owner_id,
            principal_id: prepared.native_context.principal_id,
            identity: {
              ...attestationRequest.identity,
              source_instance_id: prepared.native_context.source_instance_id,
            },
            expected_authority_namespace: prepared.native_context.authority_namespace,
            expected_store_generation: prepared.native_context.store_generation,
          }),
        );
        const attested = decodeAttestationResponse(yield* Effect.promise(() => attestation.json()));
        assert.strictEqual(attested.state, "attested");

        // Missing, tampered and ordinarily scoped credentials never reach the enrollment.
        assert.strictEqual((yield* call("context", null)).status, 401);
        assert.strictEqual((yield* call("context", `${authorization}x`)).status, 401);
        const browser = yield* sessionStore.issue({ method: "bearer-access-token" });
        assert.strictEqual((yield* call("context", `Bearer ${browser.token}`)).status, 403);

        // Existing session revocation (`t3 auth session revoke`) withdraws the native bearer.
        assert.strictEqual(
          yield* sessionStore.revoke(AuthSessionId.make(prepared.session_id)),
          true,
        );
        assert.strictEqual((yield* call("context", authorization)).status, 401);
        assert.strictEqual(providerFixture.calls.length, 0);
      }).pipe(Effect.provide(serverAuthLayer(home.baseDir)), Effect.scoped);
    }).pipe(Effect.scoped),
);

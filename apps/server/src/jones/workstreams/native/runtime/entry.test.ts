// @effect-diagnostics nodeBuiltinImport:off -- Fresh synthetic databases are retained under task-owned evidence; cleanup belongs to root.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import { expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import * as Wire from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as DateTime from "effect/DateTime";
import * as Clock from "effect/Clock";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as TestClock from "effect/testing/TestClock";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpPlatform from "effect/http/HttpPlatform";
import * as Etag from "effect/http/Etag";
import * as Fixture from "../store/sql.testkit.ts";
import * as Admin from "../cli.ts";
import * as Operations from "../../operations/service.ts";
import * as OperationStore from "../store/operationStore.ts";
import * as Startup from "./startup.ts";
import * as Entry from "./entry.ts";
import * as CurrentAuthorityFixture from "./currentAuthority.testkit.ts";
import * as RollupWorker from "./rollupWorker.ts";
import * as RollupApplication from "../rollups/application.ts";
import * as RollupSummary from "../rollups/summaryRead.ts";
import * as RollupSynthetic from "../rollups/synthetic.ts";
import * as Deferred from "effect/Deferred";
import * as Admission from "./admission.ts";
import * as RegistrationContext from "../../registrationContext/service.ts";
import * as Schema from "effect/Schema";
import * as NativePlacement from "@t3tools/contracts/jones/workstreamsNativePlacement";
import * as Runtime from "../../runtimeIntegration/native.ts";
import * as Placements from "../registry/placements.ts";
import * as Authority from "../../../../environment/NativeStoreAuthority.ts";
import * as Environment from "../../../../environment/ServerEnvironment.ts";
import * as ProjectionStore from "../../../../orchestration-v2/ProjectionStore.ts";
import * as ThreadManagement from "../../../../orchestration-v2/ThreadManagementService.ts";
import { liveThreadShell } from "../../../../mcp/McpToolAccess.testkit.ts";
import * as AuthSessions from "../../../../persistence/AuthSessions.ts";
import { runMigrations } from "../../../../persistence/Migrations.ts";
import nativeMigration from "../../../persistence/Migrations/125_JonesWorkstreamsNative.ts";
import enrollmentMigration from "../../../persistence/Migrations/126_JonesWorkstreamsSourceEnrollments.ts";
import * as SourceSetup from "./sourceSetup.ts";
import * as SourceEnrollment from "./sourceEnrollment.ts";
import * as SourceRecords from "../store/sourceEnrollmentStore.ts";
import { NativeProviderEnrollmentError } from "../../nativeProvider/enrollment.ts";
import { enrollmentRequest as originalEnrollmentRequest } from "../../enrollment/testFixtures.ts";
import { makeProviderFixture } from "../../nativeProvider/testFixtures.ts";
import { makeWorkstreamsNativeProvider } from "../../nativeProvider/service.ts";
import * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import * as Gateway from "../../../../workstreams/WorkstreamGateway.ts";
import * as Auth from "../../../../auth/EnvironmentAuth.ts";
import * as Secrets from "../../../../auth/ServerSecretStore.ts";
import * as McpOAuth from "../../../../auth/McpOAuth.ts";
import * as Config from "../../../../config.ts";
import * as McpHttpServer from "../../../../mcp/McpHttpServer.ts";
import * as McpSessions from "../../../../mcp/McpSessionRegistry.ts";
import * as PeerPins from "./peerSessionPins.ts";
import * as PeerAuthentication from "./peerMcpAuthentication.ts";
import * as PeerSetup from "./peerSessionPinSetup.ts";
import * as PeerRecords from "../store/peerSessionPinStore.ts";
import * as Registries from "../store/registryStore.ts";
import * as NativeSession from "../registry/session.ts";
import * as PeerBoundary from "../forwarding/peerReadAdmission.ts";
import peerMigration from "../../../persistence/Migrations/128_JonesWorkstreamsPeerSessionPins.ts";
import {
  WORKSTREAMS_HOME_READ_RESULT_CONTRACT,
  WorkstreamsHomeReadResult,
} from "@t3tools/contracts/jones/workstreamsHomeReadResult";
import { Tool, Toolkit } from "effect/ai";
import * as McpAccess from "../../../../mcp/McpToolAccess.ts";
import * as ControlPlane from "../../../../workstreams/ControlPlaneWorkstreamTransport.ts";
import {
  workstreamHttpApiLayer,
  workstreamResponseHeadersLayer,
} from "../../../../workstreams/http.ts";

vi.mock("../../../../../package.json", () => ({
  default: {
    version: "0.0.0-preview.20261002.100",
    jonesSource: {
      repository: "Jones-Systems/Jones-Code",
      sha: "a".repeat(40),
      tree: "b".repeat(40),
    },
  },
}));

const enrollmentRequest = {
  ...originalEnrollmentRequest,
  context: {
    ...originalEnrollmentRequest.context,
    authority_namespace: "t3-native:12345678-1234-4234-8234-123456789abc",
  },
};

const decodeHomeReadResult = Schema.decodeUnknownEffect(WorkstreamsHomeReadResult);
const decodeNativePlacement = Schema.decodeUnknownEffect(
  NativePlacement.WorkstreamsNativePlacementResult,
);
const decodeLegacyPlacement = Schema.decodeUnknownEffect(Wire.T3PlacementResult);

const registry = {
  ...Fixture.scope,
  owner_id: enrollmentRequest.context.owner_id,
  canonical_origin: enrollmentRequest.registry_origin,
  home_environment_id: enrollmentRequest.context.source_instance_id,
  server_generation: 1,
};
const principal = {
  sessionId: Wire.AuthSessionId.make("startup-http"),
  subject: "synthetic-local-browser",
  method: "bearer-access-token" as const,
  scopes: new Set([Wire.AuthOrchestrationReadScope, Wire.AuthOrchestrationOperateScope]),
};
const fresh = () => {
  const parent = NodePath.join(NodeOS.tmpdir(), "nw-native-entry");
  NodeFS.mkdirSync(parent, { recursive: true });
  const home = NodeFS.mkdtempSync(NodePath.join(parent, "entry-")),
    database = NodePath.join(home, "userdata", "statev2.sqlite");
  NodeFS.chmodSync(home, 0o700);
  NodeFS.mkdirSync(NodePath.dirname(database), { mode: 0o700 });
  return {
    home,
    database,
    layer: Admin.layer.pipe(
      Layer.provideMerge(
        Fixture.testLayer({
          filename: database,
          keys: new Map([...Fixture.secrets(), ["server-signing-key", Fixture.keyBytes]]),
        }),
      ),
      Layer.provideMerge(NodeServices.layer),
    ),
  };
};

const seed = (
  f: ReturnType<typeof fresh>,
  rollupPorts?: Parameters<typeof Startup.homeServicesLayer>[1],
) =>
  Effect.gen(function* () {
    yield* TestClock.setTime(Date.parse(enrollmentRequest.session.issued_at));
    yield* runMigrations({ toMigrationInclusive: 54 });
    yield* nativeMigration;
    yield* enrollmentMigration;
    yield* (yield* Admin.NativeAdmin).initialize({
      registry,
      principal_id: enrollmentRequest.context.principal_id,
      content_key_id: "test-home",
      local_key_id: "test-local",
    });
    const sessions = yield* AuthSessions.make;
    const issuedAt = DateTime.makeUnsafe(enrollmentRequest.session.issued_at),
      expiresAt = DateTime.makeUnsafe(enrollmentRequest.session.expires_at);
    const client = {
      label: null,
      ipAddress: null,
      userAgent: null,
      deviceType: "unknown" as const,
      os: null,
      browser: null,
    };
    yield* sessions.create({
      ...principal,
      scopes: [...principal.scopes],
      issuedAt,
      expiresAt,
      client,
    });
    yield* sessions.create({
      ...principal,
      sessionId: Wire.AuthSessionId.make("startup-mcp"),
      subject: "mcp-client",
      scopes: [...principal.scopes],
      issuedAt,
      expiresAt,
      client,
    });
    yield* sessions.create({
      ...principal,
      sessionId: Wire.AuthSessionId.make("startup-cli"),
      subject: "cli-issued-session",
      scopes: [...principal.scopes],
      issuedAt,
      expiresAt,
      client,
    });
    const provider = makeProviderFixture();
    const currentOwner = yield* CurrentAuthorityFixture.prepare({
      ...f,
      context: enrollmentRequest.context,
    });
    const authority: Authority.NativeStoreAuthority["Service"] = {
      ...provider.ports.authority,
      readCurrent: Effect.succeed({
        environmentId: enrollmentRequest.context.source_instance_id,
        authorityNamespace: enrollmentRequest.context.authority_namespace,
        storeGeneration: enrollmentRequest.context.store_generation,
      }),
      trustProvider: {
        readTrustSnapshot: () => ({
          readiness: "ready",
          trustedEnvironments: [
            {
              environmentId: registry.home_environment_id,
              authorityNamespace: enrollmentRequest.context.authority_namespace,
              storeGeneration: enrollmentRequest.context.store_generation,
            },
          ],
        }),
      },
    };
    const nativeSources = yield* SourceEnrollment.NativeSourceEnrollments.pipe(
      Effect.provide(
        SourceEnrollment.layer.pipe(
          Layer.provide(
            Layer.succeed(SourceEnrollment.SourceSetupOwner, { verifyUnchanged: Effect.void }),
          ),
          Layer.provideMerge(SourceRecords.layer),
          Layer.provide(Layer.succeed(AuthSessions.AuthSessionRepository, sessions)),
          Layer.provide(
            Layer.succeed(SourceEnrollment.SourceIssuer, {
              authority,
              build: Effect.succeed(Option.some(enrollmentRequest.context.build)),
            }),
          ),
        ),
      ),
    );
    yield* Effect.gen(function* () {
      yield* (yield* SourceSetup.NativeSourceSetup).apply({
        schema: "jones-code.native-source-setup/v1",
        enrollment: enrollmentRequest,
        expected: {
          registry_instance_id: registry.registry_instance_id,
          owner_id: registry.owner_id,
          home_environment_id: registry.home_environment_id,
          home_generation: 1,
          registry_version: 1,
          singleton_binding_version: 1,
          grant_id: "grant:" + enrollmentRequest.context.principal_id,
          grant_version: 1,
          source_binding: null,
        },
        rebind: false,
      });
    }).pipe(
      Effect.provide(SourceSetup.layer.pipe(Layer.provide(SourceRecords.layer))),
      Effect.provideService(SourceEnrollment.NativeSourceEnrollments, nativeSources),
      Effect.provideService(SourceEnrollment.SourceSetupOwner, { verifyUnchanged: Effect.void }),
    );
    const runtime: Runtime.NativeWorkstreamsRuntime["Service"] = {
      provider: makeWorkstreamsNativeProvider({ ...provider.ports, authority }),
      providerV2: makeWorkstreamsNativeProvider({ ...provider.ports, authority }, "v2"),
      enrollments: {
        getBySessionId: (id) =>
          nativeSources
            .getBySessionId(id)
            .pipe(Effect.mapError(() => new NativeProviderEnrollmentError())),
      },
    };
    const config = {
      mode: "native-home",
      registry,
      bootstrap: {
        home: f.home,
        database: f.database,
        key_provider: "server-secret-store",
        source_session_id: enrollmentRequest.session.session_id,
      },
    };
    const selection = yield* Startup.select({
      text: JSON.stringify(config),
      legacyActivationPresent: false,
      home: f.home,
      database: f.database,
      environmentId: registry.home_environment_id,
    });
    if (selection.home === null) return yield* Effect.die(selection.problem);
    const services = Startup.homeServicesLayer(
      { ...selection, home: selection.home },
      rollupPorts,
    ).pipe(
      Layer.provide(Layer.succeed(SourceEnrollment.NativeSourceEnrollments, nativeSources)),
      Layer.provide(Layer.succeed(Runtime.NativeWorkstreamsRuntime, runtime)),
      Layer.provideMerge(currentOwner),
      Layer.provide(Layer.succeed(Authority.NativeStoreAuthority, authority)),
      Layer.provide(Layer.succeed(Placements.PlacementTrust, authority.trustProvider)),
      Layer.provide(Layer.succeed(AuthSessions.AuthSessionRepository, sessions)),
      Layer.provide(
        Layer.succeed(Environment.ServerEnvironmentIdentity, {
          getEnvironmentId: Effect.succeed(Wire.EnvironmentId.make(registry.home_environment_id)),
        }),
      ),
      Layer.provide(ProjectionStore.layerMemory),
      Layer.provide(
        Layer.mock(ThreadManagement.ThreadManagementService)({
          getThreadShell: (id) =>
            Effect.succeed(id === "thread-synthetic" ? liveThreadShell(id) : null),
        }),
      ),
    );
    return { services, sessions, authority };
  });

const operation = (origin: { readonly kind: "app" | "agent" | "cli" }, suffix: string) => ({
  contract: "workstream-operations/1.0.0" as const,
  operation_id: "startup-operation-" + suffix,
  origin,
  kind: "create_workstream" as const,
  registry: { registry_id: registry.registry_instance_id, owner_id: registry.owner_id },
  workstream: {
    name: "startup " + suffix,
    lifecycle: "active" as const,
    progress: { state: "unknown" as const },
    sort_order: 0,
  },
});

it.effect(
  "production composition shares native issuance across HTTP, MCP and authenticated CLI while refusing callback-only recovery",
  () => {
    const f = fresh();
    return Effect.gen(function* () {
      const prepared = yield* seed(f);
      yield* Effect.gen(function* () {
        const gateway = yield* Gateway.WorkstreamGateway,
          operations = yield* Operations.WorkstreamOperations;
        expect(
          (yield* gateway
            .readSession()
            .pipe(Effect.provideService(Wire.EnvironmentAuthenticatedPrincipal, principal)))
            .principalId,
        ).toBe(enrollmentRequest.context.principal_id);
        expect(
          (yield* operations
            .submit({ request: operation({ kind: "app" }, "http"), wait_ms: 3000 })
            .pipe(Effect.provideService(Wire.EnvironmentAuthenticatedPrincipal, principal))).state,
        ).toBe("committed");
        const forged = yield* operations
          .submit({ request: operation({ kind: "cli" }, "forged"), wait_ms: 0 })
          .pipe(
            Effect.provideService(Wire.EnvironmentAuthenticatedPrincipal, principal),
            Effect.result,
          );
        expect(forged._tag).toBe("Failure");
        const mcp: Mcp.McpInvocationScope = {
          environmentId: Wire.EnvironmentId.make(registry.home_environment_id),
          issuedAt: 0,
          requestNamespace: "scheduled-synthetic",
          capabilities: new Set(["orchestration"]),
          thread: undefined,
          client: { sessionId: "startup-mcp", label: "agent", access: "full-access" },
        };
        yield* Effect.gen(function* () {
          expect(
            (yield* operations.submit({
              request: operation({ kind: "agent" }, "mcp"),
              wait_ms: 3000,
            })).state,
          ).toBe("committed");
        }).pipe(Effect.provideService(Mcp.McpInvocationContext, mcp));
        yield* Effect.gen(function* () {
          expect(
            (yield* operations.submit({
              request: operation({ kind: "cli" }, "cli"),
              wait_ms: 3000,
            })).state,
          ).toBe("committed");
          expect(
            yield* operations.read({
              operation_id: operation({ kind: "app" }, "http").operation_id,
            }),
          ).toEqual({ operation: null });
        }).pipe(
          Effect.provideService(Wire.EnvironmentAuthenticatedPrincipal, {
            ...principal,
            sessionId: Wire.AuthSessionId.make("startup-cli"),
            subject: "cli-issued-session",
          }),
        );
        expect(
          (yield* operations
            .submit({
              request: operation({ kind: "cli" }, "callback-only"),
              wait_ms: 0,
            })
            .pipe(
              Effect.provideService(Admission.TrustedCli, {
                environmentId: registry.home_environment_id,
                check: () => Effect.void,
              }),
              Effect.result,
            ))._tag,
        ).toBe("Failure");
        expect(
          yield* (yield* OperationStore.OperationStore).read("startup-operation-callback-only"),
        ).toBeNull();
        for (const suffix of ["http", "mcp", "cli"]) {
          const saved = yield* (yield* OperationStore.OperationStore).read(
            "startup-operation-" + suffix,
          );
          expect(saved?.destination).toMatchObject({
            registry_instance_id: registry.registry_instance_id,
            owner_id: registry.owner_id,
            home_environment_id: registry.home_environment_id,
            home_generation: 1,
            principal_id: enrollmentRequest.context.principal_id,
            binding_version: 1,
          });
        }
        expect(
          (yield* gateway
            .readMetadata()
            .pipe(Effect.provideService(Wire.EnvironmentAuthenticatedPrincipal, principal))).items,
        ).toHaveLength(3);
        yield* prepared.sessions.revoke({
          sessionId: principal.sessionId,
          revokedAt: DateTime.makeUnsafe(enrollmentRequest.session.issued_at),
        });
        expect(
          (yield* gateway
            .readMetadata()
            .pipe(Effect.provideService(Wire.EnvironmentAuthenticatedPrincipal, principal))
            .pipe(Effect.result))._tag,
        ).toBe("Failure");
      }).pipe(Effect.provide(prepared.services));
    }).pipe(Effect.scoped, Effect.provide(f.layer));
  },
);

class NativeTestApi extends HttpApi.make("environment").add(
  Wire.EnvironmentHttpApi.groups.workstreams,
) {}
it.effect(
  "actual HTTP v2 success encoding retains native provenance after production source admission",
  () => {
    const f = fresh();
    return Effect.gen(function* () {
      const prepared = yield* seed(f);
      const cp = yield* Effect.acquireRelease(
        Effect.sync(() =>
          vi.spyOn(ControlPlane, "makeControlPlaneWorkstreamTransport").mockImplementation(() => {
            throw new Error("CP construction forbidden");
          }),
        ),
        (spy) => Effect.sync(() => spy.mockRestore()),
      );
      const egress = yield* Effect.acquireRelease(
        Effect.sync(() =>
          vi.spyOn(globalThis, "fetch").mockRejectedValue(new Error("CP/network egress forbidden")),
        ),
        (spy) => Effect.sync(() => spy.mockRestore()),
      );
      yield* Effect.gen(function* () {
        const gateway = yield* Gateway.WorkstreamGateway,
          entry = yield* Entry.Entry;
        let sequence = 0;
        const committed = (action: Wire.WorkstreamCommand["action"]) =>
          Effect.gen(function* () {
            const session = yield* gateway.readSession();
            const receipt = yield* gateway.submit({
              command_id: "startup-http-command-" + ++sequence,
              expected_server_generation: session.serverGeneration,
              expected_registry_version: session.registryVersion,
              action,
            });
            if (receipt.state !== "committed") return yield* Effect.die(receipt);
            return receipt;
          }).pipe(Effect.provideService(Wire.EnvironmentAuthenticatedPrincipal, principal));
        const workstream = (yield* committed({
          operation: "create_workstream",
          name: "v2 native proof",
          lifecycle: "active",
          progress: { state: "unknown" },
          sort_order: 0,
        })).effects.workstream_versions[0]!.workstream_id;
        const reference = (yield* committed({
          operation: "register_reference",
          identity: {
            provider: "t3",
            source_instance_id: registry.home_environment_id,
            resource_kind: "thread",
            id_kind: "internal",
            native_id: "thread-synthetic",
            account_provenance: { kind: "not_account_scoped" },
          },
          pr_locator: null,
        })).effects.native_reference_id!;
        yield* committed({
          operation: "verify_reference",
          native_reference_id: reference,
          expected_attestation_version: 0,
        });
        yield* committed({
          operation: "attach_primary",
          workstream_id: workstream,
          expected_version: 1,
          native_reference_id: reference,
        });
        const clock = yield* Clock.Clock;
        const auth = Layer.succeed(Wire.EnvironmentAuthenticatedAuth, (httpEffect) =>
          httpEffect.pipe(
            Effect.provideService(Clock.Clock, clock),
            Effect.provideService(Wire.EnvironmentAuthenticatedPrincipal, principal),
          ),
        );
        const routes = HttpApiBuilder.layer(NativeTestApi).pipe(
          Layer.provide(workstreamHttpApiLayer),
          Layer.provide(Layer.succeed(Entry.Entry, entry)),
          Layer.provide(Layer.succeed(Gateway.WorkstreamGateway, gateway)),
          Layer.provide(
            Layer.succeed(Operations.WorkstreamOperations, yield* Operations.WorkstreamOperations),
          ),
          Layer.provide(
            Layer.succeed(RegistrationContext.WorkstreamsRegistrationContext, entry.registration),
          ),
          Layer.provide(auth),
          Layer.provide(workstreamResponseHeadersLayer),
          Layer.provideMerge(
            HttpPlatform.layer.pipe(
              Layer.provideMerge(NodeServices.layer),
              Layer.provideMerge(Etag.layerWeak),
            ),
          ),
          Layer.provide(NodeServices.layer),
        );
        const app = yield* Effect.acquireRelease(
          Effect.sync(() => HttpRouter.toWebHandler(routes, { disableLogger: true })),
          (value) => Effect.promise(() => value.dispose()),
        );
        const body = {
          identities: [
            {
              source_instance_id: registry.home_environment_id,
              native_thread_id: "thread-synthetic",
            },
          ],
        };
        const request = (contract?: string) =>
          Effect.promise(() =>
            app.handler(
              new Request("http://synthetic/api/workstreams/thread-placements", {
                method: "POST",
                headers: {
                  "content-type": "application/json",
                  ...(contract ? { "x-t3-workstreams-placement-contract": contract } : {}),
                },
                body: JSON.stringify(body),
              }),
            ),
          );
        const v2 = yield* request("workstreams-t3-placement/2.0.0");
        expect(v2.status).toBe(200);
        expect(v2.headers.get("cache-control")).toBe("private, no-store");
        const decoded = yield* decodeNativePlacement(yield* Effect.promise(() => v2.json()));
        expect(decoded.page.provenance).toMatchObject({
          authority: "native-home",
          registry_instance_id: registry.registry_instance_id,
          home_environment_id: registry.home_environment_id,
          home_generation: 1,
          served_by_environment_id: registry.home_environment_id,
        });
        expect(decoded.page.items[0]).toMatchObject({
          attestation_source: "native",
          evidence_contract: "workstreams-t3-provider/2.0.0",
          kind: "primary",
        });
        const v1 = yield* request();
        expect(v1.status).toBe(200);
        const legacy = yield* decodeLegacyPlacement(yield* Effect.promise(() => v1.json()));
        expect(legacy.page).not.toHaveProperty("provenance");
        expect(legacy.page.items[0]).not.toHaveProperty("attestation_source");
        expect((yield* request("workstreams-t3-placement/3.0.0")).status).toBe(400);
        expect(cp).not.toHaveBeenCalled();
        expect(egress).not.toHaveBeenCalled();
      }).pipe(Effect.provide(prepared.services));
    }).pipe(Effect.scoped, Effect.provide(f.layer));
  },
);

it.effect(
  "native home startup owns one rollup cadence and fences it when the server scope closes",
  () => {
    const f = fresh();
    return Effect.gen(function* () {
      const first = yield* Deferred.make<void>();
      const second = yield* Deferred.make<void>();
      let calls = 0;
      const fixture = RollupSynthetic.syntheticFixture();
      // External provider ports and shared admission are synthetic; startup, worker, application and SQL remain real.
      // Busy inference admission reaches the final application boundary without dispatching providers or claiming work.
      const ports = Layer.mergeAll(
        RollupSynthetic.syntheticEvidenceLayer([]),
        RollupSynthetic.syntheticGenerationLayer(fixture.identity, fixture.result),
        RollupSummary.unconfiguredAuthorizerLayer,
        Layer.succeed(RollupApplication.RollupAdmission, {
          tryAcquire: () =>
            Effect.gen(function* () {
              calls++;
              if (calls === 1) yield* Deferred.succeed(first, undefined);
              if (calls === 2) yield* Deferred.succeed(second, undefined);
              return null;
            }),
        }),
      );
      const prepared = yield* seed(f, ports);
      const worker = yield* Effect.scoped(
        Effect.gen(function* () {
          const worker = yield* RollupWorker.NativeRollupWorker;
          expect(yield* RollupWorker.NativeRollupWorker).toBe(worker);
          yield* Deferred.await(first);
          expect(calls).toBe(1);
          yield* TestClock.adjust(29_999);
          expect(calls).toBe(1);
          yield* TestClock.adjust(1);
          yield* Deferred.await(second);
          expect(calls).toBe(2);
          return worker;
        }).pipe(Effect.provide(prepared.services)),
      );
      yield* TestClock.adjust(90_000);
      expect(yield* worker.sweep()).toBeNull();
      expect(calls).toBe(2);
    }).pipe(Effect.scoped, Effect.provide(f.layer));
  },
);

// Native OAuth authentication, middleware, selected startup, encrypted pins and reads are real.
// Only receiver identity, unused provider-token resolution, owner/setup and bind qualification
// are synthetic. This in-process proof opens no listener and is not physical peer acceptance.
it.effect(
  "mounted native MCP reads require the exact authenticated peer and retain write/revocation fences",
  () => {
    const f = fresh();
    return Effect.gen(function* () {
      const prepared = yield* seed(f);
      yield* peerMigration;
      const authLayer = Auth.layer.pipe(
        Layer.provide(
          Layer.mock(Secrets.ServerSecretStore)({
            get: () => Effect.succeed(Option.some(Fixture.keyBytes)),
            getOrCreateRandom: () => Effect.succeed(Fixture.keyBytes),
          }),
        ),
        Layer.provideMerge(Layer.succeed(AuthSessions.AuthSessionRepository, prepared.sessions)),
        Layer.provide(Config.layerTest(f.home, { prefix: "nw-peer-entry-" })),
        Layer.provide(
          Layer.succeed(Environment.ServerEnvironmentIdentity, {
            getEnvironmentId: Effect.succeed(Wire.EnvironmentId.make(registry.home_environment_id)),
          }),
        ),
      );
      const peerAuthentication = PeerAuthentication.layer.pipe(
        Layer.provideMerge(PeerPins.nativeLayer),
        Layer.provideMerge(authLayer),
      );
      yield* Effect.gen(function* () {
        const auth = yield* Auth.EnvironmentAuth;
        const peers = yield* PeerPins.NativePeerSessions;
        const issued = yield* auth.issueMcpClientSession({
          label: "synthetic bound peer",
          access: "full-access",
          peerLink: true,
          client: { deviceType: "bot" },
        });
        const scope: Mcp.McpInvocationScope = {
          environmentId: Wire.EnvironmentId.make(registry.home_environment_id),
          issuedAt: DateTime.toEpochMillis(yield* DateTime.now),
          requestNamespace: "peer-setup-synthetic",
          capabilities: new Set(["orchestration"]),
          thread: undefined,
          client: {
            sessionId: issued.sessionId,
            label: "setup",
            access: "full-access",
            linked: true,
          },
        };
        const nativeRequest = {
          cookies: {},
          headers: { authorization: "Bearer " + issued.token },
        } as unknown as Parameters<typeof auth.authenticateMcpClient>[0];
        yield* (yield* PeerAuthentication.PeerMcpAuthentication).register(nativeRequest, scope);
        const facts = yield* peers.current(scope);
        const receiver = yield* prepared.authority.readCurrent;
        const store = yield* Registries.RegistryStore;
        const storeScope = {
          registry_instance_id: registry.registry_instance_id,
          owner_id: registry.owner_id,
        };
        const remote = "source:entry-peer";
        const peerPrincipal = "principal:entry-peer";
        yield* store.appendPlain(storeScope, "principals", {
          principal_id: peerPrincipal,
          kind: "environment",
          environment_id: remote,
          provenance: "{}",
        });
        yield* store.appendPlain(storeScope, "grants", {
          grant_id: "grant:entry-peer",
          principal_id: peerPrincipal,
          grant_version: 1,
          permissions: JSON.stringify(["workstreams:read", "workstreams:write"]),
          state: "active",
        });
        yield* store.appendPlain(storeScope, "source_bindings", {
          provider: "t3",
          source_instance_id: remote,
          binding_version: 1,
          authority_namespace: "source:entry-store",
          store_generation: 1,
          state: "active",
        });
        const context = (yield* store.context(registry))!;
        const receipt = {
          principal_id: peerPrincipal,
          binding_version: 1,
          registry_version: context.registry_version,
          request_sha256: "c".repeat(64),
        };
        const body = JSON.stringify({
          schema: "jones-workstreams-peer-pin-setup/1",
          assertion: {
            schema: "jones-workstreams-peer-session-pin/1",
            pin_id: "pin:entry-peer",
            revision: 1,
            receiver_authority: {
              environment_id: receiver.environmentId,
              authority_namespace: receiver.authorityNamespace,
              store_generation: receiver.storeGeneration,
            },
            session: facts.session,
            peer_environment_id: remote,
            roles: ["member_home"],
            expected_home: {
              registry_instance_id: registry.registry_instance_id,
              owner_id: registry.owner_id,
              home_environment_id: registry.home_environment_id,
              server_generation: registry.server_generation,
            },
            source_authority: {
              environment_id: remote,
              authority_namespace: "source:entry-store",
              store_generation: 1,
            },
            source_enrollment_id: "enrollment:entry-peer",
            source_binding_version: null,
            singleton_binding_version: null,
            state: "prepared",
            expires_at: DateTime.formatIso(DateTime.add(yield* DateTime.now, { minutes: 10 })),
            setup_request_id: "setup:entry-peer",
            owner_qualification_digest: "b".repeat(64),
            bind_receipt: null,
          },
          expected: {
            registry_version: context.registry_version,
            grant_id: "grant:entry-peer",
            grant_version: 1,
          },
        });
        const decoded = PeerRecords.decodeSetupRequest(body);
        const setupLayer = PeerSetup.layer.pipe(
          Layer.provide(
            Layer.succeed(PeerSetup.PeerPinSetupQualification, {
              qualify: (exact, digest) =>
                exact === body && digest === decoded.pin.setup_request_sha256
                  ? Effect.succeed({
                      invocation: scope,
                      owner_qualification_digest: "b".repeat(64),
                      checkCurrent: () => Effect.void,
                    })
                  : Effect.fail(new PeerPins.PeerSessionError({ code: "owner_unqualified" })),
              verifyBinding: (_pin, actual, version) =>
                JSON.stringify(actual) === JSON.stringify(receipt) && version === 1
                  ? Effect.void
                  : Effect.fail(new PeerPins.PeerSessionError({ code: "conflict" })),
            }),
          ),
          Layer.provide(
            Layer.succeed(SourceEnrollment.SourceSetupOwner, { verifyUnchanged: Effect.void }),
          ),
          Layer.provide(Layer.succeed(Authority.NativeStoreAuthority, prepared.authority)),
        );
        expect(yield* store.context(decoded.pin.expected_home)).toMatchObject({
          home_environment_id: registry.home_environment_id,
          server_generation: 1,
          registry_version: context.registry_version,
          state: "active",
        });
        yield* Effect.gen(function* () {
          const setup = yield* PeerSetup.PeerSessionPinSetup;
          yield* setup.apply(body, 0);
          yield* setup.bind(decoded.pin.pin_id, 1, receipt, 1);
        }).pipe(Effect.provide(setupLayer));

        const now = DateTime.formatIso(yield* DateTime.now);
        const name = "Encrypted mounted peer content";
        yield* store.put(
          storeScope,
          "records",
          {
            metadata: { workstream_id: "workstream:entry-peer", version: 1, sort_order: 0 },
            value: {
              workstream_id: "workstream:entry-peer",
              owner_id: registry.owner_id,
              name,
              lifecycle: "active",
              progress: { state: "unknown" },
              delivery: "unknown",
              freshness: "current",
              sort_order: 0,
              version: 1,
              created_at: now,
              updated_at: now,
              created_by: { principal_id: peerPrincipal },
              updated_by: { principal_id: peerPrincipal },
            },
          },
          null,
        );

        const probe = Toolkit.make(
          Tool.make("synthetic_peer_write_probe", {
            description: "Test only: verify the composed Session write fence.",
            parameters: Tool.EmptyParams,
            success: Schema.Struct({
              write_refused: Schema.Boolean,
              runtime_mode: Wire.RuntimeMode,
              interaction_mode: Wire.ProviderInteractionMode,
            }),
            dependencies: [Mcp.McpInvocationContext],
          }),
        );
        const probes = McpAccess.toLayer(
          probe,
          Effect.gen(function* () {
            const boundary = yield* Effect.serviceOption(PeerBoundary.PeerReadBoundary);
            const session = yield* NativeSession.Session;
            const incoming = yield* PeerPins.NativePeerSessions;
            return {
              synthetic_peer_write_probe: McpAccess.reads(() =>
                Effect.gen(function* () {
                  if (Option.isNone(boundary))
                    return yield* Effect.die("Missing composed peer boundary");
                  const caller = yield* boundary.value.current();
                  const result = yield* session
                    .authorize(caller.caller, "workstreams:write")
                    .pipe(Effect.result);
                  const invocation = yield* Mcp.McpInvocationContext;
                  const actual = yield* incoming.current(invocation);
                  return {
                    write_refused: result._tag === "Failure",
                    runtime_mode: actual.session.ceiling.runtime_mode,
                    interaction_mode: actual.session.ceiling.interaction_mode,
                  };
                }).pipe(Effect.orDie),
              ),
            };
          }),
        );
        const clock = yield* Clock.Clock;
        const routes = Layer.mergeAll(
          McpHttpServer.layerNativeHomeReadToolkit,
          McpHttpServer.toolkitRegistration(probe, probes),
        ).pipe(
          Layer.provideMerge(McpHttpServer.layerMcpTransport),
          Layer.provide(McpOAuth.layerMcpClientAuthenticator),
          Layer.provide(
            Layer.mock(McpSessions.McpSessionRegistry)({
              resolve: () => Effect.succeed(undefined),
            }),
          ),
          Layer.provide(prepared.services),
          Layer.provide(
            Layer.mock(Environment.ServerEnvironment)({
              getEnvironmentId: Effect.succeed(
                Wire.EnvironmentId.make(registry.home_environment_id),
              ),
            }),
          ),
          Layer.provide(
            Layer.mock(ThreadManagement.ThreadManagementService)({
              getThreadShell: () => Effect.succeed(null),
            }),
          ),
          Layer.provide(peerAuthentication),
          Layer.provide(f.layer),
          Layer.provide(Layer.succeed(Clock.Clock, clock)),
          Layer.provide(
            HttpRouter.middleware(
              (httpEffect) => httpEffect.pipe(Effect.provideService(Clock.Clock, clock)),
              { global: true },
            ),
          ),
          Layer.provideMerge(
            HttpPlatform.layer.pipe(
              Layer.provideMerge(NodeServices.layer),
              Layer.provideMerge(Etag.layerWeak),
            ),
          ),
        );
        const app = yield* Effect.acquireRelease(
          Effect.sync(() => HttpRouter.toWebHandler(routes, { disableLogger: true })),
          (value) => Effect.promise(() => value.dispose()),
        );
        let sessionId: string | null = null;
        let id = 0;
        const rpc = (method: string, params: unknown) =>
          Effect.gen(function* () {
            const requestId = ++id;
            const response = yield* Effect.promise(() =>
              app.handler(
                new Request("http://synthetic/mcp", {
                  method: "POST",
                  headers: {
                    authorization: "Bearer " + issued.token,
                    "content-type": "application/json",
                    accept: "application/json, text/event-stream",
                    "mcp-protocol-version": "2025-06-18",
                    [Mcp.MODE_LIMIT_HEADER]: "approval-required/plan",
                    ...(sessionId ? { "mcp-session-id": sessionId } : {}),
                  },
                  body: JSON.stringify({ jsonrpc: "2.0", id: requestId, method, params }),
                }),
              ),
            );
            if (response.headers.has("mcp-session-id"))
              sessionId = response.headers.get("mcp-session-id");
            const text = yield* Effect.promise(() => response.text());
            const payload = text.startsWith("data:")
              ? text
                  .split("\n")
                  .filter((l) => l.startsWith("data:"))
                  .map((l) => JSON.parse(l.slice(5)))
                  .find((message) => message.id === requestId)
              : JSON.parse(text);
            return { response, payload };
          });
        const initialized = yield* rpc("initialize", {
          protocolVersion: "2025-06-18",
          capabilities: {},
          clientInfo: { name: "native-peer-entry", version: "1" },
        });
        expect(initialized.response.status).toBe(200);
        expect(sessionId).not.toBeNull();
        const tools = yield* rpc("tools/list", {});
        expect(JSON.stringify(tools.payload)).toContain("jones_workstreams_home_read_result");
        const arguments_ = {
          contract: WORKSTREAMS_HOME_READ_RESULT_CONTRACT,
          request: { route: "read-projection", params: {} },
        };
        const read = yield* rpc("tools/call", {
          name: "jones_workstreams_home_read_result",
          arguments: arguments_,
        });
        expect(read.payload.error, JSON.stringify(read.payload)).toBeUndefined();
        expect(read.payload.result, JSON.stringify(read.payload)).toBeDefined();
        expect(read.payload.result?.isError, JSON.stringify(read.payload)).not.toBe(true);
        const decodedRead = yield* decodeHomeReadResult(
          read.payload.result.structuredContent ?? JSON.parse(read.payload.result.content[0].text),
        );
        expect(decodedRead.envelope.registry.owner_id).toBe(registry.owner_id);
        expect(decodedRead.envelope.provenance.home_environment_id).toBe(
          registry.home_environment_id,
        );
        expect(decodedRead.envelope.provenance.authority).toBe("native-home");
        expect(JSON.stringify(decodedRead.value)).toContain(name);
        const write = yield* rpc("tools/call", {
          name: "synthetic_peer_write_probe",
          arguments: {},
        });
        expect(write.payload.result?.isError).not.toBe(true);
        expect(
          write.payload.result.structuredContent ??
            JSON.parse(write.payload.result.content[0].text),
        ).toEqual({
          write_refused: true,
          runtime_mode: "approval-required",
          interaction_mode: "plan",
        });
        yield* prepared.sessions.revoke({
          sessionId: issued.sessionId,
          revokedAt: yield* DateTime.now,
        });
        // Genuine auth fails before a formerly valid MCP session can read again.
        const refused = yield* Effect.promise(() =>
          app.handler(
            new Request("http://synthetic/mcp", {
              method: "POST",
              headers: {
                authorization: "Bearer " + issued.token,
                "content-type": "application/json",
                accept: "application/json, text/event-stream",
                "mcp-session-id": sessionId!,
              },
              body: JSON.stringify({
                jsonrpc: "2.0",
                id: ++id,
                method: "tools/call",
                params: { name: "jones_workstreams_home_read_result", arguments: arguments_ },
              }),
            }),
          ),
        );
        expect(refused.status).toBe(401);
        expect(yield* Effect.promise(() => refused.text())).not.toContain(name);
      }).pipe(Effect.provide(prepared.services.pipe(Layer.provideMerge(peerAuthentication))));
    }).pipe(Effect.scoped, Effect.provide(f.layer));
  },
);

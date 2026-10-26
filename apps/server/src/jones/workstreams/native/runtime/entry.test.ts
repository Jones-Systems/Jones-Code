// @effect-diagnostics nodeBuiltinImport:off -- Fresh synthetic databases are retained under task-owned evidence; cleanup belongs to root.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
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
import * as Store from "../store/registryStore.ts";
import * as Operations from "../../operations/service.ts";
import * as OperationStore from "../store/operationStore.ts";
import * as Startup from "./startup.ts";
import * as Entry from "./entry.ts";
import * as Admission from "./admission.ts";
import * as RegistrationContext from "../../registrationContext/service.ts";
import * as Schema from "effect/Schema";
import * as NativePlacement from "@t3tools/contracts/jones/workstreamsNativePlacement";
import * as Runtime from "../../runtimeIntegration/native.ts";
import * as Placements from "../registry/placements.ts";
import * as Authority from "../../../../environment/NativeStoreAuthority.ts";
import * as Environment from "../../../../environment/ServerEnvironment.ts";
import * as ProjectionStore from "../../../../orchestration-v2/ProjectionStore.ts";
import * as AuthSessions from "../../../../persistence/AuthSessions.ts";
import { runMigrations } from "../../../../persistence/Migrations.ts";
import nativeMigration from "../../../persistence/Migrations/125_JonesWorkstreamsNative.ts";
import enrollmentMigration from "../../../persistence/Migrations/006_JonesWorkstreamsProviderEnrollments.ts";
import * as Enrollment from "../../enrollment/service.ts";
import { NativeProviderEnrollmentError } from "../../nativeProvider/enrollment.ts";
import { enrollmentRequest } from "../../enrollment/testFixtures.ts";
import { makeProviderFixture } from "../../nativeProvider/testFixtures.ts";
import { makeWorkstreamsNativeProvider } from "../../nativeProvider/service.ts";
import * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import * as Gateway from "../../../../workstreams/WorkstreamGateway.ts";
import * as ControlPlane from "../../../../workstreams/ControlPlaneWorkstreamTransport.ts";
import {
  workstreamHttpApiLayer,
  workstreamResponseHeadersLayer,
} from "../../../../workstreams/http.ts";

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
  const parent = NodePath.resolve("node_modules/.cache/nw-startup-fixtures");
  NodeFS.mkdirSync(parent, { recursive: true });
  const home = NodeFS.mkdtempSync(NodePath.join(parent, "entry-")),
    database = NodePath.join(home, "statev2.sqlite");
  return {
    home,
    database,
    layer: Admin.layer.pipe(
      Layer.provideMerge(Fixture.testLayer({ filename: database })),
      Layer.provideMerge(NodeServices.layer),
    ),
  };
};
const seed = (f: ReturnType<typeof fresh>) =>
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
    const store = yield* Store.RegistryStore;
    yield* store.appendPlain(
      { registry_instance_id: registry.registry_instance_id, owner_id: registry.owner_id },
      "source_bindings",
      {
        provider: "t3",
        source_instance_id: registry.home_environment_id,
        authority_namespace: enrollmentRequest.context.authority_namespace,
        store_generation: enrollmentRequest.context.store_generation,
        binding_version: 1,
        state: "active",
      },
    );
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
    const provider = makeProviderFixture();
    const authority: Authority.NativeStoreAuthority["Service"] = {
      ...provider.ports.authority,
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
    const enrollments = yield* Enrollment.makeNativeEnrollments({
      authority,
      build: Effect.succeed(Option.some(enrollmentRequest.context.build)),
    }).pipe(Effect.provideService(AuthSessions.AuthSessionRepository, sessions));
    yield* enrollments.reserve(enrollmentRequest);
    const runtime: Runtime.NativeWorkstreamsRuntime["Service"] = {
      provider: provider.provider(),
      providerV2: makeWorkstreamsNativeProvider(provider.ports, "v2"),
      enrollments: {
        getBySessionId: (id) =>
          enrollments
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
    const services = Startup.homeServicesLayer({ ...selection, home: selection.home }).pipe(
      Layer.provide(Layer.succeed(Runtime.NativeWorkstreamsRuntime, runtime)),
      Layer.provide(Layer.succeed(Authority.NativeStoreAuthority, authority)),
      Layer.provide(Layer.succeed(Placements.PlacementTrust, authority.trustProvider)),
      Layer.provide(Layer.succeed(AuthSessions.AuthSessionRepository, sessions)),
      Layer.provide(
        Layer.succeed(Environment.ServerEnvironmentIdentity, {
          getEnvironmentId: Effect.succeed(Wire.EnvironmentId.make(registry.home_environment_id)),
        }),
      ),
      Layer.provide(ProjectionStore.layerMemory),
    );
    return { services, sessions };
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
  "production composition shares the exact destination across HTTP, MCP and trusted CLI contexts",
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
          Effect.provideService(Admission.TrustedCli, {
            environmentId: registry.home_environment_id,
            check: () => Effect.void,
          }),
        );
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
    }).pipe(Effect.provide(f.layer));
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
    }).pipe(Effect.provide(f.layer));
  },
);

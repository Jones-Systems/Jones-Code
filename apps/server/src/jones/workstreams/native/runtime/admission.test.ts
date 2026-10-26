import { expect, it } from "@effect/vitest";
import {
  AuthSessionId,
  AuthOrchestrationReadScope,
  AuthOrchestrationOperateScope,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as DateTime from "effect/DateTime";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/sql/SqlClient";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Fixture from "../store/sql.testkit.ts";
import * as Admin from "../cli.ts";
import * as Session from "../registry/session.ts";
import * as Store from "../store/registryStore.ts";
import * as Startup from "./startup.ts";
import * as Admission from "./admission.ts";
import * as Source from "../providers/localAttestation.ts";
import { NativeHome } from "../mode/authorityMode.ts";
import * as Runtime from "../../runtimeIntegration/native.ts";
import * as Authority from "../../../../environment/NativeStoreAuthority.ts";
import { NativeStoreAuthorityPersistenceError } from "../../../../environment/nativeStoreAuthorityPersistence.ts";
import * as Enrollment from "../../enrollment/service.ts";
import { NativeProviderEnrollmentError } from "../../nativeProvider/enrollment.ts";
import { enrollmentRequest as request } from "../../enrollment/testFixtures.ts";
import { makeProviderFixture } from "../../nativeProvider/testFixtures.ts";
import { makeWorkstreamsNativeProvider } from "../../nativeProvider/service.ts";
import * as AuthSessions from "../../../../persistence/AuthSessions.ts";
import { runMigrations } from "../../../../persistence/Migrations.ts";
import nativeMigration from "../../../persistence/Migrations/125_JonesWorkstreamsNative.ts";
import enrollmentMigration from "../../../persistence/Migrations/006_JonesWorkstreamsProviderEnrollments.ts";
import * as Wire from "@t3tools/contracts";
import * as Mcp from "../../../../mcp/McpInvocationContext.ts";

const home = {
  ...Fixture.scope,
  owner_id: request.context.owner_id,
  canonical_origin: request.registry_origin,
  home_environment_id: request.context.source_instance_id,
  served_by_environment_id: request.context.source_instance_id,
  server_generation: 1,
};
const scope = { registry_instance_id: home.registry_instance_id, owner_id: home.owner_id };
const startup: Startup.Selection = {
  mode: "native-home",
  config: { mode: "native-home", registry: home },
  home,
  sourceSessionId: request.session.session_id,
  problem: null,
};
const persistence = Admin.layer.pipe(
  Layer.provideMerge(Fixture.testLayer()),
  Layer.provideMerge(NodeServices.layer),
);
const fixture = Effect.gen(function* () {
  yield* TestClock.setTime(Date.parse(request.session.issued_at));
  yield* runMigrations({ toMigrationInclusive: 54 });
  yield* nativeMigration;
  yield* enrollmentMigration;
  yield* (yield* Admin.NativeAdmin).initialize({
    registry: {
      registry_instance_id: home.registry_instance_id,
      owner_id: home.owner_id,
      canonical_origin: home.canonical_origin,
      home_environment_id: home.home_environment_id,
      server_generation: home.server_generation,
    },
    principal_id: request.context.principal_id,
    content_key_id: "test-home",
    local_key_id: "test-local",
  });
  const sessions = yield* AuthSessions.make,
    sql = yield* SqlClient.SqlClient,
    store = yield* Store.RegistryStore;
  yield* store.appendPlain(scope, "source_bindings", {
    provider: "t3",
    source_instance_id: home.home_environment_id,
    binding_version: 1,
    authority_namespace: request.context.authority_namespace,
    store_generation: request.context.store_generation,
    state: "active",
  });
  let tuple = {
    environmentId: home.home_environment_id,
    authorityNamespace: request.context.authority_namespace,
    storeGeneration: request.context.store_generation,
  };
  let fenced = false;
  const authority: Authority.NativeStoreAuthority["Service"] = {
    readCurrent: Effect.suspend(() =>
      fenced
        ? Effect.fail(new NativeStoreAuthorityPersistenceError("fenced", "Synthetic fence"))
        : Effect.succeed(tuple),
    ),
    trustProvider: {
      readTrustSnapshot: () => ({ readiness: "ready", trustedEnvironments: [tuple] }),
    },
  };
  const enrollments = yield* Enrollment.makeNativeEnrollments({
    authority,
    build: Effect.succeed(Option.some(request.context.build)),
  }).pipe(Effect.provideService(AuthSessions.AuthSessionRepository, sessions));
  yield* enrollments.reserve(request);
  const provider = makeProviderFixture();
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
  const admitted = Admission.layer.pipe(Layer.provideMerge(Session.layer));
  const services = Admission.sourceAdmissionLayer.pipe(
    Layer.provideMerge(admitted),
    Layer.provide(Layer.succeed(Runtime.NativeWorkstreamsRuntime, runtime)),
    Layer.provide(Layer.succeed(Authority.NativeStoreAuthority, authority)),
    Layer.provide(Layer.succeed(Startup.Startup, startup)),
    Layer.provide(Layer.succeed(NativeHome, home)),
  );
  const call = { expected_home: home, operation: "context" as const, thread_id: null };
  let allowWrite = true,
    callerRevoked = false;
  const caller: Admission.CallerEvidence = {
    origin: { kind: "app" },
    check: (permission, threadId) =>
      callerRevoked ||
      (permission === "workstreams:write" && !allowWrite) ||
      (threadId !== null && threadId !== "thread-synthetic")
        ? Effect.fail(new Source.ProviderError({ code: "not_granted" }))
        : Effect.void,
  };
  return {
    sessions,
    sql,
    store,
    authority,
    services,
    runtime,
    call,
    caller,
    setWrite: (value: boolean) => {
      allowWrite = value;
    },
    revokeCaller: () => {
      callerRevoked = true;
    },
    fence: () => {
      fenced = true;
    },
    generation: () => {
      tuple = { ...tuple, storeGeneration: tuple.storeGeneration + 1 };
    },
  };
});

it.effect("production admission returns the actual persisted source-issued enrollment", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* Effect.gen(function* () {
      const admission = yield* Source.SourceAdmission;
      const binding = yield* admission.authorize(f.call);
      expect(binding.session_id).toBe(request.session.session_id);
      expect(binding.enrollment_id).toBe(request.enrollment_id);
      expect(binding.principal_id).toBe(request.context.principal_id);
      expect(binding.build).toEqual(request.context.build);
      expect(
        (yield* admission.authorize({
          ...f.call,
          operation: "attest",
          thread_id: "thread-synthetic",
        })).source_instance_id,
      ).toBe(home.served_by_environment_id);
      expect(
        (yield* Effect.flip(admission.authorize({ ...f.call, thread_id: "other-thread" }))).code,
      ).toBe("not_granted");
      expect(
        (yield* Effect.flip(
          admission.authorize({ ...f.call, expected_home: { ...home, server_generation: 2 } }),
        )).code,
      ).toBe("registry_mismatch");
    }).pipe(Effect.provide(f.services), Effect.provideService(Admission.RequestCaller, f.caller));
  }).pipe(Effect.provide(persistence)),
);

it.effect(
  "source admission requires a request and an existing source session, never fabricates either",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* Effect.gen(function* () {
        expect((yield* Effect.flip((yield* Source.SourceAdmission).authorize(f.call))).code).toBe(
          "provider_unconfigured",
        );
      }).pipe(Effect.provide(f.services));
      yield* f.sessions.revoke({
        sessionId: request.session.session_id,
        revokedAt: DateTime.makeUnsafe(request.session.issued_at),
      });
      yield* Effect.gen(function* () {
        expect((yield* Effect.flip((yield* Source.SourceAdmission).authorize(f.call))).code).toBe(
          "provider_unconfigured",
        );
      }).pipe(Effect.provide(f.services), Effect.provideService(Admission.RequestCaller, f.caller));
    }).pipe(Effect.provide(persistence)),
);

it.effect(
  "caller permissions, grant revocation, generation and fences are rechecked on source operations",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* Effect.gen(function* () {
        const source = yield* Source.SourceAdmission;
        f.setWrite(false);
        yield* source.authorize({
          ...f.call,
          operation: "settle_lookup",
          thread_id: "thread-synthetic",
        });
        expect(
          (yield* Effect.flip(
            source.authorize({ ...f.call, operation: "settle", thread_id: "thread-synthetic" }),
          )).code,
        ).toBe("not_granted");
        f.setWrite(true);
        yield* f.store.appendPlain(scope, "grants", {
          principal_id: request.context.principal_id,
          grant_version: 2,
          grant_id: "grant:revoked",
          permissions: '["workstreams:read","workstreams:write"]',
          state: "revoked",
        });
        expect((yield* Effect.flip(source.authorize(f.call))).code).toBe("not_granted");
      }).pipe(Effect.provide(f.services), Effect.provideService(Admission.RequestCaller, f.caller));
    }).pipe(Effect.provide(persistence)),
);

it.effect(
  "current source binding and authority generation cannot be replaced by enrollment claims",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* Effect.gen(function* () {
        const source = yield* Source.SourceAdmission;
        f.generation();
        expect((yield* Effect.flip(source.authorize(f.call))).code).toBe("generation_mismatch");
        f.fence();
        expect((yield* Effect.flip(source.authorize(f.call))).code).toBe("fenced");
      }).pipe(Effect.provide(f.services), Effect.provideService(Admission.RequestCaller, f.caller));
    }).pipe(Effect.provide(persistence)),
);

it.effect(
  "latest source binding revocation and caller revocation refuse otherwise valid enrollment",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* Effect.gen(function* () {
        const source = yield* Source.SourceAdmission;
        yield* source.authorize(f.call);
        f.revokeCaller();
        expect((yield* Effect.flip(source.authorize(f.call))).code).toBe("not_granted");
      }).pipe(Effect.provide(f.services), Effect.provideService(Admission.RequestCaller, f.caller));
      const requestCaller: Admission.CallerEvidence = {
        origin: { kind: "cli" },
        check: () => Effect.void,
      };
      yield* f.store.appendPlain(scope, "source_bindings", {
        provider: "t3",
        source_instance_id: home.home_environment_id,
        binding_version: 2,
        authority_namespace: request.context.authority_namespace,
        store_generation: request.context.store_generation,
        state: "revoked",
      });
      yield* Effect.gen(function* () {
        expect((yield* Effect.flip((yield* Source.SourceAdmission).authorize(f.call))).code).toBe(
          "generation_mismatch",
        );
      }).pipe(
        Effect.provide(f.services),
        Effect.provideService(Admission.RequestCaller, requestCaller),
      );
    }).pipe(Effect.provide(persistence)),
);

const browserClient = {
  label: null,
  ipAddress: null,
  userAgent: null,
  deviceType: "unknown" as const,
  os: null,
  browser: null,
};
it.effect("HTTP caller evidence keeps each session's scopes and rechecks revocation", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const issuedAt = DateTime.makeUnsafe(request.session.issued_at),
      expiresAt = DateTime.makeUnsafe(request.session.expires_at);
    const a = {
      sessionId: AuthSessionId.make("browser-read"),
      subject: "user-a",
      method: "browser-session-cookie" as const,
      scopes: [AuthOrchestrationReadScope],
      client: browserClient,
      issuedAt,
      expiresAt,
    };
    const b = {
      ...a,
      sessionId: AuthSessionId.make("browser-write"),
      subject: "user-b",
      scopes: [AuthOrchestrationReadScope, AuthOrchestrationOperateScope],
    };
    yield* f.sessions.create(a);
    yield* f.sessions.create(b);
    yield* Effect.gen(function* () {
      const boundary = yield* Admission.CallerBoundary;
      const read = yield* boundary.current().pipe(
        Effect.provideService(Wire.EnvironmentAuthenticatedPrincipal, {
          ...a,
          scopes: new Set(a.scopes),
        }),
      );
      const write = yield* boundary.current().pipe(
        Effect.provideService(Wire.EnvironmentAuthenticatedPrincipal, {
          ...b,
          scopes: new Set(b.scopes),
        }),
      );
      expect(read.origin).toEqual({ kind: "app" });
      yield* read.check("workstreams:read", null);
      expect((yield* Effect.flip(read.check("workstreams:write", null))).code).toBe("not_granted");
      yield* write.check("workstreams:write", null);
      yield* f.sessions.revoke({ sessionId: a.sessionId, revokedAt: issuedAt });
      expect((yield* Effect.flip(read.check("workstreams:read", null))).code).toBe("not_granted");
      yield* write.check("workstreams:write", null);
      expect((yield* Effect.flip(boundary.current())).code).toBe("not_granted");
    }).pipe(
      Effect.provide(Admission.callerBoundaryLayer),
      Effect.provideService(AuthSessions.AuthSessionRepository, f.sessions),
      Effect.provideService(NativeHome, home),
    );
  }).pipe(Effect.provide(persistence)),
);

it.effect("MCP scopes and mode limits stay separate from HTTP and trusted CLI origins", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const sessionId = AuthSessionId.make("mcp-client-synthetic");
    yield* f.sessions.create({
      sessionId,
      subject: "mcp-client",
      method: "bearer-access-token",
      scopes: [AuthOrchestrationReadScope, AuthOrchestrationOperateScope],
      client: browserClient,
      issuedAt: DateTime.makeUnsafe(request.session.issued_at),
      expiresAt: DateTime.makeUnsafe(request.session.expires_at),
    });
    const scope: Mcp.McpInvocationScope = {
      environmentId: Wire.EnvironmentId.make(home.served_by_environment_id),
      capabilities: new Set(["orchestration"]),
      issuedAt: 0,
      requestNamespace: "scheduled-synthetic",
      thread: undefined,
      client: { sessionId, label: "synthetic-agent", access: "full-access" },
    };
    yield* Effect.gen(function* () {
      const boundary = yield* Admission.CallerBoundary;
      const agent = yield* boundary
        .current()
        .pipe(Effect.provideService(Mcp.McpInvocationContext, scope));
      expect(agent.origin).toEqual({ kind: "agent" });
      yield* agent.check("workstreams:write", null);
      const readonly = yield* boundary.current().pipe(
        Effect.provideService(Mcp.McpInvocationContext, {
          ...scope,
          client: { ...scope.client!, access: "read-only" },
        }),
      );
      yield* readonly.check("workstreams:read", null);
      expect((yield* Effect.flip(readonly.check("workstreams:write", null))).code).toBe(
        "not_granted",
      );
      const limited = yield* boundary.current().pipe(
        Effect.provideService(Mcp.McpInvocationContext, {
          ...scope,
          client: {
            ...scope.client!,
            narrowedTo: { runtimeMode: "full-access", interactionMode: "plan" },
          },
        }),
      );
      expect((yield* Effect.flip(limited.check("workstreams:write", null))).code).toBe(
        "not_granted",
      );
      const linked = yield* boundary.current().pipe(
        Effect.provideService(Mcp.McpInvocationContext, {
          ...scope,
          client: { ...scope.client!, linked: true },
        }),
      );
      expect((yield* Effect.flip(linked.check("workstreams:read", null))).code).toBe("not_granted");
      const cli = yield* boundary.current().pipe(
        Effect.provideService(Admission.TrustedCli, {
          environmentId: home.served_by_environment_id,
          check: () => Effect.void,
        }),
      );
      expect(cli.origin).toEqual({ kind: "cli" });
      yield* f.sessions.revoke({
        sessionId,
        revokedAt: DateTime.makeUnsafe(request.session.issued_at),
      });
      expect((yield* Effect.flip(agent.check("workstreams:read", null))).code).toBe("not_granted");
    }).pipe(
      Effect.provide(Admission.callerBoundaryLayer),
      Effect.provideService(AuthSessions.AuthSessionRepository, f.sessions),
      Effect.provideService(NativeHome, home),
    );
  }).pipe(Effect.provide(persistence)),
);

// @effect-diagnostics nodeBuiltinImport:off -- File-backed synthetic restart evidence is task-owned and retained for parent delivery.
import * as NodeFS from "node:fs";
import * as NodeCrypto from "node:crypto";
import { vi } from "vite-plus/test";
import { expect, it } from "@effect/vitest";
import { AuthSessionId } from "@t3tools/contracts";
import * as Wire from "@t3tools/contracts";
import * as Recorded from "./recordedCaller.ts";
import * as Recovery from "./recovery.ts";
import * as ProjectionStore from "../../../../orchestration-v2/ProjectionStore.ts";
import * as Environment from "../../../../environment/ServerEnvironment.ts";
import { canonicalWorkstreamsSnapshotJson } from "@t3tools/contracts/jones/workstreamsSnapshot";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/sql/SqlClient";
import * as AuthSessions from "../../../../persistence/AuthSessions.ts";
import { NativeStoreAuthorityPersistenceError } from "../../../../environment/nativeStoreAuthorityPersistence.ts";
import { runMigrations } from "../../../../persistence/Migrations.ts";
import migration from "../../../persistence/Migrations/126_JonesWorkstreamsSourceEnrollments.ts";
import { deriveNativeEnrollmentSession } from "../../enrollment/request.ts";
import { enrollmentRequest } from "../../enrollment/testFixtures.ts";
import * as Setup from "./sourceSetup.ts";
import * as Source from "./sourceEnrollment.ts";
import * as Records from "../store/sourceEnrollmentStore.ts";
import * as Registries from "../store/registryStore.ts";
import * as Bindings from "../store/bindingStore.ts";
import * as Admin from "../cli.ts";
import nativeMigration from "../../../persistence/Migrations/125_JonesWorkstreamsNative.ts";
import * as Admission from "./admission.ts";
import * as Startup from "./startup.ts";
import * as Session from "../registry/session.ts";
import * as Local from "../providers/localAttestation.ts";
import * as Runtime from "../../runtimeIntegration/native.ts";
import { NativeHome } from "../mode/authorityMode.ts";
import { NativeStoreAuthority } from "../../../../environment/NativeStoreAuthority.ts";
import * as Orchestrator from "../../../../orchestration-v2/Orchestrator.ts";
import * as ServerConfig from "../../../../config.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { scope, now, binding, testLayer, secrets, keyBytes } from "../store/sql.testkit.ts";

const request: Source.NativeSourceSetupRequest = {
  schema: "jones-code.native-source-setup/v1",
  enrollment: {
    ...enrollmentRequest,
    registry_origin: "https://native.workstreams.invalid",
    context: {
      ...enrollmentRequest.context,
      owner_id: scope.owner_id,
      principal_id: binding.principal_id,
      source_instance_id: "home:test",
      authority_namespace: "synthetic-authority",
      store_generation: 1,
    },
  },
  expected: {
    ...scope,
    home_environment_id: "home:test",
    home_generation: 1,
    registry_version: 1,
    singleton_binding_version: 1,
    grant_id: "grant:principal:test",
    grant_version: 1,
    source_binding: null,
  },
  rebind: false,
};
const authority = {
  environmentId: "home:test",
  authorityNamespace: "synthetic-authority",
  storeGeneration: 1,
};
const fixture = Effect.gen(function* () {
  yield* TestClock.setTime(Date.parse(now));
  yield* runMigrations({ toMigrationInclusive: 54 });
  yield* nativeMigration;
  yield* migration;
  const r = yield* Registries.RegistryStore;
  yield* (yield* Admin.NativeAdmin).initialize({
    registry: {
      ...scope,
      canonical_origin: "https://native.workstreams.invalid",
      home_environment_id: "home:test",
      server_generation: 1,
    },
    principal_id: binding.principal_id,
    content_key_id: "test-home",
    local_key_id: "test-local",
  });
  return {
    setup: yield* Setup.NativeSourceSetup,
    source: yield* Source.NativeSourceEnrollments,
    sessions: yield* AuthSessions.AuthSessionRepository,
    sql: yield* SqlClient.SqlClient,
    registries: r,
  };
});
const keys = () => new Map([...secrets(), ["server-signing-key", keyBytes]]);
const services = (
  options: {
    keys?: Map<string, Uint8Array>;
    current?: typeof authority;
    missingBuild?: boolean;
    failAuthority?: boolean;
    changeAt?: number;
    owner?: boolean;
    filename?: string;
  } = {},
) => {
  let reads = 0;
  const issuer = Layer.succeed(Source.SourceIssuer, {
    authority: {
      readCurrent: Effect.suspend(() => {
        reads++;
        if (options.failAuthority)
          return Effect.fail(
            new NativeStoreAuthorityPersistenceError("source_unavailable", "Synthetic unavailable"),
          );
        return Effect.succeed(
          options.changeAt && reads >= options.changeAt
            ? { ...authority, storeGeneration: 2 }
            : (options.current ?? authority),
        );
      }),
    },
    build: Effect.succeed(
      options.missingBuild ? Option.none() : Option.some(enrollmentRequest.context.build),
    ),
  });
  const all = Setup.layer.pipe(
    Layer.provideMerge(Source.layer),
    Layer.provideMerge(Records.layer),
    Layer.provideMerge(AuthSessions.layer),
    Layer.provideMerge(Admin.layer),
    Layer.provideMerge(
      testLayer({
        keys: options.keys ?? keys(),
        ...(options.filename ? { filename: options.filename } : {}),
      }),
    ),
    Layer.provide(issuer),
  );
  return options.owner === false
    ? all
    : all.pipe(
        Layer.provide(Layer.succeed(Source.SourceSetupOwner, { verifyUnchanged: Effect.void })),
      );
};
const counts = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return [
    (yield* sql`SELECT * FROM auth_sessions`).length,
    (yield* sql`SELECT * FROM jones_workstreams_source_enrollments`).length,
    (yield* sql`SELECT * FROM jones_workstreams_source_bindings`).length,
  ];
});

it.effect(
  "explicit issuance binds once; lost reply readback and same-ID replay preserve exact proof",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      expect(yield* f.setup.plan(request)).toMatchObject({ state: "ready" });
      expect(yield* f.setup.readback(request)).toBeNull();
      expect(yield* counts).toEqual([0, 0, 0]);
      const proof = yield* f.setup.apply(request);
      expect(proof.receipt).toEqual({ registry_version: 2, source_binding_version: 1 });
      expect(yield* f.setup.readback(request)).toEqual(proof);
      expect(yield* f.setup.apply(request)).toEqual(proof);
      expect(yield* f.setup.plan(request)).toMatchObject({ state: "applied" });
      expect(yield* f.source.getBySessionId(request.enrollment.session.session_id)).toMatchObject({
        _tag: "Some",
        value: {
          principal_id: binding.principal_id,
          authority_namespace: authority.authorityNamespace,
        },
      });
      expect(yield* counts).toEqual([1, 1, 1]);
      expect((yield* f.registries.context(scope))?.registry_version).toBe(2);
      expect(
        yield* f.sql`SELECT name FROM sqlite_master WHERE name = 'workstreams_native_enrollments'`,
      ).toHaveLength(0);
    }).pipe(Effect.provide(services())),
);

it.effect("every stale CAS pin refuses without session, ledger or history writes", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const variants: Source.NativeSourceSetupRequest[] = [
      { ...request, expected: { ...request.expected, registry_version: 0 } },
      { ...request, expected: { ...request.expected, registry_instance_id: "another-registry" } },
      { ...request, expected: { ...request.expected, owner_id: "another-owner" } },
      { ...request, expected: { ...request.expected, home_environment_id: "another-home" } },
      { ...request, expected: { ...request.expected, home_generation: 2 } },
      { ...request, expected: { ...request.expected, singleton_binding_version: 2 } },
      { ...request, expected: { ...request.expected, grant_version: 2 } },
      { ...request, expected: { ...request.expected, grant_id: "another-grant" } },
      {
        ...request,
        expected: {
          ...request.expected,
          source_binding: {
            binding_version: 1,
            authority_namespace: "old",
            store_generation: 1,
            state: "active",
          },
        },
      },
      {
        ...request,
        enrollment: {
          ...request.enrollment,
          context: { ...request.enrollment.context, principal_id: "another-principal" },
        },
      },
      {
        ...request,
        enrollment: {
          ...request.enrollment,
          context: { ...request.enrollment.context, authority_namespace: "invented" },
        },
      },
    ];
    for (const value of variants) {
      expect((yield* Effect.result(f.setup.apply(value)))._tag).toBe("Failure");
      expect(yield* counts).toEqual([0, 0, 0]);
      expect((yield* f.registries.context(scope))?.registry_version).toBe(1);
    }
  }).pipe(Effect.provide(services())),
);

it.effect(
  "authority changing after issuance rolls back session, binding and encrypted ledger together",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      expect((yield* Effect.flip(f.setup.apply(request))).code).toBe("authority_mismatch");
      expect(yield* counts).toEqual([0, 0, 0]);
      expect((yield* f.registries.context(scope))?.registry_version).toBe(1);
    }).pipe(Effect.provide(services({ changeAt: 2 }))),
);

it.effect("same ID with altered session or pins never issues a second enrollment", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* f.setup.apply(request);
    const changed = {
      ...request,
      enrollment: {
        ...request.enrollment,
        session: {
          ...request.enrollment.session,
          session_id: AuthSessionId.make("changed-session"),
        },
      },
    };
    expect((yield* Effect.flip(f.setup.apply(changed))).code).toBe("issuance_conflict");
    expect(yield* counts).toEqual([1, 1, 1]);
  }).pipe(Effect.provide(services())),
);

it.effect("revocation and expiry fence validated lookup and never renew on read", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* f.setup.apply(request);
    yield* TestClock.setTime(Date.parse(request.enrollment.session.expires_at));
    expect(
      (yield* Effect.flip(f.source.getBySessionId(request.enrollment.session.session_id))).code,
    ).toBe("session_conflict");
    yield* TestClock.setTime(Date.parse(now));
    yield* f.sessions.revoke({
      sessionId: request.enrollment.session.session_id,
      revokedAt: DateTime.makeUnsafe(now),
    });
    expect(
      (yield* Effect.flip(f.source.getValidatedBySessionId(request.enrollment.session.session_id)))
        .code,
    ).toBe("session_conflict");
    expect((yield* Effect.flip(f.setup.apply(request))).code).toBe("session_conflict");
    expect(yield* counts).toEqual([1, 1, 1]);
  }).pipe(Effect.provide(services())),
);

it.effect("grant revocation fences issued evidence", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* f.setup.apply(request);
    yield* f.registries.appendPlain(scope, "grants", {
      principal_id: binding.principal_id,
      grant_version: 2,
      grant_id: "grant:test",
      permissions: "[]",
      state: "revoked",
    });
    expect(
      (yield* Effect.flip(f.source.getBySessionId(request.enrollment.session.session_id))).code,
    ).toBe("pin_mismatch");
    expect(yield* counts).toEqual([1, 1, 1]);
  }).pipe(Effect.provide(services())),
);

it.effect(
  "explicit authentic generation rebind appends history and refuses old enrollment pins",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.registries.appendPlain(scope, "source_bindings", {
        provider: "t3",
        source_instance_id: "home:test",
        binding_version: 1,
        authority_namespace: authority.authorityNamespace,
        store_generation: 1,
        state: "active",
      });
      const next = {
        ...request,
        enrollment: {
          ...request.enrollment,
          context: { ...request.enrollment.context, store_generation: 2 },
        },
        expected: {
          ...request.expected,
          registry_version: 2,
          source_binding: {
            binding_version: 1,
            authority_namespace: authority.authorityNamespace,
            store_generation: 1,
            state: "active" as const,
          },
        },
      };
      expect((yield* Effect.flip(f.setup.apply(next))).code).toBe("rebind_required");
      const proof = yield* f.setup.apply({ ...next, rebind: true });
      expect(proof.receipt).toEqual({ registry_version: 3, source_binding_version: 2 });
      expect(yield* counts).toEqual([1, 1, 2]);
      expect((yield* f.registries.listPlain(scope, "source_bindings"))[0]).toMatchObject({
        store_generation: 1,
      });
    }).pipe(Effect.provide(services({ current: { ...authority, storeGeneration: 2 } }))),
);

it.effect.each([
  ["missing signing key", { keys: secrets() }, "key_unavailable"],
  ["missing build", { missingBuild: true }, "build_unavailable"],
  ["missing authority", { failAuthority: true }, "authority_unavailable"],
  ["missing owner", { owner: false }, "owner_unqualified"],
] as const)("%s refuses without creating state", ([_name, options, code]) =>
  Effect.gen(function* () {
    const f = yield* fixture;
    expect((yield* Effect.flip(f.setup.apply(request))).code).toBe(code);
    expect(yield* counts).toEqual([0, 0, 0]);
  }).pipe(Effect.provide(services(options))),
);

it.effect("ordinary construction and missing lookup do not issue or bind", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    expect(Option.isNone(yield* f.source.getBySessionId("unknown-selector"))).toBe(true);
    expect(yield* counts).toEqual([0, 0, 0]);
  }).pipe(Effect.provide(services())),
);

it.effect("same-request concurrent applies commit one issuance and one registry bump", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const results = yield* Effect.all([f.setup.apply(request), f.setup.apply(request)], {
      concurrency: 2,
    });
    expect(results[0]).toEqual(results[1]);
    expect(yield* counts).toEqual([1, 1, 1]);
    expect((yield* f.registries.context(scope))?.registry_version).toBe(2);
  }).pipe(Effect.provide(services())),
);

it.effect(
  "file-backed restart readback adopts committed issuance without another session or binding",
  () =>
    Effect.gen(function* () {
      const dir = "node_modules/.cache/nw-source-setup/" + NodeCrypto.randomUUID();
      NodeFS.mkdirSync(dir, { recursive: true });
      const filename = dir + "/source.sqlite";
      const proof = yield* Effect.gen(function* () {
        const f = yield* fixture;
        return yield* f.setup.apply(request);
      }).pipe(Effect.provide(services({ filename })));
      const reopened = yield* Effect.gen(function* () {
        const setup = yield* Setup.NativeSourceSetup;
        expect(yield* setup.readback(request)).toEqual(proof);
        expect(yield* setup.apply(request)).toEqual(proof);
        expect(yield* counts).toEqual([1, 1, 1]);
        return yield* setup.readback(request);
      }).pipe(Effect.provide(services({ filename })));
      expect(reopened).toEqual(proof);
    }),
);

it.effect(
  "mounted runtime selection supplies issued native enrollment to source admission with CP and network denied",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.setup.apply(request);
      const home = {
        ...scope,
        canonical_origin: request.enrollment.registry_origin,
        home_environment_id: "home:test",
        server_generation: 1,
        served_by_environment_id: "home:test",
      };
      const selection: Startup.Selection & { home: typeof home } = {
        mode: "native-home",
        config: null,
        home,
        sourceSessionId: request.enrollment.session.session_id,
        problem: null,
      };
      const sourceLayer = Admission.sourceAdmissionLayer.pipe(
        Layer.provideMerge(Admission.layer.pipe(Layer.provideMerge(Session.layer))),
      );
      const selected = Startup.chooseLayer({
        legacy: () =>
          Layer.effect(Local.SourceAdmission, Effect.die("Control Plane construction denied")),
        home: () => sourceLayer,
        unavailable: () =>
          Layer.effect(Local.SourceAdmission, Effect.die("unexpected unavailable selection")),
      });
      const fetch = vi.spyOn(globalThis, "fetch").mockImplementation(() => {
        throw new Error("Network denied");
      });
      try {
        const enrolled = yield* Effect.gen(function* () {
          return yield* (yield* Local.SourceAdmission).authorize({
            expected_home: { ...scope, home_environment_id: "home:test", server_generation: 1 },
            operation: "context",
            thread_id: null,
          });
        }).pipe(
          Effect.provide(
            selected.pipe(
              Layer.provideMerge(
                Runtime.nativeWorkstreamsRuntimeLayer.pipe(
                  Layer.provide(Layer.mock(Orchestrator.OrchestratorV2)({})),
                  Layer.provide(
                    ServerConfig.layerTest(
                      ".",
                      NodeFS.mkdtempSync("node_modules/.cache/nw-source-setup-runtime-"),
                    ).pipe(Layer.provide(NodeServices.layer)),
                  ),
                ),
              ),
            ),
          ),
          Effect.provideService(Startup.Startup, selection),
          Effect.provideService(NativeHome, home),
          Effect.provideService(Admission.RequestCaller, {
            origin: { kind: "app" },
            check: () => Effect.void,
          }),
          Effect.provideService(NativeStoreAuthority, {
            readCurrent: Effect.succeed(authority),
            trustProvider: {
              readTrustSnapshot: () => ({ trustedEnvironments: [authority], readiness: "ready" }),
            },
          }),
        );
        expect(enrolled.session_id).toBe(request.enrollment.session.session_id);
        expect(fetch).not.toHaveBeenCalled();
      } finally {
        fetch.mockRestore();
      }
    }).pipe(Effect.provide(services())),
);

it.effect("removed content key refuses before source session issuance", () => {
  const values = keys();
  return Effect.gen(function* () {
    const f = yield* fixture;
    values.delete("workstreams-content-key-test-home");
    expect((yield* Effect.flip(f.setup.apply(request))).code).toBe("key_unavailable");
    expect(yield* counts).toEqual([0, 0, 0]);
  }).pipe(Effect.provide(services({ keys: values })));
});

it.effect("a preexisting session cannot be converted into source issuance", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* f.sessions.create(deriveNativeEnrollmentSession(request.enrollment));
    expect((yield* Effect.flip(f.setup.plan(request))).code).toBe("session_conflict");
    expect((yield* Effect.flip(f.setup.apply(request))).code).toBe("session_conflict");
    expect(yield* counts).toEqual([1, 0, 0]);
  }).pipe(Effect.provide(services())),
);

it.effect("authentic source generation changes require rebind and preserve older proof", () => {
  const current = { ...authority };
  return Effect.gen(function* () {
    const f = yield* fixture;
    const old = yield* f.setup.apply(request);
    current.storeGeneration = 2;
    expect(
      (yield* Effect.flip(f.source.getValidatedBySessionId(request.enrollment.session.session_id)))
        .code,
    ).toBe("authority_mismatch");
    const next: Source.NativeSourceSetupRequest = {
      ...request,
      rebind: true,
      enrollment: {
        ...request.enrollment,
        enrollment_id: "generation-two",
        context: {
          ...request.enrollment.context,
          enrollment_id: "generation-two",
          store_generation: 2,
        },
        session: {
          ...request.enrollment.session,
          session_id: AuthSessionId.make("generation-two-session"),
        },
      },
      expected: {
        ...request.expected,
        registry_version: 2,
        source_binding: {
          binding_version: 1,
          authority_namespace: authority.authorityNamespace,
          store_generation: 1,
          state: "active",
        },
      },
    };
    yield* f.setup.apply(next);
    expect(yield* f.source.readProof({ enrollment_id: request.enrollment.enrollment_id })).toEqual(
      old,
    );
    expect(
      Option.isSome(yield* f.source.getValidatedBySessionId(next.enrollment.session.session_id)),
    ).toBe(true);
    expect(
      (yield* Effect.flip(f.source.getValidatedBySessionId(request.enrollment.session.session_id)))
        .code,
    ).toBe("authority_mismatch");
    expect(yield* counts).toEqual([2, 2, 2]);
  }).pipe(Effect.provide(services({ current })));
});

it.effect("new home generation and changed singleton fence old pins without retargeting", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const proof = yield* f.setup.apply(request);
    yield* f.registries.advance(
      scope,
      { server_generation: 1, registry_version: 2 },
      { generation: true },
    );
    yield* (yield* Bindings.BindingStore).bind(
      { ...binding, home_generation: 2, binding_version: 2 },
      1,
    );
    expect(
      (yield* Effect.flip(f.source.getValidatedBySessionId(request.enrollment.session.session_id)))
        .code,
    ).toBe("pin_mismatch");
    expect(yield* f.source.readProof({ enrollment_id: request.enrollment.enrollment_id })).toEqual(
      proof,
    );
    expect(yield* counts).toEqual([1, 1, 1]);
  }).pipe(Effect.provide(services())),
);

it.effect("reserved bearer materialization stays separate from credential-free proof", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const proof = yield* f.setup.apply(request);
    const token = yield* f.source.materialize(request.enrollment.session.session_id);
    expect(token.length).toBeGreaterThan(32);
    expect(Object.keys(proof).sort()).toEqual(["receipt", "request", "request_sha256", "schema"]);
    expect(canonicalWorkstreamsSnapshotJson(proof).includes(token)).toBe(false);
    yield* f.sessions.revoke({
      sessionId: request.enrollment.session.session_id,
      revokedAt: DateTime.makeUnsafe(now),
    });
    expect(
      (yield* Effect.flip(f.source.materialize(request.enrollment.session.session_id))).code,
    ).toBe("session_conflict");
  }).pipe(Effect.provide(services())),
);

it.effect("missing native ledger refuses setup without creating sessions or source bindings", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(Date.parse(now));
    yield* runMigrations({ toMigrationInclusive: 54 });
    yield* nativeMigration;
    yield* (yield* Admin.NativeAdmin).initialize({
      registry: {
        ...scope,
        canonical_origin: request.enrollment.registry_origin,
        home_environment_id: "home:test",
        server_generation: 1,
      },
      principal_id: binding.principal_id,
      content_key_id: "test-home",
      local_key_id: "test-local",
    });
    const setup = yield* Setup.NativeSourceSetup;
    expect((yield* Effect.flip(setup.plan(request))).code).toBe("unavailable");
    expect((yield* Effect.flip(setup.apply(request))).code).toBe("unavailable");
    const sql = yield* SqlClient.SqlClient;
    expect(yield* sql`SELECT * FROM auth_sessions`).toHaveLength(0);
    expect(yield* sql`SELECT * FROM jones_workstreams_source_bindings`).toHaveLength(0);
    expect((yield* (yield* Registries.RegistryStore).context(scope))?.registry_version).toBe(1);
  }).pipe(Effect.provide(services())),
);

it.effect("another home's shaped request is not authentic source authority", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* f.setup.apply(request);
    const other: Source.NativeSourceSetupRequest = {
      ...request,
      enrollment: {
        ...request.enrollment,
        enrollment_id: "other-home",
        context: {
          ...request.enrollment.context,
          enrollment_id: "other-home",
          source_instance_id: "home:other",
        },
        session: {
          ...request.enrollment.session,
          session_id: AuthSessionId.make("other-home-session"),
        },
      },
      expected: { ...request.expected, home_environment_id: "home:other", registry_version: 2 },
    };
    expect((yield* Effect.flip(f.setup.apply(other))).code).toBe("authority_mismatch");
    expect(yield* counts).toEqual([1, 1, 1]);
    expect((yield* f.registries.context(scope))?.home_environment_id).toBe("home:test");
  }).pipe(Effect.provide(services())),
);

it.effect(
  "mounted home recovery uses validated native enrollment and original authenticated caller with CP and network denied",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.setup.apply(request);
      const home = {
        ...scope,
        canonical_origin: request.enrollment.registry_origin,
        home_environment_id: "home:test",
        server_generation: 1,
        served_by_environment_id: "home:test",
      };
      const selection: Startup.Selection & { home: typeof home } = {
        mode: "native-home",
        config: null,
        home,
        sourceSessionId: request.enrollment.session.session_id,
        problem: null,
      };
      const sessionId = AuthSessionId.make("recorded-mounted-caller");
      const caller = {
        sessionId,
        subject: "mounted-owner",
        method: "browser-session-cookie" as const,
        scopes: [Wire.AuthOrchestrationReadScope, Wire.AuthOrchestrationOperateScope],
        client: {
          label: null,
          ipAddress: null,
          userAgent: null,
          deviceType: "unknown" as const,
          os: null,
          browser: null,
        },
        issuedAt: DateTime.makeUnsafe(request.enrollment.session.issued_at),
        expiresAt: DateTime.makeUnsafe(request.enrollment.session.expires_at),
      };
      yield* f.sessions.create(caller);
      const base = ServerConfig.layerTest(
        ".",
        NodeFS.mkdtempSync("node_modules/.cache/nw-recorded-mounted-"),
      ).pipe(Layer.provide(NodeServices.layer));
      const resources = Runtime.nativeWorkstreamsRuntimeLayer.pipe(
        Layer.provide(Layer.mock(Orchestrator.OrchestratorV2)({})),
        Layer.provide(base),
      );
      const selected = Startup.homeServicesLayer(selection).pipe(
        Layer.provideMerge(resources),
        Layer.provide(
          Layer.mock(ProjectionStore.ProjectionStoreV2)({
            getThreadShell: () => Effect.succeed(null),
          }),
        ),
        Layer.provide(
          Layer.succeed(Environment.ServerEnvironmentIdentity, {
            getEnvironmentId: Effect.succeed(Wire.EnvironmentId.make("home:test")),
          }),
        ),
      );
      const fetch = vi.spyOn(globalThis, "fetch").mockImplementation(() => {
        throw new Error("Network denied");
      });
      try {
        yield* Effect.gen(function* () {
          const callers = yield* Recorded.RecordedCallers;
          const recovery = yield* Recovery.NativeWorkstreamsRecovery;
          const record = yield* callers.captureCaller().pipe(
            Effect.provideService(Wire.EnvironmentAuthenticatedPrincipal, {
              ...caller,
              scopes: new Set(caller.scopes),
            }),
          );
          expect(record.auth).toMatchObject({ kind: "bearer", session_id: sessionId });
          expect(record.source.enrollment_session_id).toBe(request.enrollment.session.session_id);
          expect(record.source.enrollment_session_id).not.toBe(sessionId);
          expect(record.source.binding_version).toBe(1);
          yield* (yield* callers.readmitRecorded(record)).check("workstreams:write", null);
          expect(yield* recovery.sweep()).toEqual({ attempted: 0, fenced: 0, deferred: 0 });
          yield* f.sessions.revoke({
            sessionId,
            revokedAt: DateTime.makeUnsafe(request.enrollment.session.issued_at),
          });
          expect((yield* Effect.flip(callers.readmitRecorded(record))).code).toBe("not_granted");
          expect(fetch).not.toHaveBeenCalled();
        }).pipe(
          Effect.provide(selected),
          Effect.provideService(Startup.Startup, selection),
          Effect.provideService(NativeHome, home),
          Effect.provideService(NativeStoreAuthority, {
            readCurrent: Effect.succeed(authority),
            trustProvider: {
              readTrustSnapshot: () => ({ trustedEnvironments: [authority], readiness: "ready" }),
            },
          }),
        );
      } finally {
        fetch.mockRestore();
      }
    }).pipe(Effect.provide(services())),
);

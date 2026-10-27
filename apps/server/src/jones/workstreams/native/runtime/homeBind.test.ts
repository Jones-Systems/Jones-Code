// @effect-diagnostics nodeBuiltinImport:off -- Isolated SQLite fixtures are disposable unless an explicit evidence directory is supplied.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import { expect, it } from "@effect/vitest";
import { AuthSessionId, EnvironmentId } from "@t3tools/contracts";
import * as F from "@t3tools/contracts/jones/workstreamsHomeBind";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/sql/SqlClient";
import * as Auth from "../../../../auth/EnvironmentAuth.ts";
import * as AuthRows from "../../../../persistence/AuthSessions.ts";
import * as Secrets from "../../../../auth/ServerSecretStore.ts";
import * as Config from "../../../../config.ts";
import * as Environment from "../../../../environment/ServerEnvironment.ts";
import * as Authority from "../../../../environment/NativeStoreAuthority.ts";
import { runMigrations } from "../../../../persistence/Migrations.ts";
import pinsMigration from "../../../persistence/Migrations/128_JonesWorkstreamsPeerSessionPins.ts";
import migration from "../../../persistence/Migrations/129_JonesWorkstreamsMemberBindings.ts";
import * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import * as PinRecords from "../store/peerSessionPinStore.ts";
import * as Records from "../store/homeBindReceiptStore.ts";
import * as Test from "../store/sql.testkit.ts";
import * as Registries from "../store/registryStore.ts";
import * as Database from "../store/sql.ts";
import * as Keys from "../crypto/keyProvider.ts";
import * as Pins from "../runtime/peerSessionPins.ts";
import * as Setup from "../runtime/peerSessionPinSetup.ts";
import * as Owner from "../runtime/sourceEnrollment.ts";
import * as Qualification from "../runtime/homePeerPinQualification.ts";
import * as Home from "../runtime/homeBind.ts";
import { sha256EnrollmentBytes as digest } from "../../enrollment/request.ts";

const home = { ...Test.scope, home_environment_id: "home:test", server_generation: 1 };
const homeAuthority = {
  environment_id: "home:test",
  authority_namespace: "home-store",
  store_generation: 1,
};
const sourceAuthority = {
  environment_id: "source:test",
  authority_namespace: "source-store",
  store_generation: 1,
};
const signing = Layer.succeed(
  Secrets.ServerSecretStore,
  Secrets.ServerSecretStore.of({
    get: () => Effect.succeed(Option.some(Test.keyBytes)),
    getOrCreateRandom: () => Effect.succeed(Test.keyBytes),
    set: () => Effect.die("unexpected set"),
    create: () => Effect.die("unexpected create"),
    remove: () => Effect.die("unexpected remove"),
  }),
);
const serviceLayer = (filename?: string) =>
  Layer.mergeAll(PinRecords.layer, Records.layer, Pins.nativeLayer).pipe(
    Layer.provideMerge(Auth.layer.pipe(Layer.provide(signing))),
    Layer.provideMerge(Test.testLayer(filename ? { filename } : {})),
    Layer.provideMerge(Config.layerTest(process.cwd(), { prefix: "nw-home-bind-synthetic-" })),
    Layer.provideMerge(
      Layer.succeed(Environment.ServerEnvironmentIdentity, {
        getEnvironmentId: Effect.succeed(EnvironmentId.make("home:test")),
      }),
    ),
    Layer.provideMerge(NodeServices.layer),
  );
const services = serviceLayer();
// Real SQL, AES-GCM, migrations, native issuance and native WeakMap authentication. The owner
// path/build/qualification and local authority are disclosed synthetic external ports, not runtime proof.
const fixture = Effect.gen(function* () {
  yield* TestClock.setTime(Date.parse(Test.now));
  yield* runMigrations({ toMigrationInclusive: 54 });
  yield* Test.seed;
  yield* pinsMigration;
  yield* migration;
  const auth = yield* Auth.EnvironmentAuth,
    native = yield* Pins.NativePeerSessions,
    pins = yield* PinRecords.PeerSessionPinStore,
    records = yield* Records.HomeBindReceiptStore,
    registry = yield* Registries.RegistryStore,
    sql = yield* SqlClient.SqlClient,
    authRows = yield* AuthRows.AuthSessionRepository,
    db = yield* Database.NativeSql,
    keys = yield* Keys.WorkstreamsKeyProvider;
  const issued = yield* auth.issueMcpClientSession({
    label: "synthetic-home-peer",
    access: "full-access",
    peerLink: true,
    client: { deviceType: "bot" },
  });
  const invocation: Mcp.McpInvocationScope = {
    environmentId: EnvironmentId.make("home:test"),
    issuedAt: Date.parse(Test.now),
    requestNamespace: "home-bind-synthetic",
    capabilities: new Set(["orchestration"]),
    thread: undefined,
    client: {
      sessionId: issued.sessionId,
      label: "different-label",
      access: "full-access",
      linked: true,
    },
  };
  const nativeRequest = {
    cookies: {},
    headers: { authorization: `Bearer ${issued.token}` },
  } as unknown as Parameters<Auth.EnvironmentAuth["Service"]["authenticateMcpClient"]>[0];
  yield* native.authenticate(nativeRequest, invocation);
  const facts = yield* native.current(invocation);
  const setupBody =
    JSON.stringify(
      {
        schema: "jones-workstreams-peer-pin-setup/1",
        assertion: {
          schema: "jones-workstreams-peer-session-pin/1",
          pin_id: "home-pin",
          revision: 1,
          receiver_authority: homeAuthority,
          session: facts.session,
          peer_environment_id: "source:test",
          roles: ["member_home"],
          expected_home: home,
          source_authority: sourceAuthority,
          source_enrollment_id: "enrollment:test",
          source_binding_version: null,
          singleton_binding_version: null,
          state: "prepared",
          expires_at: "2026-01-01T00:20:00.000Z",
          setup_request_id: "home-setup",
          owner_qualification_digest: "b".repeat(64),
          bind_receipt: null,
        },
        expected: { registry_version: 1, grant_id: "grant:test", grant_version: 1 },
      },
      null,
      2,
    ) + "\n";
  const pin = PinRecords.decodeSetupRequest(setupBody).pin;
  yield* pins.insert({ pin, request_body: setupBody });
  yield* registry.appendPlain(Test.scope, "principals", {
    principal_id: "principal:test",
    kind: "environment",
    environment_id: "source:test",
    provenance: "{}",
  });
  yield* registry.appendPlain(Test.scope, "grants", {
    grant_id: "grant:test",
    principal_id: "principal:test",
    grant_version: 1,
    permissions: JSON.stringify(["workstreams:read", "workstreams:write"]),
    state: "active",
  });
  const bodyValue: F.WorkstreamsHomeBindBodyV2 = {
    bind_id: "bind:test",
    expected_home: home,
    expected_canonical_origin: "https://native.workstreams.invalid",
    expected_home_authority: homeAuthority,
    source: {
      authority: sourceAuthority,
      enrollment: {
        schema: "jones-code.workstreams-native-enrollment/v1",
        enrollment_id: "enrollment:test",
        registry_origin: "https://native.workstreams.invalid",
        context: {
          owner_id: "owner:test",
          principal_id: "principal:test",
          source_instance_id: "source:test",
          authority_namespace: "source-store",
          store_generation: 1,
          enrollment_id: "enrollment:test",
          protocol: "workstreams-t3-provider/1.0.0",
          build: {
            repository: "Jones-Systems/Jones-Code",
            sha: "a".repeat(40),
            tree: "b".repeat(40),
          },
        },
        session: {
          session_id: AuthSessionId.make("provider-session"),
          issued_at: Test.now,
          expires_at: "2026-01-02T00:00:00.000Z",
        },
      },
    },
    home_setup: {
      pin_id: "home-pin",
      prepared_revision: 1,
      setup_request_id: "home-setup",
      setup_request_sha256: digest(setupBody),
    },
    member_setup: {
      pin_id: "member-pin",
      prepared_revision: 1,
      setup_request_id: "member-setup",
      outbound_link_generation: "opaque:peer-secret-name",
      expected_singleton_version: null,
      singleton_binding_version: 1,
    },
    expected: {
      principal_id: "principal:test",
      registry_version: 1,
      grant_id: "grant:test",
      grant_version: 1,
      source_binding: null,
    },
    rebind: false,
  };
  const request: F.WorkstreamsHomeBindRequestV2 = {
    contract: F.WORKSTREAMS_HOME_BIND_CONTRACT,
    request_body: F.canonicalWorkstreamsHomeBindBody(bodyValue),
  };
  const candidate: F.WorkstreamsHomeBindCommittedV2 = {
    contract: request.contract,
    kind: "committed",
    request_body: request.request_body,
    receipt: {
      principal_id: "principal:test",
      binding_version: 1,
      registry_version: 2,
      request_sha256: digest(request.request_body),
    },
    home_pin_bound_revision: 2,
  };
  const record = {
    request_body: request.request_body,
    home_setup_request_body: setupBody,
    result: candidate,
  };
  let ownerValid = true,
    exactBindAllowed = true,
    acceptedBody = request.request_body,
    checkAfterQualify: Effect.Effect<void, Pins.PeerSessionError> = Effect.void;
  let current = {
    environmentId: "home:test",
    authorityNamespace: "home-store",
    storeGeneration: 1,
  };
  const qualification = Layer.succeed(Setup.PeerPinSetupQualification, {
    qualify: (body: string, hash: string) =>
      Effect.gen(function* () {
        if (
          hash !== digest(body) ||
          (body !== setupBody && (!exactBindAllowed || body !== acceptedBody))
        )
          return yield* new Pins.PeerSessionError({ code: "owner_unqualified" });
        return {
          invocation,
          owner_qualification_digest: pin.owner_qualification_digest,
          checkCurrent: () => checkAfterQualify,
        };
      }),
    verifyBinding: () => Effect.die("No external binding verifier may run under H writer"),
  });
  const owner = Layer.succeed(Owner.SourceSetupOwner, {
    verifyUnchanged: Effect.suspend(() =>
      ownerValid
        ? Effect.void
        : Effect.fail(new Owner.SourceEnrollmentError({ code: "owner_unqualified" })),
    ),
  });
  const authority = Layer.succeed(Authority.NativeStoreAuthority, {
    readCurrent: Effect.suspend(() => Effect.succeed(current)),
    trustProvider: { read: () => Effect.die("unused") },
  } as unknown as Authority.NativeStoreAuthority["Service"]);
  const mounted = Layer.mergeAll(Home.layer, Qualification.layer).pipe(
    Layer.provideMerge(qualification),
    Layer.provideMerge(owner),
    Layer.provideMerge(authority),
  );
  const invoke = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    effect.pipe(
      Effect.provideService(Mcp.McpInvocationContext, invocation),
      Effect.provide(mounted),
    );
  const setupRequest: F.WorkstreamsHomeBindSetupReadbackRequestV2 = {
    contract: request.contract,
    expected_home: home,
    home_setup_request_id: pin.setup_request_id,
  };
  const readRequest: F.WorkstreamsHomeBindReadbackRequestV2 = {
    contract: request.contract,
    expected_home: home,
    bind_id: bodyValue.bind_id,
    request_sha256: digest(request.request_body),
  };
  return {
    auth,
    native,
    pins,
    records,
    registry,
    sql,
    authRows,
    db,
    keys,
    issued,
    invocation,
    nativeRequest,
    pin,
    setupBody,
    bodyValue,
    request,
    candidate,
    record,
    mounted,
    owner,
    authority,
    qualification,
    invoke,
    setupRequest,
    readRequest,
    revokeOwner: () => {
      ownerValid = false;
    },
    refuseBind: () => {
      exactBindAllowed = false;
    },
    qualifyBindBody: (body: string) => {
      acceptedBody = body;
    },
    setQualificationCheck: (effect: Effect.Effect<void, Pins.PeerSessionError>) => {
      checkAfterQualify = effect;
    },
    restore: () => {
      current = { ...current, storeGeneration: 2 };
    },
  };
});

it.effect(
  "binds once, returns actual rows, and replays the retained outcome after unrelated registry advance",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const setup = yield* f.invoke(
        Effect.gen(function* () {
          return yield* (yield* Home.HomeBind).setupReadback(f.setupRequest, f.invocation);
        }),
      );
      expect(setup.kind).toBe("prepared");
      expect(setup.setup_request_body).toBe(f.setupBody);
      const run = f.invoke(
        Effect.gen(function* () {
          return yield* (yield* Home.HomeBind).bind(f.request, f.invocation);
        }),
      );
      const result = yield* run;
      expect(result).toEqual(f.candidate);
      expect((yield* f.registry.context(home))?.registry_version).toBe(
        result.receipt.registry_version,
      );
      expect(yield* f.registry.listPlain(home, "source_bindings")).toHaveLength(1);
      expect((yield* f.pins.read({ pin_id: f.pin.pin_id }))?.pin.revision).toBe(
        result.home_pin_bound_revision,
      );
      yield* f.registry.advance(home, { server_generation: 1, registry_version: 2 });
      expect(yield* run).toEqual(result);
      expect((yield* f.registry.context(home))?.registry_version).toBe(3);
      expect(yield* f.registry.listPlain(home, "source_bindings")).toHaveLength(1);
      const back = yield* f.invoke(
        Effect.gen(function* () {
          return yield* (yield* Home.HomeBind).readback(f.readRequest, f.invocation);
        }),
      );
      expect(back.kind).toBe("committed");
      expect(
        (yield* Effect.exit(
          f.invoke(
            Effect.gen(function* () {
              return yield* (yield* Home.HomeBind).setupReadback(f.setupRequest, f.invocation);
            }),
          ),
        ))._tag,
      ).toBe("Failure");
      const changed = {
        ...f.request,
        request_body: F.canonicalWorkstreamsHomeBindBody({ ...f.bodyValue, rebind: true }),
      };
      expect(
        (yield* Effect.exit(
          f.invoke(
            Effect.gen(function* () {
              return yield* (yield* Home.HomeBind).bind(changed, f.invocation);
            }),
          ),
        ))._tag,
      ).toBe("Failure");
    }).pipe(Effect.provide(services)),
);

it.effect("empty readback remains unknown and digest mismatch reports the retained conflict", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const read = (request: F.WorkstreamsHomeBindReadbackRequestV2) =>
      f.invoke(
        Effect.gen(function* () {
          return yield* (yield* Home.HomeBind).readback(request, f.invocation);
        }),
      );
    expect(yield* read(f.readRequest)).toMatchObject({
      kind: "unknown",
      reason: "coverage_incomplete",
    });
    yield* f.invoke(
      Effect.gen(function* () {
        return yield* (yield* Home.HomeBind).bind(f.request, f.invocation);
      }),
    );
    expect(yield* read({ ...f.readRequest, request_sha256: "f".repeat(64) })).toMatchObject({
      kind: "conflict",
      retained_request_sha256: digest(f.request.request_body),
    });
  }).pipe(Effect.provide(services)),
);

it.effect("requires exact owner bind qualification independently of setup read permission", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    f.refuseBind();
    expect(
      (yield* f.invoke(
        Effect.gen(function* () {
          return yield* (yield* Home.HomeBind).setupReadback(f.setupRequest, f.invocation);
        }),
      )).kind,
    ).toBe("prepared");
    const exit = yield* Effect.exit(
      f.invoke(
        Effect.gen(function* () {
          return yield* (yield* Home.HomeBind).bind(f.request, f.invocation);
        }),
      ),
    );
    expect(exit._tag).toBe("Failure");
    expect(yield* f.registry.listPlain(home, "source_bindings")).toHaveLength(0);
    expect((yield* f.registry.context(home))?.registry_version).toBe(1);
  }).pipe(Effect.provide(services)),
);

it.effect("rolls back every H write stage without consuming the preparation", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const run = f.invoke(
      Effect.gen(function* () {
        return yield* (yield* Home.HomeBind).bind(f.request, f.invocation);
      }),
    );
    for (const [table, operation] of [
      ["source_bindings", "INSERT"],
      ["registries", "UPDATE"],
      ["peer_session_pins", "UPDATE"],
      ["home_bind_receipts", "INSERT"],
    ] as const) {
      yield* f.sql.unsafe(
        `CREATE TRIGGER jones_home_test_failure BEFORE ${operation} ON jones_workstreams_${table} BEGIN SELECT RAISE(ABORT, 'synthetic stage failure'); END`,
      );
      expect((yield* Effect.exit(run))._tag).toBe("Failure");
      expect((yield* f.registry.context(home))?.registry_version).toBe(1);
      expect(yield* f.registry.listPlain(home, "source_bindings")).toHaveLength(0);
      expect((yield* f.pins.read({ pin_id: f.pin.pin_id }))?.pin.state).toBe("prepared");
      expect(yield* f.records.read(home, f.bodyValue.bind_id)).toBeNull();
      yield* f.sql`DROP TRIGGER jones_home_test_failure`;
    }
    expect((yield* run).kind).toBe("committed");
  }).pipe(Effect.provide(services)),
);

it.effect("rechecks revoked grants and pins after external qualification and before writes", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    f.setQualificationCheck(
      f.registry
        .appendPlain(Test.scope, "grants", {
          principal_id: "principal:test",
          grant_version: 2,
          grant_id: "grant:revoked",
          state: "revoked",
          permissions: "[]",
        })
        .pipe(Effect.mapError(Pins.mapFailure)),
    );
    expect(
      (yield* Effect.exit(
        f.invoke(
          Effect.gen(function* () {
            return yield* (yield* Home.HomeBind).bind(f.request, f.invocation);
          }),
        ),
      ))._tag,
    ).toBe("Failure");
    expect(yield* f.registry.listPlain(home, "source_bindings")).toHaveLength(0);
  }).pipe(Effect.provide(services)),
);

it.effect(
  "acquires keys and calls owner qualification before writes, never the external binding verifier",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      let keyReads = 0,
        qualifications = 0;
      const checkedKeys = Layer.succeed(Keys.WorkstreamsKeyProvider, {
        ...f.keys,
        get: (id: string) =>
          Effect.gen(function* () {
            expect((yield* f.registry.context(home).pipe(Effect.orDie))?.registry_version).toBe(1);
            keyReads++;
            return yield* f.keys.get(id);
          }),
      });
      const checkedQualification = Layer.succeed(Setup.PeerPinSetupQualification, {
        qualify: (body: string, hash: string) =>
          Effect.gen(function* () {
            expect((yield* f.registry.context(home).pipe(Effect.orDie))?.registry_version).toBe(1);
            qualifications++;
            if ((body !== f.setupBody && body !== f.request.request_body) || hash !== digest(body))
              return yield* new Pins.PeerSessionError({ code: "owner_unqualified" });
            return {
              invocation: f.invocation,
              owner_qualification_digest: f.pin.owner_qualification_digest,
              checkCurrent: () => Effect.void,
            };
          }),
        verifyBinding: () => Effect.die("external verification inside writer"),
      });
      const layer = Home.layer.pipe(
        Layer.provide(checkedKeys),
        Layer.provide(checkedQualification),
        Layer.provide(f.owner),
        Layer.provide(f.authority),
      );
      const result = yield* Effect.gen(function* () {
        return yield* (yield* Home.HomeBind).bind(f.request, f.invocation);
      }).pipe(Effect.provide(layer));
      expect(result.kind).toBe("committed");
      expect(keyReads).toBe(2);
      expect(qualifications).toBe(2);
    }).pipe(Effect.provide(services)),
);

it.effect(
  "locally rejects wrong grant/principal/registry preimages even with synthetic owner permission",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const changes: F.WorkstreamsHomeBindBodyV2[] = [
        { ...f.bodyValue, expected: { ...f.bodyValue.expected, registry_version: 9 } },
        { ...f.bodyValue, expected: { ...f.bodyValue.expected, grant_id: "wrong-grant" } },
        { ...f.bodyValue, expected: { ...f.bodyValue.expected, grant_version: 2 } },
        {
          ...f.bodyValue,
          expected: {
            ...f.bodyValue.expected,
            source_binding: {
              binding_version: 1,
              authority_namespace: "source-store",
              store_generation: 1,
              state: "active",
            },
          },
          rebind: true,
        },
        {
          ...f.bodyValue,
          expected: { ...f.bodyValue.expected, principal_id: "wrong-principal" },
          source: {
            ...f.bodyValue.source,
            enrollment: {
              ...f.bodyValue.source.enrollment,
              context: {
                ...f.bodyValue.source.enrollment.context,
                principal_id: "wrong-principal",
              },
            },
          },
        },
      ];
      for (const body of changes) {
        const request = { ...f.request, request_body: F.canonicalWorkstreamsHomeBindBody(body) };
        f.qualifyBindBody(request.request_body);
        expect(
          (yield* Effect.exit(
            f.invoke(
              Effect.gen(function* () {
                return yield* (yield* Home.HomeBind).bind(request, f.invocation);
              }),
            ),
          ))._tag,
        ).toBe("Failure");
        expect(yield* f.registry.listPlain(home, "source_bindings")).toHaveLength(0);
        expect((yield* f.registry.context(home))?.registry_version).toBe(1);
      }
    }).pipe(Effect.provide(services)),
);

it.effect("historical receipt does not bypass current grant or source-binding revocation", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* f.invoke(
      Effect.gen(function* () {
        return yield* (yield* Home.HomeBind).bind(f.request, f.invocation);
      }),
    );
    yield* f.registry.appendPlain(Test.scope, "source_bindings", {
      provider: "t3",
      source_instance_id: "source:test",
      binding_version: 2,
      authority_namespace: "source-store",
      store_generation: 1,
      state: "revoked",
    });
    expect(
      (yield* Effect.exit(
        f.invoke(
          Effect.gen(function* () {
            return yield* (yield* Home.HomeBind).readback(f.readRequest, f.invocation);
          }),
        ),
      ))._tag,
    ).toBe("Failure");
    expect((yield* f.records.read(home, f.bodyValue.bind_id))?.result).toEqual(f.candidate);
  }).pipe(Effect.provide(services)),
);

it.effect(
  "reconstructs native authentication after lost reply and replays one H commit from a file-backed database",
  () =>
    Effect.gen(function* () {
      const parent = process.env.NW_HOME_BIND_EVIDENCE_DIR;
      const dir = yield* Effect.acquireRelease(
        Effect.sync(() =>
          NodeFS.mkdtempSync(NodePath.join(parent ?? NodeOS.tmpdir(), "home-bind-runtime-")),
        ),
        (directory) =>
          Effect.sync(() => {
            if (parent === undefined) NodeFS.rmSync(directory, { recursive: true, force: true });
          }),
      );
      const filename = NodePath.join(dir, "synthetic.sqlite");
      const f = yield* Effect.gen(function* () {
        const f = yield* fixture;
        yield* f.invoke(
          Effect.gen(function* () {
            return yield* (yield* Home.HomeBind).bind(f.request, f.invocation);
          }),
        );
        return f;
      }).pipe(Effect.provide(Layer.fresh(serviceLayer(filename))));
      yield* Effect.gen(function* () {
        yield* TestClock.setTime(Date.parse(Test.now));
        const native = yield* Pins.NativePeerSessions;
        expect((yield* Effect.exit(native.current(f.invocation)))._tag).toBe("Failure");
        yield* native.authenticate(f.nativeRequest, f.invocation);
        expect(
          yield* f.invoke(
            Effect.gen(function* () {
              return yield* (yield* Home.HomeBind).bind(f.request, f.invocation);
            }),
          ),
        ).toEqual(f.candidate);
        const registry = yield* Registries.RegistryStore;
        expect((yield* registry.context(home))?.registry_version).toBe(2);
        expect(yield* registry.listPlain(home, "source_bindings")).toHaveLength(1);
      }).pipe(Effect.provide(Layer.fresh(serviceLayer(filename))));
    }).pipe(Effect.scoped),
);

it.effect("key retirement after preload aborts the writer and leaves all H rows uncommitted", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    let checks = 0;
    f.setQualificationCheck(
      Effect.gen(function* () {
        checks++;
        if (checks === 2)
          yield* f.sql`UPDATE jones_workstreams_keys SET state='retired' WHERE key_id='test-home'`.pipe(
            Effect.mapError(Pins.mapFailure),
          );
      }),
    );
    expect(
      (yield* Effect.exit(
        f.invoke(
          Effect.gen(function* () {
            return yield* (yield* Home.HomeBind).bind(f.request, f.invocation);
          }),
        ),
      ))._tag,
    ).toBe("Failure");
    expect((yield* f.registry.context(home))?.registry_version).toBe(1);
    expect(yield* f.registry.listPlain(home, "source_bindings")).toHaveLength(0);
    expect((yield* f.pins.read({ pin_id: f.pin.pin_id }))?.pin.state).toBe("prepared");
  }).pipe(Effect.provide(services)),
);

it.effect("refuses a different home/owner before loading any request-selected encryption key", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    let keyReads = 0;
    const checked = Layer.succeed(Keys.WorkstreamsKeyProvider, {
      ...f.keys,
      get: (id: string) => {
        keyReads++;
        return f.keys.get(id);
      },
    });
    const layer = Home.layer.pipe(
      Layer.provide(checked),
      Layer.provide(f.qualification),
      Layer.provide(f.owner),
      Layer.provide(f.authority),
    );
    for (const target of [
      { ...home, owner_id: "wrong-owner" },
      { ...home, home_environment_id: "wrong-home" },
    ]) {
      const request = { ...f.setupRequest, expected_home: target };
      expect(
        (yield* Effect.exit(
          Effect.gen(function* () {
            return yield* (yield* Home.HomeBind).setupReadback(request, f.invocation);
          }).pipe(Effect.provide(layer)),
        ))._tag,
      ).toBe("Failure");
    }
    expect(keyReads).toBe(0);
  }).pipe(Effect.provide(services)),
);

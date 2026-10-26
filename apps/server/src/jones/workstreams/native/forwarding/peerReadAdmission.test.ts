import { expect, it } from "@effect/vitest";
import { EnvironmentId } from "@t3tools/contracts";
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
import migration from "../../../persistence/Migrations/128_JonesWorkstreamsPeerSessionPins.ts";
import * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import * as Records from "../store/peerSessionPinStore.ts";
import * as Test from "../store/sql.testkit.ts";
import * as Registries from "../store/registryStore.ts";
import * as Pins from "../runtime/peerSessionPins.ts";
import * as Setup from "../runtime/peerSessionPinSetup.ts";
import * as Owner from "../runtime/sourceEnrollment.ts";
import { NativeHome } from "../mode/authorityMode.ts";
import * as Guard from "./guard.ts";
import * as Boundary from "./peerReadAdmission.ts";
import * as Authentication from "../runtime/peerMcpAuthentication.ts";
import * as Session from "../registry/session.ts";
import * as Bindings from "../store/bindingStore.ts";
import * as Registry from "../registry/service.ts";
import * as Projection from "../read/projection.ts";
import * as Routes from "../read/routes.ts";
import * as Local from "../registry/localTransport.ts";
import * as Protocol from "./homeReadProtocol.ts";
import * as DateTime from "effect/DateTime";
import { WORKSTREAMS_HOME_READ_RESULT_CONTRACT as contract } from "@t3tools/contracts/jones/workstreamsHomeReadResult";

const home = { ...Test.scope, home_environment_id: "home:test", server_generation: 1 };
const receiver = {
  environmentId: "home:test",
  authorityNamespace: "receiver-store",
  storeGeneration: 1,
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
const services = Layer.mergeAll(Records.layer, Authentication.layer).pipe(
  Layer.provideMerge(Pins.nativeLayer),
  Layer.provideMerge(Auth.layer.pipe(Layer.provide(signing))),
  Layer.provideMerge(Test.testLayer()),
  Layer.provideMerge(Config.layerTest(process.cwd(), { prefix: "nw-j3a-synthetic-" })),
  Layer.provideMerge(
    Layer.succeed(Environment.ServerEnvironmentIdentity, {
      getEnvironmentId: Effect.succeed(EnvironmentId.make("home:test")),
    }),
  ),
  Layer.provideMerge(NodeServices.layer),
);
const fixture = Effect.gen(function* () {
  yield* TestClock.setTime(Date.parse(Test.now));
  yield* runMigrations({ toMigrationInclusive: 54 });
  yield* Test.seed;
  yield* migration;
  const auth = yield* Auth.EnvironmentAuth,
    native = yield* Pins.NativePeerSessions,
    records = yield* Records.PeerSessionPinStore,
    sql = yield* SqlClient.SqlClient,
    rows = yield* AuthRows.AuthSessionRepository;
  const issued = yield* auth.issueMcpClientSession({
    label: "forged-label-is-not-authority",
    access: "full-access",
    peerLink: true,
    client: { deviceType: "bot" },
  });
  const scope: Mcp.McpInvocationScope = {
    environmentId: EnvironmentId.make("home:test"),
    issuedAt: Date.parse(Test.now),
    requestNamespace: "synthetic-request",
    capabilities: new Set(["orchestration"]),
    thread: undefined,
    client: {
      sessionId: issued.sessionId,
      label: "different-label",
      access: "full-access",
      linked: true,
    },
  };
  const request = {
    cookies: {},
    headers: { authorization: `Bearer ${issued.token}` },
  } as unknown as Parameters<Auth.EnvironmentAuth["Service"]["authenticateMcpClient"]>[0];
  yield* (yield* Authentication.PeerMcpAuthentication).register(request, scope);
  const facts = yield* native.current(scope);
  const assertion = {
    schema: "jones-workstreams-peer-session-pin/1" as const,
    pin_id: "pin:test",
    revision: 1,
    receiver_authority: {
      environment_id: receiver.environmentId,
      authority_namespace: receiver.authorityNamespace,
      store_generation: 1,
    },
    session: facts.session,
    peer_environment_id: "source:test",
    roles: ["member_home"] as const,
    expected_home: home,
    source_authority: {
      environment_id: "source:test",
      authority_namespace: "source-store",
      store_generation: 1,
    },
    source_enrollment_id: "enrollment:test",
    source_binding_version: null,
    singleton_binding_version: null,
    state: "prepared" as const,
    expires_at: "2026-01-01T00:20:00.000Z",
    setup_request_id: "setup:test",
    owner_qualification_digest: "b".repeat(64),
    bind_receipt: null,
  };
  const body = JSON.stringify({
    schema: "jones-workstreams-peer-pin-setup/1",
    assertion,
    expected: { registry_version: 1, grant_id: "grant:test", grant_version: 1 },
  });
  const decoded = Records.decodeSetupRequest(body);
  const registry = yield* Registries.RegistryStore;
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
  const receipt = {
    principal_id: "principal:test",
    binding_version: 1,
    registry_version: 2,
    request_sha256: "c".repeat(64),
  };
  let current = { ...receiver };
  const authority = Layer.succeed(Authority.NativeStoreAuthority, {
    readCurrent: Effect.suspend(() => Effect.succeed(current)),
    trustProvider: { read: () => Effect.die("unused") },
  } as unknown as Authority.NativeStoreAuthority["Service"]);
  const qualifier = Layer.succeed(Setup.PeerPinSetupQualification, {
    qualify: (exact: string, digest: string) =>
      exact === body && digest === decoded.pin.setup_request_sha256
        ? Effect.succeed({
            invocation: scope,
            owner_qualification_digest: assertion.owner_qualification_digest,
            checkCurrent: () => Effect.void,
          })
        : Effect.fail(new Pins.PeerSessionError({ code: "owner_unqualified" })),
    verifyBinding: (_p: unknown, r: unknown, v: number) =>
      JSON.stringify(r) === JSON.stringify(receipt) && v === 1
        ? Effect.void
        : Effect.fail(new Pins.PeerSessionError({ code: "conflict" })),
  });
  const nativeHome = {
    ...home,
    canonical_origin: "https://native.workstreams.invalid",
    served_by_environment_id: "home:test",
  };
  const foundation = Layer.mergeAll(
    Setup.layer,
    Boundary.layer.pipe(Layer.provideMerge(Guard.layer.pipe(Layer.provideMerge(Pins.layer)))),
  ).pipe(
    Layer.provideMerge(authority),
    Layer.provideMerge(qualifier),
    Layer.provideMerge(Layer.succeed(Owner.SourceSetupOwner, { verifyUnchanged: Effect.void })),
    Layer.provideMerge(Layer.succeed(NativeHome, nativeHome)),
  );
  const session = Boundary.sessionLayer.pipe(
    Layer.provideMerge(Session.layer),
    Layer.provideMerge(foundation),
  );
  const domain = Projection.layer.pipe(
    Layer.provideMerge(Registry.layer),
    Layer.provideMerge(session),
  );
  const mounted = Protocol.layer.pipe(
    Layer.provideMerge(Boundary.homeAdmissionLayer),
    Layer.provideMerge(
      Routes.layer.pipe(
        Layer.provideMerge(domain),
        // Counts/projection never use this external legacy adapter.
        Layer.provide(
          Layer.succeed(Local.LocalTransport, {
            forCaller: () => Effect.die("unexpected legacy transport"),
          }),
        ),
      ),
    ),
  );
  const bind = Effect.gen(function* () {
    const setup = yield* Setup.PeerSessionPinSetup;
    yield* registry.appendPlain(Test.scope, "source_bindings", {
      provider: "t3",
      source_instance_id: "source:test",
      binding_version: 1,
      authority_namespace: "source-store",
      store_generation: 1,
      state: "active",
    });
    return yield* setup.bind(assertion.pin_id, 1, receipt, 1);
  });
  return {
    auth,
    native,
    records,
    rows,
    sql,
    scope,
    request,
    body,
    pin: decoded.pin,
    receipt,
    registry,
    mounted,
    nativeHome,
    bind,
    restore: () => {
      current = { ...current, storeGeneration: 2 };
    },
  };
});

// Owner qualification and bind receipt verification above are synthetic ports.
// Native auth/session rows, encrypted pins, Guard, Session wrapper, registry,
// projection, HomeReads, protocol and SQLite below are real in-process services.
const run = <E>(
  test: (
    f: Effect.Success<typeof fixture>,
  ) => Effect.Effect<
    void,
    E,
    | Setup.PeerSessionPinSetup
    | Boundary.PeerReadBoundary
    | Session.Session
    | Registry.Registry
    | Protocol.HomeReadProtocol
    | Routes.HomeReads
    | Bindings.BindingStore
    | Authentication.PeerMcpAuthentication
    | Mcp.McpInvocationContext
  >,
) =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* Effect.gen(function* () {
      yield* (yield* Setup.PeerSessionPinSetup).apply(f.body, 0);
      yield* f.bind;
      yield* test(f);
    }).pipe(Effect.provideService(Mcp.McpInvocationContext, f.scope), Effect.provide(f.mounted));
  }).pipe(Effect.provide(services));

it.effect("remote B reads encrypted A data through the qualified Session without mutation", () =>
  run((f) =>
    Effect.gen(function* () {
      const boundary = yield* Boundary.PeerReadBoundary;
      const session = yield* Session.Session;
      const registry = yield* Registry.Registry;
      const protocol = yield* Protocol.HomeReadProtocol;
      const workstream = {
        workstream_id: "workstream:test",
        owner_id: home.owner_id,
        name: "Encrypted native private name",
        lifecycle: "active",
        progress: { state: "unknown" },
        delivery: "unknown",
        freshness: "current",
        sort_order: 0,
        version: 1,
        created_at: Test.now,
        updated_at: Test.now,
        created_by: { principal_id: "principal:test" },
        updated_by: { principal_id: "principal:test" },
      };
      yield* f.registry.put(
        Test.scope,
        "records",
        {
          metadata: { workstream_id: workstream.workstream_id, version: 1, sort_order: 0 },
          value: workstream,
        },
        null,
      );
      const before = JSON.stringify(yield* f.sql.unsafe("SELECT * FROM jones_workstreams_records"));
      expect(before).not.toContain(workstream.name);
      const admitted = yield* boundary.current();
      expect(Object.isFrozen(admitted)).toBe(true);
      expect(admitted.caller.source_environment_id).toBe("source:test");
      const authorization = yield* session.authorize(admitted.caller, "workstreams:read");
      expect(authorization.authorization.grant.permissions).toEqual([
        "workstreams:read",
        "workstreams:write",
      ]);
      expect(authorization.authorization.actor.principal_id).toBe("principal:test");
      const output = yield* protocol.read({
        contract,
        request: { route: "read-projection", params: {} },
      });
      expect(output.route).toBe("read-projection");
      expect(JSON.stringify(output.value)).toContain(workstream.name);
      const again = yield* boundary.current();
      expect(again.caller).not.toBe(admitted.caller);
      yield* admitted.checkCurrent();
      expect(
        (yield* Effect.exit(session.authorize(admitted.caller, "workstreams:write")))._tag,
      ).toBe("Failure");
      const write = JSON.stringify({
        command_id: "peer-write-denied-00000000000000000001",
        expected_server_generation: authorization.context.server_generation,
        expected_registry_version: authorization.context.registry_version,
        action: {
          operation: "create_workstream",
          name: "Must never be written",
          lifecycle: "active",
          progress: { state: "unknown" },
          sort_order: 0,
        },
      });
      expect(yield* registry.submit(admitted.caller, write).pipe(Effect.flip)).toMatchObject({
        code: "forbidden",
      });
      expect(JSON.stringify(yield* f.sql.unsafe("SELECT * FROM jones_workstreams_records"))).toBe(
        before,
      );
      expect(yield* f.registry.listPlain(Test.scope, "grants")).toHaveLength(1);
    }),
  ),
);

it.effect.each(["clone-caller", "unregistered-caller", "scope", "session", "run"] as const)(
  "rejects %s substitution without minting a replacement proof",
  (fault) =>
    run((f) =>
      Effect.gen(function* () {
        const boundary = yield* Boundary.PeerReadBoundary;
        const session = yield* Session.Session;
        const admitted = yield* boundary.current();
        if (fault === "clone-caller" || fault === "unregistered-caller") {
          const caller = {
            ...admitted.caller,
            ...(fault === "unregistered-caller" ? { origin: { kind: "app" as const } } : {}),
          };
          expect((yield* Effect.exit(session.authorize(caller, "workstreams:read")))._tag).toBe(
            "Failure",
          );
        } else {
          const scope = {
            ...f.scope,
            ...(fault === "session"
              ? { client: { ...f.scope.client!, sessionId: "replacement" } }
              : {}),
            ...(fault === "run" ? { requestNamespace: "different-run" } : {}),
          };
          expect(
            (yield* Effect.exit(
              session
                .authorize(admitted.caller, "workstreams:read")
                .pipe(Effect.provideService(Mcp.McpInvocationContext, scope)),
            ))._tag,
          ).toBe("Failure");
          expect(
            (yield* Effect.exit(
              admitted.checkCurrent().pipe(Effect.provideService(Mcp.McpInvocationContext, scope)),
            ))._tag,
          ).toBe("Failure");
        }
        yield* admitted.checkCurrent();
      }),
    ),
);

it.effect.each([
  "pin-revoked",
  "pin-expired",
  "session-revoked",
  "session-expired",
  "receiver-generation",
  "source-generation",
  "source-binding",
  "home-generation",
  "home-owner",
  "home-registry",
  "home-environment",
  "canonical-origin",
  "context-fenced",
  "grant-revoked",
  "grant-changed",
  "scope-mutated",
  "run-mutated",
  "thread-mutated",
  "attribution-mutated",
  "native-registration-replaced",
] as const)("fences captured proof after %s", (fault) =>
  run((f) =>
    Effect.gen(function* () {
      const admitted = yield* (yield* Boundary.PeerReadBoundary).current();
      const setup = yield* Setup.PeerSessionPinSetup;
      switch (fault) {
        case "context-fenced":
          yield* f.sql.unsafe("UPDATE jones_workstreams_registries SET state = 'fenced'");
          break;
        case "pin-revoked":
          yield* setup.revoke(f.pin.pin_id, 2);
          break;
        case "pin-expired":
          yield* TestClock.adjust("21 minutes");
          yield* f.native.current(f.scope);
          break;
        case "session-expired":
          yield* TestClock.adjust("31 days");
          expect((yield* Effect.exit(f.native.current(f.scope)))._tag).toBe("Failure");
          break;
        case "session-revoked":
          yield* f.rows.revoke({
            sessionId: f.pin.session.session_id as Parameters<typeof f.rows.revoke>[0]["sessionId"],
            revokedAt: DateTime.makeUnsafe(Test.now),
          });
          break;
        case "receiver-generation":
          f.restore();
          break;
        case "source-generation":
        case "source-binding":
          yield* f.registry.appendPlain(Test.scope, "source_bindings", {
            provider: "t3",
            source_instance_id: "source:test",
            binding_version: 2,
            authority_namespace: "source-store",
            store_generation: fault === "source-generation" ? 2 : 1,
            state: "active",
          });
          break;
        case "grant-revoked":
        case "grant-changed":
          yield* f.registry.appendPlain(Test.scope, "grants", {
            grant_id: "grant:test",
            principal_id: "principal:test",
            grant_version: 2,
            permissions: JSON.stringify(["workstreams:read"]),
            state: fault === "grant-revoked" ? "revoked" : "active",
          });
          break;
        case "scope-mutated":
          (f.scope.capabilities as Set<Mcp.McpCapability>).delete("orchestration");
          break;
        case "run-mutated":
          Object.assign(f.scope, { requestNamespace: "replaced-run" });
          break;
        case "thread-mutated":
          Object.assign(f.scope, {
            thread: {
              threadId: "thread:replacement",
              providerSessionId: "replaced-session",
              providerInstanceId: "codex",
            },
          });
          break;
        case "attribution-mutated":
          Object.assign(f.scope.client!, {
            remoteOrigin: {
              schemaVersion: "t3.linked-message-origin/v1",
              environmentId: "source:changed",
              linkSessionId: f.scope.client!.sessionId,
              thread: { threadId: "thread:changed" },
            },
          });
          break;
        case "native-registration-replaced":
          yield* f.native.authenticate(f.request, f.scope);
          break;
        default: {
          const column = {
            "home-generation": "server_generation",
            "home-owner": "owner_id",
            "home-registry": "registry_instance_id",
            "home-environment": "home_environment_id",
            "canonical-origin": "canonical_origin",
          }[fault];
          const value = fault === "home-generation" ? 2 : "changed";
          // Registry identity columns are SQL-immutable. Simulate changed configured home
          // against the intact real store, rather than disabling its protections.
          Object.assign(f.nativeHome, { [column]: value });
        }
      }
      expect((yield* Effect.exit(admitted.checkCurrent()))._tag).toBe("Failure");
      expect(
        (yield* Effect.exit(
          (yield* Session.Session).authorize(admitted.caller, "workstreams:read"),
        ))._tag,
      ).toBe("Failure");
    }),
  ),
);

it.effect.each(["missing", "prepared", "canonical-origin"] as const)(
  "current never admits %s setup",
  (fault) =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* Effect.gen(function* () {
        if (fault !== "missing") yield* (yield* Setup.PeerSessionPinSetup).apply(f.body, 0);
        if (fault === "canonical-origin") {
          yield* f.bind;
          Object.assign(f.nativeHome, { canonical_origin: "https://drift.invalid" });
        }
        expect((yield* Effect.exit((yield* Boundary.PeerReadBoundary).current()))._tag).toBe(
          "Failure",
        );
      }).pipe(Effect.provideService(Mcp.McpInvocationContext, f.scope), Effect.provide(f.mounted));
    }).pipe(Effect.provide(services)),
);

it.effect(
  "ordinary local Session behavior remains exact and unqualified remote callers stay refused",
  () =>
    run((f) =>
      Effect.gen(function* () {
        const session = yield* Session.Session;
        const remote = (yield* (yield* Boundary.PeerReadBoundary).current()).caller;
        const unlinked = { ...f.scope, client: { ...f.scope.client!, linked: false } };
        expect(
          (yield* Effect.exit(
            session
              .authorize(remote, "workstreams:read")
              .pipe(Effect.provideService(Mcp.McpInvocationContext, unlinked)),
          ))._tag,
        ).toBe("Failure");
        yield* f.registry.appendPlain(Test.scope, "principals", {
          principal_id: "principal:local",
          environment_id: "home:test",
          kind: "environment",
          provenance: "{}",
        });
        yield* f.registry.appendPlain(Test.scope, "grants", {
          grant_id: "grant:local",
          principal_id: "principal:local",
          grant_version: 1,
          permissions: JSON.stringify(["workstreams:read", "workstreams:write"]),
          state: "active",
        });
        yield* (yield* Bindings.BindingStore).bind(
          { ...Test.binding, principal_id: "principal:local" },
          null,
        );
        const local = {
          principal_id: "principal:local",
          source_environment_id: "home:test",
          binding_version: 1,
          origin: { kind: "app" as const },
        };
        for (const permission of ["workstreams:read", "workstreams:write"] as const) {
          const result = yield* session
            .authorize(local, permission)
            .pipe(Effect.provideService(Mcp.McpInvocationContext, unlinked));
          expect(result.authorization.actor.principal_id).toBe("principal:local");
        }
        expect((yield* Effect.exit(session.authorize(local, "workstreams:read")))._tag).toBe(
          "Failure",
        );
      }),
    ),
);

it.effect(
  "qualification refusal lets general preview continue while Workstreams proof is unavailable",
  () =>
    run((f) =>
      Effect.gen(function* () {
        const boundary = yield* Boundary.PeerReadBoundary;
        const admitted = yield* boundary.current();
        (f.scope.capabilities as Set<Mcp.McpCapability>).delete("orchestration");
        (f.scope.capabilities as Set<Mcp.McpCapability>).add("preview");
        yield* (yield* Authentication.PeerMcpAuthentication).register(f.request, f.scope);
        yield* Mcp.requireMcpCapability("preview");
        expect((yield* Effect.exit(boundary.current()))._tag).toBe("Failure");
        expect((yield* Effect.exit(admitted.checkCurrent()))._tag).toBe("Failure");
        (f.scope.capabilities as Set<Mcp.McpCapability>).clear();
        (f.scope.capabilities as Set<Mcp.McpCapability>).add("orchestration");
        expect((yield* Effect.exit(boundary.current()))._tag).toBe("Failure");
      }),
    ),
);

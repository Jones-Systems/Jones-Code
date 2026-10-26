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
import * as Guard from "../forwarding/guard.ts";

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
const services = Layer.mergeAll(Records.layer, Pins.nativeLayer).pipe(
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
  yield* native.authenticate(request, scope);
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
  const mounted = Layer.mergeAll(
    Setup.layer,
    Guard.layer.pipe(Layer.provideMerge(Pins.layer)),
  ).pipe(
    Layer.provideMerge(authority),
    Layer.provideMerge(qualifier),
    Layer.provideMerge(Layer.succeed(Owner.SourceSetupOwner, { verifyUnchanged: Effect.void })),
    Layer.provideMerge(
      Layer.succeed(NativeHome, {
        ...home,
        canonical_origin: "https://native.workstreams.invalid",
        served_by_environment_id: "home:test",
      }),
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
    bind,
    restore: () => {
      current = { ...current, storeGeneration: 2 };
    },
  };
});

it.effect(
  "plans, applies, reconciles a lost reply, binds and revokes without changing original request bytes",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const before = yield* f.sql`SELECT * FROM auth_sessions`;
      yield* Effect.gen(function* () {
        const setup = yield* Setup.PeerSessionPinSetup;
        expect((yield* setup.plan(f.body)).state).toBe("ready");
        expect((yield* setup.apply(f.body, 0)).state).toBe("prepared");
        expect((yield* setup.apply(f.body, 0)).revision).toBe(1);
        expect((yield* setup.plan(f.body)).state).toBe("applied");
        expect((yield* Effect.exit(setup.apply(f.body, 9)))._tag).toBe("Failure");
        const bound = yield* f.bind;
        expect(bound.state).toBe("bound");
        expect((yield* setup.apply(f.body, 0)).revision).toBe(2);
        expect((yield* setup.readback(f.pin.setup_request_id))?.request_body).toBe(f.body);
        expect((yield* Effect.exit(setup.bind(f.pin.pin_id, 1, f.receipt, 1)))._tag).toBe(
          "Failure",
        );
        expect((yield* setup.revoke(f.pin.pin_id, 2)).state).toBe("revoked");
        expect((yield* setup.readback(f.pin.setup_request_id))?.request_body).toBe(f.body);
      }).pipe(Effect.provide(f.mounted));
      expect(yield* f.sql`SELECT * FROM auth_sessions`).toEqual(before);
    }).pipe(Effect.provide(services)),
);
it.effect("missing qualified owner ports fail closed", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const missing = Setup.layer.pipe(
      Layer.provide(
        Layer.succeed(Authority.NativeStoreAuthority, {
          readCurrent: Effect.succeed(receiver),
        } as unknown as Authority.NativeStoreAuthority["Service"]),
      ),
    );
    yield* Effect.gen(function* () {
      expect((yield* Effect.exit((yield* Setup.PeerSessionPinSetup).apply(f.body, 0)))._tag).toBe(
        "Failure",
      );
    }).pipe(Effect.provide(missing));
    expect((yield* f.sql`SELECT * FROM jones_workstreams_peer_session_pins`).length).toBe(0);
  }).pipe(Effect.provide(services)),
);

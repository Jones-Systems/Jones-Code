import { expect, it } from "@effect/vitest";
import { EnvironmentId } from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as DateTime from "effect/DateTime";
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
import * as Bindings from "../store/bindingStore.ts";
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
  "requires genuine native linked authentication, object provenance and active native session",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      expect((yield* Effect.exit(f.native.current({ ...f.scope })))._tag).toBe("Failure");
      const ordinary = yield* f.auth.issueMcpClientSession({
        label: "peer source:test",
        access: "full-access",
        peerLink: false,
        client: { deviceType: "bot" },
      });
      const forged = {
        ...f.scope,
        client: { ...f.scope.client!, sessionId: ordinary.sessionId, linked: true },
      };
      const request = {
        cookies: {},
        headers: { authorization: `Bearer ${ordinary.token}` },
      } as unknown as typeof f.request;
      expect((yield* Effect.exit(f.native.authenticate(request, forged)))._tag).toBe("Failure");
      yield* f.records.insert({ pin: f.pin, request_body: f.body });
      yield* Effect.gen(function* () {
        const pins = yield* Pins.PeerSessionPins;
        expect(
          (yield* Effect.exit(pins.forInvocation(f.scope, { role: "member_home" })))._tag,
        ).toBe("Failure");
        const q = yield* pins.forInvocation(f.scope, {
          role: "member_home",
          setupOnlyPrepared: true,
        });
        expect(
          (yield* Effect.exit(
            pins.forInvocation(f.scope, { role: "home_source", setupOnlyPrepared: true }),
          ))._tag,
        ).toBe("Failure");
        f.restore();
        expect((yield* Effect.exit(q.checkCurrent()))._tag).toBe("Failure");
      }).pipe(Effect.provide(f.mounted));
      yield* f.rows.revoke({
        sessionId: ordinary.sessionId,
        revokedAt: DateTime.makeUnsafe(Test.now),
      });
      yield* f.rows.revoke({
        sessionId: f.pin.session.session_id as typeof ordinary.sessionId,
        revokedAt: DateTime.makeUnsafe(Test.now),
      });
      expect((yield* Effect.exit(f.native.current(f.scope)))._tag).toBe("Failure");
    }).pipe(Effect.provide(services)),
);
it.effect("fences expiry and request ceiling changes after native authentication", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* f.records.insert({ pin: f.pin, request_body: f.body });
    yield* Effect.gen(function* () {
      const q = yield* (yield* Pins.PeerSessionPins).forInvocation(f.scope, {
        role: "member_home",
        setupOnlyPrepared: true,
      });
      yield* TestClock.adjust("21 minutes");
      expect((yield* Effect.exit(q.checkCurrent()))._tag).toBe("Failure");
    }).pipe(Effect.provide(f.mounted));
    const scope = {
      ...f.scope,
      client: {
        ...f.scope.client!,
        narrowedTo: { runtimeMode: "approval-required" as const, interactionMode: "plan" as const },
      },
    };
    yield* f.native.authenticate(f.request, scope);
    expect(
      Pins.ceilingAllows((yield* f.native.current(scope)).session.ceiling, "workstreams:write"),
    ).toBe(false);
  }).pipe(Effect.provide(services)),
);

it.effect.each([
  [
    "receiver environment",
    (p: Records.PeerPinSetupRequest["assertion"]) => ({
      ...p,
      receiver_authority: { ...p.receiver_authority, environment_id: "foreign" },
    }),
  ],
  [
    "receiver namespace",
    (p: Records.PeerPinSetupRequest["assertion"]) => ({
      ...p,
      receiver_authority: { ...p.receiver_authority, authority_namespace: "foreign" },
    }),
  ],
  [
    "receiver generation",
    (p: Records.PeerPinSetupRequest["assertion"]) => ({
      ...p,
      receiver_authority: { ...p.receiver_authority, store_generation: 2 },
    }),
  ],
  [
    "peer/source association",
    (p: Records.PeerPinSetupRequest["assertion"]) => ({ ...p, peer_environment_id: "foreign" }),
  ],
  [
    "home environment",
    (p: Records.PeerPinSetupRequest["assertion"]) => ({
      ...p,
      expected_home: { ...p.expected_home, home_environment_id: "foreign" },
    }),
  ],
] as const)("rejects wrong %s even with a genuine native session", ([_label, change]) =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const request = JSON.parse(f.body) as Records.PeerPinSetupRequest;
    const body = JSON.stringify({ ...request, assertion: change(request.assertion) }),
      pin = Records.decodeSetupRequest(body).pin;
    yield* f.records.insert({ pin, request_body: body });
    yield* Effect.gen(function* () {
      expect(
        (yield* Effect.exit(
          (yield* Pins.PeerSessionPins).forInvocation(f.scope, {
            role: "member_home",
            setupOnlyPrepared: true,
          }),
        ))._tag,
      ).toBe("Failure");
    }).pipe(Effect.provide(f.mounted));
  }).pipe(Effect.provide(services)),
);
it.effect(
  "home-source role requires current local source tuple and pinned singleton home generation",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const request = JSON.parse(f.body) as Records.PeerPinSetupRequest;
      const assertion = {
        ...request.assertion,
        roles: ["home_source"] as const,
        source_authority: request.assertion.receiver_authority,
        peer_environment_id: "remote-home",
        expected_home: { ...home, home_environment_id: "remote-home" },
      };
      const body = JSON.stringify({ ...request, assertion }),
        pin = Records.decodeSetupRequest(body).pin;
      yield* f.records.insert({ pin, request_body: body });
      const bound = {
        ...pin,
        revision: 2,
        state: "bound" as const,
        source_binding_version: 1,
        singleton_binding_version: 1,
        bind_receipt: f.receipt,
      };
      yield* f.records.transition(bound, 1);
      const bindings = yield* Bindings.BindingStore;
      yield* bindings.bind(
        {
          ...Test.scope,
          home_environment_id: "remote-home",
          home_generation: 1,
          principal_id: "principal:test",
          binding_version: 1,
          state: "active",
        },
        null,
      );
      yield* Effect.gen(function* () {
        const pins = yield* Pins.PeerSessionPins,
          q = yield* pins.forInvocation(f.scope, { role: "home_source" });
        yield* q.checkCurrent();
        expect(
          (yield* Effect.exit(pins.forInvocation(f.scope, { role: "member_home" })))._tag,
        ).toBe("Failure");
        yield* bindings.bind(
          {
            ...Test.scope,
            home_environment_id: "remote-home",
            home_generation: 2,
            principal_id: "principal:test",
            binding_version: 2,
            state: "active",
          },
          1,
        );
        expect((yield* Effect.exit(q.checkCurrent()))._tag).toBe("Failure");
      }).pipe(Effect.provide(f.mounted));
    }).pipe(Effect.provide(services)),
);
it.effect("relinking a native session cannot inherit the old pin or widen a captured ceiling", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* f.records.insert({ pin: f.pin, request_body: f.body });
    const replacement = yield* f.auth.issueMcpClientSession({
      label: "same peer label",
      access: "full-access",
      peerLink: true,
      client: { deviceType: "bot" },
    });
    const scope = { ...f.scope, client: { ...f.scope.client!, sessionId: replacement.sessionId } };
    yield* f.native.authenticate(
      {
        cookies: {},
        headers: { authorization: `Bearer ${replacement.token}` },
      } as unknown as typeof f.request,
      scope,
    );
    yield* Effect.gen(function* () {
      expect(
        (yield* Effect.exit(
          (yield* Pins.PeerSessionPins).forInvocation(scope, {
            role: "member_home",
            setupOnlyPrepared: true,
          }),
        ))._tag,
      ).toBe("Failure");
    }).pipe(Effect.provide(f.mounted));
    const narrowed = {
      ...f.scope,
      client: {
        ...f.scope.client!,
        narrowedTo: { runtimeMode: "approval-required" as const, interactionMode: "plan" as const },
      },
    };
    yield* f.native.authenticate(f.request, narrowed);
    expect((yield* f.native.current(narrowed)).session.ceiling.permissions).toEqual([
      "workstreams:read",
    ]);
  }).pipe(Effect.provide(services)),
);

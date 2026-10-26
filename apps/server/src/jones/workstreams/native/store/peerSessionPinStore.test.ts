// @effect-diagnostics nodeBuiltinImport:off -- File-backed synthetic encryption/restart evidence is retained for root cleanup.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
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
  "encrypts exact original setup bytes, preserves them through CAS and refuses reassignment",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.records.insert({ pin: f.pin, request_body: f.body });
      const stored = yield* f.sql<{
        envelope: string;
      }>`SELECT envelope FROM jones_workstreams_peer_session_pins`;
      expect(stored[0]!.envelope).not.toContain("source-store");
      expect(stored[0]!.envelope).not.toContain(f.pin.session.subject);
      expect((yield* f.records.read({ session_id: f.pin.session.session_id }))?.request_body).toBe(
        f.body,
      );
      const next = {
        ...f.pin,
        revision: 2,
        state: "bound" as const,
        source_binding_version: 1,
        singleton_binding_version: 1,
        bind_receipt: f.receipt,
      };
      yield* f.records.transition(next, 1);
      expect((yield* f.records.read({ pin_id: f.pin.pin_id }))?.request_body).toBe(f.body);
      expect((yield* Effect.exit(f.records.transition(next, 1)))._tag).toBe("Failure");
      expect(
        (yield* Effect.exit(
          f.records.insert({ pin: { ...f.pin, pin_id: "another" }, request_body: f.body }),
        ))._tag,
      ).toBe("Failure");
      expect(
        (yield* Effect.exit(
          f.records.transition(
            { ...next, revision: 3, state: "revoked", expires_at: "2026-01-01T00:25:00.000Z" },
            2,
          ),
        ))._tag,
      ).toBe("Failure");
      yield* f.records.transition({ ...next, revision: 3, state: "revoked" }, 2);
      expect((yield* f.records.read({ setup_request_id: f.pin.setup_request_id }))?.pin.state).toBe(
        "revoked",
      );
      // Retained ciphertext is bound to original owner/revision in AAD, including after a direct tamper.
      yield* f.sql`DROP TRIGGER jones_workstreams_peer_session_pins_revision_guard`;
      yield* f.sql`UPDATE jones_workstreams_peer_session_pins SET owner_id='other-owner'`;
      expect((yield* Effect.exit(f.records.read({ pin_id: f.pin.pin_id })))._tag).toBe("Failure");
    }).pipe(Effect.provide(services)),
);

it.effect("retains protected setup evidence in encrypted DB/WAL across store reconstruction", () =>
  Effect.gen(function* () {
    const assertion = {
      schema: "jones-workstreams-peer-session-pin/1",
      pin_id: "file-pin",
      revision: 1,
      receiver_authority: {
        environment_id: "home:test",
        authority_namespace: "receiver-store",
        store_generation: 1,
      },
      session: {
        session_id: "file-session",
        subject: "mcp-client",
        method: "bearer-access-token",
        issued_at: Test.now,
        expires_at: "2026-01-01T01:00:00.000Z",
        peer_link: true,
        ceiling: {
          access: "read-only",
          runtime_mode: "approval-required",
          interaction_mode: "default",
          scopes: ["orchestration:read"],
          permissions: ["workstreams:read"],
        },
      },
      peer_environment_id: "source:test",
      roles: ["member_home"],
      expected_home: home,
      source_authority: {
        environment_id: "source:test",
        authority_namespace: "source-store",
        store_generation: 1,
      },
      source_enrollment_id: "enrollment:test",
      source_binding_version: null,
      singleton_binding_version: null,
      state: "prepared",
      expires_at: "2026-01-01T00:20:00.000Z",
      setup_request_id: "file-setup",
      owner_qualification_digest: "b".repeat(64),
      bind_receipt: null,
    };
    const body = JSON.stringify({
        schema: "jones-workstreams-peer-pin-setup/1",
        assertion,
        expected: { registry_version: 0, grant_id: "grant:test", grant_version: 1 },
      }),
      f = {
        body,
        pin: Records.decodeSetupRequest(body).pin,
        receipt: {
          principal_id: "principal:test",
          binding_version: 1,
          registry_version: 2,
          request_sha256: "c".repeat(64),
        },
      };
    const parent = process.env.NW_J3A_EVIDENCE_DIR ?? NodeOS.tmpdir();
    NodeFS.mkdirSync(parent, { recursive: true });
    const dir = NodeFS.mkdtempSync(NodePath.join(parent, "nw-j3a-store-")),
      filename = NodePath.join(dir, "synthetic.sqlite");
    const persistent = () => Records.layer.pipe(Layer.provideMerge(Test.testLayer({ filename })));
    yield* Effect.gen(function* () {
      yield* (yield* SqlClient.SqlClient)`PRAGMA journal_mode = WAL`;
      yield* Test.seed;
      yield* migration;
      const records = yield* Records.PeerSessionPinStore,
        sql = yield* SqlClient.SqlClient;
      yield* sql`PRAGMA wal_autocheckpoint = 0`;
      yield* records.insert({ pin: f.pin, request_body: f.body });
      const next = {
        ...f.pin,
        revision: 2,
        state: "bound" as const,
        source_binding_version: 1,
        singleton_binding_version: 1,
        bind_receipt: f.receipt,
      };
      yield* records.transition(next, 1);
      for (const path of [filename, filename + "-wal"]) {
        expect(NodeFS.existsSync(path)).toBe(true);
        const bytes = NodeFS.readFileSync(path);
        expect(bytes.includes(Buffer.from("source-store"))).toBe(false);
        expect(bytes.includes(Buffer.from(f.body))).toBe(false);
      }
    }).pipe(Effect.provide(persistent()));
    yield* Effect.gen(function* () {
      const r = yield* (yield* Records.PeerSessionPinStore).read({
        setup_request_id: f.pin.setup_request_id,
      });
      expect(r?.request_body).toBe(f.body);
      expect(r?.pin.state).toBe("bound");
      expect(r?.pin.setup_request_sha256).toBe(
        Records.decodeSetupRequest(f.body).pin.setup_request_sha256,
      );
    }).pipe(Effect.provide(persistent()));
  }),
);

it.effect(
  "reserves native session/setup identities globally and serializes competing CAS transitions",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.records.insert({ pin: f.pin, request_body: f.body });
      const original = JSON.parse(f.body) as Records.PeerPinSetupRequest;
      const otherBody = JSON.stringify({
        ...original,
        assertion: {
          ...original.assertion,
          pin_id: "another-pin",
          setup_request_id: "another-setup",
        },
      });
      expect(
        (yield* Effect.exit(
          f.records.insert({
            pin: Records.decodeSetupRequest(otherBody).pin,
            request_body: otherBody,
          }),
        ))._tag,
      ).toBe("Failure");
      const outcomes = yield* Effect.all(
        [
          Effect.exit(
            f.records.transition(
              {
                ...f.pin,
                revision: 2,
                state: "bound",
                source_binding_version: 1,
                singleton_binding_version: 1,
                bind_receipt: f.receipt,
              },
              1,
            ),
          ),
          Effect.exit(f.records.transition({ ...f.pin, revision: 2, state: "revoked" }, 1)),
        ],
        { concurrency: 2 },
      );
      expect(outcomes.filter((o) => o._tag === "Success").length).toBe(1);
      expect((yield* f.records.read({ pin_id: f.pin.pin_id }))?.pin.revision).toBe(2);
      expect(() =>
        Records.decodeSetupRequest(
          JSON.stringify({
            ...original,
            assertion: {
              ...original.assertion,
              session: { ...original.assertion.session, bearer: "forbidden" },
            },
          }),
        ),
      ).toThrow();
    }).pipe(Effect.provide(services)),
);

// @effect-diagnostics nodeBuiltinImport:off -- Retained synthetic SQLite evidence stays in the declared external evidence cone.
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
const services = Layer.mergeAll(PinRecords.layer, Records.layer, Pins.nativeLayer).pipe(
  Layer.provideMerge(Auth.layer.pipe(Layer.provide(signing))),
  Layer.provideMerge(Test.testLayer()),
  Layer.provideMerge(Config.layerTest(process.cwd(), { prefix: "nw-home-bind-synthetic-" })),
  Layer.provideMerge(
    Layer.succeed(Environment.ServerEnvironmentIdentity, {
      getEnvironmentId: Effect.succeed(EnvironmentId.make("home:test")),
    }),
  ),
  Layer.provideMerge(NodeServices.layer),
);
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
  "genuine native authentication is required; a copied invocation object or auth row is insufficient",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* Effect.gen(function* () {
        const q = yield* Qualification.HomePeerPinQualification;
        expect((yield* q.current(f.invocation, home)).record.pin.pin_id).toBe(f.pin.pin_id);
        expect((yield* Effect.exit(q.current({ ...f.invocation }, home)))._tag).toBe("Failure");
        expect(
          (yield* Effect.exit(q.current({ ...f.invocation, requestNamespace: "other" }, home)))
            ._tag,
        ).toBe("Failure");
      }).pipe(Effect.provide(f.mounted));
    }).pipe(Effect.provide(services)),
);

it.effect("missing owner qualification has no production fallback", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const layer = Qualification.layer.pipe(Layer.provide(f.authority));
    expect(
      (yield* Effect.exit(
        Effect.gen(function* () {
          return yield* (yield* Qualification.HomePeerPinQualification).current(f.invocation, home);
        }).pipe(Effect.provide(layer)),
      ))._tag,
    ).toBe("Failure");
  }).pipe(Effect.provide(services)),
);

it.effect("fences owner, native session, pin, grant and receiver changes after admission", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* Effect.gen(function* () {
      const q = yield* Qualification.HomePeerPinQualification;
      const peer = yield* q.current(f.invocation, home);
      f.restore();
      expect((yield* Effect.exit(peer.checkLocal(f.pin)))._tag).toBe("Failure");
    }).pipe(Effect.provide(f.mounted));
  }).pipe(Effect.provide(services)),
);

it.effect("refuses revoked sessions even when their original invocation remains registered", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* f.sql`UPDATE auth_sessions SET revoked_at=${Test.now} WHERE session_id=${f.issued.sessionId}`;
    expect(
      (yield* Effect.exit(
        f.invoke(
          Effect.gen(function* () {
            return yield* (yield* Qualification.HomePeerPinQualification).current(
              f.invocation,
              home,
            );
          }),
        ),
      ))._tag,
    ).toBe("Failure");
  }).pipe(Effect.provide(services)),
);

it.effect(
  "rejects wrong setup/home and revoked pins; forged evidence cannot pass local verification",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* Effect.gen(function* () {
        const q = yield* Qualification.HomePeerPinQualification;
        expect((yield* Effect.exit(q.current(f.invocation, home, "wrong-setup")))._tag).toBe(
          "Failure",
        );
        expect(
          (yield* Effect.exit(q.current(f.invocation, { ...home, server_generation: 2 })))._tag,
        ).toBe("Failure");
        expect(
          (yield* Effect.exit(
            q.verifyBinding({} as Qualification.HomeBindingEvidence, f.pin, f.candidate.receipt, 1),
          ))._tag,
        ).toBe("Failure");
        const peer = yield* q.current(f.invocation, home);
        yield* f.pins.transition({ ...f.pin, state: "revoked", revision: 2 }, 1);
        expect((yield* Effect.exit(peer.checkLocal(f.pin)))._tag).toBe("Failure");
      }).pipe(Effect.provide(f.mounted));
    }).pipe(Effect.provide(services)),
);

it.effect(
  "qualifies exact source/build/body, detects changed digest/namespace and distinguishes provider sessions",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* Effect.gen(function* () {
        const q = yield* Qualification.HomePeerPinQualification,
          peer = yield* q.current(f.invocation, home);
        const changes: F.WorkstreamsHomeBindBodyV2[] = [
          { ...f.bodyValue, expected_home: { ...home, server_generation: 2 } },
          {
            ...f.bodyValue,
            expected_home_authority: { ...homeAuthority, authority_namespace: "wrong-home-store" },
          },
          {
            ...f.bodyValue,
            source: {
              ...f.bodyValue.source,
              authority: { ...sourceAuthority, store_generation: 2 },
              enrollment: {
                ...f.bodyValue.source.enrollment,
                context: { ...f.bodyValue.source.enrollment.context, store_generation: 2 },
              },
            },
          },
          {
            ...f.bodyValue,
            home_setup: { ...f.bodyValue.home_setup, setup_request_sha256: "e".repeat(64) },
          },
          {
            ...f.bodyValue,
            source: {
              ...f.bodyValue.source,
              authority: { ...sourceAuthority, authority_namespace: "other" },
              enrollment: {
                ...f.bodyValue.source.enrollment,
                context: { ...f.bodyValue.source.enrollment.context, authority_namespace: "other" },
              },
            },
          },
          {
            ...f.bodyValue,
            source: {
              ...f.bodyValue.source,
              enrollment: {
                ...f.bodyValue.source.enrollment,
                session: {
                  ...f.bodyValue.source.enrollment.session,
                  session_id: f.issued.sessionId,
                },
              },
            },
          },
        ];
        for (const body of changes) {
          const bytes = F.canonicalWorkstreamsHomeBindBody(body);
          f.qualifyBindBody(bytes);
          expect((yield* Effect.exit(q.prepare(peer, bytes)))._tag).toBe("Failure");
        }
        f.revokeOwner();
        expect((yield* Effect.exit(q.prepare(peer, f.request.request_body)))._tag).toBe("Failure");
      }).pipe(Effect.provide(f.mounted));
    }).pipe(Effect.provide(services)),
);

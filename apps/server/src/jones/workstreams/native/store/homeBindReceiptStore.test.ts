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
  "encrypts exact bind/setup bytes separately and refuses changed bytes or preparation consumption",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.records.insert(f.record);
      yield* f.records.insert(f.record);
      expect(yield* f.records.read(home, f.bodyValue.bind_id)).toEqual(f.record);
      const rows = yield* f.sql<{
        request_envelope: string;
        result_envelope: string;
      }>`SELECT request_envelope,result_envelope FROM jones_workstreams_home_bind_receipts`;
      expect(rows[0]!.request_envelope).not.toContain("source-store");
      expect(rows[0]!.result_envelope).not.toContain("principal:test");
      const otherBody = F.canonicalWorkstreamsHomeBindBody({
        ...f.bodyValue,
        bind_id: "other-bind",
      });
      const otherResult = {
        ...f.candidate,
        request_body: otherBody,
        receipt: { ...f.candidate.receipt, request_sha256: digest(otherBody) },
      };
      expect(
        (yield* Effect.exit(
          f.records.insert({ ...f.record, request_body: otherBody, result: otherResult }),
        ))._tag,
      ).toBe("Failure");
      expect(
        (yield* Effect.exit(
          f.records.insert({ ...f.record, home_setup_request_body: f.setupBody.trim() }),
        ))._tag,
      ).toBe("Failure");
      expect(
        (yield* Effect.exit(
          f.records.insert({
            ...f.record,
            result: {
              ...f.candidate,
              receipt: { ...f.candidate.receipt, request_sha256: "f".repeat(64) },
            },
          }),
        ))._tag,
      ).toBe("Failure");
    }).pipe(Effect.provide(services)),
);

it.effect("authenticates every selector and purpose on read even after direct SQL tampering", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* f.records.insert(f.record);
    const original = (yield* f.sql<
      Record<string, string | number>
    >`SELECT * FROM jones_workstreams_home_bind_receipts`)[0]!;
    yield* f.sql`DROP TRIGGER jones_workstreams_home_bind_receipts_no_update`;
    const changes = [
      ["request_sha256", "f".repeat(64)],
      ["home_pin_id", "wrong-pin"],
      ["home_setup_request_id", "wrong-setup"],
      ["home_setup_request_sha256", "e".repeat(64)],
      ["request_envelope", original.result_envelope!],
      ["result_envelope", original.request_envelope!],
    ] as const;
    for (const [column, value] of changes) {
      yield* f.sql.unsafe(`UPDATE jones_workstreams_home_bind_receipts SET ${column}=?`, [value]);
      expect((yield* Effect.exit(f.records.read(home, f.bodyValue.bind_id)))._tag).toBe("Failure");
      yield* f.sql.unsafe(`UPDATE jones_workstreams_home_bind_receipts SET ${column}=?`, [
        original[column]!,
      ]);
    }
    yield* f.sql`UPDATE jones_workstreams_home_bind_receipts SET owner_id='other-owner'`;
    expect(
      (yield* Effect.exit(
        f.records.read({ ...home, owner_id: "other-owner" }, f.bodyValue.bind_id),
      ))._tag,
    ).toBe("Failure");
  }).pipe(Effect.provide(services)),
);

it.effect("survives lost reply and reconstructed encrypted store without plaintext in DB/WAL", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const parent = process.env.NW_HOME_BIND_EVIDENCE_DIR;
    const dir = yield* Effect.acquireRelease(
      Effect.sync(() =>
        NodeFS.mkdtempSync(NodePath.join(parent ?? NodeOS.tmpdir(), "home-bind-store-")),
      ),
      (directory) =>
        Effect.sync(() => {
          if (parent === undefined) NodeFS.rmSync(directory, { recursive: true, force: true });
        }),
    );
    const filename = NodePath.join(dir, "synthetic.sqlite");
    const persistent = () =>
      Layer.fresh(Records.layer.pipe(Layer.provideMerge(Test.testLayer({ filename }))));
    yield* Effect.gen(function* () {
      yield* Test.seed;
      yield* migration;
      const sql = yield* SqlClient.SqlClient,
        records = yield* Records.HomeBindReceiptStore;
      yield* sql`PRAGMA journal_mode=WAL`;
      yield* sql`PRAGMA wal_autocheckpoint=0`;
      yield* records.insert(f.record);
      for (const path of [filename, filename + "-wal"]) {
        expect(NodeFS.existsSync(path)).toBe(true);
        const bytes = NodeFS.readFileSync(path);
        expect(bytes.includes(Buffer.from(f.setupBody))).toBe(false);
        expect(bytes.includes(Buffer.from(f.request.request_body))).toBe(false);
      }
    }).pipe(Effect.provide(persistent()));
    yield* Effect.gen(function* () {
      const records = yield* Records.HomeBindReceiptStore;
      expect(yield* records.read(home, f.bodyValue.bind_id)).toEqual(f.record);
      yield* records.insert(f.record);
    }).pipe(Effect.provide(persistent()));
  }).pipe(Effect.provide(services), Effect.scoped),
);

it.effect(
  "refuses a correctly authenticated local-key envelope used in a home-scoped receipt",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.records.insert(f.record);
      const envelope = yield* f.db.sealJson(
        {
          ...Test.scope,
          key_scope: "local",
          purpose: "home_bind_request",
          record_id: f.bodyValue.bind_id,
          revision: 1,
        },
        { request_body: f.request.request_body, home_setup_request_body: f.setupBody },
      );
      yield* f.sql`DROP TRIGGER jones_workstreams_home_bind_receipts_no_update`;
      yield* f.sql`UPDATE jones_workstreams_home_bind_receipts SET request_envelope=${envelope}`;
      expect((yield* Effect.exit(f.records.read(home, f.bodyValue.bind_id)))._tag).toBe("Failure");
    }).pipe(Effect.provide(services)),
);

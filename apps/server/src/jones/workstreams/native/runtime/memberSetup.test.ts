// @effect-diagnostics nodeBuiltinImport:off -- Synthetic file-backed evidence is retained in the assigned external cone for root cleanup.
// @effect-diagnostics preferSchemaOverJson:off -- Tests deliberately retain and corrupt exact wire JSON bytes.
import * as NodeFS from "node:fs";
import { expect, it } from "@effect/vitest";
import { AuthSessionId, EnvironmentId } from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/sql/SqlClient";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import * as HomeWire from "@t3tools/contracts/jones/workstreamsHomeBind";
import * as MemberWire from "@t3tools/contracts/jones/workstreamsMemberEnrollment";
import {
  sha256EnrollmentBytes as sha,
  deriveNativeEnrollmentSession,
} from "../../enrollment/request.ts";
import * as Auth from "../../../../auth/EnvironmentAuth.ts";
import * as Secrets from "../../../../auth/ServerSecretStore.ts";
import * as SessionStore from "../../../../auth/SessionStore.ts";
import * as AuthRows from "../../../../persistence/AuthSessions.ts";
import * as Config from "../../../../config.ts";
import * as Environment from "../../../../environment/ServerEnvironment.ts";
import * as Authority from "../../../../environment/NativeStoreAuthority.ts";
import { runMigrations } from "../../../../persistence/Migrations.ts";
import migration126 from "../../../persistence/Migrations/126_JonesWorkstreamsSourceEnrollments.ts";
import migration128 from "../../../persistence/Migrations/128_JonesWorkstreamsPeerSessionPins.ts";
import migration129 from "../../../persistence/Migrations/129_JonesWorkstreamsMemberBindings.ts";
import type * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import * as PeerLinks from "../../../../peer/PeerLinks.ts";
import * as PeerClient from "../../../../peer/PeerMcpClient.ts";
import * as Test from "../store/sql.testkit.ts";
import * as Records from "../store/memberEnrollmentStore.ts";
import * as PinRecords from "../store/peerSessionPinStore.ts";
import * as Bindings from "../store/bindingStore.ts";
import * as KeyProvider from "../crypto/keyProvider.ts";
import * as NativePins from "./peerSessionPins.ts";
import * as PinSetup from "./peerSessionPinSetup.ts";
import * as Source from "./sourceEnrollment.ts";
import * as Members from "./memberEnrollment.ts";
import * as Setup from "./memberSetup.ts";
import * as Qualification from "./memberPeerPinQualification.ts";

const decodeUnknownSyncHomeWireWorkstreamsHomeBindBodyV2 = Schema.decodeUnknownSync(
  HomeWire.WorkstreamsHomeBindBodyV2,
);
const decodeUnknownSyncMemberWireWorkstreamsMemberEnrollmentIntentV1 = Schema.decodeUnknownSync(
  MemberWire.WorkstreamsMemberEnrollmentIntentV1,
);
const decodeUnknownSyncMemberWireWorkstreamsMemberEnrollmentPreparedV1 = Schema.decodeUnknownSync(
  MemberWire.WorkstreamsMemberEnrollmentPreparedV1,
);
const decodeUnknownSyncHomeWireWorkstreamsHomeBindCommittedV2 = Schema.decodeUnknownSync(
  HomeWire.WorkstreamsHomeBindCommittedV2,
);
const decodeUnknownSyncMemberWireWorkstreamsMemberEnrollmentBoundV1 = Schema.decodeUnknownSync(
  MemberWire.WorkstreamsMemberEnrollmentBoundV1,
);
const decodeUnknownEffectHomeWireWorkstreamsSourceIssuanceRequestV1 = Schema.decodeUnknownEffect(
  HomeWire.WorkstreamsSourceIssuanceRequestV1,
);
const evidence =
  "/home/malcolmjones/.local/share/agent-work/native-workstreams-d1842794/nw-member-enrollment-evidence";
const home = { ...Test.scope, home_environment_id: "home:test", server_generation: 1 };
const sourceAuthority = {
  environment_id: "member:test",
  authority_namespace: "member-store",
  store_generation: 1,
};
const homeAuthority = {
  environment_id: "home:test",
  authority_namespace: "home-store",
  store_generation: 1,
};
const build = {
  repository: "Jones-Systems/Jones-Code" as const,
  sha: "a".repeat(40),
  tree: "b".repeat(40),
};
const issuance = {
  schema: "jones-code.workstreams-native-enrollment/v1",
  enrollment_id: "member-enrollment:test",
  registry_origin: "https://native.workstreams.invalid",
  context: {
    owner_id: Test.scope.owner_id,
    principal_id: "principal:test",
    source_instance_id: "member:test",
    authority_namespace: "member-store",
    store_generation: 1,
    enrollment_id: "member-enrollment:test",
    protocol: "workstreams-t3-provider/1.0.0",
    build,
  },
  session: {
    session_id: "provider-session:test",
    issued_at: Test.now,
    expires_at: "2026-01-02T00:00:00.000Z",
  },
};
const syntheticSession = {
  session_id: "member-link:test",
  subject: "mcp-client",
  method: "bearer-access-token",
  issued_at: Test.now,
  expires_at: "2026-01-31T00:00:00.000Z",
  peer_link: true,
  ceiling: {
    access: "full-access",
    runtime_mode: "approval-required",
    interaction_mode: "default",
    scopes: [
      "orchestration:read",
      "orchestration:operate",
      "settings:write",
      "source-control:write",
    ],
    permissions: ["workstreams:read", "workstreams:write"],
  },
};
const makeIntent = (session: unknown = syntheticSession) => {
  const assertion = (atHome: boolean) => ({
    schema: "jones-workstreams-peer-session-pin/1",
    pin_id: atHome ? "home-pin:test" : "member-pin:test",
    revision: 1,
    receiver_authority: atHome ? homeAuthority : sourceAuthority,
    session: atHome ? { ...syntheticSession, session_id: "home-link:test" } : session,
    peer_environment_id: atHome ? "member:test" : "home:test",
    roles: [atHome ? "member_home" : "home_source"],
    expected_home: home,
    source_authority: sourceAuthority,
    source_enrollment_id: issuance.enrollment_id,
    source_binding_version: null,
    singleton_binding_version: null,
    state: "prepared",
    expires_at: "2026-01-01T00:20:00.000Z",
    setup_request_id: atHome ? "home-setup:test" : "member-setup:test",
    owner_qualification_digest: "c".repeat(64),
    bind_receipt: null,
  });
  const preconditions = { registry_version: 9, grant_id: "grant:test", grant_version: 1 };
  const setup = (atHome: boolean) =>
    JSON.stringify(
      {
        schema: "jones-workstreams-peer-pin-setup/1",
        assertion: assertion(atHome),
        expected: preconditions,
      },
      null,
      2,
    ) + "\n";
  const homeBytes = setup(true),
    memberBytes = setup(false);
  const body = HomeWire.canonicalWorkstreamsHomeBindBody(
    decodeUnknownSyncHomeWireWorkstreamsHomeBindBodyV2({
      bind_id: "bind:test",
      expected_home: home,
      expected_canonical_origin: issuance.registry_origin,
      expected_home_authority: homeAuthority,
      source: { authority: sourceAuthority, enrollment: issuance },
      home_setup: {
        pin_id: "home-pin:test",
        prepared_revision: 1,
        setup_request_id: "home-setup:test",
        setup_request_sha256: sha(homeBytes),
      },
      member_setup: {
        pin_id: "member-pin:test",
        prepared_revision: 1,
        setup_request_id: "member-setup:test",
        outbound_link_generation: "opaque-link-generation:1",
        expected_singleton_version: null,
        singleton_binding_version: 1,
      },
      expected: { principal_id: "principal:test", ...preconditions, source_binding: null },
      rebind: false,
    }),
  );
  return decodeUnknownSyncMemberWireWorkstreamsMemberEnrollmentIntentV1({
    issuance_body: JSON.stringify(issuance, null, 2) + "\n",
    bind_request_body: body,
    bind_request_sha256: sha(body),
    home_setup: {
      contract: HomeWire.WORKSTREAMS_HOME_BIND_CONTRACT,
      kind: "prepared",
      request: {
        contract: HomeWire.WORKSTREAMS_HOME_BIND_CONTRACT,
        expected_home: home,
        home_setup_request_id: "home-setup:test",
      },
      canonical_origin: issuance.registry_origin,
      home_authority: homeAuthority,
      setup_request_body: homeBytes,
      pin: { ...assertion(true), setup_request_sha256: sha(homeBytes) },
    },
    member_setup_request_body: memberBytes,
    member_setup_request_sha256: sha(memberBytes),
  });
};
const preparedRecord = (intent = makeIntent()) =>
  decodeUnknownSyncMemberWireWorkstreamsMemberEnrollmentPreparedV1({
    schema: MemberWire.WORKSTREAMS_MEMBER_ENROLLMENT_SCHEMA,
    intent,
    intent_sha256: sha(MemberWire.canonicalWorkstreamsMemberEnrollmentIntent(intent)),
    state: "prepared",
    revision: 1,
    home_result: null,
    local_binding: null,
  });
const homeResult = (intent = makeIntent()) =>
  decodeUnknownSyncHomeWireWorkstreamsHomeBindCommittedV2({
    contract: HomeWire.WORKSTREAMS_HOME_BIND_CONTRACT,
    kind: "committed",
    request_body: intent.bind_request_body,
    receipt: {
      principal_id: "principal:test",
      binding_version: 1,
      registry_version: 10,
      request_sha256: intent.bind_request_sha256,
    },
    home_pin_bound_revision: 2,
  });
const boundRecord = (intent = makeIntent()) =>
  decodeUnknownSyncMemberWireWorkstreamsMemberEnrollmentBoundV1({
    ...preparedRecord(intent),
    state: "bound",
    revision: 2,
    home_result: homeResult(intent),
    local_binding: { pin_bound_revision: 2, singleton_binding_version: 1 },
  });

const baseServices = (filename?: string) =>
  NativePins.nativeLayer.pipe(
    Layer.provideMerge(AuthRows.layer),
    Layer.provideMerge(
      Auth.layer.pipe(
        Layer.provide(
          Layer.succeed(Secrets.ServerSecretStore, {
            get: () => Effect.succeed(Option.some(Test.keyBytes)),
            getOrCreateRandom: () => Effect.succeed(Test.keyBytes),
            set: () => Effect.die("unexpected synthetic write"),
            create: () => Effect.die("unexpected synthetic create"),
            remove: () => Effect.die("unexpected synthetic remove"),
          }),
        ),
      ),
    ),
    Layer.provideMerge(
      Test.testLayer({
        ...(filename ? { filename } : {}),
        keys: new Map([...Test.secrets(), ["server-signing-key", Test.keyBytes]]),
      }),
    ),
    Layer.provideMerge(Config.layerTest(process.cwd(), NodeFS.mkdtempSync(evidence + "/config-"))),
    Layer.provideMerge(
      Layer.succeed(Environment.ServerEnvironmentIdentity, {
        getEnvironmentId: Effect.succeed(EnvironmentId.make("member:test")),
      }),
    ),
    Layer.provideMerge(NodeServices.layer),
  );
const fixture = Effect.gen(function* () {
  yield* TestClock.setTime(Date.parse(Test.now));
  yield* runMigrations({ toMigrationInclusive: 54 });
  yield* Test.seed;
  yield* migration126;
  yield* migration128;
  yield* migration129;
  yield* (yield* SqlClient.SqlClient)`CREATE TABLE jones_workstreams_member_writer_probe(value TEXT)`;
  const auth = yield* Auth.EnvironmentAuth,
    native = yield* NativePins.NativePeerSessions,
    sql = yield* SqlClient.SqlClient,
    sessions = yield* AuthRows.AuthSessionRepository;
  const issued = yield* auth.issueMcpClientSession({
    label: "synthetic-link",
    access: "full-access",
    peerLink: true,
    client: { deviceType: "bot" },
  });
  const invocation: Mcp.McpInvocationScope = {
    environmentId: EnvironmentId.make("member:test"),
    issuedAt: Date.parse(Test.now),
    requestNamespace: "synthetic-member-request",
    capabilities: new Set(["orchestration"]),
    thread: undefined,
    client: {
      sessionId: issued.sessionId,
      label: "not-authority",
      access: "full-access",
      linked: true,
    },
  };
  const request = {
    cookies: {},
    headers: { authorization: `Bearer ${issued.token}` },
  } as unknown as Parameters<Auth.EnvironmentAuth["Service"]["authenticateMcpClient"]>[0];
  yield* native.authenticate(request, invocation);
  const session = (yield* native.current(invocation)).session;
  const intent = makeIntent(session);
  const state = {
    generation: "opaque-link-generation:1",
    resolvedGeneration: "opaque-link-generation:1",
    owner: true,
    sourceGeneration: 1,
    currentBuild: { ...build },
    committed: true,
    loseBindReply: false,
    wrongReadback: false,
    wrongSetup: false,
    unsupported: false,
    afterReadback: Effect.void as Effect.Effect<void>,
    afterBind: Effect.void as Effect.Effect<void>,
    beforeReadback: Effect.void as Effect.Effect<void>,
    beforeKeyGet: Effect.void as Effect.Effect<void>,
  };
  const calls: Array<{
    method: string;
    tool?: string;
    args?: unknown;
    headers: Readonly<Record<string, string>>;
  }> = [];
  const linkLayer = Layer.mock(PeerLinks.PeerLinks)({
    currentGeneration: () => Effect.sync(() => Option.some(state.generation)),
    resolve: () =>
      Effect.succeed({
        url: issuance.registry_origin,
        token: "synthetic-home-link-token",
        generation: state.resolvedGeneration,
        link: {},
      } as PeerLinks.ResolvedPeerLink),
    recordOutcome: () => Effect.void,
  });
  // Synthetic remote endpoint; the actual PeerMcpClient performs negotiation, generation
  // fences, limits headers, decoding and no-retry behavior. This is not a mounted H producer.
  const http = HttpClient.make((request) =>
    Effect.gen(function* () {
      if (request.body._tag !== "Uint8Array") return yield* Effect.die("expected JSON");
      const body = JSON.parse(new TextDecoder().decode(request.body.body));
      calls.push({
        method: body.method,
        tool: body.params?.name,
        args: body.params?.arguments,
        headers: request.headers,
      });
      let response: Response;
      if (body.method === "initialize")
        response = Response.json(
          {
            jsonrpc: "2.0",
            id: body.id,
            result: {
              protocolVersion: "2025-06-18",
              capabilities: {},
              serverInfo: { name: "synthetic-H", version: "1" },
            },
          },
          { headers: { "mcp-session-id": "synthetic-outbound-session" } },
        );
      else if (body.method === "notifications/initialized")
        response = new Response(null, { status: 202 });
      else {
        let result: unknown;
        if (body.params.name === "jones_workstreams_home_bind_setup")
          result = state.wrongSetup
            ? { ...intent.home_setup, canonical_origin: "https://wrong.invalid" }
            : intent.home_setup;
        else if (body.params.name === "jones_workstreams_home_bind") {
          yield* state.afterBind;
          if (state.loseBindReply)
            return HttpClientResponse.fromWeb(request, new Response(null, { status: 503 }));
          result = homeResult(intent);
        } else if (body.params.name === "jones_workstreams_home_bind_readback") {
          yield* state.beforeReadback;
          result = state.committed
            ? {
                contract: HomeWire.WORKSTREAMS_HOME_BIND_CONTRACT,
                request: body.params.arguments,
                kind: "committed",
                result: state.wrongReadback
                  ? {
                      ...homeResult(intent),
                      receipt: { ...homeResult(intent).receipt, request_sha256: "f".repeat(64) },
                    }
                  : homeResult(intent),
              }
            : {
                contract: HomeWire.WORKSTREAMS_HOME_BIND_CONTRACT,
                request: body.params.arguments,
                kind: "unknown",
                reason: "in_flight",
              };
          yield* state.afterReadback;
        } else return yield* Effect.die("unexpected dynamic tool");
        response = state.unsupported
          ? Response.json({
              jsonrpc: "2.0",
              id: body.id,
              error: { code: -32601, message: "unsupported" },
            })
          : Response.json({
              jsonrpc: "2.0",
              id: body.id,
              result: { content: [], structuredContent: result },
            });
      }
      return HttpClientResponse.fromWeb(request, response);
    }),
  );
  const issuer = Layer.succeed(Source.SourceIssuer, {
    authority: {
      readCurrent: Effect.sync(() => ({
        environmentId: "member:test",
        authorityNamespace: "member-store",
        storeGeneration: state.sourceGeneration,
      })),
    },
    build: Effect.sync(() => Option.some(state.currentBuild)),
  });
  const owner = Layer.succeed(Source.SourceSetupOwner, {
    verifyUnchanged: Effect.suspend(() =>
      state.owner
        ? Effect.void
        : Effect.fail(new Source.SourceEnrollmentError({ code: "owner_unqualified" })),
    ),
  });
  const port = Layer.succeed(PinSetup.PeerPinSetupQualification, {
    qualify: (bytes, digest) =>
      bytes === intent.member_setup_request_body && digest === intent.member_setup_request_sha256
        ? Effect.succeed({
            invocation,
            owner_qualification_digest: "c".repeat(64),
            checkCurrent: () => Effect.void,
          })
        : Effect.fail(new NativePins.PeerSessionError({ code: "owner_unqualified" })),
    verifyBinding: () => Effect.die("M must use its private readback verifier"),
  });
  const keyLayer = Layer.effect(
    KeyProvider.WorkstreamsKeyProvider,
    Effect.map(KeyProvider.WorkstreamsKeyProvider, (originalKeys) => ({
      ...originalKeys,
      get: (id: string) => state.beforeKeyGet.pipe(Effect.flatMap(() => originalKeys.get(id))),
    })),
  );
  const dependencies = Layer.mergeAll(
    keyLayer,
    linkLayer,
    issuer,
    owner,
    port,
    Layer.succeed(Authority.NativeStoreAuthority, {
      readCurrent: Effect.sync(() => ({
        environmentId: "member:test",
        authorityNamespace: "member-store",
        storeGeneration: state.sourceGeneration,
      })),
    } as unknown as Authority.NativeStoreAuthority["Service"]),
  );
  const qualification = Qualification.layer.pipe(
    Layer.provideMerge(
      PeerClient.layer.pipe(Layer.provide(Layer.succeed(HttpClient.HttpClient, http))),
    ),
  );
  const stores = Layer.mergeAll(Records.layer, PinRecords.layer, Bindings.layer);
  const mounted = Layer.mergeAll(
    Setup.layer.pipe(Layer.provideMerge(qualification)),
    Members.layer.pipe(Layer.provideMerge(stores)),
  ).pipe(Layer.provideMerge(dependencies));
  const counts = Effect.gen(function* () {
    const provider = yield* sessions.getById({
      sessionId: AuthSessionId.make(issuance.session.session_id),
    });
    const ledger = yield* sql`SELECT state,revision FROM jones_workstreams_member_enrollments`;
    const pin = yield* sql`SELECT state,revision FROM jones_workstreams_peer_session_pins`;
    const singleton = yield* sql`SELECT binding_version FROM jones_workstreams_binding_state`;
    return { provider: Option.isSome(provider), ledger, pin, singleton };
  });
  return { intent, state, calls, mounted, invocation, request, native, sessions, sql, counts };
});

it.effect(
  "atomic member acceptance issues the original provider session once and lost M reply replays",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* Effect.gen(function* () {
        const setup = yield* Setup.MemberSetup,
          members = yield* Members.MemberEnrollments;
        expect(yield* setup.readback(f.intent)).toBeNull();
        const proof = yield* setup.apply(f.intent);
        expect(proof).toEqual(boundRecord(f.intent));
        expect(yield* setup.apply(f.intent)).toEqual(proof);
        expect(yield* setup.readback(f.intent)).toEqual(proof);
        expect(yield* f.counts).toMatchObject({
          provider: true,
          ledger: [{ state: "bound", revision: 2 }],
          pin: [{ state: "bound", revision: 2 }],
          singleton: [{ binding_version: 1 }],
        });
        expect(f.calls.filter((c) => c.tool === "jones_workstreams_home_bind")).toHaveLength(1);
        const token = yield* members.materialize(issuance.session.session_id);
        expect((yield* (yield* SessionStore.SessionStore).verify(token)).sessionId).toBe(
          issuance.session.session_id,
        );
        expect(f.calls.every((c) => c.headers.authorization !== `Bearer ${token}`)).toBe(true);
        expect(yield* members.getValidatedBySessionId(issuance.session.session_id)).toMatchObject({
          _tag: "Some",
          value: proof,
        });
      }).pipe(Effect.provide(f.mounted));
    }).pipe(Effect.provide(baseServices())),
);

it.effect(
  "lost H reply preserves preparation; unknown readback never redispatches or issues another ID",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* Effect.gen(function* () {
        const setup = yield* Setup.MemberSetup;
        f.state.loseBindReply = true;
        f.state.committed = false;
        expect(yield* setup.apply(f.intent).pipe(Effect.result)).toMatchObject({
          _tag: "Failure",
          failure: { code: "remote_unknown" },
        });
        expect(yield* f.counts).toMatchObject({
          provider: false,
          ledger: [{ state: "prepared", revision: 1 }],
          pin: [{ state: "prepared", revision: 1 }],
          singleton: [],
        });
        expect(yield* setup.apply(f.intent).pipe(Effect.result)).toMatchObject({
          _tag: "Failure",
          failure: { code: "remote_unknown" },
        });
        expect(yield* setup.readback(f.intent).pipe(Effect.result)).toMatchObject({
          _tag: "Failure",
          failure: { code: "remote_unknown" },
        });
        expect(f.calls.filter((c) => c.tool === "jones_workstreams_home_bind")).toHaveLength(1);
        f.state.committed = true;
        f.state.loseBindReply = false;
        expect(yield* setup.readback(f.intent)).toEqual(boundRecord(f.intent));
        expect(yield* f.counts).toMatchObject({
          provider: true,
          singleton: [{ binding_version: 1 }],
        });
        expect(f.calls.filter((c) => c.tool === "jones_workstreams_home_bind")).toHaveLength(1);
      }).pipe(Effect.provide(f.mounted));
    }).pipe(Effect.provide(baseServices())),
);

it.effect(
  "provider reservation, singleton, pin and proof roll back together on acceptance failure",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* Effect.gen(function* () {
        const setup = yield* Setup.MemberSetup;
        yield* f.sql`CREATE TRIGGER synthetic_accept_failure BEFORE UPDATE ON jones_workstreams_member_enrollments BEGIN SELECT RAISE(ABORT,'deliberate acceptance failure'); END`;
        expect(yield* setup.apply(f.intent).pipe(Effect.result)).toMatchObject({ _tag: "Failure" });
        expect(yield* f.counts).toMatchObject({
          provider: false,
          ledger: [{ state: "prepared", revision: 1 }],
          pin: [{ state: "prepared", revision: 1 }],
          singleton: [],
        });
        yield* f.sql`DROP TRIGGER synthetic_accept_failure`;
        expect(yield* setup.readback(f.intent)).toEqual(boundRecord(f.intent));
        expect(f.calls.filter((c) => c.tool === "jones_workstreams_home_bind")).toHaveLength(1);
      }).pipe(Effect.provide(f.mounted));
    }).pipe(Effect.provide(baseServices())),
);

it.effect("held remote readback releases the writer and a relink fences final acceptance", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* Effect.gen(function* () {
      const entered = yield* Deferred.make<void>(),
        release = yield* Deferred.make<void>();
      f.state.beforeReadback = Effect.gen(function* () {
        yield* Deferred.succeed(entered, undefined);
        yield* Deferred.await(release);
      });
      const setup = yield* Setup.MemberSetup;
      const worker = yield* Effect.forkChild(setup.apply(f.intent).pipe(Effect.result));
      yield* Deferred.await(entered);
      // This separate writer completes while remote I/O remains deliberately blocked.
      yield* f.sql.withTransaction(
        f.sql`INSERT INTO jones_workstreams_member_writer_probe(value) VALUES ('writer released')`,
      );
      f.state.generation = "relinked";
      yield* Deferred.succeed(release, undefined);
      expect(yield* Fiber.join(worker)).toMatchObject({ _tag: "Failure" });
      expect(yield* f.counts).toMatchObject({
        provider: false,
        singleton: [],
        ledger: [{ state: "prepared" }],
      });
    }).pipe(Effect.provide(f.mounted));
  }).pipe(
    Effect.tap(() => Effect.void),
    Effect.provide(baseServices()),
  ),
);

it.effect.each(["revocation", "owner", "authority", "pin", "singleton"] as const)(
  "final local %s change refuses association after authenticated remote readback",
  (change) =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* Effect.gen(function* () {
        const pinStore = yield* PinRecords.PeerSessionPinStore,
          bindingStore = yield* Bindings.BindingStore;
        f.state.afterReadback = Effect.gen(function* () {
          if (change === "revocation")
            yield* f.sessions
              .revoke({
                sessionId: AuthSessionId.make(f.invocation.client!.sessionId),
                revokedAt: DateTime.makeUnsafe(Test.now),
              })
              .pipe(Effect.orDie);
          if (change === "owner") f.state.owner = false;
          if (change === "authority") f.state.sourceGeneration = 2;
          if (change === "pin")
            yield* pinStore
              .transition(
                {
                  ...PinRecords.decodeSetupRequest(f.intent.member_setup_request_body).pin,
                  revision: 2,
                  state: "revoked",
                },
                1,
              )
              .pipe(Effect.orDie);
          if (change === "singleton")
            yield* bindingStore
              .bind(
                {
                  ...Test.scope,
                  home_environment_id: "home:test",
                  home_generation: 1,
                  principal_id: "principal:test",
                  binding_version: 1,
                  state: "active",
                },
                null,
              )
              .pipe(Effect.orDie);
        });
        expect(yield* (yield* Setup.MemberSetup).apply(f.intent).pipe(Effect.result)).toMatchObject(
          { _tag: "Failure" },
        );
        const result = yield* f.counts;
        expect(result.provider).toBe(false);
        expect(result.ledger).toEqual([{ state: "prepared", revision: 1 }]);
      }).pipe(Effect.provide(f.mounted));
    }).pipe(Effect.provide(baseServices())),
);

it.effect(
  "source session collision and revoked/relinked preparation produce no member effect",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const enrollment =
        yield* decodeUnknownEffectHomeWireWorkstreamsSourceIssuanceRequestV1(issuance);
      yield* Effect.gen(function* () {
        const setup = yield* Setup.MemberSetup;
        f.state.generation = "relinked";
        expect(yield* setup.apply(f.intent).pipe(Effect.result)).toMatchObject({
          _tag: "Failure",
          failure: { code: "link_changed" },
        });
        expect(f.calls).toHaveLength(0);
        f.state.generation = "opaque-link-generation:1";
        yield* f.sessions.createIfAbsent({
          ...deriveNativeEnrollmentSession(enrollment),
          subject: "unrelated-existing-session",
        });
        expect(yield* setup.apply(f.intent).pipe(Effect.result)).toMatchObject({
          _tag: "Failure",
          failure: { code: "session_conflict" },
        });
        expect((yield* f.counts).ledger).toEqual([]);
        expect(f.calls.filter((c) => c.tool === "jones_workstreams_home_bind")).toHaveLength(0);
      }).pipe(Effect.provide(f.mounted));
    }).pipe(Effect.provide(baseServices())),
);

it.effect(
  "a fresh runtime reconstructs accepted proof from the local ledger and prepared proof through new authenticated readback",
  () =>
    Effect.gen(function* () {
      const filename = NodeFS.mkdtempSync(evidence + "/member-restart-") + "/state.sqlite";
      const f = yield* Effect.scoped(
        Effect.gen(function* () {
          const f = yield* fixture;
          yield* Effect.gen(function* () {
            f.state.loseBindReply = true;
            expect(
              yield* (yield* Setup.MemberSetup).apply(f.intent).pipe(Effect.result),
            ).toMatchObject({ _tag: "Failure", failure: { code: "remote_unknown" } });
          }).pipe(Effect.provide(f.mounted));
          return f;
        }).pipe(Effect.provide(baseServices(filename))),
      );
      f.state.loseBindReply = false;
      const proof = yield* Effect.scoped(
        Effect.gen(function* () {
          yield* TestClock.setTime(Date.parse(Test.now));
          // A restart has a fresh WeakMap. The native authentication entry point must
          // register the exact owner-qualified scope again before any private witness exists.
          const native = yield* NativePins.NativePeerSessions;
          expect(yield* native.current(f.invocation).pipe(Effect.result)).toMatchObject({
            _tag: "Failure",
          });
          yield* native.authenticate(f.request, f.invocation);
          return yield* Effect.gen(function* () {
            const proof = yield* (yield* Setup.MemberSetup).readback(f.intent);
            expect(proof).toEqual(boundRecord(f.intent));
            return proof;
          }).pipe(Effect.provide(f.mounted));
        }).pipe(Effect.provide(baseServices(filename))),
      );
      const readbackCalls = f.calls.filter(
        (c) => c.tool === "jones_workstreams_home_bind_readback",
      ).length;
      yield* Effect.scoped(
        Effect.gen(function* () {
          yield* TestClock.setTime(Date.parse(Test.now));
          yield* (yield* NativePins.NativePeerSessions).authenticate(f.request, f.invocation);
          yield* Effect.gen(function* () {
            expect(yield* (yield* Setup.MemberSetup).apply(f.intent)).toEqual(proof);
            const sql = yield* SqlClient.SqlClient;
            expect(
              yield* sql`SELECT session_id FROM auth_sessions WHERE session_id=${issuance.session.session_id}`,
            ).toHaveLength(1);
            expect(yield* sql`SELECT binding_version FROM jones_workstreams_binding_state`).toEqual(
              [{ binding_version: 1 }],
            );
          }).pipe(Effect.provide(f.mounted));
        }).pipe(Effect.provide(baseServices(filename))),
      );
      expect(f.calls.filter((c) => c.tool === "jones_workstreams_home_bind")).toHaveLength(1);
      expect(f.calls.filter((c) => c.tool === "jones_workstreams_home_bind_readback")).toHaveLength(
        readbackCalls,
      );
    }),
);

it.effect("key acquisition waits outside the writer and final source checks still apply", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* Effect.gen(function* () {
      const entered = yield* Deferred.make<void>(),
        release = yield* Deferred.make<void>();
      let first = true;
      f.state.beforeKeyGet = Effect.suspend(() => {
        if (!first) return Effect.void;
        first = false;
        return Effect.gen(function* () {
          yield* Deferred.succeed(entered, undefined);
          yield* Deferred.await(release);
        });
      });
      const worker = yield* Effect.forkChild(
        (yield* Setup.MemberSetup).apply(f.intent).pipe(Effect.result),
      );
      yield* Deferred.await(entered);
      yield* f.sql.withTransaction(
        f.sql`INSERT INTO jones_workstreams_member_writer_probe(value) VALUES ('key wait outside writer')`,
      );
      f.state.sourceGeneration = 2;
      yield* Deferred.succeed(release, undefined);
      expect(yield* Fiber.join(worker)).toMatchObject({ _tag: "Failure" });
      expect(yield* f.counts).toMatchObject({
        provider: false,
        ledger: [],
        pin: [],
        singleton: [],
      });
      expect(f.calls).toEqual([]);
    }).pipe(Effect.provide(f.mounted));
  }).pipe(Effect.provide(baseServices())),
);

it.effect(
  "revocation before preparation refuses without pin, provider session or remote dispatch",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.sessions.revoke({
        sessionId: AuthSessionId.make(f.invocation.client!.sessionId),
        revokedAt: DateTime.makeUnsafe(Test.now),
      });
      yield* Effect.gen(function* () {
        expect(yield* (yield* Setup.MemberSetup).apply(f.intent).pipe(Effect.result)).toMatchObject(
          { _tag: "Failure" },
        );
        expect(yield* f.counts).toMatchObject({
          provider: false,
          ledger: [],
          pin: [],
          singleton: [],
        });
        expect(f.calls).toEqual([]);
      }).pipe(Effect.provide(f.mounted));
    }).pipe(Effect.provide(baseServices())),
);

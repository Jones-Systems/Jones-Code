// @effect-diagnostics nodeBuiltinImport:off -- Synthetic file-backed fixtures are retained externally for ROOT, the cleanup owner.
// @effect-diagnostics preferSchemaOverJson:off -- Tamper tests intentionally preserve noncanonical wire bytes and re-encode fixtures.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";
import { expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  AuthSessionId,
  EnvironmentId,
  ProviderInstanceId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as HomeWire from "@t3tools/contracts/jones/workstreamsHomeBind";
import * as MemberWire from "@t3tools/contracts/jones/workstreamsMemberEnrollment";
import * as Forward from "@t3tools/contracts/jones/workstreamsForwarding";
import * as Parent from "@t3tools/contracts/jones/workstreamsForwardQualification";
import * as Raw from "@t3tools/contracts/jones/workstreamsForwardReadmission";
import { canonicalWorkstreamsSnapshotJson as canonical } from "@t3tools/contracts/jones/workstreamsSnapshot";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/sql/SqlClient";
import * as HttpServer from "effect/http/HttpServer";
import * as NetAddress from "effect/net/NetAddress";
import * as Auth from "../../../../auth/EnvironmentAuth.ts";
import * as Sessions from "../../../../persistence/AuthSessions.ts";
import * as Secrets from "../../../../auth/ServerSecretStore.ts";
import * as Config from "../../../../config.ts";
import * as Environment from "../../../../environment/ServerEnvironment.ts";
import * as Authority from "../../../../environment/NativeStoreAuthority.ts";
import * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import * as Credentials from "../../../../mcp/McpSessionRegistry.ts";
import { liveThreadShell } from "../../../../mcp/McpToolAccess.testkit.ts";
import * as Threads from "../../../../orchestration-v2/ThreadManagementService.ts";
import type {
  ProjectionRecords,
  ProjectionRecordField,
} from "../../../../orchestration-v2/ProjectionStore.ts";
import * as Providers from "../../../../orchestration-v2/ProviderSessionManager.ts";
import * as Links from "../../../../peer/PeerLinks.ts";
import { runMigrations } from "../../../../persistence/Migrations.ts";
import migration126 from "../../../persistence/Migrations/126_JonesWorkstreamsSourceEnrollments.ts";
import migration128 from "../../../persistence/Migrations/128_JonesWorkstreamsPeerSessionPins.ts";
import migration129 from "../../../persistence/Migrations/129_JonesWorkstreamsMemberBindings.ts";
import * as Source from "../runtime/sourceEnrollment.ts";
import * as Pins from "../runtime/peerSessionPins.ts";
import * as Members from "../runtime/memberEnrollment.ts";
import * as Admission from "../runtime/memberAdmission.ts";
import * as Local from "../runtime/memberRecordedCaller.ts";
import * as Startup from "../runtime/startup.ts";
import * as Operations from "../store/operationStore.ts";
import * as PinRecords from "../store/peerSessionPinStore.ts";
import * as MemberRecords from "../store/memberEnrollmentStore.ts";
import * as Bindings from "../store/bindingStore.ts";
import * as Test from "../store/sql.testkit.ts";
import * as Readmission from "./readmissionV2.ts";

const decodeBindBody = Schema.decodeUnknownEffect(HomeWire.WorkstreamsHomeBindBodyV2);
const decodeIntent = Schema.decodeUnknownEffect(MemberWire.WorkstreamsMemberEnrollmentIntentV1);
const decodePrepared = Schema.decodeUnknownEffect(MemberWire.WorkstreamsMemberEnrollmentPreparedV1);
const decodeBound = Schema.decodeUnknownEffect(MemberWire.WorkstreamsMemberEnrollmentBoundV1);
const decodeForward = Schema.decodeUnknownEffect(Forward.WorkstreamsForwardRequest);

const sha = (bytes: string) => NodeCrypto.createHash("sha256").update(bytes).digest("hex");
const home = { ...Test.scope, home_environment_id: "home:test", server_generation: 1 };
const source = {
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
const origin = { kind: "agent" as const };
const issuance = {
  schema: "jones-code.workstreams-native-enrollment/v1",
  enrollment_id: "enrollment:test",
  registry_origin: "https://native.workstreams.invalid",
  context: {
    owner_id: Test.scope.owner_id,
    principal_id: "principal:test",
    source_instance_id: source.environment_id,
    authority_namespace: source.authority_namespace,
    store_generation: 1,
    enrollment_id: "enrollment:test",
    protocol: "workstreams-t3-provider/1.0.0",
    build,
  },
  session: {
    session_id: "provider-enrollment:test",
    issued_at: Test.now,
    expires_at: "2026-01-02T00:00:00.000Z",
  },
};
// These are the declared external ports: source owner/authority/build, link-generation
// lookup, thread projections and provider attachment. Auth, credentials, pins, encrypted
// member/operation stores and both readmission services below are their real implementations.
const scenario = () => {
  const parent =
    process.env.NW_SOURCE_READMISSION_EVIDENCE_DIR ??
    NodePath.join(NodeOS.tmpdir(), "nw-source-readmission-evidence");
  NodeFS.mkdirSync(parent, { recursive: true });
  const directory = NodeFS.mkdtempSync(NodePath.join(parent, "fixture-"));
  const state = {
    enforce: false,
    owner: true,
    generation: "opaque-member-to-home:original",
    authority: {
      environmentId: source.environment_id,
      authorityNamespace: source.authority_namespace,
      storeGeneration: 1,
    },
    shell: liveThreadShell(ThreadId.make("caller:original")),
    attached: true,
    unavailableKey: false,
    external: [] as string[],
    ownerCalls: 0,
    keyBarrier: Effect.void as Effect.Effect<void>,
    ownerHook: Effect.void as Effect.Effect<void>,
  };
  const outside = (port: string) =>
    Effect.gen(function* () {
      if (!state.enforce) return;
      state.external.push(port);
      const client = yield* Effect.serviceOption(SqlClient.SqlClient);
      if (Option.isSome(client))
        expect(Option.isSome(yield* Effect.serviceOption(client.value.transactionService))).toBe(
          false,
        );
    });
  const values = Test.secrets();
  values.set("server-signing-key", Test.keyBytes);
  const secrets = Secrets.ServerSecretStore.of({
    get: (name) =>
      outside("secret").pipe(
        Effect.andThen(Effect.suspend(() => (state.enforce ? state.keyBarrier : Effect.void))),
        Effect.andThen(
          Effect.sync(() =>
            Option.fromNullishOr(state.unavailableKey ? undefined : values.get(name)),
          ),
        ),
      ),
    getOrCreateRandom: () => Effect.succeed(Test.keyBytes),
    set: () => Effect.die("unexpected secret write"),
    create: () => Effect.die("unexpected secret create"),
    remove: () => Effect.die("unexpected secret remove"),
  });
  const base = Pins.nativeLayer.pipe(
    Layer.provideMerge(Auth.layer),
    Layer.provideMerge(Sessions.layer),
    Layer.provideMerge(
      Test.testLayer({ filename: NodePath.join(directory, "readmission.sqlite"), secrets }),
    ),
    Layer.provideMerge(Config.layerTest(process.cwd(), NodePath.join(directory, "config"))),
    Layer.provideMerge(
      Layer.succeed(Environment.ServerEnvironmentIdentity, {
        getEnvironmentId: Effect.succeed(EnvironmentId.make(source.environment_id)),
      }),
    ),
    Layer.provideMerge(NodeServices.layer),
  );
  const prepare = Effect.gen(function* () {
    yield* TestClock.setTime(Date.parse(Test.now));
    yield* runMigrations({ toMigrationInclusive: 54 });
    yield* Test.seed;
    yield* migration126;
    yield* migration128;
    yield* migration129;
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TABLE jones_workstreams_readmission_writer_probe(value TEXT)`;
    const auth = yield* Auth.EnvironmentAuth,
      native = yield* Pins.NativePeerSessions,
      sessions = yield* Sessions.AuthSessionRepository;
    const authenticate = Effect.fn("ReadmissionFixture.authenticate")(function* () {
      const issued = yield* auth.issueMcpClientSession({
        label: "synthetic-home",
        access: "full-access",
        peerLink: true,
        client: { deviceType: "bot" },
      });
      const invocation: Mcp.McpInvocationScope = {
        environmentId: EnvironmentId.make(source.environment_id),
        issuedAt: Date.parse(Test.now),
        requestNamespace: "home-lookup:test",
        capabilities: new Set(["orchestration"]),
        thread: undefined,
        client: {
          sessionId: issued.sessionId,
          label: "not-authority",
          access: "full-access",
          linked: true,
        },
      };
      yield* native.authenticate(
        {
          cookies: {},
          headers: { authorization: `Bearer ${issued.token}` },
        } as unknown as Parameters<Auth.EnvironmentAuth["Service"]["authenticateMcpClient"]>[0],
        invocation,
      );
      return { invocation, facts: yield* native.current(invocation) };
    });
    const { invocation, facts } = yield* authenticate();
    const setupBody = (atHome: boolean) =>
      JSON.stringify(
        {
          schema: "jones-workstreams-peer-pin-setup/1",
          assertion: {
            schema: "jones-workstreams-peer-session-pin/1",
            pin_id: atHome ? "home-pin:test" : "member-pin:test",
            revision: 1,
            receiver_authority: atHome ? homeAuthority : source,
            session: atHome
              ? { ...facts.session, session_id: "remote-home-session:test" }
              : facts.session,
            peer_environment_id: atHome ? source.environment_id : home.home_environment_id,
            roles: [atHome ? "member_home" : "home_source"],
            expected_home: home,
            source_authority: source,
            source_enrollment_id: issuance.enrollment_id,
            source_binding_version: null,
            singleton_binding_version: null,
            state: "prepared",
            expires_at: "2026-01-01T00:20:00.000Z",
            setup_request_id: atHome ? "home-setup:test" : "member-setup:test",
            owner_qualification_digest: "c".repeat(64),
            bind_receipt: null,
          },
          expected: { registry_version: 9, grant_id: "grant:test", grant_version: 1 },
        },
        null,
        2,
      ) + "\n";
    const memberBytes = setupBody(false),
      homeBytes = setupBody(true);
    const homePin = PinRecords.decodeSetupRequest(homeBytes).pin;
    const memberPin = PinRecords.decodeSetupRequest(memberBytes).pin;
    const exactIssuance = issuance;
    const body = HomeWire.canonicalWorkstreamsHomeBindBody(
      yield* decodeBindBody({
        bind_id: "bind:test",
        expected_home: home,
        expected_canonical_origin: issuance.registry_origin,
        expected_home_authority: homeAuthority,
        source: { authority: source, enrollment: exactIssuance },
        home_setup: {
          pin_id: homePin.pin_id,
          prepared_revision: 1,
          setup_request_id: homePin.setup_request_id,
          setup_request_sha256: sha(homeBytes),
        },
        member_setup: {
          pin_id: memberPin.pin_id,
          prepared_revision: 1,
          setup_request_id: memberPin.setup_request_id,
          outbound_link_generation: state.generation,
          expected_singleton_version: null,
          singleton_binding_version: 1,
        },
        expected: {
          principal_id: "principal:test",
          registry_version: 9,
          grant_id: "grant:test",
          grant_version: 1,
          source_binding: null,
        },
        rebind: false,
      }),
    );
    const intent = yield* decodeIntent({
      issuance_body: JSON.stringify(exactIssuance, null, 2) + "\n",
      bind_request_body: body,
      bind_request_sha256: sha(body),
      home_setup: {
        contract: HomeWire.WORKSTREAMS_HOME_BIND_CONTRACT,
        kind: "prepared",
        request: {
          contract: HomeWire.WORKSTREAMS_HOME_BIND_CONTRACT,
          expected_home: home,
          home_setup_request_id: homePin.setup_request_id,
        },
        canonical_origin: issuance.registry_origin,
        home_authority: homeAuthority,
        setup_request_body: homeBytes,
        pin: homePin,
      },
      member_setup_request_body: memberBytes,
      member_setup_request_sha256: sha(memberBytes),
    });
    const receipt = {
      principal_id: "principal:test",
      binding_version: 1,
      registry_version: 10,
      request_sha256: sha(body),
    };
    const prepared = yield* decodePrepared({
      schema: MemberWire.WORKSTREAMS_MEMBER_ENROLLMENT_SCHEMA,
      intent,
      intent_sha256: sha(MemberWire.canonicalWorkstreamsMemberEnrollmentIntent(intent)),
      state: "prepared",
      revision: 1,
      home_result: null,
      local_binding: null,
    });
    const bound = yield* decodeBound({
      ...prepared,
      state: "bound",
      revision: 2,
      home_result: {
        contract: HomeWire.WORKSTREAMS_HOME_BIND_CONTRACT,
        kind: "committed",
        request_body: body,
        receipt,
        home_pin_bound_revision: 2,
      },
      local_binding: { pin_bound_revision: 2, singleton_binding_version: 1 },
    });
    // This HttpServer value supplies only registry URL metadata; it never opens a listener.
    const credentials = yield* Credentials.__testing
      .make({ now: () => Date.parse(Test.now), livenessWindowMs: 60_000 })
      .pipe(
        Effect.provideService(HttpServer.HttpServer, {
          address: NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", 43123),
          serve: (() => Effect.void) as HttpServer.HttpServer["Service"]["serve"],
        }),
        Effect.provideService(Environment.ServerEnvironment, {
          getEnvironmentId: Effect.succeed(EnvironmentId.make(source.environment_id)),
          getDescriptor: Effect.die("unused descriptor"),
        }),
      );
    const agent = yield* credentials.issue({
      threadId: state.shell.id,
      providerInstanceId: ProviderInstanceId.make("codex"),
    });
    const scope = yield* credentials.lookupByCredentialId(agent.config.providerSessionId);
    if (!scope) return yield* Effect.die("missing real issued credential");
    const authority = outside("authority").pipe(Effect.andThen(Effect.sync(() => state.authority)));
    const external = Layer.mergeAll(
      Layer.succeed(Credentials.McpSessionRegistry, credentials),
      Layer.mock(Threads.ThreadManagementService)({
        getThreadShell: (id) =>
          outside("thread").pipe(
            Effect.andThen(
              Effect.sync(() => (id === state.shell.id ? state.shell : liveThreadShell(id))),
            ),
          ),
        getThreadRecords: <K extends ProjectionRecordField>() =>
          outside("projection").pipe(
            Effect.andThen(
              Effect.sync(
                () =>
                  ({
                    runs: ["run:mcp-test", "run:replacement"].map((id) => ({
                      id,
                      providerInstanceId: "codex",
                      providerThreadId: "provider-thread:test",
                    })),
                    providerThreads: [
                      {
                        id: "provider-thread:test",
                        providerSessionId: "native-provider-session:test",
                      },
                    ],
                  }) as unknown as ProjectionRecords<K>,
              ),
            ),
          ),
      }),
      Layer.mock(Providers.ProviderSessionManagerV2)({
        isMcpCallerAttached: () =>
          outside("attachment").pipe(Effect.andThen(Effect.sync(() => state.attached))),
      }),
      Layer.mock(Links.PeerLinks)({
        currentGeneration: () =>
          outside("link-generation").pipe(
            Effect.andThen(Effect.sync(() => Option.some(state.generation))),
          ),
        resolve: () => Effect.die("lookup producer must never resolve a network link"),
        recordOutcome: () => Effect.die("lookup producer must never send"),
      }),
      Layer.succeed(Authority.NativeStoreAuthority, {
        readCurrent: authority,
        trustProvider: {
          readTrustSnapshot: () => ({ trustedEnvironments: [state.authority], readiness: "ready" }),
        },
      }),
      Layer.succeed(Source.SourceIssuer, {
        authority: { readCurrent: authority },
        build: Effect.succeed(Option.some(build)),
      }),
      Layer.succeed(Source.SourceSetupOwner, {
        verifyUnchanged: outside("owner").pipe(
          Effect.andThen(
            Effect.sync(() => {
              state.ownerCalls++;
            }),
          ),
          Effect.andThen(Effect.suspend(() => state.ownerHook)),
          Effect.andThen(
            Effect.suspend(() =>
              state.owner
                ? Effect.void
                : Effect.fail(new Source.SourceEnrollmentError({ code: "owner_unqualified" })),
            ),
          ),
        ),
      }),
    );
    const support = Members.layer.pipe(
      Layer.provideMerge(Layer.mergeAll(MemberRecords.layer, PinRecords.layer)),
      Layer.provideMerge(external),
    );
    // Historical bound enrollment is seeded in the genuine encrypted stores. It
    // remains historical: MemberAdmission and PeerSessionPins perform live checks.
    yield* Effect.gen(function* () {
      yield* (yield* MemberRecords.MemberEnrollmentStore).insert(prepared);
      yield* (yield* Members.MemberEnrollments).reserveSession(prepared);
      const records = yield* PinRecords.PeerSessionPinStore;
      yield* records.insert({ pin: memberPin, request_body: memberBytes });
      yield* records.transition(
        {
          ...memberPin,
          state: "bound",
          revision: 2,
          bind_receipt: receipt,
          source_binding_version: 1,
          singleton_binding_version: 1,
        },
        1,
      );
      yield* (yield* Bindings.BindingStore).bind(Test.binding, null);
      yield* (yield* MemberRecords.MemberEnrollmentStore).accept(bound, 1);
    }).pipe(Effect.provide(support));
    const runtime = Readmission.layer.pipe(
      Layer.provideMerge(Pins.layer),
      Layer.provideMerge(Admission.layer.pipe(Layer.provideMerge(Local.layer))),
      Layer.provideMerge(
        Layer.succeed(Startup.Startup, {
          mode: "native-member",
          config: { mode: "native-member", registry: null },
          home: null,
          sourceSessionId: issuance.session.session_id,
          problem: null,
        }),
      ),
      Layer.provideMerge(support),
    );
    const accept = Effect.fn("ReadmissionFixture.accept")(function* (
      kind: "command" | "operation" | "appearance" = "command",
      evidence?: unknown,
    ) {
      const caller = yield* (yield* Admission.MemberAdmission)
        .captureWrite({ kind: "metadata" })
        .pipe(Effect.provideService(Mcp.McpInvocationContext, scope));
      const id = kind + ":source-original-00001";
      const rawCommand =
        JSON.stringify(
          {
            command_id: id,
            expected_server_generation: 1,
            expected_registry_version: 0,
            action: {
              operation: "create_workstream",
              name: "raw-command-sentinel",
              lifecycle: "active",
              progress: { state: "unknown" },
              sort_order: 0,
            },
          },
          null,
          2,
        ) + "\n";
      const rawOperation =
        JSON.stringify(
          {
            contract: "workstream-operations/1.0.0",
            operation_id: id,
            origin,
            kind: "create_workstream",
            registry: {
              registry_id: Test.scope.registry_instance_id,
              owner_id: Test.scope.owner_id,
            },
            workstream: {
              name: "parent-sentinel",
              lifecycle: "active",
              progress: { state: "unknown" },
              sort_order: 0,
            },
          },
          null,
          2,
        ) + "\n";
      const rawAppearance =
        JSON.stringify(
          {
            command_id: id,
            expected_server_generation: 1,
            expected_version: 0,
            workstream_id: "workstream:test",
            border_color: "#AABBCC",
          },
          null,
          2,
        ) + "\n";
      const bytes =
        kind === "command" ? rawCommand : kind === "operation" ? rawOperation : rawAppearance;
      const a = caller.association;
      const identity: Forward.WorkstreamsForwardIdentity = {
        forward_id: "jwf-distinct-forward-00001",
        source_operation_id: id,
        parent_operation_id: null,
        command_ids: [kind === "operation" ? "wsc-home-built-child-00001" : id],
        request_sha256: sha(bytes),
        recorded_caller_sha256: sha(canonical(caller)),
        expected_home: a.home,
        canonical_origin: a.canonical_origin,
        source_authority: a.source,
        source_enrollment_id: a.enrollment_id,
        source_enrollment_session_id: a.enrollment_session_id,
        principal_id: a.principal_id,
        singleton_binding_version: a.singleton_binding_version,
        source_binding_version: a.source_binding_version,
        member_pin_id: a.member_pin_id,
        member_pin_bound_revision: a.pin_bound_revision,
        outbound_link_generation: a.link_generation,
      };
      const common = {
        expected_home: a.home,
        relay: {
          source_environment_id: source.environment_id,
          origin,
          linked_origin_environment_id: null,
          hop: 1,
        },
      };
      const request = yield* decodeForward({
        contract: Forward.WORKSTREAMS_NATIVE_FORWARDING_CONTRACT,
        identity,
        kind,
        ...(kind === "command"
          ? {
              request: {
                ...common,
                body: bytes,
                body_sha256: sha(bytes),
                ...(evidence === undefined ? {} : { evidence }),
              },
            }
          : kind === "operation"
            ? {
                request: {
                  ...common,
                  request: JSON.parse(bytes),
                  request_sha256: sha(bytes),
                  ...(evidence === undefined ? {} : { evidence }),
                },
                request_body: bytes,
              }
            : {
                request: {
                  ...common,
                  request: JSON.parse(bytes),
                  request_body: bytes,
                  request_sha256: sha(bytes),
                },
              }),
      });
      const envelope = JSON.stringify(request, null, 2) + "\n";
      yield* (yield* Operations.OperationStore).acceptMemberForward({
        operation_id: id,
        kind: kind === "operation" ? "create_workstream" : kind,
        thread_environment_id: null,
        thread_id: null,
        origin,
        destination: {
          ...Test.scope,
          home_environment_id: home.home_environment_id,
          home_generation: 1,
          principal_id: "principal:test",
          binding_version: 1,
        },
        child_command_ids: identity.command_ids,
        request_body: bytes,
        accepted_at: Test.now,
        member_forward: {
          schema: "jones-workstreams-member-forward-accepted/1",
          kind: "MEMBER-FORWARD",
          forward_request_body: envelope,
          forward_request_sha256: sha(envelope),
          recorded_caller: caller,
        },
      });
      return {
        caller,
        request,
        envelope,
        bytes,
        parent: {
          contract: Parent.WORKSTREAMS_SOURCE_READMISSION_CONTRACT_V3,
          accepted_forward: identity,
          receiving_home_pin_id: a.home_pin_id,
          receiving_home_pin_revision: a.home_pin_bound_revision,
        } satisfies Parent.WorkstreamsSourceParentReadmissionRequestV3,
        raw: {
          contract: Raw.WORKSTREAMS_SOURCE_READMISSION_CONTRACT_V2,
          accepted_forward: identity,
          command_id: identity.command_ids[0]!,
          command_request_sha256: sha(bytes),
        } satisfies Raw.WorkstreamsSourceReadmissionRequestV2,
      };
    });
    const counts = Effect.gen(function* () {
      const operations =
        yield* sql`SELECT operation_id,state,request_sha256,request_envelope,result_envelope FROM jones_workstreams_operations ORDER BY operation_id`;
      const effects = yield* sql`SELECT * FROM jones_workstreams_external_reservations`;
      const results = yield* sql`SELECT * FROM jones_workstreams_external_results`;
      const receipts = yield* sql`SELECT * FROM jones_workstreams_receipts`;
      const authRows = yield* sql`SELECT * FROM auth_sessions ORDER BY session_id`;
      return { operations, effects, results, receipts, authRows };
    });
    state.ownerCalls = 0;
    return {
      sql,
      sessions,
      invocation,
      authenticate,
      credentials,
      scope,
      state,
      runtime,
      accept,
      counts,
      directory,
    };
  });
  return { state, base, prepare };
};

it.effect(
  "looks up only encrypted accepted source records, echoing exact requests/bytes and narrowing ceilings",
  () => {
    const s = scenario();
    return Effect.gen(function* () {
      const f = yield* s.prepare;
      yield* Effect.gen(function* () {
        const r = yield* f.accept();
        const producer = yield* Readmission.SourceReadmission;
        const before = yield* f.counts;
        f.state.enforce = true;
        const parent = yield* producer.lookupParent(r.parent, f.invocation);
        expect(parent).toMatchObject({
          state: "authorized",
          request: r.parent,
          forward_request_sha256: sha(r.envelope),
        });
        const raw = yield* producer.lookupRaw(r.raw, f.invocation);
        expect(raw).toMatchObject({ state: "authorized", request: r.raw });
        expect(raw).not.toHaveProperty("home_authorization_revision");
        expect(Object.keys(producer).sort()).toEqual(["lookupParent", "lookupRaw"]);
        expect(yield* f.counts).toEqual(before);
        expect(f.state.external).toEqual(
          expect.arrayContaining([
            "secret",
            "owner",
            "authority",
            "projection",
            "attachment",
            "link-generation",
          ]),
        );
        const stored = yield* f.sql<{
          request_envelope: string;
        }>`SELECT request_envelope FROM jones_workstreams_operations`;
        expect(stored[0]?.request_envelope).not.toContain("raw-command-sentinel");
        expect(
          yield* producer.lookupRaw(
            { ...r.raw, command_request_sha256: sha(canonical(JSON.parse(r.bytes))) },
            f.invocation,
          ),
        ).toMatchObject({ state: "denied", reason: "request_conflict" });
        expect(
          yield* producer.lookupParent(
            {
              ...r.parent,
              accepted_forward: {
                ...r.parent.accepted_forward,
                source_operation_id: r.parent.accepted_forward.forward_id,
              },
            },
            f.invocation,
          ),
        ).toMatchObject({ state: "denied", reason: "not_found" });
      }).pipe(Effect.provide(f.runtime));
    }).pipe(Effect.provide(s.base));
  },
);

it.effect.each(["operation", "appearance"] as const)(
  "V3 qualifies retained %s parents; V2 cannot authorize their children",
  (kind) => {
    const s = scenario();
    return Effect.gen(function* () {
      const f = yield* s.prepare;
      yield* Effect.gen(function* () {
        const r = yield* f.accept(kind),
          producer = yield* Readmission.SourceReadmission;
        expect(yield* producer.lookupParent(r.parent, f.invocation)).toMatchObject({
          state: "authorized",
          forward_request_sha256: sha(r.envelope),
        });
        expect(yield* producer.lookupRaw(r.raw, f.invocation)).toMatchObject({
          state: "denied",
          reason: "request_conflict",
        });
      }).pipe(Effect.provide(f.runtime));
    }).pipe(Effect.provide(s.base));
  },
);

it.effect.each([
  "home-id",
  "home-revision",
  "member-id",
  "member-revision",
  "identity",
  "reconstructed-invocation",
  "relink",
] as const)("refuses %s substitution without writes", (change) => {
  const s = scenario();
  return Effect.gen(function* () {
    const f = yield* s.prepare;
    yield* Effect.gen(function* () {
      const r = yield* f.accept(),
        producer = yield* Readmission.SourceReadmission;
      let request = r.parent,
        invocation = f.invocation;
      if (change === "home-id")
        request = { ...request, receiving_home_pin_id: r.caller.association.member_pin_id };
      if (change === "home-revision")
        request = {
          ...request,
          receiving_home_pin_revision: request.receiving_home_pin_revision + 1,
        };
      if (change === "member-id")
        request = {
          ...request,
          accepted_forward: {
            ...request.accepted_forward,
            member_pin_id: r.caller.association.home_pin_id,
          },
        };
      if (change === "member-revision")
        request = {
          ...request,
          accepted_forward: { ...request.accepted_forward, member_pin_bound_revision: 8 },
        };
      if (change === "identity")
        request = {
          ...request,
          accepted_forward: {
            ...request.accepted_forward,
            parent_operation_id: "parent:replacement-00001",
          },
        };
      if (change === "reconstructed-invocation") invocation = { ...invocation };
      if (change === "relink") invocation = (yield* f.authenticate()).invocation;
      const before = yield* f.counts;
      expect((yield* producer.lookupParent(request, invocation)).state).toBe("denied");
      expect(yield* f.counts).toEqual(before);
    }).pipe(Effect.provide(f.runtime));
  }).pipe(Effect.provide(s.base));
});

it.effect.each([
  "caller-replacement",
  "credential-revocation",
  "peer-revocation",
  "source-revocation",
  "generation",
  "authority",
  "singleton",
  "owner",
  "plan",
  "key-unavailable",
] as const)("fresh readmission fences %s", (change) => {
  const s = scenario();
  return Effect.gen(function* () {
    const f = yield* s.prepare;
    yield* Effect.gen(function* () {
      const r = yield* f.accept(),
        producer = yield* Readmission.SourceReadmission;
      if (change === "caller-replacement")
        f.state.shell = { ...f.state.shell, activeRunId: RunId.make("run:replacement") };
      if (change === "credential-revocation") yield* f.credentials.revokeThread(f.state.shell.id);
      if (change === "peer-revocation" || change === "source-revocation")
        yield* f.sessions.revoke({
          sessionId: AuthSessionId.make(
            change === "peer-revocation"
              ? f.invocation.client!.sessionId
              : issuance.session.session_id,
          ),
          revokedAt: DateTime.makeUnsafe(Test.now),
        });
      if (change === "generation") f.state.generation = "relinked:other-generation";
      if (change === "authority") f.state.authority = { ...f.state.authority, storeGeneration: 2 };
      if (change === "singleton")
        yield* f.sql`UPDATE jones_workstreams_binding_state SET binding_version=2`;
      if (change === "owner") f.state.owner = false;
      if (change === "plan") f.state.shell = { ...f.state.shell, interactionMode: "plan" };
      if (change === "key-unavailable") f.state.unavailableKey = true;
      expect((yield* producer.lookupParent(r.parent, f.invocation)).state).not.toBe("authorized");
      expect((yield* producer.lookupRaw(r.raw, f.invocation)).state).not.toBe("authorized");
    }).pipe(Effect.provide(f.runtime));
  }).pipe(Effect.provide(s.base));
});

it.effect(
  "current ceilings narrow an accepted agent without adopting its replacement run or expanding scope",
  () => {
    const s = scenario();
    return Effect.gen(function* () {
      const f = yield* s.prepare;
      yield* Effect.gen(function* () {
        const r = yield* f.accept(),
          producer = yield* Readmission.SourceReadmission;
        f.state.shell = { ...f.state.shell, runtimeMode: "approval-required" };
        const response = yield* producer.lookupParent(r.parent, f.invocation);
        expect(response.state).toBe("authorized");
        if (response.state !== "authorized")
          return yield* Effect.die("expected narrowed observation");
        expect(response.current_ceiling.runtime_mode).toBe("approval-required");
        expect(response.current_ceiling.access).toBe("approval-required");
        expect(
          response.current_ceiling.scopes.every((scope) =>
            response.original_ceiling.scopes.includes(scope),
          ),
        ).toBe(true);
        expect(
          response.current_ceiling.permissions.every((permission) =>
            response.original_ceiling.permissions.includes(permission),
          ),
        ).toBe(true);
      }).pipe(Effect.provide(f.runtime));
    }).pipe(Effect.provide(s.base));
  },
);

it.effect.each(["home-session", "source-session", "caller"] as const)(
  "checks original %s again after preparation",
  (change) => {
    const s = scenario();
    return Effect.gen(function* () {
      const f = yield* s.prepare;
      yield* Effect.gen(function* () {
        const r = yield* f.accept(),
          producer = yield* Readmission.SourceReadmission;
        f.state.ownerCalls = 0;
        f.state.ownerHook = Effect.suspend(() =>
          f.state.ownerCalls === 2
            ? change === "caller"
              ? Effect.sync(() => {
                  f.state.shell = { ...f.state.shell, activeRunId: RunId.make("run:replacement") };
                })
              : f.sessions
                  .revoke({
                    sessionId: AuthSessionId.make(
                      change === "home-session"
                        ? f.invocation.client!.sessionId
                        : issuance.session.session_id,
                    ),
                    revokedAt: DateTime.makeUnsafe(Test.now),
                  })
                  .pipe(Effect.orDie)
            : Effect.void,
        );
        expect((yield* producer.lookupParent(r.parent, f.invocation)).state).toBe("denied");
      }).pipe(Effect.provide(f.runtime));
    }).pipe(Effect.provide(s.base));
  },
);

it.effect(
  "rejects an actual ambient transaction before external preparation and lets unrelated writers pass a key wait",
  () => {
    const s = scenario();
    return Effect.gen(function* () {
      const f = yield* s.prepare;
      yield* Effect.gen(function* () {
        const r = yield* f.accept(),
          producer = yield* Readmission.SourceReadmission;
        f.state.enforce = true;
        const seen = f.state.external.length;
        expect(
          yield* f.sql
            .withTransaction(producer.lookupParent(r.parent, f.invocation))
            .pipe(Effect.result),
        ).toMatchObject({ _tag: "Failure", failure: { code: "ambient_transaction" } });
        expect(
          yield* f.sql.withTransaction(producer.lookupRaw(r.raw, f.invocation)).pipe(Effect.result),
        ).toMatchObject({ _tag: "Failure", failure: { code: "ambient_transaction" } });
        expect(f.state.external.length).toBe(seen);
        const entered = yield* Deferred.make<void>(),
          release = yield* Deferred.make<void>();
        f.state.keyBarrier = Deferred.succeed(entered, undefined).pipe(
          Effect.andThen(Deferred.await(release)),
        );
        const lookup = yield* producer.lookupParent(r.parent, f.invocation).pipe(Effect.forkChild);
        yield* Deferred.await(entered);
        yield* f.sql.withTransaction(
          f.sql`INSERT INTO jones_workstreams_readmission_writer_probe VALUES ('unrelated-commit')`,
        );
        expect(
          (yield* f.sql`SELECT * FROM jones_workstreams_readmission_writer_probe`).length,
        ).toBe(1);
        yield* Deferred.succeed(release, undefined);
        expect((yield* Fiber.join(lookup)).state).toBe("authorized");
      }).pipe(Effect.provide(f.runtime));
    }).pipe(Effect.provide(s.base));
  },
);

it.effect("a source observation is not a distributed revocation lock", () => {
  const s = scenario();
  return Effect.gen(function* () {
    const f = yield* s.prepare;
    yield* Effect.gen(function* () {
      const r = yield* f.accept(),
        producer = yield* Readmission.SourceReadmission;
      const observed = yield* producer.lookupParent(r.parent, f.invocation);
      yield* f.sessions.revoke({
        sessionId: AuthSessionId.make(issuance.session.session_id),
        revokedAt: DateTime.makeUnsafe(Test.now),
      });
      expect(observed.state).toBe("authorized");
      expect((yield* producer.lookupParent(r.parent, f.invocation)).state).not.toBe("authorized");
    }).pipe(Effect.provide(f.runtime));
  }).pipe(Effect.provide(s.base));
});

it.effect(
  "V2 refuses raw supplied evidence while V3 hashes the entire retained envelope including evidence",
  () => {
    const s = scenario();
    return Effect.gen(function* () {
      const f = yield* s.prepare;
      yield* Effect.gen(function* () {
        const evidence = {
          protocol: "workstreams-t3-provider/2.0.0",
          state: "attested",
          request: {
            owner_id: home.owner_id,
            principal_id: "principal:test",
            identity: {
              provider: "t3",
              source_instance_id: source.environment_id,
              native_id: "thread:test",
              resource_kind: "thread",
              id_kind: "internal",
              account_provenance: { kind: "not_account_scoped" },
            },
            expected_authority_namespace: source.authority_namespace,
            expected_store_generation: 1,
          },
          attestation: {
            provider: "t3",
            source_instance_id: source.environment_id,
            native_id: "thread:test",
            authority_namespace: source.authority_namespace,
            store_generation: 1,
            evidence_sha256: "e".repeat(64),
          },
          attested_at: Test.now,
          expires_at: "2026-01-01T00:05:00.000Z",
        };
        const r = yield* f.accept("command", evidence),
          producer = yield* Readmission.SourceReadmission;
        const observation = yield* producer.lookupParent(r.parent, f.invocation);
        expect(observation).toMatchObject({
          state: "authorized",
          forward_request_sha256: sha(r.envelope),
        });
        const replacement = JSON.parse(r.envelope) as Forward.WorkstreamsForwardRequest;
        if (replacement.kind !== "command" || replacement.request.evidence === undefined)
          return yield* Effect.die("fixture lost supplied evidence");
        const replaced = {
          ...replacement,
          request: {
            ...replacement.request,
            evidence: {
              ...replacement.request.evidence,
              attestation: {
                ...replacement.request.evidence.attestation,
                native_id: "replacement-thread",
              },
            },
          },
        };
        expect(sha(JSON.stringify(replaced, null, 2) + "\n")).not.toBe(sha(r.envelope));
        expect(yield* producer.lookupRaw(r.raw, f.invocation)).toMatchObject({
          state: "denied",
          reason: "request_conflict",
        });
      }).pipe(Effect.provide(f.runtime));
    }).pipe(Effect.provide(s.base));
  },
);

it.effect(
  "closed explicit versions reject caller JSON and never negotiate V1 as dispatch authority",
  () => {
    const s = scenario();
    return Effect.gen(function* () {
      const f = yield* s.prepare;
      yield* Effect.gen(function* () {
        const r = yield* f.accept(),
          producer = yield* Readmission.SourceReadmission;
        expect(
          yield* producer
            .lookupParent(
              {
                ...r.parent,
                caller: r.caller,
              } as Parent.WorkstreamsSourceParentReadmissionRequestV3,
              f.invocation,
            )
            .pipe(Effect.result),
        ).toMatchObject({ _tag: "Failure", failure: { code: "invalid_request" } });
        expect(
          yield* producer
            .lookupRaw(
              {
                ...r.raw,
                contract: "workstreams-source-readmission/1.0.0",
              } as unknown as Raw.WorkstreamsSourceReadmissionRequestV2,
              f.invocation,
            )
            .pipe(Effect.result),
        ).toMatchObject({ _tag: "Failure", failure: { code: "invalid_request" } });
        expect(
          yield* producer.lookupParent(
            {
              ...r.parent,
              accepted_forward: {
                ...r.parent.accepted_forward,
                recorded_caller_sha256: "0".repeat(64),
              },
            },
            f.invocation,
          ),
        ).toMatchObject({ state: "denied", reason: "request_conflict" });
      }).pipe(Effect.provide(f.runtime));
    }).pipe(Effect.provide(s.base));
  },
);

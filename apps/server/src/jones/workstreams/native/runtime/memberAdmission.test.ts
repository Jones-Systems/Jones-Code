// @effect-diagnostics nodeBuiltinImport:off -- Isolated test configuration is scoped; explicitly requested evidence is retained.
// @effect-diagnostics preferSchemaOverJson:off -- Tests deliberately retain and corrupt exact wire JSON bytes.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { expect, it } from "@effect/vitest";
import {
  AuthSessionId,
  EnvironmentId,
  EnvironmentAuthenticatedPrincipal,
  AuthOrchestrationReadScope,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/sql/SqlClient";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import * as HomeWire from "@t3tools/contracts/jones/workstreamsHomeBind";
import * as MemberWire from "@t3tools/contracts/jones/workstreamsMemberEnrollment";
import { sha256EnrollmentBytes as sha } from "../../enrollment/request.ts";
import * as Auth from "../../../../auth/EnvironmentAuth.ts";
import * as Secrets from "../../../../auth/ServerSecretStore.ts";
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
const decodeUnknownSyncHomeWireWorkstreamsHomeBindCommittedV2 = Schema.decodeUnknownSync(
  HomeWire.WorkstreamsHomeBindCommittedV2,
);
const configLayer = Layer.unwrap(
  Effect.acquireRelease(
    Effect.sync(() =>
      NodeFS.mkdtempSync(
        NodePath.join(
          process.env.NW_MEMBER_ADMISSION_EVIDENCE_DIR ?? NodeOS.tmpdir(),
          "nw-member-config-",
        ),
      ),
    ),
    (directory) =>
      Effect.sync(() => {
        if (process.env.NW_MEMBER_ADMISSION_EVIDENCE_DIR === undefined)
          NodeFS.rmSync(directory, { recursive: true, force: true });
      }),
  ).pipe(Effect.map((directory) => Config.layerTest(process.cwd(), directory))),
);
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
const makeIntent = (
  session: unknown = syntheticSession,
  outboundCeiling = syntheticSession.ceiling,
) => {
  const assertion = (atHome: boolean) => ({
    schema: "jones-workstreams-peer-session-pin/1",
    pin_id: atHome ? "home-pin:test" : "member-pin:test",
    revision: 1,
    receiver_authority: atHome ? homeAuthority : sourceAuthority,
    session: atHome
      ? { ...syntheticSession, ceiling: outboundCeiling, session_id: "home-link:test" }
      : session,
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
    Layer.provideMerge(configLayer),
    Layer.provideMerge(
      Layer.succeed(Environment.ServerEnvironmentIdentity, {
        getEnvironmentId: Effect.succeed(EnvironmentId.make("member:test")),
      }),
    ),
    Layer.provideMerge(NodeServices.layer),
  );
const makeFixture = (outboundCeiling = syntheticSession.ceiling) =>
  Effect.gen(function* () {
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
    const intent = makeIntent(session, outboundCeiling);
    const state = {
      generation: "opaque-link-generation:1",
      resolvedGeneration: "opaque-link-generation:1",
      owner: true,
      sourceGeneration: 1,
      sourceNamespace: "member-store",
      currentBuild: { ...build },
      committed: true,
      loseBindReply: false,
      wrongReadback: false,
      wrongSetup: false,
      unsupported: false,
      afterReadback: Effect.void as Effect.Effect<void>,
      afterBind: Effect.void as Effect.Effect<void>,
      beforeReadback: Effect.void as Effect.Effect<void>,
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
          authorityNamespace: state.sourceNamespace,
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
    const dependencies = Layer.mergeAll(
      linkLayer,
      issuer,
      owner,
      port,
      Layer.succeed(Authority.NativeStoreAuthority, {
        readCurrent: Effect.sync(() => ({
          environmentId: "member:test",
          authorityNamespace: state.sourceNamespace,
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

const fixture = makeFixture();

import * as Admission from "./memberAdmission.ts";
import * as Local from "./memberRecordedCaller.ts";
import * as Startup from "./startup.ts";

// Real M writer/store/session/pin/singleton and local session adapters; remote H is synthetic.
it.effect.each([
  "none",
  "link",
  "source",
  "singleton",
  "provider-session",
  "peer-session",
  "caller-session",
  "namespace",
  "pin-expired",
] as const)(
  "member admission consumes encrypted validated M association and fences %s drift",
  (change) =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* Effect.gen(function* () {
        yield* (yield* Setup.MemberSetup).apply(f.intent);
        const principal = {
          sessionId: AuthSessionId.make("local-owner"),
          subject: "owner",
          method: "bearer-access-token" as const,
          scopes: new Set([AuthOrchestrationReadScope]),
        };
        yield* f.sessions.create({
          ...principal,
          scopes: [...principal.scopes],
          issuedAt: DateTime.makeUnsafe(Test.now),
          expiresAt: DateTime.makeUnsafe("2026-01-02T00:00:00.000Z"),
          client: {
            label: null,
            ipAddress: null,
            userAgent: null,
            deviceType: "bot",
            os: null,
            browser: null,
          },
        });
        const selected: Startup.Selection = {
          mode: "native-member",
          config: { mode: "native-member", registry: null },
          home: null,
          sourceSessionId: issuance.session.session_id,
          problem: null,
        };
        const layers = Admission.layer.pipe(
          Layer.provide(Local.layer),
          Layer.provide(Layer.succeed(Startup.Startup, selected)),
        );
        yield* Effect.gen(function* () {
          const admission = yield* Admission.MemberAdmission;
          const record = yield* admission.capture();
          const current = yield* admission.readmit(record);
          expect(record.association).toMatchObject({
            principal_id: "principal:test",
            source: sourceAuthority,
            link_generation: "opaque-link-generation:1",
            singleton_binding_version: 1,
          });
          expect(record).not.toHaveProperty("grant_version");
          expect(current.limits).toEqual({
            runtimeMode: "approval-required",
            interactionMode: "default",
          });
          if (change === "none") {
            expect(
              yield* admission
                .readmit({
                  ...record,
                  association: { ...record.association, principal_id: "other" },
                })
                .pipe(Effect.result),
            ).toMatchObject({ _tag: "Failure" });
            expect(yield* admission.readmit({ version: 1 }).pipe(Effect.result)).toMatchObject({
              _tag: "Failure",
            });
            const sql = yield* SqlClient.SqlClient;
            expect((yield* sql`SELECT * FROM jones_workstreams_principals`).length).toBe(0);
            expect((yield* sql`SELECT * FROM jones_workstreams_grants`).length).toBe(0);
            const rows = yield* sql<{
              envelope: string;
            }>`SELECT envelope FROM jones_workstreams_member_enrollments`;
            expect(rows[0]?.envelope).not.toContain("principal:test");
            return;
          }
          if (change === "link") f.state.generation = "relinked";
          if (change === "source") f.state.sourceGeneration = 2;
          if (change === "namespace") f.state.sourceNamespace = "replacement-store";
          if (change === "pin-expired")
            yield* TestClock.setTime(Date.parse("2026-01-01T00:20:00.000Z"));
          if (change === "singleton")
            yield* f.sql`UPDATE jones_workstreams_binding_state SET binding_version=2`;
          if (change.endsWith("session"))
            yield* f.sessions.revoke({
              sessionId: AuthSessionId.make(
                change === "provider-session"
                  ? issuance.session.session_id
                  : change === "peer-session"
                    ? f.invocation.client!.sessionId
                    : principal.sessionId,
              ),
              revokedAt: DateTime.makeUnsafe(Test.now),
            });
          expect(yield* current.checkCurrent().pipe(Effect.result)).toMatchObject({
            _tag: "Failure",
          });
        }).pipe(
          Effect.provide(layers),
          Effect.provideService(EnvironmentAuthenticatedPrincipal, principal),
        );
      }).pipe(Effect.provide(f.mounted));
    }).pipe(Effect.provide(baseServices())),
);

import { AuthOrchestrationOperateScope } from "@t3tools/contracts";
it.effect.each([
  "none",
  "read-only-local",
  "read-only-outbound",
  "plan-outbound",
  "caller-revoked",
  "caller-expired",
  "provider-session",
  "peer-session",
  "pin-expired",
  "link",
  "source",
  "singleton",
  "selected-member",
  "association",
] as const)("write admission V2 uses genuine M and fences %s changes", (change) =>
  Effect.gen(function* () {
    const outbound =
      change === "read-only-outbound"
        ? {
            ...syntheticSession.ceiling,
            access: "read-only",
            runtime_mode: "approval-required",
            permissions: ["workstreams:read"],
            scopes: ["orchestration:read"],
          }
        : change === "plan-outbound"
          ? { ...syntheticSession.ceiling, interaction_mode: "plan" }
          : syntheticSession.ceiling;
    const f = yield* makeFixture(outbound);
    yield* Effect.gen(function* () {
      yield* (yield* Setup.MemberSetup).apply(f.intent);
      const principal = {
        sessionId: AuthSessionId.make("write-local-owner"),
        subject: "owner",
        method: "bearer-access-token" as const,
        scopes: new Set(
          change === "read-only-local"
            ? [AuthOrchestrationReadScope]
            : [AuthOrchestrationReadScope, AuthOrchestrationOperateScope],
        ),
      };
      yield* f.sessions.create({
        ...principal,
        scopes: [...principal.scopes],
        issuedAt: DateTime.makeUnsafe(Test.now),
        expiresAt: DateTime.makeUnsafe("2026-01-01T00:10:00.000Z"),
        client: {
          label: null,
          ipAddress: null,
          userAgent: null,
          deviceType: "bot",
          os: null,
          browser: null,
        },
      });
      const selected = {
        mode: "native-member" as const,
        config: { mode: "native-member" as const, registry: null },
        home: null,
        sourceSessionId: issuance.session.session_id,
        problem: null,
      } satisfies Startup.Selection;
      const layers = Admission.layer.pipe(
        Layer.provide(Local.layer),
        Layer.provide(Layer.succeed(Startup.Startup, selected)),
      );
      yield* Effect.gen(function* () {
        const admission = yield* Admission.MemberAdmission;
        if (["read-only-local", "read-only-outbound", "plan-outbound"].includes(change)) {
          expect(
            yield* admission.captureWrite({ kind: "metadata" }).pipe(Effect.result),
          ).toMatchObject({ _tag: "Failure" });
          return;
        }
        const record = yield* admission.captureWrite({ kind: "metadata" });
        const admitted = yield* admission.readmitWrite(record);
        expect(record.schema).toBe("jones-workstreams-member-recorded-write-caller/2");
        expect(record.association).toMatchObject({
          home_pin_id: "home-pin:test",
          member_pin_id: "member-pin:test",
          home_pin_bound_revision: 2,
        });
        expect(yield* admission.readmit(record).pipe(Effect.result)).toMatchObject({
          _tag: "Failure",
        });
        if (change === "none") {
          yield* (yield* admission.readmitWriteRetained(record)).checkCurrent();
          const read = yield* admission.capture();
          expect(yield* admission.readmitWrite(read).pipe(Effect.result)).toMatchObject({
            _tag: "Failure",
          });
          expect((yield* f.sql`SELECT * FROM jones_workstreams_grants`).length).toBe(0);
          expect((yield* f.sql`SELECT * FROM jones_workstreams_principals`).length).toBe(0);
          return;
        }
        if (change === "association") {
          for (const association of [
            { ...record.association, principal_id: "other" },
            { ...record.association, source_binding_version: 2 },
            { ...record.association, singleton_binding_version: 2 },
            { ...record.association, home_pin_id: "other" },
            { ...record.association, member_pin_id: "other" },
            {
              ...record.association,
              home: { ...record.association.home, home_environment_id: "other" },
            },
            { ...record.association, enrollment_id: "other" },
          ])
            expect(
              yield* admission.readmitWrite({ ...record, association }).pipe(Effect.result),
            ).toMatchObject({ _tag: "Failure" });
          return;
        }
        if (change === "caller-revoked")
          yield* f.sessions.revoke({
            sessionId: principal.sessionId,
            revokedAt: DateTime.makeUnsafe(Test.now),
          });
        if (change === "caller-expired")
          yield* TestClock.setTime(Date.parse("2026-01-01T00:10:00.000Z"));
        if (change === "pin-expired")
          yield* TestClock.setTime(Date.parse("2026-01-01T00:20:00.000Z"));
        if (change === "provider-session" || change === "peer-session")
          yield* f.sessions.revoke({
            sessionId: AuthSessionId.make(
              change === "provider-session"
                ? issuance.session.session_id
                : f.invocation.client!.sessionId,
            ),
            revokedAt: DateTime.makeUnsafe(Test.now),
          });
        if (change === "link") f.state.generation = "relinked";
        if (change === "source") f.state.sourceGeneration = 2;
        if (change === "singleton")
          yield* f.sql`UPDATE jones_workstreams_binding_state SET binding_version=2`;
        if (change === "selected-member") selected.sourceSessionId = "replacement-session";
        expect(yield* admitted.checkCurrent().pipe(Effect.result)).toMatchObject({
          _tag: "Failure",
        });
        expect(yield* admission.readmitWriteRetained(record).pipe(Effect.result)).toMatchObject({
          _tag: "Failure",
        });
      }).pipe(
        Effect.provide(layers),
        Effect.provideService(EnvironmentAuthenticatedPrincipal, principal),
      );
    }).pipe(Effect.provide(f.mounted));
  }).pipe(Effect.provide(baseServices())),
);

// @effect-diagnostics nodeBuiltinImport:off -- Two isolated file-backed homes and all fixture evidence are retained for root cleanup.
// @effect-diagnostics preferSchemaOverJson:off -- This external HTTP transport preserves/corrupts exact JSON bytes intentionally.
// @effect-diagnostics effectFnOpportunity:off -- Test-local harness callbacks are framework boundaries.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeHttp from "node:http";
import * as NodeNet from "node:net";
import { expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import {
  EnvironmentId,
  EnvironmentAuthenticatedPrincipal,
  RuntimeMode,
  ProviderInteractionMode,
} from "@t3tools/contracts";
import * as W from "@t3tools/contracts/jones/workstreamsPeerSetup";
import * as F from "@t3tools/contracts/jones/workstreamsHomeBind";
import * as ForwardWire from "@t3tools/contracts/jones/workstreamsForwarding";
import * as ParentWire from "@t3tools/contracts/jones/workstreamsForwardQualification";
import * as RawWire from "@t3tools/contracts/jones/workstreamsForwardReadmission";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeSqlite from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Context from "effect/Context";
import * as Option from "effect/Option";
import * as Scope from "effect/Scope";
import * as Exit from "effect/Exit";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import type * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as DateTime from "effect/DateTime";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/sql/SqlClient";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpResponse from "effect/http/HttpClientResponse";
import * as HttpRequest from "effect/http/HttpServerRequest";
import * as ServerResponse from "effect/http/HttpServerResponse";
import * as Cli from "../runtime/peerSetupCli.ts";
import * as Auth from "../../../../auth/EnvironmentAuth.ts";
import * as Sessions from "../../../../auth/SessionStore.ts";
import * as Secrets from "../../../../auth/ServerSecretStore.ts";
import * as AuthRows from "../../../../persistence/AuthSessions.ts";
import * as Config from "../../../../config.ts";
import * as Environment from "../../../../environment/ServerEnvironment.ts";
import * as Authority from "../../../../environment/NativeStoreAuthority.ts";
import { runMigrations } from "../../../../persistence/Migrations.ts";
import m125 from "../../../persistence/Migrations/125_JonesWorkstreamsNative.ts";
import m126 from "../../../persistence/Migrations/126_JonesWorkstreamsSourceEnrollments.ts";
import m128 from "../../../persistence/Migrations/128_JonesWorkstreamsPeerSessionPins.ts";
import m129 from "../../../persistence/Migrations/129_JonesWorkstreamsMemberBindings.ts";
import m130 from "../../../persistence/Migrations/130_JonesWorkstreamsForwardReceipts.ts";
import * as MemberApp from "../runtime/memberApplication.ts";
import * as Forwarder from "../forwarding/forwarder.ts";
import * as SourcePort from "../forwarding/sourceReadmissionPort.ts";
import * as HomePort from "../forwarding/homeForwardPort.ts";
import * as SourceQualification from "../forwarding/sourceForwardQualification.ts";
import * as Receiving from "../forwarding/receivingAuthority.ts";
import * as OperationStore from "../store/operationStore.ts";
import * as NativeSession from "../registry/session.ts";
import { NativeHome } from "../mode/authorityMode.ts";
import * as NativeForwards from "../../../mcp/workstreams/nativeForwards.ts";
import * as Runtime from "../../runtimeIntegration/native.ts";
import * as ProviderTest from "../../nativeProvider/testFixtures.ts";
import { makeWorkstreamsNativeProvider } from "../../nativeProvider/service.ts";
import { NativeProviderEnrollmentError } from "../../nativeProvider/enrollment.ts";
import * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import * as Access from "../../../../mcp/McpToolAccess.ts";
import { ThreadManagementService } from "../../../../orchestration-v2/ThreadManagementService.ts";
import * as PeerLinks from "../../../../peer/PeerLinks.ts";
import * as PeerClient from "../../../../peer/PeerMcpClient.ts";
import * as Tools from "../../../mcp/workstreams/nativeSetups.ts";
import * as BindTools from "../../../mcp/workstreams/nativeBinds.ts";
import { syntheticT3Home } from "../../enrollment/testHome.ts";
import * as Test from "../store/sql.testkit.ts";
import * as Registry from "../store/registryStore.ts";
import * as PinRecords from "../store/peerSessionPinStore.ts";
import * as MemberRecords from "../store/memberEnrollmentStore.ts";
import * as Bindings from "../store/bindingStore.ts";
import * as Database from "../store/sql.ts";
import * as Envelope from "../crypto/envelope.ts";
import * as Keys from "../crypto/keyProvider.ts";
import * as Owner from "../runtime/setupOwner.ts";
import * as Setup from "../runtime/peerSetup.ts";
import * as Files from "../runtime/peerSetupRequestFile.ts";
import * as Native from "../runtime/peerSessionPins.ts";
import * as NativeAuth from "../runtime/peerMcpAuthentication.ts";
import * as Members from "../runtime/memberEnrollment.ts";
import * as Home from "../runtime/homeBind.ts";
import * as Startup from "../runtime/startup.ts";
import { selectMember } from "../runtime/memberStartup.ts";
import * as Admission from "../runtime/memberAdmission.ts";
import * as Callers from "../runtime/memberRecordedCaller.ts";
import * as MemberPreparation from "../runtime/memberPreparation.ts";
import * as CurrentFixture from "../runtime/currentAuthority.testkit.ts";
import { RegistryError } from "../registry/session.ts";

vi.mock("../../../../../package.json", () => ({
  default: {
    version: "0.0.0-preview.20261002.100",
    jonesSource: {
      repository: "Jones-Systems/Jones-Code",
      sha: "a".repeat(40),
      tree: "b".repeat(40),
    },
  },
}));
const home = { ...Test.scope, home_environment_id: "home:test", server_generation: 1 };
const address: W.WorkstreamsPeerSetupAddress = {
  setup_id: "setup:test",
  source_environment_id: "member:test",
  expected_home: home,
};
const threads = Layer.mock(ThreadManagementService)({ getThreadShell: () => Effect.succeed(null) });
const mode = Schema.decodeUnknownSync(
  Schema.Struct({ runtimeMode: RuntimeMode, interactionMode: ProviderInteractionMode }),
);

const decodeForwardArgs = Schema.decodeUnknownEffect(ForwardWire.WorkstreamsForwardRequest);
const decodeReadbackArgs = Schema.decodeUnknownEffect(
  ForwardWire.WorkstreamsForwardReadbackRequest,
);
const decodeSourceArgs = Schema.decodeUnknownEffect(
  Schema.Union([
    ParentWire.WorkstreamsSourceParentReadmissionRequestV3,
    RawWire.WorkstreamsSourceReadmissionRequestV2,
  ]),
);

const fixture = Effect.gen(function* () {
  yield* TestClock.setTime(Date.parse(Test.now));
  const retention = yield* Scope.make();
  const originalIssues = new Map<
    string,
    Effect.Success<ReturnType<Auth.EnvironmentAuth["Service"]["issueMcpClientSession"]>>
  >();
  const controls = {
    beforeSecret: (_env: string, _name: string): Effect.Effect<void> => Effect.void,
  };
  const makeEnvironment = (
    env: string,
    existing?: import("../../enrollment/testHome.ts").SyntheticT3Home,
  ) =>
    Effect.gen(function* () {
      const f =
        existing ??
        (yield* syntheticT3Home("nw-combined-" + env.split(":")[0] + "-").pipe(
          Effect.provideService(Scope.Scope, retention),
        ));
      if (!existing) {
        NodeFS.writeFileSync(f.environmentPath, env + "\n");
        const authority = JSON.parse(NodeFS.readFileSync(f.authorityPath, "utf8"));
        NodeFS.writeFileSync(
          f.authorityPath,
          JSON.stringify({
            ...authority,
            environment_id: env,
            authority_namespace:
              env === "home:test"
                ? "t3-native:12345678-1234-4234-8234-123456789abc"
                : "t3-native:12345678-1234-4234-8234-123456789abd",
            store_generation: 1,
          }),
        );
        NodeFS.mkdirSync(f.secretsDir, { mode: 0o700 });
        for (const [name, key] of [
          ...Test.secrets(),
          ["server-signing-key", new Uint8Array(32).fill(env === "home:test" ? 41 : 51)] as const,
        ])
          NodeFS.writeFileSync(NodePath.join(f.secretsDir, name + ".bin"), key, { mode: 0o600 });
      }
      const readSecret = (name: string) =>
        controls.beforeSecret(env, name).pipe(
          Effect.andThen(
            Effect.sync(() => {
              if (!/^[A-Za-z0-9._:-]+$/.test(name))
                throw new Error("invalid synthetic secret name");
              const path = NodePath.join(f.secretsDir, name + ".bin");
              return NodeFS.existsSync(path)
                ? Option.some(new Uint8Array(NodeFS.readFileSync(path)))
                : Option.none<Uint8Array>();
            }),
          ),
        );
      const secrets = Layer.succeed(Secrets.ServerSecretStore, {
        get: readSecret,
        getOrCreateRandom: (name) =>
          readSecret(name).pipe(
            Effect.flatMap((value) =>
              Option.isSome(value)
                ? Effect.succeed(value.value)
                : Effect.die("missing existing fixture key"),
            ),
          ),
        set: () => Effect.die("no fixture key mutation"),
        create: () => Effect.die("no fixture key creation"),
        remove: () => Effect.die("no fixture key removal"),
      });
      const storage = Layer.mergeAll(
        Registry.layer,
        PinRecords.layer,
        MemberRecords.layer,
        Bindings.layer,
      ).pipe(
        Layer.provideMerge(
          Database.layer.pipe(
            Layer.provideMerge(Envelope.layer.pipe(Layer.provideMerge(Keys.layer))),
          ),
        ),
      );
      const native = NativeAuth.layer.pipe(
        Layer.provideMerge(Native.nativeLayer.pipe(Layer.provideMerge(Auth.layer))),
      );
      const base = Layer.mergeAll(storage, native).pipe(
        Layer.provideMerge(secrets),
        Layer.provideMerge(NodeSqlite.layer({ filename: f.dbPath })),
        Layer.provideMerge(Config.layerTest(process.cwd(), f.baseDir)),
        Layer.provideMerge(
          Layer.succeed(Environment.ServerEnvironmentIdentity, {
            getEnvironmentId: Effect.succeed(EnvironmentId.make(env)),
          }),
        ),
        Layer.provideMerge(NodeServices.layer),
        Layer.provideMerge(NodeCrypto.layer),
      );
      const life = yield* Scope.make();
      yield* Effect.addFinalizer(() => Scope.close(life, Exit.void));
      const context = yield* Layer.build(Layer.fresh(base)).pipe(
        Effect.provideService(Scope.Scope, life),
      );
      if (!existing)
        yield* Effect.gen(function* () {
          yield* runMigrations({ toMigrationInclusive: 54 });
          yield* m125;
          yield* m126;
          yield* m128;
          yield* m129;
          const sql = yield* SqlClient.SqlClient;
          yield* sql`CREATE TABLE jones_workstreams_setup_writer_probe(value TEXT)`;
          yield* sql`INSERT INTO jones_workstreams_keys(key_id,registry_instance_id,scope,state,created_at)
        VALUES ('test-local',${home.registry_instance_id},'local','active',${Test.now})`;
          if (env === "home:test") {
            yield* sql`INSERT INTO jones_workstreams_keys(key_id,registry_instance_id,scope,state,created_at)
          VALUES ('test-home',${home.registry_instance_id},'home','active',${Test.now})`;
            const registry = yield* Registry.RegistryStore;
            yield* registry.initialize({
              ...home,
              canonical_origin: "https://native.workstreams.invalid",
              registry_version: 0,
              lineage: "native",
              active_key_id: "test-home",
              state: "active",
            });
            yield* registry.appendPlain(Test.scope, "principals", {
              principal_id: "principal:test",
              kind: "environment",
              environment_id: "member:test",
              provenance: "{}",
            });
            yield* registry.appendPlain(Test.scope, "grants", {
              grant_id: "grant:test",
              principal_id: "principal:test",
              grant_version: 1,
              permissions: JSON.stringify(["workstreams:read", "workstreams:write"]),
              state: "active",
            });
          }
        }).pipe(Effect.provide(context));
      NodeFS.chmodSync(f.dbPath, 0o600);
      const live = Authority.NativeStoreAuthority.of({
        readCurrent: Effect.sync(() => {
          const a = JSON.parse(NodeFS.readFileSync(f.authorityPath, "utf8"));
          return {
            environmentId: a.environment_id as string,
            authorityNamespace: a.authority_namespace as string,
            storeGeneration: a.store_generation as number,
          };
        }),
        trustProvider: {
          readTrustSnapshot: () => ({ readiness: "ready", trustedEnvironments: [] }),
        },
      });
      const owner = yield* Owner.makeStandaloneSetupOwner(
        f,
        Context.get(context, SqlClient.SqlClient),
        live,
      ).pipe(Effect.provide(context));
      const issued = existing
        ? originalIssues.get(env)!
        : yield* Context.get(context, Auth.EnvironmentAuth).issueMcpClientSession({
            label: "label is not peer identity",
            access: "full-access",
            peerLink: true,
            client: { deviceType: "bot" },
          });
      if (!existing) originalIssues.set(env, issued);
      return { f, env, context, owner, issued, life, live };
    });
  const h = yield* makeEnvironment("home:test"),
    m = yield* makeEnvironment("member:test");
  const calls: Array<{
    receiver: string;
    tool: string;
    args: unknown;
    scope: Mcp.McpInvocationScope;
  }> = [];
  const state = {
    narrow: false,
    mutateFinal: false,
    loseBindReply: false,
    loseForwardReply: false,
    forwardOffline: false,
    loseMemberReply: false,
    forgedReceipt: false,
    unknownReadback: false,
    corruptIntent: false,
    register: true,
    beforeDispatch: Effect.void as Effect.Effect<void>,
    beforeReadback: Effect.void as Effect.Effect<void>,
  };
  type Extension = (
    args: unknown,
  ) => Effect.Effect<
    { readonly isFailure: boolean; readonly result: unknown } | undefined,
    never,
    Mcp.McpInvocationContext | ThreadManagementService
  >;
  const extensions = new Map<string, Extension>();
  let dispatch: (
    receiver: string,
    request: HttpClientRequest.HttpClientRequest,
  ) => Effect.Effect<Response> = () => Effect.die("transport not mounted");
  const mount = (local: typeof h, remote: typeof h) =>
    Effect.gen(function* () {
      let generation = "link:" + local.env + "->" + remote.env;
      const links = Layer.mock(PeerLinks.PeerLinks)({
        currentGeneration: () => Effect.sync(() => Option.some(generation)),
        resolve: () =>
          Effect.succeed({
            url: "https://" + remote.env.split(":")[0] + ".invalid",
            token: remote.issued.token,
            generation,
            link: {
              environmentId: EnvironmentId.make(remote.env),
              label: "ignored",
              urls: ["https://peer.invalid"],
              access: "full-access",
              linkedAt: DateTime.makeUnsafe(Test.now),
              expiresAt: DateTime.makeUnsafe("2026-01-31T00:00:00.000Z"),
              lastReachedAt: null,
              lastError: null,
            },
          }),
        recordOutcome: () => Effect.void,
      });
      const http = HttpClient.make((request) =>
        Effect.gen(function* () {
          const response = yield* dispatch(remote.env, request).pipe(Effect.orDie);
          return HttpResponse.fromWeb(request, response);
        }),
      );
      const peer = PeerClient.layer.pipe(
        Layer.provideMerge(links),
        Layer.provide(Layer.succeed(HttpClient.HttpClient, http)),
      );
      const requests = Files.layer(local.f.baseDir);
      const setup = Setup.layer.pipe(
        Layer.provideMerge(requests),
        Layer.provideMerge(peer),
        Layer.provide(Layer.succeed(Owner.SetupOwner, { current: Effect.succeed(local.owner) })),
      );
      const context = yield* Layer.build(setup).pipe(
        Effect.provide(local.context),
        Effect.provideService(Scope.Scope, local.life),
      );
      const all = Context.merge(local.context, context);
      const service = Context.get(context, Setup.PeerSetup);
      const toolkits = yield* Effect.gen(function* () {
        const setupTools = yield* Tools.toolkit.pipe(
          Effect.provide(Access.HandlersLayer.layer(Tools.handlers)),
        );
        const bindTools = yield* BindTools.toolkit.pipe(
          Effect.provide(
            Access.HandlersLayer.layer(BindTools.handlers).pipe(
              Layer.provide(Layer.succeed(Home.HomeBind, service.homeBind)),
            ),
          ),
        );
        return { setupTools, bindTools };
      }).pipe(Effect.provide(all));
      return {
        ...local,
        all,
        service,
        ...toolkits,
        relink: () => {
          generation += ":changed";
        },
      };
    });
  let H = yield* mount(h, m),
    M = yield* mount(m, h);
  const choose = (env: string) => (env === H.env ? H : M);
  dispatch = (receiver, request) =>
    Effect.gen(function* () {
      const at = choose(receiver);
      if (request.body._tag !== "Uint8Array")
        return yield* Effect.die("expected fixed JSON request");
      const body = JSON.parse(new TextDecoder().decode(request.body.body));
      if (body.method === "initialize")
        return Response.json(
          {
            jsonrpc: "2.0",
            id: body.id,
            result: {
              protocolVersion: "2025-06-18",
              capabilities: {},
              serverInfo: { name: "synthetic-native-runtime", version: "1" },
            },
          },
          { headers: { "mcp-session-id": "transport:" + receiver } },
        );
      if (body.method === "notifications/initialized") return new Response(null, { status: 202 });
      if (
        state.forwardOffline &&
        [HomePort.HOME_FORWARD_TOOL, HomePort.HOME_FORWARD_READBACK_TOOL].includes(body.params.name)
      )
        return new Response(null, { status: 503 });
      yield* state.beforeDispatch;
      const req = HttpRequest.fromWeb(
        new Request(request.url, { method: "POST", headers: request.headers }),
      );
      const auth = Context.get(at.all, Auth.EnvironmentAuth);
      const authenticated = yield* auth.authenticateMcpClient(req).pipe(Effect.result);
      if (authenticated._tag === "Failure") return new Response(null, { status: 401 });
      const client = authenticated.success;
      const header = request.headers[Mcp.MODE_LIMIT_HEADER];
      const limits = header?.split("/");
      const scope: Mcp.McpInvocationScope = {
        environmentId: EnvironmentId.make(receiver),
        thread: undefined,
        capabilities: new Set(["orchestration"]),
        issuedAt: Date.parse(Test.now),
        requestNamespace: "final:" + receiver + ":" + calls.length,
        client: {
          sessionId: client.sessionId,
          label: client.label,
          access: client.access,
          linked: client.peerLink,
          ...(limits
            ? { narrowedTo: mode({ runtimeMode: limits[0], interactionMode: limits[1] }) }
            : {}),
        },
      };
      if (state.narrow && scope.client)
        Object.assign(scope.client, {
          narrowedTo: { runtimeMode: "approval-required", interactionMode: "plan" },
        });
      if (state.register)
        yield* Context.get(at.all, NativeAuth.PeerMcpAuthentication).register(req, scope);
      if (state.mutateFinal) Object.assign(scope, { requestNamespace: "mutated-final-scope" });
      calls.push({ receiver, tool: body.params.name, args: body.params.arguments, scope });
      if (body.params.name === "jones_workstreams_home_bind_readback") yield* state.beforeReadback;
      const last = (
        replies: ReadonlyArray<{ readonly isFailure: boolean; readonly result: unknown }>,
      ) => replies.at(-1);
      const handled = (() => {
        switch (body.params.name) {
          case "jones_workstreams_peer_setup_read":
            return at.setupTools
              .handle("jones_workstreams_peer_setup_read", body.params.arguments)
              .pipe(Stream.unwrap, Stream.runCollect, Effect.map(last));
          case "jones_workstreams_peer_setup_apply":
            return at.setupTools
              .handle("jones_workstreams_peer_setup_apply", body.params.arguments)
              .pipe(Stream.unwrap, Stream.runCollect, Effect.map(last));
          case "jones_workstreams_home_bind_setup":
            return at.bindTools
              .handle("jones_workstreams_home_bind_setup", body.params.arguments)
              .pipe(Stream.unwrap, Stream.runCollect, Effect.map(last));
          case "jones_workstreams_home_bind":
            return at.bindTools
              .handle("jones_workstreams_home_bind", body.params.arguments)
              .pipe(Stream.unwrap, Stream.runCollect, Effect.map(last));
          case "jones_workstreams_home_bind_readback":
            return at.bindTools
              .handle("jones_workstreams_home_bind_readback", body.params.arguments)
              .pipe(Stream.unwrap, Stream.runCollect, Effect.map(last));
          default: {
            const extension = extensions.get(receiver + "/" + body.params.name);
            return extension
              ? extension(body.params.arguments)
              : Effect.die("unregistered fixture tool");
          }
        }
      })();
      const reply = yield* handled.pipe(
        Effect.provideService(Mcp.McpInvocationContext, scope),
        Effect.provide(threads.pipe(Layer.provideMerge(Layer.succeedContext(at.all)))),
      );
      if (!reply || reply.isFailure)
        return Response.json({
          jsonrpc: "2.0",
          id: body.id,
          error: { code: -32000, message: "fixed handler refused", data: reply?.result },
        });
      let result: unknown = reply.result;
      if (
        state.corruptIntent &&
        body.params.name === "jones_workstreams_peer_setup_read" &&
        body.params.arguments.operation === "source_intent"
      )
        result = { ...(result as object), request_body: '{"forged":"source"}' };
      if (state.unknownReadback && body.params.name === "jones_workstreams_home_bind_readback")
        result = {
          contract: F.WORKSTREAMS_HOME_BIND_CONTRACT,
          request: body.params.arguments,
          kind: "unknown",
          reason: "in_flight",
        };
      if (
        state.forgedReceipt &&
        body.params.name === "jones_workstreams_home_bind" &&
        typeof result === "object" &&
        result !== null &&
        "receipt" in result
      )
        result = {
          ...result,
          receipt: { ...(result.receipt as object), principal_id: "principal:forged" },
        };
      if (
        state.loseMemberReply &&
        body.params.name === "jones_workstreams_peer_setup_apply" &&
        body.params.arguments.operation === "accept_member"
      )
        return new Response(null, { status: 503 });
      if (state.loseBindReply && body.params.name === "jones_workstreams_home_bind")
        return new Response(null, { status: 503 });
      if (state.loseForwardReply && body.params.name === HomePort.HOME_FORWARD_TOOL)
        return new Response(null, { status: 503 });
      return Response.json({
        jsonrpc: "2.0",
        id: body.id,
        result: { content: [], structuredContent: result },
      });
    }).pipe(Effect.orDie);
  let selectedAddress = { ...address };
  const request = Effect.gen(function* () {
    const bytes = yield* Context.get(M.all, Files.PeerSetupRequestFiles).read(selectedAddress);
    if (!bytes) return yield* Effect.die("request not retained");
    return { bytes, request: W.decodeWorkstreamsPeerSetupRequest(bytes) };
  });
  const counts = (at: typeof H) =>
    Effect.gen(function* () {
      const sql = Context.get(at.all, SqlClient.SqlClient);
      return {
        bindings: yield* sql`SELECT * FROM jones_workstreams_binding_state`,
        pins: yield* sql`SELECT state,revision FROM jones_workstreams_peer_session_pins`,
        receipts: yield* sql`SELECT bind_id FROM jones_workstreams_home_bind_receipts`,
        enrollments:
          yield* sql`SELECT state,revision,enrollment_id,session_id FROM jones_workstreams_member_enrollments`,
        sourceBindings: yield* sql`SELECT * FROM jones_workstreams_source_bindings`,
        grants: yield* sql`SELECT grant_id FROM jones_workstreams_grants`,
        registries: yield* sql`SELECT registry_instance_id FROM jones_workstreams_registries`,
        sessions: yield* sql`SELECT session_id FROM auth_sessions`,
      };
    });
  const prepare = () =>
    M.service.ownerOperation({ operation: "prepare", ...selectedAddress, lifetime_days: 1 });
  const follow = (
    prepared: W.WorkstreamsPeerSetupResult,
    operation: "apply" | "resume" | "status",
  ) =>
    M.service.ownerOperation({
      operation,
      ...selectedAddress,
      request_sha256: prepared.request_sha256,
    });
  const restart = () =>
    Effect.gen(function* () {
      const oldH = H,
        oldM = M;
      yield* Scope.close(H.life, Exit.void);
      yield* Scope.close(M.life, Exit.void);
      const h = yield* makeEnvironment("home:test", oldH.f),
        m = yield* makeEnvironment("member:test", oldM.f);
      H = yield* mount(h, m);
      M = yield* mount(m, h);
    });
  return {
    get H() {
      return H;
    },
    get M() {
      return M;
    },
    calls,
    mountTool: (receiver: string, tool: string, handler: Extension) =>
      extensions.set(receiver + "/" + tool, handler),
    state,
    request,
    counts,
    prepare,
    follow,
    choose,
    restart,
    controls,
    nextSetup: () => {
      selectedAddress = { ...address, setup_id: "setup:second" };
    },
  };
});

it.effect(
  "full owner exchange uses real authenticated H/M producers and original accepted enrollment",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const beforeM = yield* f.counts(f.M),
        beforeH = yield* f.counts(f.H);
      expect(beforeM.registries).toEqual([]);
      expect(beforeM.grants).toEqual([]);
      const prepared = yield* f.prepare();
      const retained = yield* f.request;
      expect(prepared.state).toBe("retained");
      expect((yield* f.counts(f.M)).enrollments).toEqual([]);
      expect((yield* f.counts(f.H)).pins).toEqual([]);
      expect((yield* f.follow(prepared, "status")).state).toBe("retained");
      const bound = yield* f.follow(prepared, "apply");
      expect(bound.state).toBe("bound");
      const h = yield* f.counts(f.H),
        m = yield* f.counts(f.M);
      expect(h.receipts).toHaveLength(1);
      expect(h.pins).toEqual([{ state: "bound", revision: 2 }]);
      expect(m.pins).toEqual([{ state: "bound", revision: 2 }]);
      expect(m.enrollments).toHaveLength(1);
      expect(m.registries).toEqual([]);
      expect(m.grants).toEqual([]);
      expect(h.grants).toEqual(beforeH.grants);
      expect(m.sessions).toHaveLength(beforeM.sessions.length + 1);
      const b = F.decodeWorkstreamsHomeBindBody(retained.request.intent.bind_request_body);
      expect(m.enrollments[0]).toMatchObject({
        session_id: b.source.enrollment.session.session_id,
        enrollment_id: b.source.enrollment.enrollment_id,
      });
      expect((yield* f.request).bytes).toBe(retained.bytes);
      expect((yield* f.follow(prepared, "resume")).state).toBe("bound");
      expect(f.calls.filter((c) => c.tool === "jones_workstreams_home_bind")).toHaveLength(1);
      for (const c of f.calls) {
        const native = Context.get(f.choose(c.receiver).all, Native.NativePeerSessions);
        expect((yield* native.current({ ...c.scope }).pipe(Effect.result))._tag).toBe("Failure");
        expect((yield* native.current(c.scope)).session.session_id).toBe(c.scope.client!.sessionId);
      }
      // Consume genuine accepted M evidence through ordinary startup and request-local A admission.
      const op = yield* Owner.refreshSetupOwner(f.M.owner);
      const stores = Members.layer.pipe(
        Layer.provideMerge(Layer.mergeAll(MemberRecords.layer, PinRecords.layer, Bindings.layer)),
      );
      yield* Effect.gen(function* () {
        const members = yield* Members.MemberEnrollments;
        expect(
          Option.isSome(
            yield* members.getValidatedBySessionId(b.source.enrollment.session.session_id),
          ),
        ).toBe(true);
        const selection = yield* selectMember({
          text: JSON.stringify({
            mode: "native-member",
            registry: { ...home, canonical_origin: "https://native.workstreams.invalid" },
            bootstrap: {
              home: f.M.f.baseDir,
              database: f.M.f.dbPath,
              key_provider: "server-secret-store",
              source_session_id: b.source.enrollment.session.session_id,
            },
          }),
          legacyActivationPresent: false,
          home: f.M.f.baseDir,
          database: f.M.f.dbPath,
          environmentId: f.M.env,
        });
        expect(selection.mode).toBe("native-member");
        const issued = yield* Context.get(f.M.all, Auth.EnvironmentAuth).issueSession({
          subject: "cli-issued-session",
          scopes: ["orchestration:read"],
        });
        const principal = yield* Context.get(f.M.all, Auth.EnvironmentAuth).authenticateHttpRequest(
          HttpRequest.fromWeb(
            new Request("https://member.invalid/api/workstreams", {
              headers: { authorization: "Bearer " + issued.token },
            }),
          ),
        );
        const admission = Admission.layer.pipe(
          Layer.provideMerge(Callers.layer),
          Layer.provide(Layer.succeed(Startup.Startup, selection)),
        );
        yield* Effect.gen(function* () {
          const a = yield* Admission.MemberAdmission;
          expect((yield* a.current()).record.association.enrollment_session_id).toBe(
            b.source.enrollment.session.session_id,
          );
        }).pipe(
          Effect.provide(admission),
          Effect.provideService(EnvironmentAuthenticatedPrincipal, {
            ...principal,
            scopes: new Set(principal.scopes),
          }),
        );
      }).pipe(
        Effect.provide(stores.pipe(Layer.provideMerge(Owner.setupOwnerLayer(op)))),
        Effect.provide(f.M.all),
      );
      for (const at of [f.H, f.M]) {
        const requestFile = NodePath.join(
          at.f.baseDir,
          "workstreams",
          "native-peer-setup",
          address.setup_id + ".request.enc",
        );
        const encrypted = NodeFS.readFileSync(requestFile, "utf8");
        expect(encrypted).not.toContain(retained.request.intent.issuance_body);
        expect(encrypted).not.toContain(f.H.issued.token);
        expect(encrypted).not.toContain(f.M.issued.token);
        for (const path of [at.f.dbPath, at.f.dbPath + "-wal"]) {
          if (NodeFS.existsSync(path)) {
            const bytes = NodeFS.readFileSync(path);
            expect(bytes.includes(Buffer.from(retained.request.intent.bind_request_body))).toBe(
              false,
            );
            expect(bytes.includes(Buffer.from(f.H.issued.token))).toBe(false);
            expect(bytes.includes(Buffer.from(f.M.issued.token))).toBe(false);
            for (const key of Test.secrets().values()) {
              expect(bytes.includes(Buffer.from(key))).toBe(false);
              expect(bytes.includes(Buffer.from(Buffer.from(key).toString("hex")))).toBe(false);
            }
          }
        }
      }
    }).pipe(Effect.scoped),
);

it.effect(
  "lost reply survives fresh SQL/auth runtimes; status inspects and explicit resume never redispatches",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture,
        p = yield* f.prepare(),
        original = yield* f.request;
      f.state.loseBindReply = true;
      f.state.unknownReadback = true;
      expect((yield* f.follow(p, "apply").pipe(Effect.result))._tag).toBe("Failure");
      expect((yield* f.counts(f.H)).receipts).toHaveLength(1);
      expect((yield* f.counts(f.M)).enrollments).toEqual([
        expect.objectContaining({ state: "prepared", revision: 1 }),
      ]);
      const calls = f.calls.length;
      expect((yield* f.follow(p, "status")).state).toBe("unknown");
      expect(f.calls).toHaveLength(calls);
      const oldScope = f.calls[0]!;
      yield* f.restart();
      expect(
        (yield* Context.get(f.choose(oldScope.receiver).all, Native.NativePeerSessions)
          .current(oldScope.scope)
          .pipe(Effect.result))._tag,
      ).toBe("Failure");
      expect((yield* f.request).bytes).toBe(original.bytes);
      expect((yield* f.follow(p, "resume").pipe(Effect.result))._tag).toBe("Failure");
      f.state.unknownReadback = false;
      f.state.loseBindReply = false;
      expect((yield* f.follow(p, "resume")).state).toBe("bound");
      expect((yield* f.follow(p, "apply")).state).toBe("bound");
      expect(f.calls.filter((c) => c.tool === "jones_workstreams_home_bind")).toHaveLength(1);
      const m = yield* f.counts(f.M),
        b = F.decodeWorkstreamsHomeBindBody(original.request.intent.bind_request_body);
      expect(m.enrollments[0]).toMatchObject({
        session_id: b.source.enrollment.session.session_id,
        enrollment_id: b.source.enrollment.enrollment_id,
      });
      expect(m.registries).toEqual([]);
      expect(m.grants).toEqual([]);
    }).pipe(Effect.scoped),
);
it.effect.each(["unregistered", "plan", "changed-final-scope", "forged-source-intent"] as const)(
  "refuses %s native authority before binding",
  (kind) =>
    Effect.gen(function* () {
      const f = yield* fixture;
      if (kind === "unregistered") {
        f.state.register = false;
        expect((yield* f.prepare().pipe(Effect.result))._tag).toBe("Failure");
      } else {
        const p = yield* f.prepare();
        if (kind === "forged-source-intent") f.state.corruptIntent = true;
        if (kind === "plan") f.state.narrow = true;
        if (kind === "changed-final-scope") f.state.mutateFinal = true;
        expect((yield* f.follow(p, "apply").pipe(Effect.result))._tag).toBe("Failure");
      }
      expect((yield* f.counts(f.H)).receipts).toEqual([]);
      expect((yield* f.counts(f.M)).enrollments).toEqual([]);
      expect(f.calls.filter((c) => c.tool === "jones_workstreams_home_bind")).toEqual([]);
    }).pipe(Effect.scoped),
);
it.effect.each([
  "setup-bytes",
  "bind-bytes",
  "source-owner",
  "home",
  "build",
  "grant",
  "session",
  "generation",
] as const)("changed %s fences retained setup before effects", (kind) =>
  Effect.gen(function* () {
    const f = yield* fixture,
      p = yield* f.prepare(),
      r = yield* f.request;
    if (["setup-bytes", "bind-bytes", "source-owner", "home"].includes(kind)) {
      const changed = JSON.parse(r.bytes);
      if (kind === "setup-bytes") changed.intent.home_setup.setup_request_body += " ";
      if (kind === "bind-bytes") changed.intent.bind_request_body += " ";
      if (kind === "source-owner")
        changed.member_observation.owner.qualification_sha256 = "f".repeat(64);
      if (kind === "home") changed.expected_home.server_generation++;
      const encrypted = yield* Context.get(f.M.all, Envelope.WorkstreamsEnvelope).seal(
        {
          ...home,
          key_scope: "local",
          purpose: "peer_setup_request",
          record_id: address.setup_id,
          revision: 1,
        },
        JSON.stringify(changed),
      );
      const file = NodePath.join(
        f.M.f.baseDir,
        "workstreams",
        "native-peer-setup",
        address.setup_id + ".request.enc",
      );
      NodeFS.copyFileSync(file, file + ".original");
      NodeFS.writeFileSync(file, encrypted);
    }
    if (kind === "build") {
      const receipt = JSON.parse(NodeFS.readFileSync(f.M.f.receiptPath, "utf8"));
      NodeFS.writeFileSync(
        f.M.f.receiptPath,
        JSON.stringify({ ...receipt, sourceTree: "f".repeat(40) }),
      );
    }
    if (kind === "grant")
      yield* Context.get(f.H.all, Registry.RegistryStore).appendPlain(Test.scope, "grants", {
        grant_id: "grant:revoked",
        principal_id: "principal:test",
        grant_version: 2,
        permissions: JSON.stringify(["workstreams:read", "workstreams:write"]),
        state: "revoked",
      });
    if (kind === "session")
      yield* Context.get(f.M.all, AuthRows.AuthSessionRepository).revoke({
        sessionId: f.M.issued.sessionId,
        revokedAt: DateTime.makeUnsafe(Test.now),
      });
    if (kind === "generation") f.M.relink();
    expect((yield* f.follow(p, "apply").pipe(Effect.result))._tag).toBe("Failure");
    expect((yield* f.counts(f.H)).receipts).toEqual([]);
    expect((yield* f.counts(f.M)).enrollments).toEqual([]);
    expect(f.calls.filter((c) => c.tool === "jones_workstreams_home_bind")).toEqual([]);
  }).pipe(Effect.scoped),
);
const failureStages = [
  { name: "H prepare pin", at: "H", table: "jones_workstreams_peer_session_pins", event: "INSERT" },
  {
    name: "M prepare enrollment",
    at: "M",
    table: "jones_workstreams_member_enrollments",
    event: "INSERT",
  },
  {
    name: "H source binding",
    at: "H",
    table: "jones_workstreams_source_bindings",
    event: "INSERT",
  },
  { name: "H bound pin", at: "H", table: "jones_workstreams_peer_session_pins", event: "UPDATE" },
  { name: "H receipt", at: "H", table: "jones_workstreams_home_bind_receipts", event: "INSERT" },
  { name: "M original session", at: "M", table: "auth_sessions", event: "INSERT" },
  { name: "M singleton", at: "M", table: "jones_workstreams_binding_state", event: "INSERT" },
  { name: "M bound pin", at: "M", table: "jones_workstreams_peer_session_pins", event: "UPDATE" },
  {
    name: "M accepted enrollment",
    at: "M",
    table: "jones_workstreams_member_enrollments",
    event: "UPDATE",
  },
] as const;
it.effect.each(failureStages)(
  "rollback at $name preserves identities and never redispatches",
  (stage) =>
    Effect.gen(function* () {
      const f = yield* fixture,
        p = yield* f.prepare(),
        r = yield* f.request;
      const local = stage.at === "H" ? f.H : f.M,
        sql = Context.get(local.all, SqlClient.SqlClient),
        beforeM = yield* f.counts(f.M);
      yield* sql.unsafe(
        "CREATE TRIGGER fixture_fail BEFORE " +
          stage.event +
          " ON " +
          stage.table +
          " BEGIN SELECT RAISE(ABORT,'fixture failpoint'); END",
      );
      expect((yield* f.follow(p, "apply").pipe(Effect.result))._tag).toBe("Failure");
      const h = yield* f.counts(f.H),
        m = yield* f.counts(f.M);
      expect(m.bindings).toEqual([]);
      expect(m.sessions).toEqual(beforeM.sessions);
      expect(m.registries).toEqual([]);
      expect(m.grants).toEqual([]);
      if (stage.name.startsWith("M ") && !stage.name.startsWith("M prepare")) {
        expect(h.receipts).toHaveLength(1);
        expect(m.enrollments).toEqual([
          expect.objectContaining({ state: "prepared", revision: 1 }),
        ]);
        expect(m.pins).toEqual([{ state: "prepared", revision: 1 }]);
      } else {
        expect(h.receipts).toEqual([]);
        expect(h.sourceBindings).toEqual([]);
      }
      const binds = f.calls.filter((c) => c.tool === "jones_workstreams_home_bind").length;
      yield* f.follow(p, "resume").pipe(Effect.result);
      expect(f.calls.filter((c) => c.tool === "jones_workstreams_home_bind")).toHaveLength(binds);
      expect((yield* f.request).bytes).toBe(r.bytes);
    }).pipe(Effect.scoped),
);
it.effect("authentication/key/readback waits release local writers", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    for (const kind of ["authentication", "key"] as const) {
      const entered = yield* Deferred.make<void>(),
        release = yield* Deferred.make<void>();
      let first = true;
      const block = Effect.suspend(() => {
        if (!first) return Effect.void;
        first = false;
        return Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release)));
      });
      if (kind === "authentication") f.state.beforeDispatch = block;
      else
        f.controls.beforeSecret = (env, name) =>
          env === "member:test" && name.startsWith("workstreams-content") ? block : Effect.void;
      const worker = yield* Effect.forkChild(f.prepare().pipe(Effect.result));
      yield* Effect.raceFirst(
        Deferred.await(entered),
        Fiber.join(worker).pipe(
          Effect.andThen(Effect.die("worker completed before entering the controlled wait")),
        ),
      );
      for (const at of [f.H, f.M]) {
        const sql = Context.get(at.all, SqlClient.SqlClient);
        yield* sql.withTransaction(
          sql`INSERT INTO jones_workstreams_setup_writer_probe VALUES (${kind})`,
        );
      }
      yield* Deferred.succeed(release, undefined);
      expect((yield* Fiber.join(worker))._tag).toBe("Success");
      f.state.beforeDispatch = Effect.void;
      f.controls.beforeSecret = () => Effect.void;
      if (kind === "authentication") f.nextSetup();
    }
    const p = yield* f.prepare(),
      entered = yield* Deferred.make<void>(),
      release = yield* Deferred.make<void>();
    let first = true;
    f.state.beforeReadback = Effect.suspend(() => {
      if (!first) return Effect.void;
      first = false;
      return Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release)));
    });
    const worker = yield* Effect.forkChild(f.follow(p, "apply").pipe(Effect.result));
    yield* Effect.raceFirst(
      Deferred.await(entered),
      Fiber.join(worker).pipe(
        Effect.andThen(Effect.die("worker completed before entering the controlled wait")),
      ),
    );
    const sql = Context.get(f.M.all, SqlClient.SqlClient);
    yield* sql.withTransaction(
      sql`INSERT INTO jones_workstreams_setup_writer_probe VALUES ('readback writer free')`,
    );
    yield* Deferred.succeed(release, undefined);
    expect((yield* Fiber.join(worker))._tag).toBe("Success");
  }).pipe(Effect.scoped),
);
it.effect("same-request concurrent CAS produces one binding/receipt/original enrollment", () =>
  Effect.gen(function* () {
    const f = yield* fixture,
      p = yield* f.prepare();
    const outcomes = yield* Effect.all(
      [f.follow(p, "apply").pipe(Effect.result), f.follow(p, "apply").pipe(Effect.result)],
      { concurrency: 2 },
    );
    expect(outcomes.some((x) => x._tag === "Success" && x.success.state === "bound")).toBe(true);
    expect((yield* f.counts(f.H)).receipts).toHaveLength(1);
    expect((yield* f.counts(f.M)).enrollments).toHaveLength(1);
    expect(f.calls.filter((c) => c.tool === "jones_workstreams_home_bind")).toHaveLength(1);
  }).pipe(Effect.scoped),
);

it.effect(
  "CLI fixed HTTP adapter uses real local native auth and dispatched owner handler through the complete H/M flow",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const tokens: string[] = [];
      const http = HttpClient.make((request) =>
        Effect.gen(function* () {
          expect(request.url).toBe("http://127.0.0.1:1111" + Cli.PEER_SETUP_OWNER_PATH);
          expect(request.body._tag).toBe("Uint8Array");
          if (request.body._tag !== "Uint8Array")
            return yield* Effect.die("expected closed CLI JSON body");
          tokens.push(request.headers.authorization!.slice("Bearer ".length));
          const wire = HttpRequest.fromWeb(
            new Request(request.url, {
              method: "POST",
              headers: request.headers,
              body: Buffer.from(request.body.body),
            }),
          );
          const principal = yield* Context.get(
            f.M.all,
            Auth.EnvironmentAuth,
          ).authenticateHttpRequest(wire);
          const socket = new NodeNet.Socket();
          Object.defineProperty(socket, "remoteAddress", { value: "127.0.0.1" });
          const source = new NodeHttp.IncomingMessage(socket);
          source.method = "POST";
          source.url = Cli.PEER_SETUP_OWNER_PATH;
          Object.defineProperty(wire, "source", { value: source });
          Object.defineProperty(wire, "stream", { value: Stream.make(request.body.body) });
          const response = yield* Cli.createPeerSetupOwnerHandler(f.M.service)().pipe(
            Effect.provideService(HttpRequest.HttpServerRequest, wire),
            Effect.provideService(EnvironmentAuthenticatedPrincipal, {
              ...principal,
              scopes: new Set(principal.scopes),
            }),
            Effect.ensuring(Effect.sync(() => socket.destroy())),
          );
          return HttpResponse.fromWeb(request, ServerResponse.toWeb(response));
        }).pipe(Effect.scoped, Effect.orDie),
      );
      const endpoint = yield* Cli.makePeerSetupCliHttpEndpoint(
        f.M.f.baseDir,
        "http://127.0.0.1:1111",
      ).pipe(
        Effect.provide(Cli.existingCliSessionsLayer),
        Effect.provide(f.M.all),
        Effect.provideService(HttpClient.HttpClient, http),
      );
      const prepared = yield* endpoint.run(f.M.f.baseDir, {
        operation: "prepare",
        ...address,
        lifetime_days: 1,
      });
      const bound = yield* endpoint.run(f.M.f.baseDir, {
        operation: "apply",
        ...address,
        request_sha256: prepared.request_sha256,
      });
      expect(bound.state).toBe("bound");
      expect((yield* f.counts(f.H)).receipts).toHaveLength(1);
      expect((yield* f.counts(f.M)).enrollments).toHaveLength(1);
      for (const token of tokens)
        expect(
          (yield* Context.get(f.M.all, Sessions.SessionStore).verify(token).pipe(Effect.result))
            ._tag,
        ).toBe("Failure");
      const file = NodePath.join(
        f.M.f.baseDir,
        "workstreams",
        "native-peer-setup",
        address.setup_id + ".request.enc",
      );
      for (const token of tokens) expect(NodeFS.readFileSync(file, "utf8")).not.toContain(token);
    }).pipe(Effect.scoped),
);

it.effect(
  "a forged home receipt cannot be accepted; explicit authenticated readback recovers the real original receipt",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture,
        p = yield* f.prepare();
      f.state.forgedReceipt = true;
      expect((yield* f.follow(p, "apply").pipe(Effect.result))._tag).toBe("Failure");
      expect((yield* f.counts(f.H)).receipts).toHaveLength(1);
      expect((yield* f.counts(f.M)).enrollments).toEqual([
        expect.objectContaining({ state: "prepared", revision: 1 }),
      ]);
      expect((yield* f.counts(f.M)).bindings).toEqual([]);
      f.state.forgedReceipt = false;
      expect((yield* f.follow(p, "resume")).state).toBe("bound");
      expect(f.calls.filter((c) => c.tool === "jones_workstreams_home_bind")).toHaveLength(1);
    }).pipe(Effect.scoped),
);
it.effect(
  "lost member acceptance reply preserves accepted identity and apply/resume never redispatch the bind",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture,
        p = yield* f.prepare(),
        original = yield* f.request;
      f.state.loseMemberReply = true;
      expect((yield* f.follow(p, "apply").pipe(Effect.result))._tag).toBe("Failure");
      expect((yield* f.counts(f.H)).receipts).toHaveLength(1);
      expect((yield* f.counts(f.M)).enrollments).toEqual([
        expect.objectContaining({ state: "bound", revision: 2 }),
      ]);
      const calls = f.calls.length;
      expect((yield* f.follow(p, "status")).state).toBe("bound");
      expect(f.calls).toHaveLength(calls);
      f.state.loseMemberReply = false;
      expect((yield* f.follow(p, "apply")).state).toBe("bound");
      expect((yield* f.follow(p, "resume")).state).toBe("bound");
      expect((yield* f.request).bytes).toBe(original.bytes);
      expect(f.calls.filter((c) => c.tool === "jones_workstreams_home_bind")).toHaveLength(1);
    }).pipe(Effect.scoped),
);

it.effect(
  "prepares genuine accepted M on the selected application SQL and expires every escaped member service",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture;
        const prepared = yield* f.prepare();
        expect((yield* f.follow(prepared, "apply")).state).toBe("bound");
        const retained = yield* f.request;
        const b = F.decodeWorkstreamsHomeBindBody(retained.request.intent.bind_request_body);
        const authority = yield* CurrentFixture.prepare({
          home: f.M.f.baseDir,
          database: f.M.f.dbPath,
          context: {
            source_instance_id: b.source.authority.environment_id,
            authority_namespace: b.source.authority.authority_namespace,
            store_generation: b.source.authority.store_generation,
          },
        }).pipe(Effect.provide(f.M.all));
        const sql = Context.get(f.M.all, SqlClient.SqlClient);
        const input = {
          text: JSON.stringify({
            mode: "native-member",
            registry: { ...home, canonical_origin: "https://native.workstreams.invalid" },
            bootstrap: {
              home: f.M.f.baseDir,
              database: f.M.f.dbPath,
              key_provider: "server-secret-store",
              source_session_id: b.source.enrollment.session.session_id,
            },
          }),
          legacyActivationPresent: false,
          home: f.M.f.baseDir,
          database: f.M.f.dbPath,
          environmentId: f.M.env,
        };
        yield* Effect.gen(function* () {
          const preparation = yield* MemberPreparation.MemberPreparation;
          const before = yield* sql`SELECT total_changes() AS changes`;
          const escaped = yield* preparation.withPrepared((members) =>
            Effect.gen(function* () {
              const proof = yield* members.getValidatedBySessionId(
                b.source.enrollment.session.session_id,
              );
              expect(Option.isSome(proof)).toBe(true);
              expect(
                yield* selectMember(input).pipe(
                  Effect.provideService(Members.MemberEnrollments, members),
                ),
              ).toMatchObject({
                mode: "native-member",
                problem: null,
                sourceSessionId: b.source.enrollment.session.session_id,
              });
              expect(
                yield* members
                  .materialize(b.source.enrollment.session.session_id)
                  .pipe(Effect.flip),
              ).toMatchObject({ code: "owner_unqualified" });
              return () => members.getValidatedBySessionId(b.source.enrollment.session.session_id);
            }),
          );
          expect(yield* sql`SELECT total_changes() AS changes`).toEqual(before);
          expect(yield* escaped().pipe(Effect.flip)).toMatchObject({ code: "owner_unqualified" });
          expect(
            yield* sql
              .withTransaction(
                preparation.withPrepared(() => Effect.die("Unexpected ambient callback")),
              )
              .pipe(Effect.flip),
          ).toMatchObject({ code: "preparation_in_transaction" });
          const otherSql = Context.get(f.H.all, SqlClient.SqlClient);
          expect(
            yield* preparation
              .withPrepared(() => Effect.die("Unexpected substituted SQL"))
              .pipe(Effect.provideService(SqlClient.SqlClient, otherSql), Effect.flip),
          ).toMatchObject({ code: "authority_fenced" });
          const sentinel = new RegistryError({ code: "invalid_request" });
          expect(
            yield* preparation.withPrepared(() => Effect.fail(sentinel)).pipe(Effect.flip),
          ).toBe(sentinel);
          expect(yield* sql`SELECT * FROM jones_workstreams_grants`).toEqual([]);
          expect(yield* sql`SELECT * FROM jones_workstreams_registries`).toEqual([]);
        }).pipe(
          Effect.provide(
            MemberPreparation.layer.pipe(
              Layer.provide(authority),
              Layer.provide(Layer.succeed(Authority.NativeStoreAuthority, f.M.live)),
              Layer.provideMerge(Layer.succeedContext(f.M.all)),
            ),
          ),
        );
      }),
    ),
);

it.effect(
  "composes genuine member application forwarding with registered home and source handlers",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture;
        expect((yield* f.follow(yield* f.prepare(), "apply")).state).toBe("bound");
        const bind = F.decodeWorkstreamsHomeBindBody(
          (yield* f.request).request.intent.bind_request_body,
        );
        yield* m130.pipe(Effect.provide(f.H.all));
        // Release/build/launcher and remote HTTP are synthetic external ports; owner, auth,
        // SQL, encrypted stores, member admission, registration and forwarding are production.
        const sourceAuthority = yield* CurrentFixture.prepare({
          home: f.M.f.baseDir,
          database: f.M.f.dbPath,
          context: {
            source_instance_id: bind.source.authority.environment_id,
            authority_namespace: bind.source.authority.authority_namespace,
            store_generation: bind.source.authority.store_generation,
          },
        }).pipe(Effect.provide(f.M.all));
        const sourceBase = Layer.mergeAll(
          Layer.succeedContext(f.M.all),
          Layer.succeed(Authority.NativeStoreAuthority, f.M.live),
        );
        const resources = yield* Layer.build(
          Layer.mergeAll(
            MemberPreparation.layer,
            OperationStore.layer,
            Native.layer,
            Forwarder.locksLayer,
          ).pipe(Layer.provideMerge(sourceAuthority), Layer.provideMerge(sourceBase)),
        );
        const selected = yield* Context.get(resources, MemberPreparation.MemberPreparation)
          .withPrepared((members) =>
            selectMember({
              text: JSON.stringify({
                mode: "native-member",
                registry: { ...home, canonical_origin: "https://native.workstreams.invalid" },
                bootstrap: {
                  home: f.M.f.baseDir,
                  database: f.M.f.dbPath,
                  key_provider: "server-secret-store",
                  source_session_id: bind.source.enrollment.session.session_id,
                },
              }),
              legacyActivationPresent: false,
              home: f.M.f.baseDir,
              database: f.M.f.dbPath,
              environmentId: f.M.env,
            }).pipe(Effect.provideService(Members.MemberEnrollments, members)),
          )
          .pipe(Effect.provide(resources));
        expect(selected).toMatchObject({ mode: "native-member", problem: null });
        const providerPorts = ProviderTest.makeProviderFixture().ports;
        const runtime = Runtime.NativeWorkstreamsRuntime.of({
          provider: makeWorkstreamsNativeProvider(providerPorts),
          providerV2: makeWorkstreamsNativeProvider(providerPorts, "v2"),
          enrollments: { getBySessionId: () => Effect.fail(new NativeProviderEnrollmentError()) },
        });
        const app = yield* MemberApp.MemberApplication.pipe(
          Effect.provide(
            MemberApp.layer.pipe(
              Layer.provide(Layer.succeed(Startup.Startup, selected)),
              Layer.provide(Layer.succeed(Runtime.NativeWorkstreamsRuntime, runtime)),
              Layer.provide(Layer.succeedContext(resources)),
            ),
          ),
        );
        const sourceTools = yield* NativeForwards.sourceToolkit.pipe(
          Effect.provide(
            Access.HandlersLayer.layer(NativeForwards.sourceHandlers).pipe(
              Layer.provide(
                Layer.succeed(SourcePort.SourceReadmission, {
                  lookupParent: app.lookupParent,
                  lookupRaw: app.lookupRaw,
                }),
              ),
            ),
          ),
        );
        f.mountTool(f.M.env, SourcePort.SOURCE_READMISSION_TOOL, (args) =>
          decodeSourceArgs(args).pipe(
            Effect.flatMap((decoded) =>
              sourceTools.handle(SourcePort.SOURCE_READMISSION_TOOL, decoded).pipe(
                Stream.unwrap,
                Stream.runCollect,
                Effect.map((replies) => replies.at(-1)),
              ),
            ),
            Effect.orDie,
          ),
        );
        const homeContext = JSON.parse(NodeFS.readFileSync(f.H.f.authorityPath, "utf8"));
        const homeAuthority = yield* CurrentFixture.prepare({
          home: f.H.f.baseDir,
          database: f.H.f.dbPath,
          context: {
            source_instance_id: homeContext.environment_id,
            authority_namespace: homeContext.authority_namespace,
            store_generation: homeContext.store_generation,
          },
        }).pipe(Effect.provide(f.H.all));
        const homeBase = Layer.mergeAll(
          Layer.succeedContext(f.H.all),
          Layer.succeed(Authority.NativeStoreAuthority, f.H.live),
          Layer.succeed(NativeHome, {
            ...home,
            canonical_origin: "https://native.workstreams.invalid",
            served_by_environment_id: f.H.env,
          }),
        );
        const homeTools = yield* HomePort.HomeTools.pipe(
          Effect.provide(
            Receiving.layer.pipe(
              Layer.provide(Layer.mergeAll(SourceQualification.layer, NativeSession.layer)),
              Layer.provide(homeAuthority),
              Layer.provide(homeBase),
            ),
          ),
        );
        const homeBox = yield* NativeForwards.homeToolkit.pipe(
          Effect.provide(
            Access.HandlersLayer.layer(NativeForwards.homeHandlers).pipe(
              Layer.provide(Layer.succeed(HomePort.HomeTools, homeTools)),
            ),
          ),
        );
        f.mountTool(f.H.env, HomePort.HOME_FORWARD_TOOL, (args) =>
          decodeForwardArgs(args).pipe(
            Effect.flatMap((decoded) =>
              homeBox.handle(HomePort.HOME_FORWARD_TOOL, decoded).pipe(
                Stream.unwrap,
                Stream.runCollect,
                Effect.map((replies) => replies.at(-1)),
              ),
            ),
            Effect.orDie,
          ),
        );
        f.mountTool(f.H.env, HomePort.HOME_FORWARD_READBACK_TOOL, (args) =>
          decodeReadbackArgs(args).pipe(
            Effect.flatMap((decoded) =>
              homeBox.handle(HomePort.HOME_FORWARD_READBACK_TOOL, decoded).pipe(
                Stream.unwrap,
                Stream.runCollect,
                Effect.map((replies) => replies.at(-1)),
              ),
            ),
            Effect.orDie,
          ),
        );
        const auth = Context.get(f.M.all, Auth.EnvironmentAuth);
        const issued = yield* auth.issueSession({
          subject: "member-application",
          scopes: ["orchestration:read", "orchestration:operate"],
        });
        const authenticated = yield* auth.authenticateHttpRequest(
          HttpRequest.fromWeb(
            new Request("https://member.invalid/api/workstreams", {
              headers: { authorization: "Bearer " + issued.token },
            }),
          ),
        );
        const principal = { ...authenticated, scopes: new Set(authenticated.scopes) };
        const request = {
          contract: "workstream-operations/1.0.0" as const,
          operation_id: "operation:composed-member-01",
          origin: { kind: "app" as const },
          kind: "create_workstream" as const,
          registry: { registry_id: home.registry_instance_id, owner_id: home.owner_id },
          workstream: {
            name: "Composed member",
            lifecycle: "active" as const,
            progress: { state: "unknown" as const },
            sort_order: 0,
          },
        };
        const before = f.calls.length;
        expect(
          yield* app
            .submitOperation({ request, wait_ms: 0 })
            .pipe(
              Effect.provideService(EnvironmentAuthenticatedPrincipal, principal),
              Effect.provideService(SqlClient.SqlClient, Context.get(f.H.all, SqlClient.SqlClient)),
              Effect.flip,
            ),
        ).toMatchObject({ code: "authority_fenced" });
        expect(
          yield* Context.get(f.M.all, SqlClient.SqlClient)
            .withTransaction(app.submitOperation({ request, wait_ms: 0 }))
            .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, principal), Effect.flip),
        ).toMatchObject({ code: "preparation_in_transaction" });
        expect(f.calls).toHaveLength(before);
        const result = yield* app
          .submitOperation({ request, wait_ms: 0 })
          .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, principal));
        expect(result).toMatchObject({
          state: "committed",
          kind: "create_workstream",
          operation_id: request.operation_id,
        });
        const repeated = yield* app
          .submitOperation({ request, wait_ms: 0 })
          .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, principal));
        expect(repeated).toEqual(result);
        const hsql = Context.get(f.H.all, SqlClient.SqlClient),
          msql = Context.get(f.M.all, SqlClient.SqlClient);
        expect(yield* hsql`SELECT workstream_id FROM jones_workstreams_records`).toHaveLength(1);
        expect(yield* hsql`SELECT state FROM jones_workstreams_forward_receipts`).toEqual([
          { state: "completed" },
        ]);
        expect(yield* msql`SELECT state FROM jones_workstreams_operations`).toEqual([
          { state: "committed" },
        ]);
        expect(yield* msql`SELECT * FROM jones_workstreams_registries`).toEqual([]);
        expect(yield* msql`SELECT * FROM jones_workstreams_grants`).toEqual([]);
        expect(
          f.calls.slice(before).some((call) => call.tool === "jones_workstreams_home_read_result"),
        ).toBe(false);
        expect(
          f.calls.slice(before).some((call) => call.tool === SourcePort.SOURCE_READMISSION_TOOL),
        ).toBe(true);
        expect(yield* app.recoverOpen()).toEqual([]);
        const lostRequest = { ...request, operation_id: "operation:composed-lost-reply-01" };
        f.state.loseForwardReply = true;
        const lost = yield* app
          .submitOperation({ request: lostRequest, wait_ms: 0 })
          .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, principal));
        expect(lost.state).toBe("reconciling");
        expect(yield* hsql`SELECT state FROM jones_workstreams_forward_receipts`).toEqual([
          { state: "completed" },
          { state: "completed" },
        ]);
        const forwards = f.calls.filter((call) => call.tool === HomePort.HOME_FORWARD_TOOL).length;
        f.state.loseForwardReply = false;
        const recovered = yield* app.recoverOpen();
        expect(recovered).toEqual([
          expect.objectContaining({ operation_id: lostRequest.operation_id, state: "committed" }),
        ]);
        expect(f.calls.filter((call) => call.tool === HomePort.HOME_FORWARD_TOOL)).toHaveLength(
          forwards,
        );
        expect(yield* app.recoverOpen()).toEqual([]);
        f.state.forwardOffline = true;
        expect(
          (yield* app
            .submitOperation({
              request: { ...request, operation_id: "operation:composed-offline-01" },
              wait_ms: 0,
            })
            .pipe(
              Effect.provideService(EnvironmentAuthenticatedPrincipal, principal),
              Effect.result,
            ))._tag,
        ).toBe("Failure");
        f.state.forwardOffline = false;
        expect(
          yield* msql`SELECT operation_id FROM jones_workstreams_operations WHERE operation_id='operation:composed-offline-01'`,
        ).toEqual([]);
        const retainedCalls = f.calls.length;
        expect(yield* auth.revokeSession(issued.sessionId)).toBe(true);
        expect(
          (yield* app
            .readOperation(request.operation_id)
            .pipe(
              Effect.provideService(EnvironmentAuthenticatedPrincipal, principal),
              Effect.result,
            ))._tag,
        ).toBe("Failure");
        expect(
          (yield* app
            .submitOperation({ request, wait_ms: 0 })
            .pipe(
              Effect.provideService(EnvironmentAuthenticatedPrincipal, principal),
              Effect.result,
            ))._tag,
        ).toBe("Failure");
        expect(f.calls).toHaveLength(retainedCalls);
        expect(yield* hsql`SELECT workstream_id FROM jones_workstreams_records`).toHaveLength(2);
        expect(yield* msql`SELECT state FROM jones_workstreams_operations`).toEqual([
          { state: "committed" },
          { state: "committed" },
        ]);
      }),
    ),
);

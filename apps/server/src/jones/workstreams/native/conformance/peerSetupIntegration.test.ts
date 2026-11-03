import * as NativeExportCli from "../read/exportCli.ts";
import * as Stdio from "effect/Stdio";
import * as Sink from "effect/Sink";
import * as ReadExport from "../read/export.ts";
import * as HomeAdmission from "../runtime/admission.ts";
import { validateNativeReadExport } from "../../../../../../../packages/client-runtime/src/state/workstreams/nativeExport.ts";
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
  EnvironmentAuthenticatedAuth,
  EnvironmentHttpApi,
  RuntimeMode,
  ProviderInteractionMode,
} from "@t3tools/contracts";
import * as W from "@t3tools/contracts/jones/workstreamsPeerSetup";
import * as F from "@t3tools/contracts/jones/workstreamsHomeBind";
import * as AccessWire from "@t3tools/contracts/jones/workstreamsHomeAccess";
import * as RegistrationWire from "@t3tools/contracts/jones/workstreamsHomeRegistration";
import * as PublicRegistration from "../../registrationContext/service.ts";
import * as ForwardWire from "@t3tools/contracts/jones/workstreamsForwarding";
import * as ParentWire from "@t3tools/contracts/jones/workstreamsForwardQualification";
import * as RawWire from "@t3tools/contracts/jones/workstreamsForwardReadmission";
import * as RetainedWire from "@t3tools/contracts/jones/workstreamsRetainedSourceEffect";
import { canonicalWorkstreamsSnapshotJson as canonicalSnapshot } from "@t3tools/contracts/jones/workstreamsSnapshot";
import * as RetainedPort from "../forwarding/retainedSourceEffectPort.ts";
import * as Integration from "../integration.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeSqlite from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Clock from "effect/Clock";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpPlatform from "effect/http/HttpPlatform";
import * as Etag from "effect/http/Etag";
import * as NativePlacement from "@t3tools/contracts/jones/workstreamsNativePlacement";
import * as AuthHttp from "../../../../auth/http.ts";
import {
  workstreamHttpApiLayer,
  workstreamResponseHeadersLayer,
} from "../../../../workstreams/http.ts";
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
import * as PublicOperations from "../../operations/service.ts";
import * as OperationWire from "@t3tools/contracts/jones/workstreamOperations";
import * as MemberServices from "../runtime/memberServices.ts";
import * as MemberRecovery from "../runtime/memberRecovery.ts";
import * as ReadRoutes from "../read/routes.ts";
import * as ReadProjection from "../read/projection.ts";
import * as HomeRead from "../forwarding/homeReadProtocol.ts";
import * as PeerRead from "../forwarding/peerReadAdmission.ts";
import * as NativeReadTools from "../../../mcp/workstreams/nativeReads.ts";
import * as ReadWire from "@t3tools/contracts/jones/workstreamsHomeReadResult";
import {
  NativeWorkstreamReader,
  nativeReadRequest,
} from "../../../../../../../packages/client-runtime/src/state/workstreams/nativeRead.ts";
import * as PublicGateway from "../../../../workstreams/WorkstreamGateway.ts";
import * as LocalReads from "../registry/localTransport.ts";
import * as RegistryService from "../registry/service.ts";
import * as RegistryCounts from "../registry/counts.ts";
import * as RegistryAppearance from "../registry/appearance.ts";
import * as Placements from "../registry/placements.ts";
import * as EffectRunner from "../registry/effectRunner.ts";
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
import { makeWorkstreamsNativeProvider, sha256Bytes } from "../../nativeProvider/service.ts";
import { NativeProviderEnrollmentError } from "../../nativeProvider/enrollment.ts";
import * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import * as Access from "../../../../mcp/McpToolAccess.ts";
import { ThreadManagementService } from "../../../../orchestration-v2/ThreadManagementService.ts";
import { liveThreadShell } from "../../../../mcp/McpToolAccess.testkit.ts";
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
const decodeRetainedArgs = Schema.decodeUnknownEffect(
  NativeForwards.retainedSourceToolkit.tools.jones_workstreams_source_retained_effect
    .parametersSchema,
);
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

class MemberPlacementTestApi extends HttpApi.make("environment").add(
  EnvironmentHttpApi.groups.workstreams,
) {}
const decodeNativePlacement = Schema.decodeUnknownEffect(
  NativePlacement.WorkstreamsNativePlacementResult,
);
const decodeReadArgs = Schema.decodeUnknownEffect(ReadWire.WorkstreamsHomeReadResultRequest);
const decodePublicReadResult = Schema.decodeUnknownEffect(ReadWire.WorkstreamsHomeReadResult);
const isPublicOperation = Schema.is(OperationWire.WorkstreamOperationResult);
const decodeAccessArgs = Schema.decodeUnknownEffect(AccessWire.WorkstreamsHomeAccessRequest);
const decodeRegistrationArgs = Schema.decodeUnknownEffect(
  RegistrationWire.WorkstreamsHomeRegistrationRequest,
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
          // Explicit external projection fixture; caller/session/enrollment/owner services stay genuine.
          Layer.mock(ThreadManagementService)({
            getThreadShell: (id) =>
              Effect.succeed(
                id === "thread:retained-source"
                  ? liveThreadShell(id, { runtimeMode: "approval-required" })
                  : null,
              ),
          }),
        );
        const resources = yield* Layer.build(
          Layer.mergeAll(
            MemberPreparation.layer,
            OperationStore.layer,
            Native.layer,
            Forwarder.locksLayer,
          ).pipe(Layer.provideMerge(sourceAuthority), Layer.provideMerge(sourceBase)),
        );
        const selectedInput = {
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
        };
        const selectedSql = Context.get(resources, SqlClient.SqlClient);
        const selectedCalls = f.calls.length;
        const changesBefore = yield* selectedSql<{ n: number }>`SELECT total_changes() AS n`;
        yield* selectedSql`PRAGMA query_only = ON`;
        NodeFS.writeFileSync(
          NodePath.join(f.M.f.baseDir, "workstreams-native.json"),
          selectedInput.text,
        );
        const selected = yield* Startup.Startup.pipe(
          Effect.provide(Startup.layer),
          Effect.provide(resources),
          Effect.provideService(HostProcess.Environment, {}),
        );
        expect(selected).toMatchObject({ mode: "native-member", problem: null });
        expect(yield* selectedSql`SELECT total_changes() AS n`).toEqual(changesBefore);
        expect(yield* selectedSql`PRAGMA query_only`).toEqual([{ query_only: 1 }]);
        expect(f.calls).toHaveLength(selectedCalls);
        expect(yield* selectedSql`SELECT * FROM jones_workstreams_registries`).toEqual([]);
        expect(yield* selectedSql`SELECT * FROM jones_workstreams_grants`).toEqual([]);
        yield* selectedSql`PRAGMA query_only = OFF`;
        expect(
          (yield* selectedSql
            .withTransaction(Startup.select(selectedInput))
            .pipe(Effect.provide(resources))).problem?.code,
        ).toBe("authority_fenced");
        let currentSourceBuild = bind.source.enrollment.context.build;
        const providerPorts = {
          ...ProviderTest.makeProviderFixture().ports,
          authority: { readCurrent: f.M.live.readCurrent },
          build: Effect.sync(() => Option.some(currentSourceBuild)),
        };
        const runtime = Runtime.NativeWorkstreamsRuntime.of({
          provider: makeWorkstreamsNativeProvider(providerPorts),
          providerV2: makeWorkstreamsNativeProvider(providerPorts, "v2"),
          enrollments: { getBySessionId: () => Effect.fail(new NativeProviderEnrollmentError()) },
        });
        if (selected.mode !== "native-member" || !selected.sourceSessionId)
          throw new Error("Genuine member selection failed.");
        const selectedServices = yield* Layer.build(
          MemberServices.layer({
            ...selected,
            mode: "native-member",
            sourceSessionId: selected.sourceSessionId,
          }).pipe(
            Layer.provide(Layer.succeed(Runtime.NativeWorkstreamsRuntime, runtime)),
            Layer.provide(Layer.succeedContext(resources)),
          ),
        );
        const app = Context.get(selectedServices, MemberApp.MemberApplication);
        const recovery = Context.get(selectedServices, MemberRecovery.MemberRecovery);
        expect(Option.isSome(Context.getOption(selectedServices, ReadRoutes.AuthorizedReads))).toBe(
          true,
        );
        expect(Option.isNone(Context.getOption(selectedServices, NativeHome))).toBe(true);
        const sourceTools = yield* NativeForwards.sourceToolkit.pipe(
          Effect.provide(
            Access.HandlersLayer.layer(NativeForwards.sourceHandlers).pipe(
              Layer.provide(Layer.succeedContext(selectedServices)),
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
        f.mountTool(f.H.env, HomePort.HOME_ACCESS_TOOL, (args) =>
          decodeAccessArgs(args).pipe(
            Effect.flatMap((decoded) =>
              homeBox.handle(HomePort.HOME_ACCESS_TOOL, decoded).pipe(
                Stream.unwrap,
                Stream.runCollect,
                Effect.map((replies) => replies.at(-1)),
              ),
            ),
            Effect.orDie,
          ),
        );
        f.mountTool(f.H.env, HomePort.HOME_REGISTRATION_TOOL, (args) =>
          decodeRegistrationArgs(args).pipe(
            Effect.flatMap((decoded) =>
              homeBox.handle(HomePort.HOME_REGISTRATION_TOOL, decoded).pipe(
                Stream.unwrap,
                Stream.runCollect,
                Effect.map((replies) => replies.at(-1)),
              ),
            ),
            Effect.orDie,
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
        // Keep one genuine home read producer across requests so its cursors retain their owner.
        // Provider dispatch is deliberately unavailable in this read-only graph.
        const readRegistry = RegistryService.layer.pipe(
          Layer.provideMerge(Integration.nativeSessionLayer),
          Layer.provideMerge(Integration.nativePersistenceLayer),
        );
        const localReads = LocalReads.layer.pipe(
          Layer.provideMerge(
            Layer.mergeAll(RegistryCounts.layer, RegistryAppearance.layer, Placements.layer).pipe(
              Layer.provideMerge(readRegistry),
            ),
          ),
          Layer.provide(
            Layer.succeed(EffectRunner.EffectRunner, {
              run: () => Effect.die("Unexpected read-side effect dispatch"),
              recover: () => Effect.die("Unexpected read-side effect recovery"),
              recoverOne: () => Effect.die("Unexpected read-side effect lookup"),
            }),
          ),
          Layer.provide(Layer.succeed(Placements.PlacementTrust, f.H.live.trustProvider)),
        );
        const homeRead = yield* HomeRead.HomeReadProtocol.pipe(
          Effect.provide(
            HomeRead.layer.pipe(
              Layer.provide(PeerRead.homeAdmissionLayer),
              Layer.provideMerge(
                ReadRoutes.layer.pipe(
                  Layer.provideMerge(ReadProjection.layer),
                  Layer.provideMerge(localReads),
                ),
              ),
              Layer.provideMerge(Integration.nativePeerReadBoundaryLayer),
              Layer.provide(homeBase),
            ),
          ),
        );
        const readBox = yield* NativeReadTools.toolkit.pipe(
          Effect.provide(
            Access.HandlersLayer.layer(NativeReadTools.handlers).pipe(
              Layer.provide(Layer.succeed(HomeRead.HomeReadProtocol, homeRead)),
            ),
          ),
        );
        f.mountTool(f.H.env, "jones_workstreams_home_read_result", (args) =>
          decodeReadArgs(args).pipe(
            Effect.flatMap((decoded) =>
              readBox.handle("jones_workstreams_home_read_result", decoded).pipe(
                Stream.unwrap,
                Stream.runCollect,
                Effect.map((replies) => replies.at(-1)),
              ),
            ),
            Effect.orDie,
          ),
        );
        const gateway = Context.get(selectedServices, PublicGateway.WorkstreamGateway);
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
        const sessionAccess = yield* app
          .access()
          .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, principal));
        expect(sessionAccess).toMatchObject({
          principal_id: (yield* Context.get(f.H.all, SqlClient.SqlClient)<{ principal_id: string }>`
            SELECT principal_id FROM jones_workstreams_principals
            WHERE registry_instance_id=${home.registry_instance_id} AND owner_id=${home.owner_id}
              AND environment_id=${f.M.env}
          `)[0]?.principal_id,
          home: { ...home, canonical_origin: "https://native.workstreams.invalid" },
          capabilities: { permissions: ["workstreams:read", "workstreams:write"] },
        });
        const registration = Context.get(
          selectedServices,
          PublicRegistration.WorkstreamsRegistrationContext,
        );
        currentSourceBuild = { ...bind.source.enrollment.context.build, sha: "c".repeat(40) };
        const registered = yield* registration
          .read()
          .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, principal));
        expect(registered).toMatchObject({
          context: {
            protocol: "workstreams-registration-context/2.0.0",
            owner_id: home.owner_id,
            principal_id: sessionAccess.principal_id,
            authorization_revision: sessionAccess.authorization_revision,
            server_generation: home.server_generation,
            sources: [
              {
                provider: "t3",
                source_instance_id: f.M.env,
                authority_namespace: bind.source.authority.authority_namespace,
                store_generation: bind.source.authority.store_generation,
                native_protocol: "workstreams-t3-provider/1.0.0",
                build: currentSourceBuild,
              },
            ],
          },
          t3Availability: { state: "ready" },
        });
        const homeGrant = yield* Context.get(f.H.all, SqlClient.SqlClient)<{ grant_id: string }>`
          SELECT grant_id FROM jones_workstreams_grants
          WHERE principal_id=${sessionAccess.principal_id}
          ORDER BY grant_version DESC LIMIT 1
        `;
        expect(registered.context.grant_id).toBe(homeGrant[0]?.grant_id);
        expect(
          registered.context.sources[0]?.provider === "t3" &&
            registered.context.sources[0].build.sha,
        ).not.toBe(bind.source.enrollment.context.build.sha);
        currentSourceBuild = bind.source.enrollment.context.build;
        expect(yield* selectedSql`SELECT * FROM jones_workstreams_registries`).toEqual([]);
        expect(yield* selectedSql`SELECT * FROM jones_workstreams_grants`).toEqual([]);
        const publicSession = yield* gateway
          .readSession()
          .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, principal));
        expect(publicSession).toMatchObject({
          registryId: "https://native.workstreams.invalid",
          principalId: sessionAccess.principal_id,
          permissions: ["workstreams:read", "workstreams:write"],
        });
        const emptyPage = yield* gateway
          .readMetadata({ limit: 1 })
          .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, principal));
        expect(emptyPage.items).toEqual([]);
        expect(emptyPage.binding).toEqual(publicSession);
        expect(
          yield* gateway
            .readRegistryCounts()
            .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, principal)),
        ).toMatchObject({ counts: { total: 0 }, binding: publicSession });
        const readOnlySession = yield* auth.issueSession({
          subject: "member-readonly",
          scopes: ["orchestration:read"],
        });
        const readOnlyAuth = yield* auth.authenticateHttpRequest(
          HttpRequest.fromWeb(
            new Request("https://member.invalid/api/workstreams", {
              headers: { authorization: "Bearer " + readOnlySession.token },
            }),
          ),
        );
        expect(
          (yield* app.access().pipe(
            Effect.provideService(EnvironmentAuthenticatedPrincipal, {
              ...readOnlyAuth,
              scopes: new Set(readOnlyAuth.scopes),
            }),
          )).capabilities.permissions,
        ).toEqual(["workstreams:read"]);
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
        const operations = Context.get(selectedServices, PublicOperations.WorkstreamOperations);
        const result = yield* operations
          .submit({ request, wait_ms: 0 })
          .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, principal));
        expect(result).toMatchObject({
          state: "committed",
          kind: "create_workstream",
          operation_id: request.operation_id,
        });
        expect(isPublicOperation(result)).toBe(true);
        expect(result.request_sha256).toBe(sha256Bytes(canonicalSnapshot(request)));
        const repeated = yield* operations
          .submit({ request, wait_ms: 0 })
          .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, principal));
        expect(repeated).toEqual(result);
        expect(
          yield* operations
            .read({ operation_id: request.operation_id })
            .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, principal)),
        ).toEqual({ operation: result });
        expect(
          yield* operations.read({ operation_id: request.operation_id }).pipe(
            Effect.provideService(EnvironmentAuthenticatedPrincipal, {
              ...readOnlyAuth,
              scopes: new Set(readOnlyAuth.scopes),
            }),
          ),
        ).toEqual({ operation: null });
        expect(
          yield* operations
            .setupStatus()
            .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, principal)),
        ).toMatchObject({
          activation: { state: "enabled" },
          registry: { state: "ready", registry_id: home.registry_instance_id, can_write: true },
          authority_effect: "none",
        });
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
        const publicPage = yield* gateway
          .readMetadata({ limit: 1 })
          .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, principal));
        expect(publicPage.items).toHaveLength(1);
        const publicId = publicPage.items[0]!.workstreamId;
        expect(
          yield* gateway
            .readDetail(publicId)
            .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, principal)),
        ).toMatchObject({ workstream: { workstream_id: publicId, name: request.workstream.name } });
        const readOnlyPrincipal = { ...readOnlyAuth, scopes: new Set(readOnlyAuth.scopes) };
        expect(
          (yield* gateway
            .readMetadata()
            .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, readOnlyPrincipal)))
            .items,
        ).toHaveLength(1);
        expect(yield* app.recoverOpen()).toEqual([]);
        const lostRequest = { ...request, operation_id: "operation:composed-lost-reply-01" };
        f.state.loseForwardReply = true;
        const lost = yield* operations
          .submit({ request: lostRequest, wait_ms: 0 })
          .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, principal));
        expect(lost.state).toBe("reconciling");
        expect(
          yield* operations
            .read({ operation_id: lostRequest.operation_id })
            .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, principal)),
        ).toEqual({ operation: lost });
        expect(
          yield* operations
            .read({ open: true })
            .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, principal)),
        ).toEqual({ operations: [lost] });
        const otherSession = yield* auth.issueSession({
          subject: "other-member-reader",
          scopes: ["orchestration:read", "orchestration:operate"],
        });
        const otherAuth = yield* auth.authenticateHttpRequest(
          HttpRequest.fromWeb(
            new Request("https://member.invalid/api/workstreams", {
              headers: { authorization: "Bearer " + otherSession.token },
            }),
          ),
        );
        const otherPrincipal = { ...otherAuth, scopes: new Set(otherAuth.scopes) };
        const visibleCalls = f.calls.length;
        expect(
          yield* operations
            .read({ operation_id: lostRequest.operation_id })
            .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, otherPrincipal)),
        ).toEqual({ operation: null });
        expect(
          yield* operations
            .read({ open: true })
            .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, otherPrincipal)),
        ).toEqual({ operations: [] });
        expect(
          f.calls.slice(visibleCalls).every((call) => call.tool === HomePort.HOME_ACCESS_TOOL),
        ).toBe(true);
        expect(yield* hsql`SELECT state FROM jones_workstreams_forward_receipts`).toEqual([
          { state: "completed" },
          { state: "completed" },
        ]);
        const forwards = f.calls.filter((call) => call.tool === HomePort.HOME_FORWARD_TOOL).length;
        f.state.loseForwardReply = false;
        // The mounted background owner has no HTTP/MCP caller. It readmits the original retained one.
        expect(yield* recovery.sweep()).toEqual([]);
        expect(
          yield* msql`SELECT state FROM jones_workstreams_operations WHERE operation_id=${lostRequest.operation_id}`,
        ).toEqual([{ state: "reconciling" }]);
        yield* TestClock.adjust(30_000);
        // Join any timer-owned sweep, so a queued SQL callback cannot masquerade as completion.
        yield* recovery.sweep();
        const recovered = yield* app
          .readOperation(lostRequest.operation_id)
          .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, principal));
        expect(recovered).toMatchObject({
          operation_id: lostRequest.operation_id,
          state: "committed",
        });
        expect(f.calls.filter((call) => call.tool === HomePort.HOME_FORWARD_TOOL)).toHaveLength(
          forwards,
        );
        expect(yield* app.recoverOpen()).toEqual([]);
        expect(
          yield* operations
            .read({ operation_id: lostRequest.operation_id })
            .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, principal)),
        ).toMatchObject({ operation: { state: "committed" } });
        expect(
          yield* operations
            .read({ open: true })
            .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, principal)),
        ).toEqual({ operations: [] });
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
        const revokedRequest = {
          ...request,
          operation_id: "operation:composed-revoked-recovery-01",
        };
        f.state.loseForwardReply = true;
        expect(
          (yield* app
            .submitOperation({ request: revokedRequest, wait_ms: 0 })
            .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, principal))).state,
        ).toBe("reconciling");
        f.state.loseForwardReply = false;
        const retainedCalls = f.calls.length;
        expect(yield* auth.revokeSession(issued.sessionId)).toBe(true);
        yield* TestClock.adjust(30_000);
        yield* recovery.sweep();
        expect(
          yield* msql`SELECT state FROM jones_workstreams_operations WHERE operation_id=${revokedRequest.operation_id}`,
        ).toEqual([{ state: "reconciling" }]);
        expect(f.calls).toHaveLength(retainedCalls);
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
        expect(
          yield* registration
            .read()
            .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, principal), Effect.flip),
        ).toMatchObject({ reason: "registry_unavailable" });
        expect(f.calls).toHaveLength(retainedCalls);
        expect(
          yield* gateway
            .readMetadata()
            .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, principal), Effect.flip),
        ).toMatchObject({ reason: "authorization-not-current" });
        expect(f.calls).toHaveLength(retainedCalls);
        expect(
          (yield* gateway
            .readMetadata()
            .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, readOnlyPrincipal)))
            .items.length,
        ).toBeGreaterThan(0);
        expect(yield* hsql`SELECT workstream_id FROM jones_workstreams_records`).toHaveLength(3);
        expect(yield* msql`SELECT state FROM jones_workstreams_operations`).toEqual([
          { state: "committed" },
          { state: "committed" },
          { state: "reconciling" },
        ]);

        // Seed one exact raw source intent through genuine admission and encrypted storage.
        // This harness qualifies the member-side producer; initial raw home dispatch has separate evidence.
        const nextSession = yield* auth.issueSession({
          subject: "retained-source-application",
          scopes: ["orchestration:read", "orchestration:operate"],
        });
        const nextAuth = yield* auth.authenticateHttpRequest(
          HttpRequest.fromWeb(
            new Request("https://member.invalid/api/workstreams", {
              headers: { authorization: "Bearer " + nextSession.token },
            }),
          ),
        );
        const nextPrincipal = { ...nextAuth, scopes: new Set(nextAuth.scopes) };
        const placementOperation = yield* operations
          .submit({
            request: {
              contract: "workstream-operations/1.0.0",
              operation_id: "operation:member-placement-application-01",
              origin: { kind: "app" },
              kind: "set_primary",
              thread: { environment_id: f.M.env, thread_id: "thread:retained-source" },
              target: {
                kind: "workstream",
                registry_id: home.registry_instance_id,
                owner_id: home.owner_id,
                workstream_id: publicId,
              },
              expected: { kind: "unassigned" },
            },
            wait_ms: 0,
          })
          .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, nextPrincipal));
        expect(placementOperation).toMatchObject({
          state: "committed",
          kind: "set_primary",
          result: { membership_id: expect.any(String) },
        });
        if (placementOperation.state !== "committed")
          return yield* Effect.die("Genuine member primary placement did not commit");
        const currentSourceBindings = yield* hsql<{ binding_version: number }>`
          SELECT binding_version FROM jones_workstreams_source_bindings
          WHERE provider='t3' AND source_instance_id=${f.M.env}
          ORDER BY binding_version DESC LIMIT 1
        `;

        // Mount the real public group and real bearer middleware over the selected member graph.
        // Requests run in-process; no listener or installed application is started.
        const httpClock = yield* Clock.Clock;
        const httpAuthentication = yield* Layer.build(
          AuthHttp.layerAuthenticatedAuth.pipe(
            Layer.provide(Layer.succeed(Auth.EnvironmentAuth, auth)),
          ),
        );
        const authenticate = Context.get(httpAuthentication, EnvironmentAuthenticatedAuth);
        const placementRoutes = HttpApiBuilder.layer(MemberPlacementTestApi).pipe(
          Layer.provide(workstreamHttpApiLayer),
          Layer.provide(Layer.succeedContext(selectedServices)),
          Layer.provide(
            Layer.succeed(EnvironmentAuthenticatedAuth, (effect, endpoint) =>
              authenticate(effect, endpoint).pipe(Effect.provideService(Clock.Clock, httpClock)),
            ),
          ),
          Layer.provide(workstreamResponseHeadersLayer),
          Layer.provideMerge(
            HttpPlatform.layer.pipe(
              Layer.provideMerge(NodeServices.layer),
              Layer.provideMerge(Etag.layerWeak),
            ),
          ),
          Layer.provide(NodeServices.layer),
        );
        const placementHttp = yield* Effect.acquireRelease(
          Effect.sync(() => HttpRouter.toWebHandler(placementRoutes, { disableLogger: true })),
          (value) => Effect.promise(() => value.dispose()),
        );
        const requestPlacements = (token: string | undefined, contract?: string) =>
          Effect.promise(() =>
            placementHttp.handler(
              new Request("https://member.invalid/api/workstreams/thread-placements", {
                method: "POST",
                headers: {
                  "content-type": "application/json",
                  ...(token === undefined ? {} : { authorization: "Bearer " + token }),
                  ...(contract === undefined
                    ? {}
                    : { "x-t3-workstreams-placement-contract": contract }),
                },
                body: JSON.stringify({
                  identities: [
                    { source_instance_id: f.M.env, native_thread_id: "thread:retained-source" },
                  ],
                }),
              }),
            ),
          );
        const beforePlacementReads = f.calls.filter(
          (c) => c.tool === HomePort.HOME_FORWARD_TOOL,
        ).length;
        const placementResponse = yield* requestPlacements(
          nextSession.token,
          "workstreams-t3-placement/2.0.0",
        );
        expect(placementResponse.status).toBe(200);
        expect(placementResponse.headers.get("cache-control")).toBe("private, no-store");
        const nativePlacement = yield* decodeNativePlacement(
          yield* Effect.promise(() => placementResponse.json()),
        );
        expect(nativePlacement.page.items).toHaveLength(1);
        expect(nativePlacement.page.items[0]).toMatchObject({
          membership_id: placementOperation.result.membership_id,
          workstream_id: publicId,
          source_instance_id: f.M.env,
          native_thread_id: "thread:retained-source",
          kind: "primary",
          attestation_source: "native",
          evidence_contract: "workstreams-t3-provider/2.0.0",
          authority_namespace: bind.source.authority.authority_namespace,
          store_generation: bind.source.authority.store_generation,
          source_binding_version: currentSourceBindings[0]?.binding_version,
        });
        expect(nativePlacement.page.provenance).toMatchObject({
          authority: "native-home",
          registry_instance_id: home.registry_instance_id,
          home_environment_id: f.H.env,
          home_generation: home.server_generation,
          served_by_environment_id: f.H.env,
        });
        expect(nativePlacement.page.next_cursor).toBeNull();
        // This fixture's independent local trust provider is empty. Remote evidence must not fill it.
        expect(nativePlacement.trustedEnvironments).toEqual([]);
        const readOnlyPlacement = yield* requestPlacements(
          readOnlySession.token,
          "workstreams-t3-placement/2.0.0",
        );
        expect(readOnlyPlacement.status).toBe(200);
        expect(
          (yield* decodeNativePlacement(yield* Effect.promise(() => readOnlyPlacement.json()))).page
            .items,
        ).toEqual(nativePlacement.page.items);
        expect((yield* requestPlacements(undefined, "workstreams-t3-placement/2.0.0")).status).toBe(
          401,
        );
        expect(
          (yield* requestPlacements(nextSession.token, "workstreams-t3-placement/3.0.0")).status,
        ).toBe(400);
        // The old Gateway codec remains unchanged and cannot erase this native proof to accept it.
        expect((yield* requestPlacements(nextSession.token)).status).toBe(500);
        expect(f.calls.filter((c) => c.tool === HomePort.HOME_FORWARD_TOOL)).toHaveLength(
          beforePlacementReads,
        );
        expect(yield* msql`SELECT * FROM jones_workstreams_registries`).toEqual([]);
        expect(yield* msql`SELECT * FROM jones_workstreams_grants`).toEqual([]);

        const requestNativeRead = (token: string | undefined, payload: unknown) =>
          Effect.promise(() =>
            placementHttp.handler(
              new Request("https://member.invalid/api/workstreams/native/v1/read", {
                method: "POST",
                headers: {
                  "content-type": "application/json",
                  ...(token === undefined ? {} : { authorization: "Bearer " + token }),
                },
                body: JSON.stringify(payload),
              }),
            ),
          );
        const countsRequest = nativeReadRequest({ route: "counts", params: {} });
        const countsResponse = yield* requestNativeRead(nextSession.token, countsRequest);
        expect(countsResponse.status).toBe(200);
        expect(countsResponse.headers.get("cache-control")).toBe("private, no-store");
        expect(countsResponse.headers.get("x-content-type-options")).toBe("nosniff");
        const countsResult = yield* decodePublicReadResult(
          yield* Effect.promise(() => countsResponse.json()),
        );
        expect(countsResult).toMatchObject({
          route: "counts",
          envelope: {
            registry: { registry_instance_id: home.registry_instance_id },
            provenance: {
              home_environment_id: f.H.env,
              served_by_environment_id: f.H.env,
              authority: "native-home",
              source: "live",
              stale: false,
            },
          },
          value: { active: 3, total: 3 },
        });

        // Two separate public requests must retain the same real home snapshot lease.
        const firstPageResponse = yield* requestNativeRead(
          nextSession.token,
          nativeReadRequest({ route: "workstreams", params: { limit: 1 } }),
        );
        expect(firstPageResponse.status).toBe(200);
        const firstPage = yield* decodePublicReadResult(
          yield* Effect.promise(() => firstPageResponse.json()),
        );
        if (firstPage.route !== "workstreams" || firstPage.value.next_cursor === null)
          return yield* Effect.die("Expected a genuine multi-page home collection");
        const continuation = nativeReadRequest({
          route: "workstreams",
          params: { limit: 1, cursor: firstPage.value.next_cursor },
        });
        yield* TestClock.adjust(1);
        const secondPageResponse = yield* requestNativeRead(nextSession.token, continuation);
        expect(secondPageResponse.status).toBe(200);
        const secondPage = yield* decodePublicReadResult(
          yield* Effect.promise(() => secondPageResponse.json()),
        );
        expect(secondPage.envelope).toEqual(firstPage.envelope);
        expect(secondPage.authorization_revision).toBe(firstPage.authorization_revision);

        // Exercise the unchanged shared client through actual authentication/HTTP/member/peer/home layers.
        // This explicit test scope is not an environment capability advertisement.
        const publicReadBinding = (yield* gateway
          .readMetadata()
          .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, nextPrincipal))).binding;
        const { registry_version: _registryVersion, ...httpReadRegistry } =
          countsResult.envelope.registry;
        const readScope = {
          capability: {
            contract: "workstreams-native/1.0.0" as const,
            mode: "native-member" as const,
            registry: {
              ...httpReadRegistry,
              home_environment_id: countsResult.envelope.provenance.home_environment_id,
            },
            home_link: "ok" as const,
            reads: ["workstreams-read/1.0.0" as const],
            placement_proofs: ["workstreams-t3-placement/2.0.0" as const],
          },
          binding: publicReadBinding,
        };
        const readQueries: ReadWire.WorkstreamsHomeReadResultRequest["request"][] = [
          { route: "workstreams", params: { limit: 1 } },
          { route: "references", params: { limit: 1 } },
          { route: "read-projection", params: { limit: 1 } },
          { route: "memberships", params: { workstream_id: publicId, limit: 1 } },
          { route: "declarations", params: { workstream_id: publicId, limit: 1 } },
          { route: "lifecycle-declarations", params: { workstream_id: publicId, limit: 1 } },
          { route: "edges", params: { workstream_id: publicId, limit: 1 } },
          { route: "history", params: { workstream_id: publicId, limit: 1 } },
          {
            route: "placements",
            params: {
              limit: 1,
              identities: [
                { source_instance_id: f.M.env, native_thread_id: "thread:retained-source" },
              ],
            },
          },
        ];
        const readBodies: ReadWire.WorkstreamsHomeReadResultRequest[] = [];
        const reader = new NativeWorkstreamReader({
          scope: readScope,
          sha256: async (bytes) => sha256Bytes(bytes),
          read: async (body, signal) => {
            readBodies.push(body);
            const response = await placementHttp.handler(
              new Request("https://member.invalid/api/workstreams/native/v1/read", {
                method: "POST",
                headers: {
                  "content-type": "application/json",
                  authorization: "Bearer " + nextSession.token,
                },
                body: JSON.stringify(body),
                ...(signal === undefined ? {} : { signal }),
              }),
            );
            if (!response.ok)
              throw Object.assign(new Error("Native HTTP read refused."), {
                status: response.status,
              });
            return response.json();
          },
        });
        yield* Effect.promise(async () => {
          for (const query of readQueries) {
            const complete = await reader.complete(
              nativeReadRequest(
                query,
                query.route === "placements" ? "workstreams-t3-placement/2.0.0" : undefined,
              ),
            );
            expect(complete.stale).toBe(false);
            expect(complete.envelope.coverage.registry).toBe("complete");
            expect(
              complete.pages.every(
                (page) =>
                  page.envelope.provenance.observed_at === complete.envelope.provenance.observed_at,
              ),
            ).toBe(true);
            if (query.route === "workstreams" || query.route === "read-projection") {
              expect(complete.items).toHaveLength(3);
              expect(complete.pages).toHaveLength(3);
            }
            if (query.route === "placements")
              expect(complete.items).toMatchObject([
                {
                  membership_id: placementOperation.result.membership_id,
                  attestation_source: "native",
                  evidence_contract: "workstreams-t3-provider/2.0.0",
                },
              ]);
          }
        });
        expect(readBodies.some((body) => "cursor" in body.request.params)).toBe(true);

        // The home-only export consumes the same production projection/admission factories.
        // External owner/build fixtures and in-process HTTP remain disclosed; no capability is advertised.
        const homeReadServices = Layer.mergeAll(ReadExport.layer, ReadRoutes.authorizedLayer).pipe(
          Layer.provideMerge(ReadRoutes.layer),
          Layer.provideMerge(ReadProjection.layer),
          Layer.provideMerge(HomeAdmission.layer),
          Layer.provide(HomeAdmission.callerBoundaryLayer),
          Layer.provideMerge(localReads),
          Layer.provide(homeBase),
        );
        const homeReadContext = yield* Layer.build(homeReadServices);
        // This peer-only fixture initialized registry/grants without a home-local app binding.
        // Seed that isolated home setup through the real CAS store before exercising app reads.
        const homeBindings = Context.get(homeReadContext, Bindings.BindingStore);
        expect(yield* homeBindings.read()).toBeNull();
        const homeExportStore = Context.get(homeReadContext, Registry.RegistryStore);
        yield* homeExportStore.appendPlain(Test.scope, "principals", {
          principal_id: "principal:home-export",
          kind: "environment",
          environment_id: f.H.env,
          provenance: "{}",
        });
        yield* homeExportStore.appendPlain(Test.scope, "grants", {
          grant_id: "grant:home-export",
          principal_id: "principal:home-export",
          grant_version: 1,
          permissions: '["workstreams:read"]',
          state: "active",
        });
        yield* homeBindings.bind(
          {
            registry_instance_id: home.registry_instance_id,
            owner_id: home.owner_id,
            home_environment_id: f.H.env,
            home_generation: home.server_generation,
            principal_id: "principal:home-export",
            binding_version: 1,
            state: "active",
          },
          null,
        );
        expect(yield* hsql`SELECT principal_id FROM jones_workstreams_binding_state`).toEqual([
          { principal_id: "principal:home-export" },
        ]);
        const homeAuth = Context.get(f.H.all, Auth.EnvironmentAuth);
        const exportSession = yield* homeAuth.issueSession({
          subject: "native-export-reader",
          scopes: ["orchestration:read"],
        });
        const exportAuthenticated = yield* homeAuth.authenticateHttpRequest(
          HttpRequest.fromWeb(
            new Request("https://home.invalid/api/workstreams/native/v1/export", {
              headers: { authorization: "Bearer " + exportSession.token },
            }),
          ),
        );
        const exportPrincipal = {
          ...exportAuthenticated,
          scopes: new Set(exportAuthenticated.scopes),
        };
        const homeMiddleware = yield* Layer.build(
          AuthHttp.layerAuthenticatedAuth.pipe(
            Layer.provide(Layer.succeed(Auth.EnvironmentAuth, homeAuth)),
          ),
        );
        const authenticateHome = Context.get(homeMiddleware, EnvironmentAuthenticatedAuth);
        const exportRoutes = HttpApiBuilder.layer(MemberPlacementTestApi).pipe(
          Layer.provide(workstreamHttpApiLayer),
          Layer.provide(Layer.succeedContext(Context.merge(selectedServices, homeReadContext))),
          Layer.provide(
            Layer.succeed(EnvironmentAuthenticatedAuth, (effect, endpoint) =>
              authenticateHome(effect, endpoint).pipe(
                Effect.provideService(Clock.Clock, httpClock),
              ),
            ),
          ),
          Layer.provide(workstreamResponseHeadersLayer),
          Layer.provideMerge(
            HttpPlatform.layer.pipe(
              Layer.provideMerge(NodeServices.layer),
              Layer.provideMerge(Etag.layerWeak),
            ),
          ),
          Layer.provide(NodeServices.layer),
        );
        const exportHttp = yield* Effect.acquireRelease(
          Effect.sync(() => HttpRouter.toWebHandler(exportRoutes, { disableLogger: true })),
          (value) => Effect.promise(() => value.dispose()),
        );
        const exportRequest = (token?: string) =>
          Effect.promise(() =>
            exportHttp.handler(
              new Request("https://home.invalid/api/workstreams/native/v1/export?page_size=1", {
                headers: token === undefined ? {} : { authorization: "Bearer " + token },
              }),
            ),
          );
        const homeCounts = yield* Context.get(homeReadContext, ReadRoutes.AuthorizedReads)
          .read({ route: "counts", params: {} })
          .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, exportPrincipal));
        if (homeCounts.route !== "counts") return yield* Effect.die("Expected genuine home counts");
        const homeBinding = yield* Context.get(homeReadContext, Bindings.BindingStore).read();
        if (!homeBinding) return yield* Effect.die("Missing genuine home singleton");
        const exportScope = {
          capability: {
            contract: readScope.capability.contract,
            mode: "native-home" as const,
            registry: readScope.capability.registry,
            reads: readScope.capability.reads,
            placement_proofs: readScope.capability.placement_proofs,
          },
          binding: {
            ...publicReadBinding,
            principalId: homeBinding.principal_id,
            authorizationRevision: homeCounts.authorization_revision,
            registryVersion: homeCounts.envelope.registry.registry_version,
            permissions: ["workstreams:read" as const],
          },
        };
        const expectedExport = yield* Stream.runCollect(
          Context.get(homeReadContext, ReadExport.ReadExport).stream(1),
        ).pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, exportPrincipal));
        const exportCalls = f.calls.length;
        const exportResponse = yield* exportRequest(exportSession.token);
        expect(exportResponse.status).toBe(200);
        expect(exportResponse.headers.get("content-type")).toContain("application/x-ndjson");
        expect(exportResponse.headers.get("cache-control")).toBe("private, no-store");
        expect(exportResponse.headers.get("x-content-type-options")).toBe("nosniff");
        const exportBytes = yield* Effect.promise(
          async () => new Uint8Array(await exportResponse.arrayBuffer()),
        );
        expect(exportBytes).toEqual(new TextEncoder().encode(expectedExport.join("")));
        const completedExport = yield* Effect.promise(() =>
          validateNativeReadExport(
            (async function* () {
              // Split arbitrary byte/line boundaries; the validator consumes the original bytes.
              for (let i = 0; i < exportBytes.length; i += 7) yield exportBytes.slice(i, i + 7);
            })(),
            { scope: exportScope, sha256: async (bytes) => sha256Bytes(bytes) },
          ),
        );
        expect(completedExport.complete).toBe(true);
        expect(completedExport.pages).toHaveLength(3);
        expect(completedExport.pages.flatMap((page) => page.items)).toHaveLength(3);
        // The CLI uses the same actual signer/reservation machinery with a fixed read-only purpose.
        const readCliSessions = Context.get(
          yield* Layer.build(Cli.existingReadExportCliSessionsLayer).pipe(Effect.provide(f.H.all)),
          Cli.PeerSetupCliSessions,
        );
        const cliOutput: Uint8Array[] = [];
        const cliStdio = Stdio.make({
          args: Effect.succeed([]),
          stdin: Stream.empty,
          stdout: () =>
            Sink.forEach((part: string | Uint8Array) =>
              Effect.sync(() => {
                cliOutput.push(
                  typeof part === "string" ? new TextEncoder().encode(part) : part.slice(),
                );
              }),
            ),
          stderr: () => Sink.drain,
        });
        const cliHttp = HttpClient.make((request) =>
          Effect.gen(function* () {
            const web = new Request(request.url, {
              method: request.method,
              headers: request.headers,
              ...(request.body._tag === "Uint8Array"
                ? { body: Buffer.from(request.body.body) }
                : {}),
            });
            const response = yield* Effect.promise(() => exportHttp.handler(web));
            return HttpResponse.fromWeb(request, response);
          }),
        );
        const cliIssued = yield* readCliSessions.issue();
        expect(
          (yield* Context.get(f.H.all, Sessions.SessionStore).verify(cliIssued.token)).scopes,
        ).toEqual(["orchestration:read"]);
        yield* NativeExportCli.runNativeReadExportCli(
          "http://127.0.0.1:1111",
          { authorization: "Bearer " + cliIssued.token },
          1,
        ).pipe(
          Effect.provideService(HttpClient.HttpClient, cliHttp),
          Effect.provideService(Stdio.Stdio, cliStdio),
          Effect.ensuring(readCliSessions.revoke(cliIssued.sessionId).pipe(Effect.orDie)),
        );
        expect(Buffer.concat(cliOutput)).toEqual(Buffer.from(exportBytes));
        expect(
          (yield* Context.get(f.H.all, Sessions.SessionStore)
            .verify(cliIssued.token)
            .pipe(Effect.result))._tag,
        ).toBe("Failure");
        expect((yield* exportRequest()).status).toBe(401);
        expect(yield* homeAuth.revokeSession(exportSession.sessionId)).toBe(true);
        expect((yield* exportRequest(exportSession.token)).status).toBe(401);
        expect(f.calls).toHaveLength(exportCalls);
        // A member has no exporter mounted; successful reads do not imply export support.
        const memberExport = yield* Effect.promise(() =>
          placementHttp.handler(
            new Request("https://member.invalid/api/workstreams/native/v1/export", {
              headers: { authorization: "Bearer " + nextSession.token },
            }),
          ),
        );
        expect(memberExport.status).toBe(409);
        expect(yield* Effect.promise(() => memberExport.json())).toEqual({
          _tag: "EnvironmentHttpConflictError",
          message: "workstreams_native_unavailable",
        });
        expect(f.calls).toHaveLength(exportCalls);

        expect((yield* requestNativeRead(readOnlySession.token, countsRequest)).status).toBe(200);
        expect((yield* requestNativeRead(undefined, countsRequest)).status).toBe(401);
        expect(
          (yield* requestNativeRead(nextSession.token, {
            ...countsRequest,
            contract: "workstreams-native-home-read-result/2.0.0",
          })).status,
        ).toBe(400);
        expect(
          (yield* requestNativeRead(nextSession.token, {
            ...countsRequest,
            caller: { principal_id: "forged" },
          })).status,
        ).toBe(400);
        expect(
          (yield* requestNativeRead(nextSession.token, {
            ...countsRequest,
            placement_contract: "workstreams-t3-placement/2.0.0",
          })).status,
        ).toBe(400);
        expect(
          (yield* requestNativeRead(nextSession.token, {
            ...countsRequest,
            request: { route: "counts", params: { home: "forged" } },
          })).status,
        ).toBe(400);
        expect(f.calls.filter((c) => c.tool === HomePort.HOME_FORWARD_TOOL)).toHaveLength(
          beforePlacementReads,
        );

        const retained = yield* Context.get(resources, MemberPreparation.MemberPreparation)
          .withPrepared((members) =>
            Effect.flatMap(Effect.context<never>(), (requestContext) =>
              Effect.gen(function* () {
                expect(
                  (yield* members.getValidatedBySessionId(
                    bind.source.enrollment.session.session_id,
                  ))._tag,
                ).toBe("Some");
                expect(
                  Option.isSome(yield* Effect.serviceOption(EnvironmentAuthenticatedPrincipal)),
                ).toBe(true);
                expect(Option.isSome(yield* Effect.serviceOption(Mcp.McpInvocationContext))).toBe(
                  false,
                );
                expect(yield* SqlClient.SqlClient).toBe(msql);
                const admission = yield* Admission.MemberAdmission;
                const caller = yield* admission.captureWrite({
                  kind: "thread",
                  environment_id: f.M.env,
                  thread_id: "thread:retained-source",
                });
                yield* (yield* admission.readmitWrite(caller)).checkCurrent();
                const a = caller.association;
                const id = "command:retained-source-application-01";
                const body =
                  JSON.stringify(
                    {
                      command_id: id,
                      expected_server_generation: a.home.server_generation,
                      expected_registry_version: 0,
                      action: {
                        operation: "verify_reference",
                        native_reference_id: "reference:retained-source",
                        expected_attestation_version: 0,
                      },
                    },
                    null,
                    2,
                  ) + "\n";
                const identity: ForwardWire.WorkstreamsForwardIdentity = {
                  forward_id: "jwf-retained-source-application-01",
                  source_operation_id: id,
                  parent_operation_id: null,
                  command_ids: [id],
                  request_sha256: sha256Bytes(body),
                  recorded_caller_sha256: sha256Bytes(canonicalSnapshot(caller)),
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
                const forward: ForwardWire.WorkstreamsForwardRequest = {
                  contract: ForwardWire.WORKSTREAMS_NATIVE_FORWARDING_CONTRACT,
                  kind: "command",
                  identity,
                  request: {
                    expected_home: a.home,
                    body,
                    body_sha256: sha256Bytes(body),
                    relay: {
                      source_environment_id: a.source.environment_id,
                      origin: caller.local.origin,
                      linked_origin_environment_id: null,
                      hop: 1,
                    },
                  },
                };
                const envelope = JSON.stringify(forward, null, 2) + "\n";
                yield* Context.get(resources, OperationStore.OperationStore).acceptMemberForward({
                  operation_id: id,
                  kind: "command",
                  thread_environment_id: f.M.env,
                  thread_id: "thread:retained-source",
                  origin: caller.local.origin,
                  destination: {
                    registry_instance_id: a.home.registry_instance_id,
                    owner_id: a.home.owner_id,
                    home_environment_id: a.home.home_environment_id,
                    home_generation: a.home.server_generation,
                    principal_id: a.principal_id,
                    binding_version: a.singleton_binding_version,
                  },
                  child_command_ids: [id],
                  request_body: body,
                  accepted_at: Test.now,
                  member_forward: {
                    schema: "jones-workstreams-member-forward-accepted/1",
                    kind: "MEMBER-FORWARD",
                    forward_request_body: envelope,
                    forward_request_sha256: sha256Bytes(envelope),
                    recorded_caller: caller,
                  },
                });
                return { caller, identity };
              }).pipe(
                Effect.provide(
                  Layer.fresh(Integration.nativeMemberAdmissionRequestLayer).pipe(
                    Layer.provideMerge(
                      Layer.succeedContext(
                        Context.merge(
                          requestContext,
                          Context.make(Startup.Startup, selected).pipe(
                            Context.add(SqlClient.SqlClient, msql),
                            Context.add(
                              AuthRows.AuthSessionRepository,
                              Context.get(resources, AuthRows.AuthSessionRepository),
                            ),
                            Context.add(
                              ThreadManagementService,
                              Context.get(resources, ThreadManagementService),
                            ),
                          ),
                        ).pipe(Context.add(Members.MemberEnrollments, members)),
                      ),
                    ),
                  ),
                ),
              ),
            ),
          )
          .pipe(
            Effect.provideService(SqlClient.SqlClient, msql),
            Effect.provideService(EnvironmentAuthenticatedPrincipal, nextPrincipal),
          );
        const retainedToolkit = yield* NativeForwards.retainedSourceToolkit.pipe(
          Effect.provide(
            Access.HandlersLayer.layer(NativeForwards.retainedSourceHandlers).pipe(
              Layer.provide(Layer.succeedContext(selectedServices)),
            ),
          ),
        );
        f.mountTool(f.M.env, RetainedPort.SOURCE_RETAINED_EFFECT_TOOL, (args) =>
          decodeRetainedArgs(args).pipe(
            Effect.flatMap((decoded) =>
              retainedToolkit.handle(RetainedPort.SOURCE_RETAINED_EFFECT_TOOL, decoded).pipe(
                Stream.unwrap,
                Stream.runCollect,
                Effect.map((replies) => replies.at(-1)),
              ),
            ),
            Effect.orDie,
          ),
        );
        const a = retained.caller.association;
        const carrier: RetainedWire.WorkstreamsRetainedSourceEffectRequestV1 = {
          contract: RetainedWire.WORKSTREAMS_SOURCE_RETAINED_EFFECT_CONTRACT_V1,
          source_readmission: {
            contract: RawWire.WORKSTREAMS_SOURCE_READMISSION_CONTRACT_V2,
            accepted_forward: retained.identity,
            command_id: retained.identity.command_ids[0]!,
            command_request_sha256: retained.identity.request_sha256,
          },
          receiving_home_pin_id: a.home_pin_id,
          receiving_home_pin_revision: a.home_pin_bound_revision,
          kind: "attest",
          request: {
            owner_id: a.home.owner_id,
            principal_id: a.principal_id,
            identity: {
              provider: "t3",
              resource_kind: "thread",
              source_instance_id: f.M.env,
              id_kind: "internal",
              native_id: "thread:retained-source",
              account_provenance: { kind: "not_account_scoped" },
            },
            expected_authority_namespace: a.source.authority_namespace,
            expected_store_generation: a.source.store_generation,
          },
        };
        const memberEnvironment = EnvironmentId.make(f.M.env);
        const generation = yield* Context.get(f.H.all, PeerLinks.PeerLinks).currentGeneration(
          memberEnvironment,
        );
        if (Option.isNone(generation))
          return yield* Effect.die("Missing genuine outbound generation");
        const invokeRetained = () =>
          Context.get(f.H.all, PeerClient.PeerMcpClient).call({
            environmentId: memberEnvironment,
            expectedGeneration: generation.value,
            tool: RetainedPort.SOURCE_RETAINED_EFFECT_TOOL,
            arguments: { ...carrier },
            limits: { runtimeMode: "full-access", interactionMode: "default" },
            success: RetainedWire.WorkstreamsRetainedSourceEffectResultV1,
          });
        expect(yield* invokeRetained()).toMatchObject({
          state: "result",
          request: carrier,
          result: { state: "attested", request: carrier.request },
        });

        const publicWrites = () =>
          f.calls.filter((call) => call.tool === HomePort.HOME_FORWARD_TOOL).length;
        const gatewaySession = yield* gateway
          .readSession()
          .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, nextPrincipal));
        const rawCreate = {
          command_id: "command:gateway-create-application-01",
          expected_server_generation: gatewaySession.serverGeneration,
          expected_registry_version: gatewaySession.registryVersion,
          action: {
            operation: "create_workstream" as const,
            name: "Gateway member write",
            lifecycle: "active" as const,
            progress: { state: "unknown" as const },
            sort_order: 4,
          },
        };
        const beforeGatewayWrite = publicWrites();
        const rawReceipt = yield* gateway
          .submit(rawCreate)
          .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, nextPrincipal));
        expect(rawReceipt).toMatchObject({
          command_id: rawCreate.command_id,
          state: "committed",
          operation: "create_workstream",
          registry_version: gatewaySession.registryVersion + 1,
        });
        expect(publicWrites()).toBe(beforeGatewayWrite + 1);
        const rawRow = yield* Context.get(
          resources,
          OperationStore.OperationStore,
        ).readMemberForward(rawCreate.command_id);
        expect(rawRow?.state).toBe("committed");
        if (!rawRow) return yield* Effect.die("Missing genuine Gateway intent");
        expect(rawReceipt.request_sha256).toBe(sha256Bytes(rawRow.request_body));
        expect(
          yield* gateway
            .submit(rawCreate)
            .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, nextPrincipal)),
        ).toEqual(rawReceipt);
        expect(
          yield* gateway
            .pollCommand(rawCreate.command_id)
            .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, nextPrincipal)),
        ).toEqual(rawReceipt);
        expect(
          yield* gateway
            .readReceipt(rawCreate.command_id)
            .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, nextPrincipal)),
        ).toMatchObject({ kind: "receipt", receipt: rawReceipt });
        expect(
          yield* gateway
            .resubmit(rawCreate.command_id, rawRow.request_body)
            .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, nextPrincipal)),
        ).toEqual({ kind: "receipt", receipt: rawReceipt });
        expect(
          yield* gateway
            .resubmit(rawCreate.command_id, rawRow.request_body + " ")
            .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, nextPrincipal)),
        ).toEqual({ kind: "idempotency_conflict" });
        expect(
          yield* gateway
            .pollCommand(rawCreate.command_id)
            .pipe(
              Effect.provideService(EnvironmentAuthenticatedPrincipal, readOnlyPrincipal),
              Effect.flip,
            ),
        ).toMatchObject({ reason: "unknown-effect" });
        const missingCommand = {
          ...rawCreate,
          command_id: "command:gateway-missing-application-01",
        };
        expect(
          yield* gateway
            .readReceipt(missingCommand.command_id)
            .pipe(
              Effect.provideService(EnvironmentAuthenticatedPrincipal, nextPrincipal),
              Effect.flip,
            ),
        ).toMatchObject({ reason: "unknown-effect" });
        expect(
          yield* gateway
            .resubmit(missingCommand.command_id, canonicalSnapshot(missingCommand))
            .pipe(
              Effect.provideService(EnvironmentAuthenticatedPrincipal, nextPrincipal),
              Effect.flip,
            ),
        ).toMatchObject({ reason: "unknown-effect" });
        expect(
          yield* msql`SELECT operation_id FROM jones_workstreams_operations
          WHERE operation_id=${missingCommand.command_id}`,
        ).toEqual([]);
        expect(publicWrites()).toBe(beforeGatewayWrite + 1);

        const appearance = {
          command_id: "command:gateway-appearance-application-01",
          workstream_id: publicId,
          expected_server_generation: gatewaySession.serverGeneration,
          expected_version: 0,
          border_color: "#112233",
        };
        expect(
          yield* gateway
            .saveAppearance(appearance)
            .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, nextPrincipal)),
        ).toEqual({ workstream_id: publicId, border_color: "#112233", version: 1 });
        expect(
          yield* gateway
            .readAppearance([publicId])
            .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, nextPrincipal)),
        ).toMatchObject({
          supported: true,
          page: { items: [{ workstream_id: publicId, border_color: "#112233", version: 1 }] },
        });
        const deniedCommand = {
          ...rawCreate,
          command_id: "command:gateway-readonly-application-01",
        };
        const afterAppearance = publicWrites();
        expect(
          yield* gateway
            .submit(deniedCommand)
            .pipe(
              Effect.provideService(EnvironmentAuthenticatedPrincipal, readOnlyPrincipal),
              Effect.flip,
            ),
        ).toMatchObject({ reason: "authorization-not-current" });
        expect(
          yield* msql`SELECT operation_id FROM jones_workstreams_operations
          WHERE operation_id=${deniedCommand.command_id}`,
        ).toEqual([]);
        expect(publicWrites()).toBe(afterAppearance);

        const lostBinding = yield* gateway
          .readSession()
          .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, nextPrincipal));
        const lostCommand = {
          ...rawCreate,
          command_id: "command:gateway-lost-reply-application-01",
          expected_registry_version: lostBinding.registryVersion,
        };
        f.state.loseForwardReply = true;
        expect(
          yield* gateway
            .submit(lostCommand)
            .pipe(
              Effect.provideService(EnvironmentAuthenticatedPrincipal, nextPrincipal),
              Effect.flip,
            ),
        ).toMatchObject({ reason: "unknown-effect" });
        f.state.loseForwardReply = false;
        const afterLost = publicWrites();
        expect(afterLost).toBe(afterAppearance + 1);
        expect(
          yield* msql`SELECT state FROM jones_workstreams_operations
          WHERE operation_id=${lostCommand.command_id}`,
        ).toEqual([{ state: "reconciling" }]);
        const lostReceipt = yield* gateway
          .pollCommand(lostCommand.command_id)
          .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, nextPrincipal));
        expect(lostReceipt).toMatchObject({
          command_id: lostCommand.command_id,
          state: "committed",
        });
        expect(
          yield* gateway
            .readReceipt(lostCommand.command_id)
            .pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, nextPrincipal)),
        ).toMatchObject({ kind: "receipt", receipt: lostReceipt });
        expect(publicWrites()).toBe(afterLost);
        expect(
          yield* msql`SELECT state FROM jones_workstreams_operations
          WHERE operation_id=${lostCommand.command_id}`,
        ).toEqual([{ state: "reconciling" }]);
        expect(yield* auth.revokeSession(nextSession.sessionId)).toBe(true);
        expect((yield* requestNativeRead(nextSession.token, continuation)).status).toBe(401);
        yield* Effect.promise(async () => {
          await expect(reader.read(countsRequest)).rejects.toMatchObject({ status: 401 });
        });
        expect(
          (yield* requestPlacements(nextSession.token, "workstreams-t3-placement/2.0.0")).status,
        ).toBe(401);
        expect(yield* invokeRetained()).toMatchObject({
          state: "denied",
          reason: "caller_changed",
        });
        expect(
          yield* msql`SELECT state FROM jones_workstreams_operations WHERE operation_id='command:retained-source-application-01'`,
        ).toEqual([{ state: "accepted" }]);
        expect(yield* msql`SELECT * FROM jones_workstreams_grants`).toEqual([]);
        expect(yield* msql`SELECT * FROM jones_workstreams_registries`).toEqual([]);
      }),
    ),
);

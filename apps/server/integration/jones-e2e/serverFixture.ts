// @effect-diagnostics nodeBuiltinImport:off -- Test-owned loopback listeners only; no child processes or host state.
import * as NodeHttp from "node:http";
import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CommandId,
  EnvironmentHttpApi,
  type OrchestrationV2LinkOrigin,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpServer from "effect/http/HttpServer";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import { McpSchema } from "effect/ai";
// oxlint-disable-next-line t3code/no-raw-mcp-registration -- Test fixture reads the service for callTool; McpHttpServer owns all registration.
import { McpServer } from "effect/ai";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";

import * as EnvironmentAuth from "../../src/auth/EnvironmentAuth.ts";
import * as AuthHttp from "../../src/auth/http.ts";
import * as McpOAuth from "../../src/auth/McpOAuth.ts";
import * as McpOAuthHttp from "../../src/auth/mcpOAuthHttp.ts";
import * as ServerSecretStore from "../../src/auth/ServerSecretStore.ts";
import * as ServerConfig from "../../src/config.ts";
import * as ServerEnvironment from "../../src/environment/ServerEnvironment.ts";
import * as ServerHttp from "../../src/http.ts";
import * as McpHttpServer from "../../src/mcp/McpHttpServer.ts";
import * as McpInvocationContext from "../../src/mcp/McpInvocationContext.ts";
import * as McpSessionRegistry from "../../src/mcp/McpSessionRegistry.ts";
import * as CommandReceiptStore from "../../src/orchestration-v2/CommandReceiptStore.ts";
import * as Orchestrator from "../../src/orchestration-v2/Orchestrator.ts";
import * as ProviderAdapterRegistry from "../../src/orchestration-v2/ProviderAdapterRegistry.ts";
import * as RuntimeLayer from "../../src/orchestration-v2/runtimeLayer.ts";
import * as ThreadLaunch from "../../src/orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagement from "../../src/orchestration-v2/ThreadManagementService.ts";
import * as ProviderReplayHarness from "../../src/orchestration-v2/testkit/ProviderReplayHarness.ts";
import { CodexProviderCapabilitiesV2 } from "../../src/orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as PeerForwarding from "../../src/peer/PeerForwarding.ts";
import * as PeerLinks from "../../src/peer/PeerLinks.ts";
import * as PeerMcpClient from "../../src/peer/PeerMcpClient.ts";
import * as RemoteDelegation from "../../src/peer/RemoteDelegation.ts";
import { descriptorOf } from "../../src/peer/PeerLinks.testkit.ts";
import * as Sqlite from "../../src/persistence/Sqlite.ts";
import * as ProcessRunner from "../../src/processRunner.ts";
import * as ProjectEnrichment from "../../src/project/ProjectEnrichmentService.ts";
import * as ProjectService from "../../src/project/ProjectService.ts";
import * as ProviderRegistry from "../../src/provider/ProviderRegistry.ts";
import * as ScheduledTasks from "../../src/scheduledTasks/ScheduledTaskService.ts";
import * as SecretRequests from "../../src/secrets/SecretRequests.ts";
import * as Ownership from "../../src/serverOwnership.ts";
import * as RuntimeState from "../../src/serverRuntimeState.ts";
import * as ServerSettings from "../../src/serverSettings.ts";
import * as GitVcsDriver from "../../src/vcs/GitVcsDriver.ts";
import * as WorkspacePaths from "../../src/workspace/WorkspacePaths.ts";

class FixtureApi extends HttpApi.make("environment")
  .add(EnvironmentHttpApi.groups.metadata)
  .add(EnvironmentHttpApi.groups.mcpOAuth) {}

const modelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "synthetic-no-provider-turns",
};

// Metadata lets the real decider commit message intent. No provider is allowed to open.
const provider: ProviderAdapter.ProviderAdapterV2["Service"] = {
  instanceId: modelSelection.instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
  openSession: () => Effect.die("JE fixture must never open a provider session"),
};

const mcpClient = McpSchema.McpServerClient.of({
  clientId: 1,
  protocolVersion: "2025-06-18",
  clientCapabilities: {},
  clientInfo: { name: "jones-e2e", version: "1" },
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "jones-e2e", version: "1" },
  },
  getClient: Effect.die("JE fixture does not request a client callback"),
});

export const makeHome = (prefix: string) =>
  Effect.gen(function* () {
    // The native writer fence must never pick up a running desktop's protected profile.
    if (process.env.T3CODE_DESKTOP_USER_DATA_DIR || process.env.T3CODE_JONES_TRIAL_DESCRIPTOR) {
      return yield* Effect.die("Run JE fixtures without desktop/trial environment overrides");
    }
    const fs = yield* FileSystem.FileSystem;
    return yield* fs.makeTempDirectoryScoped({ prefix: `jones-e2e-${prefix}-` });
  });

/**
 * Real independently scoped HTTP/MCP servers and disk SQLite, in the Vitest process.
 * Pairing, OAuth, link fencing, dispatch and persistence are production services.
 * Descriptor build/platform metadata and unused toolkit dependencies are substitutes.
 * No workstream registry/native enrollment is claimed; loopbackControlPlane's injected
 * auth/authority/build/registry fixture is a separate workstreams boundary, not peer auth.
 * Closing the caller's scope stops this listener and closes SQLite before its home is removed.
 */
export const startServer = (
  home: string,
  afterListening?: (url: string) => Effect.Effect<void, Error>,
) =>
  Effect.gen(function* () {
    const configuration = yield* ServerConfig.layerTest(home, home).pipe(
      Layer.provide(NodeServices.layer),
      Layer.build,
    );
    const config = Context.get(configuration, ServerConfig.ServerConfig);
    const owner = yield* Ownership.acquireServerOwnership(config.serverRuntimeStatePath).pipe(
      Effect.provide(ProcessRunner.layer),
    );
    const database = yield* Sqlite.layerFromPath(config.dbPath).pipe(Layer.build);
    const dbLayer = Layer.succeedContext(database);
    const configLayer = Layer.succeedContext(configuration);
    const settings = ServerSettings.layerTest();
    const registry = ProviderAdapterRegistry.layerFromAdapters([provider]);
    const core = yield* ProviderReplayHarness.layerWithRegistry({ name: "jones-e2e" }, registry, {
      serverConfigLayer: configLayer,
      databaseLayer: dbLayer,
      runEffectWorker: false,
      runContinuationWorker: false,
    }).pipe(Layer.build);
    const base = Layer.mergeAll(
      Layer.succeedContext(core),
      configLayer,
      dbLayer,
      settings,
      registry,
      NodeServices.layer,
    );
    const services = yield* Layer.mergeAll(
      ThreadManagement.layer.pipe(Layer.provide(CommandReceiptStore.layer)),
      CommandReceiptStore.layer,
      RuntimeLayer.layerProjectService.pipe(
        Layer.provide(WorkspacePaths.layer),
        Layer.provide(
          Layer.mock(ProjectEnrichment.ProjectEnrichmentService)({
            peek: () =>
              Effect.succeed({
                repositoryIdentity: null,
                faviconPath: null,
                repositoryIdentityResolved: true,
              }),
            getAvailable: () =>
              Effect.succeed({
                repositoryIdentity: null,
                faviconPath: null,
                repositoryIdentityResolved: true,
              }),
            request: () => Effect.void,
            invalidate: () => Effect.void,
          }),
        ),
      ),
      ServerSecretStore.layer,
      ServerEnvironment.layerIdentity,
    ).pipe(Layer.provideMerge(base), Layer.build);
    const environmentId = yield* Context.get(services, ServerEnvironment.ServerEnvironmentIdentity)
      .getEnvironmentId;
    const environment = Layer.succeed(ServerEnvironment.ServerEnvironment, {
      getEnvironmentId: Effect.succeed(environmentId),
      getDescriptor: Effect.succeed(descriptorOf(environmentId, "JE synthetic environment")),
    });
    const linked = yield* PeerMcpClient.layer.pipe(
      Layer.provideMerge(PeerLinks.layer),
      Layer.provideMerge(EnvironmentAuth.layer),
      Layer.provideMerge(environment),
      Layer.provideMerge(FetchHttpClient.layer),
      Layer.provideMerge(Layer.succeedContext(services)),
      Layer.build,
    );
    const forwarding = yield* PeerForwarding.layer.pipe(
      Layer.provideMerge(Layer.succeedContext(linked)),
      Layer.build,
    );
    const registered = yield* Deferred.make<McpServer.McpServer["Service"]>();
    const tools = Layer.mergeAll(
      McpHttpServer.layerOrchestratorToolkit,
      Layer.effectDiscard(
        McpServer.McpServer.pipe(Effect.flatMap((server) => Deferred.succeed(registered, server))),
      ),
    ).pipe(
      Layer.provideMerge(McpHttpServer.layerMcpTransport),
      Layer.provide(McpOAuth.layerMcpClientAuthenticator),
      Layer.provide(
        Layer.mock(McpSessionRegistry.McpSessionRegistry)({
          resolve: () => Effect.succeed(undefined),
        }),
      ),
      Layer.provide(Layer.mock(ThreadLaunch.ThreadLaunchService)({})),
      Layer.provide(Layer.mock(GitVcsDriver.GitVcsDriver)({})),
      Layer.provide(Layer.mock(ProviderRegistry.ProviderRegistry)({})),
      Layer.provide(Layer.mock(ScheduledTasks.ScheduledTaskService)({})),
      Layer.provide(Layer.mock(SecretRequests.SecretRequests)({})),
      Layer.provide(Layer.mock(RemoteDelegation.RemoteDelegation)({})),
    );
    const routes = Layer.mergeAll(
      HttpApiBuilder.layer(FixtureApi).pipe(
        Layer.provide(McpOAuthHttp.layer.pipe(Layer.provide(McpOAuth.layer))),
        Layer.provide(ServerHttp.layerServerEnvironmentHttpApi),
        Layer.provide(AuthHttp.layerAuthenticatedAuth),
      ),
      tools,
    ).pipe(Layer.provide(Layer.succeedContext(forwarding)));
    const listening = yield* HttpRouter.serve(routes, {
      disableListenLog: true,
      disableLogger: true,
    }).pipe(
      Layer.provideMerge(
        NodeHttpServer.layer(NodeHttp.createServer, { host: "127.0.0.1", port: 0 }),
      ),
      Layer.provide(NodeServices.layer),
      Layer.fresh,
      Layer.build,
    );
    const address = Context.get(listening, HttpServer.HttpServer).address;
    if (!("port" in address)) return yield* Effect.die("Expected a loopback TCP listener");
    const url = `http://127.0.0.1:${address.port}`;
    yield* owner.publish(
      yield* RuntimeState.makePersistedServerRuntimeState({ config, port: address.port }),
    );
    if (afterListening !== undefined) yield* afterListening(url);
    const mcp = yield* Deferred.await(registered);
    return {
      config,
      environmentId,
      url,
      auth: Context.get(linked, EnvironmentAuth.EnvironmentAuth),
      links: Context.get(linked, PeerLinks.PeerLinks),
      threads: Context.get(services, ThreadManagement.ThreadManagementService),
      projects: Context.get(services, ProjectService.ProjectService),
      orchestrator: Context.get(core, Orchestrator.OrchestratorV2),
      receipts: Context.get(services, CommandReceiptStore.CommandReceiptStoreV2),
      sql: Context.get(database, SqlClient.SqlClient),
      call: (name: string, args: Record<string, unknown>, callerThreadId?: ThreadId) =>
        mcp.callTool({ name, arguments: args }).pipe(
          Effect.provideService(McpInvocationContext.McpInvocationContext, {
            environmentId,
            requestNamespace: "jones-e2e-owner",
            thread:
              callerThreadId === undefined
                ? undefined
                : {
                    threadId: callerThreadId,
                    providerSessionId: "synthetic-mcp-caller",
                    providerInstanceId: modelSelection.instanceId,
                  },
            client:
              callerThreadId === undefined
                ? {
                    sessionId: "jones-e2e-owner",
                    label: "Synthetic owner",
                    access: "full-access",
                  }
                : undefined,
            capabilities: new Set(["orchestration"] as const),
            issuedAt: 1,
          }),
          Effect.provideService(McpSchema.McpServerClient, mcpClient),
        ),
    };
  });

export type TestServer = Effect.Success<ReturnType<typeof startServer>>;

export const createThread = (
  server: TestServer,
  input: { threadId: ThreadId; projectId: ProjectId; linkOrigin?: OrchestrationV2LinkOrigin },
) =>
  server.threads.dispatch({
    type: "thread.create",
    commandId: CommandId.make(`create:${input.threadId}`),
    ...input,
    title: "Same synthetic title",
    modelSelection,
    runtimeMode: "auto",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    createdBy: "user",
    creationSource: "web",
  });

export const failureOf = (result: McpSchema.CallToolResult): unknown => {
  const first = result.content[0];
  return result.isError === true && first?.type === "text" ? JSON.parse(first.text) : undefined;
};

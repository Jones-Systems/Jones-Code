// @effect-diagnostics nodeBuiltinImport:off -- Synthetic conversation state must live outside system temporary roots.
import * as NodeOS from "node:os";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  OrchestratorMcpEnvironmentTarget,
  OrchestratorMcpFailure,
  ProviderDriverKind,
  ProviderInstanceId,
  type ServerProvider,
} from "@t3tools/contracts";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { McpSchema, McpServer, Tool, Toolkit } from "effect/ai";
import * as Config from "../../config.ts";
import * as Access from "../../mcp/McpToolAccess.ts";
import * as Http from "../../mcp/McpHttpServer.ts";
import * as Invocation from "../../mcp/McpInvocationContext.ts";
import * as Mcp from "../../mcp/OrchestratorMcpService.ts";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import { ClaudeProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/ClaudeAdapterV2.ts";
import * as Receipts from "../../orchestration-v2/CommandReceiptStore.ts";
import * as Sink from "../../orchestration-v2/EventSink.ts";
import * as Legacy from "../../orchestration-v2/legacy/LegacyV1ThreadImporter.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as Registry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as Launch from "../../orchestration-v2/ThreadLaunchService.ts";
import * as Threads from "../../orchestration-v2/ThreadManagementService.ts";
import * as Harness from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as Database from "../../persistence/Sqlite.ts";
import * as Remote from "../../peer/RemoteDelegation.ts";
import * as Projects from "../../project/ProjectService.ts";
import * as Providers from "../../provider/ProviderRegistry.ts";
import * as Scheduled from "../../scheduledTasks/ScheduledTaskService.ts";
import * as Secrets from "../../secrets/SecretRequests.ts";
import * as Settings from "../../serverSettings.ts";
import * as Git from "../../vcs/GitVcsDriver.ts";
import * as Conversation from "../conversationWorkspace/ConversationPolicy.ts";
import * as PeerForwarding from "../../peer/PeerForwarding.ts";
import * as ServerEnvironment from "../../environment/ServerEnvironment.ts";
import * as Personal from "../workstreams/personal/service.ts";
import { fixture } from "../taskContinuation/testkit.ts";

const instanceId = ProviderInstanceId.make("codex");
const f = fixture();
const scope = f.scope;
const probe = {
  success: Schema.Boolean,
  failure: OrchestratorMcpFailure,
  failureMode: "return" as const,
  dependencies: [Invocation.McpInvocationContext, Threads.ThreadManagementService],
};
const hidden = McpSchema.EnabledWhen;
const tools = Toolkit.make(
  Tool.make("read", probe),
  Tool.make("act", probe).annotate(hidden, Invocation.hiddenFromOutsideClients),
  Tool.make("write", probe),
  Tool.make("capability", probe),
  Tool.make("thread", probe),
  Tool.make("start", probe),
  Tool.make("continue", probe),
  Tool.make("environment", probe),
  Tool.make("operation", { ...probe, parameters: Schema.Struct({ read: Schema.Boolean }) }),
);
const answer = () => Effect.succeed(true);
const probes = Access.toLayer(tools, {
  read: Access.reads(answer),
  act: Access.actsAsCaller(answer),
  write: Access.writes(answer),
  capability: Access.writesWithCapability(answer),
  thread: Access.writesThreads(() => [], answer),
  start: Access.startsThreads(() => ({}), answer, "stamped"),
  continue: Access.startsOrContinuesThreads(() => ({}), answer, "stamped"),
  environment: Access.writesEnvironment(answer),
  operation: Access.readsOrWritesThreads(
    (input) => input.read,
    () => [],
    answer,
  ),
});
const client = McpSchema.McpServerClient.of({
  clientId: 1,
  protocolVersion: "2025-06-18",
  clientCapabilities: {},
  clientInfo: { name: "composition", version: "1" },
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "composition", version: "1" },
  },
  getClient: Effect.die("The probe never calls back to a client."),
});
const decodeToolResult = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.Union([Schema.Boolean, Schema.Struct({ code: Schema.String })])),
);
const call = (name: string, args: Record<string, unknown> = {}) =>
  McpServer.McpServer.pipe(
    Effect.flatMap((server) => server.callTool({ name, arguments: args })),
    Effect.flatMap(({ content }) =>
      decodeToolResult(content.map((part) => (part.type === "text" ? part.text : "")).join("")),
    ),
    Effect.provideService(Invocation.McpInvocationContext, scope),
    Effect.provideService(McpSchema.McpServerClient, client),
  );

type Services =
  | Mcp.OrchestratorMcpService
  | Orchestrator.OrchestratorV2
  | Threads.ThreadManagementService
  | Personal.PersonalWorkstreams
  | Conversation.ConversationPolicy
  | Sink.EventSinkV2
  | SqlClient.SqlClient
  | Settings.ServerSettingsService;

// Real receipts, admission, projections and policy share a database. Only provider discovery and
// unrelated, uncalled boundaries are substituted; opening a provider process is an error.
const scenario = <E>(
  body: (context: {
    cwd: string;
    catalog: Ref.Ref<ReadonlyArray<ServerProvider>>;
  }) => Effect.Effect<void, E, Services>,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace("mcp-delegation-composition");
      const baseConfig = yield* Harness.makeReplayServerConfig("mcp-delegation-composition");
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const cacheRoot = path.join(NodeOS.homedir(), "Library", "Caches");
      yield* fs.makeDirectory(cacheRoot, { recursive: true });
      const conversationState = yield* fs.makeTempDirectoryScoped({
        directory: cacheRoot,
        prefix: "jones-mcp-conversation-",
      });
      const config = { ...baseConfig, stateDir: yield* fs.realPath(conversationState) };
      const registry = Registry.layerFromAdapters([
        {
          instanceId,
          driver: ProviderDriverKind.make("codex"),
          getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
          planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
          openSession: () => Effect.die("Admission tests never open a provider process."),
        },
        {
          instanceId: f.provider.providerInstanceId,
          driver: f.provider.driver,
          getCapabilities: () => Effect.succeed(ClaudeProviderCapabilitiesV2),
          planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
          openSession: () => Effect.die("Status tests never open a provider process."),
        },
      ]);
      const catalog = yield* Ref.make<ReadonlyArray<ServerProvider>>([
        {
          instanceId,
          driver: ProviderDriverKind.make("codex"),
          enabled: true,
          installed: true,
          version: "test",
          status: "ready",
          auth: { status: "authenticated" },
          checkedAt: "2026-10-25T20:00:00.000Z",
          slashCommands: [],
          skills: [],
          models: [
            {
              slug: f.parent.thread.modelSelection.model,
              name: "Fixture",
              isCustom: true,
              capabilities: null,
            },
          ],
        },
      ]);
      const database = Database.layerMemory;
      const settings = Settings.layerTest();
      const configLayer = Layer.succeed(Config.ServerConfig, config);
      const runtime = Harness.layerWithRegistry(
        { name: "mcp-delegation-composition", runtimePolicyOverride: { cwd } },
        registry,
        { databaseLayer: database, serverConfigLayer: configLayer, runEffectWorker: false },
      );
      const threads = Threads.layer.pipe(
        Layer.provideMerge(runtime),
        Layer.provide(settings),
        Layer.provide(Receipts.layer.pipe(Layer.provide(database))),
        Layer.provide(Legacy.layer.pipe(Layer.provide(runtime), Layer.provide(database))),
      );
      const policy = Conversation.layer.pipe(Layer.provide(database), Layer.provide(configLayer));
      const personal = Personal.layer.pipe(
        Layer.provide(database),
        Layer.provide(configLayer),
        Layer.provide(
          Layer.succeed(ServerEnvironment.ServerEnvironmentIdentity, {
            getEnvironmentId: Effect.succeed(scope.environmentId),
          }),
        ),
      );
      const dependencies = Layer.mergeAll(
        threads,
        database,
        settings,
        policy,
        registry,
        personal,
        Layer.mock(Providers.ProviderRegistry)({
          getProviders: Ref.get(catalog),
          refreshInstance: () => Ref.get(catalog),
        }),
        Layer.mock(Launch.ThreadLaunchService)({}),
        Layer.mock(Git.GitVcsDriver)({}),
        Layer.mock(Projects.ProjectService)({}),
        Layer.mock(Scheduled.ScheduledTaskService)({}),
        Layer.mock(Secrets.SecretRequests)({}),
        Layer.mock(Remote.RemoteDelegation)({}),
      ).pipe(Layer.provideMerge(Layer.merge(NodeServices.layer, NodeCrypto.layer)));
      const layer = Layer.mergeAll(dependencies, Mcp.layer.pipe(Layer.provide(dependencies)));
      yield* body({ cwd, catalog }).pipe(Effect.provide(layer));
    }),
  ).pipe(Effect.provide(Layer.merge(NodeServices.layer, NodeCrypto.layer)));

const startParent = (cwd: string) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make("composition:parent"),
      createdBy: "user",
      creationSource: "web",
      threadId: scope.thread!.threadId,
      projectId: f.parent.thread.projectId,
      title: "Parent",
      modelSelection: f.parent.thread.modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: cwd,
    });
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make("composition:start"),
      createdBy: "user",
      creationSource: "web",
      threadId: scope.thread!.threadId,
      messageId: MessageId.make("composition:message"),
      text: "Delegate the work.",
      attachments: [],
      dispatchMode: { type: "start_immediately" },
    });
  });

it.effect(
  "refuses unsolicited supervision before admission, but replays an accepted child without a current catalog",
  () =>
    scenario(({ cwd, catalog }) =>
      Effect.gen(function* () {
        yield* startParent(cwd);
        const service = yield* Mcp.OrchestratorMcpService;
        const threads = yield* Threads.ThreadManagementService;
        const request = {
          task: "Read the source.",
          runtimeMode: "approval-required" as const,
          mode: "async" as const,
          clientRequestId: "composition:delegate",
        };
        const refused = yield* service.delegateTask(scope, request).pipe(Effect.flip);
        assert.equal(refused.code, "invalid_request");
        assert.include(refused.message, "supervisionRequestedByUser");
        assert.isEmpty((yield* threads.getThreadProjection(scope.thread!.threadId)).subagents);
        const first = yield* service.delegateTask(scope, {
          ...request,
          supervisionRequestedByUser: true,
        });
        assert.isNotNull(first.childThreadId);
        const child = yield* threads.getThreadProjection(first.childThreadId!);
        assert.equal(child.thread.runtimeMode, "approval-required");
        assert.equal(child.thread.lineage.parentThreadId, scope.thread!.threadId);
        assert.equal(child.thread.forkedFrom?.type, "node");
        yield* Ref.set(catalog, []);
        const replay = yield* service.delegateTask(scope, request);
        assert.equal(replay.taskId, first.taskId);
        assert.equal(replay.childThreadId, first.childThreadId);
        assert.equal(replay.model, first.model);
        assert.lengthOf((yield* threads.getThreadProjection(scope.thread!.threadId)).subagents, 1);
        const fresh = yield* service
          .delegateTask(scope, { task: "New work.", clientRequestId: "composition:fresh" })
          .pipe(Effect.flip);
        assert.equal(fresh.code, "provider_unavailable");
      }),
    ),
);

it.effect(
  "registered MCP write wrappers consult the real conversation policy while reads remain available",
  () =>
    scenario(({ cwd }) =>
      Effect.gen(function* () {
        yield* startParent(cwd);
        const policy = yield* Conversation.ConversationPolicy;
        yield* policy.allocate({
          threadId: scope.thread!.threadId,
          projectId: f.parent.thread.projectId,
          projectRoot: cwd,
        });
        const server = Http.toolkitRegistration(tools, probes).pipe(
          Layer.provideMerge(Http.layerInProcessMcpServer),
        );
        yield* Effect.gen(function* () {
          assert.equal(yield* call("read"), true);
          assert.equal(yield* call("operation", { read: true }), true);
          assert.deepEqual(yield* call("operation", { read: false }), { code: "attach_required" });
          for (const name of [
            "act",
            "write",
            "capability",
            "thread",
            "start",
            "continue",
            "environment",
          ]) {
            const result = yield* call(name);
            assert.deepEqual(result, { code: "attach_required" });
          }
          yield* policy.markAttached(scope.thread!.threadId);
          for (const name of [
            "act",
            "write",
            "capability",
            "thread",
            "start",
            "continue",
            "environment",
          ])
            assert.equal(yield* call(name), true);
        }).pipe(Effect.provide(server));
      }),
    ),
);

it.effect("task_status exposes captured attempt provenance after the provider row changes", () =>
  scenario(() =>
    Effect.gen(function* () {
      const sink = yield* Sink.EventSinkV2;
      yield* sink.write({
        events: [
          f.event("thread.created", f.parent.thread.id, f.parent.thread),
          f.event("run.created", f.parent.thread.id, f.parent.runs[0]!),
          f.event("subagent.updated", f.parent.thread.id, f.task),
          f.event("thread.created", f.child.thread.id, f.child.thread),
          f.event("provider-thread.updated", f.child.thread.id, {
            ...f.provider,
            nativeThreadRef: { ...f.provider.nativeThreadRef!, nativeId: "replacement-native" },
          }),
          f.event("run.created", f.child.thread.id, f.run),
          f.event("run-attempt.created", f.child.thread.id, f.attempt),
          f.event("provider-turn.updated", f.child.thread.id, f.child.providerTurns[0]!),
          f.event("context-transfer.created", f.child.thread.id, f.child.contextTransfers[0]!),
        ],
      });
      const service = yield* Mcp.OrchestratorMcpService;
      const result = yield* service.taskStatus(scope, f.task.id);
      assert.deepEqual(result.nativeSession, {
        nativeSessionId: "original-native",
        sourceRunId: f.run.id,
        sourceAttemptId: f.attempt.id,
        evidence: "attempt",
      });
      const { nativeThreadId: _capture, ...uncaptured } = f.attempt;
      yield* sink.write({
        events: [f.event("run-attempt.updated", f.child.thread.id, uncaptured)],
      });
      assert.isUndefined((yield* service.taskStatus(scope, f.task.id)).nativeSession);
    }),
  ),
);

it.effect("linked workstream assignment carries the source definition after caller admission", () =>
  scenario(({ cwd }) =>
    Effect.gen(function* () {
      yield* startParent(cwd);
      const personal = yield* Personal.PersonalWorkstreams;
      const created = yield* personal.create({ id: "composition", name: "Source workstream" });
      const forwarded = yield* Ref.make<ReadonlyArray<unknown>>([]);
      const assignment = Toolkit.make(
        Tool.make("t3_thread_workstream_set", {
          ...probe,
          parameters: Schema.Struct({
            environmentId: OrchestratorMcpEnvironmentTarget,
            workstreamId: Schema.NullOr(Schema.String),
          }),
        }),
      );
      const handlers = Access.toLayer(assignment, {
        t3_thread_workstream_set: Access.writesThreads(
          () => [],
          () => Effect.die("This assignment must run in its linked environment."),
        ),
      });
      const server = Http.toolkitRegistration(assignment, handlers).pipe(
        Layer.provideMerge(Http.layerInProcessMcpServer),
        Layer.provide(
          Layer.mock(PeerForwarding.PeerForwarding)({
            call: (_scope, _tool, _environmentId, params) =>
              Ref.update(forwarded, (calls) => [...calls, params]).pipe(Effect.as(true as never)),
          }),
        ),
      );
      yield* Effect.gen(function* () {
        const mcp = yield* McpServer.McpServer;
        const invoke = (workstreamId: string | null) =>
          mcp
            .callTool({
              name: "t3_thread_workstream_set",
              arguments: { environmentId: "linked-synthetic", workstreamId },
            })
            .pipe(
              Effect.provideService(Invocation.McpInvocationContext, scope),
              Effect.provideService(McpSchema.McpServerClient, client),
            );
        yield* invoke("composition");
        assert.deepEqual(yield* Ref.get(forwarded), [
          {
            environmentId: "linked-synthetic",
            workstreamId: "composition",
            workstream: created.workstream,
          },
        ]);
        yield* invoke(null);
        assert.deepEqual((yield* Ref.get(forwarded))[1], {
          environmentId: "linked-synthetic",
          workstreamId: null,
        });
        const policy = yield* Conversation.ConversationPolicy;
        yield* policy.allocate({
          threadId: scope.thread!.threadId,
          projectId: f.parent.thread.projectId,
          projectRoot: cwd,
        });
        yield* invoke("composition");
        assert.lengthOf(yield* Ref.get(forwarded), 2);
      }).pipe(Effect.provide(server));
    }),
  ),
);

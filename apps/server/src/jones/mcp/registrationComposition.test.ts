import { expect, expectTypeOf, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as C from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { McpSchema, McpServer } from "effect/ai";
import type * as RuntimeLayer from "../../orchestration-v2/runtimeLayer.ts";
import * as ServerConfig from "../../config.ts";
import * as McpHttpServer from "../../mcp/McpHttpServer.ts";
import * as Invocation from "../../mcp/McpInvocationContext.ts";
import { OrchestratorToolkit } from "../../mcp/toolkits/orchestrator/tools.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import type { ProviderAdapterV2Shape } from "@t3tools/provider-core/server/ProviderAdapter";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as Registry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as Receipts from "../../orchestration-v2/CommandReceiptStore.ts";
import * as Replay from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as Sqlite from "../../persistence/Sqlite.ts";
import * as ProjectService from "../../project/ProjectService.ts";
import * as ProviderRegistry from "../../provider/testUtils/providerRegistryMock.ts";
import * as ScheduledTasks from "../../scheduledTasks/ScheduledTaskService.ts";
import * as SecretRequests from "../../secrets/SecretRequests.ts";
import * as ThreadLaunch from "../../orchestration-v2/ThreadLaunchService.ts";
import * as GitVcsDriver from "../../vcs/GitVcsDriver.ts";
import * as PeerForwarding from "../../peer/PeerForwarding.ts";
import * as PeerLinkRequests from "../../peer/PeerLinkRequests.ts";
import * as PeerLinks from "../../peer/PeerLinks.ts";
import * as RemoteDelegation from "../../peer/RemoteDelegation.ts";
import * as Continuity from "../goals/continuity/GoalContinuity.ts";
import * as ContinuityRuntime from "../goals/continuity/RuntimeIntegration.ts";

const instanceId = C.ProviderInstanceId.make("codex");
const threadId = C.ThreadId.make("registration-composition-thread");
const owner: C.ContinuityOwner = {
  environmentId: C.EnvironmentId.make("registration-composition-environment"),
  threadId,
  providerInstanceId: instanceId,
  providerThreadId: C.ProviderThreadId.make("registration-composition-provider"),
  driver: C.ProviderDriverKind.make("codex"),
  nativeThreadId: "synthetic-native-conversation",
};
const invocation: Invocation.McpInvocationScope = {
  environmentId: owner.environmentId,
  requestNamespace: "registration-composition-owner",
  capabilities: new Set(["orchestration"]),
  issuedAt: 0,
  thread: undefined,
  client: { sessionId: "synthetic-owner", label: "Synthetic owner", access: "full-access" },
};
const client = McpSchema.McpServerClient.of({
  clientId: 1,
  protocolVersion: "2025-06-18",
  clientCapabilities: {},
  clientInfo: { name: "registration-composition", version: "1" },
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "registration-composition", version: "1" },
  },
  getClient: Effect.die("This registration fixture never requests a remote client"),
});

const database = Sqlite.layerMemory;
const adapter = {
  instanceId,
  driver: owner.driver,
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("Registration tests never start a provider"),
} as ProviderAdapterV2Shape;
const adapters = Registry.layerFromAdapters([adapter]);
const runtime = Replay.layerWithRegistryAndServices(
  { name: "mcp-registration-composition" },
  adapters,
  {
    databaseLayer: database,
    runEffectWorker: false,
    configureMcp: false,
    serverConfigLayer: ServerConfig.layerTest(process.cwd(), {
      prefix: "mcp-registration-composition-",
    }).pipe(Layer.provide(NodeServices.layer), Layer.orDie),
  },
);

// Only unrelated catalog, launch, secret, project and peer operations are doubles.
// The restored owners, event/projection/receipt stores and SQL are real; the replay
// thread adapter forwards its reads and dispatches to the real orchestrator.
const unrelatedServices = Layer.mergeAll(
  ProviderRegistry.layer(),
  adapters,
  Layer.mock(ProjectService.ProjectService)({}),
  Layer.mock(ThreadLaunch.ThreadLaunchService)({}),
  Layer.mock(GitVcsDriver.GitVcsDriver)({}),
  Layer.mock(ScheduledTasks.ScheduledTaskService)({}),
  Layer.mock(SecretRequests.SecretRequests)({}),
  Layer.mock(PeerForwarding.PeerForwarding)({}),
  Layer.mock(PeerLinkRequests.PeerLinkRequests)({}),
  Layer.mock(PeerLinks.PeerLinks)({}),
  Layer.mock(RemoteDelegation.RemoteDelegation)({}),
);
const registration = McpHttpServer.layerOrchestratorToolkit.pipe(
  Layer.provideMerge(McpHttpServer.layerInProcessMcpServer),
  Layer.provide(ContinuityRuntime.layer),
  Layer.provide(Receipts.layer),
  Layer.provide(unrelatedServices),
  Layer.provideMerge(runtime),
  Layer.provide(database),
  Layer.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
);

const call = (name: string, args: Record<string, unknown>, scope = invocation) =>
  McpServer.McpServer.pipe(
    Effect.flatMap((server) => server.callTool({ name, arguments: args })),
    Effect.provideService(Invocation.McpInvocationContext, scope),
    Effect.provideService(McpSchema.McpServerClient, client),
  );
const decodeJsonUnknown = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const failure = (result: McpSchema.CallToolResult) => {
  expect(result.isError).toBe(true);
  return decodeJsonUnknown(
    result.content.map((part) => (part.type === "text" ? part.text : "")).join(""),
  );
};

it.effect("enumerates the four retained tools through the production registration", () =>
  Effect.gen(function* () {
    expectTypeOf<
      Extract<Layer.Success<typeof RuntimeLayer.layerProduction>, Continuity.GoalContinuity>
    >().toEqualTypeOf<Continuity.GoalContinuity>();
    const server = yield* McpServer.McpServer;
    const names = server.tools.map(({ tool }) => tool.name);
    for (const name of [
      "t3_thread_goal",
      "t3_thread_continuity",
      "t3_document_sections",
      "t3_task_continue",
    ] as const) {
      expect(OrchestratorToolkit.tools[name].name).toBe(name);
      expect(names.filter((registered) => registered === name)).toEqual([name]);
    }
  }).pipe(Effect.provide(registration)),
);

it.effect("dispatches all four registered tools with real retained owners and shared SQL", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: C.CommandId.make("registration-create"),
      threadId,
      projectId: C.ProjectId.make("registration-project"),
      title: "Synthetic registration fixture",
      modelSelection: { instanceId, model: "synthetic-model" },
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdBy: "user",
      creationSource: "web",
    });
    const goal = yield* call("t3_thread_goal", { threadId, action: "read" });
    expect(goal.isError).toBe(false);
    expect(goal.structuredContent).toMatchObject({
      threadId,
      outcome: "unsupported",
      reason: "provider_goal_controls_unsupported",
    });
    const document = yield* call("t3_document_sections", {
      repositoryIdentity: "github:Synthetic/Document-Test",
      commitOid: "a".repeat(40),
      documentPath: "fixture.md",
      documentSha256: "b".repeat(64),
      sectionIds: ["one"],
    });
    expect(failure(document)).toMatchObject({ code: "target_required" });

    const now = yield* DateTime.now;
    const provider: C.OrchestrationV2ProviderThread = {
      id: owner.providerThreadId,
      appThreadId: threadId,
      providerInstanceId: instanceId,
      driver: owner.driver,
      providerSessionId: null,
      ownerNodeId: null,
      nativeThreadRef: { driver: owner.driver, nativeId: owner.nativeThreadId, strength: "strong" },
      nativeConversationHeadRef: null,
      status: "active",
      firstRunOrdinal: 1,
      lastRunOrdinal: 1,
      handoffIds: [],
      forkedFrom: null,
      pendingBackgroundTasks: [],
      contextUsage: null,
      nativeMetadata: null,
      goal: null,
      createdAt: now,
      updatedAt: now,
    };
    const { thread } = yield* orchestrator.getThreadProjection(threadId);
    yield* (yield* EventSink.EventSinkV2).write({
      events: [
        {
          id: C.EventId.make("registration-provider"),
          type: "provider-thread.updated",
          threadId,
          occurredAt: now,
          payload: provider,
        },
        {
          id: C.EventId.make("registration-active-provider"),
          type: "thread.metadata-updated",
          threadId,
          occurredAt: now,
          payload: { ...thread, activeProviderThreadId: provider.id },
        },
      ],
    });
    const continuity = yield* call("t3_thread_continuity", {
      threadId,
      request: { action: "read" },
    });
    expect(continuity.isError).toBe(false);
    expect(continuity.structuredContent).toEqual({ outcome: "absent" });

    yield* orchestrator.dispatch({
      type: "message.dispatch",
      commandId: C.CommandId.make("registration-active-caller"),
      messageId: C.MessageId.make("registration-active-caller"),
      threadId,
      text: "Synthetic live caller; the effect worker stays stopped",
      attachments: [],
      createdBy: "user",
      creationSource: "web",
      dispatchMode: { type: "start_immediately" },
    });
    expect((yield* orchestrator.getThreadShell(threadId))?.activeRunId).not.toBeNull();
    const task = yield* call(
      "t3_task_continue",
      {
        taskId: C.NodeId.make("missing-task"),
        message: "Continue the original conversation",
        clientRequestId: "registration-continue",
      },
      {
        ...invocation,
        client: undefined,
        thread: {
          threadId,
          providerInstanceId: instanceId,
          providerSessionId: "synthetic-credential",
        },
      },
    );
    expect(failure(task)).toMatchObject({ code: "task_not_found" });
  }).pipe(Effect.provide(registration)),
);

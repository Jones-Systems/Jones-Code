import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  EnvironmentId,
  EventId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  RunId,
  ThreadId,
  type OrchestrationV2ThreadProjection,
  type ServerProvider,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";

import { emptyProjection } from "../../../orchestration-v2/ProjectionStore.ts";
import { v2PullRequestThread } from "../../../orchestration-v2/testkit/pullRequestFixtures.ts";
import * as ProviderAdapterRegistry from "../../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ThreadLaunchService from "../../../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagementService from "../../../orchestration-v2/ThreadManagementService.ts";
import * as ProviderRegistry from "../../../provider/ProviderRegistry.ts";
import * as ProjectService from "../../../project/ProjectService.ts";
import * as ScheduledTaskService from "../../../scheduledTasks/ScheduledTaskService.ts";
import * as ServerSettings from "../../../serverSettings.ts";
import * as SecretRequests from "../../../secrets/SecretRequests.ts";
import * as RemoteDelegation from "../../../peer/RemoteDelegation.ts";
import * as GitVcsDriver from "../../../vcs/GitVcsDriver.ts";
import type { McpInvocationScope } from "../../../mcp/McpInvocationContext.ts";
import * as OrchestratorMcpService from "../../../mcp/OrchestratorMcpService.ts";

const layerOrchestratorService = OrchestratorMcpService.layer.pipe(
  Layer.provide(Layer.mock(ThreadLaunchService.ThreadLaunchService)({})),
  Layer.provide(Layer.mock(GitVcsDriver.GitVcsDriver)({})),
);

describe("Jones MCP provider compatibility", () => {
  const parentThreadId = ThreadId.make("thread:mcp-providers-parent");
  const childThreadId = ThreadId.make("thread:mcp-providers-child");
  const parentRunId = RunId.make("run:mcp-providers-parent");
  const parentNodeId = NodeId.make("node:mcp-providers-root");
  const taskId = NodeId.make("node:mcp-providers-task");
  const projectId = ProjectId.make("project:mcp-providers");
  const codexInstanceId = ProviderInstanceId.make("codex");
  const antigravityInstanceId = ProviderInstanceId.make("antigravity");

  const scope: McpInvocationScope = {
    environmentId: EnvironmentId.make("environment:mcp-providers"),
    requestNamespace: "provider-session:mcp-providers",
    thread: {
      threadId: parentThreadId,
      providerSessionId: "provider-session:mcp-providers",
      providerInstanceId: codexInstanceId,
    },
    client: undefined,
    capabilities: new Set(["orchestration"]),
    issuedAt: 1,
  };

  const providerSnapshot = (input: {
    readonly instanceId: ProviderInstanceId;
    readonly driver: ProviderDriverKind;
    readonly model?: string;
    readonly enabled?: boolean;
  }): ServerProvider => ({
    instanceId: input.instanceId,
    driver: input.driver,
    enabled: input.enabled ?? true,
    installed: input.enabled !== false,
    version: input.enabled === false ? null : "test",
    status: input.enabled === false ? "disabled" : "ready",
    auth: { status: input.enabled === false ? "unknown" : "authenticated" },
    ...(input.enabled === false ? { message: "Provider is disabled in T3 Code settings." } : {}),
    checkedAt: "2026-09-13T00:00:00.000Z",
    models:
      input.model === undefined
        ? []
        : [{ slug: input.model, name: input.model, isCustom: false, capabilities: null }],
    slashCommands: [],
    skills: [],
  });

  const providerRegistryLayer = (providers: ReadonlyArray<ServerProvider>) =>
    Layer.mock(ProviderRegistry.ProviderRegistry)({
      getProviders: Effect.succeed(providers),
      refreshInstance: () => Effect.succeed(providers),
    });

  const adapterRegistryLayer = (instanceIds: ReadonlyArray<ProviderInstanceId>) =>
    Layer.succeed(
      ProviderAdapterRegistry.ProviderAdapterRegistryV2,
      ProviderAdapterRegistry.ProviderAdapterRegistryV2.of({
        list: () => Effect.succeed(instanceIds),
        get: (instanceId) =>
          instanceIds.includes(instanceId)
            ? Effect.succeed({
                instanceId,
              } as unknown as ProviderAdapter.ProviderAdapterV2["Service"])
            : Effect.fail(
                new ProviderAdapterRegistry.ProviderAdapterRegistryLookupError({ instanceId }),
              ),
      }),
    );

  const parentProjection = (
    subagents: ReadonlyArray<unknown>,
    modelSelection: {
      readonly instanceId: ProviderInstanceId;
      readonly model: string;
      readonly options?: ReadonlyArray<{ readonly id: string; readonly value: unknown }>;
    } = { instanceId: codexInstanceId, model: "gpt-5.4" },
  ): OrchestrationV2ThreadProjection =>
    ({
      thread: {
        id: parentThreadId,
        projectId,
        title: "MCP parent",
        createdBy: "user",
        creationSource: "web",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
      },
      runs: [
        {
          id: parentRunId,
          ordinal: 1,
          status: "running",
          rootNodeId: parentNodeId,
          providerInstanceId: codexInstanceId,
          modelSelection,
        },
      ],
      contextTransfers: [],
      subagents,
    }) as unknown as OrchestrationV2ThreadProjection;

  const childProjection = {
    thread: { id: childThreadId },
    runs: [],
    contextTransfers: [],
    messages: [],
    subagents: [],
    providerThreads: [],
    runtimeRequests: [],
    turnItems: [],
  } as unknown as OrchestrationV2ThreadProjection;

  it.effect("leaves models the user hid out of the capability catalog", () =>
    Effect.gen(function* () {
      const codex = providerSnapshot({
        instanceId: codexInstanceId,
        driver: ProviderDriverKind.make("codex"),
        model: "gpt-5.4",
      });
      const layerDependencies = Layer.mergeAll(
        NodeServices.layer,
        Layer.mock(ThreadManagementService.ThreadManagementService)({
          getThreadRecords: () => Effect.succeed(parentProjection([])),
        }),
        ServerSettings.layerTest({
          providerModelPreferences: {
            [codexInstanceId]: { hiddenModels: ["gpt-5.4-mini", "my-model"], modelOrder: [] },
          },
        }),
        providerRegistryLayer([
          {
            ...codex,
            models: [
              ...codex.models,
              { slug: "gpt-5.4-mini", name: "Mini", isCustom: false, capabilities: null },
              { slug: "my-model", name: "Mine", isCustom: true, capabilities: null },
            ],
          },
        ]),
        adapterRegistryLayer([codexInstanceId]),
        Layer.mock(ProjectService.ProjectService)({}),
        Layer.mock(SecretRequests.SecretRequests)({}),
        Layer.mock(RemoteDelegation.RemoteDelegation)({}),
        Layer.mock(ScheduledTaskService.ScheduledTaskService)({}),
      );

      yield* Effect.gen(function* () {
        const service = yield* OrchestratorMcpService.OrchestratorMcpService;
        const capabilities = yield* service.capabilities(scope);
        assert.deepEqual(
          capabilities.providers[0]?.models.map((model) => model.id),
          ["gpt-5.4", "my-model"],
        );
      }).pipe(Effect.provide(layerOrchestratorService.pipe(Layer.provide(layerDependencies))));
    }),
  );

  it.effect(
    "delegates to an Antigravity instance whose adapter resolves through the registry",
    () =>
      Effect.gen(function* () {
        let delegated = false;
        const task = {
          id: taskId,
          threadId: parentThreadId,
          runId: parentRunId,
          parentNodeId,
          origin: "app_owned",
          createdBy: "agent",
          driver: ProviderDriverKind.make("antigravity"),
          providerInstanceId: antigravityInstanceId,
          providerThreadId: null,
          childThreadId,
          nativeTaskRef: null,
          prompt: "Summarize the diff.",
          title: null,
          model: "ant-model",
          status: "running",
          result: null,
          startedAt: null,
          completedAt: null,
        };
        const dispatched = yield* Ref.make<ReadonlyArray<unknown>>([]);
        const layerDependencies = Layer.mergeAll(
          NodeServices.layer,
          Layer.mock(ThreadManagementService.ThreadManagementService)({
            hasReceipt: () => Effect.succeed(false),
            assertProviderInstanceEnabled: () => Effect.void,
            getThreadRecords: (threadId) =>
              Effect.succeed(
                threadId === parentThreadId
                  ? parentProjection(delegated ? [task] : [])
                  : childProjection,
              ),
            dispatch: (command) =>
              Ref.update(dispatched, (commands) => [...commands, command]).pipe(
                Effect.andThen(
                  Effect.sync(() => {
                    delegated = true;
                  }),
                ),
                Effect.as({
                  sequence: 1,
                  storedEvents: [
                    {
                      sequence: 1,
                      commandId: null,
                      event: { type: "subagent.updated", payload: task },
                    },
                  ],
                } as never),
              ),
          }),
          ServerSettings.layerTest(),
          Layer.mock(ProviderRegistry.ProviderRegistry)({
            getProviders: Effect.succeed([
              providerSnapshot({
                instanceId: codexInstanceId,
                driver: ProviderDriverKind.make("codex"),
                model: "gpt-5.4",
              }),
              providerSnapshot({
                instanceId: antigravityInstanceId,
                driver: ProviderDriverKind.make("antigravity"),
                model: "ant-model",
              }),
            ]),
          }),
          adapterRegistryLayer([codexInstanceId, antigravityInstanceId]),
          Layer.mock(ProjectService.ProjectService)({}),
          Layer.mock(SecretRequests.SecretRequests)({}),
          Layer.mock(RemoteDelegation.RemoteDelegation)({}),
          Layer.mock(ScheduledTaskService.ScheduledTaskService)({}),
        );

        yield* Effect.gen(function* () {
          const service = yield* OrchestratorMcpService.OrchestratorMcpService;
          const result = yield* service.delegateTask(scope, {
            task: "Summarize the diff.",
            target: { providerInstanceId: antigravityInstanceId, model: "ant-model" },
            mode: "async",
            clientRequestId: "delegate-antigravity-1",
          });
          assert.equal(result.status, "running");
          assert.equal(result.providerInstanceId, antigravityInstanceId);
          const commands = yield* Ref.get(dispatched);
          assert.equal(commands.length, 1);
          const request = commands[0] as {
            type: string;
            modelSelection: { instanceId: string; model: string };
          };
          assert.equal(request.type, "delegated_task.request");
          assert.equal(request.modelSelection.instanceId, antigravityInstanceId);
          assert.equal(request.modelSelection.model, "ant-model");
        }).pipe(Effect.provide(layerOrchestratorService.pipe(Layer.provide(layerDependencies))));
      }),
  );

  it.effect("resolves a driverKind target to a capable Antigravity instance", () =>
    Effect.gen(function* () {
      const dispatched = yield* Ref.make<ReadonlyArray<unknown>>([]);
      const task = {
        id: taskId,
        threadId: parentThreadId,
        runId: parentRunId,
        parentNodeId,
        origin: "app_owned",
        createdBy: "agent",
        driver: ProviderDriverKind.make("antigravity"),
        providerInstanceId: antigravityInstanceId,
        providerThreadId: null,
        childThreadId,
        nativeTaskRef: null,
        prompt: "Summarize the diff.",
        title: null,
        model: "ant-model",
        status: "running",
        result: null,
        startedAt: null,
        completedAt: null,
      };
      let delegated = false;
      const layerDependencies = Layer.mergeAll(
        NodeServices.layer,
        Layer.mock(ThreadManagementService.ThreadManagementService)({
          hasReceipt: () => Effect.succeed(false),
          assertProviderInstanceEnabled: () => Effect.void,
          isProviderInstanceEnabledForProject: () => Effect.succeed(true),
          getThreadRecords: (threadId) =>
            Effect.succeed(
              threadId === parentThreadId
                ? parentProjection(delegated ? [task] : [])
                : childProjection,
            ),
          dispatch: (command) =>
            Ref.update(dispatched, (commands) => [...commands, command]).pipe(
              Effect.andThen(
                Effect.sync(() => {
                  delegated = true;
                }),
              ),
              Effect.as({
                sequence: 1,
                storedEvents: [
                  {
                    sequence: 1,
                    commandId: null,
                    event: { type: "subagent.updated", payload: task },
                  },
                ],
              } as never),
            ),
        }),
        ServerSettings.layerTest(),
        Layer.mock(ProviderRegistry.ProviderRegistry)({
          getProviders: Effect.succeed([
            providerSnapshot({
              instanceId: codexInstanceId,
              driver: ProviderDriverKind.make("codex"),
              model: "gpt-5.4",
            }),
            providerSnapshot({
              instanceId: antigravityInstanceId,
              driver: ProviderDriverKind.make("antigravity"),
              model: "ant-model",
            }),
          ]),
        }),
        adapterRegistryLayer([codexInstanceId, antigravityInstanceId]),
        Layer.mock(ProjectService.ProjectService)({}),
        Layer.mock(SecretRequests.SecretRequests)({}),
        Layer.mock(RemoteDelegation.RemoteDelegation)({}),
        Layer.mock(ScheduledTaskService.ScheduledTaskService)({}),
      );

      yield* Effect.gen(function* () {
        const service = yield* OrchestratorMcpService.OrchestratorMcpService;
        const result = yield* service.delegateTask(scope, {
          task: "Summarize the diff.",
          target: { driverKind: ProviderDriverKind.make("antigravity") },
          mode: "async",
          clientRequestId: "delegate-antigravity-driver-1",
        });
        assert.equal(result.status, "running");
        const commands = yield* Ref.get(dispatched);
        assert.equal(commands.length, 1);
        const request = commands[0] as {
          modelSelection: { instanceId: string; model: string };
        };
        assert.equal(request.modelSelection.instanceId, antigravityInstanceId);
        assert.equal(request.modelSelection.model, "ant-model");
      }).pipe(Effect.provide(layerOrchestratorService.pipe(Layer.provide(layerDependencies))));
    }),
  );
});

it.effect(
  "serializes historical and blocked flags through actual MCP thread list and detail methods",
  () =>
    Effect.gen(function* () {
      const providerInstanceId = ProviderInstanceId.make("codex");
      const threadId = ThreadId.make("thread:mcp-block-serializer");
      const scope: McpInvocationScope = {
        environmentId: EnvironmentId.make("environment:mcp-block-serializer"),
        requestNamespace: "session:mcp-block-serializer",
        thread: { threadId, providerSessionId: "session:mcp-block-serializer", providerInstanceId },
        client: undefined,
        capabilities: new Set(["orchestration"]),
        issuedAt: 1,
      };
      for (const blocked of [undefined, false, true]) {
        const shell = {
          ...v2PullRequestThread({
            id: threadId,
            projectId: ProjectId.make("project:mcp-block-serializer"),
            title: "Serializer fixture",
            modelSelection: { instanceId: providerInstanceId, model: "test-model" },
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: null,
            pullRequests: [],
            latestUserMessageAt: null,
            createdAt: "2026-10-04T00:00:00Z",
            updatedAt: "2026-10-04T00:00:00Z",
            archivedAt: null,
            settledOverride: null,
            settledAt: null,
          }),
          ...(blocked === undefined ? {} : { threadMessagesBlocked: blocked }),
        };
        const projection = emptyProjection({
          type: "thread.created",
          id: EventId.make("fixture:mcp-block-serializer"),
          threadId,
          occurredAt: shell.createdAt,
          payload: { ...shell, lastVisitedAt: null },
        });
        const dependencies = Layer.mergeAll(
          NodeServices.layer,
          ServerSettings.layerTest(),
          Layer.mock(ThreadManagementService.ThreadManagementService)({
            getThreadRecords: () => Effect.succeed(projection),
            getThreadShell: () => Effect.succeed(shell),
            getProjectThreadRecords: () => Effect.succeed(projection),
            listProjectThreads: () => Effect.succeed([shell]),
            ...(blocked === undefined
              ? {}
              : {
                  observeThreadActivity: () =>
                    Effect.succeed({
                      foreground: null,
                      background: "monitoring" as const,
                      backgroundStatus: "known" as const,
                    }),
                }),
            getTimelinePage: () => Effect.succeed({ items: [], totalItems: 0, hasMore: false }),
          }),
          Layer.mock(ProviderRegistry.ProviderRegistry)({}),
          Layer.mock(ProviderAdapterRegistry.ProviderAdapterRegistryV2)({}),
          Layer.mock(ScheduledTaskService.ScheduledTaskService)({}),
          Layer.mock(ProjectService.ProjectService)({}),
          Layer.mock(SecretRequests.SecretRequests)({}),
          Layer.mock(RemoteDelegation.RemoteDelegation)({}),
        );
        yield* Effect.gen(function* () {
          const service = yield* OrchestratorMcpService.OrchestratorMcpService;
          const listed = yield* service.listThreads(scope, {});
          const detail = yield* service.readThread(scope, { threadId });
          assert.equal(listed.threads[0]?.threadMessagesBlocked, blocked ?? false);
          assert.equal(listed.threads[0]?.status, shell.activityRunStatus ?? shell.status);
          assert.equal(
            listed.threads[0]?.activityObservation?.background,
            blocked === undefined ? undefined : "monitoring",
          );
          assert.equal(detail.thread.threadMessagesBlocked, blocked ?? false);
          assert.equal(listed.threads[0]?.threadId, threadId);
          assert.equal(detail.thread.threadId, threadId);
          assert.equal(listed.threads[0]?.title, "Serializer fixture");
          assert.equal(detail.thread.title, "Serializer fixture");
        }).pipe(Effect.provide(layerOrchestratorService.pipe(Layer.provide(dependencies))));
      }
    }),
);

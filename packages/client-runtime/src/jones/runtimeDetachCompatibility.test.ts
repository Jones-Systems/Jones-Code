import {
  CommandId,
  EnvironmentId,
  type VcsListRefsInput,
  type VcsListRefsResult,
  ORCHESTRATION_V2_WS_METHODS,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ThreadId,
  WS_METHODS,
  type OrchestrationV2Command,
  type OrchestrationV2ProviderSession,
  type OrchestrationV2ThreadLaunchInput,
  type OrchestrationV2ThreadProjection,
  type ProjectMutation,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SubscriptionRef from "effect/SubscriptionRef";
import {
  AVAILABLE_CONNECTION_STATE,
  PrimaryConnectionTarget,
  type PreparedConnection,
} from "../connection/model.ts";
import * as EnvironmentSupervisor from "../connection/supervisor.ts";
import * as RpcSession from "../rpc/session.ts";
import type { EnvironmentRpcFailure } from "../rpc/client.ts";
import type { WsRpcProtocolClient } from "../rpc/protocol.ts";
import { v2Now, v2Projection, v2ThreadId } from "../state/orchestrationV2TestFixtures.ts";
import { stopThreadSession } from "../operations/commands.ts";

const layerTestCrypto = Layer.succeed(
  Crypto.Crypto,
  Crypto.make({
    randomBytes: (size) => new Uint8Array(size),
    digest: (_algorithm, data) => Effect.succeed(data),
  }),
);

const TARGET = new PrimaryConnectionTarget({
  environmentId: EnvironmentId.make("environment-1"),
  label: "Test environment",
  httpBaseUrl: "https://environment.example.test",
  wsBaseUrl: "wss://environment.example.test",
});

const makeSupervisor = Effect.fn("TestEnvironmentCommands.makeSupervisor")(function* (input: {
  readonly listRefs?: (
    input: VcsListRefsInput,
  ) => Effect.Effect<VcsListRefsResult, EnvironmentRpcFailure<typeof WS_METHODS.vcsListRefs>>;
  readonly onLaunch?: () => Effect.Effect<void>;
  readonly commands: OrchestrationV2Command[];
  readonly projects: ProjectMutation[];
  readonly launches?: OrchestrationV2ThreadLaunchInput[];
  readonly projection?: OrchestrationV2ThreadProjection;
  readonly projectionRequests?: ThreadId[];
  readonly advertiseServerResolvedCommandContext?: boolean;
  readonly advertiseQueuedToolBoundaryDelivery?: boolean;
}) {
  const client = {
    [WS_METHODS.vcsListRefs]: input.listRefs ?? (() => Effect.never),
    [ORCHESTRATION_V2_WS_METHODS.dispatchCommand]: (command: OrchestrationV2Command) =>
      Effect.sync(() => {
        input.commands.push(command);
        return { sequence: input.commands.length };
      }),
    [ORCHESTRATION_V2_WS_METHODS.getThreadProjection]: (requestInput: {
      readonly threadId: ThreadId;
    }) =>
      Effect.sync(() => {
        input.projectionRequests?.push(requestInput.threadId);
        return input.projection ?? v2Projection;
      }),
    [ORCHESTRATION_V2_WS_METHODS.launchThread]: (launchInput: OrchestrationV2ThreadLaunchInput) =>
      Effect.gen(function* () {
        input.launches?.push(launchInput);
        yield* input.onLaunch?.() ?? Effect.void;
        return {
          threadId: launchInput.threadId ?? v2ThreadId,
          projection: input.projection ?? v2Projection,
          resumed: false,
        };
      }),
    [WS_METHODS.projectsMutate]: (mutation: ProjectMutation) =>
      Effect.sync(() => {
        input.projects.push(mutation);
        return {
          id: mutation.projectId,
          title: mutation.type === "project.create" ? mutation.title : "Project",
          workspaceRoot:
            mutation.type === "project.create" ? mutation.workspaceRoot : "/workspace/project",
          repositoryIdentity: null,
          faviconPath: null,
          defaultModelSelection: null,
          scripts: [],
          createdAt: "2026-06-06T00:00:00.000Z",
          updatedAt: "2026-06-06T00:00:00.000Z",
          deletedAt: null,
        };
      }),
  } as unknown as WsRpcProtocolClient;
  const session: RpcSession.RpcSession = {
    client,
    initialConfig: Effect.succeed({
      environment: {
        capabilities: {
          repositoryIdentity: true,
          ...(input.advertiseQueuedToolBoundaryDelivery === undefined
            ? {}
            : { queuedToolBoundaryDelivery: input.advertiseQueuedToolBoundaryDelivery }),
          ...(input.advertiseServerResolvedCommandContext === false
            ? {}
            : { serverResolvedCommandContext: true }),
        },
      },
    } as never),
    subscribeServerConfig: (input) => client.subscribeServerConfig(input),
    ready: Effect.void,
    probe: Effect.void,
    closed: Effect.never,
  };
  return EnvironmentSupervisor.EnvironmentSupervisor.of({
    target: TARGET,
    state: yield* SubscriptionRef.make(AVAILABLE_CONNECTION_STATE),
    session: yield* SubscriptionRef.make(Option.some(session)),
    prepared: yield* SubscriptionRef.make(Option.none<PreparedConnection>()),
    connect: Effect.void,
    disconnect: Effect.void,
    retryNow: Effect.void,
  } satisfies EnvironmentSupervisor.EnvironmentSupervisor["Service"]);
});

describe("V2 environment commands", () => {
  it.effect(
    "stops all projected sessions with detach only, preserving thread lifecycle state",
    () =>
      Effect.gen(function* () {
        const projection: OrchestrationV2ThreadProjection = {
          ...v2Projection,
          thread: { ...v2Projection.thread, settledOverride: "active" },
          providerSessions: (["ready", "running"] as const).map(
            (status, index): OrchestrationV2ProviderSession => ({
              id: ProviderSessionId.make(`session-${index}`),
              driver: ProviderDriverKind.make("codex"),
              providerInstanceId: ProviderInstanceId.make("codex"),
              status,
              cwd: "/workspace/project",
              model: null,
              capabilities: {
                sessions: {
                  supportsMultipleProviderThreadsPerSession: false,
                  supportsModelSwitchInSession: false,
                  supportsProviderSwitchingViaHandoff: false,
                  supportsRuntimeModeSwitchInSession: false,
                  pendingRequestsSurviveRestart: false,
                },
                threads: {
                  canCreateEmptyThread: false,
                  canReadThreadSnapshot: false,
                  canRollbackThread: false,
                  canForkThread: false,
                  canForkFromTurn: false,
                  canForkFromSubagentThread: false,
                  exposesNativeThreadId: false,
                },
                turns: {
                  exposesNativeTurnId: false,
                  emitsTurnStarted: false,
                  emitsTurnCompleted: false,
                  supportsInterrupt: false,
                  supportsActiveSteering: false,
                  supportsSteeringByInterruptRestart: false,
                  supportsQueuedMessages: false,
                  terminalStatusQuality: "none",
                },
                streaming: {
                  streamsAssistantText: false,
                  streamsReasoning: false,
                  streamsToolOutput: false,
                  streamsPlanText: false,
                  emitsMessageCompleted: false,
                },
                tools: {
                  exposesToolItemIds: false,
                  emitsToolStarted: false,
                  emitsToolCompleted: false,
                  emitsToolOutput: false,
                  supportsMcpTools: false,
                  supportsDynamicToolCallbacks: false,
                },
                approvals: {
                  supportsCommandApproval: false,
                  supportsFileReadApproval: false,
                  supportsFileChangeApproval: false,
                  supportsApplyPatchApproval: false,
                  approvalsHaveNativeRequestIds: false,
                  approvalCallbacksAreLiveOnly: false,
                  approvalsCanOriginateFromSubagents: false,
                },
                planning: {
                  emitsPlanUpdated: false,
                  emitsTodoList: false,
                  emitsProposedPlan: false,
                  supportsStructuredQuestions: false,
                  planDeltasHaveItemIds: false,
                },
                subagents: {
                  supportsSubagents: false,
                  exposesSubagentThreadIds: false,
                  emitsSubagentLifecycle: false,
                  canWaitForSubagents: false,
                  canCloseSubagents: false,
                  canForkSubagentThread: false,
                },
                context: {
                  acceptsSystemContext: false,
                  acceptsDeveloperContext: false,
                  acceptsSyntheticUserContext: false,
                  canGenerateSummaries: false,
                  canConsumeHandoffSummaries: false,
                  supportsDeltaHandoff: false,
                  supportsFullThreadHandoff: false,
                  maxRecommendedHandoffChars: null,
                },
                checkpointing: {
                  appCanCheckpointFilesystem: false,
                  supportsNestedCheckpointScopes: false,
                  providerCanRollbackConversation: false,
                  providerRollbackReturnsSnapshot: false,
                  providerCanReadConversationSnapshot: false,
                },
                identity: {
                  nativeThreadIds: "none",
                  nativeTurnIds: "none",
                  nativeItemIds: "none",
                  nativeRequestIds: "none",
                },
                runtimePolicy: {
                  enforcement: "client-boundary",
                },
              },
              createdAt: v2Now,
              updatedAt: v2Now,
              lastError: null,
            }),
          ),
        };
        const commands: OrchestrationV2Command[] = [];
        const projectionRequests: ThreadId[] = [];
        const supervisor = yield* makeSupervisor({
          commands,
          projects: [],
          projection,
          projectionRequests,
        });
        const result = yield* stopThreadSession({
          threadId: v2ThreadId,
          commandId: CommandId.make("stop-command"),
        }).pipe(Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor));
        expect(projectionRequests).toEqual([v2ThreadId]);
        expect(commands).toEqual(
          projection.providerSessions.map((session) => ({
            type: "provider-session.detach",
            threadId: v2ThreadId,
            commandId: `stop-command:detach:${session.id}`,
            providerSessionId: session.id,
            reason: "client-requested",
          })),
        );
        expect(result).toEqual({ sequence: 2 });
      }).pipe(Effect.provide(layerTestCrypto)),
  );

  it.effect("does not dispatch any lifecycle command when no provider sessions remain", () =>
    Effect.gen(function* () {
      const commands: OrchestrationV2Command[] = [];
      const supervisor = yield* makeSupervisor({
        commands,
        projects: [],
        projection: v2Projection,
      });
      const result = yield* stopThreadSession({
        threadId: v2ThreadId,
        commandId: CommandId.make("stop-empty"),
      }).pipe(Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor));
      expect(commands).toEqual([]);
      expect(result).toEqual({ sequence: 0 });
    }).pipe(Effect.provide(layerTestCrypto)),
  );
});

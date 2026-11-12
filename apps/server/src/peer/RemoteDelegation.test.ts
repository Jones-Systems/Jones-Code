import * as InProcessMcp from "../mcp/McpHttpServer.ts";
import * as DelegationIntents from "../jones/remoteDelegation/IntentStore.ts";
import type {
  OrchestratorMcpThreadReadResult,
  OrchestratorMcpThreadWaitResult,
} from "@t3tools/contracts";
import {
  AuthSessionId,
  OrchestratorMcpDelegateTaskResult,
  AuthAdministrativeScopes,
  OrchestratorMcpFailure,
  type OrchestratorMcpThreadWaitInput,
} from "@t3tools/contracts";
import * as Duration from "effect/Duration";
import * as TestClock from "effect/testing/TestClock";
import * as ProviderInstanceRegistry from "../provider/ProviderInstanceRegistry.ts";
import type { ProviderInstance } from "@t3tools/provider-core/server/driver";
import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import * as RemoteAdmission from "../jones/remoteAdmission/RemoteAdmission.ts";
import * as AdmissionIntake from "../jones/remoteAdmissionIntegration/RemoteAdmissionIntake.ts";
import * as AdmissionCancel from "../jones/remoteAdmissionIntegration/cancelTool.ts";
import * as DocumentText from "../provider/DocumentText.ts";
import { NodeHttpServer } from "@effect/platform-node";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CommandId,
  EventId,
  MessageId,
  NodeId,
  type OrchestrationV2Run,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  type Project,
  type RepositoryIdentity,
  type ServerProvider,
  RunId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as SqlStatement from "effect/sql/Statement";
import * as Digest from "effect/crypto/Digest";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as Tracer from "effect/Tracer";
import type * as SqlClient from "effect/sql/SqlClient";
import { McpSchema, McpServer } from "effect/ai";

import * as ServerConfig from "../config.ts";
import * as McpHttpServer from "../mcp/McpHttpServer.ts";
import * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import { idleThreadProjection, liveThreadShell } from "../mcp/McpToolAccess.testkit.ts";
import { CodexProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderAdapterRegistry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as ThreadLaunch from "../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as ProviderReplayHarness from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ManagedProjectFolders from "../project/ManagedProjectFolders.ts";
import * as ProjectCloneTracker from "../project/ProjectCloneTracker.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as RepositoryIdentityResolver from "../project/RepositoryIdentityResolver.ts";
import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import * as ScheduledTaskService from "../scheduledTasks/ScheduledTaskService.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as CommandReceiptStore from "../orchestration-v2/CommandReceiptStore.ts";
import * as SourceControlRepositoryService from "../sourceControl/SourceControlRepositoryService.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as PeerForwarding from "./PeerForwarding.ts";
import * as PeerLinks from "./PeerLinks.ts";
import { descriptorOf, layerLinkingEnvironment, linkTo, servePeer } from "./PeerLinks.testkit.ts";
import * as RemoteDelegation from "./RemoteDelegation.ts";
import * as PeerLinkRequests from "./PeerLinkRequests.ts";

// The laptop's agent delegates a task to the box through a link. The box is
// its real /mcp behind real OAuth, with one thread the test finishes; the
// laptop is its real orchestrator and toolkits. The parent here wakes as it
// would for a local child.

const laptop = descriptorOf("environment-laptop", "Laptop");
const box = descriptorOf("environment-box", "Box");
const boxProject = ProjectId.make("project:box");
const instanceId = ProviderInstanceId.make("codex");
const driver = ProviderDriverKind.make("codex");
const modelSelection = { instanceId, model: "gpt-5.4" };

type LaunchedWith = Pick<
  ThreadLaunch.ThreadLaunchInput,
  "modelSelection" | "runtimeMode" | "interactionMode" | "workspaceStrategy"
>;

/** The thread the box launches for the task, which the test finishes. */
interface BoxThread {
  readonly launched: Ref.Ref<ThreadId | null>;
  readonly launchCalls: Ref.Ref<ReadonlyArray<ThreadLaunch.ThreadLaunchInput>>;
  /** The link session that launched it, as the box stamps it. */
  readonly linkOrigin: Ref.Ref<{ readonly sessionId: string; readonly label: string } | null>;
  /** The parent the launch names, so the thread there can link back to it. */
  readonly delegatedFrom: Ref.Ref<ThreadLaunch.ThreadLaunchInput["delegatedFrom"] | null>;
  /** The model and modes it was launched with. */
  readonly launchedWith: Ref.Ref<LaunchedWith | null>;
  /** The first message the thread there was launched with. */
  readonly message: Ref.Ref<string | null>;
  readonly status: Ref.Ref<OrchestrationV2Run["status"]>;
  readonly reply: Ref.Ref<string | null>;
  readonly error: Ref.Ref<string | null>;
  readonly laterError: Ref.Ref<string | null>;
  /** A later run's reply, after the task's own, as a follow-up message would leave. */
  readonly laterReply: Ref.Ref<string | null>;
  readonly laterRun: Ref.Ref<OrchestrationV2Run | null>;
  readonly waitStarted: Deferred.Deferred<void>;
  /** The model the provider there reported running the task's run. */
  readonly executedModel: Ref.Ref<string | null>;
  /** The run each wait asked for, in order. */
  readonly waitedFor: Ref.Ref<ReadonlyArray<RunId | undefined>>;
  /** The thread's run ends with a task of its own still running, whose result wakes it again. */
  readonly nested: Ref.Ref<boolean>;
  /** Completed once a wait there blocks on that task. */
  readonly nestedWaiting: Deferred.Deferred<void>;
  /** That task settled and the run it woke answered, with `laterReply`. */
  readonly nestedSettled: Deferred.Deferred<void>;
  /** Waiting there fails with this, as a thread deleted there does. */
  readonly gone: Ref.Ref<boolean>;
  readonly interrupted: Ref.Ref<number>;
  /** What each interrupt or stop there did, once per command id. */
  readonly stops: Ref.Ref<ReadonlyArray<"interrupt" | "thread.stop" | "delegated-tasks">>;
  /** The launch starts no run, as one recorded before its run was kept. */
  readonly runless: Ref.Ref<boolean>;
  /** Completed on the first interrupt there. */
  readonly interruptedOnce: Deferred.Deferred<void>;
  readonly finished: Deferred.Deferred<void>;
  /** While set, reading the thread there waits: completed once a read starts, released by `releaseRead`. */
  readonly holdRead: Ref.Ref<boolean>;
  readonly readStarted: Deferred.Deferred<void>;
  readonly releaseRead: Deferred.Deferred<void>;
  readonly holdFirstStop: Ref.Ref<boolean>;
  readonly stopStarted: Deferred.Deferred<void>;
  readonly releaseStop: Deferred.Deferred<void>;
  /** Messages sent to the thread there after its launch, each starting a later run. */
  readonly sent: Ref.Ref<ReadonlyArray<string>>;
  /** Runs as the launch there is answered, before the laptop hears back. */
  readonly afterLaunch: Ref.Ref<Effect.Effect<void>>;
}

const makeBoxThread = Effect.gen(function* () {
  return {
    launched: yield* Ref.make<ThreadId | null>(null),
    launchCalls: yield* Ref.make<ReadonlyArray<ThreadLaunch.ThreadLaunchInput>>([]),
    linkOrigin: yield* Ref.make<{ readonly sessionId: string; readonly label: string } | null>(
      null,
    ),
    delegatedFrom: yield* Ref.make<ThreadLaunch.ThreadLaunchInput["delegatedFrom"] | null>(null),
    launchedWith: yield* Ref.make<LaunchedWith | null>(null),
    message: yield* Ref.make<string | null>(null),
    status: yield* Ref.make<OrchestrationV2Run["status"]>("running"),
    reply: yield* Ref.make<string | null>("Working…"),
    error: yield* Ref.make<string | null>(null),
    laterError: yield* Ref.make<string | null>(null),
    laterReply: yield* Ref.make<string | null>(null),
    laterRun: yield* Ref.make<OrchestrationV2Run | null>(null),
    waitStarted: yield* Deferred.make<void>(),
    executedModel: yield* Ref.make<string | null>(null),
    waitedFor: yield* Ref.make<ReadonlyArray<RunId | undefined>>([]),
    nested: yield* Ref.make(false),
    nestedWaiting: yield* Deferred.make<void>(),
    nestedSettled: yield* Deferred.make<void>(),
    gone: yield* Ref.make(false),
    interrupted: yield* Ref.make(0),
    stops: yield* Ref.make<ReadonlyArray<"interrupt" | "thread.stop" | "delegated-tasks">>([]),
    runless: yield* Ref.make(false),
    interruptedOnce: yield* Deferred.make<void>(),
    finished: yield* Deferred.make<void>(),
    holdRead: yield* Ref.make(false),
    readStarted: yield* Deferred.make<void>(),
    releaseRead: yield* Deferred.make<void>(),
    holdFirstStop: yield* Ref.make(false),
    stopStarted: yield* Deferred.make<void>(),
    releaseStop: yield* Deferred.make<void>(),
    sent: yield* Ref.make<ReadonlyArray<string>>([]),
    afterLaunch: yield* Ref.make<Effect.Effect<void>>(Effect.void),
  } satisfies BoxThread;
});

/** The box's projects: more than a page, with this repository's last. */
const boxProjects: ReadonlyArray<Project> = [
  ...Array.from({ length: 120 }, (_, index) => ({
    id: ProjectId.make(`project:box-other-${index}`),
    title: `other ${index}`,
    workspaceRoot: `/srv/other-${index}`,
    repositoryIdentity: null,
    defaultModelSelection: null,
    scripts: [],
    createdAt: "2026-10-01T00:00:00.000Z",
    updatedAt: "2026-10-01T00:00:00.000Z",
    deletedAt: null,
  })),
  {
    id: boxProject,
    title: "app",
    workspaceRoot: "/srv/app",
    repositoryIdentity: {
      canonicalKey: "github.com/acme/app",
      locator: {
        source: "git-remote",
        remoteName: "origin",
        remoteUrl: "git@github.com:acme/app.git",
      },
    },
    defaultModelSelection: null,
    scripts: [],
    createdAt: "2026-10-01T00:00:00.000Z",
    updatedAt: "2026-10-01T00:00:00.000Z",
    deletedAt: null,
  },
];

const serveBox = (thread: BoxThread) => {
  return servePeer(box, boxToolkitLayer(thread));
};

const boxRunId = RunId.make("run:box");
const boxLaterRunId = RunId.make("run:box-later");

const boxToolkitLayer = (thread: BoxThread) => {
  const runId = boxRunId;
  const interrupts = new Set<string>();
  const shellOf = (threadId: ThreadId) =>
    Effect.gen(function* () {
      if ((yield* Ref.get(thread.launched)) !== threadId) return null;
      const linkOrigin = yield* Ref.get(thread.linkOrigin);
      return {
        ...liveThreadShell(threadId, { runtimeMode: "full-access" }),
        projectId: boxProject,
        ...(linkOrigin === null ? {} : { linkOrigin }),
      };
    });
  const runOf = (status: OrchestrationV2Run["status"]) =>
    ({
      id: runId,
      threadId: ThreadId.make("thread:box"),
      ordinal: 1,
      status,
      modelSelection,
      activeAttemptId: "attempt:box",
      requestedAt: DateTime.makeUnsafe(0),
      startedAt: DateTime.makeUnsafe(0),
      completedAt: ["completed", "failed", "cancelled", "interrupted", "rolled_back"].includes(
        status,
      )
        ? DateTime.makeUnsafe(1_000)
        : null,
    }) as unknown as OrchestrationV2Run;
  const projectionOf = (threadId: ThreadId) =>
    Effect.gen(function* () {
      const shell = yield* shellOf(threadId);
      const status = yield* Ref.get(thread.status);
      const runless = yield* Ref.get(thread.runless);
      const laterRun = yield* Ref.get(thread.laterRun);
      const model = yield* Ref.get(thread.executedModel);
      return shell === null
        ? (null as never)
        : {
            ...idleThreadProjection(shell),
            runs: runless ? [] : [runOf(status), ...(laterRun === null ? [] : [laterRun])],
            providerTurns: (model === null
              ? []
              : [{ runAttemptId: "attempt:box", ordinal: 1, model }]) as never,
          };
    });
  // One record per command id, as its receipt replays a repeat.
  const recordStop = (commandId: string, kind: "interrupt" | "thread.stop" | "delegated-tasks") =>
    Effect.gen(function* () {
      if (interrupts.has(`${kind}:${commandId}`)) return;
      interrupts.add(`${kind}:${commandId}`);
      yield* Ref.update(thread.stops, (seen) => [...seen, kind]);
      if (kind !== "delegated-tasks") {
        yield* Ref.update(thread.interrupted, (count) => count + 1);
        yield* Deferred.succeed(thread.interruptedOnce, undefined);
      }
    });
  const layerBoxThreads = Layer.mock(ThreadManagement.ThreadManagementService)({
    getThreadShell: shellOf,
    getProjectThreadRecords: (input) => projectionOf(input.threadId),
    getThreadRecords: (threadId) => projectionOf(threadId),
    waitForThread: (input) =>
      Effect.gen(function* () {
        yield* Ref.update(thread.waitedFor, (seen) => [...seen, input.runId]);
        yield* Deferred.succeed(thread.waitStarted, undefined);
        // A thread with no run has nothing to wait for.
        if (yield* Ref.get(thread.runless)) {
          return { threadId: input.threadId, run: null, timedOut: false };
        }
        if (yield* Ref.get(thread.gone)) {
          return yield* new ThreadManagement.ThreadManagementThreadNotFoundError({
            projectId: boxProject,
            threadId: input.threadId,
          });
        }
        const done = yield* Deferred.await(thread.finished).pipe(
          Effect.timeoutOption(input.timeoutMs),
        );
        // Only a waiter that asks waits past the run for the thread's own task.
        if (
          Option.isSome(done) &&
          input.untilDelegatedWorkSettled === true &&
          (yield* Ref.get(thread.nested))
        ) {
          yield* Deferred.succeed(thread.nestedWaiting, undefined);
          const settled = yield* Deferred.await(thread.nestedSettled).pipe(
            Effect.timeoutOption(input.timeoutMs),
          );
          return {
            threadId: input.threadId,
            run: { id: runId, status: yield* Ref.get(thread.status) } as OrchestrationV2Run,
            timedOut: Option.isNone(settled),
            ...(Option.isSome(settled)
              ? { resultRun: { id: boxLaterRunId, status: "completed" } as OrchestrationV2Run }
              : {}),
          };
        }
        // Unpinned, a wait reports the latest run: a later one once there is one.
        const later = input.runId === undefined && (yield* Ref.get(thread.laterReply)) !== null;
        return {
          threadId: input.threadId,
          run: {
            id: later ? boxLaterRunId : runId,
            status: later ? "completed" : yield* Ref.get(thread.status),
          } as OrchestrationV2Run,
          timedOut: Option.isNone(done),
        };
      }),
    getTimelinePage: (threadId, options) =>
      Effect.gen(function* () {
        if (yield* Ref.get(thread.holdRead)) {
          yield* Deferred.succeed(thread.readStarted, undefined);
          yield* Deferred.await(thread.releaseRead);
        }
        const reply = yield* Ref.get(thread.reply);
        const later = yield* Ref.get(thread.laterReply);
        const error = yield* Ref.get(thread.error);
        const laterError = yield* Ref.get(thread.laterError);
        // One item per page, with errors available only in the activity view.
        const replies = [
          ...(reply === null
            ? []
            : [
                {
                  id: TurnItemId.make("item:reply"),
                  runId,
                  text: reply,
                  type: "assistant_message",
                },
              ]),
          ...(error === null
            ? []
            : [{ id: TurnItemId.make("item:error"), runId, text: error, type: "error" }]),
          ...(later === null
            ? []
            : [
                {
                  id: TurnItemId.make("item:later-reply"),
                  runId: boxLaterRunId,
                  text: later,
                  type: "assistant_message",
                },
              ]),
          ...(laterError === null
            ? []
            : [
                {
                  id: TurnItemId.make("item:later-error"),
                  runId: boxLaterRunId,
                  text: laterError,
                  type: "error",
                },
              ]),
        ].map((entry, position) => ({ ...entry, position }));
        const visible =
          options.itemId === undefined
            ? replies.filter(
                (entry) => options.view === "activity" || entry.type === "assistant_message",
              )
            : replies.filter((entry) => entry.id === options.itemId);
        const matching = visible
          .filter((entry) => entry.position > (options.afterPosition ?? -1))
          .slice(0, 1);
        return {
          items: matching.map(({ id, runId: itemRunId, text, position, type }) => ({
            position,
            visibility: "local" as const,
            sourceThreadId: threadId,
            sourceItemId: id,
            item: {
              id,
              threadId,
              runId: itemRunId,
              nodeId: null,
              providerThreadId: null,
              providerTurnId: null,
              nativeItemRef: null,
              parentItemId: null,
              ordinal: 1,
              status: "completed" as const,
              title: null,
              startedAt: DateTime.makeUnsafe(0),
              completedAt: DateTime.makeUnsafe(0),
              updatedAt: DateTime.makeUnsafe(0),
              ...(type === "error"
                ? { type, failure: { message: text } }
                : {
                    type: "assistant_message" as const,
                    messageId: MessageId.make(`message:${id}`),
                    text,
                    streaming: false,
                  }),
            } as never,
          })),
          totalItems: replies.length,
          hasMore:
            options.itemId === undefined &&
            matching.length > 0 &&
            visible.some((entry) => entry.position > matching.at(-1)!.position),
        };
      }),
    interruptThread: (input) =>
      recordStop(input.commandId, "interrupt").pipe(
        Effect.as({ type: "interrupt_requested", run: runOf("running") } as never),
      ),
    dispatch: (command) =>
      command.type === "thread.stop"
        ? recordStop(command.commandId, "thread.stop").pipe(
            Effect.tap(() =>
              Effect.gen(function* () {
                if (!(yield* Ref.getAndSet(thread.holdFirstStop, false))) return;
                yield* Deferred.succeed(thread.stopStarted, undefined);
                yield* Deferred.await(thread.releaseStop);
              }),
            ),
            Effect.as({ sequence: 0, storedEvents: [] } as never),
          )
        : Effect.die(`The box does not expect ${command.type}`),
    stopDelegatedTasks: (input) => recordStop(input.commandId, "delegated-tasks"),
    sendToThread: (input) =>
      Ref.update(thread.sent, (seen) => [...seen, input.text]).pipe(
        Effect.as({
          run: { id: boxLaterRunId, status: "running" },
          delivery: "started",
        } as never),
      ),
  });
  const layerBoxLaunches = Layer.mock(ThreadLaunch.ThreadLaunchService)({
    checkWorktreeBase: () => Effect.void,
    launch: (input) =>
      Ref.get(thread.runless).pipe(
        Effect.tap(() => Ref.update(thread.launchCalls, (calls) => [...calls, input])),
        Effect.flatMap((runless) =>
          Ref.set(thread.launched, input.threadId!).pipe(
            Effect.andThen(Ref.set(thread.linkOrigin, input.linkOrigin ?? null)),
            Effect.andThen(Ref.set(thread.delegatedFrom, input.delegatedFrom ?? null)),
            Effect.andThen(Ref.set(thread.message, input.initialMessage?.text ?? null)),
            Effect.andThen(
              Ref.set(thread.launchedWith, {
                modelSelection: input.modelSelection,
                runtimeMode: input.runtimeMode,
                interactionMode: input.interactionMode,
                workspaceStrategy: input.workspaceStrategy,
              }),
            ),
            Effect.andThen(Effect.flatten(Ref.get(thread.afterLaunch))),
            Effect.as({
              threadId: input.threadId,
              projection: {
                thread: {
                  id: input.threadId,
                  projectId: input.projectId,
                  title: input.title,
                  modelSelection: input.modelSelection,
                },
                runs:
                  input.initialMessage?.messageId && !runless
                    ? [
                        {
                          id: runId,
                          status: "running",
                          userMessageId: input.initialMessage.messageId,
                        },
                      ]
                    : [],
              },
              resumed: false,
            } as unknown as ThreadLaunch.ThreadLaunchResult),
          ),
        ),
      ),
  });
  return Layer.merge(McpHttpServer.layerOrchestratorToolkit, McpHttpServer.layerProjectRegistration)
    .pipe(
      Layer.provide(DocumentText.layer.pipe(Layer.provideMerge(NodeServices.layer))),
      Layer.provide(NodeCrypto.layer),
      Layer.provide(layerBoxThreads),
      Layer.provide(layerBoxLaunches),
      Layer.provide(Layer.mock(Orchestrator.OrchestratorV2)({})),
      Layer.provide(Layer.mock(RemoteDelegation.RemoteDelegation)({})),
      Layer.provide(
        Layer.mergeAll(
          SqlitePersistence.layerMemory,
          Layer.mock(EventSink.EventSinkV2)({}),
          Layer.mock(CommandReceiptStore.CommandReceiptStoreV2)({
            getByCommandId: () => Effect.succeedNone,
          }),
        ),
      ),
      Layer.provide(Layer.mock(PeerForwarding.PeerForwarding)({})),
      Layer.provide(Layer.mock(PeerLinkRequests.PeerLinkRequests)({})),
      Layer.provide(Layer.mock(PeerLinks.PeerLinks)({})),
      Layer.provide(Layer.mock(ProjectCloneTracker.ProjectCloneTracker)({})),
      Layer.provide(
        Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({ namedProjectsRoot: "/p" }),
      ),
      Layer.provide(Layer.mock(GitVcsDriver.GitVcsDriver)({})),
      Layer.provide(Layer.mock(SourceControlRepositoryService.SourceControlRepositoryService)({})),
      Layer.provide(
        ServerConfig.layerTest(process.cwd(), { prefix: "t3-remote-delegation-box-" }).pipe(
          Layer.provide(NodeServices.layer),
        ),
      ),
      Layer.provide(
        Layer.mock(ProviderRegistry.ProviderRegistry)({ getProviders: Effect.succeed([]) }),
      ),
      Layer.provide(
        Layer.mock(ProviderAdapterRegistry.ProviderAdapterRegistryV2)({
          list: () => Effect.succeed([]),
        }),
      ),
      Layer.provide(Layer.mock(ScheduledTaskService.ScheduledTaskService)({})),
      Layer.provide(
        Layer.mock(ProjectService.ProjectService)({
          getById: (id) =>
            Effect.succeed(
              Option.fromUndefinedOr(boxProjects.find((project) => project.id === id)),
            ),
          // More than a page, so finding the project with this repository pages.
          listPage: ({ cursor, limit }) =>
            Effect.succeed({
              projects: boxProjects.slice(cursor, cursor + limit),
              nextCursor: cursor + limit < boxProjects.length ? cursor + limit : null,
            }),
        }),
      ),
      Layer.provide(Layer.mock(SecretRequests.SecretRequests)({})),
    )
    .pipe(
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(DocumentText.layer),
      Layer.provide(NodeServices.layer),
    );
};

const mcpClient = McpSchema.McpServerClient.of({
  clientId: 1,
  protocolVersion: "2025-06-18",
  clientCapabilities: {},
  clientInfo: { name: "remote-delegation", version: "1" },
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "remote-delegation", version: "1" },
  },
  getClient: Effect.die("unused"),
});

const adapter = {
  instanceId,
  driver,
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("No provider process needed"),
} as ProviderAdapter.ProviderAdapterV2["Service"];

/**
 * The laptop: a real orchestrator (in-memory SQLite) whose parent thread has
 * a live run, the real orchestrator toolkit, and RemoteDelegation. Built in
 * its own scope so a test can tear it down and build it again on the same
 * database, as a restart does.
 */
const makeLaptop = (
  database: Context.Context<SqlClient.SqlClient>,
  offers: SubscriptionRef.SubscriptionRef<
    ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
  >,
  /** The laptop's links from before a restart, which keep their sessions there. */
  kept?: Context.Context<Layer.Success<ReturnType<typeof layerLinkingEnvironment>>>,
  observeStopFailure?: (error: OrchestratorMcpFailure) => Effect.Effect<void>,
  transformObservation?: (
    read: OrchestratorMcpThreadReadResult,
  ) => Effect.Effect<OrchestratorMcpThreadReadResult, OrchestratorMcpFailure>,
  transformWait?: (
    wait: Effect.Effect<OrchestratorMcpThreadWaitResult, OrchestratorMcpFailure>,
    input: OrchestratorMcpThreadWaitInput,
  ) => Effect.Effect<OrchestratorMcpThreadWaitResult, OrchestratorMcpFailure>,
  observeForwardedCall?: (toolName: string) => void,
  observeStopCall?: (
    input: Parameters<PeerForwarding.PeerForwarding["Service"]["interruptThread"]>[2],
  ) => void,
  recoveryHooks?: {
    readonly openTasks?: (
      scan: ProjectionStore.ProjectionStoreV2["Service"]["getOpenRemoteDelegatedTasks"],
    ) => ProjectionStore.ProjectionStoreV2["Service"]["getOpenRemoteDelegatedTasks"];
    readonly domainEvents?: (
      events: ThreadManagement.ThreadManagementService["Service"]["streamDomainEvents"],
    ) => ThreadManagement.ThreadManagementService["Service"]["streamDomainEvents"];
    readonly read?: (
      read: OrchestratorMcpThreadReadResult,
    ) => Effect.Effect<OrchestratorMcpThreadReadResult, OrchestratorMcpFailure>;
    readonly interrupt?: (
      stop: ReturnType<PeerForwarding.PeerForwarding["Service"]["interruptThread"]>,
    ) => ReturnType<PeerForwarding.PeerForwarding["Service"]["interruptThread"]>;
  },
) =>
  Effect.gen(function* () {
    const linking = kept ?? (yield* layerLinkingEnvironment(laptop).pipe(Layer.build));
    const layerDatabase = Layer.succeedContext(database);
    const layerOrchestrator = Layer.mergeAll(
      layerDatabase,
      ProjectionStore.layer.pipe(Layer.provide(layerDatabase)),
      ProviderReplayHarness.layerWithRegistry(
        { name: "remote-delegation-laptop" },
        ProviderAdapterRegistry.layerFromAdapters([adapter]),
        { databaseLayer: layerDatabase, runEffectWorker: false },
      ),
    ).pipe(
      Layer.provide(
        Layer.succeed(ProviderContinuationRequests.ProviderContinuationRequests, {
          offer: (request) => SubscriptionRef.update(offers, (seen) => [...seen, request]),
          take: Effect.never,
        }),
      ),
    );
    const layerThreads = ThreadManagement.layer.pipe(
      Layer.provide(CommandReceiptStore.layer.pipe(Layer.provide(layerDatabase))),
      Layer.provideMerge(Layer.merge(layerOrchestrator, ServerSettings.layerTest())),
    );
    const layerForwarding =
      observeStopFailure === undefined &&
      transformObservation === undefined &&
      transformWait === undefined &&
      observeForwardedCall === undefined &&
      observeStopCall === undefined &&
      recoveryHooks?.read === undefined &&
      recoveryHooks?.interrupt === undefined
        ? PeerForwarding.layer
        : Layer.effect(
            PeerForwarding.PeerForwarding,
            Effect.gen(function* () {
              const forwarding = yield* PeerForwarding.PeerForwarding;
              const call: typeof forwarding.call = (scope, tool, environmentId, input) => {
                observeForwardedCall?.(tool.name);
                return forwarding.call(scope, tool, environmentId, input);
              };
              return {
                ...forwarding,
                call,
                waitForThread: (...args: Parameters<typeof forwarding.waitForThread>) => {
                  const wait = forwarding.waitForThread(...args);
                  return transformWait === undefined ? wait : transformWait(wait, args[2]);
                },
                readThread: (...args: Parameters<typeof forwarding.readThread>) =>
                  forwarding.readThread(...args).pipe(
                    Effect.flatMap((read) => recoveryHooks?.read?.(read) ?? Effect.succeed(read)),
                    Effect.filterOrElse(
                      () =>
                        args[2].childProgressForRunId === undefined ||
                        transformObservation === undefined,
                      (read) =>
                        transformObservation === undefined
                          ? Effect.succeed(read)
                          : transformObservation(read),
                    ),
                  ),
                interruptThread: (...args: Parameters<typeof forwarding.interruptThread>) => {
                  observeStopCall?.(args[2]);
                  return forwarding
                    .interruptThread(...args)
                    .pipe(
                      (stop) => recoveryHooks?.interrupt?.(stop) ?? stop,
                      Effect.tapError(observeStopFailure ?? (() => Effect.void)),
                    );
                },
              };
            }),
          ).pipe(Layer.provide(PeerForwarding.layer));
    const layerHere = McpHttpServer.layerOrchestratorToolkit.pipe(
      Layer.provideMerge(InProcessMcp.layerInProcessMcpServer),
      Layer.provideMerge(
        RemoteDelegation.layer.pipe(
          Layer.provide(
            Layer.merge(
              Layer.effect(
                ThreadManagement.ThreadManagementService,
                Effect.gen(function* () {
                  const threads = yield* ThreadManagement.ThreadManagementService;
                  return {
                    ...threads,
                    streamDomainEvents:
                      recoveryHooks?.domainEvents?.(threads.streamDomainEvents) ??
                      threads.streamDomainEvents,
                  };
                }),
              ),
              Layer.effect(
                ProjectionStore.ProjectionStoreV2,
                Effect.gen(function* () {
                  const projections = yield* ProjectionStore.ProjectionStoreV2;
                  return {
                    ...projections,
                    getOpenRemoteDelegatedTasks:
                      recoveryHooks?.openTasks?.(projections.getOpenRemoteDelegatedTasks) ??
                      projections.getOpenRemoteDelegatedTasks,
                  };
                }),
              ),
            ).pipe(Layer.provide(layerThreads)),
          ),
          Layer.provideMerge(layerForwarding),
          Layer.provide(
            Layer.mock(ProjectService.ProjectService)({
              // A cold enrichment cache: the project itself reports no identity yet.
              getById: () =>
                Effect.succeedSome({
                  workspaceRoot: "/home/me/app",
                  repositoryIdentity: null,
                } as never),
            }),
          ),
          Layer.provide(
            Layer.mock(RepositoryIdentityResolver.RepositoryIdentityResolver)({
              resolve: (cwd) =>
                Effect.succeed(
                  cwd === "/home/me/app"
                    ? ({ canonicalKey: "github.com/acme/app" } as RepositoryIdentity)
                    : null,
                ),
            }),
          ),
        ),
      ),
      Layer.provide(Layer.succeedContext(linking)),
      Layer.provide(Layer.mock(PeerLinkRequests.PeerLinkRequests)({})),
      Layer.provide(NodeCrypto.layer),
      Layer.provideMerge(layerThreads),
      Layer.provide(CommandReceiptStore.layer.pipe(Layer.provide(layerDatabase))),
      Layer.provide(Layer.mock(ProviderRegistry.ProviderRegistry)({})),
      Layer.provide(Layer.mock(ProviderAdapterRegistry.ProviderAdapterRegistryV2)({})),
      Layer.provide(Layer.mock(ScheduledTaskService.ScheduledTaskService)({})),
      Layer.provide(Layer.mock(ProjectService.ProjectService)({})),
      Layer.provide(Layer.mock(ProjectCloneTracker.ProjectCloneTracker)({})),
      Layer.provide(Layer.mock(SecretRequests.SecretRequests)({})),
      Layer.provide(Layer.mock(ThreadLaunch.ThreadLaunchService)({})),
      Layer.provide(Layer.mock(GitVcsDriver.GitVcsDriver)({})),
      Layer.provide(DocumentText.layer.pipe(Layer.provide(NodeServices.layer))),
      Layer.fresh,
    );
    const here = yield* Layer.build(layerHere);
    const server = Context.get(here, McpServer.McpServer);
    return {
      intents: yield* DelegationIntents.make.pipe(Effect.provide(linking)),
      links: Context.get(linking, PeerLinks.PeerLinks),
      settings: Context.get(linking, ServerSettings.ServerSettingsService),
      orchestrator: Context.get(here, Orchestrator.OrchestratorV2),
      remote: Context.get(here, RemoteDelegation.RemoteDelegation),
      threads: Context.get(here, ThreadManagement.ThreadManagementService),
      projections: Context.get(here, ProjectionStore.ProjectionStoreV2),
      sink: Context.get(here, EventSink.EventSinkV2),
      call: (name: string, args: Record<string, unknown>, scope = parentScope) =>
        server
          .callTool({ name, arguments: args })
          .pipe(
            Effect.provideService(McpInvocationContext.McpInvocationContext, scope),
            Effect.provideService(McpSchema.McpServerClient, mcpClient),
          ),
    };
  });

const parentThreadId = ThreadId.make("thread:laptop-parent");
const parentRunId = RunId.make("run:laptop-parent");
const parentRootNode = NodeId.make("node:laptop-parent:root");
const parentScope: McpInvocationContext.McpInvocationScope = {
  environmentId: laptop.environmentId,
  requestNamespace: "provider-session:laptop-parent",
  thread: {
    threadId: parentThreadId,
    providerSessionId: "provider-session:laptop-parent",
    providerInstanceId: instanceId,
  },
  client: undefined,
  capabilities: new Set(["orchestration"]),
  issuedAt: 1,
};

/** The laptop's parent thread, mid-turn when its agent delegates. */
const seedParent = (laptopEnv: Effect.Success<ReturnType<typeof makeLaptop>>) =>
  Effect.gen(function* () {
    const now = yield* DateTime.now;
    const providerThreadId = ProviderThreadId.make("provider-thread:laptop-parent");
    yield* laptopEnv.orchestrator.dispatch({
      type: "thread.create",
      createdBy: "user",
      creationSource: "web",
      commandId: CommandId.make("command:create:laptop-parent"),
      threadId: parentThreadId,
      projectId: ProjectId.make("project:laptop"),
      title: "Laptop parent",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
    });
    yield* laptopEnv.sink.write({
      commandId: CommandId.make("command:seed:laptop-parent"),
      events: [
        {
          id: EventId.make("event:seed-provider-thread:laptop-parent"),
          type: "provider-thread.updated",
          threadId: parentThreadId,
          driver,
          providerInstanceId: instanceId,
          occurredAt: now,
          payload: {
            id: providerThreadId,
            driver,
            providerInstanceId: instanceId,
            providerSessionId: null,
            appThreadId: parentThreadId,
            ownerNodeId: parentRootNode,
            nativeThreadRef: { driver, nativeId: "native:laptop-parent", strength: "strong" },
            nativeConversationHeadRef: null,
            status: "active",
            firstRunOrdinal: 1,
            lastRunOrdinal: 1,
            handoffIds: [],
            forkedFrom: null,
            createdAt: now,
            updatedAt: now,
          },
        },
        {
          id: EventId.make("event:seed-node:laptop-parent"),
          type: "node.updated",
          threadId: parentThreadId,
          runId: parentRunId,
          nodeId: parentRootNode,
          driver,
          providerInstanceId: instanceId,
          occurredAt: now,
          payload: {
            id: parentRootNode,
            threadId: parentThreadId,
            runId: parentRunId,
            parentNodeId: null,
            rootNodeId: parentRootNode,
            kind: "root_turn",
            status: "running",
            countsForRun: true,
            providerThreadId,
            providerTurnId: null,
            nativeItemRef: null,
            runtimeRequestId: null,
            checkpointScopeId: null,
            startedAt: now,
            completedAt: null,
          },
        },
        {
          id: EventId.make("event:seed-run:laptop-parent"),
          type: "run.updated",
          threadId: parentThreadId,
          runId: parentRunId,
          nodeId: parentRootNode,
          providerInstanceId: instanceId,
          occurredAt: now,
          payload: {
            id: parentRunId,
            threadId: parentThreadId,
            ordinal: 1,
            providerInstanceId: instanceId,
            modelSelection,
            providerThreadId,
            userMessageId: MessageId.make("message:seed-user:laptop-parent"),
            rootNodeId: parentRootNode,
            activeAttemptId: null,
            status: "running",
            requestedAt: now,
            startedAt: now,
            completedAt: null,
            checkpointId: null,
            contextHandoffId: null,
          },
        },
      ],
    });
  });

const delegateToBox = (laptopEnv: Effect.Success<ReturnType<typeof makeLaptop>>) =>
  laptopEnv
    .call("delegate_task", {
      task: "Run the full suite on the box.",
      target: {
        environmentId: box.environmentId,
        providerInstanceId: instanceId,
        model: "gpt-5.4",
      },
      mode: "async",
      clientRequestId: "suite-on-box",
    })
    .pipe(
      Effect.map((result) => {
        expect(result.isError, JSON.stringify(result.content)).toBe(false);
        return result.structuredContent as {
          taskId: NodeId;
          status: string;
          childThreadId: null;
          remoteChild: { environmentId: string; threadId: ThreadId; label: string };
          link?: string;
        };
      }),
    );

/** Waits until the task here has the result its thread there ended with. */
const taskResult = (laptopEnv: Effect.Success<ReturnType<typeof makeLaptop>>, taskId: NodeId) =>
  laptopEnv.orchestrator
    .streamStoredEventsFrom({
      threadId: parentThreadId,
      afterSequence: 0,
      eventType: "subagent.updated",
    })
    .pipe(
      Stream.filter(
        (stored) =>
          stored.event.type === "subagent.updated" &&
          stored.event.payload.id === taskId &&
          stored.event.payload.result !== null,
      ),
      Stream.runHead,
      Effect.map((stored) =>
        Option.isSome(stored) && stored.value.event.type === "subagent.updated"
          ? stored.value.event.payload
          : undefined,
      ),
    );

it.effect("a task delegated to the box wakes the laptop's parent when it ends there", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);

      const delegated = yield* delegateToBox(a);
      expect(delegated).toMatchObject({
        status: "running",
        childThreadId: null,
        remoteChild: { environmentId: box.environmentId, label: "Box" },
      });
      // The box launched it in the project with the same repository.
      expect(yield* Ref.get(thread.launched)).toBe(delegated.remoteChild.threadId);
      // Its link opens the thread there, so the parent can mention it like any other.
      expect(delegated.link).toBe(
        `[Laptop parent](t3-thread://v2/${box.environmentId}/${delegated.remoteChild.threadId})`,
      );
      // ...naming the parent here, so the thread there links back to it.
      expect(yield* Ref.get(thread.delegatedFrom)).toEqual({
        environmentId: laptop.environmentId,
        threadId: parentThreadId,
        title: "Laptop parent",
      });
      // A retry with the same key launches nothing new and records nothing new.
      const retried = yield* delegateToBox(a);
      expect(retried.taskId).toBe(delegated.taskId);
      expect((yield* a.orchestrator.getThreadProjection(parentThreadId)).subagents).toHaveLength(1);

      const historyReads: Array<string> = [];
      const historyTracer = Tracer.make({
        span(options) {
          const span = new Tracer.NativeSpan(options);
          const end = span.end.bind(span);
          span.end = (endTime, exit) => {
            end(endTime, exit);
            const query = span.attributes.get("db.query.text");
            if (
              typeof query === "string" &&
              /\bSELECT\s+payload_json\s+FROM orchestration_v2_projection_(?:runs|context_transfers)\b/i.test(
                query,
              )
            ) {
              historyReads.push(query);
            }
          };
          return span;
        },
      });
      const running = yield* a
        .call("task_status", { taskId: delegated.taskId })
        .pipe(Effect.withTracer(historyTracer));
      expect(running.structuredContent).toMatchObject({
        taskId: delegated.taskId,
        childThreadId: null,
        remoteChild: delegated.remoteChild,
        status: "running",
        workState: "working",
        summary: null,
        hasPendingQuestion: false,
        pendingApproval: null,
      });
      // A running task there needs its local row, not the parent's run or transfer history.
      expect(historyReads).toEqual([]);

      yield* Ref.set(thread.reply, "All 412 tests pass on the box.");
      yield* Ref.set(thread.status, "completed");
      yield* Deferred.succeed(thread.finished, undefined);
      const task = yield* taskResult(a, delegated.taskId);
      expect(task).toMatchObject({ status: "completed", result: "All 412 tests pass on the box." });
      // The parent's live run claims the result, as for a child here.
      const parentRun = (yield* a.orchestrator.getThreadProjection(parentThreadId)).runs.find(
        (run) => run.id === parentRunId,
      );
      expect(parentRun?.delegatedCompletion?.delivery?.taskIds).toEqual([delegated.taskId]);
      // The wake is offered after the result commits, so the result can be seen first.
      const offered = yield* SubscriptionRef.changes(offers).pipe(
        Stream.filter((seen) => seen.some((offer) => offer.threadId === parentThreadId)),
        Stream.runHead,
      );
      expect(Option.isSome(offered)).toBe(true);

      const delivery = parentRun?.delegatedCompletion?.delivery;
      if (delivery == null)
        return yield* Effect.die("The exact completion delivery was not captured");
      const digester = yield* Digest.Digest.pipe(Effect.provide(NodeCrypto.layer));
      expect(delivery.taskIds).toEqual([delegated.taskId]);
      const messageReads: Array<string> = [];
      const queryLedger: Array<{
        query: string;
        digest: string;
        parameters: ReadonlyArray<unknown>;
        ancestry: string[];
      }> = [];
      const tracer = Tracer.make({
        span(options) {
          const span = new Tracer.NativeSpan(options);
          const end = span.end.bind(span);
          span.end = (endTime, exit) => {
            end(endTime, exit);
            const query = span.attributes.get("db.query.text");
            if (
              typeof query === "string" &&
              /SELECT[\s\S]*FROM orchestration_v2_projection_messages/i.test(query)
            ) {
              messageReads.push(query);
            }
          };
          return span;
        },
      });
      const status = yield* a.call("task_status", { taskId: delegated.taskId }).pipe(
        Effect.withTracer(tracer),
        Effect.provideService(SqlStatement.CurrentTransformer, (statement, _sql, _fiber, span) =>
          Effect.gen(function* () {
            const [query, parameters] = statement.compile();
            if (/SELECT[\s\S]*FROM orchestration_v2_projection_messages/i.test(query)) {
              const ancestry: string[] = [];
              let parent = Option.getOrUndefined(span.parent);
              while (parent?._tag === "Span") {
                ancestry.push(parent.name);
                parent = Option.getOrUndefined(parent.parent);
              }
              queryLedger.push({
                query,
                digest: yield* digester.digestHex(Digest.sha256, new TextEncoder().encode(query)),
                parameters,
                ancestry,
              });
            }
            return statement;
          }),
        ),
      );
      expect(status.structuredContent).toMatchObject({
        status: "completed",
        summary: "All 412 tests pass on the box.",
      });
      // Three thin Shell lookups revalidate the caller; one payload lookup validates this delivery.
      // Reviewed raw-SQL hashes are independent of the builder and every observed query.
      const shellDigest = "159b65345b7062b6802b11fa27cb76319b00e19aef2772d833f3cee748be71bc";
      const payloadDigest = "35a52bf9e8af7ef0a4435df85ea59d95d851e6eeabf322c3b107425f2d1d9a7e";
      const shellParameters = [
        "/compact",
        "/compact",
        "/compact",
        "/compact",
        "/compact",
        "/compact",
        parentThreadId,
      ];
      expect(messageReads).toHaveLength(4);
      expect(messageReads.filter((query) => !/\bmessage_id\s+IN\b/i.test(query))).toHaveLength(3);
      expect(queryLedger.map(({ query: _query, ...entry }) => entry)).toEqual([
        {
          digest: shellDigest,
          parameters: shellParameters,
          ancestry: ["sql.transaction", "OrchestratorV2.getThreadShell", "mcp.loadCaller"],
        },
        {
          digest: shellDigest,
          parameters: shellParameters,
          ancestry: [
            "sql.transaction",
            "OrchestratorV2.getThreadShell",
            "mcp.loadCaller",
            "mcp.readCaller",
            "PeerForwarding.forward",
            "RemoteDelegation.observe",
            "OrchestratorMcpService.readRemoteTask",
            "OrchestratorMcpService.readTask",
            "OrchestratorMcpService.taskStatus",
          ],
        },
        {
          digest: shellDigest,
          parameters: shellParameters,
          ancestry: [
            "sql.transaction",
            "OrchestratorV2.getThreadShell",
            "mcp.loadCaller",
            "mcp.readCaller",
            "PeerForwarding.forward",
            "RemoteDelegation.lastReply",
            "RemoteDelegation.lastReply",
            "RemoteDelegation.observe",
            "OrchestratorMcpService.readRemoteTask",
            "OrchestratorMcpService.readTask",
            "OrchestratorMcpService.taskStatus",
          ],
        },
        {
          digest: payloadDigest,
          parameters: [
            parentThreadId,
            JSON.stringify([delivery.messageId]),
            JSON.stringify(["user"]),
          ],
          ancestry: [
            "ProjectionStore.readCanonicalProjection",
            "sql.transaction",
            "Orchestrator.dispatchDelegatedTaskCompletionDeliveryResolution",
            "orchestrationV2.dispatch.once",
            "orchestrationV2.dispatch.withReceipt",
            "OrchestratorMcpService.readRemoteTask",
            "OrchestratorMcpService.readTask",
            "OrchestratorMcpService.taskStatus",
          ],
        },
      ]);
      expect(messageReads).toEqual(queryLedger.map((entry) => entry.query));
      const shellQueries = queryLedger.filter((entry) => entry.digest === shellDigest);
      expect(shellQueries).toHaveLength(3);
      expect(new Set(shellQueries.map((entry) => entry.query)).size).toBe(1);
      const acknowledged = yield* a.projections.getThreadRecords(parentThreadId, ["subagents"], {
        subagentIds: [delegated.taskId],
      });
      expect(acknowledged.subagents[0]?.completionDelivery).toEqual({
        state: "acknowledged",
        observedByRunId: parentRunId,
      });
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect.each(["no active parent run", "a newer run owned by another provider"] as const)(
  "preserves remote observation guards with %s",
  (observer) =>
    Effect.scoped(
      Effect.gen(function* () {
        const thread = yield* makeBoxThread;
        const b = yield* serveBox(thread);
        const offers = yield* SubscriptionRef.make<
          ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
        >([]);
        const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
        const a = yield* makeLaptop(database, offers);
        yield* linkTo(a.links, b, "full-access");
        yield* seedParent(a);
        const delegated = yield* delegateToBox(a);

        yield* Ref.set(thread.reply, "Remote result remains available.");
        yield* Ref.set(thread.status, "completed");
        yield* Deferred.succeed(thread.finished, undefined);
        yield* taskResult(a, delegated.taskId);
        yield* SubscriptionRef.changes(offers).pipe(
          Stream.filter((seen) => seen.some((offer) => offer.threadId === parentThreadId)),
          Stream.runHead,
        );

        const parent = yield* a.projections.getThreadRecords(
          parentThreadId,
          ["runs", "subagents"],
          {
            subagentIds: [delegated.taskId],
          },
        );
        expect(parent.subagents[0]?.completionDelivery?.state).not.toBe("acknowledged");
        const parentRun = parent.runs.find((run) => run.id === parentRunId);
        if (parentRun === undefined) return yield* Effect.die("The parent run was not seeded.");
        const now = yield* DateTime.now;
        const otherInstanceId = ProviderInstanceId.make("codex-other");
        const observingRun: OrchestrationV2Run =
          observer === "no active parent run"
            ? { ...parentRun, status: "completed", completedAt: now }
            : {
                id: RunId.make("run:laptop-other-provider"),
                threadId: parentThreadId,
                ordinal: parentRun.ordinal + 1,
                providerInstanceId: otherInstanceId,
                modelSelection: { instanceId: otherInstanceId, model: "gpt-5.4" },
                providerThreadId: null,
                userMessageId: MessageId.make("message:laptop-other-provider"),
                rootNodeId: null,
                activeAttemptId: null,
                status: "preparing",
                requestedAt: now,
                startedAt: null,
                completedAt: null,
                checkpointId: null,
                contextHandoffId: null,
              };
        yield* a.sink.write({
          commandId: CommandId.make("command:change-remote-result-observer"),
          events: [
            {
              id: EventId.make("event:change-remote-result-observer"),
              type: "run.updated",
              threadId: parentThreadId,
              runId: observingRun.id,
              providerInstanceId: observingRun.providerInstanceId,
              occurredAt: now,
              payload: observingRun,
            },
          ],
        });

        const status = yield* a.call("task_status", { taskId: delegated.taskId });
        if (observer === "no active parent run") {
          expect(status.isError).toBe(true);
          expect(status.content).toEqual([
            {
              type: "text",
              text: JSON.stringify({
                _tag: "OrchestratorMcpFailure",
                code: "parent_not_active",
                message: "The calling provider no longer owns an active thread run.",
              }),
            },
          ]);
          const unacknowledged = yield* a.projections.getThreadRecords(
            parentThreadId,
            ["subagents"],
            { subagentIds: [delegated.taskId] },
          );
          expect(unacknowledged.subagents[0]?.result).toBe("Remote result remains available.");
          expect(unacknowledged.subagents[0]?.completionDelivery?.state).not.toBe("acknowledged");
          return;
        }
        expect(status.isError, JSON.stringify(status.content)).not.toBe(true);
        expect(status.structuredContent).toMatchObject({
          status: "completed",
          summary: "Remote result remains available.",
        });
        const acknowledged = yield* a.projections.getThreadRecords(parentThreadId, ["subagents"], {
          subagentIds: [delegated.taskId],
        });
        expect(acknowledged.subagents[0]?.completionDelivery).toEqual({
          state: "acknowledged",
          observedByRunId: parentRunId,
        });
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect(
  "with the beta off here, a task already running there still ends, and no new one starts",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const thread = yield* makeBoxThread;
        const b = yield* serveBox(thread);
        const offers = yield* SubscriptionRef.make<
          ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
        >([]);
        const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
        const a = yield* makeLaptop(database, offers);
        yield* linkTo(a.links, b, "full-access");
        yield* seedParent(a);
        const delegated = yield* delegateToBox(a);

        yield* a.settings.updateSettings({ linkedEnvironmentsEnabled: false });
        const refused = yield* a.call("delegate_task", {
          task: "Run the linter on the box.",
          target: {
            environmentId: box.environmentId,
            providerInstanceId: instanceId,
            model: "gpt-5.4",
          },
          mode: "async",
          clientRequestId: "lint-on-box",
        });
        expect(refused.isError).toBe(true);
        expect(JSON.stringify(refused.content)).toContain(
          PeerForwarding.linkedEnvironmentsOff.message,
        );
        expect((yield* a.orchestrator.getThreadProjection(parentThreadId)).subagents).toHaveLength(
          1,
        );

        yield* Ref.set(thread.reply, "All 412 tests pass on the box.");
        yield* Ref.set(thread.status, "completed");
        yield* Deferred.succeed(thread.finished, undefined);
        expect(yield* taskResult(a, delegated.taskId)).toMatchObject({
          status: "completed",
          result: "All 412 tests pass on the box.",
        });
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);
const decodeOrchestratorMcpDelegateTaskResult = Schema.decodeUnknownEffect(
  OrchestratorMcpDelegateTaskResult,
);

it.effect("a task named only by environment inherits the parent's model, within the link", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      // The parent runs with full access; the link allows less.
      yield* linkTo(a.links, b, "auto-accept-edits");
      yield* seedParent(a);

      const result = yield* a.call("delegate_task", {
        task: "Run the suite on the box.",
        target: { environmentId: box.environmentId },
      });
      expect(result.isError, JSON.stringify(result.content)).toBe(false);
      // The parent's model, and the link's modes rather than the parent's.
      expect(yield* Ref.get(thread.launchedWith)).toEqual({
        modelSelection,
        runtimeMode: "auto-accept-edits",
        interactionMode: "default",
        workspaceStrategy: { type: "root" },
      });

      // Asking for more than the link allows is still refused there.
      const broaderInput = {
        task: "Run the suite on the box.",
        target: { environmentId: box.environmentId },
        runtimeMode: "full-access",
        clientRequestId: "broader",
      };
      const launchesBeforeRefusal = yield* Ref.get(thread.launchCalls);
      expect(launchesBeforeRefusal).toHaveLength(1);
      const broader = yield* a.call("delegate_task", broaderInput);
      expect(broader.isError, JSON.stringify(broader.content)).toBe(false);
      const refused = yield* decodeOrchestratorMcpDelegateTaskResult(broader.structuredContent);
      expect(refused).toMatchObject({
        status: "failed",
        childThreadId: null,
        summary: "Child runtime mode full-access is broader than parent mode auto-accept-edits.",
      });
      const deniedIntent = yield* a.intents.byTask(refused.taskId);
      expect(deniedIntent).toMatchObject({
        taskId: refused.taskId,
        status: "denied",
        launched: null,
        envelope: { environmentId: box.environmentId, request: { parentThreadId } },
      });
      const failedTask = (yield* a.orchestrator.getThreadProjection(parentThreadId)).subagents.find(
        (task) => task.id === refused.taskId,
      );
      expect(failedTask).toMatchObject({
        id: refused.taskId,
        threadId: parentThreadId,
        status: "failed",
        childThreadId: null,
        result: "Child runtime mode full-access is broader than parent mode auto-accept-edits.",
      });
      expect(failedTask?.remoteChild).toBeUndefined();
      if (deniedIntent === undefined) return yield* Effect.die("Missing definitive refusal intent");
      const deniedCommand = CommandId.make(`${deniedIntent.commandId}:denied`);
      const terminalEvents = Array.from(
        yield* a.sink.readByCommandId({ commandId: deniedCommand }).pipe(Stream.runCollect),
      );
      expect(
        terminalEvents.filter(
          (stored) =>
            stored.event.type === "subagent.updated" &&
            stored.event.payload.id === refused.taskId &&
            stored.event.payload.status === "failed",
        ),
      ).toHaveLength(1);
      const replayed = yield* a.call("delegate_task", broaderInput);
      expect(replayed.isError, JSON.stringify(replayed.content)).toBe(false);
      expect(
        yield* decodeOrchestratorMcpDelegateTaskResult(replayed.structuredContent),
      ).toMatchObject({ taskId: refused.taskId, status: "failed", summary: refused.summary });
      expect(
        (yield* a.orchestrator.getThreadProjection(parentThreadId)).subagents.find(
          (task) => task.id === refused.taskId,
        ),
      ).toEqual(failedTask);
      expect(
        Array.from(
          yield* a.sink.readByCommandId({ commandId: deniedCommand }).pipe(Stream.runCollect),
        ),
      ).toEqual(terminalEvents);
      expect(yield* Ref.get(thread.launchCalls)).toEqual(launchesBeforeRefusal);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("refuses mutually exclusive remote worktree options before reserving or launching", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const forwarded: string[] = [];
      const a = yield* makeLaptop(
        database,
        offers,
        undefined,
        undefined,
        undefined,
        undefined,
        (tool) => forwarded.push(tool),
      );
      yield* linkTo(a.links, b, "auto");
      yield* seedParent(a);

      const result = yield* a.call("delegate_task", {
        task: "Run the suite on the box.",
        target: { environmentId: box.environmentId },
        isolation: "worktree",
        workspaceStrategy: { type: "worktree" },
        clientRequestId: "exclusive-worktree-input",
      });
      expect(result.isError).toBe(true);
      const failureText = result.content.find((block) => block.type === "text")?.text;
      expect(JSON.parse(failureText ?? "null")).toMatchObject({
        _tag: "OrchestratorMcpFailure",
        code: "invalid_request",
        message: "Pass isolation or workspaceStrategy, not both.",
      });
      expect((yield* a.orchestrator.getThreadProjection(parentThreadId)).subagents).toEqual([]);
      expect(yield* a.intents.pending).toEqual([]);
      expect(
        yield* a.intents.get(
          CommandId.make(
            "command:mcp:provider-session%3Alaptop-parent:remote-delegate:exclusive-worktree-input",
          ),
        ),
      ).toBeUndefined();
      expect(forwarded).toEqual([]);
      expect(yield* Ref.get(thread.launchCalls)).toEqual([]);
      expect(yield* Ref.get(thread.launched)).toBeNull();
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("follows an open task again after the laptop restarts", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      // The laptop before the restart delegates, then goes away.
      const delegated = yield* Effect.scoped(
        Effect.gen(function* () {
          const before = yield* makeLaptop(database, offers);
          yield* linkTo(before.links, b, "full-access");
          yield* seedParent(before);
          return yield* delegateToBox(before);
        }),
      );
      // The task finishes on the box while the laptop is down.
      yield* Ref.set(thread.reply, "Finished while you were away.");
      yield* Ref.set(thread.status, "completed");
      yield* Deferred.succeed(thread.finished, undefined);

      const after = yield* makeLaptop(database, offers);
      // The link lives in the laptop's own store, so it needs linking again here.
      yield* linkTo(after.links, b, "full-access");
      yield* after.remote.start();
      const task = yield* taskResult(after, delegated.taskId);
      expect(task).toMatchObject({ status: "completed", result: "Finished while you were away." });
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("fails the task when the box revokes the link, and cancel interrupts it there", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);

      const cancelled = yield* delegateToBox(a);
      const cancel = yield* a.call("task_cancel", {
        taskId: cancelled.taskId,
        reason: "Not needed",
      });
      expect(cancel.structuredContent).toEqual({
        taskId: cancelled.taskId,
        status: "cancel_requested",
      });
      // Cancelling stops the thread there as Stop does: its queue and its own tasks too.
      expect(yield* Ref.get(thread.stops)).toEqual(["thread.stop", "delegated-tasks"]);
      expect(yield* taskResult(a, cancelled.taskId)).toMatchObject({ status: "cancelled" });

      // Cancelling again, once the result is in, still stops what came after it there.
      const again = yield* a.call("task_cancel", {
        taskId: cancelled.taskId,
        clientRequestId: "cancel-again",
      });
      expect(again.structuredContent).toEqual({ taskId: cancelled.taskId, status: "cancelled" });
      expect(yield* Ref.get(thread.interrupted)).toBe(2);
      // ...and a retry of that cancel stops nothing new.
      yield* a.call("task_cancel", { taskId: cancelled.taskId, clientRequestId: "cancel-again" });
      expect(yield* Ref.get(thread.interrupted)).toBe(2);

      // A second task, then the box revokes the link before it ends.
      const revoked = yield* a
        .call("delegate_task", {
          task: "Another one.",
          target: {
            environmentId: box.environmentId,
            providerInstanceId: instanceId,
            model: "gpt-5.4",
          },
          clientRequestId: "second",
        })
        .pipe(Effect.map((result) => result.structuredContent as { taskId: NodeId }));
      const [session] = yield* b.linkedSessions;
      yield* b.auth.revokeSession(session!.sessionId);
      yield* Deferred.succeed(thread.finished, undefined);
      const failed = yield* taskResult(a, revoked.taskId);
      expect(failed?.status).toBe("failed");
      expect(failed?.result).toBe(
        "The linked environment no longer accepts this link. It may have been revoked there; link it again.",
      );
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("a task's result is its own run's whole last reply, not a later run's", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);

      const result = yield* a.call("delegate_task", {
        task: "Review the diff.",
        role: "review",
        target: { environmentId: box.environmentId },
        clientRequestId: "review-on-box",
      });
      expect(result.isError, JSON.stringify(result.content)).toBe(false);
      const { taskId } = result.structuredContent as { taskId: NodeId };
      // The role reaches the thread there, as it does a child here.
      expect(yield* Ref.get(thread.message)).toBe(
        "Act as the review sub-agent for this task.\n\nReview the diff.",
      );

      // A reply longer than one read, then a later run's reply on the same thread.
      const long = "x".repeat(45_000);
      yield* Ref.set(thread.reply, long);
      yield* Ref.set(thread.laterReply, "Someone else asked a follow-up.");
      yield* Ref.set(thread.executedModel, "gpt-5.4-mini");
      yield* Ref.set(thread.status, "completed");
      yield* Deferred.succeed(thread.finished, undefined);
      const task = yield* taskResult(a, taskId);
      expect(task?.status).toBe("completed");
      expect(task?.result).toBe(
        `Requested model gpt-5.4; the provider reported running gpt-5.4-mini.\n\n${long}`,
      );
      // Every wait was for the run the launch started.
      const waits = yield* Ref.get(thread.waitedFor);
      expect(waits.length).toBeGreaterThan(0);
      expect(waits.every((runId) => runId === boxRunId)).toBe(true);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("a task whose thread there waits on its own task ends with the answer after it", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);
      const delegated = yield* delegateToBox(a);

      // The task's run there ends after delegating a review of its own.
      yield* Ref.set(thread.nested, true);
      yield* Ref.set(thread.reply, "Started a review; I will answer when it reports.");
      yield* Ref.set(thread.status, "completed");
      yield* Deferred.succeed(thread.finished, undefined);
      const first = yield* Effect.raceFirst(
        Deferred.await(thread.nestedWaiting).pipe(Effect.as("waiting there" as const)),
        taskResult(a, delegated.taskId).pipe(Effect.as("completed here" as const)),
      );
      expect(first).toBe("waiting there");

      // The review reports and wakes the thread there, whose next run answers.
      yield* Ref.set(thread.laterReply, "The review found two issues; both are fixed.");
      yield* Deferred.succeed(thread.nestedSettled, undefined);
      expect(yield* taskResult(a, delegated.taskId)).toMatchObject({
        status: "completed",
        result: "The review found two issues; both are fixed.",
      });
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("the same key from another provider session is another task", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);

      const first = yield* delegateToBox(a);
      const later = yield* a.call(
        "delegate_task",
        {
          task: "Run the full suite on the box.",
          target: { environmentId: box.environmentId },
          clientRequestId: "suite-on-box",
        },
        { ...parentScope, requestNamespace: "provider-session:laptop-parent-2" },
      );
      expect(later.isError, JSON.stringify(later.content)).toBe(false);
      const second = later.structuredContent as {
        taskId: NodeId;
        remoteChild: { threadId: ThreadId };
      };
      expect(second.taskId).not.toBe(first.taskId);
      expect(second.remoteChild.threadId).not.toBe(first.remoteChild.threadId);
      expect((yield* a.orchestrator.getThreadProjection(parentThreadId)).subagents).toHaveLength(2);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect.each([null, "Working before the provider failed."])(
  "reports the failed remote run's error instead of its earlier reply (%s)",
  (reply) =>
    Effect.scoped(
      Effect.gen(function* () {
        const thread = yield* makeBoxThread;
        const b = yield* serveBox(thread);
        const offers = yield* SubscriptionRef.make<
          ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
        >([]);
        const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
        const a = yield* makeLaptop(database, offers);
        yield* linkTo(a.links, b, "full-access");
        yield* seedParent(a);
        const delegated = yield* delegateToBox(a);
        yield* Ref.set(thread.reply, reply);
        const error = "Provider failed: authentication expired on the destination.";
        yield* Ref.set(thread.error, error);
        yield* Ref.set(thread.status, "failed");
        yield* Deferred.succeed(thread.finished, undefined);
        expect(yield* taskResult(a, delegated.taskId)).toMatchObject({
          status: "failed",
          result: error,
        });
        const status = yield* a.call("task_status", { taskId: delegated.taskId });
        expect(status.structuredContent).toMatchObject({
          status: "failed",
          summary: error,
          latestTerminalSummary: error,
        });
        const offered = yield* SubscriptionRef.changes(offers).pipe(
          Stream.filter((seen) => seen.some((offer) => offer.threadId === parentThreadId)),
          Stream.runHead,
        );
        expect(Option.isSome(offered)).toBe(true);
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect.each([false, true])(
  "ignores later-run errors and reads the failed run's full error (ownError=%s)",
  (ownError) =>
    Effect.scoped(
      Effect.gen(function* () {
        const thread = yield* makeBoxThread;
        const b = yield* serveBox(thread);
        const offers = yield* SubscriptionRef.make<
          ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
        >([]);
        const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
        const a = yield* makeLaptop(database, offers);
        yield* linkTo(a.links, b, "full-access");
        yield* seedParent(a);
        const delegated = yield* delegateToBox(a);
        yield* Ref.set(thread.reply, null);
        const error = `Provider failed: ${"detail ".repeat(10_000)}end of error.`;
        if (ownError) yield* Ref.set(thread.error, error);
        yield* Ref.set(thread.laterReply, "A later user run answered.");
        yield* Ref.set(thread.laterError, "Another run's provider error.");
        yield* Ref.set(thread.status, "failed");
        yield* Deferred.succeed(thread.finished, undefined);
        expect(yield* taskResult(a, delegated.taskId)).toMatchObject({
          status: "failed",
          result: ownError
            ? error
            : "(The thread in the linked environment ended without a reply.)",
        });
        expect(yield* Ref.get(thread.waitedFor)).toEqual([boxRunId]);
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("keeps a successful remote reply despite an error item in the run's activity", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);
      const delegated = yield* delegateToBox(a);
      yield* Ref.set(thread.reply, "Recovered and completed the task.");
      yield* Ref.set(thread.error, "A recoverable tool error.");
      yield* Ref.set(thread.status, "completed");
      yield* Deferred.succeed(thread.finished, undefined);
      expect(yield* taskResult(a, delegated.taskId)).toMatchObject({
        status: "completed",
        result: "Recovered and completed the task.",
      });
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("fails the task when its thread there is gone, or the link here is", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);

      yield* Ref.set(thread.gone, true);
      const gone = yield* delegateToBox(a);
      const failed = yield* taskResult(a, gone.taskId);
      expect(failed?.status).toBe("failed");
      expect(failed?.result).toContain("was not found");

      // The link is forgotten here while the task runs there.
      yield* Ref.set(thread.gone, false);
      const forgotten = yield* a
        .call("delegate_task", {
          task: "Another one.",
          target: { environmentId: box.environmentId },
          clientRequestId: "forgotten",
        })
        .pipe(Effect.map((result) => result.structuredContent as { taskId: NodeId }));
      yield* a.links.unlink(box.environmentId);
      yield* Ref.set(thread.status, "completed");
      yield* Deferred.succeed(thread.finished, undefined);
      const unlinked = yield* taskResult(a, forgotten.taskId);
      expect(unlinked?.status).toBe("failed");
      expect(unlinked?.result).toContain("not linked to");
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("cancel ends the task here even when the link is gone", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);

      const delegated = yield* delegateToBox(a);
      // Let the follower enter its original pending wait before unlink/cancel race.
      yield* Deferred.await(thread.waitStarted);
      yield* a.links.unlink(box.environmentId);
      const cancel = yield* a.call("task_cancel", { taskId: delegated.taskId });
      expect(cancel.structuredContent).toEqual({
        taskId: delegated.taskId,
        status: "cancel_requested",
      });
      const task = yield* taskResult(a, delegated.taskId);
      expect(task?.status).toBe("cancelled");
      expect(task?.result).toContain("could not be stopped in Box");
      expect(yield* Ref.get(thread.interrupted)).toBe(0);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect.each([false, true])(
  "a parent stopped during final-text read keeps its result and retries a stop defect (%s)",
  (defect) =>
    Effect.scoped(
      Effect.gen(function* () {
        const thread = yield* makeBoxThread;
        const b = yield* serveBox(thread);
        const offers = yield* SubscriptionRef.make<
          ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
        >([]);
        const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
        let stopAttempts = 0;
        const a = yield* makeLaptop(
          database,
          offers,
          undefined,
          undefined,
          undefined,
          undefined,
          undefined,
          undefined,
          {
            interrupt: (stop) =>
              stop.pipe(
                Effect.flatMap((result) => {
                  stopAttempts++;
                  return defect && stopAttempts === 1
                    ? Effect.die("synthetic defect after remote stop")
                    : Effect.succeed(result);
                }),
              ),
          },
        );
        yield* linkTo(a.links, b, "full-access");
        yield* seedParent(a);
        const delegated = yield* delegateToBox(a);

        // The task's run ends there, and while the follower reads its reply,
        // the parent here is stopped: more work may follow that run there.
        yield* Ref.set(thread.holdRead, true);
        yield* Ref.set(thread.reply, "Done there.");
        yield* Ref.set(thread.status, "completed");
        yield* Deferred.succeed(thread.finished, undefined);
        yield* Deferred.await(thread.readStarted);
        yield* a.threads.stopDelegatedTasks({
          threadId: parentThreadId,
          commandId: CommandId.make("command:stop:laptop-parent-mid-read"),
        });
        yield* Deferred.succeed(thread.releaseRead, undefined);

        // The Stop's result stands, and the thread there is still stopped.
        expect(yield* taskResult(a, delegated.taskId)).toMatchObject({ status: "interrupted" });
        expect(yield* stoppedThere(a, delegated.taskId, null)).toMatchObject({
          status: "interrupted",
        });
        expect(yield* Ref.get(thread.stops)).toEqual(["thread.stop", "delegated-tasks"]);
        expect(stopAttempts).toBe(defect ? 2 : 1);
      }),
    ).pipe(
      Effect.timeout(Duration.seconds(8)),
      Effect.provide(NodeHttpServer.layerTest),
      TestClock.withLive,
    ),
);

it.effect("a Stop refused by lowered parent modes retains the original remote debt", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const stopRefused = yield* Deferred.make<OrchestratorMcpFailure>();
      const a = yield* makeLaptop(database, offers, undefined, (error) =>
        Deferred.succeed(stopRefused, error).pipe(Effect.asVoid),
      );
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);
      const delegated = yield* delegateToBox(a);

      // The parent is lowered below the full-access thread there, then stopped.
      yield* a.orchestrator.dispatch({
        type: "thread.runtime-mode.set",
        commandId: CommandId.make("command:lower:laptop-parent"),
        threadId: parentThreadId,
        runtimeMode: "approval-required",
      });
      yield* a.threads.stopDelegatedTasks({
        threadId: parentThreadId,
        commandId: CommandId.make("command:stop:laptop-parent-lowered"),
      });

      const refusal = yield* Deferred.await(stopRefused);
      expect(refusal.code).toBe("runtime_mode_escalation_denied");
      expect(refusal.message).toContain(
        "full-access is broader than parent mode approval-required",
      );
      const pending = (yield* a.orchestrator.getThreadProjection(parentThreadId)).subagents.find(
        (task) => task.id === delegated.taskId,
      );
      expect(pending).toMatchObject({
        id: delegated.taskId,
        threadId: parentThreadId,
        runId: parentRunId,
        status: "interrupted",
        result: "Stopped with its parent.",
        remoteChild: { ...delegated.remoteChild, runId: boxRunId },
      });
      expect(pending?.remoteChild?.stoppedThere).not.toBe(true);
      expect(yield* a.projections.getOpenRemoteDelegatedTasks).toEqual([
        { parentThreadId, taskId: delegated.taskId },
      ]);
      expect(yield* Ref.get(thread.stops)).toEqual([]);
      expect(yield* Ref.get(thread.interrupted)).toBe(0);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("stopping the parent stops its task there", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);

      const delegated = yield* delegateToBox(a);
      // What the delegated-tasks.stop effect runs once the parent's Stop commits.
      yield* a.threads.stopDelegatedTasks({
        threadId: parentThreadId,
        commandId: CommandId.make("command:stop:laptop-parent"),
        reason: "User stopped",
      });
      expect(yield* taskResult(a, delegated.taskId)).toMatchObject({
        status: "interrupted",
        result: "Stopped with its parent: User stopped",
      });
      // The follower sees it ended here and stops it there.
      yield* Deferred.await(thread.interruptedOnce);
      expect(yield* Ref.get(thread.interrupted)).toBe(1);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

/** A follower retry schedule the test steps: each failed attempt is offered, then waits for `retry`. */
const steppedRetries = Effect.gen(function* () {
  const failed = yield* Queue.unbounded<void>();
  const retry = yield* Queue.unbounded<void>();
  const schedule = Schedule.forever.pipe(
    Schedule.tap(() => Queue.offer(failed, undefined).pipe(Effect.andThen(Queue.take(retry)))),
  );
  return { failed, retry, schedule };
});

/** Waits until the task here records that the `stopCount`th Stop after its result reached there. */
const stoppedThere = (
  laptopEnv: Effect.Success<ReturnType<typeof makeLaptop>>,
  taskId: NodeId,
  // `null` for a cancel's stop, which carries no count.
  stopCount: number | null = 1,
) =>
  laptopEnv.orchestrator
    .streamStoredEventsFrom({
      threadId: parentThreadId,
      afterSequence: 0,
      eventType: "subagent.updated",
    })
    .pipe(
      Stream.filter(
        (stored) =>
          stored.event.type === "subagent.updated" &&
          stored.event.payload.id === taskId &&
          stored.event.payload.remoteChild?.stoppedThere === true &&
          stored.event.payload.remoteChild.stopCount === (stopCount ?? undefined),
      ),
      Stream.runHead,
      Effect.map((stored) =>
        Option.isSome(stored) && stored.value.event.type === "subagent.updated"
          ? stored.value.event.payload
          : undefined,
      ),
    );

it.effect("stopping the parent stops follow-up work on a task there that has its result", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* a.remote.start();
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);
      const delegated = yield* delegateToBox(a);
      yield* Ref.set(thread.reply, "All green.");
      yield* Ref.set(thread.status, "completed");
      yield* Deferred.succeed(thread.finished, undefined);
      expect(yield* taskResult(a, delegated.taskId)).toMatchObject({
        status: "completed",
        result: "All green.",
      });

      // The parent sends the thread there a follow-up, which starts a run there.
      const sent = yield* a.call("t3_thread_send", {
        environmentId: box.environmentId,
        threadId: delegated.remoteChild.threadId,
        message: "Now run the slow suite too.",
      });
      expect(sent.isError, JSON.stringify(sent.content)).toBe(false);
      expect(yield* Ref.get(thread.sent)).toEqual([
        "**T3M** · From Laptop parent (remote environment `environment-laptop`, thread `thread:laptop-parent`; via linked client T3 Code · Laptop) · Sent: 12/31 19:00:00\n\nNow run the slow suite too.",
      ]);
      expect(yield* Ref.get(thread.interrupted)).toBe(0);

      yield* a.threads.stopDelegatedTasks({
        threadId: parentThreadId,
        commandId: CommandId.make("command:stop:laptop-parent-after-result"),
      });
      // The thread there is stopped as Stop does, and the task keeps its result.
      const task = yield* stoppedThere(a, delegated.taskId);
      expect(yield* Ref.get(thread.stops)).toEqual(["thread.stop", "delegated-tasks"]);
      expect(task).toMatchObject({ status: "completed", result: "All green." });
      expect(yield* a.projections.getOpenRemoteDelegatedTasks).toEqual([]);

      // A second Stop owes a second stop there, not a replay of the first.
      yield* a.threads.stopDelegatedTasks({
        threadId: parentThreadId,
        commandId: CommandId.make("command:stop:laptop-parent-again"),
      });
      expect(yield* stoppedThere(a, delegated.taskId, 2)).toMatchObject({ status: "completed" });
      expect(yield* Ref.get(thread.interrupted)).toBe(2);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("a delayed Stop acknowledgement leaves a newer Stop owed", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* a.remote.start();
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);
      const delegated = yield* delegateToBox(a);
      yield* Ref.set(thread.reply, "All green.");
      yield* Ref.set(thread.status, "completed");
      yield* Deferred.succeed(thread.finished, undefined);
      yield* taskResult(a, delegated.taskId);

      yield* Ref.set(thread.holdFirstStop, true);
      yield* a.threads.stopDelegatedTasks({
        threadId: parentThreadId,
        commandId: CommandId.make("command:stop:delayed-first"),
      });
      yield* Deferred.await(thread.stopStarted);
      // Work starts after the first Stop landed there, but before its acknowledgement returns.
      yield* a.call("t3_thread_send", {
        environmentId: box.environmentId,
        threadId: delegated.remoteChild.threadId,
        message: "Run another check.",
      });
      yield* a.threads.stopDelegatedTasks({
        threadId: parentThreadId,
        commandId: CommandId.make("command:stop:delayed-second"),
      });
      const pending = yield* a.threads.getThreadRecords(parentThreadId, ["subagents"]);
      expect(
        pending.subagents.find((task) => task.id === delegated.taskId)?.remoteChild,
      ).toMatchObject({ stoppedThere: false, stopCount: 2 });
      yield* Deferred.succeed(thread.releaseStop, undefined);
      expect(yield* stoppedThere(a, delegated.taskId, 2)).toMatchObject({
        status: "completed",
        result: "All green.",
      });
      expect(yield* Ref.get(thread.interrupted)).toBe(2);
      expect(yield* a.projections.getOpenRemoteDelegatedTasks).toEqual([]);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("a Stop on a task that has its result reaches there after a restart", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const linking = yield* layerLinkingEnvironment(laptop).pipe(Layer.build);
      // The laptop records the Stop, then goes away before its follower sends it.
      const delegated = yield* Effect.scoped(
        Effect.gen(function* () {
          const before = yield* makeLaptop(database, offers, linking);
          yield* linkTo(before.links, b, "full-access");
          yield* seedParent(before);
          const task = yield* delegateToBox(before);
          yield* Ref.set(thread.status, "completed");
          yield* Deferred.succeed(thread.finished, undefined);
          yield* taskResult(before, task.taskId);
          yield* before.threads.stopDelegatedTasks({
            threadId: parentThreadId,
            commandId: CommandId.make("command:stop:laptop-parent-before-restart"),
          });
          return task;
        }),
      );
      expect(yield* Ref.get(thread.interrupted)).toBe(0);

      const after = yield* makeLaptop(database, offers, linking);
      expect(yield* after.projections.getOpenRemoteDelegatedTasks).toEqual([
        { parentThreadId, taskId: delegated.taskId },
      ]);
      yield* after.remote.start();
      expect(yield* stoppedThere(after, delegated.taskId)).toMatchObject({ status: "completed" });
      expect(yield* Ref.get(thread.stops)).toEqual(["thread.stop", "delegated-tasks"]);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("a parent stopped while the box is unreachable still stops its task there", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const retries = yield* steppedRetries;
      const a = yield* makeLaptop(database, offers).pipe(
        Effect.provideService(RemoteDelegation.FollowRetrySchedule, retries.schedule),
      );
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);
      // The box sleeps as soon as the task is there, so its follower's first attempt fails.
      yield* Ref.set(
        thread.afterLaunch,
        Ref.set(b.descriptor, descriptorOf("environment-elsewhere", "Elsewhere")),
      );
      const delegated = yield* delegateToBox(a);
      yield* Queue.take(retries.failed);
      // The parent is stopped meanwhile.
      yield* a.threads.stopDelegatedTasks({
        threadId: parentThreadId,
        commandId: CommandId.make("command:stop:laptop-parent-offline"),
      });
      expect(yield* taskResult(a, delegated.taskId)).toMatchObject({ status: "interrupted" });

      // Once the box is back, the follower's next attempt stops the task there.
      yield* Ref.set(b.descriptor, box);
      yield* Queue.offer(retries.retry, undefined);
      yield* Deferred.await(thread.interruptedOnce);
      expect(yield* Ref.get(thread.interrupted)).toBe(1);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("a task cancelled while the box is unreachable is stopped there after a restart", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      // Its links outlive the restart, so the box knows it as the same link session.
      const linking = yield* layerLinkingEnvironment(laptop).pipe(Layer.build);
      // The laptop cancels while the box sleeps, then restarts before it wakes.
      const delegated = yield* Effect.scoped(
        Effect.gen(function* () {
          const before = yield* makeLaptop(database, offers, linking);
          yield* linkTo(before.links, b, "full-access");
          yield* seedParent(before);
          const task = yield* delegateToBox(before);
          yield* Ref.set(b.descriptor, descriptorOf("environment-elsewhere", "Elsewhere"));
          yield* before.call("task_cancel", { taskId: task.taskId });
          expect(yield* taskResult(before, task.taskId)).toMatchObject({ status: "cancelled" });
          return task;
        }),
      );
      expect(yield* Ref.get(thread.interrupted)).toBe(0);

      yield* Ref.set(b.descriptor, box);
      const after = yield* makeLaptop(database, offers, linking);
      yield* after.remote.start();
      yield* Deferred.await(thread.interruptedOnce);
      // Once the stop is recorded, nothing is owed there any more.
      yield* stoppedThere(after, delegated.taskId, null);
      expect(yield* after.projections.getOpenRemoteDelegatedTasks).toEqual([]);
      expect(yield* Ref.get(thread.stops)).toEqual(["thread.stop", "delegated-tasks"]);
      // The result here is still the cancel's.
      expect(yield* taskResult(after, delegated.taskId)).toMatchObject({ status: "cancelled" });
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect(
  "replaying a runless remote launch parks its observed child without resending or using a latest reply",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const thread = yield* makeBoxThread;
        const b = yield* serveBox(thread);
        const offers = yield* SubscriptionRef.make<
          ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
        >([]);
        const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
        const a = yield* makeLaptop(database, offers);
        yield* linkTo(a.links, b, "full-access");
        yield* seedParent(a);
        // As a task recorded before its run was kept, whose thread there is idle.
        yield* Ref.set(thread.runless, true);
        yield* Ref.set(thread.reply, "Done before it was followed.");

        const delegated = yield* delegateToBox(a);
        const originalTask = (yield* a.orchestrator.getThreadProjection(
          parentThreadId,
        )).subagents.find((task) => task.id === delegated.taskId);
        expect(originalTask).toMatchObject({
          id: delegated.taskId,
          threadId: parentThreadId,
          status: "running",
          result: null,
        });
        expect(originalTask?.remoteChild).toBeUndefined();
        expect(originalTask?.completionDelivery).toBeUndefined();
        const originalIntent = yield* a.intents.byTask(delegated.taskId);
        if (originalIntent === undefined || originalIntent.launched === null)
          return yield* Effect.die("The original runless launch response was not retained");
        expect(originalIntent).toMatchObject({
          commandId: "command:mcp:provider-session%3Alaptop-parent:remote-delegate:suite-on-box",
          taskId: delegated.taskId,
          status: "pending",
          launchStarted: true,
          envelope: {
            environmentId: box.environmentId,
            request: { parentThreadId },
            launch: { clientRequestId: "delegate:thread:laptop-parent:suite-on-box" },
          },
          launched: { threadId: yield* Ref.get(thread.launched), runId: null },
        });
        const originalCalls = yield* Ref.get(thread.launchCalls);
        expect(originalCalls).toHaveLength(1);
        const replayed = yield* delegateToBox(a);
        expect(replayed.taskId).toBe(delegated.taskId);
        const noticeCommand = CommandId.make(`${originalIntent.commandId}:reconciliation-notice`);
        const notice = yield* a.orchestrator
          .streamStoredEventsFrom({ threadId: parentThreadId, afterSequence: 0 })
          .pipe(
            Stream.filter(
              (stored) =>
                stored.commandId === noticeCommand && stored.event.type === "message.updated",
            ),
            Stream.runHead,
          );
        expect(Option.isSome(notice)).toBe(true);
        const parked = (yield* a.orchestrator.getThreadProjection(parentThreadId)).subagents;
        expect(parked).toHaveLength(1);
        expect(parked[0]).toMatchObject({
          id: delegated.taskId,
          threadId: parentThreadId,
          status: "waiting",
          result: null,
        });
        expect(parked[0]?.remoteChild).toBeUndefined();
        expect(parked[0]?.completionDelivery).toBeUndefined();
        expect(yield* a.intents.byTask(delegated.taskId)).toEqual(originalIntent);
        expect(yield* Ref.get(thread.launchCalls)).toEqual(originalCalls);
        expect(yield* Ref.get(thread.launched)).toBe(originalIntent.launched.threadId);
        expect(yield* Ref.get(thread.waitedFor)).toEqual([]);
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("a local task runs in this thread's project", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* seedParent(a);

      const result = yield* a.call("delegate_task", {
        task: "Run it here.",
        target: { projectId: "project:elsewhere" },
      });
      expect(result.isError).toBe(true);
      expect(JSON.stringify(result.content)).toContain("invalid_request");
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect(
  "refuses unsolicited remote supervision before side effects and keeps the key reusable",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const thread = yield* makeBoxThread;
        const b = yield* serveBox(thread);
        const offers = yield* SubscriptionRef.make<
          ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
        >([]);
        const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
        const forwarded: Array<string> = [];
        const a = yield* makeLaptop(
          database,
          offers,
          undefined,
          undefined,
          undefined,
          undefined,
          (toolName) => forwarded.push(toolName),
        );
        yield* linkTo(a.links, b, "full-access");
        yield* seedParent(a);
        const request = {
          task: "Inspect the diff on the box.",
          target: { environmentId: box.environmentId },
          runtimeMode: "approval-required",
          clientRequestId: "supervised-remote-key",
        };
        const refused = yield* a.call("delegate_task", request);
        expect(refused.isError).toBe(true);
        expect(JSON.stringify(refused.content)).toContain("invalid_request");
        expect(forwarded).toEqual([]);
        expect(yield* Ref.get(thread.launched)).toBeNull();
        expect((yield* a.orchestrator.getThreadProjection(parentThreadId)).subagents).toHaveLength(
          0,
        );
        const store = a.intents;
        expect(yield* store.pending).toEqual([]);
        const accepted = yield* a.call("delegate_task", {
          ...request,
          supervisionRequestedByUser: true,
        });
        expect(accepted.isError, JSON.stringify(accepted.content)).toBe(false);
        expect(forwarded.filter((name) => name === "t3_thread_launch")).toHaveLength(1);
        expect((yield* Ref.get(thread.launchedWith))?.runtimeMode).toBe("approval-required");
        const replayed = yield* a.call("delegate_task", {
          ...request,
          supervisionRequestedByUser: true,
        });
        const acceptedTask = accepted.structuredContent as { taskId: NodeId };
        expect(replayed.structuredContent).toMatchObject({ taskId: acceptedTask.taskId });
        expect(forwarded.filter((name) => name === "t3_thread_launch")).toHaveLength(1);
        expect((yield* a.orchestrator.getThreadProjection(parentThreadId)).subagents).toHaveLength(
          1,
        );
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("replays a legacy pending remote intent with narrower modes without new opt-in", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* linkTo(a.links, b, "full-access");
      yield* seedParent(a);
      const link = yield* a.links.get(box.environmentId).pipe(Effect.map(Option.getOrThrow));
      const key = "legacy-pending-supervision";
      const commandId = CommandId.make(
        `command:mcp:${encodeURIComponent(parentScope.requestNamespace)}:remote-delegate:${key}`,
      );
      const request = {
        task: "Inspect the legacy task.",
        target: { environmentId: box.environmentId },
        runtimeMode: "approval-required",
        clientRequestId: key,
      };
      const store = a.intents;
      yield* store.prepare(commandId, request, {
        scope: {
          environmentId: parentScope.environmentId,
          requestNamespace: parentScope.requestNamespace,
          thread: parentScope.thread!,
        },
        environmentId: box.environmentId,
        linkedAt: DateTime.formatIso(link.linkedAt),
        label: link.label,
        launch: {
          projectId: boxProject,
          title: "Laptop parent",
          modelSelection,
          runtimeMode: "approval-required",
          message: request.task,
          clientRequestId: `delegate:${parentThreadId}:${key}`,
          delegatedFrom: {
            environmentId: laptop.environmentId,
            threadId: parentThreadId,
            title: "Laptop parent",
          },
        },
        request: {
          type: "delegated_task.remote.request",
          commandId,
          parentThreadId,
          parentRunId,
          parentNodeId: parentRootNode,
          task: request.task,
          driver: ProviderDriverKind.make("remote"),
          modelSelection,
          completionWake: "always",
        },
      });
      expect(yield* store.pending).toHaveLength(1);
      const result = yield* a.remote.delegate(
        parentScope as McpInvocationContext.McpThreadInvocationScope,
        { ...request, runtimeMode: "approval-required" },
      );
      expect(result.taskId).toBeDefined();
      const saved = yield* store.get(commandId);
      expect(saved?.taskId).toBe(result.taskId);
      expect((yield* a.orchestrator.getThreadProjection(parentThreadId)).subagents).toHaveLength(1);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect.each(["defect", "typed transient"] as const)(
  "recovers a follower after one %s without another launch, result, or parent wake",
  (failure) =>
    Effect.scoped(
      Effect.gen(function* () {
        const thread = yield* makeBoxThread;
        const b = yield* serveBox(thread);
        const offers = yield* SubscriptionRef.make<
          ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
        >([]);
        const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
        let waits = 0;
        let launches = 0;
        let results = 0;
        const retryStarted = yield* Deferred.make<void>();
        const resultRecorded = yield* Deferred.make<void>();
        const a = yield* makeLaptop(
          database,
          offers,
          undefined,
          undefined,
          undefined,
          (wait, input) =>
            Effect.suspend(() => {
              expect(input.untilDelegatedWorkSettled).toBe(true);
              waits++;
              if (waits === 1)
                return failure === "defect"
                  ? Effect.die("synthetic follower defect")
                  : Effect.fail(
                      new OrchestratorMcpFailure({
                        code: "orchestration_error",
                        message: "Synthetic transient wait failure.",
                      }),
                    );
              return Deferred.succeed(retryStarted, undefined).pipe(Effect.andThen(wait));
            }),
          (name) => {
            if (name === "t3_thread_launch") launches++;
          },
          undefined,
          {
            domainEvents: (events) =>
              events.pipe(
                Stream.tap((event) =>
                  Effect.gen(function* () {
                    if (event.type !== "subagent.updated" || event.payload.result === null) return;
                    results++;
                    yield* Deferred.succeed(resultRecorded, undefined);
                  }),
                ),
              ),
          },
        );
        yield* linkTo(a.links, b, "full-access");
        yield* seedParent(a);
        yield* a.remote.start();
        const delegated = yield* delegateToBox(a);
        yield* Deferred.await(retryStarted);
        const open = (yield* a.threads.getThreadRecords(parentThreadId, ["subagents"]))
          .subagents[0]!;
        yield* a.remote.observe(parentScope as McpInvocationContext.McpThreadInvocationScope, open);
        yield* a.remote.observe(parentScope as McpInvocationContext.McpThreadInvocationScope, open);
        yield* Ref.set(thread.reply, "Recovered exact result.");
        yield* Ref.set(thread.status, "completed");
        yield* Deferred.succeed(thread.finished, undefined);
        expect(yield* taskResult(a, delegated.taskId)).toMatchObject({
          status: "completed",
          result: "Recovered exact result.",
        });
        const delivered = yield* SubscriptionRef.changes(offers).pipe(
          Stream.filter((seen) => seen.some((offer) => offer.threadId === parentThreadId)),
          Stream.runHead,
        );
        expect(Option.isSome(delivered) && delivered.value).toHaveLength(1);
        yield* Deferred.await(resultRecorded);
        expect(launches).toBe(1);
        expect(waits).toBe(2);
        expect(results).toBe(1);
      }),
    ).pipe(
      Effect.timeout(Duration.seconds(8)),
      Effect.provide(NodeHttpServer.layerTest),
      TestClock.withLive,
    ),
);

it.effect.each(["lost", "existing"] as const)(
  "observing an open task re-arms a %s follower without concurrent waits",
  (follower) =>
    Effect.scoped(
      Effect.gen(function* () {
        const thread = yield* makeBoxThread;
        const b = yield* serveBox(thread);
        const offers = yield* SubscriptionRef.make<
          ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
        >([]);
        const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
        const lost = yield* Deferred.make<void>();
        let waits = 0;
        const a = yield* makeLaptop(database, offers, undefined, undefined, undefined, (wait) =>
          Effect.suspend(() => {
            waits++;
            return follower === "lost" && waits === 1
              ? Effect.interrupt.pipe(Effect.ensuring(Deferred.succeed(lost, undefined)))
              : wait;
          }),
        );
        yield* linkTo(a.links, b, "full-access");
        yield* seedParent(a);
        const delegated = yield* delegateToBox(a);
        yield* follower === "lost" ? Deferred.await(lost) : Deferred.await(thread.waitStarted);
        const open = (yield* a.threads.getThreadRecords(parentThreadId, ["subagents"]))
          .subagents[0]!;
        yield* a.remote.observe(parentScope as McpInvocationContext.McpThreadInvocationScope, open);
        yield* a.remote.observe(parentScope as McpInvocationContext.McpThreadInvocationScope, open);
        yield* Deferred.await(thread.waitStarted);
        yield* Ref.set(thread.reply, "Observed exact result.");
        yield* Ref.set(thread.status, "completed");
        yield* Deferred.succeed(thread.finished, undefined);
        expect(yield* taskResult(a, delegated.taskId)).toMatchObject({
          status: "completed",
          result: "Observed exact result.",
        });
        expect(waits).toBe(follower === "lost" ? 2 : 1);
      }),
    ).pipe(
      Effect.timeout(Duration.seconds(5)),
      Effect.provide(NodeHttpServer.layerTest),
      TestClock.withLive,
    ),
);

it.effect.each([
  "wrong outer thread",
  "running result run",
  "mismatched original run",
  "unattributed run",
  "terminal but unsettled",
  "incomplete final text",
] as const)(
  "retries a matching observation with $0 without false fleet completion",
  (malformation) =>
    Effect.scoped(
      Effect.gen(function* () {
        const thread = yield* makeBoxThread;
        const b = yield* serveBox(thread);
        const offers = yield* SubscriptionRef.make<
          ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
        >([]);
        const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
        const retryStarted = yield* Deferred.make<void>();
        const allowValidResponse = yield* Deferred.make<void>();
        let malformed = false;
        let incompleteRead = false;
        const forwardedAfterMalformed: string[] = [];
        const a = yield* makeLaptop(
          database,
          offers,
          undefined,
          undefined,
          undefined,
          (wait) =>
            wait.pipe(
              Effect.flatMap((result) =>
                Effect.gen(function* () {
                  if (!malformed) {
                    malformed = true;
                    expect(result.remoteObservation).toBeDefined();
                    switch (malformation) {
                      case "wrong outer thread":
                        return { ...result, threadId: ThreadId.make("wrong-thread") };
                      case "running result run":
                        return {
                          ...result,
                          resultRun: { runId: boxRunId, status: "running" as const },
                        };
                      case "mismatched original run":
                        return {
                          ...result,
                          remoteObservation: {
                            ...result.remoteObservation!,
                            originalRunId: RunId.make("run:wrong-task"),
                          },
                        };
                      case "unattributed run":
                        return { ...result, runId: null };
                      case "terminal but unsettled":
                        expect(result.remoteObservation?.childProgress?.hasPendingChildRuns).toBe(
                          false,
                        );
                        return { ...result, timedOut: true };
                      case "incomplete final text":
                        return result;
                    }
                  }
                  yield* Deferred.succeed(retryStarted, undefined);
                  yield* Deferred.await(allowValidResponse);
                  return result;
                }),
              ),
            ),
          (tool) => {
            if (malformed) forwardedAfterMalformed.push(tool);
          },
          undefined,
          {
            read: (read) => {
              if (malformation !== "incomplete final text" || !malformed || incompleteRead)
                return Effect.succeed(read);
              incompleteRead = true;
              return Effect.succeed({
                ...read,
                items: read.items.map((item) => ({
                  ...item,
                  textTruncated: true,
                  nextTextOffset: null,
                })),
              });
            },
          },
        );
        yield* linkTo(a.links, b, "full-access");
        yield* seedParent(a);
        const delegated = yield* delegateToBox(a);
        yield* Ref.set(thread.reply, "Exact settled result.");
        yield* Ref.set(thread.status, "completed");
        yield* Deferred.succeed(thread.finished, undefined);
        const outcome = yield* Effect.raceFirst(
          Deferred.await(retryStarted).pipe(Effect.as("retry")),
          taskResult(a, delegated.taskId).pipe(Effect.as("false completion")),
        ).pipe(Effect.timeout(Duration.seconds(5)));
        expect(outcome).toBe("retry");
        const pending = (yield* a.orchestrator.getThreadProjection(parentThreadId)).subagents.find(
          (task) => task.id === delegated.taskId,
        );
        expect(pending?.result).toBeNull();
        expect(pending?.remoteChild?.stoppedThere).not.toBe(true);
        expect(yield* Ref.get(thread.stops)).toEqual([]);
        if (malformation !== "incomplete final text")
          expect(forwardedAfterMalformed).not.toContain("t3_thread_read");
        yield* Deferred.succeed(allowValidResponse, undefined);
        expect(yield* taskResult(a, delegated.taskId)).toMatchObject({
          status: "completed",
          result: "Exact settled result.",
        });
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest), TestClock.withLive),
);

it.effect(
  "task_status observes queued follow-up work and a failed provider start without replacing the original result",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const thread = yield* makeBoxThread;
        const b = yield* serveBox(thread);
        const offers = yield* SubscriptionRef.make<
          ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
        >([]);
        const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
        const a = yield* makeLaptop(database, offers);
        yield* linkTo(a.links, b, "full-access");
        yield* seedParent(a);
        const delegated = yield* delegateToBox(a);
        yield* Ref.set(thread.reply, "Original completed result.");
        yield* Ref.set(thread.status, "completed");
        yield* Deferred.succeed(thread.finished, undefined);
        yield* taskResult(a, delegated.taskId);
        const later = {
          id: boxLaterRunId,
          threadId: delegated.remoteChild.threadId,
          ordinal: 2,
          status: "queued",
          queueHeld: false,
          modelSelection,
          requestedAt: DateTime.makeUnsafe(2_000),
          startedAt: null,
          completedAt: null,
        } as OrchestrationV2Run;
        yield* Ref.set(thread.laterRun, later);
        const pending = yield* a.call("task_status", { taskId: delegated.taskId });
        expect(pending.isError).not.toBe(true);
        expect(pending.structuredContent).toMatchObject({
          status: "completed",
          summary: "Original completed result.",
          hasPendingChildRuns: true,
          latestTerminalRunId: boxRunId,
          latestTerminalStatus: "completed",
          latestTerminalSummary: "Original completed result.",
          remoteObservation: {
            originalRunId: boxRunId,
            threadId: delegated.remoteChild.threadId,
            environmentId: box.environmentId,
          },
        });
        yield* Ref.set(thread.laterRun, { ...later, queueHeld: true });
        expect(
          (yield* a.call("task_status", { taskId: delegated.taskId })).structuredContent,
        ).toMatchObject({ hasPendingChildRuns: false });
        yield* Ref.set(thread.laterRun, {
          ...later,
          status: "failed",
          completedAt: DateTime.makeUnsafe(3_000),
        });
        yield* Ref.set(thread.laterReply, "Earlier partial follow-up output.");
        yield* Ref.set(thread.laterError, "The follow-up provider failed before starting.");
        const failed = yield* a.call("task_status", { taskId: delegated.taskId });
        expect(failed.isError).not.toBe(true);
        expect(failed.structuredContent).toMatchObject({
          status: "completed",
          summary: "Original completed result.",
          hasPendingChildRuns: false,
          latestTerminalRunId: boxLaterRunId,
          latestTerminalStatus: "failed",
          latestTerminalSummary: "The follow-up provider failed before starting.",
        });
        const original = (yield* a.orchestrator.getThreadProjection(parentThreadId)).subagents.find(
          (task) => task.id === delegated.taskId,
        );
        expect(original).toMatchObject({
          status: "completed",
          result: "Original completed result.",
        });
        const missing = yield* a.remote
          .observe(parentScope as McpInvocationContext.McpThreadInvocationScope, {
            ...original!,
            remoteChild: { ...original!.remoteChild!, runId: undefined },
          })
          .pipe(Effect.flip);
        expect(missing.message).toContain("original remote run is not attributed");
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect.each(["unsupported", "unavailable", "mismatched"] as const)(
  "task_status leaves terminal delivery unacknowledged when fresh progress is %s",
  (failure) =>
    Effect.scoped(
      Effect.gen(function* () {
        const thread = yield* makeBoxThread;
        const b = yield* serveBox(thread);
        const offers = yield* SubscriptionRef.make<
          ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
        >([]);
        const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
        const a = yield* makeLaptop(database, offers, undefined, undefined, (read) => {
          if (failure === "unavailable")
            return Effect.fail(
              new OrchestratorMcpFailure({
                code: "orchestration_error",
                message: "Synthetic unavailable remote observation.",
              }),
            );
          if (failure === "unsupported") {
            const { remoteObservation: _remoteObservation, ...legacy } = read;
            return Effect.succeed(legacy);
          }
          return Effect.succeed({
            ...read,
            remoteObservation: {
              ...read.remoteObservation!,
              originalRunId: RunId.make("run:wrong-task"),
            },
          });
        });
        yield* linkTo(a.links, b, "full-access");
        yield* seedParent(a);
        const delegated = yield* delegateToBox(a);
        yield* Ref.set(thread.reply, "Original completed result.");
        yield* Ref.set(thread.status, "completed");
        yield* Deferred.succeed(thread.finished, undefined);
        const before = yield* taskResult(a, delegated.taskId);
        const status = yield* a.call("task_status", { taskId: delegated.taskId });
        expect(status.isError).toBe(true);
        expect(JSON.stringify(status.content)).toContain(
          failure === "mismatched" ? "mismatched task binding" : failure,
        );
        const after = (yield* a.orchestrator.getThreadProjection(parentThreadId)).subagents.find(
          (task) => task.id === delegated.taskId,
        )!;
        expect(after.result).toBe("Original completed result.");
        expect(after.completionDelivery).toEqual(before?.completionDelivery);
        expect(after.completionDelivery?.state).not.toBe("acknowledged");
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("delegates worktree isolation to the destination launch defaults", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const a = yield* makeLaptop(database, offers);
      yield* linkTo(a.links, b, "auto");
      yield* seedParent(a);

      const result = yield* a.call("delegate_task", {
        task: "Run the suite on the box.",
        target: { environmentId: box.environmentId },
        isolation: "worktree",
      });
      expect(result.isError, JSON.stringify(result.content)).toBe(false);
      expect((yield* Ref.get(thread.launchedWith))?.workspaceStrategy).toEqual({
        type: "worktree",
      });
      expect(yield* Ref.get(thread.launched)).not.toBeNull();
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("retries the startup open-task scan after a defect", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const delegated = yield* Effect.scoped(
        Effect.gen(function* () {
          const before = yield* makeLaptop(database, offers);
          yield* linkTo(before.links, b, "full-access");
          yield* seedParent(before);
          return yield* delegateToBox(before);
        }),
      );
      yield* Ref.set(thread.reply, "Recovered during startup.");
      yield* Ref.set(thread.status, "completed");
      yield* Deferred.succeed(thread.finished, undefined);
      let scans = 0;
      const after = yield* makeLaptop(
        database,
        offers,
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        {
          openTasks: (scan) =>
            Effect.suspend(() => {
              scans++;
              return scans === 1 ? Effect.die("synthetic startup scan defect") : scan;
            }),
        },
      );
      yield* linkTo(after.links, b, "full-access");
      yield* after.remote.start();
      expect(yield* taskResult(after, delegated.taskId)).toMatchObject({
        status: "completed",
        result: "Recovered during startup.",
      });
      expect(scans).toBe(2);
    }),
  ).pipe(
    Effect.timeout(Duration.seconds(8)),
    Effect.provide(NodeHttpServer.layerTest),
    TestClock.withLive,
  ),
);

it.effect(
  "recovers a failed domain subscription and follows Stop debt recorded during backoff",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const thread = yield* makeBoxThread;
        const b = yield* serveBox(thread);
        const offers = yield* SubscriptionRef.make<
          ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
        >([]);
        const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
        const resubscribed = yield* Deferred.make<void>();
        const failSubscription = yield* Deferred.make<void>();
        let subscriptions = 0;
        const a = yield* makeLaptop(
          database,
          offers,
          undefined,
          undefined,
          undefined,
          undefined,
          undefined,
          undefined,
          {
            domainEvents: (events) =>
              Stream.suspend(() => {
                subscriptions++;
                return subscriptions === 1
                  ? Stream.unwrap(
                      Deferred.await(failSubscription).pipe(
                        Effect.as(Stream.die("synthetic domain subscription defect")),
                      ),
                    )
                  : Stream.unwrap(
                      Deferred.succeed(resubscribed, undefined).pipe(Effect.as(events)),
                    );
              }),
          },
        );
        yield* linkTo(a.links, b, "full-access");
        yield* seedParent(a);
        yield* a.remote.start();
        const delegated = yield* delegateToBox(a);
        yield* Ref.set(thread.reply, "First result stays.");
        yield* Ref.set(thread.status, "completed");
        yield* Deferred.succeed(thread.finished, undefined);
        yield* taskResult(a, delegated.taskId);
        expect(yield* a.projections.getOpenRemoteDelegatedTasks).toEqual([]);
        yield* Effect.yieldNow;
        yield* Deferred.succeed(failSubscription, undefined);
        yield* a.threads.stopDelegatedTasks({
          threadId: parentThreadId,
          commandId: CommandId.make("command:stop:after-subscription-recovery"),
        });
        yield* Deferred.await(resubscribed);
        expect(yield* stoppedThere(a, delegated.taskId, 1)).toMatchObject({
          status: "completed",
          result: "First result stays.",
        });
        expect(yield* Ref.get(thread.stops)).toEqual(["thread.stop", "delegated-tasks"]);
        expect(subscriptions).toBe(2);
      }),
    ).pipe(
      Effect.timeout(Duration.seconds(8)),
      Effect.provide(NodeHttpServer.layerTest),
      TestClock.withLive,
    ),
);

it.effect("scope shutdown interrupts a follower without re-arming or writing its result", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      let waits = 0;
      let subscriptions = 0;
      let scans = 0;
      const running = yield* Effect.scoped(
        Effect.gen(function* () {
          const a = yield* makeLaptop(
            database,
            offers,
            undefined,
            undefined,
            undefined,
            (wait) => Effect.sync(() => waits++).pipe(Effect.andThen(wait)),
            undefined,
            undefined,
            {
              domainEvents: (events) =>
                Stream.suspend(() => {
                  subscriptions++;
                  return events;
                }),
              openTasks: (scan) => Effect.sync(() => scans++).pipe(Effect.andThen(scan)),
            },
          );
          yield* linkTo(a.links, b, "full-access");
          yield* seedParent(a);
          yield* a.remote.start();
          yield* delegateToBox(a);
          return yield* Effect.never;
        }),
      ).pipe(Effect.forkChild);
      yield* Deferred.await(thread.waitStarted);
      yield* Fiber.interrupt(running);
      yield* Ref.set(thread.reply, "Too late for the closed service.");
      yield* Ref.set(thread.status, "completed");
      yield* Deferred.succeed(thread.finished, undefined);
      yield* TestClock.adjust(Duration.minutes(10));
      const after = yield* makeLaptop(database, offers);
      const tasks = (yield* after.threads.getThreadRecords(parentThreadId, ["subagents"]))
        .subagents;
      expect(tasks).toHaveLength(1);
      expect(tasks[0]?.result).toBeNull();
      expect(waits).toBe(1);
      expect(subscriptions).toBe(1);
      expect(scans).toBe(1);
      expect(yield* Ref.get(thread.stops)).toEqual([]);
      expect(yield* SubscriptionRef.get(offers)).toEqual([]);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect.each(["completed", "failed"] as const)(
  "CSR-F1 reconciles an already launched legacy %s result with its complete original-run text",
  (status) =>
    Effect.scoped(
      Effect.gen(function* () {
        const thread = yield* makeBoxThread;
        const b = yield* serveBox(thread);
        const offers = yield* SubscriptionRef.make<
          ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
        >([]);
        const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
        const delegated = yield* Effect.scoped(
          Effect.gen(function* () {
            const before = yield* makeLaptop(database, offers);
            yield* linkTo(before.links, b, "full-access");
            yield* seedParent(before);
            return yield* delegateToBox(before);
          }),
        );
        const result = `${status}: ${"x".repeat(65_000)} complete`;
        yield* Ref.set(thread.reply, status === "completed" ? result : "Earlier partial reply.");
        if (status === "failed") yield* Ref.set(thread.error, result);
        yield* Ref.set(thread.laterReply, "Unrelated later run.");
        yield* Ref.set(thread.status, status);
        yield* Deferred.succeed(thread.finished, undefined);
        let waits = 0;
        const after = yield* makeLaptop(
          database,
          offers,
          undefined,
          undefined,
          (read) => {
            const { remoteObservation: _, ...legacy } = read;
            return Effect.succeed(legacy);
          },
          (wait) =>
            wait.pipe(
              Effect.map(({ remoteObservation: _, ...legacy }) => {
                waits++;
                return legacy;
              }),
            ),
        );
        yield* linkTo(after.links, b, "full-access");
        yield* after.remote.start();
        const completed = yield* taskResult(after, delegated.taskId).pipe(
          Effect.timeout(Duration.seconds(3)),
        );
        expect(completed).toMatchObject({ status, result });
        expect(waits).toBe(1);
        expect(
          yield* after.threads.readRemoteTaskObservation!(parentThreadId, delegated.taskId),
        ).toBeUndefined();
        const freshStatus = yield* after.call("task_status", { taskId: delegated.taskId });
        expect(freshStatus.isError).toBe(true);
        expect(JSON.stringify(freshStatus.content)).toContain("unsupported");
        expect(yield* Ref.get(thread.stops)).toEqual([]);
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest), TestClock.withLive),
);

it.effect("CSR-F1 fails and stops nonterminal legacy work after one bounded wait", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const thread = yield* makeBoxThread;
      const b = yield* serveBox(thread);
      const offers = yield* SubscriptionRef.make<
        ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
      >([]);
      const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
      const delegated = yield* Effect.scoped(
        Effect.gen(function* () {
          const before = yield* makeLaptop(database, offers);
          yield* linkTo(before.links, b, "full-access");
          yield* seedParent(before);
          return yield* delegateToBox(before);
        }),
      );
      const waits: Array<OrchestratorMcpThreadWaitInput> = [];
      const after = yield* makeLaptop(
        database,
        offers,
        undefined,
        undefined,
        undefined,
        (_wait, input) =>
          Effect.sync(() => {
            waits.push(input);
            return {
              threadId: input.threadId,
              runId: input.runId ?? null,
              status: "running" as const,
              timedOut: true,
            };
          }),
      );
      yield* linkTo(after.links, b, "full-access");
      yield* after.remote.start();
      const failed = yield* taskResult(after, delegated.taskId).pipe(
        Effect.timeout(Duration.seconds(3)),
      );
      expect(failed?.status).toBe("failed");
      expect(failed?.result).toMatch(/observation.*unsupported/i);
      expect(waits).toHaveLength(1);
      expect(waits[0]).toMatchObject({
        threadId: delegated.remoteChild.threadId,
        runId: boxRunId,
        timeoutMs: 50_000,
      });
      yield* Deferred.await(thread.interruptedOnce).pipe(Effect.timeout(Duration.seconds(8)));
      expect(yield* Ref.get(thread.stops)).toContain("thread.stop");
      expect(waits).toHaveLength(1);
      const current = (yield* after.orchestrator.getThreadProjection(
        parentThreadId,
      )).subagents.find((task) => task.id === delegated.taskId);
      expect(current?.result).toBe(failed?.result);
      expect(
        yield* after.threads.readRemoteTaskObservation!(parentThreadId, delegated.taskId),
      ).toBeUndefined();
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest), TestClock.withLive),
);

it.effect(
  "a stop refused by lowered modes remains owed until the original child can be stopped",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const thread = yield* makeBoxThread;
        const b = yield* serveBox(thread);
        const offers = yield* SubscriptionRef.make<
          ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
        >([]);
        const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
        const stopRefused = yield* Deferred.make<OrchestratorMcpFailure>();
        const stopRequests: Array<
          Parameters<PeerForwarding.PeerForwarding["Service"]["interruptThread"]>[2]
        > = [];
        const a = yield* makeLaptop(
          database,
          offers,
          undefined,
          (error) => Deferred.succeed(stopRefused, error).pipe(Effect.asVoid),
          undefined,
          undefined,
          undefined,
          (input) => stopRequests.push(input),
        );
        yield* linkTo(a.links, b, "full-access");
        yield* seedParent(a);
        const delegated = yield* delegateToBox(a);

        // The parent is lowered below the full-access thread there, then stopped.
        yield* a.orchestrator.dispatch({
          type: "thread.runtime-mode.set",
          commandId: CommandId.make("command:lower:laptop-parent"),
          threadId: parentThreadId,
          runtimeMode: "approval-required",
        });
        yield* a.threads.stopDelegatedTasks({
          threadId: parentThreadId,
          commandId: CommandId.make("command:stop:laptop-parent-lowered"),
        });

        const refusal = yield* Deferred.await(stopRefused).pipe(
          Effect.timeout(Duration.seconds(10)),
          Effect.catchTags({
            TimeoutError: () =>
              Effect.die(
                "The follower did not observe a remote stop refusal while modes were lowered.",
              ),
          }),
        );
        expect(refusal.code).toBe("runtime_mode_escalation_denied");
        expect(refusal.message).toContain(
          "full-access is broader than parent mode approval-required",
        );
        const pending = (yield* a.orchestrator.getThreadProjection(parentThreadId)).subagents.find(
          (task) => task.id === delegated.taskId,
        )!;
        expect(pending.status).toBe("interrupted");
        expect(pending.remoteChild?.stoppedThere).not.toBe(true);
        expect(yield* Ref.get(thread.stops)).toEqual([]);
        expect(yield* a.projections.getOpenRemoteDelegatedTasks).toContainEqual({
          parentThreadId,
          taskId: delegated.taskId,
        });
        const rawStopCount = pending.remoteChild?.stopCount;
        expect(rawStopCount).toBeUndefined();
        expect(rawStopCount ?? 0).toBe(0);
        expect(Object.hasOwn(pending.remoteChild ?? {}, "stopCount")).toBe(false);
        expect(pending).toMatchObject({
          id: delegated.taskId,
          threadId: parentThreadId,
          runId: parentRunId,
          result: "Stopped with its parent.",
          remoteChild: { ...delegated.remoteChild, runId: boxRunId },
        });
        const originalIntent = yield* a.intents.byTask(delegated.taskId);
        expect(originalIntent).toMatchObject({
          taskId: delegated.taskId,
          status: "recorded",
          envelope: {
            environmentId: box.environmentId,
            request: { parentThreadId },
            launch: { clientRequestId: "delegate:thread:laptop-parent:suite-on-box" },
          },
          launched: { threadId: pending.remoteChild!.threadId, runId: boxRunId },
        });
        yield* a.orchestrator.dispatch({
          type: "thread.runtime-mode.set",
          commandId: CommandId.make("command:restore:laptop-parent"),
          threadId: parentThreadId,
          runtimeMode: "full-access",
        });
        yield* Deferred.await(thread.interruptedOnce).pipe(
          Effect.timeout(Duration.seconds(10)),
          Effect.catchTags({
            TimeoutError: () =>
              Effect.die(
                "The original remote child was not stopped after its parent's modes were restored.",
              ),
          }),
        );
        const acknowledged = yield* stoppedThere(a, delegated.taskId, null).pipe(
          Effect.timeout(Duration.seconds(10)),
          Effect.catchTags({
            TimeoutError: () =>
              Effect.die("The original remote child's stop was not durably acknowledged."),
          }),
        );
        expect(acknowledged?.remoteChild).toMatchObject({
          environmentId: box.environmentId,
          threadId: pending.remoteChild!.threadId,
          runId: boxRunId,
          stoppedThere: true,
        });
        expect(acknowledged).toMatchObject({
          id: delegated.taskId,
          threadId: parentThreadId,
          runId: parentRunId,
          result: pending.result,
          completedAt: pending.completedAt,
        });
        expect(yield* a.intents.byTask(delegated.taskId)).toEqual(originalIntent);
        expect(acknowledged?.status).toBe("interrupted");
        expect(acknowledged?.remoteChild?.stopCount).toBeUndefined();
        expect(Object.hasOwn(acknowledged?.remoteChild ?? {}, "stopCount")).toBe(false);
        expect(stopRequests.length).toBeGreaterThanOrEqual(2);
        expect(
          stopRequests.map(({ threadId, stop, clientRequestId }) => ({
            threadId,
            stop,
            clientRequestId,
          })),
        ).toEqual(
          Array.from({ length: stopRequests.length }, () => ({
            threadId: pending.remoteChild!.threadId,
            stop: true,
            clientRequestId: `stop:${pending.remoteChild!.threadId}`,
          })),
        );
        expect(yield* Ref.get(thread.interrupted)).toBe(1);
        expect(yield* Ref.get(thread.stops)).toEqual(["thread.stop", "delegated-tasks"]);
        expect(yield* a.projections.getOpenRemoteDelegatedTasks).toEqual([]);
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest), TestClock.withLive),
);

it.effect(
  "a task without an attributed remote run remains pending instead of using a latest reply",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const thread = yield* makeBoxThread;
        const b = yield* serveBox(thread);
        const offers = yield* SubscriptionRef.make<
          ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
        >([]);
        const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
        const a = yield* makeLaptop(database, offers);
        yield* linkTo(a.links, b, "full-access");
        yield* seedParent(a);
        // An old reply does not identify the run belonging to this launch.
        yield* Ref.set(thread.runless, true);
        yield* Ref.set(thread.reply, "Done before it was followed.");

        const delegated = yield* delegateToBox(a);
        const task = (yield* a.orchestrator.getThreadProjection(parentThreadId)).subagents.find(
          (task) => task.id === delegated.taskId,
        );
        expect(task?.status).toBe("running");
        expect(task?.result).toBeNull();
        expect(task?.remoteChild).toBeUndefined();
        expect(yield* Ref.get(thread.waitedFor)).toEqual([]);
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect.each(["pass", "hold"] as const)(
  "authenticated peer admission %s preserves the reserved task and launches once",
  (verdict) =>
    Effect.scoped(
      Effect.gen(function* () {
        const thread = yield* makeBoxThread;
        let receivingAuth: EnvironmentAuth.EnvironmentAuth["Service"] | undefined;
        let evaluations = 0;
        const admissionInstance: ProviderInstance = {
          instanceId,
          driverKind: driver,
          continuationIdentity: { driverKind: driver, continuationKey: instanceId },
          displayName: undefined,
          enabled: true,
          snapshot: {
            getSnapshot: Effect.succeed({
              instanceId,
              driver,
              enabled: true,
              installed: true,
              status: "ready",
              auth: { status: "authenticated" },
              checkedAt: "2026-11-02T00:00:00Z",
              version: null,
              models: [
                {
                  slug: modelSelection.model,
                  name: "Synthetic admission model",
                  isCustom: false,
                  capabilities: null,
                },
              ],
              slashCommands: [],
              skills: [],
              supportsRemoteRequestEvaluation: true,
            } satisfies ServerProvider),
            refresh: Effect.die("Admission fixtures must not refresh the provider."),
            resolveMaintenance: () =>
              Effect.die("Admission fixtures must not resolve maintenance."),
            streamChanges: Stream.empty,
            applyUsageLimits: () => Effect.die("Admission fixtures must not apply usage limits."),
          },
          get orchestrationAdapter(): ProviderInstance["orchestrationAdapter"] {
            throw new Error("Admission fixtures must not start a provider session.");
          },
          get textGeneration(): ProviderInstance["textGeneration"] {
            throw new Error("Admission fixtures use their injected request evaluator.");
          },
        };
        const admissionContext = yield* AdmissionIntake.layer.pipe(
          Layer.provide(
            Layer.mock(ProviderInstanceRegistry.ProviderInstanceRegistry)({
              getCatalogSource: (id) => (id === instanceId ? admissionInstance : undefined),
              getInstance: (id) =>
                Effect.succeed(id === instanceId ? admissionInstance : undefined),
            }),
          ),
          Layer.provide(
            RemoteAdmission.layerWithEvaluator(() =>
              Effect.sync(() => {
                evaluations++;
                return {
                  verdict,
                  category: verdict === "pass" ? "none" : "risky",
                  reason: "Synthetic peer admission",
                };
              }),
            ),
          ),
          Layer.provide(
            Layer.mock(EnvironmentAuth.EnvironmentAuth)({
              listSessions: () =>
                receivingAuth === undefined
                  ? Effect.die("Receiving auth not ready")
                  : receivingAuth.listSessions(),
            }),
          ),
          Layer.provide(SqlitePersistence.layerMemory),
          Layer.fresh,
          Layer.build,
        );
        const intake = Context.get(admissionContext, AdmissionIntake.RemoteAdmissionIntake);
        yield* intake.updatePolicy({
          expectedRevision: "initial",
          enabled: true,
          modelSelection,
        });
        const b = yield* servePeer(
          box,
          Layer.merge(
            boxToolkitLayer(thread),
            McpHttpServer.toolkitRegistration(AdmissionCancel.toolkit, AdmissionCancel.handlers),
          ).pipe(Layer.provide(Layer.succeedContext(admissionContext))),
        );
        receivingAuth = b.auth;
        const offers = yield* SubscriptionRef.make<
          ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>
        >([]);
        const database = yield* SqlitePersistence.layerMemory.pipe(Layer.build);
        const a = yield* makeLaptop(database, offers);
        yield* linkTo(a.links, b, "full-access");
        yield* seedParent(a);
        const delegated = yield* delegateToBox(a);
        if (verdict === "hold") {
          expect(yield* Ref.get(thread.launched)).toBeNull();
          const status = yield* a.call("task_status", { taskId: delegated.taskId });
          expect(status.structuredContent).toMatchObject({
            status: "running",
            hasPendingQuestion: true,
            summary: "Awaiting owner approval in Box.",
          });
          const projection = yield* a.orchestrator.getThreadProjection(parentThreadId);
          expect(projection.subagents[0]).toMatchObject({ status: "waiting", result: null });
          expect(
            projection.messages.filter((message) =>
              message.text.includes("Awaiting owner approval in Box."),
            ),
          ).toHaveLength(1);
          const held = (yield* intake.snapshot()).pending[0]!;
          expect(held.status).toBe("held");
          yield* intake.decide(
            { id: held.id, bindingDigest: held.bindingDigest, decision: "approve" },
            {
              sessionId: AuthSessionId.make("synthetic-owner"),
              subject: "one-time-token",
              method: "browser-session-cookie",
              scopes: new Set(AuthAdministrativeScopes),
            },
          );
          yield* a.threads
            .streamStoredEventsFrom({
              threadId: parentThreadId,
              afterSequence: 0,
              eventType: "subagent.updated",
            })
            .pipe(
              Stream.filter(
                (stored) =>
                  stored.event.type === "subagent.updated" &&
                  stored.event.payload.id === delegated.taskId &&
                  stored.event.payload.remoteChild !== undefined,
              ),
              Stream.runHead,
              Effect.timeout(Duration.seconds(10)),
            );
        }
        expect(yield* Ref.get(thread.launched)).not.toBeNull();
        const replayed = yield* delegateToBox(a);
        expect(replayed.taskId).toBe(delegated.taskId);
        expect(evaluations).toBe(1);
        expect((yield* a.orchestrator.getThreadProjection(parentThreadId)).subagents).toHaveLength(
          1,
        );
        yield* Ref.set(thread.reply, "Approved task result.");
        yield* Ref.set(thread.status, "completed");
        yield* Deferred.succeed(thread.finished, undefined);
        expect(yield* taskResult(a, delegated.taskId)).toMatchObject({
          status: "completed",
          result: "Approved task result.",
        });
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest), TestClock.withLive),
);

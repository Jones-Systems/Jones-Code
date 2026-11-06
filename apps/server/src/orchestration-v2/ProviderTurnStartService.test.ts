import * as ConversationPolicy from "../jones/conversationWorkspace/ConversationPolicy.ts";
import * as WorkspaceProvenance from "../jones/workspaceLifecycle/Provenance.ts";
import * as ProviderSwitchService from "./ProviderSwitchService.ts";
import * as RestartPreparation from "../jones/orchestration/RestartPreparation.ts";
import * as ControlAdmission from "../jones/orchestration/ControlAdmission.ts";
import * as ContinuityAdmissionTestkit from "../jones/goals/continuity/admission.testkit.ts";
import type { ContinuityRequirement } from "@t3tools/provider-core/server/jones/GoalContinuityContext";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as PlannedUpdateContinuity from "../jones/updates/PlannedUpdateContinuity.ts";
import { expect, vi } from "vite-plus/test";
import { it as effectIt } from "@effect/vitest";
import {
  CheckpointScopeId,
  ContextHandoffId,
  DEFAULT_SERVER_SETTINGS,
  MessageId,
  NodeId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSetupError,
  RunAttemptId,
  RunId,
  ThreadId,
  ProjectId,
  type ModelSelection,
  type ServerProviderModel,
  type ServerProvider,
  type OrchestrationV2ThreadProjection,
  OrchestrationV2DomainEvent,
  OrchestrationV2AppThread,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import * as ServerConfig from "../config.ts";
import { withWorkspaceLease } from "../workspace/workspaceLease.ts";
import * as Git from "../vcs/GitVcsDriver.ts";
import { git as nativeGit } from "../jones/workspaceRetirement/Filesystem.ts";
import {
  acquireNativeFixture,
  releaseNativeFixture,
} from "../jones/workspaceRecreation/Native.testkit.ts";
import {
  binding as checkoutBinding,
  incarnation as checkoutIncarnation,
} from "../jones/workspaceRetirement/OutputAttestation.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ProviderAuthService from "../provider/ProviderAuthService.ts";
import * as ProviderInstanceRegistry from "../provider/ProviderInstanceRegistry.ts";
import type { ProviderInstance } from "@t3tools/provider-core/server/driver";
import * as ServerSettings from "../serverSettings.ts";
import * as ContextHandoffService from "./ContextHandoffService.ts";
import * as EventSink from "./EventSink.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as ProviderTurnStart from "./ProviderTurnStartService.ts";
import * as RunExecutionService from "./RunExecutionService.ts";
import * as RuntimePolicy from "./RuntimePolicy.ts";
import * as WorkspaceLifecycle from "../jones/workspaceLifecycle/WorkspaceLifecycle.ts";
import { LifecycleError } from "../jones/workspaceLifecycle/Protocol.ts";
import {
  CurrentEffectClaim,
  EffectClaimLost,
  findEffectClaimLost,
} from "../jones/workspaceLifecycle/ActiveEffectClaim.ts";
import {
  ProviderControlOutcomeUnknown,
  findProviderControlOutcomeUnknown,
} from "@t3tools/provider-core/server/jones/providerControlOutcome";

const isDomainEvent = Schema.is(OrchestrationV2DomainEvent);

const managedWorktreesDir = "/tmp/provider-turn-start-worktrees";
// fs.realpath applies `..` and `.` before following symlinks.
const normalize = (target: string) => {
  const parts: Array<string> = [];
  for (const part of target.split("/")) {
    if (part === "..") parts.pop();
    else if (part !== "" && part !== ".") parts.push(part);
  }
  return `/${parts.join("/")}`;
};
const layerWorktreeLocation = (worktreesDir = managedWorktreesDir) =>
  Layer.mergeAll(
    Path.layer,
    Layer.mock(ServerConfig.ServerConfig)({ worktreesDir } as ServerConfig.ServerConfig["Service"]),
    ServerSettings.layerTest(),
  );

type RecreateCheckout = NonNullable<Git.GitVcsDriver["Service"]["recreateCheckout"]>;
interface RecreationFixtureBinding {
  readonly projectRoot: string;
  readonly baseDir: string;
  readonly worktreesDir: string;
}
const observedRecreationLayer = (
  binding: RecreationFixtureBinding | undefined,
  recreateCheckout: ReturnType<typeof vi.fn<RecreateCheckout>>,
) =>
  binding === undefined
    ? Layer.empty
    : Layer.effect(
        Git.GitVcsDriver,
        Effect.gen(function* () {
          const driver = yield* Git.GitVcsDriver;
          const actual = driver.recreateCheckout;
          if (actual === undefined) return yield* Effect.die("Real recreation adapter unavailable");
          recreateCheckout.mockImplementation((input) => actual(input));
          return Git.GitVcsDriver.of({ ...driver, recreateCheckout });
        }),
      ).pipe(
        Layer.provide(
          Git.layer.pipe(
            Layer.provide(ServerConfig.layerTest(binding.projectRoot, binding.baseDir)),
          ),
        ),
        Layer.provide(NodeServices.layer),
      );

class NativeTurnStartFixtureError extends Schema.TaggedError<NativeTurnStartFixtureError>()(
  "NativeTurnStartFixtureError",
  { cause: Schema.Defect() },
) {
  override get message() {
    return "Native turn-start fixture operation failed";
  }
}

const nativeFixtureOperation = <A>(action: () => Promise<A>) =>
  Effect.tryPromise({
    try: action,
    catch: (cause) => new NativeTurnStartFixtureError({ cause }),
  }).pipe(Effect.uninterruptible);
const startRecreationFixture = Effect.gen(function* () {
  const fixture = yield* Effect.acquireRelease(
    nativeFixtureOperation(acquireNativeFixture),
    (fixture) => nativeFixtureOperation(() => releaseNativeFixture(fixture)).pipe(Effect.orDie),
  );
  const fs = yield* FileSystem.FileSystem;
  yield* nativeFixtureOperation(() =>
    nativeGit(fixture.checkout, ["branch", "-m", "feature/restore"]),
  );
  return { ...fixture, fs };
});

function startWithMissingWorktree(
  worktreePath: string,
  realPaths: Record<string, string> = {
    [managedWorktreesDir]: managedWorktreesDir,
    [`${managedWorktreesDir}/project`]: `${managedWorktreesDir}/project`,
  },
  lifecycleLayer?: Layer.Layer<WorkspaceLifecycle.WorkspaceLifecycle>,
  provenanceLayer?: Layer.Layer<WorkspaceProvenance.Provenance>,
  native?: RecreationFixtureBinding,
) {
  const threadId = ThreadId.make("thread_provider_turn_start_projection_failure");
  const runId = RunId.make("run_provider_turn_start_projection_failure");
  const attemptId = RunAttemptId.make("attempt_provider_turn_start_projection_failure");
  const rootNodeId = NodeId.make("node_provider_turn_start_projection_failure");
  const providerThreadId = ProviderThreadId.make(
    "provider_thread_provider_turn_start_projection_failure",
  );
  const providerSessionId = ProviderSessionId.make(
    "provider_session_provider_turn_start_projection_failure",
  );
  const messageId = MessageId.make("message_provider_turn_start_projection_failure");
  const modelSelection: ModelSelection = {
    instanceId: ProviderInstanceId.make("codex"),
    model: "gpt-5.4",
  };
  const checkpointScopeId = CheckpointScopeId.make(
    "checkpoint_scope_provider_turn_start_projection_failure",
  );
  let projection = {
    thread: {
      id: threadId,
      projectId: ProjectId.make("project_provider_turn_start_projection_failure"),
      branch: "feature/restore",
      worktreePath,
    },
    runs: [
      {
        id: runId,
        threadId,
        providerInstanceId: modelSelection.instanceId,
        modelSelection,
        status: "starting",
        rootNodeId,
        activeAttemptId: attemptId,
        providerThreadId,
        userMessageId: messageId,
        ordinal: 2,
      },
    ],
    nodes: [{ id: rootNodeId, checkpointScopeId }],
    attempts: [{ id: attemptId }],
    providerThreads: [{ id: providerThreadId, providerSessionId }],
    messages: [{ id: messageId, text: "Continue", attachments: [] }],
    checkpointScopes: [{ id: checkpointScopeId }],
    contextHandoffs: [],
    contextTransfers: [],
    turnItems: [],
  } as unknown as OrchestrationV2ThreadProjection;
  let projectionReadCount = 0;
  const writeIfRunCurrent = vi.fn(
    ({ events }: Parameters<EventSink.EventSinkV2["Service"]["writeIfRunCurrent"]>[0]) =>
      Effect.sync(() => {
        for (const event of events)
          projection = ProjectionStore.applyToProjection(projection, event);
        return { committed: true, storedEvents: [] };
      }),
  );
  const startRootRun = vi.fn(() => Effect.void);
  const recreateCheckout = vi.fn<RecreateCheckout>();
  const recreationLayer = observedRecreationLayer(native, recreateCheckout);
  const layer = ProviderTurnStart.layer.pipe(
    Layer.provide(ContinuityAdmissionTestkit.layerAbsent),
    Layer.provide(ControlAdmission.layer),
    Layer.provide(
      Layer.mock(RestartPreparation.RestartPreparation)({ assertThreadOpen: () => Effect.void }),
    ),
    Layer.provide(
      Layer.mock(ProviderSwitchService.ProviderSwitchServiceV2)({
        plan: () =>
          Effect.succeed({
            instanceChanged: false,
            modelChanged: false,
            targetProviderThreadId: null,
            releaseProviderSessionIds: [],
            transition: { type: "reuse" },
          }),
      }),
    ),
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(ContextHandoffService.ContextHandoffServiceV2)({}),
        Layer.mock(EventSink.EventSinkV2)({
          writeIfRunCurrent,
          readStoppedRuntime: () => Effect.succeed(null),
          hasStoppedRuntime: () => Effect.succeed(false),
          assertRuntimeStopStartAllowed: () => Effect.void,
          assertImportedHistoryStartAllowed: () => Effect.void,
        }),
        IdAllocator.layer,
        Layer.succeed(FileSystem.FileSystem, {
          exists: () => Effect.succeed(false),
          realPath: (target: string) =>
            normalize(target) in realPaths
              ? Effect.succeed(realPaths[normalize(target)])
              : Effect.fail(
                  PlatformError.systemError({
                    _tag: "NotFound",
                    module: "FileSystem",
                    method: "realPath",
                    pathOrDescriptor: target,
                  }),
                ),
        } as never),
        layerWorktreeLocation(native?.worktreesDir),
        Layer.mock(ProjectService.ProjectService)({
          getById: () =>
            Effect.succeedSome({
              workspaceRoot: native?.projectRoot ?? "/tmp/provider-turn-start-project",
            } as never),
        }),
        Layer.mock(ProjectionStore.ProjectionStoreV2)({
          getThreadRecords: () => {
            projectionReadCount += 1;
            return Effect.succeed(projection);
          },
          getTurnStartContext: () => {
            projectionReadCount += 1;
            return Effect.succeed({
              ...projection,
              hasConversation: projection.messages.some(
                (m) =>
                  m.role === "user" &&
                  (m.text.trim().toLowerCase() !== "/compact" || m.attachments.length > 0),
              ),
            });
          },
          getRuntimeRecoveryProjection: () => {
            projectionReadCount += 1;
            return Effect.fail(
              new ProjectionStore.ProjectionStoreReadError({
                threadId,
                cause: "simulated inherited-background projection failure",
              }),
            );
          },
        }),
        Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
          reconcileThreadBindingPublication: () => Effect.succeed(null),
          awaitThreadRetirement: () => Effect.void,
          hasRetiredWriter: () => Effect.succeed(false),
        }),
        Layer.mock(ProviderAuthService.ProviderAuthService)({
          tryHandlePromptCommand: () => Effect.succeed(false),
        }),
        Layer.mock(RunExecutionService.RunExecutionServiceV2)({ startRootRun }),
        ServerSettings.layerTest(),
        Layer.mock(ProviderInstanceRegistry.ProviderInstanceRegistry)({
          getCatalogSource: () => undefined,
        }),
        Layer.mock(RuntimePolicy.RuntimePolicyV2)({}),
      ),
    ),
    Layer.provideMerge(recreationLayer),
  );

  const start = Effect.gen(function* () {
    return yield* (yield* ProviderTurnStart.ProviderTurnStartServiceV2)
      .start({ threadId, runId })
      .pipe(Effect.flip);
  }).pipe(
    Effect.provide(
      layer.pipe(
        Layer.provideMerge(
          Layer.merge(lifecycleLayer ?? Layer.empty, provenanceLayer ?? Layer.empty),
        ),
      ),
    ),
  );
  return {
    start,
    recreateCheckout,
    writeIfRunCurrent,
    startRootRun,
    projectionReads: () => projectionReadCount,
  };
}

effectIt.effect(
  "holds managed missing-checkout recreation before prune or creation when registry invalidation is unavailable",
  () =>
    Effect.gen(function* () {
      const hold = Layer.mock(WorkspaceLifecycle.WorkspaceLifecycle)({
        resume: () => Effect.fail(new LifecycleError({ reason: "unavailable" })),
      });
      const harness = startWithMissingWorktree(
        `${managedWorktreesDir}/project/held`,
        undefined,
        hold,
      );
      yield* harness.start;
      expect(harness.recreateCheckout).not.toHaveBeenCalled();
      expect(harness.startRootRun).not.toHaveBeenCalled();
    }),
);

effectIt.effect.each([
  {
    label: "<managed>/<repo>/<branch>",
    worktreePath: `${managedWorktreesDir}/project/feature-restore`,
    recreated: true,
  },
  {
    label: "the real path of a symlinked managed folder",
    worktreePath: "/real-worktrees/project/feature-restore",
    realPaths: { [managedWorktreesDir]: "/real-worktrees", "/real-worktrees": "/real-worktrees" },
    recreated: true,
  },
  {
    label: "a path outside the managed folder",
    worktreePath: "/tmp/provider-turn-start-project/.git/hooks",
    recreated: false,
  },
  {
    label: "a symlink inside the managed folder that leads out",
    worktreePath: `${managedWorktreesDir}/project/hooks`,
    realPaths: {
      [managedWorktreesDir]: managedWorktreesDir,
      [`${managedWorktreesDir}/project`]: "/tmp/provider-turn-start-project/.git",
    },
    recreated: false,
  },
  {
    label: "a folder nested below a worktree",
    worktreePath: `${managedWorktreesDir}/project/feature-restore/link/hooks`,
    recreated: false,
  },
  {
    label: "'..' out of the managed folder",
    worktreePath: `${managedWorktreesDir}/project/../../outside`,
    recreated: false,
  },
  {
    // Left raw on purpose: git follows `link` before applying `..`.
    label: "'..' after a symlink inside the managed folder",
    worktreePath: `${managedWorktreesDir}/project/x/link/../../hooks`,
    realPaths: {
      [managedWorktreesDir]: managedWorktreesDir,
      [`${managedWorktreesDir}/project`]: `${managedWorktreesDir}/project`,
      [`${managedWorktreesDir}/project/x`]: `${managedWorktreesDir}/project/x`,
      [`${managedWorktreesDir}/project/x/link`]: "/tmp/provider-turn-start-project/.git/refs/heads",
    },
    recreated: false,
  },
  { label: "a relative path", worktreePath: "project/feature-restore", recreated: false },
])("recreates a missing worktree only at the managed location: $label", (input) =>
  Effect.gen(function* () {
    const f = yield* startRecreationFixture;
    const remap = (value: string) =>
      value
        .replace(managedWorktreesDir, `${f.root}/managed`)
        .replace("/real-worktrees", `${f.root}/real-worktrees`)
        .replace("/tmp/provider-turn-start-project", f.repo);
    const worktreePath = remap(input.worktreePath);
    const realPaths = Object.fromEntries(
      Object.entries(
        input.realPaths ?? {
          [managedWorktreesDir]: managedWorktreesDir,
          [`${managedWorktreesDir}/project`]: `${managedWorktreesDir}/project`,
        },
      ).map(([from, to]) => [remap(from), remap(to)]),
    );
    let prior: string | undefined;
    if (input.recreated) {
      yield* f.fs.makeDirectory(worktreePath.slice(0, worktreePath.lastIndexOf("/")), {
        recursive: true,
      });
      yield* nativeFixtureOperation(() =>
        nativeGit(f.repo, ["worktree", "move", f.checkout, worktreePath]),
      );
      prior = checkoutIncarnation(
        yield* nativeFixtureOperation(() => checkoutBinding(worktreePath)),
      );
      yield* f.fs.remove(worktreePath, { recursive: true });
    }
    const harness = startWithMissingWorktree(worktreePath, realPaths, undefined, undefined, {
      projectRoot: f.repo,
      baseDir: `${f.root}/state`,
      worktreesDir: remap(managedWorktreesDir),
    });
    yield* harness.start;
    expect(harness.recreateCheckout).toHaveBeenCalledTimes(input.recreated ? 1 : 0);
    if (input.recreated) {
      expect(
        checkoutIncarnation(yield* nativeFixtureOperation(() => checkoutBinding(worktreePath))),
      ).not.toBe(prior);
      expect(
        (yield* nativeFixtureOperation(() => nativeGit(worktreePath, ["rev-parse", "HEAD"])))
          .toString()
          .trim(),
      ).toBe(f.source.head);
      expect(
        (yield* nativeFixtureOperation(() => nativeGit(worktreePath, ["symbolic-ref", "HEAD"])))
          .toString()
          .trim(),
      ).toBe("refs/heads/feature/restore");
      expect(harness.writeIfRunCurrent).toHaveBeenCalledWith(
        expect.objectContaining({
          checkoutRecreation: { worktreePath, branch: "feature/restore" },
        }),
      );
    }
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

function makeLocalCommandHarness(input: {
  readonly conversationLayer?: Layer.Layer<ConversationPolicy.ConversationPolicy>;
  readonly conversationDriver?: string;
  readonly continuityRequirement?: ContinuityRequirement;
  readonly text: string;
  readonly driver?: "codex" | "claudeAgent";
  readonly goal?: OrchestrationV2ThreadProjection["providerThreads"][number]["goal"];
  readonly worktreePath?: string;
  readonly worktreesDir?: string;
  readonly fileSystem?: FileSystem.FileSystem;
  readonly recreation?: RecreationFixtureBinding;
  readonly previousNativeSession?: boolean;
  readonly previousMessages?: ReadonlyArray<string>;
  readonly keepWarm?: boolean;
  readonly liveKeepWarmSession?: boolean;
  readonly plannedResume?: "same" | "changed";
  readonly nativeResume?: "same" | "changed" | "missing";
  readonly handoffRecovery?: {
    readonly compact?: ProviderAdapter.ProviderAdapterV2SessionRuntime["compactForContextHandoff"];
    readonly usedTokens?: number;
  };
  readonly effortProof?: {
    readonly selection?: ModelSelection;
    readonly models: ReadonlyArray<ServerProviderModel>;
    readonly getSettings: ServerSettings.ServerSettingsService["Service"]["getSettings"];
    readonly beforeDispatch?: Effect.Effect<void>;
    readonly duringHandoff?: Effect.Effect<void>;
  };
  readonly logoutFailure?: string;
  readonly openFailure?: unknown;
  readonly retirementFailure?: unknown;
  readonly ensureThreadFailure?: unknown;
  readonly historyReadFailureAfterFallback?: unknown;
  readonly unknownResumeBinding?: boolean;
  readonly resumeFailure?: unknown;
  readonly interruptOpen?: boolean;
  readonly interruptRunBeforeOpenFailure?: boolean;
  readonly writeFailure?: unknown;
  readonly failReadsAfterRunning?: boolean;
  readonly previousTurn?: {
    readonly runStatus: "failed" | "completed";
    readonly turnStatus: "running" | "completed" | "interrupted";
    readonly differentThread?: boolean;
    readonly liveSession?: boolean;
    readonly interruptFailure?: boolean;
    readonly interruptCause?: unknown;
    readonly interruptCurrentRun?: boolean;
  };
}) {
  const now = DateTime.makeUnsafe("2026-09-04T12:00:00Z");
  const threadId = ThreadId.make("thread-native-account-command");
  const runId = RunId.make("run-native-account-command");
  const rootNodeId = NodeId.make("root-native-account-command");
  const attemptId = RunAttemptId.make("attempt-native-account-command");
  const providerThreadId = ProviderThreadId.make("new-provider-thread");
  const providerSessionId = ProviderSessionId.make("new-provider-session");
  const oldProviderThreadId = ProviderThreadId.make("existing-native-provider-thread");
  const oldInstanceId = ProviderInstanceId.make("antigravity-personal");
  const newInstanceId = ProviderInstanceId.make("codex-personal");
  const checkpointScopeId = CheckpointScopeId.make("scope-native-account-command");
  const messageId = MessageId.make(
    input.keepWarm ? "work-mode:thread:generation" : "message-native-account-command",
  );
  const run: OrchestrationV2ThreadProjection["runs"][number] = {
    id: runId,
    threadId,
    ordinal: 2,
    providerInstanceId: newInstanceId,
    modelSelection: input.effortProof?.selection ?? { instanceId: newInstanceId, model: "gpt-5.4" },
    providerThreadId,
    userMessageId: messageId,
    rootNodeId,
    activeAttemptId: attemptId,
    status: "starting",
    requestedAt: now,
    startedAt: null,
    completedAt: null,
    checkpointId: null,
    contextHandoffId: null,
  };
  const providerThread: OrchestrationV2ThreadProjection["providerThreads"][number] = {
    id: providerThreadId,
    driver: ProviderDriverKind.make(input.conversationDriver ?? input.driver ?? "codex"),
    providerInstanceId: newInstanceId,
    providerSessionId,
    appThreadId: threadId,
    ownerNodeId: null,
    nativeThreadRef:
      input.plannedResume === undefined &&
      (input.nativeResume === undefined || input.nativeResume === "missing")
        ? null
        : {
            driver: ProviderDriverKind.make("codex"),
            nativeId: "planned-native-thread",
            strength: "strong",
          },
    nativeConversationHeadRef: null,
    ...("goal" in input ? { goal: input.goal } : {}),
    ...(input.handoffRecovery === undefined
      ? {}
      : {
          contextUsage: {
            usedTokens: input.handoffRecovery.usedTokens ?? 195_000,
            maxTokens: 200_000,
          },
        }),
    status: "not_loaded",
    firstRunOrdinal: 2,
    lastRunOrdinal: 2,
    handoffIds: [],
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
  };
  const message: OrchestrationV2ThreadProjection["messages"][number] = {
    id: messageId,
    threadId,
    runId,
    nodeId: rootNodeId,
    role: "user",
    text: input.text,
    attachments: [],
    streaming: false,
    createdBy: "user",
    creationSource: "web",
    createdAt: now,
    updatedAt: now,
  };
  const providerInstances = new Map<ProviderInstanceId, ProviderInstance>();
  for (const instanceId of new Set([
    newInstanceId,
    run.modelSelection.instanceId,
    ProviderInstanceId.make("codex-work"),
  ])) {
    const driverKind = ProviderDriverKind.make(input.conversationDriver ?? "codex");
    providerInstances.set(instanceId, {
      instanceId,
      driverKind,
      continuationIdentity: { driverKind, continuationKey: instanceId },
      displayName: undefined,
      enabled: true,
      snapshot: {
        getSnapshot: Effect.succeed({
          instanceId,
          driver: driverKind,
          enabled: true,
          installed: true,
          status: "ready",
          auth: { status: "authenticated" },
          checkedAt: "2026-09-04T12:00:00Z",
          version: null,
          models: input.effortProof?.models ?? [],
          slashCommands: [],
          skills: [],
        } satisfies ServerProvider),
        refresh: Effect.die("Turn-start fixtures must not refresh providers."),
        resolveMaintenance: () => Effect.die("Turn-start fixtures must not resolve maintenance."),
        streamChanges: Stream.empty,
        applyUsageLimits: () => Effect.die("Turn-start fixtures must not apply usage limits."),
      },
      get orchestrationAdapter(): ProviderInstance["orchestrationAdapter"] {
        throw new Error("Turn-start fixtures use their provided session manager.");
      },
      get textGeneration(): ProviderInstance["textGeneration"] {
        throw new Error("Turn-start fixtures must not generate text.");
      },
    });
  }
  let projection: OrchestrationV2ThreadProjection = {
    thread: {
      id: threadId,
      activeProviderThreadId: providerThreadId,
      projectId: ProjectId.make("project-native-account-command"),
      title: "Native account command fixture",
      createdBy: "user",
      creationSource: "web",
      providerInstanceId: run.providerInstanceId,
      modelSelection: run.modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: input.worktreePath === undefined ? null : "feature/restore",
      worktreePath: input.worktreePath ?? null,
      lineage: { rootThreadId: threadId, parentThreadId: null, relationshipToParent: null },
      forkedFrom: null,
      createdAt: now,
      updatedAt: now,
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      lastVisitedAt: now,
      deletedAt: null,
    },
    runs: [
      ...(input.previousNativeSession
        ? [
            {
              ...run,
              id: RunId.make("previous-native-run"),
              ordinal: 1,
              status: "completed" as const,
              providerInstanceId: oldInstanceId,
              providerThreadId: oldProviderThreadId,
            },
          ]
        : []),
      run,
    ],
    attempts: [
      {
        id: attemptId,
        runId,
        rootNodeId,
        attemptOrdinal: 1,
        providerInstanceId: newInstanceId,
        providerThreadId,
        providerTurnId: null,
        reason: "initial",
        status: "pending",
        startedAt: null,
        completedAt: null,
      },
    ],
    nodes: [
      {
        id: rootNodeId,
        threadId,
        runId,
        parentNodeId: null,
        rootNodeId,
        kind: "root_turn",
        status: "pending",
        countsForRun: true,
        providerThreadId,
        providerTurnId: null,
        nativeItemRef: null,
        runtimeRequestId: null,
        checkpointScopeId,
        startedAt: null,
        completedAt: null,
      },
    ],
    providerThreads: [
      ...(input.previousNativeSession
        ? [
            {
              ...providerThread,
              id: oldProviderThreadId,
              providerInstanceId: oldInstanceId,
              driver: ProviderDriverKind.make("antigravity"),
              lastRunOrdinal: 1,
              nativeThreadRef: {
                driver: ProviderDriverKind.make("antigravity"),
                nativeId: "existing-session",
                strength: "strong" as const,
              },
            },
          ]
        : []),
      providerThread,
    ],
    messages: [
      ...(input.previousMessages ?? []).map((text, index) => ({
        ...message,
        id: MessageId.make(`previous-message-${index}`),
        text,
      })),
      {
        ...message,
        ...(input.keepWarm
          ? { createdBy: "system" as const, creationSource: "server" as const }
          : {}),
      },
    ],
    checkpointScopes: [
      {
        id: checkpointScopeId,
        threadId,
        runId,
        nodeId: rootNodeId,
        parentScopeId: null,
        providerThreadId,
        kind: "root_run",
        ordinalWithinParent: 0,
        advancesAppRunCount: true,
        cwd: "/tmp/native-account-command",
        createdAt: now,
      },
    ],
    providerSessions:
      "retirementFailure" in input
        ? [
            {
              id: providerSessionId,
              driver: providerThread.driver,
              providerInstanceId: newInstanceId,
            } as OrchestrationV2ThreadProjection["providerSessions"][number],
          ]
        : [],
    providerTurns: [],
    contextHandoffs:
      input.effortProof?.duringHandoff === undefined
        ? []
        : [
            {
              id: ContextHandoffId.make("effort-handoff"),
              threadId,
              targetRunId: runId,
              fromProviderThreadIds: [oldProviderThreadId],
              toProviderThreadId: providerThreadId,
              coveredRunOrdinals: { from: 1, to: 1 },
              strategy: "full_thread_summary",
              status: "ready",
              summaryMessageId: null,
              summaryText: "Prior synthetic account context",
              createdByProviderInstanceId: null,
              createdAt: now,
              updatedAt: now,
            },
          ],
    contextTransfers: [],
    turnItems: [],
    visibleTurnItems: [],
    runtimeRequests: [],
    subagents: [],
    plans: [],
    checkpoints: [],
    updatedAt: now,
  };
  if (
    "historyReadFailureAfterFallback" in input ||
    input.unknownResumeBinding === true ||
    "resumeFailure" in input
  ) {
    const nativeThreadRef = {
      driver: providerThread.driver,
      nativeId: "native-resume-thread",
      strength: "strong" as const,
    };
    projection = {
      ...projection,
      providerThreads: projection.providerThreads.map((candidate) =>
        candidate.id === providerThreadId ? { ...candidate, nativeThreadRef } : candidate,
      ),
    };
  }
  const previousTurnId = ProviderTurnId.make("previous-provider-turn");
  if (input.previousTurn !== undefined) {
    const previousRunId = RunId.make("previous-failed-run");
    const previousAttemptId = RunAttemptId.make("previous-failed-attempt");
    const previousProviderThreadId = input.previousTurn.differentThread
      ? oldProviderThreadId
      : providerThreadId;
    projection = {
      ...projection,
      runs: [
        {
          ...run,
          id: previousRunId,
          ordinal: 1,
          status: input.previousTurn.runStatus,
          rootNodeId: NodeId.make("previous-root"),
          activeAttemptId: previousAttemptId,
          providerThreadId: previousProviderThreadId,
          completedAt: now,
        },
        run,
      ],
      providerThreads: [
        {
          ...providerThread,
          nativeThreadRef: {
            driver: providerThread.driver,
            nativeId: "existing-native-conversation",
            strength: "strong",
          },
        },
      ],
      providerTurns: [
        {
          id: previousTurnId,
          providerThreadId: previousProviderThreadId,
          nodeId: NodeId.make("previous-root"),
          runAttemptId: previousAttemptId,
          ordinal: 1,
          nativeTurnRef: null,
          status: input.previousTurn.turnStatus,
          startedAt: now,
          completedAt: input.previousTurn.turnStatus === "running" ? null : now,
        },
      ],
    };
  }
  let providerBusy =
    input.previousTurn?.runStatus === "failed" &&
    input.previousTurn.turnStatus === "running" &&
    !input.previousTurn.differentThread &&
    input.previousTurn.liveSession !== false;
  const interruptTurn = vi.fn((request: ProviderAdapter.ProviderAdapterV2InterruptInput) =>
    Effect.gen(function* () {
      expect(request.providerTurnId).toBe(previousTurnId);
      expect(request.providerThread.id).toBe(providerThreadId);
      if (
        input.previousTurn?.interruptFailure ||
        input.previousTurn?.interruptCause !== undefined
      ) {
        return yield* new ProviderAdapter.ProviderAdapterInterruptError({
          driver: providerThread.driver,
          providerThreadId,
          providerTurnId: request.providerTurnId,
          cause: input.previousTurn.interruptCause ?? "provider interrupt failed",
        });
      }
      providerBusy = false;
      if (input.previousTurn?.interruptCurrentRun) interruptRun();
    }),
  );
  const events: Array<OrchestrationV2DomainEvent> = [];
  const reconcilePublication = vi.fn(() => null);
  const interruptRun = () => {
    projection = {
      ...projection,
      runs: projection.runs.map((candidate) =>
        candidate.id === runId
          ? { ...candidate, status: "interrupted", completedAt: now }
          : candidate,
      ),
    };
  };
  const ensureThread = vi.fn(() =>
    Effect.sync(() => {
      if (input.interruptRunBeforeOpenFailure === true) interruptRun();
    }).pipe(
      Effect.andThen(
        Effect.fail(
          new ProviderAdapter.ProviderAdapterEventStreamError({
            driver: providerThread.driver,
            providerSessionId,
            cause: input.ensureThreadFailure,
          }),
        ),
      ),
    ),
  );
  const unknownResumeFailure = new ProviderAdapter.ProviderRuntimeBindingError({
    driver: providerThread.driver,
    detail:
      "Failed to establish the actual provider runtime binding; native effects must not be replayed.",
    cause: new ProviderAdapter.ProviderRuntimeBindingError({
      driver: providerThread.driver,
      detail: "Codex has an unconfirmed native effect; another resume is not safe.",
    }),
  });
  const resumeFallbackSession = {
    driver: providerThread.driver,
    resumeThread: () =>
      Effect.fail(
        input.unknownResumeBinding === true
          ? new ProviderAdapter.ProviderAdapterEventStreamError({
              driver: providerThread.driver,
              providerSessionId,
              cause: unknownResumeFailure,
            })
          : new ProviderAdapter.ProviderAdapterEventStreamError({
              driver: providerThread.driver,
              providerSessionId,
              cause: input.resumeFailure ?? "native thread is gone",
            }),
      ),
    ensureThread: vi.fn(() => Effect.succeed(providerThread)),
  };
  const dispatched: Array<ProviderAdapter.ProviderAdapterV2TurnInput> = [];
  const successEnsure = vi.fn(() => Effect.succeed(providerThread));
  const open = vi.fn(
    (
      _openInput: Parameters<ProviderSessionManager.ProviderSessionManagerV2["Service"]["open"]>[0],
    ) =>
      input.interruptOpen === true
        ? Effect.interrupt
        : "historyReadFailureAfterFallback" in input ||
            input.unknownResumeBinding === true ||
            "resumeFailure" in input
          ? Effect.succeed(resumeFallbackSession as never)
          : "ensureThreadFailure" in input
            ? Effect.succeed({ driver: providerThread.driver, ensureThread } as never)
            : "openFailure" in input
              ? Effect.sync(() => {
                  if (input.interruptRunBeforeOpenFailure === true) interruptRun();
                }).pipe(
                  Effect.andThen(
                    Effect.fail(
                      new ProviderSessionManager.ProviderSessionOpenError({
                        instanceId: newInstanceId,
                        providerSessionId,
                        cause: input.openFailure,
                      }),
                    ),
                  ),
                )
              : input.failReadsAfterRunning === true ||
                  input.effortProof !== undefined ||
                  input.previousTurn !== undefined ||
                  input.plannedResume !== undefined ||
                  input.nativeResume !== undefined
                ? Effect.succeed({
                    driver: providerThread.driver,
                    providerSession: {
                      id: providerSessionId,
                      driver: providerThread.driver,
                      providerInstanceId: newInstanceId,
                      status: "ready",
                      cwd: "/tmp/native-account-command",
                      model: null,
                      capabilities: CodexProviderCapabilitiesV2,
                      createdAt: now,
                      updatedAt: now,
                      lastError: null,
                    },
                    ensureThread: successEnsure,
                    resumeThread: () =>
                      Effect.succeed(
                        input.previousTurn !== undefined
                          ? projection.providerThreads[0]
                          : {
                              ...providerThread,
                              nativeThreadRef: {
                                driver: providerThread.driver,
                                nativeId:
                                  input.plannedResume === "changed" ||
                                  input.nativeResume === "changed"
                                    ? "different-native-thread"
                                    : "planned-native-thread",
                                strength: "strong" as const,
                              },
                            },
                      ),
                    ...(input.handoffRecovery?.compact === undefined
                      ? {}
                      : { compactForContextHandoff: input.handoffRecovery.compact }),
                    startTurn: (value: ProviderAdapter.ProviderAdapterV2TurnInput) =>
                      Effect.sync(() => {
                        dispatched.push(value);
                      }),
                  } as never)
                : Effect.die("A local command must not open a native session."),
  );
  const startRootRun = vi.fn<
    (input: RunExecutionService.RunExecutionServiceV2StartRootRunInput) => Effect.Effect<void>
  >((startInput) =>
    input.effortProof !== undefined
      ? Effect.gen(function* () {
          yield* input.effortProof!.beforeDispatch ?? Effect.void;
          yield* startInput.session
            .startTurn({
              appThread: startInput.appThread,
              threadId,
              runId,
              runOrdinal: startInput.run.ordinal,
              providerTurnOrdinal: startInput.providerTurnOrdinal,
              attemptId,
              rootNodeId,
              providerThread: startInput.providerThread,
              message: startInput.message,
              modelSelection: startInput.modelSelection,
              runtimePolicy: startInput.runtimePolicy,
            })
            .pipe(Effect.orDie);
        })
      : input.failReadsAfterRunning === true ||
          input.plannedResume !== undefined ||
          input.nativeResume !== undefined ||
          input.previousTurn !== undefined
        ? Effect.suspend(() =>
            providerBusy ? Effect.die("The previous provider turn is still running.") : Effect.void,
          )
        : Effect.die("A local command must not start a native turn."),
  );
  const failReadIfRunning = Effect.suspend(() =>
    input.failReadsAfterRunning === true &&
    projection.runs.find((candidate) => candidate.id === runId)?.status === "running"
      ? Effect.fail(
          new ProjectionStore.ProjectionStoreReadError({ threadId, cause: "database unavailable" }),
        )
      : Effect.void,
  );
  const tryHandlePromptCommand = vi.fn(() =>
    input.logoutFailure === undefined
      ? Effect.succeed(true)
      : Effect.fail(
          new ProviderSetupError({
            instanceId: oldInstanceId,
            operation: "logout",
            detail: input.logoutFailure,
          }),
        ),
  );
  const writeIfRunCurrent = vi.fn(({ events: incoming, activeAttemptId, expectedStatus }) =>
    "writeFailure" in input
      ? Effect.fail(
          new EventSink.EventSinkWriteError({
            eventCount: incoming.length,
            cause: input.writeFailure,
          }),
        )
      : Effect.sync(() => {
          const current = projection.runs.find((candidate) => candidate.id === runId);
          const committed =
            current !== undefined &&
            current.activeAttemptId === activeAttemptId &&
            current.status === expectedStatus;
          if (committed) {
            for (const event of incoming) {
              expect(isDomainEvent(event)).toBe(true);
              events.push(event);
              projection = ProjectionStore.applyToProjection(projection, event);
            }
          }
          return { committed, storedEvents: [] };
        }),
  );
  const resolveRuntimePolicy = vi.fn(
    ({ thread }: Parameters<RuntimePolicy.RuntimePolicyV2Shape["resolve"]>[0]) =>
      Effect.succeed({ cwd: thread.worktreePath ?? "/tmp/native-account-command" } as never),
  );
  const recreateCheckout = vi.fn<RecreateCheckout>();
  const getThreadRecords = vi.fn<ProjectionStore.ProjectionStoreV2Shape["getThreadRecords"]>(() =>
    Effect.succeed(projection),
  );
  const getTurnStartContext = vi.fn(() =>
    Effect.succeed({
      ...projection,
      hasConversation: projection.messages.some(
        (m) =>
          m.role === "user" &&
          (m.text.trim().toLowerCase() !== "/compact" || m.attachments.length > 0),
      ),
    }),
  );
  const recreationLayer = observedRecreationLayer(input.recreation, recreateCheckout);
  const layer = ProviderTurnStart.layer.pipe(
    Layer.provide(input.conversationLayer ?? Layer.empty),
    Layer.provide(ContinuityAdmissionTestkit.layerWithRequirement(input.continuityRequirement)),
    Layer.provide(ControlAdmission.layer),
    Layer.provide(
      Layer.mock(RestartPreparation.RestartPreparation)({ assertThreadOpen: () => Effect.void }),
    ),
    Layer.provide(
      Layer.mock(ProviderSwitchService.ProviderSwitchServiceV2)({
        plan: () =>
          Effect.succeed({
            instanceChanged: false,
            modelChanged: false,
            targetProviderThreadId: null,
            releaseProviderSessionIds: "retirementFailure" in input ? [providerSessionId] : [],
            transition: { type: "reuse" },
          }),
      }),
    ),
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(PlannedUpdateContinuity.PlannedUpdateContinuity)({
          allowWorkStart: () => Effect.succeed(input.plannedResume !== undefined),
        }),
        Layer.mock(ContextHandoffService.ContextHandoffServiceV2)({
          prepareProviderHandoff: () => Effect.die("history read must fail first"),
        }),
        Layer.mock(EventSink.EventSinkV2)({
          writeIfRunCurrent,
          readStoppedRuntime: () => Effect.succeed(null),
          hasStoppedRuntime: () => Effect.succeed(false),
          assertRuntimeStopStartAllowed: () => Effect.void,
          assertImportedHistoryStartAllowed: () => Effect.void,
          write: () => (input.effortProof?.duringHandoff ?? Effect.void).pipe(Effect.as([])),
        }),
        IdAllocator.layer,
        layerWorktreeLocation(input.worktreesDir),
        input.fileSystem === undefined
          ? FileSystem.layerNoop({ exists: () => Effect.succeed(true) })
          : Layer.succeed(FileSystem.FileSystem, input.fileSystem),
        Layer.mock(ProjectService.ProjectService)({
          getById: () =>
            Effect.succeedSome({
              workspaceRoot: input.recreation?.projectRoot ?? "/tmp/native-account-project",
            } as never),
        }),
        Layer.mock(ProjectionStore.ProjectionStoreV2)({
          getThreadRecords,
          getTurnStartContext,
          getRuntimeRecoveryProjection: () =>
            Effect.as(failReadIfRunning, {
              ...projection,
              hasConversation: projection.messages.some(
                (m) =>
                  m.role === "user" &&
                  (m.text.trim().toLowerCase() !== "/compact" || m.attachments.length > 0),
              ),
            }),
          getTurnStartHistory: () =>
            input.effortProof === undefined
              ? Effect.fail(
                  new ProjectionStore.ProjectionStoreReadError({
                    threadId,
                    cause: input.historyReadFailureAfterFallback,
                  }),
                )
              : Effect.succeed([]),
        }),
        Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
          reconcileThreadBindingPublication: () => Effect.sync(reconcilePublication),
          awaitThreadRetirement: () => Effect.void,
          hasRetiredWriter: () => Effect.succeed(false),
          open,
          detach: ({ providerSessionId, requireWriterRetirement }) => {
            expect(requireWriterRetirement).toBe(true);
            return Effect.fail(
              new ProviderSessionManager.ProviderSessionReleaseError({
                providerSessionId,
                reason: "manual_shutdown",
                cause: input.retirementFailure,
              }),
            );
          },
          get: () =>
            Effect.succeed(
              input.liveKeepWarmSession
                ? Option.some(resumeFallbackSession as never)
                : input.previousTurn === undefined || input.previousTurn.liveSession === false
                  ? Option.none()
                  : Option.some({
                      driver: providerThread.driver,
                      providerSessionId,
                      interruptTurn,
                    } as unknown as ProviderAdapter.ProviderAdapterV2SessionRuntime),
            ),
        }),
        Layer.mock(ProviderAuthService.ProviderAuthService)({ tryHandlePromptCommand }),
        Layer.mock(RunExecutionService.RunExecutionServiceV2)({ startRootRun }),

        Layer.mock(ServerSettings.ServerSettingsService)({
          getSettings: input.effortProof?.getSettings ?? Effect.succeed(DEFAULT_SERVER_SETTINGS),
        }),
        Layer.mock(ProviderInstanceRegistry.ProviderInstanceRegistry)({
          getCatalogSource: (instanceId) => providerInstances.get(instanceId),
          getInstance: (instanceId) => Effect.succeed(providerInstances.get(instanceId)),
        }),
        Layer.mock(RuntimePolicy.RuntimePolicyV2)({
          resolve: resolveRuntimePolicy,
        }),
      ),
    ),
    Layer.provideMerge(recreationLayer),
  );
  return {
    getThreadRecords,
    getTurnStartContext,
    setProjection: (next: OrchestrationV2ThreadProjection) => {
      projection = next;
    },
    resolveRuntimePolicy,
    recreateCheckout,
    reconcilePublication,
    open,
    fallbackEnsure: resumeFallbackSession.ensureThread,
    ensureThread,
    successEnsure,
    unknownResumeFailure,
    writeIfRunCurrent,
    startRootRun,
    dispatched,
    interruptRun,
    interruptTurn,
    tryHandlePromptCommand,
    events,
    oldInstanceId,
    newInstanceId,
    attemptId,
    projection: () => projection,
    captureStartFailure: Effect.gen(function* () {
      const capture = (yield* ProviderTurnStart.ProviderTurnStartServiceV2).captureStartFailure;
      expect(capture).toBeDefined();
      return yield* capture!({ threadId, runId });
    }).pipe(Effect.provide(layer)),
    start: Effect.gen(function* () {
      yield* (yield* ProviderTurnStart.ProviderTurnStartServiceV2).start({ threadId, runId });
    }).pipe(Effect.provide(layer)),
    startWithRetry: Effect.gen(function* () {
      yield* (yield* ProviderTurnStart.ProviderTurnStartServiceV2).start({
        threadId,
        runId,
        willRetry: true,
      });
    }).pipe(Effect.provide(layer)),
  };
}

effectIt.effect.each(["delivery_pending", "delivery_unknown", "recovery_required"] as const)(
  "keeps a historical continuity fence before provider loading (%s)",
  (reason) =>
    Effect.gen(function* () {
      const harness = makeLocalCommandHarness({
        text: "Continue",
        continuityRequirement: {
          outcome: "hold",
          reason,
          current: null,
          gate: {
            outcome: "hold",
            reason,
            fenceRevision: "9",
            deliveryIds: ["historical-delivery"],
          },
        },
      });
      yield* harness.start;
      expect(harness.open).not.toHaveBeenCalled();
      expect(harness.fallbackEnsure).not.toHaveBeenCalled();
      expect(harness.startRootRun).not.toHaveBeenCalled();
      expect(harness.projection().runs.at(-1)?.status).toBe("failed");
    }),
);

effectIt.effect(
  "refuses a missing continuity binding before a fresh native conversation is created",
  () =>
    Effect.gen(function* () {
      const harness = makeLocalCommandHarness({
        text: "Continue",
        continuityRequirement: {
          outcome: "required",
          owner: {
            environmentId: "environment:mcp-test",
            threadId: "thread-native-account-command",
            providerInstanceId: "codex-personal",
            providerThreadId: "new-provider-thread",
            driver: "codex",
            nativeThreadId: "original-continuity-native",
          } as import("@t3tools/contracts").ContinuityOwner,
          target: {
            goalId: "goal",
            revision: "1",
            generation: "1",
            snapshotDigest: "a".repeat(64),
          },
          gate: { outcome: "clear", fenceRevision: "0" },
        },
      });
      yield* harness.start;
      expect(harness.open).not.toHaveBeenCalled();
      expect(harness.fallbackEnsure).not.toHaveBeenCalled();
      expect(harness.startRootRun).not.toHaveBeenCalled();
    }),
);

effectIt.effect.each(["Continue", "/goal resume"])(
  "holds provider start before opening a native session when registry invalidation is unknown: %s",
  (text) =>
    Effect.gen(function* () {
      const harness = makeLocalCommandHarness({ text });
      harness.tryHandlePromptCommand.mockReturnValue(Effect.succeed(false));
      const result = yield* Effect.exit(
        harness.start.pipe(
          Effect.provide(
            Layer.mock(WorkspaceLifecycle.WorkspaceLifecycle)({
              resume: () => Effect.fail(new LifecycleError({ reason: "unknown" })),
            }),
          ),
        ),
      );
      expect(result._tag).toBe("Failure");
      expect(harness.open).not.toHaveBeenCalled();
      expect(harness.startRootRun).not.toHaveBeenCalled();
      expect(harness.dispatched).toEqual([]);
    }),
);

effectIt.effect("recovers an abandoned failed turn before continuing its native conversation", () =>
  Effect.gen(function* () {
    const harness = makeLocalCommandHarness({
      text: "Continue",
      previousTurn: { runStatus: "failed", turnStatus: "running" },
    });

    yield* harness.start;

    expect(harness.interruptTurn).toHaveBeenCalledOnce();
    expect(harness.projection().runs.at(-1)?.status).toBe("running");
    expect(harness.projection().providerTurns[0]).toMatchObject({
      status: "interrupted",
      completedAt: expect.anything(),
    });
    expect(harness.startRootRun).toHaveBeenCalledOnce();
    expect(harness.startRootRun.mock.calls[0]?.[0].providerThread.nativeThreadRef?.nativeId).toBe(
      "existing-native-conversation",
    );
  }),
);

effectIt.effect.each([
  { runStatus: "failed", turnStatus: "completed" },
  { runStatus: "completed", turnStatus: "completed" },
  { runStatus: "failed", turnStatus: "running", differentThread: true },
  { runStatus: "failed", turnStatus: "running", liveSession: false },
  { runStatus: "failed", turnStatus: "interrupted", interruptFailure: true },
] as const)("continues without interrupting when recovery does not apply: %j", (previousTurn) =>
  Effect.gen(function* () {
    const harness = makeLocalCommandHarness({ text: "Continue", previousTurn });

    yield* harness.start;

    expect(harness.interruptTurn).not.toHaveBeenCalled();
    expect(harness.projection().runs.at(-1)?.status).toBe("running");
  }),
);

effectIt.effect("does not start another turn when abandoned-turn cleanup fails", () =>
  Effect.gen(function* () {
    const harness = makeLocalCommandHarness({
      text: "Continue",
      previousTurn: { runStatus: "failed", turnStatus: "running", interruptFailure: true },
    });

    yield* harness.start;

    expect(harness.interruptTurn).toHaveBeenCalledOnce();
    expect(harness.startRootRun).not.toHaveBeenCalled();
    expect(harness.projection().runs.at(-1)?.status).toBe("failed");
  }),
);

effectIt.effect("retries abandoned-turn cleanup without failing the continuation early", () =>
  Effect.gen(function* () {
    const harness = makeLocalCommandHarness({
      text: "Continue",
      previousTurn: { runStatus: "failed", turnStatus: "running", interruptFailure: true },
    });

    const error = yield* harness.startWithRetry.pipe(Effect.flip);

    expect(error._tag).toBe("ProviderTurnStartError");
    expect(harness.projection().runs.at(-1)?.status).toBe("starting");
    expect(harness.open).not.toHaveBeenCalled();
  }),
);

effectIt.effect("does not start the continuation if recording recovery fails", () =>
  Effect.gen(function* () {
    const harness = makeLocalCommandHarness({
      text: "Continue",
      previousTurn: { runStatus: "failed", turnStatus: "running" },
      writeFailure: new Error("database unavailable"),
    });

    const error = yield* harness.start.pipe(Effect.flip);

    expect(error._tag).toBe("ProviderTurnStartError");
    expect(harness.projection().runs.at(-1)?.status).toBe("starting");
    expect(harness.projection().providerTurns[0]?.status).toBe("running");
    expect(harness.open).not.toHaveBeenCalled();
  }),
);

effectIt.effect("does not resume a continuation stopped while recovery was in flight", () =>
  Effect.gen(function* () {
    const harness = makeLocalCommandHarness({
      text: "Continue",
      previousTurn: { runStatus: "failed", turnStatus: "running", interruptCurrentRun: true },
    });

    yield* harness.start;

    expect(harness.projection().runs.at(-1)?.status).toBe("interrupted");
    expect(harness.open).not.toHaveBeenCalled();
  }),
);

effectIt.effect.each(["cleanup-first", "startup-first"])(
  "coordinates provider startup with %s",
  (order) =>
    Effect.gen(function* () {
      const f = yield* startRecreationFixture;
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const workspaceRoot = f.root;
      const worktreePath = path.join(workspaceRoot, "project", "worktree");
      yield* fileSystem.makeDirectory(path.dirname(worktreePath), { recursive: true });
      yield* nativeFixtureOperation(() =>
        nativeGit(f.repo, ["worktree", "move", f.checkout, worktreePath]),
      );
      const prior = yield* nativeFixtureOperation(() => checkoutBinding(worktreePath));
      const release = yield* Deferred.make<void>();
      const openEntered = yield* Deferred.make<void>();
      const harness = makeLocalCommandHarness({
        text: "Continue",
        worktreePath,
        worktreesDir: workspaceRoot,
        fileSystem,
        recreation: {
          projectRoot: f.repo,
          baseDir: `${f.root}/state`,
          worktreesDir: workspaceRoot,
        },
        failReadsAfterRunning: true,
      });
      expect(Schema.is(OrchestrationV2AppThread)(harness.projection().thread)).toBe(true);
      const open = harness.open.getMockImplementation()!;
      harness.open.mockImplementation((input) =>
        Effect.gen(function* () {
          expect(yield* fileSystem.exists(worktreePath).pipe(Effect.orDie)).toBe(true);
          const head = yield* nativeFixtureOperation(() =>
            nativeGit(worktreePath, ["rev-parse", "HEAD"]),
          ).pipe(Effect.orDie);
          expect(head.toString().trim()).toBe(f.source.head);
          if (order === "cleanup-first") {
            const current = yield* nativeFixtureOperation(() => checkoutBinding(worktreePath)).pipe(
              Effect.orDie,
            );
            expect(checkoutIncarnation(current)).not.toBe(checkoutIncarnation(prior));
          }
          yield* Deferred.succeed(openEntered, undefined);
          if (order === "startup-first") yield* Deferred.await(release);
          return yield* open(input);
        }),
      );
      let cleanupEntered = false;
      const cleanupEffect = withWorkspaceLease(
        worktreePath,
        Effect.gen(function* () {
          cleanupEntered = true;
          if (order === "cleanup-first") yield* Deferred.await(release);
          else {
            expect(harness.projection().providerSessions).toHaveLength(1);
            expect(harness.projection().runs.at(-1)?.status).toBe("running");
          }
          yield* fileSystem.remove(worktreePath, { recursive: true, force: true });
        }),
      );
      const cleanup =
        order === "cleanup-first"
          ? yield* cleanupEffect.pipe(Effect.forkChild({ startImmediately: true }))
          : undefined;
      const startup = yield* harness.start.pipe(Effect.forkChild({ startImmediately: true }));
      if (order === "cleanup-first") expect(harness.open).not.toHaveBeenCalled();
      if (order === "startup-first") yield* Deferred.await(openEntered);
      const laterCleanup =
        order === "startup-first"
          ? yield* cleanupEffect.pipe(Effect.forkChild({ startImmediately: true }))
          : undefined;
      if (order === "startup-first") expect(cleanupEntered).toBe(false);
      yield* Deferred.succeed(release, undefined);
      if (cleanup !== undefined) yield* Fiber.join(cleanup);
      yield* Fiber.join(startup);
      if (laterCleanup !== undefined) yield* Fiber.join(laterCleanup);
      if (order === "cleanup-first") {
        expect(harness.recreateCheckout).toHaveBeenCalledTimes(1);
        expect(harness.recreateCheckout).toHaveBeenCalledWith(
          expect.objectContaining({
            cwd: f.repo,
            source: expect.objectContaining({
              head: f.source.head,
              branch: "refs/heads/feature/restore",
              binding: expect.objectContaining({ path: worktreePath }),
            }),
          }),
        );
        expect(yield* fileSystem.exists(worktreePath)).toBe(true);
      } else {
        expect(harness.recreateCheckout).not.toHaveBeenCalled();
        expect(yield* fileSystem.exists(worktreePath)).toBe(false);
      }
      expect(harness.resolveRuntimePolicy).toHaveBeenCalledWith(
        expect.objectContaining({
          thread: expect.objectContaining({ worktreePath }),
        }),
      );
      expect(harness.open).toHaveBeenCalledWith(
        expect.objectContaining({
          runtimePolicy: expect.objectContaining({ cwd: worktreePath }),
        }),
      );
      expect(harness.startRootRun).toHaveBeenCalledWith(
        expect.objectContaining({
          appThread: expect.objectContaining({ worktreePath }),
          runtimePolicy: expect.objectContaining({ cwd: worktreePath }),
        }),
      );
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

effectIt.effect("reads current startup state after waiting for the workspace lease", () =>
  Effect.gen(function* () {
    const worktreePath = "/tmp/provider-start-fresh-state";
    const release = yield* Deferred.make<void>();
    const readEntered = yield* Deferred.make<void>();
    const harness = makeLocalCommandHarness({
      text: "Continue",
      worktreePath,
      failReadsAfterRunning: true,
    });
    harness.getThreadRecords.mockImplementationOnce(() =>
      Deferred.succeed(readEntered, undefined).pipe(Effect.as(harness.projection())),
    );
    const holder = yield* withWorkspaceLease(worktreePath, Deferred.await(release)).pipe(
      Effect.forkChild({ startImmediately: true }),
    );
    const startup = yield* harness.start.pipe(Effect.forkChild({ startImmediately: true }));
    yield* Deferred.await(readEntered);
    expect(harness.getTurnStartContext).not.toHaveBeenCalled();
    const projection = harness.projection();
    harness.setProjection({
      ...projection,
      runs: projection.runs.map((run) => ({ ...run, status: "running" })),
    });
    yield* Deferred.succeed(release, undefined);
    yield* Fiber.join(holder);
    yield* Fiber.join(startup);
    expect(harness.open).not.toHaveBeenCalled();
    expect(harness.startRootRun).not.toHaveBeenCalled();
  }),
);

// regression: #15784 superseded provider startups kept running
effectIt.effect.each([
  { boundary: "policy resolution", loss: "replaced" },
  { boundary: "session opening", loss: "interrupted" },
])("does not continue startup when $loss during $boundary", ({ boundary, loss }) =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const gate = Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release)));
    const harness = makeLocalCommandHarness({ text: "Continue" });
    const projection = harness.projection();
    const providerThread = projection.providerThreads[0]!;
    const ensureThread = vi.fn(() => Effect.succeed(providerThread));
    const resumeThread = vi.fn(() => Effect.succeed(providerThread));
    const forkThread = vi.fn(() => Effect.succeed(providerThread));
    harness.open.mockReturnValue(
      (boundary === "session opening" ? gate : Effect.void).pipe(
        Effect.as({
          driver: providerThread.driver,
          ensureThread,
          resumeThread,
          forkThread,
        } as never),
      ),
    );
    if (boundary === "policy resolution") {
      harness.resolveRuntimePolicy.mockReturnValue(
        gate.pipe(Effect.as({ cwd: "/tmp/native-account-command" } as never)),
      );
    }

    const startup = yield* harness.start.pipe(Effect.forkChild({ startImmediately: true }));
    yield* Deferred.await(entered);
    const current = harness.projection();
    const replacementId = RunAttemptId.make("replacement-attempt");
    const updated = {
      ...current,
      runs: current.runs.map((candidate) =>
        loss === "replaced"
          ? { ...candidate, activeAttemptId: replacementId }
          : { ...candidate, status: "interrupted" as const, completedAt: current.updatedAt },
      ),
      attempts:
        loss === "replaced"
          ? [...current.attempts, { ...current.attempts[0]!, id: replacementId, attemptOrdinal: 2 }]
          : current.attempts,
    };
    harness.setProjection(updated);
    yield* Deferred.succeed(release, undefined);
    yield* Fiber.join(startup);

    expect(harness.open).toHaveBeenCalledTimes(boundary === "session opening" ? 1 : 0);
    expect(ensureThread).not.toHaveBeenCalled();
    expect(resumeThread).not.toHaveBeenCalled();
    expect(forkThread).not.toHaveBeenCalled();
    expect(harness.startRootRun).not.toHaveBeenCalled();
    expect(harness.writeIfRunCurrent).not.toHaveBeenCalled();
    expect(harness.events).toEqual([]);
    expect(harness.projection()).toEqual(updated);
  }),
);

effectIt.effect("starts another provider in the same checkout while a turn is pending", () =>
  Effect.gen(function* () {
    const worktreePath = "/tmp/provider-start-shared-checkout";
    const turnEntered = yield* Deferred.make<void>();
    const releaseTurn = yield* Deferred.make<void>();
    const first = makeLocalCommandHarness({
      text: "Continue",
      worktreePath,
      failReadsAfterRunning: true,
    });
    const second = makeLocalCommandHarness({
      text: "Continue",
      worktreePath,
      failReadsAfterRunning: true,
    });
    first.startRootRun.mockImplementation(() =>
      Deferred.succeed(turnEntered, undefined).pipe(Effect.andThen(Deferred.await(releaseTurn))),
    );
    const pending = yield* first.start.pipe(Effect.forkChild({ startImmediately: true }));
    yield* Deferred.await(turnEntered);
    yield* second.start;
    expect(second.startRootRun).toHaveBeenCalledOnce();
    expect(second.projection().runs.at(-1)?.status).toBe("running");
    yield* Deferred.succeed(releaseTurn, undefined);
    yield* Fiber.join(pending);
  }),
);

effectIt.effect("terminalizes a starting run when its provider session cannot open", () =>
  Effect.gen(function* () {
    const harness = makeLocalCommandHarness({
      text: "Continue",
      openFailure: new Error("DESCRIPTION is not valid ACP JSON"),
    });

    yield* harness.start;

    expect(harness.open).toHaveBeenCalledOnce();
    expect(harness.startRootRun).not.toHaveBeenCalled();
    expect(harness.writeIfRunCurrent).toHaveBeenCalledWith(
      expect.objectContaining({
        activeAttemptId: harness.attemptId,
        expectedStatus: "starting",
      }),
    );
    const projection = harness.projection();
    expect(projection.runs.at(-1)).toMatchObject({ status: "failed", startedAt: null });
    expect(projection.attempts[0]).toMatchObject({ status: "failed", startedAt: null });
    expect(projection.nodes[0]).toMatchObject({ status: "failed", startedAt: null });
    expect(projection.turnItems).toMatchObject([
      {
        type: "error",
        status: "failed",
        failure: {
          class: "provider_error",
          message: "DESCRIPTION is not valid ACP JSON",
        },
      },
    ]);
  }),
);

effectIt.effect("claim loss refuses local publication lookup before any provider work", () =>
  Effect.gen(function* () {
    const harness = makeLocalCommandHarness({ text: "Continue once" });
    const loss = new EffectClaimLost({ effectId: "effect:before-local-publication" });
    const settleCapturedFailure = yield* harness.captureStartFailure;
    const failed = yield* harness.startWithRetry.pipe(
      Effect.provide(
        Layer.mock(CurrentEffectClaim)({ nativeCustody: true, check: Effect.fail(loss) }),
      ),
      Effect.tapError(settleCapturedFailure),
      Effect.result,
    );
    expect(failed._tag).toBe("Failure");
    if (failed._tag === "Failure") expect(findEffectClaimLost(failed.failure)).toBeDefined();
    expect(harness.reconcilePublication).not.toHaveBeenCalled();
    expect(harness.open).not.toHaveBeenCalled();
    expect(harness.startRootRun).not.toHaveBeenCalled();
    expect(harness.projection().runs.at(-1)?.status).toBe("failed");
    expect(harness.projection().attempts[0]?.status).toBe("failed");
    expect(harness.projection().nodes[0]?.status).toBe("failed");
    expect(
      harness.events.filter(
        (event) => event.type === "run.updated" && event.payload.status === "failed",
      ),
    ).toHaveLength(1);
  }),
);

effectIt.effect.each(["open", "ensure", "resume", "retirement", "recovery"] as const)(
  "terminalizes captured custody loss during %s even when retry is allowed",
  (stage) =>
    Effect.gen(function* () {
      const loss = new EffectClaimLost({ effectId: `effect:original-${stage}` });
      const harness = makeLocalCommandHarness({
        text: "Continue once",
        ...(stage === "open" ? { openFailure: loss } : {}),
        ...(stage === "ensure" ? { ensureThreadFailure: loss } : {}),
        ...(stage === "resume" ? { resumeFailure: loss, previousNativeSession: true } : {}),
        ...(stage === "retirement" ? { retirementFailure: loss } : {}),
        ...(stage === "recovery"
          ? {
              previousTurn: {
                runStatus: "failed" as const,
                turnStatus: "running" as const,
                interruptCause: loss,
              },
            }
          : {}),
      });
      const failed = yield* harness.startWithRetry.pipe(Effect.result);
      expect(failed._tag).toBe("Failure");
      if (failed._tag === "Failure") expect(findEffectClaimLost(failed.failure)).toBeDefined();
      yield* harness.startWithRetry;
      expect(harness.startRootRun).not.toHaveBeenCalled();
      expect(harness.fallbackEnsure).not.toHaveBeenCalled();
      expect(harness.projection().runs.at(-1)?.status).toBe("failed");
      expect(
        harness.projection().attempts.find((attempt) => attempt.id === harness.attemptId)?.status,
      ).toBe("failed");
      expect(
        harness.events.filter(
          (event) => event.type === "run.updated" && event.payload.status === "failed",
        ),
      ).toHaveLength(1);
    }),
);

effectIt.effect("keeps typed retirement uncertainty terminal during retryable opening", () =>
  Effect.gen(function* () {
    const unknown = new ProviderControlOutcomeUnknown({
      driver: "claudeAgent",
      providerSessionId: "original-session",
      nativeThreadId: "original-native",
      operation: "retire",
    });
    const harness = makeLocalCommandHarness({ text: "Continue once", retirementFailure: unknown });
    const failed = yield* harness.startWithRetry.pipe(Effect.result);
    expect(failed._tag).toBe("Failure");
    if (failed._tag === "Failure")
      expect(findProviderControlOutcomeUnknown(failed.failure)).toBeDefined();
    expect(harness.open).not.toHaveBeenCalled();
    expect(harness.startRootRun).not.toHaveBeenCalled();
    expect(harness.projection().runs.at(-1)?.status).toBe("failed");
  }),
);

effectIt.effect.each([false, true])(
  "pre-issue failure retains its captured attempt (successor=%s)",
  (successor) =>
    Effect.gen(function* () {
      const harness = makeLocalCommandHarness({ text: "Prepared native prompt" });
      const original = harness.projection();
      const fail = yield* harness.captureStartFailure;
      const successorAttempt = RunAttemptId.make("attempt:successor-after-native-validation");
      if (successor)
        harness.setProjection({
          ...original,
          runs: original.runs.map((run) => ({ ...run, activeAttemptId: successorAttempt })),
          attempts: [...original.attempts, { ...original.attempts[0]!, id: successorAttempt }],
        });
      yield* fail(new EffectClaimLost({ effectId: "effect:before-native-issue" }));
      yield* fail(new Error("late issue response"));
      expect(harness.open).not.toHaveBeenCalled();
      expect(harness.startRootRun).not.toHaveBeenCalled();
      expect(harness.writeIfRunCurrent).toHaveBeenCalledWith(
        expect.objectContaining({ activeAttemptId: harness.attemptId, expectedStatus: "starting" }),
      );
      if (successor) {
        expect(harness.projection().runs.at(-1)?.activeAttemptId).toBe(successorAttempt);
        expect(harness.projection().runs.at(-1)?.status).toBe("starting");
        expect(harness.events).toEqual([]);
      } else {
        expect(harness.projection().runs.at(-1)?.status).toBe("failed");
        expect(harness.projection().attempts[0]?.status).toBe("failed");
        expect(harness.events.filter((event) => event.type === "run.updated")).toHaveLength(1);
      }
    }),
);

effectIt.effect("leaves the run starting when a session-open failure will be retried", () =>
  Effect.gen(function* () {
    const harness = makeLocalCommandHarness({
      text: "Continue",
      openFailure: new Error("provider session rejected"),
    });

    const error = yield* harness.startWithRetry.pipe(Effect.flip);

    expect(error._tag).toBe("ProviderTurnStartError");
    expect(harness.writeIfRunCurrent).not.toHaveBeenCalled();
    expect(harness.projection().runs.at(-1)?.status).toBe("starting");
  }),
);

effectIt.effect("does not terminalize a provider-session open interruption", () =>
  Effect.gen(function* () {
    const harness = makeLocalCommandHarness({ text: "Continue", interruptOpen: true });

    const exit = yield* Effect.exit(harness.start);

    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") {
      expect(Cause.hasInterruptsOnly(exit.cause)).toBe(true);
    }
    expect(harness.writeIfRunCurrent).not.toHaveBeenCalled();
    expect(harness.startRootRun).not.toHaveBeenCalled();
    expect(harness.projection().runs.at(-1)?.status).toBe("starting");
    expect(harness.events).toEqual([]);
  }),
);

effectIt.effect("does not overwrite a run interrupted while its provider session opens", () =>
  Effect.gen(function* () {
    const harness = makeLocalCommandHarness({
      text: "Continue",
      openFailure: new Error("provider session rejected"),
      interruptRunBeforeOpenFailure: true,
    });

    yield* harness.start;

    expect(harness.writeIfRunCurrent).toHaveBeenCalledOnce();
    expect(harness.startRootRun).not.toHaveBeenCalled();
    const projection = harness.projection();
    expect(projection.runs.at(-1)?.status).toBe("interrupted");
    expect(projection.attempts[0]?.status).toBe("pending");
    expect(projection.nodes[0]?.status).toBe("pending");
    expect(projection.turnItems).toEqual([]);
    expect(harness.events).toEqual([]);
  }),
);

effectIt.effect("does not mistake a failed state read for a superseded run", () =>
  Effect.gen(function* () {
    const harness = makeLocalCommandHarness({ text: "Continue", failReadsAfterRunning: true });

    yield* harness.start;

    expect(harness.projection().runs.at(-1)?.status).toBe("running");
    const controls = harness.startRootRun.mock.calls[0]?.[0];
    expect(controls).toBeDefined();
    if (controls === undefined) return;
    const startCheck = yield* Effect.flip(controls.shouldStartProviderTurn!());
    const finalizeCheck = yield* Effect.flip(controls.shouldFinalizeRun!());
    expect(startCheck._tag).toBe("ProjectionStoreReadError");
    expect(finalizeCheck._tag).toBe("ProjectionStoreReadError");
  }),
);

effectIt.effect(
  "signs out the existing native provider before opening the newly selected provider",
  () =>
    Effect.gen(function* () {
      const harness = makeLocalCommandHarness({ text: "/logout", previousNativeSession: true });

      yield* harness.start;
      yield* harness.start;

      expect(harness.tryHandlePromptCommand).toHaveBeenCalledExactlyOnceWith({
        instanceId: harness.oldInstanceId,
        text: "/logout",
        hasAttachments: false,
      });
      expect(harness.open).not.toHaveBeenCalled();
      expect(harness.startRootRun).not.toHaveBeenCalled();
      const projection = harness.projection();
      expect(projection.runs.at(-1)?.status).toBe("completed");
      expect(projection.attempts[0]?.status).toBe("completed");
      expect(projection.nodes[0]?.status).toBe("completed");
      expect(projection.turnItems).toMatchObject([
        {
          type: "command_execution",
          title: "Provider signed out",
          output: "Provider signed out",
          status: "completed",
        },
      ]);
      expect(projection.providerTurns).toEqual([]);
      expect(projection.checkpoints).toEqual([]);
    }),
);

for (const previousMessages of [[], ["/compact", " /COMPACT "]]) {
  effectIt.effect(
    `rejects compaction without conversation context after ${previousMessages.length} prior compactions`,
    () =>
      Effect.gen(function* () {
        const harness = makeLocalCommandHarness({ text: "/compact", previousMessages });

        yield* harness.start;

        expect(harness.open).not.toHaveBeenCalled();
        expect(harness.tryHandlePromptCommand).not.toHaveBeenCalled();
        expect(harness.startRootRun).not.toHaveBeenCalled();
        expect(harness.projection().runs.at(-1)?.status).toBe("failed");
        expect(harness.projection().turnItems).toMatchObject([
          {
            type: "error",
            failure: {
              class: "validation_error",
              message: "Start a conversation before compacting this thread.",
            },
          },
        ]);
      }),
  );
}

effectIt.effect("planned update Work Mode reopens its captured native conversation", () =>
  Effect.gen(function* () {
    const harness = makeLocalCommandHarness({
      text: "@@@@@",
      keepWarm: true,
      plannedResume: "same",
    });
    yield* harness.start;
    expect(harness.open).toHaveBeenCalledOnce();
    expect(harness.open).toHaveBeenCalledWith(
      expect.objectContaining({ initialNativeThreadId: "planned-native-thread" }),
    );
    expect(harness.startRootRun).toHaveBeenCalledOnce();
    expect(harness.projection().runs.at(-1)?.status).toBe("running");
  }),
);

effectIt.effect(
  "planned update Work Mode refuses a changed native identity before sending its prompt",
  () =>
    Effect.gen(function* () {
      const harness = makeLocalCommandHarness({
        text: "@@@@@",
        keepWarm: true,
        plannedResume: "changed",
      });
      yield* harness.start;
      expect(harness.open).toHaveBeenCalledOnce();
      expect(harness.startRootRun).not.toHaveBeenCalled();
      expect(harness.projection().runs.at(-1)?.status).toBe("failed");
    }),
);

effectIt.effect("keep-warm does not reopen an expired provider session", () =>
  Effect.gen(function* () {
    const harness = makeLocalCommandHarness({ text: "@@@@@", keepWarm: true });
    yield* harness.start;
    expect(harness.open).not.toHaveBeenCalled();
    expect(harness.startRootRun).not.toHaveBeenCalled();
    expect(harness.projection().runs.at(-1)?.status).toBe("failed");
  }),
);

effectIt.effect.each(["/goal resume", "  /goal   RESUME  "])(
  "goal resume preserves its native conversation after a resume failure: %s",
  (text) =>
    Effect.gen(function* () {
      const harness = makeLocalCommandHarness({
        text,
        historyReadFailureAfterFallback: new Error("must not reach fallback history"),
      });
      harness.tryHandlePromptCommand.mockReturnValue(Effect.succeed(false));
      const originalRef = harness.projection().providerThreads.at(-1)?.nativeThreadRef;
      const result = yield* harness.start.pipe(Effect.result);
      expect(harness.fallbackEnsure).not.toHaveBeenCalled();
      expect(result._tag).toBe("Success");
      expect(harness.startRootRun).not.toHaveBeenCalled();
      expect(harness.dispatched).toHaveLength(0);
      expect(harness.projection().providerThreads.at(-1)?.nativeThreadRef).toEqual(originalRef);
      expect(harness.projection().runs.at(-1)?.status).toBe("failed");
      expect(harness.projection().contextHandoffs).toHaveLength(0);
    }),
);

effectIt.effect.each(["same", "changed", "missing"] as const)(
  "goal resume requires the original native identity: %s",
  (nativeResume) =>
    Effect.gen(function* () {
      const harness = makeLocalCommandHarness({ text: "/goal resume", nativeResume });
      harness.tryHandlePromptCommand.mockReturnValue(Effect.succeed(false));
      const originalRef = harness.projection().providerThreads.at(-1)?.nativeThreadRef;
      yield* harness.start;
      expect(harness.successEnsure).not.toHaveBeenCalled();
      expect(harness.projection().providerThreads.at(-1)?.nativeThreadRef).toEqual(originalRef);
      expect(harness.startRootRun).toHaveBeenCalledTimes(nativeResume === "same" ? 1 : 0);
      expect(harness.projection().runs.at(-1)?.status).toBe(
        nativeResume === "same" ? "running" : "failed",
      );
    }),
);

effectIt.effect(
  "retryable goal resume returns the original failure without creating a conversation",
  () =>
    Effect.gen(function* () {
      const harness = makeLocalCommandHarness({
        text: "/goal resume",
        historyReadFailureAfterFallback: new Error("must not reach fallback history"),
      });
      harness.tryHandlePromptCommand.mockReturnValue(Effect.succeed(false));
      const result = yield* harness.startWithRetry.pipe(Effect.result);
      expect(result._tag).toBe("Failure");
      expect(harness.fallbackEnsure).not.toHaveBeenCalled();
      expect(harness.startRootRun).not.toHaveBeenCalled();
      expect(harness.events).toHaveLength(0);
    }),
);

effectIt.effect.each([undefined, null])(
  "Codex continuation with unknown projected goal preserves its conversation: %s",
  (goal) =>
    Effect.gen(function* () {
      const harness = makeLocalCommandHarness({
        text: "Continue once",
        ...(goal === undefined ? {} : { goal }),
        historyReadFailureAfterFallback: new Error("must not reach fallback history"),
      });
      const original = harness.projection().providerThreads.at(-1);
      yield* harness.start;
      expect(harness.fallbackEnsure).not.toHaveBeenCalled();
      expect(harness.startRootRun).not.toHaveBeenCalled();
      expect(harness.dispatched).toHaveLength(0);
      expect(harness.projection().contextHandoffs).toHaveLength(0);
      expect(harness.projection().providerThreads.at(-1)?.nativeThreadRef).toEqual(
        original?.nativeThreadRef,
      );
      expect(harness.projection().providerThreads.at(-1)?.goal).toEqual(original?.goal);
      expect(harness.projection().runs.at(-1)?.status).toBe("failed");
      expect(harness.projection().turnItems).toMatchObject([
        {
          type: "error",
          title: "Existing provider conversation was preserved; continuation failed",
        },
      ]);
    }),
);

effectIt.effect.each(["/compact", " /COMPACT "])(
  "compaction preserves its native conversation after a resume failure: %s",
  (text) =>
    Effect.gen(function* () {
      const harness = makeLocalCommandHarness({
        text,
        previousMessages: ["Existing conversation"],
        historyReadFailureAfterFallback: new Error("must not reach fallback history"),
      });
      harness.tryHandlePromptCommand.mockReturnValue(Effect.succeed(false));
      const originalRef = harness.projection().providerThreads.at(-1)?.nativeThreadRef;
      yield* harness.start;
      expect(harness.fallbackEnsure).not.toHaveBeenCalled();
      expect(harness.startRootRun).not.toHaveBeenCalled();
      expect(harness.dispatched).toHaveLength(0);
      expect(harness.projection().contextHandoffs).toHaveLength(0);
      expect(harness.projection().providerThreads.at(-1)?.nativeThreadRef).toEqual(originalRef);
      expect(harness.projection().runs.at(-1)?.status).toBe("failed");
    }),
);

effectIt.effect.each([
  "active",
  "paused",
  "blocked",
  "usage_limited",
  "budget_limited",
  "complete",
] as const)("preserves a retained %s goal after resume failure", (status) =>
  Effect.gen(function* () {
    const goal = { objective: "Retained synthetic goal", status };
    const harness = makeLocalCommandHarness({
      text: "Continue",
      goal,
      historyReadFailureAfterFallback: new Error("must not reach fallback history"),
    });
    const originalRef = harness.projection().providerThreads.at(-1)?.nativeThreadRef;
    yield* harness.start;
    expect(harness.fallbackEnsure).not.toHaveBeenCalled();
    expect(harness.startRootRun).not.toHaveBeenCalled();
    expect(harness.projection().contextHandoffs).toHaveLength(0);
    expect(harness.projection().providerThreads.at(-1)?.nativeThreadRef).toEqual(originalRef);
    expect(harness.projection().providerThreads.at(-1)?.goal).toEqual(goal);
    expect(harness.projection().runs.at(-1)?.status).toBe("failed");
  }),
);

for (const text of ["Continue", "/compact"]) {
  effectIt.effect.each(["same", "changed", "missing"] as const)(
    `preserves original native identity before ${text}: %s`,
    (nativeResume) =>
      Effect.gen(function* () {
        const harness = makeLocalCommandHarness({
          text,
          previousMessages: ["Existing conversation"],
          goal: { objective: "Retained synthetic goal", status: "paused" },
          nativeResume,
        });
        harness.tryHandlePromptCommand.mockReturnValue(Effect.succeed(false));
        const originalRef = harness.projection().providerThreads.at(-1)?.nativeThreadRef;
        yield* harness.start;
        expect(harness.successEnsure).not.toHaveBeenCalled();
        expect(harness.projection().providerThreads.at(-1)?.nativeThreadRef).toEqual(originalRef);
        expect(harness.startRootRun).toHaveBeenCalledTimes(nativeResume === "same" ? 1 : 0);
        expect(harness.projection().runs.at(-1)?.status).toBe(
          nativeResume === "same" ? "running" : "failed",
        );
      }),
  );
}

effectIt.effect("a new Codex conversation still creates its initial native thread", () =>
  Effect.gen(function* () {
    const harness = makeLocalCommandHarness({
      text: "Start a conversation",
      nativeResume: "missing",
    });
    yield* harness.start;
    expect(harness.successEnsure).toHaveBeenCalledOnce();
    expect(harness.startRootRun).toHaveBeenCalledOnce();
    expect(harness.projection().runs.at(-1)?.status).toBe("running");
  }),
);

effectIt.effect("non-Codex chat retains fresh-conversation fallback after a resume failure", () =>
  Effect.gen(function* () {
    const harness = makeLocalCommandHarness({
      text: "Continue once",
      driver: "claudeAgent",
      historyReadFailureAfterFallback: new Error("stop after fallback"),
    });
    yield* harness.start.pipe(Effect.result);
    expect(harness.fallbackEnsure).toHaveBeenCalledOnce();
  }),
);

effectIt.effect("keep-warm does not replace a native conversation when resume fails", () =>
  Effect.gen(function* () {
    const harness = makeLocalCommandHarness({
      text: "@@@@@",
      keepWarm: true,
      liveKeepWarmSession: true,
      historyReadFailureAfterFallback: new Error("must not reach fallback history"),
    });
    yield* harness.start;
    expect(harness.open).not.toHaveBeenCalled();
    expect(harness.fallbackEnsure).not.toHaveBeenCalled();
    expect(harness.startRootRun).not.toHaveBeenCalled();
    expect(harness.projection().runs.at(-1)?.status).toBe("failed");
  }),
);
effectIt.effect(
  "fails visibly without starting a fresh native conversation after an unknown resume binding",
  () =>
    Effect.gen(function* () {
      const harness = makeLocalCommandHarness({
        text: "Continue once",
        unknownResumeBinding: true,
      });
      yield* harness.start;
      expect(harness.fallbackEnsure).not.toHaveBeenCalled();
      expect(harness.startRootRun).not.toHaveBeenCalled();
      expect(harness.projection().runs.at(-1)?.status).toBe("failed");
      const failure = harness.events.find(
        (event) => event.type === "run.updated" && event.payload.status === "failed",
      );
      expect(failure).toBeDefined();
      expect(ProviderAdapter.hasUnknownRuntimeBinding(harness.unknownResumeFailure)).toBe(true);
      expect(harness.projection().turnItems).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            type: "error",
            failure: expect.objectContaining({
              code: "provider_runtime_binding_native_effect_unconfirmed_resume",
            }),
          }),
        ]),
      );
      const retry = makeLocalCommandHarness({ text: "Continue once", unknownResumeBinding: true });
      const retryResult = yield* retry.startWithRetry.pipe(Effect.result);
      expect(retryResult._tag).toBe("Success");
      expect(retry.projection().runs.at(-1)?.status).toBe("failed");
      expect(retry.fallbackEnsure).not.toHaveBeenCalled();
      expect(retry.startRootRun).not.toHaveBeenCalled();
      expect(
        retry.events.filter(
          (event) => event.type === "run.updated" && event.payload.status === "failed",
        ),
      ).toHaveLength(1);
    }),
);

effectIt.effect("settles an unknown ensure binding once even when the worker permits retry", () =>
  Effect.gen(function* () {
    const failure = new ProviderAdapter.ProviderRuntimeBindingError({
      driver: ProviderDriverKind.make("codex"),
      detail: "Codex startup has an unconfirmed native effect; another admission is not safe.",
    });
    const harness = makeLocalCommandHarness({
      text: "Start once",
      ensureThreadFailure: failure,
    });
    yield* harness.startWithRetry;
    yield* harness.startWithRetry;
    expect(harness.ensureThread).toHaveBeenCalledOnce();
    expect(harness.fallbackEnsure).not.toHaveBeenCalled();
    expect(harness.startRootRun).not.toHaveBeenCalled();
    expect(harness.dispatched).toHaveLength(0);
    expect(harness.projection().runs.at(-1)?.status).toBe("failed");
    expect(
      harness.events.filter(
        (event) => event.type === "run.updated" && event.payload.status === "failed",
      ),
    ).toHaveLength(1);
  }),
);

const effortModels: ReadonlyArray<ServerProviderModel> = [
  {
    slug: "gpt-5.4",
    name: "GPT",
    isCustom: false,
    capabilities: {
      optionDescriptors: [
        {
          id: "reasoningEffort",
          label: "Reasoning",
          type: "select",
          options: ["low", "medium", "high", "xhigh"].map((id) => ({ id, label: id })),
          currentValue: "medium",
        },
      ],
    },
  },
];

effectIt.effect.each([
  { model: "gpt-5.4", effort: "high", explicit: undefined, expected: "high" },
  { model: "gpt-5.4", effort: "xhigh", explicit: "low", expected: undefined },
  { model: "another-model", effort: "high", explicit: undefined, expected: undefined },
  { model: "gpt-5.4", effort: "unsupported", explicit: undefined, expected: undefined },
])("resolves dispatch-only defaults without replacing the persisted selection: %j", (row) =>
  Effect.gen(function* () {
    const selection: ModelSelection = {
      instanceId: ProviderInstanceId.make("codex-personal"),
      model: "gpt-5.4",
      ...(row.explicit === undefined
        ? {}
        : { options: [{ id: "reasoningEffort", value: row.explicit }] }),
    };
    const harness = makeLocalCommandHarness({
      text: "Continue",
      effortProof: {
        selection,
        models: effortModels,
        getSettings: Effect.succeed({
          ...DEFAULT_SERVER_SETTINGS,
          defaultModelSelection: {
            instanceId: ProviderInstanceId.make("codex-work"),
            model: row.model,
            options: [{ id: "reasoningEffort", value: row.effort }],
          },
        }),
      },
    });
    yield* harness.start;
    expect(harness.dispatched).toHaveLength(1);
    expect(harness.dispatched[0]?.configuredReasoningEffort).toBe(row.expected);
    expect(harness.dispatched[0]?.modelSelection).toEqual(selection);
    expect(harness.open.mock.calls[0]?.[0].modelSelection).toEqual(selection);
    expect(harness.projection().runs.at(-1)?.modelSelection).toEqual(selection);
  }),
);

effectIt.effect("rechecks the attempt after reading current settings", () =>
  Effect.gen(function* () {
    const harness = makeLocalCommandHarness({
      text: "Continue",
      effortProof: {
        models: effortModels,
        getSettings: Effect.sync(() => {
          harness.interruptRun();
          return DEFAULT_SERVER_SETTINGS;
        }),
      },
    });
    yield* harness.start;
    expect(harness.dispatched).toHaveLength(0);
    expect(harness.projection().runs.at(-1)?.status).toBe("interrupted");
  }),
);

effectIt.effect("compacts once for a reclaimable handoff and sends the intact request", () =>
  Effect.gen(function* () {
    const compactions: ProviderAdapter.ProviderAdapterV2TurnInput[] = [];
    const harness = makeLocalCommandHarness({
      text: "Keep this exact pending request",
      plannedResume: "same",
      handoffRecovery: {
        compact: (input) =>
          Effect.sync(() => {
            compactions.push(input);
            return { usedTokens: 1000, maxTokens: 200000 };
          }),
      },
      effortProof: {
        models: effortModels,
        duringHandoff: Effect.void,
        getSettings: Effect.succeed(DEFAULT_SERVER_SETTINGS),
      },
    });
    yield* harness.start;
    expect(compactions).toHaveLength(1);
    expect(harness.dispatched).toHaveLength(1);
    expect(harness.dispatched[0]?.message.text).toMatch(
      /User message:\nKeep this exact pending request$/,
    );
    expect(harness.dispatched[0]?.attemptId).toBe(compactions[0]?.attemptId);
    expect(harness.dispatched[0]?.providerThread.nativeThreadRef).toEqual(
      compactions[0]?.providerThread.nativeThreadRef,
    );
  }),
);

effectIt.effect.each(["enough", "unsupported", "oversized", "second-budget", "stop"] as const)(
  "handoff recovery is bounded: %s",
  (condition) =>
    Effect.gen(function* () {
      let compacted = 0;
      const harness = makeLocalCommandHarness({
        text: condition === "oversized" ? "x".repeat(200000) : "Pending request",
        plannedResume: "same",
        handoffRecovery: {
          usedTokens: condition === "enough" ? 1000 : 195000,
          ...(condition === "unsupported"
            ? {}
            : {
                compact: () =>
                  Effect.sync(() => {
                    compacted++;
                    if (condition === "stop") harness.interruptRun();
                    return {
                      usedTokens: condition === "second-budget" ? 195000 : 1000,
                      maxTokens: 200000,
                    };
                  }),
              }),
        },
        effortProof: {
          models: effortModels,
          duringHandoff: Effect.void,
          getSettings: Effect.succeed(DEFAULT_SERVER_SETTINGS),
        },
      });
      yield* Effect.exit(harness.start);
      expect(compacted).toBe(condition === "second-budget" || condition === "stop" ? 1 : 0);
      expect(harness.dispatched).toHaveLength(condition === "enough" ? 1 : 0);
    }),
);

effectIt.effect("reads changed defaults after the actual handoff delivery awaits", () =>
  Effect.gen(function* () {
    let effort = "low";
    const harness = makeLocalCommandHarness({
      text: "Continue",
      effortProof: {
        models: effortModels,
        duringHandoff: Effect.gen(function* () {
          yield* Effect.yieldNow;
          effort = "xhigh";
        }),
        getSettings: Effect.sync(() => ({
          ...DEFAULT_SERVER_SETTINGS,
          defaultModelSelection: {
            instanceId: ProviderInstanceId.make("codex-work"),
            model: "gpt-5.4",
            options: [{ id: "reasoningEffort", value: effort }],
          },
        })),
      },
    });
    yield* harness.start;
    expect(harness.dispatched[0]?.configuredReasoningEffort).toBe("xhigh");
    expect(harness.dispatched[0]?.modelSelection.options).toBeUndefined();
  }),
);

effectIt.effect(
  "uses the project effort default before the environment default at actual delivery",
  () =>
    Effect.gen(function* () {
      const instanceId = ProviderInstanceId.make("codex-personal");
      const model = "gpt-5.4";
      const harness = makeLocalCommandHarness({
        text: "Continue",
        effortProof: {
          models: effortModels,
          getSettings: Effect.succeed({
            ...DEFAULT_SERVER_SETTINGS,
            providerInstances: {
              [instanceId]: { driver: ProviderDriverKind.make("codex"), enabled: true, config: {} },
            },
            defaultModelSelection: {
              instanceId,
              model,
              options: [{ id: "reasoningEffort", value: "low" }],
            },
            projectSettingsOverrides: {
              "project-native-account-command": {
                defaultModelSelection: {
                  instanceId,
                  model,
                  options: [{ id: "reasoningEffort", value: "high" }],
                },
              },
            },
          }),
        },
      });
      yield* harness.start;
      expect(harness.dispatched[0]?.configuredReasoningEffort).toBe("high");
      expect(harness.open.mock.calls[0]?.[0].modelSelection.options).toBeUndefined();
      expect(harness.projection().runs.at(-1)?.modelSelection.options).toBeUndefined();
    }),
);

effectIt.effect("terminalizes an unresolved writer retirement before opening its replacement", () =>
  Effect.gen(function* () {
    const harness = makeLocalCommandHarness({
      text: "Continue",
      retirementFailure: new Error("Previous writer is still alive."),
    });
    yield* harness.start;
    expect(harness.open).not.toHaveBeenCalled();
    expect(harness.startRootRun).not.toHaveBeenCalled();
    const projection = harness.projection();
    expect(projection.runs.at(-1)).toMatchObject({ status: "failed", startedAt: null });
    expect(projection.attempts[0]).toMatchObject({ status: "failed", startedAt: null });
    expect(projection.nodes[0]).toMatchObject({ status: "failed", startedAt: null });
    expect(projection.turnItems).toMatchObject([
      { type: "error", title: "Previous provider writer could not retire", status: "failed" },
    ]);
  }),
);

effectIt.effect("recreation skips provenance and preserves the native recovery failure", () =>
  Effect.gen(function* () {
    const f = yield* startRecreationFixture;
    const worktreesDir = `${f.root}/managed`;
    const worktreePath = `${worktreesDir}/project/provenance`;
    yield* f.fs.makeDirectory(`${worktreesDir}/project`, { recursive: true });
    yield* nativeFixtureOperation(() =>
      nativeGit(f.repo, ["worktree", "move", f.checkout, worktreePath]),
    );
    const prior = checkoutIncarnation(
      yield* nativeFixtureOperation(() => checkoutBinding(worktreePath)),
    );
    yield* f.fs.remove(worktreePath, { recursive: true });
    const captures: Array<{ reason: string; runId: string | undefined }> = [];
    const harness = startWithMissingWorktree(
      worktreePath,
      { [worktreesDir]: worktreesDir, [`${worktreesDir}/project`]: `${worktreesDir}/project` },
      undefined,
      Layer.mock(WorkspaceProvenance.Provenance)({
        capture: (input) =>
          Effect.sync(() => {
            captures.push({ reason: input.reason, runId: input.runId });
            return {} as never;
          }),
      }),
      { projectRoot: f.repo, baseDir: `${f.root}/state`, worktreesDir },
    );
    const failure = yield* harness.start;
    expect(captures).toEqual([]);
    expect(harness.recreateCheckout).toHaveBeenCalledTimes(1);
    expect(harness.recreateCheckout).toHaveBeenCalledWith(
      expect.objectContaining({
        cwd: f.repo,
        source: expect.objectContaining({
          head: f.source.head,
          branch: "refs/heads/feature/restore",
          binding: expect.objectContaining({ path: worktreePath }),
        }),
      }),
    );
    expect(
      checkoutIncarnation(yield* nativeFixtureOperation(() => checkoutBinding(worktreePath))),
    ).not.toBe(prior);
    expect(
      (yield* nativeFixtureOperation(() => nativeGit(worktreePath, ["rev-parse", "HEAD"])))
        .toString()
        .trim(),
    ).toBe(f.source.head);
    expect(failure).toMatchObject({
      _tag: "ProviderTurnStartError",
      cause: {
        _tag: "ProjectionStoreReadError",
        cause: "simulated inherited-background projection failure",
      },
    });
    expect(harness.startRootRun).not.toHaveBeenCalled();
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

effectIt.effect.each([
  { sink: "failure", attachFails: false },
  { sink: "stalled", attachFails: false },
  { sink: "stalled", attachFails: true },
] as const)(
  "resumed eager start skips $sink observation and preserves attachment failure=$attachFails",
  ({ sink, attachFails }) =>
    Effect.gen(function* () {
      let sinkStarts = 0;
      let attachments = 0;
      const diagnostics: Array<unknown> = [];
      const logger = Logger.make(({ message }) => {
        diagnostics.push(message);
      });
      const binding: ConversationPolicy.Binding = {
        threadId: "thread-native-account-command",
        projectId: "project-native-account-command",
        ownerThreadId: "thread-native-account-command",
        path: "/conversation",
        device: "fixture",
        inode: "fixture",
        commonDirectory: "/objects",
        commit: "a".repeat(40),
        baseRef: "main",
        state: "conversation",
      };
      const conversationLayer = Layer.mock(ConversationPolicy.ConversationPolicy)({
        ownsPath: (path) => path === binding.path,
        lookup: () => Effect.succeed(null),
        startTurn: () => Effect.succeed({ binding, context: "Recorded source context" }),
        fallback: () =>
          Effect.sync(() => {
            sinkStarts++;
          }).pipe(
            Effect.andThen(
              sink === "stalled"
                ? Effect.never
                : Effect.fail(
                    new ConversationPolicy.ConversationWorkspaceError({
                      threadId: binding.threadId,
                      reason: "storage",
                    }),
                  ),
            ),
          ),
        attachForTurn: (threadId, runId, text, messageAttachments) =>
          Effect.sync(() => {
            expect(threadId).toBe(binding.threadId);
            expect(runId).toBe("run-native-account-command");
            expect(text).toBe("Continue the original request");
            expect(messageAttachments).toEqual([]);
            attachments++;
          }).pipe(
            Effect.andThen(
              attachFails
                ? Effect.fail(
                    new ConversationPolicy.ConversationWorkspaceError({
                      threadId: binding.threadId,
                      reason: "handoff_unknown",
                    }),
                  )
                : Effect.void,
            ),
          ),
      });
      const harness = makeLocalCommandHarness({
        text: "Continue the original request",
        conversationLayer,
        conversationDriver: "cursor",
      });
      const projection = harness.projection();
      harness.setProjection({
        ...projection,
        thread: {
          ...projection.thread,
          lineage: {
            parentThreadId: null,
            relationshipToParent: null,
            rootThreadId: projection.thread.id,
          },
        },
      });
      const result = yield* harness.start.pipe(
        Effect.result,
        Effect.provide(Logger.layer([logger], { mergeWithExisting: false })),
      );
      expect(result._tag).toBe(attachFails ? "Failure" : "Success");
      expect(sinkStarts).toBe(0);
      expect(diagnostics).toContainEqual([
        "Conversation workspace uses eager allocation",
        {
          threadId: projection.thread.id,
          runId: projection.runs[0]!.id,
          driver: "cursor",
          reason: "eager: resumed adapter has no conversation proof",
          observation: "omitted",
          observationReason: "unbounded_sink",
        },
      ]);
      expect(attachments).toBe(1);
      expect(harness.recreateCheckout).not.toHaveBeenCalled();
      expect(harness.open).not.toHaveBeenCalled();
      expect(harness.startRootRun).not.toHaveBeenCalled();
    }),
);

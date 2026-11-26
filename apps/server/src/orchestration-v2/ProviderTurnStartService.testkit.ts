import * as ContinuityAdmissionTestkit from "../jones/goals/continuity/admission.testkit.ts";
import * as ControlAdmission from "../jones/orchestration/ControlAdmission.ts";
import * as RestartPreparation from "../jones/orchestration/RestartPreparation.ts";
import {
  EventId,
  SourceControlProviderError,
  SourceControlProviderKind,
  type CommandId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as ServerConfig from "../config.ts";
import * as GitManager from "../git/GitManager.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as VcsDriverRegistry from "../vcs/VcsDriverRegistry.ts";
import * as VcsProjectConfig from "../vcs/VcsProjectConfig.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as ProjectSetupScriptRunner from "../project/ProjectSetupScriptRunner.ts";
import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import * as SourceControlProviderRegistry from "../sourceControl/SourceControlProviderRegistry.ts";
import * as TextGeneration from "../textGeneration/TextGeneration.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProjectStore from "./ProjectStore.ts";
import * as PullRequestService from "../pullRequest/PullRequestService.ts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";

import * as GitWorkflow from "../git/GitWorkflowService.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as TaskFolders from "../workspace/TaskFolders.ts";

// No project in these tests tracks a repository, so no thread works in a task folder.
export const layerNoTaskFolders = Layer.mock(TaskFolders.TaskFolders)({
  plan: () => Effect.succeed(null),
  find: () => Effect.succeed(null),
});

export const layer = Layer.mergeAll(
  Layer.mock(ProjectSetupScriptRunner.ProjectSetupScriptRunner)({
    runForThread: () => Effect.succeed({ status: "no-script" }),
  }),
  layerNoTaskFolders,
  Layer.mock(GitWorkflow.GitWorkflowService)({
    pruneWorktrees: () => Effect.void,
    createWorktree: () => Effect.succeed({} as never),
  }),
  Layer.mock(ProjectService.ProjectService)({
    getById: () => Effect.succeedNone,
  }),
);

export const layer = Layer.mergeAll(
  layerWorkspace,
  ContinuityAdmissionTestkit.layerAbsent,
  ControlAdmission.layer,
  Layer.mock(RestartPreparation.RestartPreparation)({ assertThreadOpen: () => Effect.void }),
);

// One SQLite graph owns command acceptance, event lineage, projections and outbox leases.
export const layerWorkerPersistence = EventSink.layerFromStores.pipe(
  Layer.provideMerge(
    Layer.mergeAll(
      CommandReceiptStore.layer,
      EffectOutbox.layer,
      EventStore.layer,
      ProjectionStore.layer,
      ProjectStore.layer,
      TurnItemPositionStore.layer,
    ),
  ),
  Layer.provideMerge(SqlitePersistence.layerMemory),
);

// These cases own no external resources. An unexpected cleanup call must fail at its port.
export const layerWorkerCleanup = ResourceCleanupService.layer.pipe(
  Layer.provide(
    Layer.mergeAll(
      Layer.mock(TerminalManager.TerminalManager)({}),
      Layer.mock(PreviewManager.PreviewManager)({}),
      Layer.mock(DeviceService.DeviceService)({}),
      Layer.mock(ScheduledTaskService.ScheduledTaskService)({}),
      FileSystem.layerNoop({}),
      // The real config owner derives every path in its own scoped directory; only
      // that derivation sees a real filesystem, and cleanup keeps the noop port above.
      ServerConfig.layerTest(process.cwd(), { prefix: "t3-worker-cleanup-" }).pipe(
        Layer.provide(NodeServices.layer),
      ),
    ),
  ),
);

export const makeWorkerPersistence = Effect.fn("ProviderTurnStartTestkit.makeWorkerPersistence")(
  function* (input: {
    readonly projection: () => OrchestrationV2ThreadProjection;
    readonly acceptEvents: (events: ReadonlyArray<OrchestrationV2DomainEvent>) => void;
  }) {
    const durableSink = yield* EventSink.EventSinkV2;
    let baseline: OrchestrationV2ThreadProjection | undefined;
    let sequence = 0;
    const synchronize = Effect.fn("ProviderTurnStartTestkit.synchronize")(function* () {
      const current = input.projection();
      if (current === baseline) return;
      const events: Array<OrchestrationV2DomainEvent> = [];
      const common = () => ({
        id: EventId.make(`worker-fixture:${++sequence}`),
        threadId: current.thread.id,
        occurredAt: current.updatedAt,
      });
      if (current.thread !== baseline?.thread) {
        const run = current.runs.at(-1)!;
        events.push({
          ...common(),
          type: baseline === undefined ? "thread.created" : "thread.metadata-updated",
          payload: Object.assign(
            {
              title: "Provider start worker fixture",
              createdBy: "user",
              creationSource: "web",
              providerInstanceId: run.providerInstanceId,
              modelSelection: run.modelSelection,
              runtimeMode: "full-access",
              interactionMode: "default",
              forkedFrom: null,
              createdAt: current.updatedAt,
              updatedAt: current.updatedAt,
              settledOverride: null,
              snoozedUntil: null,
              snoozedAt: null,
              lastVisitedAt: null,
            },
            current.thread,
          ),
        });
      }
      for (const row of current.providerThreads) {
        if (baseline?.providerThreads.find((old) => old.id === row.id) === row) continue;
        events.push({ ...common(), type: "provider-thread.updated", payload: row });
      }
      for (const row of current.runs) {
        if (baseline?.runs.find((old) => old.id === row.id) === row) continue;
        events.push({ ...common(), type: "run.updated", payload: row });
      }
      for (const row of current.attempts) {
        const old = baseline?.attempts.find((old) => old.id === row.id);
        if (old === row) continue;
        events.push({
          ...common(),
          type: old === undefined ? "run-attempt.created" : "run-attempt.updated",
          payload: row,
        });
      }
      for (const row of current.nodes) {
        if (baseline?.nodes.find((old) => old.id === row.id) === row) continue;
        events.push({ ...common(), type: "node.updated", payload: row });
      }
      for (const row of current.messages) {
        if (baseline?.messages.find((old) => old.id === row.id) === row) continue;
        events.push({ ...common(), type: "message.updated", payload: row });
      }
      for (const row of current.checkpointScopes) {
        if (baseline?.checkpointScopes.find((old) => old.id === row.id) === row) continue;
        events.push({ ...common(), type: "checkpoint-scope.created", payload: row });
      }
      for (const row of current.providerTurns) {
        if (baseline?.providerTurns.find((old) => old.id === row.id) === row) continue;
        events.push({ ...common(), type: "provider-turn.updated", payload: row });
      }
      if (events.length > 0) yield* durableSink.write({ events });
      baseline = current;
    });
    const accept = (events: ReadonlyArray<OrchestrationV2DomainEvent>) => {
      input.acceptEvents(events);
      baseline = input.projection();
    };
    const sink: EventSink.EventSinkV2["Service"] = {
      ...durableSink,
      write: (write) =>
        synchronize().pipe(
          Effect.andThen(durableSink.write(write)),
          Effect.tap((stored) => Effect.sync(() => accept(stored.map(({ event }) => event)))),
        ),
      writeIfRunCurrent: (write) =>
        synchronize().pipe(
          Effect.andThen(durableSink.writeIfRunCurrent(write)),
          Effect.tap((result) =>
            Effect.sync(() => accept(result.storedEvents.map(({ event }) => event))),
          ),
        ),
    };
    const commitEffect = Effect.fn("ProviderTurnStartTestkit.commitEffect")(function* (effect: {
      readonly id: string;
      readonly commandId: CommandId;
      readonly request: EffectOutbox.OrchestrationEffectRequestV2;
    }) {
      yield* synchronize();
      const projection = input.projection();
      const run = projection.runs.at(-1)!;
      const attempt = projection.attempts.find((row) => row.id === run.activeAttemptId)!;
      const node = projection.nodes.find((row) => row.id === run.rootNodeId)!;
      const common = { threadId: run.threadId, occurredAt: projection.updatedAt };
      return yield* durableSink.commitCommand({
        commandId: effect.commandId,
        commandType:
          effect.request.type === "provider-turn.restart" ? "message.steer" : "message.dispatch",
        threadId: run.threadId,
        acceptedAt: projection.updatedAt,
        events: [
          { ...common, id: EventId.make(`${effect.id}:run`), type: "run.updated", payload: run },
          {
            ...common,
            id: EventId.make(`${effect.id}:attempt`),
            type: "run-attempt.created",
            payload: attempt,
          },
          { ...common, id: EventId.make(`${effect.id}:node`), type: "node.updated", payload: node },
        ],
        effects: [{ ...effect, threadId: run.threadId }],
      });
    });
    yield* synchronize();
    return { sink, synchronize, commitEffect };
  },
);

// Keep repository detection, branch selection and worktree creation on the real Git path.
export const layerRealGit = GitWorkflow.layer.pipe(
  Layer.provide(GitManager.layer),
  Layer.provide(
    Layer.mergeAll(
      Layer.mock(SourceControlProviderRegistry.SourceControlProviderRegistry)({
        resolveLink: () => undefined,
        resolve: ({ cwd }) =>
          Effect.fail(
            new SourceControlProviderError({
              provider: SourceControlProviderKind.make("unknown"),
              operation: "resolve",
              cwd,
              detail: "No source control provider in tests",
            }),
          ),
      }),
      Layer.mock(TextGeneration.TextGeneration)({}),
      Layer.mock(ProviderRegistry.ProviderRegistry)({}),
      Layer.mock(ProjectSetupScriptRunner.ProjectSetupScriptRunner)({}),
      Layer.mock(ProjectionStore.ProjectionStoreV2)({}),
      Layer.mock(ProjectStore.ProjectStoreV2)({}),
      Layer.mock(PullRequestService.PullRequestService)({}),
      ServerSettings.layerTest(),
    ),
  ),
  Layer.provide(VcsDriverRegistry.layer.pipe(Layer.provide(VcsProjectConfig.layer))),
  Layer.provideMerge(GitVcsDriver.layer),
  Layer.provideMerge(VcsProcess.layer),
  Layer.provideMerge(ServerConfig.layerTest(process.cwd(), { prefix: "t3-recreate-" })),
  Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
);

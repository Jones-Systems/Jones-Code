import {
  CommandId,
  type EnvironmentId,
  NodeId,
  OrchestratorMcpFailure,
  type OrchestratorMcpDelegateTaskInput,
  type OrchestratorMcpTaskCancelResult,
  type OrchestrationV2RemoteTaskChild,
  type OrchestrationV2Subagent,
  type ProjectId,
  type ProviderInstanceId,
  ProviderDriverKind,
  type RunId,
  type RemoteTaskObservation,
  type ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Duration from "effect/Duration";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schedule from "effect/Schedule";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import type * as Tool from "effect/ai/Tool";

import type * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import { ProjectToolkit } from "../mcp/toolkits/project/tools.ts";
import {
  resolveInteractionMode,
  resolveRuntimeMode,
  taskPrompt,
} from "../mcp/delegatedTaskModes.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as RepositoryIdentityResolver from "../project/RepositoryIdentityResolver.ts";
import { forkParked } from "../serverActivation.ts";
import * as PeerForwarding from "./PeerForwarding.ts";
import * as PeerLinks from "./PeerLinks.ts";
import * as RemoteLifecycle from "../jones/remoteDelegation/Lifecycle.ts";
import * as PeerMonitor from "../jones/remoteDelegation/PeerMonitor.ts";
import * as FinalText from "../jones/remoteDelegation/FinalText.ts";
import * as ObservationSupport from "../jones/remoteDelegation/ObservationSupport.ts";
import { refuseUnsolicitedSupervision } from "../jones/mcp/delegatedTaskSupervision/DelegatedTaskSupervision.ts";

/** How long one follow of a remote task waits on its thread there before checking again. */
const FOLLOW_WAIT_MS = 50_000;
/** A linked environment that stops answering is retried, backing off to this. */
const FOLLOW_MAX_BACKOFF = Duration.minutes(5);
const backoff = Schedule.exponential(Duration.seconds(2)).pipe(
  Schedule.modifyDelay(({ duration }) =>
    Effect.succeed(Duration.min(duration, FOLLOW_MAX_BACKOFF)),
  ),
);

type ThreadScope = McpInvocationContext.McpThreadInvocationScope;

/**
 * `delegate_task` to a linked environment. The task's child is an ordinary
 * thread there, launched through the link; the parent here records the task
 * like a local one, without a child thread, and a follower completes it when
 * the thread there ends, so the parent wakes as it would for a local child.
 */
export class RemoteDelegation extends Context.Service<
  RemoteDelegation,
  {
    readonly observe: (
      scope: ThreadScope,
      task: OrchestrationV2Subagent,
    ) => Effect.Effect<
      {
        readonly observation: RemoteTaskObservation;
        readonly latestTerminalSummary: string | null;
      },
      OrchestratorMcpFailure
    >;
    readonly delegate: (
      scope: ThreadScope,
      input: OrchestratorMcpDelegateTaskInput & {
        readonly target: NonNullable<OrchestratorMcpDelegateTaskInput["target"]> & {
          readonly environmentId: EnvironmentId;
        };
      },
    ) => Effect.Effect<{ readonly taskId: NodeId }, OrchestratorMcpFailure>;
    /**
     * Stops the task's thread there, then completes it here as cancelled. A
     * task that already has its result keeps it; its thread there is still
     * stopped, as cancelling a local task stops follow-up work on its child.
     */
    readonly cancel: (
      scope: ThreadScope,
      task: OrchestrationV2Subagent,
      input: {
        readonly reason?: string | undefined;
        readonly clientRequestId?: string | undefined;
      },
    ) => Effect.Effect<OrchestratorMcpTaskCancelResult, OrchestratorMcpFailure>;
    /**
     * Follows every open remote task until it ends, including after a
     * restart, and a finished one again when a Stop here owes it a stop there.
     */
    readonly start: () => Effect.Effect<void, never, Scope.Scope>;
  }
>()("t3/peer/RemoteDelegation") {}

/** Loss of access fails the local task, while remote stop debt remains until acknowledged. */
const isFinal = (error: OrchestratorMcpFailure) =>
  error.code === "capability_denied" ||
  error.code === "thread_not_found" ||
  error.code === "run_not_found";

const make = Effect.gen(function* () {
  const forwarding = yield* PeerForwarding.PeerForwarding;
  const links = yield* PeerLinks.PeerLinks;
  const threads = yield* ThreadManagement.ThreadManagementService;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const projects = yield* ProjectService.ProjectService;
  const repositoryIdentities = yield* RepositoryIdentityResolver.RepositoryIdentityResolver;
  const crypto = yield* Crypto.Crypto;
  const hereId = yield* ServerEnvironment.ServerEnvironment.pipe(
    Effect.flatMap((environment) => environment.getEnvironmentId),
  );
  // Followers live as long as this service, so shutting down stops them.
  const followers = yield* Scope.Scope;
  const monitor = yield* PeerMonitor.make;
  const lifecycle = yield* RemoteLifecycle.make((parentThreadId, taskId) =>
    followOne(parentThreadId, taskId).pipe(Effect.forkIn(followers), Effect.asVoid),
  );

  /** The peer's project with this thread's repository, or the one the caller named. */
  const resolveRemoteProject = Effect.fn("RemoteDelegation.resolveRemoteProject")(function* (
    scope: ThreadScope,
    environmentId: EnvironmentId,
    parentProjectId: ProjectId,
    requested: ProjectId | undefined,
  ) {
    if (requested !== undefined) return requested;
    const here = yield* projects.getById(parentProjectId).pipe(
      Effect.map(Option.getOrUndefined),
      Effect.orElseSucceed(() => undefined),
    );
    // The project's own identity is a one-minute cache that reads blank when
    // cold, so resolve it here, as the other side's t3_project_list does.
    const key =
      here === undefined
        ? undefined
        : (yield* repositoryIdentities.resolve(here.workspaceRoot))?.canonicalKey;
    if (key === undefined) {
      return yield* new OrchestratorMcpFailure({
        code: "target_required",
        message: "Pass target.projectId: this thread's project has no repository to match there.",
      });
    }
    const matches: Array<{ readonly id: ProjectId; readonly title: string }> = [];
    let cursor: number | null = 0;
    while (cursor !== null) {
      const listed: Tool.Success<typeof ProjectToolkit.tools.t3_project_list> =
        yield* forwarding.call(scope, ProjectToolkit.tools.t3_project_list, environmentId, {
          cursor,
          limit: 100,
        });
      matches.push(
        ...listed.projects.filter((project) => project.repositoryIdentity?.canonicalKey === key),
      );
      cursor = listed.nextCursor;
    }
    if (matches.length === 1) return matches[0]!.id;
    return yield* new OrchestratorMcpFailure({
      code: "invalid_request",
      message:
        matches.length === 0
          ? `No project there has this repository (${key}). Pass target.projectId; t3_project_list with this environmentId lists them. To bring the repository there, use t3_project_clone and then t3_project_create with this environmentId.`
          : `Several projects there have this repository: ${matches.map((project) => `${project.title} (${project.id})`).join(", ")}. Pass target.projectId.`,
    });
  });

  const observe: RemoteDelegation["Service"]["observe"] = Effect.fn("RemoteDelegation.observe")(
    function* (scope, task) {
      const remote = task.remoteChild;
      if (remote?.runId === undefined)
        return yield* new OrchestratorMcpFailure({
          code: "orchestration_error",
          message:
            "Remote child progress is unavailable: the original remote run is not attributed.",
        });
      const snapshot = yield* forwarding.readThread(scope, remote.environmentId, {
        threadId: remote.threadId,
        childProgressForRunId: remote.runId,
        limit: 1,
      });
      const observation = snapshot.remoteObservation;
      if (observation === undefined || observation.childProgress === undefined)
        return yield* new OrchestratorMcpFailure({
          code: "orchestration_error",
          message:
            "Remote child progress is unsupported by this linked environment. Upgrade the receiving server to observe later runs.",
        });
      if (
        observation.environmentId !== remote.environmentId ||
        observation.threadId !== remote.threadId ||
        observation.originalRunId !== remote.runId
      )
        return yield* new OrchestratorMcpFailure({
          code: "orchestration_error",
          message: "Remote child progress returned a mismatched task binding.",
        });
      const progress = observation.childProgress;
      const latestTerminalSummary =
        progress.latestTerminalRunId === null
          ? null
          : ((yield* lastReply(
              scope,
              remote,
              progress.latestTerminalRunId,
              progress.latestTerminalStatus === "failed",
            )) ?? null);
      return { observation, latestTerminalSummary };
    },
  );

  const delegate: RemoteDelegation["Service"]["delegate"] = Effect.fn("RemoteDelegation.delegate")(
    function* (scope, input) {
      const key = input.clientRequestId ?? (yield* crypto.randomUUIDv4().pipe(Effect.orDie));
      const commandId = CommandId.make(
        `command:mcp:${encodeURIComponent(scope.requestNamespace)}:remote-delegate:${encodeURIComponent(key)}`,
      );
      const replayed = yield* lifecycle.replay(commandId, input);
      if (replayed !== undefined) return replayed;
      const parent = yield* threads.getThreadRecords(scope.thread.threadId, ["runs"]).pipe(
        Effect.mapError(
          () =>
            new OrchestratorMcpFailure({
              code: "thread_not_found",
              message: "The calling thread was not found.",
            }),
        ),
      );
      const parentRun = parent.runs
        .filter(ThreadManagement.isActiveRun)
        .toSorted((left, right) => right.ordinal - left.ordinal)[0];
      if (
        parentRun === undefined ||
        parentRun.rootNodeId === null ||
        parentRun.providerInstanceId !== scope.thread.providerInstanceId
      ) {
        return yield* new OrchestratorMcpFailure({
          code: "parent_not_active",
          message: "Delegated tasks require an active run owned by this MCP provider session.",
        });
      }
      yield* refuseUnsolicitedSupervision(parent.thread, input);
      const parentNodeId = parentRun.rootNodeId;
      const { target } = input;
      if (input.isolation !== undefined && input.workspaceStrategy !== undefined)
        return yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message: "Pass isolation or workspaceStrategy, not both.",
        });
      // The destination resolves omitted worktree base/defaults in its normal launch service.
      // A parent commit or uncommitted files from this host never substitute for that base.
      const workspaceStrategy =
        input.workspaceStrategy ??
        (input.isolation === "worktree" ? { type: "worktree" as const } : undefined);
      // The provider and model inherit as they do for a task here. Another
      // provider there has no model to inherit, so the caller names one.
      const inherited = parent.thread.modelSelection;
      const instanceId = target.providerInstanceId ?? inherited.instanceId;
      const model =
        target.model ?? (instanceId === inherited.instanceId ? inherited.model : undefined);
      if (model === undefined) {
        return yield* new OrchestratorMcpFailure({
          code: "target_required",
          message: "Pass target.model from orchestrator_capabilities with this environmentId.",
        });
      }
      const options =
        target.options ??
        (instanceId === inherited.instanceId && model === inherited.model
          ? inherited.options
          : undefined);
      // Modes asked for are checked against this thread. Omitted ones are left
      // to the other side, which inherits this thread's modes capped by the
      // link's access, so a narrower link still takes the task.
      const runtimeMode =
        input.runtimeMode === undefined || input.runtimeMode === "inherit"
          ? undefined
          : yield* resolveRuntimeMode(parent.thread.runtimeMode, input.runtimeMode);
      const interactionMode =
        input.interactionMode === undefined || input.interactionMode === "inherit"
          ? undefined
          : yield* resolveInteractionMode(parent.thread.interactionMode, input.interactionMode);
      const projectId = yield* resolveRemoteProject(
        scope,
        target.environmentId,
        parent.thread.projectId,
        target.projectId ?? input.projectId,
      );
      const task = taskPrompt(input);
      const link = yield* links.get(target.environmentId).pipe(
        Effect.map(Option.getOrUndefined),
        Effect.mapError(
          () =>
            new OrchestratorMcpFailure({
              code: "orchestration_error",
              message: "Unable to read the destination link.",
            }),
        ),
      );
      if (link === undefined)
        return yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message: "The destination is not linked.",
        });
      const modelSelection = { instanceId, model, ...(options === undefined ? {} : { options }) };
      return yield* lifecycle.submit(commandId, input, {
        scope: {
          environmentId: scope.environmentId,
          requestNamespace: scope.requestNamespace,
          thread: scope.thread,
        },
        environmentId: target.environmentId,
        linkedAt: DateTime.formatIso(link.linkedAt),
        label: link.label,
        launch: {
          projectId,
          title: input.title ?? parent.thread.title,
          modelSelection,
          ...(runtimeMode === undefined ? {} : { runtimeMode }),
          ...(interactionMode === undefined ? {} : { interactionMode }),
          ...(workspaceStrategy === undefined ? {} : { workspaceStrategy }),
          message: task,
          clientRequestId: `delegate:${scope.thread.threadId}:${key}`,
          delegatedFrom: {
            environmentId: hereId,
            threadId: parent.thread.id,
            title: parent.thread.title,
          },
        },
        request: {
          type: "delegated_task.remote.request",
          commandId,
          parentThreadId: scope.thread.threadId,
          parentRunId: parentRun.id,
          parentNodeId,
          task,
          ...(input.title === undefined ? {} : { title: input.title }),
          driver: ProviderDriverKind.make("remote"),
          modelSelection,
          completionWake: input.mode === "wait" ? "settled_only" : "always",
        },
      });
    },
  );

  /**
   * Each completion attempt uses its own command so one failed attempt cannot block its successor.
   * A refusal because cancel, Stop or an earlier report already supplied the result is terminal.
   * stoppedThere discharges remote stop debt even for a task whose result already exists.
   */
  const complete = Effect.fn("RemoteDelegation.complete")(function* (
    parentThreadId: ThreadId,
    taskId: NodeId,
    status: "completed" | "failed" | "cancelled" | "interrupted",
    result: string,
    stoppedThere: "ran-out" | "stopped" | undefined,
    stopCount?: number,
  ) {
    const attempt = yield* crypto.randomUUIDv4().pipe(Effect.orDie);
    yield* threads
      .dispatch({
        type: "delegated_task.remote.complete",
        commandId: CommandId.make(`command:remote-task-complete:${taskId}:${attempt}`),
        parentThreadId,
        taskId,
        status,
        result,
        ...(stoppedThere === undefined ? {} : { stoppedThere }),
        ...(stopCount === undefined ? {} : { stopCount }),
      })
      .pipe(
        Effect.catch((error) =>
          threads
            .getThreadRecords(parentThreadId, ["subagents"])
            .pipe(
              Effect.flatMap(({ subagents }) =>
                subagents.find((task) => task.id === taskId)?.result != null
                  ? Effect.void
                  : Effect.fail(error),
              ),
            ),
        ),
      );
  });

  /**
   * How this environment reaches a task's thread there: as the parent
   * thread, under one namespace per parent, so a stop sent again after a
   * restart or a dropped route is recorded there once.
   */
  const followScope = (
    parent: { readonly threadId: ThreadId; readonly providerInstanceId: ProviderInstanceId },
    remote: OrchestrationV2RemoteTaskChild,
  ): ThreadScope => ({
    environmentId: hereId,
    requestNamespace: `remote-task:${parent.threadId}`,
    thread: {
      threadId: parent.threadId,
      providerSessionId: `remote-task:${remote.threadId}`,
      providerInstanceId: parent.providerInstanceId,
    },
    client: undefined,
    capabilities: new Set(["orchestration"]),
    issuedAt: 0,
  });

  /**
   * Stop all current work on the owned remote child thread: run, queued messages, PR watches and descendants.
   * key identifies the stop; the follower sends one, each cancel its own, and remote recording deduplicates retries.
   */
  const stopThere = (
    scope: ThreadScope,
    remote: OrchestrationV2RemoteTaskChild,
    key: string,
    reason?: string,
  ) =>
    forwarding
      .interruptThread(scope, remote.environmentId, {
        threadId: remote.threadId,
        stop: true,
        ...(reason === undefined ? {} : { reason }),
        clientRequestId: `${key}:${remote.threadId}`,
      })
      .pipe(Effect.asVoid);

  const lastReply = Effect.fn("RemoteDelegation.lastReply")(function* (
    scope: ThreadScope,
    remote: OrchestrationV2RemoteTaskChild,
    runId: RunId,
    failed = false,
  ) {
    return yield* FinalText.lastReply(
      (input) => forwarding.readThread(scope, remote.environmentId, input),
      remote.threadId,
      runId,
      failed,
    );
  });

  /**
   * Why following a task can never succeed, if that is what `error` means.
   * The far side's message already says what was refused and what to do; a
   * link forgotten here only reads as a bad request, so it is named.
   */
  const finalReason = Effect.fn("RemoteDelegation.finalReason")(
    function* (parentThreadId: ThreadId, taskId: NodeId, error: OrchestratorMcpFailure) {
      if (isFinal(error)) return error.message;
      if (error.code !== "invalid_request") return undefined;
      const { subagents } = yield* threads.getThreadRecords(parentThreadId, ["subagents"]);
      const environmentId = subagents.find((task) => task.id === taskId)?.remoteChild
        ?.environmentId;
      if (environmentId === undefined) return error.message;
      const link = yield* links.get(environmentId);
      return Option.isSome(link) ? undefined : error.message;
    },
    Effect.orElseSucceed(() => undefined),
  );

  /** A task's thread there that ended without a reply. */
  const noReply = "(The thread in the linked environment ended without a reply.)";

  /**
   * Follow remote completion and complete locally, or stop remotely if the local task ended first.
   * Revoked/expired/missing links or missing thread/run fail the task. Only a missing thread proves
   * no remaining stop debt; lost access retains that debt across bounded retries and restart.
   */
  const follow = (parentThreadId: ThreadId, taskId: NodeId): Effect.Effect<void> =>
    followOnce(parentThreadId, taskId).pipe(
      // `false` means it is still running there: follow again straight away.
      Effect.repeat({ while: (done) => !done }),
      Effect.catchTags({
        OrchestratorMcpFailure: (error) =>
          Effect.gen(function* () {
            const reason = yield* finalReason(parentThreadId, taskId, error);
            if (reason === undefined) return yield* error;
            const { subagents } = yield* threads.getThreadRecords(parentThreadId, ["subagents"]);
            const stopCount = subagents.find((task) => task.id === taskId)?.remoteChild?.stopCount;
            yield* complete(
              parentThreadId,
              taskId,
              "failed",
              reason,
              error.code === "thread_not_found" ? "stopped" : undefined,
              stopCount,
            );
            if (error.code !== "thread_not_found") {
              yield* threads.dispatch({
                type: "delegated_task.remote.stop",
                commandId: CommandId.make(`command:remote-stop-unresolved:${taskId}`),
                parentThreadId,
                taskId,
              });
              return yield* error;
            }
          }),
      }),
      Effect.retry(backoff),
      Effect.ignoreCause({ log: true }),
    );

  /**
   * One wait on the task's thread there; true once the task here is done.
   * A task completed here first (cancelled, or its parent stopped) is
   * stopped there, and done once that stop is recorded here.
   */
  const followOnce = Effect.fn("RemoteDelegation.followOnce")(function* (
    parentThreadId: ThreadId,
    taskId: NodeId,
  ) {
    const sequence = yield* threads.getThreadEventSequence(parentThreadId);
    const records = yield* threads.getThreadRecords(parentThreadId, ["subagents"]);
    const task = records.subagents.find((candidate) => candidate.id === taskId);
    if (task?.remoteChild === undefined) return true;
    const remote = task.remoteChild;
    const scope = followScope(
      { threadId: parentThreadId, providerInstanceId: records.thread.providerInstanceId },
      remote,
    );
    // Ended here, maybe while this follower backed off from an unreachable
    // machine or before a restart: it still has to be stopped there. So
    // does one its parent here was stopped after, for what followed there.
    if (task.result !== null) {
      if (
        remote.stoppedThere === false ||
        ((task.status === "cancelled" || task.status === "interrupted") &&
          remote.stoppedThere !== true)
      ) {
        const key = remote.stopCount === undefined ? "stop" : `stop-${remote.stopCount}`;
        yield* stopThere(scope, remote, key);
        yield* complete(
          parentThreadId,
          taskId,
          terminalStatus(task.status),
          task.result,
          "stopped",
          remote.stopCount,
        );
        return yield* settled(parentThreadId, taskId);
      }
      return true;
    }
    if (remote.runId === undefined)
      return yield* new OrchestratorMcpFailure({
        code: "orchestration_error",
        message: "The remote task has no attributed run; reconciliation remains pending.",
      });
    const previousObservation =
      threads.readRemoteTaskObservation === undefined
        ? undefined
        : yield* threads.readRemoteTaskObservation(parentThreadId, taskId);
    if (previousObservation !== undefined && threads.observeRemoteTask !== undefined)
      yield* threads.observeRemoteTask({
        parentThreadId,
        taskId,
        observation: previousObservation,
      });
    if (previousObservation !== undefined && threads.reconcileRemoteTaskQuestions !== undefined)
      yield* threads.reconcileRemoteTaskQuestions(parentThreadId, taskId);
    const endedHere = threads
      .streamStoredEventsFrom({
        threadId: parentThreadId,
        afterSequence: sequence,
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
        Effect.flatMap((ended) =>
          Option.isSome(ended) ? Effect.succeed({ endedHere: true } as const) : Effect.never,
        ),
      );
    const waited = yield* monitor(
      remote.environmentId,
      forwarding.waitForThread(scope, remote.environmentId, {
        threadId: remote.threadId,
        ...(remote.runId === undefined ? {} : { runId: remote.runId }),
        timeoutMs: FOLLOW_WAIT_MS,
        // A thread there whose run ended may still be waiting on its own
        // tasks to wake it for the answer, which a task here waits for too.
        // Observation support is required for new launches. Existing tasks on
        // older peers still get one bounded chance to return their final result.
        untilDelegatedWorkSettled: true,
        observeBlockingQuestions: true,
        ...(previousObservation === undefined
          ? {}
          : { questionResumeToken: previousObservation.blockingQuestions.resumeToken }),
      }),
    ).pipe(Effect.raceFirst(endedHere));
    // The next look reads the result and stops it there.
    if ("endedHere" in waited) return false;
    if (waited.threadId !== remote.threadId)
      return yield* new OrchestratorMcpFailure({
        code: "orchestration_error",
        message:
          "The remote wait returned a mismatched task thread; reconciliation remains pending.",
      });
    if (
      waited.remoteObservation === undefined &&
      (waited.timedOut ||
        waited.observationChanged === true ||
        waited.threadId !== remote.threadId ||
        waited.runId !== remote.runId ||
        !isTerminal(waited.status) ||
        (waited.resultRun !== undefined && !isTerminal(waited.resultRun.status)))
    )
      return yield* ObservationSupport.unsupported();
    if (waited.remoteObservation !== undefined) {
      const observation = waited.remoteObservation;
      if (
        observation.environmentId !== remote.environmentId ||
        observation.threadId !== remote.threadId ||
        observation.originalRunId !== remote.runId
      )
        return yield* new OrchestratorMcpFailure({
          code: "orchestration_error",
          message: "The remote question observation returned a mismatched task binding.",
        });
      if (threads.observeRemoteTask === undefined)
        return yield* new OrchestratorMcpFailure({
          code: "orchestration_error",
          message: "Remote question observation storage is unavailable.",
        });
      yield* threads.observeRemoteTask({ parentThreadId, taskId, observation });
    }
    if (waited.timedOut || waited.observationChanged === true) return false;
    // Missing run attribution is unresolved, never an empty successful completion.
    if (waited.runId === null)
      return yield* new OrchestratorMcpFailure({
        code: "orchestration_error",
        message: "The remote task's run could not be attributed; reconciliation remains pending.",
      });
    if (!isTerminal(waited.status)) return false;
    if (waited.resultRun !== undefined && !isTerminal(waited.resultRun.status))
      return yield* new OrchestratorMcpFailure({
        code: "orchestration_error",
        message:
          "The remote wait selected an unfinished result run; reconciliation remains pending.",
      });
    const resultRun = waited.resultRun ?? {
      runId: remote.runId ?? waited.runId,
      status: waited.status,
    };
    const result = yield* lastReply(scope, remote, resultRun.runId, resultRun.status === "failed");
    yield* complete(
      parentThreadId,
      taskId,
      terminalStatus(resultRun.status),
      result ?? noReply,
      "ran-out",
    );
    return yield* settled(parentThreadId, taskId);
  });

  /**
   * Whether the task is done here after the follower reported its run ran
   * out there. A cancel or Stop that landed while the reply was read keeps
   * its result, and still owes a stop there for what may follow the run;
   * the next look sends it.
   */
  const settled = (parentThreadId: ThreadId, taskId: NodeId) =>
    threads
      .getThreadRecords(parentThreadId, ["subagents"])
      .pipe(
        Effect.map(
          ({ subagents }) =>
            subagents.find((task) => task.id === taskId)?.remoteChild?.stoppedThere === true,
        ),
      );

  const following = new Map<NodeId, { again: boolean }>();

  /**
   * `follow`, at most once per task. Asked again while it runs, say by a
   * Stop here on a task that already has its result, it looks once more
   * before it ends, so that stop is not missed.
   */
  const followOne = (parentThreadId: ThreadId, taskId: NodeId) =>
    Effect.suspend(() => {
      const running = following.get(taskId);
      if (running !== undefined) {
        running.again = true;
        return Effect.void;
      }
      const state = { again: false };
      following.set(taskId, state);
      return follow(parentThreadId, taskId).pipe(
        // Checked and released in one step, so a request between them is never lost.
        Effect.repeat({
          while: () => {
            if (state.again) {
              state.again = false;
              return true;
            }
            following.delete(taskId);
            return false;
          },
        }),
        Effect.ensuring(
          Effect.sync(() => {
            if (following.get(taskId) === state) following.delete(taskId);
          }),
        ),
      );
    });

  const cancel: RemoteDelegation["Service"]["cancel"] = Effect.fn("RemoteDelegation.cancel")(
    function* (scope, task, input) {
      const remote = task.remoteChild;
      if (remote === undefined) return yield* lifecycle.cancelPending(task, input.reason);
      const { reason } = input;
      const there = followScope(scope.thread, remote);
      // A later cancel stops what started there since; a retry of this one, nothing new.
      const key = `cancel:${encodeURIComponent(scope.requestNamespace)}:${
        input.clientRequestId ?? (yield* crypto.randomUUIDv4().pipe(Effect.orDie))
      }`;
      if (task.result !== null) {
        // Record stop debt before contacting an unreachable or refusing peer.
        yield* threads
          .dispatch({
            type: "delegated_task.remote.stop",
            commandId: CommandId.make(`command:remote-cancel:${task.id}:${key}`),
            parentThreadId: task.threadId,
            taskId: task.id,
          })
          .pipe(
            Effect.mapError(
              () =>
                new OrchestratorMcpFailure({
                  code: "orchestration_error",
                  message: "Unable to record the remote stop request.",
                }),
            ),
          );
        const current = yield* threads.getThreadRecords(task.threadId, ["subagents"]).pipe(
          Effect.mapError(
            () =>
              new OrchestratorMcpFailure({
                code: "orchestration_error",
                message: "Unable to read the remote stop request.",
              }),
          ),
        );
        const stopCount = current.subagents.find((candidate) => candidate.id === task.id)
          ?.remoteChild?.stopCount;
        yield* stopThere(there, remote, key, reason).pipe(
          Effect.andThen(
            complete(
              task.threadId,
              task.id,
              terminalStatus(task.status),
              task.result,
              "stopped",
              stopCount,
            ),
          ),
          Effect.ignoreCause({ log: true }),
        );
        return {
          taskId: task.id,
          status:
            task.status === "completed" || task.status === "failed" || task.status === "interrupted"
              ? task.status
              : "cancelled",
        } satisfies OrchestratorMcpTaskCancelResult;
      }
      // A link that is gone or a machine that is down still cancels the task
      // here; its follower keeps trying to stop it there, across restarts.
      const unstopped = yield* stopThere(there, remote, key, reason).pipe(
        Effect.as(undefined),
        Effect.catch((error) => Effect.succeed(error.message)),
      );
      const cancelled = reason === undefined ? "Cancelled." : `Cancelled: ${reason}`;
      yield* complete(
        task.threadId,
        task.id,
        "cancelled",
        unstopped === undefined
          ? cancelled
          : `${cancelled} It could not be stopped in ${remote.label} and may still run there: ${unstopped}`,
        unstopped === undefined ? "stopped" : undefined,
        remote.stopCount,
      ).pipe(
        Effect.tapError((cause) =>
          Effect.logWarning("remote-delegation.cancel-record-failed", { taskId: task.id, cause }),
        ),
        Effect.mapError(
          () =>
            new OrchestratorMcpFailure({
              code: "orchestration_error",
              message: "Unable to record the cancellation.",
            }),
        ),
      );
      return {
        taskId: task.id,
        status: "cancel_requested",
      } satisfies OrchestratorMcpTaskCancelResult;
    },
  );

  const start: RemoteDelegation["Service"]["start"] = () =>
    forkParked(
      Effect.gen(function* () {
        yield* lifecycle.start;
        if (threads.recoverRemoteTaskQuestions !== undefined)
          yield* threads.recoverRemoteTaskQuestions.pipe(
            Effect.retry(backoff),
            Effect.forkIn(followers),
          );
        // A finished task's follower has ended, so a Stop here that owes its
        // thread there a stop starts it again. Started before the scan below;
        // a Stop that slips between the two stays owed for the next start.
        yield* Stream.runForEach(threads.streamDomainEvents, (event) =>
          event.type === "subagent.updated" && event.payload.remoteChild?.stoppedThere === false
            ? followOne(event.threadId, event.payload.id).pipe(Effect.forkIn(followers))
            : Effect.void,
        ).pipe(
          Effect.ignoreCause({ log: true }),
          Effect.forkIn(followers, { startImmediately: true }),
        );
        const open = yield* projections.getOpenRemoteDelegatedTasks;
        yield* Effect.forEach(
          open,
          ({ parentThreadId, taskId }) => followOne(parentThreadId, taskId),
          {
            concurrency: "unbounded",
            discard: true,
          },
        );
      }).pipe(Effect.ignoreCause({ log: true })),
    );

  return RemoteDelegation.of({ observe, delegate, cancel, start });
});

const isTerminal = (status: string) =>
  status === "completed" ||
  status === "failed" ||
  status === "cancelled" ||
  status === "interrupted" ||
  status === "rolled_back";

const terminalStatus = (status: string): "completed" | "failed" | "cancelled" | "interrupted" =>
  status === "completed" || status === "failed" || status === "interrupted" ? status : "cancelled";

export const layer = Layer.effect(RemoteDelegation, make);

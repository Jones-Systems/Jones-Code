import {
  CommandId,
  AuthOrchestrationOperateScope,
  type EnvironmentId,
  type OrchestrationV2ThreadProjection,
  type OrchestrationV2ThreadShell,
  type ThreadId,
} from "@t3tools/contracts";
import {
  monitorActionUnavailableReason,
  monitorReplyDispatchMode,
  monitorThreadKey,
  buildMonitorCohort,
} from "@t3tools/client-runtime/jones/thread-monitor";
import {
  runAtomCommand,
  squashAtomCommandFailure,
  type AtomCommandResult,
} from "@t3tools/client-runtime/state/runtime";
import { derivePendingThreadRequests } from "@t3tools/client-runtime/state/thread-requests";
import {
  THREAD_MONITOR_LIMITS,
  type ThreadMonitorAction,
  type ThreadMonitorActionResult,
  type ThreadMonitorPublisherFrame,
} from "@t3tools/contracts/jones/desktopThreadMonitor";
import * as Option from "effect/Option";
import type { AppRouter } from "../../router";
import { appAtomRegistry } from "../../rpc/atomRegistry";
import { threadEnvironment, environmentThreadDetails } from "../../state/threads";
import { readEnvironmentScope } from "../../state/session";
import { environmentServerConfigsAtom } from "../../state/server";
import {
  restoreObservedSettlement,
  settlementRestoreTransport,
  readEnvironmentSupportsSettlementRestore,
} from "../settlementRestore/settlementRestore";
import { findLatestProposedPlan, hasActionableProposedPlan } from "../../session-logic";
import { buildPlanImplementationPrompt } from "../../proposedPlan";
import { normalizeMonitorAnswers } from "./answers";
import { environmentSnapshotAtom } from "../../state/shell";
import { vcsEnvironment } from "../../state/vcs";
import { dispatchPlanFollowUp, planFollowUpSource } from "./planFollowUp";
import { monitorInputFor, monitorInputsAtom } from "./inputs";
import { monitorStore, refOf, type MonitorStore } from "./store";

const quiet = { reportFailure: false, reportDefect: false };
const activeStatus = (status: string) =>
  ["preparing", "queued", "starting", "running", "waiting"].includes(status);
export const monitorWorkActive = (thread: OrchestrationV2ThreadShell) =>
  activeStatus(thread.status) ||
  thread.activeRunId !== null ||
  thread.pendingRuntimeRequest !== null;
export function readMonitorSelectedProjection(ref: {
  environmentId: EnvironmentId;
  threadId: ThreadId;
}): OrchestrationV2ThreadProjection | null {
  if (
    monitorStore.selection === null ||
    monitorThreadKey(monitorStore.selection) !== monitorThreadKey(ref)
  )
    return null;
  const state = appAtomRegistry.get(environmentThreadDetails.stateAtom(ref));
  return state.status === "live" && Option.isNone(state.error)
    ? Option.getOrNull(state.data)
    : null;
}
export function monitorSavedContextUnavailableReason(
  thread: OrchestrationV2ThreadShell,
  environmentId: EnvironmentId,
): string | null {
  const provider = appAtomRegistry
    .get(environmentServerConfigsAtom)
    .get(environmentId)
    ?.providers.find((provider) => provider.instanceId === thread.providerInstanceId);
  if (
    !provider?.enabled ||
    !provider.installed ||
    provider.status === "error" ||
    provider.status === "disabled" ||
    provider.availability === "unavailable"
  )
    return "The saved provider is unavailable";
  if (
    thread.modelSelection.instanceId !== thread.providerInstanceId ||
    !provider.models.some((model) => model.slug === thread.modelSelection.model)
  )
    return "The saved model is unavailable; review this thread in Jones Code";
  if (
    provider.supportedRuntimeModes !== undefined &&
    !provider.supportedRuntimeModes.includes(thread.runtimeMode)
  )
    return "The saved runtime mode is unavailable";
  if (thread.interactionMode === "plan" && provider.showInteractionModeToggle !== true)
    return "The saved interaction mode is unavailable";
  return null;
}

export function monitorPlanUnavailableReason(
  thread: OrchestrationV2ThreadShell,
  projection: OrchestrationV2ThreadProjection | null,
  environmentId = monitorStore.selection?.environmentId,
): string | null {
  if (!projection) return "Waiting for current thread detail";
  if (
    monitorWorkActive(thread) ||
    projection.runtimeRequests.some((request) => request.status === "pending") ||
    projection.runs.some((run) => activeStatus(run.status))
  )
    return "Resolve pending requests and work before using this plan";
  const config = appAtomRegistry
    .get(environmentServerConfigsAtom)
    .get(environmentId as EnvironmentId);
  const provider = config?.providers.find(
    (candidate) => candidate.instanceId === thread.providerInstanceId,
  );
  if (
    !provider?.enabled ||
    !provider.installed ||
    provider.status === "error" ||
    provider.status === "disabled" ||
    provider.availability === "unavailable"
  )
    return "The saved provider is unavailable";
  if (!provider.models.some((model) => model.slug === thread.modelSelection.model))
    return "The saved model is unavailable; review this thread in Jones Code";
  if (provider.showInteractionModeToggle !== true)
    return "The provider does not offer the required plan follow-up mode";
  if (
    provider.supportedRuntimeModes !== undefined &&
    !provider.supportedRuntimeModes.includes(thread.runtimeMode)
  )
    return "The saved runtime mode is unavailable";
  return null;
}

class MonitorPreflightUnavailable extends Error {}

export class MonitorActionController {
  private get ledger() {
    return this.store.actionLedger;
  }
  constructor(
    private readonly router: AppRouter,
    private readonly publish: (frame: ThreadMonitorPublisherFrame) => Promise<void>,
    private readonly store: MonitorStore = monitorStore,
  ) {}
  private async result(
    action: ThreadMonitorAction,
    status: ThreadMonitorActionResult["status"],
    reason: string | null,
  ) {
    if (!this.store.fence.accepts(action)) return;
    if (!this.ledger.transition(action.actionId, status === "unavailable" ? "rejected" : status))
      return;
    const result: ThreadMonitorActionResult = {
      ...refOf(action),
      type: "actionResult",
      actionId: action.actionId,
      commandId: action.commandId,
      status,
      reason,
      ...("draftRevision" in action ? { submittedDraftRevision: action.draftRevision } : {}),
      ...(action.type === "settle" && status === "accepted"
        ? { settleCommandId: action.commandId }
        : {}),
    };
    const draft = this.store.result(result);
    await this.publish(result);
    if (draft) await this.publish({ type: "draft", draft });
  }
  private validate(action: ThreadMonitorAction) {
    const input = monitorInputFor(appAtomRegistry.get(monitorInputsAtom), action.environmentId);
    const thread = input?.threads.find(
      (thread) =>
        thread.id === action.threadId && thread.deletedAt === null && thread.archivedAt === null,
    );
    const availability = monitorActionUnavailableReason(action, {
      acceptsLifetime: (ref) => this.store.fence.accepts(ref),
      threadExists: !!thread,
      transportConnected: input?.environment.transport === "connected",
      snapshotLive: input?.environment.freshness === "live",
      canOperate: readEnvironmentScope(action.environmentId, AuthOrchestrationOperateScope),
      canQueue: input?.canQueue === true,
      workOrRequestActive: thread ? monitorWorkActive(thread) : false,
    });
    const requiresCohort =
      action.type !== "open" && action.type !== "restore" && action.type !== "reconcile";
    const outsideCohort =
      thread &&
      (thread.settledAt !== null ||
        thread.settledOverride === "settled" ||
        thread.lineage.relationshipToParent === "subagent" ||
        input?.delegatedUnderParent?.has(`${action.environmentId}:${thread.id}`) ||
        input?.delegatedUnderParent?.has(monitorThreadKey(action)));
    const reason =
      availability ??
      (requiresCohort && outsideCohort
        ? "This thread is no longer an eligible monitor target. Open it in Jones Code."
        : null);
    return { input, thread, reason };
  }
  async dispatch(action: ThreadMonitorAction): Promise<void> {
    if (!this.store.fence.accepts(action)) return;
    const admitted = this.ledger.admit(action);
    if (admitted === "duplicate") {
      const prior = this.store.results.get(action.actionId);
      if (prior) await this.publish(prior);
      return;
    }
    if (admitted !== "new") {
      await this.publish({
        ...refOf(action),
        type: "actionResult",
        actionId: action.actionId,
        commandId: action.commandId,
        status: "unavailable",
        reason:
          admitted === "full"
            ? "Monitor action history is full. Reopen Jones Code before another mutation."
            : "Action identity conflicts with an earlier command",
      });
      return;
    }
    this.store.recordIdentity(action);
    const current = this.validate(action);
    if (current.reason || !current.thread || !current.input) {
      await this.result(action, "unavailable", current.reason ?? "Thread is unavailable");
      return;
    }
    if (
      action.type !== "open" &&
      action.type !== "reconcile" &&
      this.store.pendingThreads(action.environmentId).has(action.threadId)
    ) {
      await this.result(
        action,
        "unavailable",
        "An earlier action is pending or unknown. Reconcile its exact identity first.",
      );
      return;
    }
    if (action.type === "send") {
      const reason = monitorSavedContextUnavailableReason(current.thread, action.environmentId);
      if (reason) {
        await this.result(action, "unavailable", reason);
        return;
      }
    }
    if (action.type === "reconcile") {
      await this.reconcile(action);
      return;
    }
    if (action.type === "open") {
      try {
        await this.router.navigate({
          to: "/$environmentId/$threadId",
          params: { environmentId: action.environmentId, threadId: action.threadId },
        });
        await this.result(action, "accepted", null);
      } catch {
        await this.result(action, "unavailable", "Thread navigation is unavailable");
      }
      return;
    }
    const row = buildMonitorCohort(
      [{ ...current.input, pendingActionThreadIds: new Set() }],
      Date.now(),
    ).rows.find((row) => row.threadId === action.threadId);
    if (
      action.type === "settle" &&
      (!row?.actions.settle.available || row.updatedAt !== action.expectedUpdatedAt)
    ) {
      await this.result(
        action,
        "unavailable",
        row?.actions.settle.reason ?? "Thread changed; review its current state before settling",
      );
      return;
    }
    const projection = readMonitorSelectedProjection(action);
    if (action.type === "respondToApproval") {
      const approval =
        projection &&
        derivePendingThreadRequests(projection).approvals.find(
          (request) => request.requestId === action.requestId,
        );
      if (
        !approval ||
        approval.responseCapability !== "live" ||
        !approval.options?.some((option) => option.decision === action.decision)
      ) {
        await this.result(
          action,
          "unavailable",
          "This exact approval decision is no longer offered",
        );
        return;
      }
    }
    if (
      action.type === "respondToUserInput" &&
      (!projection || normalizeMonitorAnswers(projection, action) === null)
    ) {
      await this.result(
        action,
        "unavailable",
        "Answer every current question with its offered choices or supported custom text",
      );
      return;
    }
    if (action.type === "usePlan") {
      const plan = findLatestProposedPlan(projection, projection?.runs.at(-1)?.id);
      const reason = monitorPlanUnavailableReason(current.thread, projection);
      if (reason || !hasActionableProposedPlan(plan) || plan?.id !== action.planId) {
        await this.result(action, "unavailable", reason ?? "The current plan has changed");
        return;
      }
    }
    if (action.type === "restore") {
      const observed = [...this.store.results.values()].find(
        (result) =>
          result.status === "accepted" &&
          result.settleCommandId === action.settleCommandId &&
          monitorThreadKey(result) === monitorThreadKey(action),
      );
      if (!observed || !readEnvironmentSupportsSettlementRestore(action.environmentId)) {
        await this.result(action, "unavailable", "Exact settlement restoration is unavailable");
        return;
      }
    }
    const timeout = setTimeout(() => {
      void this.result(
        action,
        "unknown",
        "Acceptance is unknown. Reconcile the original command; do not resend with a new identity.",
      );
    }, THREAD_MONITOR_LIMITS.actionTimeoutMs);
    try {
      await this.result(action, "pending", "Awaiting durable command acceptance");
      const result = await this.execute(action, current.thread, projection);
      if (result._tag === "Success")
        await this.result(
          action,
          "accepted",
          "Command accepted. Provider work may still be pending.",
        );
      else {
        const error = squashAtomCommandFailure(result);
        const tag =
          typeof error === "object" && error !== null && "_tag" in error ? error._tag : null;
        const rejected =
          tag === "OrchestrationDispatchCommandError" ||
          tag === "EnvironmentAuthorizationError" ||
          tag === "MonitorGuardedSettleUnavailable";
        await this.result(
          action,
          rejected ? "rejected" : "unknown",
          rejected
            ? "The server rejected this command. Your draft is kept."
            : "The command’s acceptance could not be proved. Reconcile its original identity.",
        );
      }
    } catch (error) {
      await this.result(
        action,
        error instanceof MonitorPreflightUnavailable ? "unavailable" : "unknown",
        error instanceof MonitorPreflightUnavailable
          ? error.message
          : "Command acceptance is unknown. Your draft and original command identity are kept.",
      );
    } finally {
      clearTimeout(timeout);
    }
  }
  private async execute(
    action: Exclude<ThreadMonitorAction, { type: "open" | "reconcile" }>,
    thread: OrchestrationV2ThreadShell,
    projection: OrchestrationV2ThreadProjection | null,
  ): Promise<AtomCommandResult<unknown, unknown>> {
    const target = { environmentId: action.environmentId };
    const base = { threadId: action.threadId, commandId: action.commandId };
    const denied = (preflight = true) => {
      const current = this.validate(action);
      const reason =
        current.reason ??
        (current.thread && (action.type === "send" || action.type === "usePlan")
          ? monitorSavedContextUnavailableReason(current.thread, action.environmentId)
          : null);
      if (reason) throw preflight ? new MonitorPreflightUnavailable(reason) : new Error(reason);
    };
    denied();
    switch (action.type) {
      case "send":
        return runAtomCommand(
          appAtomRegistry,
          threadEnvironment.startTurn,
          {
            ...target,
            input: {
              ...base,
              message: {
                messageId: action.messageId,
                role: "user",
                text: action.text,
                attachments: [],
              },
              modelSelection: thread.modelSelection,
              runtimeMode: thread.runtimeMode,
              interactionMode: thread.interactionMode,
              dispatchMode: monitorReplyDispatchMode(monitorWorkActive(thread)),
            },
          },
          quiet,
        );
      case "settle":
        return runAtomCommand(
          appAtomRegistry,
          threadEnvironment.settle,
          {
            ...target,
            input: {
              ...base,
              guard: {
                policy: "preserve_pending_work",
                expectedUpdatedAt: action.expectedUpdatedAt,
              },
            },
          },
          quiet,
        );
      case "restore":
        return restoreObservedSettlement(
          { environmentId: action.environmentId, threadId: action.threadId },
          action.settleCommandId,
          { settleWasNoop: false },
        );
      case "respondToApproval": {
        const current = readMonitorSelectedProjection(action);
        const request =
          current &&
          derivePendingThreadRequests(current).approvals.find(
            (request) => request.requestId === action.requestId,
          );
        if (
          !request ||
          request.responseCapability !== "live" ||
          !request.options?.some((option) => option.decision === action.decision)
        )
          throw new MonitorPreflightUnavailable("The offered approval changed before dispatch");
        return runAtomCommand(
          appAtomRegistry,
          threadEnvironment.respondToApproval,
          { ...target, input: { ...base, requestId: action.requestId, decision: action.decision } },
          quiet,
        );
      }
      case "respondToUserInput": {
        const current = readMonitorSelectedProjection(action);
        const answers = current && normalizeMonitorAnswers(current, action);
        if (!answers)
          throw new MonitorPreflightUnavailable("The current questions changed before dispatch");
        return runAtomCommand(
          appAtomRegistry,
          threadEnvironment.respondToUserInput,
          { ...target, input: { ...base, requestId: action.requestId, answers } },
          quiet,
        );
      }
      case "usePlan": {
        const plan = findLatestProposedPlan(projection, projection?.runs.at(-1)?.id)!;
        const project = appAtomRegistry
          .get(environmentSnapshotAtom(action.environmentId))
          ?.projects.find((project) => project.id === thread.projectId);
        const cwd = thread.worktreePath ?? project?.workspaceRoot;
        if (!cwd) throw new MonitorPreflightUnavailable("Plan workspace is unavailable");
        {
          const refs = await runAtomCommand(
            appAtomRegistry,
            vcsEnvironment.readRefs,
            { ...target, input: { cwd, limit: 100 } },
            quiet,
          );
          if (refs._tag !== "Success")
            throw new MonitorPreflightUnavailable("Cannot verify the plan workspace");
          if (
            refs.value.isRepo &&
            (thread.branch === null ||
              refs.value.refs.find((ref) => ref.current)?.name !== thread.branch)
          )
            throw new MonitorPreflightUnavailable(
              "The current checkout branch differs from the saved plan workspace. Review it in Jones Code.",
            );
        }
        denied();
        const freshProjection = readMonitorSelectedProjection(action);
        const freshThread = monitorInputFor(
          appAtomRegistry.get(monitorInputsAtom),
          action.environmentId,
        )?.threads.find((candidate) => candidate.id === thread.id);
        if (
          !freshThread ||
          monitorPlanUnavailableReason(freshThread, freshProjection) !== null ||
          findLatestProposedPlan(freshProjection, freshProjection?.runs.at(-1)?.id)?.id !==
            action.planId
        )
          throw new MonitorPreflightUnavailable("Plan context changed");
        return dispatchPlanFollowUp({
          persistSettings: () =>
            runAtomCommand(
              appAtomRegistry,
              threadEnvironment.setInteractionMode,
              {
                ...target,
                input: {
                  threadId: action.threadId,
                  commandId: CommandId.make(`${action.commandId}:interaction`),
                  interactionMode: "default",
                },
              },
              quiet,
            ),
          onSettingsPersisted: () => {},
          startTurn: () => {
            denied(false);
            return runAtomCommand(
              appAtomRegistry,
              threadEnvironment.startTurn,
              {
                ...target,
                input: {
                  ...base,
                  message: {
                    messageId: action.messageId,
                    role: "user",
                    text: buildPlanImplementationPrompt(plan.planMarkdown),
                    attachments: [],
                  },
                  modelSelection: thread.modelSelection,
                  runtimeMode: thread.runtimeMode,
                  interactionMode: "default",
                  dispatchMode: monitorReplyDispatchMode(false),
                  ...planFollowUpSource("default", thread.id, plan.id),
                },
              },
              quiet,
            );
          },
        });
      }
    }
  }
  private async reconcile(action: Extract<ThreadMonitorAction, { type: "reconcile" }>) {
    const original = this.store.identities.get(action.originalActionId);
    if (
      !original ||
      original.commandId !== action.originalCommandId ||
      monitorThreadKey(original) !== monitorThreadKey(action)
    ) {
      await this.result(action, "unavailable", "Original command identity is unavailable");
      return;
    }
    const known = this.store.results.get(original.actionId);
    if (known?.status === "accepted" || known?.status === "rejected") {
      await this.publish(known);
      await this.result(action, "accepted", "Original outcome is known");
      return;
    }
    if (original.type === "restore" && original.settleCommandId !== undefined) {
      const restored = await restoreObservedSettlement(
        { environmentId: action.environmentId, threadId: action.threadId },
        original.settleCommandId,
        { settleWasNoop: false },
      );
      if (restored._tag === "Success") {
        const result: ThreadMonitorActionResult = {
          ...refOf(action),
          type: "actionResult",
          actionId: original.actionId,
          commandId: original.commandId,
          status: "accepted",
          reason: "Exact placement restoration reconciled",
        };
        if (this.store.fence.accepts(action)) {
          this.ledger.transition(original.actionId, "accepted");
          this.store.result(result);
          await this.publish(result);
          await this.result(action, "accepted", "Original placement restoration is confirmed");
        }
        return;
      }
    }
    if (
      original.type === "settle" &&
      readEnvironmentSupportsSettlementRestore(action.environmentId)
    ) {
      const inspected = await settlementRestoreTransport.inspect(action.environmentId, {
        threadId: action.threadId,
        settleCommandId: original.commandId,
      });
      if (
        inspected._tag === "Success" &&
        inspected.value.record?.settleCommandId === original.commandId
      ) {
        const result: ThreadMonitorActionResult = {
          ...refOf(action),
          type: "actionResult",
          actionId: original.actionId,
          commandId: original.commandId,
          status: "accepted",
          reason: "The exact settle command is persisted",
          settleCommandId: original.commandId,
        };
        if (this.store.fence.accepts(action)) {
          this.ledger.transition(original.actionId, "accepted");
          this.store.result(result);
          await this.publish(result);
          await this.result(action, "accepted", "Original settlement is confirmed");
        }
        return;
      }
    }
    const projection = readMonitorSelectedProjection(action);
    if (
      (original.type === "send" || original.type === "usePlan") &&
      (action.messageId === undefined || original.messageId === action.messageId) &&
      projection?.messages.some(
        (message) => message.id === original.messageId && message.role === "user",
      )
    ) {
      const result: ThreadMonitorActionResult = {
        ...refOf(action),
        type: "actionResult",
        actionId: original.actionId,
        commandId: original.commandId,
        status: "accepted",
        reason: "The exact submitted message is persisted",
        ...(original.draftRevision === undefined
          ? {}
          : { submittedDraftRevision: original.draftRevision }),
      };
      this.ledger.transition(original.actionId, "accepted");
      const draft = this.store.result(result);
      await this.publish(result);
      if (draft) await this.publish({ type: "draft", draft });
      await this.result(action, "accepted", "Exact message identity reconciled");
      return;
    }
    await this.result(
      action,
      "unavailable",
      "The original outcome remains unknown. Open current detail and reconcile again; no command has been resent.",
    );
  }
}

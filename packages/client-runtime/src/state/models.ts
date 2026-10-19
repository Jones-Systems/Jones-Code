import {
  shellAttentionInput,
  type ThreadRunSummary,
  type ThreadRuntimeSummary,
} from "@t3tools/shared/jones/threadAttention";
export type { ThreadRunSummary, ThreadRuntimeSummary } from "@t3tools/shared/jones/threadAttention";
import { threadPullRequestsOf } from "@t3tools/shared/threadPullRequests";
import type {
  ThreadLinkedPullRequest,
  EnvironmentId,
  OrchestrationProjectShell,
  OrchestrationV2ProviderGoal,
  OrchestrationV2ThreadProjection,
  OrchestrationV2ThreadShell,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

import { formatSubagentDisplayTitle } from "./subagentDisplay.ts";

export interface EnvironmentProject extends OrchestrationProjectShell {
  readonly environmentId: EnvironmentId;
  readonly isScratch?: true;
}

/**
 * The projection stays nested so the server projection and all structurally
 * shared collections retain their identities.
 */
export interface EnvironmentThread {
  readonly environmentId: EnvironmentId;
  readonly projection: OrchestrationV2ThreadProjection;
}

export function threadRuntimeIsActive(runtime: ThreadRuntimeSummary | null | undefined): boolean {
  return runtime !== null && runtime !== undefined && threadRunStatusIsActive(runtime.status);
}

export function threadRuntimeCanArchive(runtime: ThreadRuntimeSummary | null | undefined): boolean {
  if (runtime?.status === "queued") return runtime.activeRunId === null;
  return (
    runtime?.status !== "preparing" &&
    runtime?.status !== "starting" &&
    runtime?.status !== "running"
  );
}

function threadRunStatusIsActive(status: ThreadRuntimeSummary["status"]): boolean {
  return (
    status === "preparing" ||
    status === "queued" ||
    status === "starting" ||
    status === "running" ||
    status === "waiting"
  );
}

export interface EnvironmentThreadShell {
  readonly runtimeIdentity?: OrchestrationV2ThreadShell["runtimeIdentity"];
  readonly environmentId: EnvironmentId;
  readonly id: ThreadId;
  readonly projectId: ProjectId;
  readonly title: string;
  readonly providerInstanceId: ProviderInstanceId;
  readonly modelSelection: OrchestrationV2ThreadShell["modelSelection"];
  readonly runtimeMode: OrchestrationV2ThreadShell["runtimeMode"];
  readonly interactionMode: OrchestrationV2ThreadShell["interactionMode"];
  readonly branch: string | null;
  readonly worktreePath: string | null;
  readonly lineage: OrchestrationV2ThreadShell["lineage"];
  readonly forkedFrom: OrchestrationV2ThreadShell["forkedFrom"];
  readonly activeProviderThreadId: OrchestrationV2ThreadShell["activeProviderThreadId"];
  readonly latestRun: ThreadRunSummary | null;
  readonly runtime: ThreadRuntimeSummary | null;
  readonly latestUserMessageAt: string | null;
  readonly latestUserAuthoredMessageAt?: string | null;
  readonly hasPendingApprovals: boolean;
  readonly hasPendingUserInput: boolean;
  readonly pendingSubagentRequest?: PendingSubagentRequest | null;
  readonly hasActionableProposedPlan: boolean;
  readonly pendingBackgroundTasks: ReadonlyArray<
    NonNullable<OrchestrationV2ThreadShell["pendingBackgroundTasks"]>[number]
  >;
  readonly providerInstanceHistory: ReadonlyArray<ProviderInstanceId>;
  readonly goal: OrchestrationV2ProviderGoal | null;
  readonly itemCount: number;
  readonly visibleItemCount: number;
  readonly createdAt: string;
  readonly updatedAt: string;
  readonly archivedAt: string | null;
  readonly settledOverride: "settled" | "active" | null;
  readonly settledAt: string | null;
  readonly unsettledAt: string | null;
  readonly snoozedUntil: string | null;
  readonly snoozedAt: string | null;
  readonly limitRecovery?: import("@t3tools/contracts").OrchestrationV2LimitRecovery | null;
  readonly pinnedAt: string | null;
  readonly autoSettleDisabledAt?: string | null;
  readonly pinOrderKey: string | null;
  readonly activeOrderKey: string | null;
  readonly pullRequests: ReadonlyArray<import("@t3tools/contracts").ThreadPullRequestLink>;
  readonly linkedPullRequest?: ThreadLinkedPullRequest | null;
  readonly branchPullRequest?: ThreadLinkedPullRequest | null;
  /**
   * Server-tracked visited watermark. `undefined` means the environment's
   * server predates visited tracking and clients should fall back to any
   * local visited state they keep.
   */
  readonly lastVisitedAt?: string | null;
  readonly titleRegeneration?: { readonly requestId: string; readonly startedAt: string } | null;
  readonly deletedAt: string | null;
  readonly source: OrchestrationV2ThreadShell;
}

function iso(value: DateTime.Utc): string {
  return DateTime.formatIso(value);
}

function nullableIso(value: DateTime.Utc | null): string | null {
  return value === null ? null : iso(value);
}

export function scopeProject(
  environmentId: EnvironmentId,
  project: OrchestrationProjectShell,
): EnvironmentProject {
  return { ...project, environmentId };
}

export type PendingSubagentRequest = "approval" | "user_input";

export function presentThreadShell(
  environmentId: EnvironmentId,
  thread: OrchestrationV2ThreadShell,
  pendingSubagentRequest: PendingSubagentRequest | null = null,
): EnvironmentThreadShell {
  const updatedAt = iso(thread.updatedAt);
  const attention = shellAttentionInput(thread, pendingSubagentRequest);
  return {
    environmentId,
    id: thread.id,
    projectId: thread.projectId,
    title:
      thread.lineage.relationshipToParent === "subagent"
        ? formatSubagentDisplayTitle(thread.title)
        : thread.title,
    providerInstanceId: thread.providerInstanceId,
    modelSelection: thread.modelSelection,
    runtimeMode: thread.runtimeMode,
    interactionMode: thread.interactionMode,
    branch: thread.branch,
    worktreePath: thread.worktreePath,
    pullRequests: threadPullRequestsOf(thread),
    linkedPullRequest: thread.linkedPullRequest ?? null,
    branchPullRequest: thread.branchPullRequest ?? null,
    lineage: thread.lineage,
    forkedFrom: thread.forkedFrom,
    activeProviderThreadId: thread.activeProviderThreadId,
    ...(thread.runtimeIdentity === undefined ? {} : { runtimeIdentity: thread.runtimeIdentity }),
    latestRun: attention.latestRun,
    runtime: attention.runtime,
    latestUserMessageAt: nullableIso(thread.latestUserMessageAt),
    ...(thread.latestUserAuthoredMessageAt === undefined
      ? {}
      : { latestUserAuthoredMessageAt: nullableIso(thread.latestUserAuthoredMessageAt) }),
    hasPendingApprovals: attention.hasPendingApprovals,
    hasPendingUserInput: attention.hasPendingUserInput,
    pendingSubagentRequest,
    hasActionableProposedPlan: thread.hasActionableProposedPlan,
    pendingBackgroundTasks: thread.pendingBackgroundTasks ?? [],
    providerInstanceHistory: thread.providerInstanceHistory ?? [],
    goal: thread.goal ?? null,
    itemCount: thread.itemCount,
    visibleItemCount: thread.visibleItemCount,
    createdAt: iso(thread.createdAt),
    updatedAt,
    archivedAt: nullableIso(thread.archivedAt),
    settledOverride: thread.settledOverride,
    settledAt: nullableIso(thread.settledAt),
    unsettledAt: nullableIso(thread.unsettledAt ?? null),
    snoozedUntil: nullableIso(thread.snoozedUntil ?? null),
    snoozedAt: nullableIso(thread.snoozedAt ?? null),
    limitRecovery: thread.limitRecovery ?? null,
    pinnedAt: nullableIso(thread.pinnedAt ?? null),
    autoSettleDisabledAt: nullableIso(thread.autoSettleDisabledAt ?? null),
    pinOrderKey: thread.pinOrderKey ?? null,
    activeOrderKey: thread.activeOrderKey ?? null,
    ...(thread.lastVisitedAt === undefined
      ? {}
      : { lastVisitedAt: nullableIso(thread.lastVisitedAt) }),
    titleRegeneration:
      thread.titleRegeneration == null
        ? null
        : {
            requestId: thread.titleRegeneration.requestId,
            startedAt: iso(thread.titleRegeneration.startedAt),
          },
    deletedAt: nullableIso(thread.deletedAt),
    source: thread,
  };
}

export function scopeThreadShell(
  environmentId: EnvironmentId,
  thread: OrchestrationV2ThreadShell,
): EnvironmentThreadShell {
  return presentThreadShell(environmentId, thread);
}

const THREAD_PROVIDER_STACK_LIMIT = 3;

export function resolveThreadProviderStack(
  thread: Pick<EnvironmentThreadShell, "providerInstanceHistory" | "modelSelection" | "runtime">,
): ReadonlyArray<ProviderInstanceId> {
  const current = thread.runtime?.providerInstanceId ?? thread.modelSelection.instanceId;
  const previous = thread.providerInstanceHistory.filter((instanceId) => instanceId !== current);
  return [...previous.slice(-(THREAD_PROVIDER_STACK_LIMIT - 1)), current];
}

/**
 * Both shell and detail timers count from the activity-owning run's work
 * start, never last activity. A wake keeps the start of the work it continues.
 */
export function resolveThreadWorkingStartedAt(input: {
  readonly latestRun: Pick<
    ThreadRunSummary,
    "runId" | "startedAt" | "requestedAt" | "completedAt"
  > | null;
  readonly runtime: Pick<ThreadRuntimeSummary, "activeRunId" | "activityStartedAt"> | null;
}): string | null {
  const valid = (value: string | null | undefined) =>
    value != null && Number.isFinite(Date.parse(value)) ? value : null;
  if (input.runtime?.activityStartedAt !== undefined) return valid(input.runtime.activityStartedAt);
  const run = input.latestRun;
  if (run?.completedAt === null && run.runId === input.runtime?.activeRunId) {
    return valid(run.startedAt) ?? valid(run.requestedAt);
  }
  return null;
}

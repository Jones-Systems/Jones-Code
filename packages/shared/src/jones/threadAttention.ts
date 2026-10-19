import type {
  MessageId,
  OrchestrationV2RunStatus,
  OrchestrationV2ProviderSettlement,
  OrchestrationV2ProviderFailureClass,
  OrchestrationV2ThreadShell,
  PlanId,
  ProviderInstanceId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import { isProviderNativeSubagentThread } from "@t3tools/contracts";
import type {
  ThreadAttentionStatus,
  ThreadAttentionPill,
} from "@t3tools/contracts/jones/threadAttention";
import * as DateTime from "effect/DateTime";
import { backgroundWorkHoldsCompletion } from "../orchestrationV2PendingBackgroundWork.ts";

export type PendingSubagentRequest = "approval" | "user_input";
const EMPTY_SUBAGENT_REQUESTS: ReadonlyMap<ThreadId, PendingSubagentRequest> = new Map();

export interface ThreadRunSummary {
  readonly runId: RunId;
  readonly status: OrchestrationV2RunStatus;
  readonly requestedAt: string | null;
  readonly startedAt: string | null;
  readonly completedAt: string | null;
  readonly providerSettlement?:
    | (Omit<OrchestrationV2ProviderSettlement, "completedAt"> & { readonly completedAt: string })
    | null;
  readonly assistantMessageId: MessageId | null;
  readonly sourcePlanRef?: {
    readonly threadId: ThreadId;
    readonly planId: PlanId;
  };
}

export interface ThreadRuntimeSummary {
  readonly status: OrchestrationV2RunStatus | "idle";
  readonly activeRunId: RunId | null;
  readonly activityStartedAt?: string | null | undefined;
  readonly providerInstanceId: ProviderInstanceId;
  readonly providerName: string | null;
  readonly lastError: string | null;
  readonly lastErrorClass?: OrchestrationV2ProviderFailureClass | null;
  readonly usageLimitResetAt?: string | null;
  readonly updatedAt: string;
}

function iso(value: DateTime.Utc): string {
  return DateTime.formatIso(value);
}

function nullableIso(value: DateTime.Utc | null): string | null {
  return value === null ? null : iso(value);
}

function terminalRunStatus(status: OrchestrationV2RunStatus): boolean {
  return (
    status === "completed" ||
    status === "interrupted" ||
    status === "failed" ||
    status === "cancelled" ||
    status === "rolled_back"
  );
}

// Only work that wakes the agent holds it: commands it left running, such as a
// dev server, present the run's own status (#14872). The server suppresses the
// roster while an interruptible activity run exists, so a remaining roster is
// stronger than checkpoint-oriented waiting.
function shellRuntime(thread: OrchestrationV2ThreadShell): ThreadRuntimeSummary | null {
  const parkAtIdle =
    backgroundWorkHoldsCompletion(thread.pendingBackgroundTasks ?? []) &&
    thread.status !== "failed";
  if (thread.latestRunId === null && thread.activeProviderThreadId === null && !parkAtIdle) {
    return null;
  }
  const status = parkAtIdle ? "idle" : (thread.activityRunStatus ?? thread.status);
  return {
    status,
    activeRunId: thread.activeRunId,
    activityStartedAt:
      thread.activityRunStartedAt === undefined
        ? undefined
        : nullableIso(thread.activityRunStartedAt),
    providerInstanceId: thread.providerInstanceId,
    providerName: null,
    lastError: thread.lastError ?? null,
    lastErrorClass: thread.lastErrorClass ?? null,
    usageLimitResetAt: thread.usageLimitResetAt ?? null,
    updatedAt: iso(thread.updatedAt),
  };
}

function shellLatestRun(thread: OrchestrationV2ThreadShell): ThreadRunSummary | null {
  const updatedAt = iso(thread.updatedAt);
  const latestRun =
    thread.latestRunId === null
      ? null
      : ({
          runId: thread.latestRunId,
          status: thread.status === "idle" ? "completed" : thread.status,
          requestedAt: nullableIso(thread.latestRunRequestedAt ?? null),
          startedAt: nullableIso(thread.latestRunStartedAt ?? null),
          completedAt:
            thread.latestRunCompletedAt === undefined
              ? thread.status === "idle" || terminalRunStatus(thread.status)
                ? updatedAt
                : null
              : nullableIso(thread.latestRunCompletedAt),
          ...(thread.latestRunProviderSettlement === undefined
            ? {}
            : {
                providerSettlement:
                  thread.latestRunProviderSettlement === null
                    ? null
                    : {
                        ...thread.latestRunProviderSettlement,
                        completedAt: iso(thread.latestRunProviderSettlement.completedAt),
                      },
              }),
          assistantMessageId: null,
        } satisfies ThreadRunSummary);
  return latestRun;
}
export interface ThreadAttentionInput {
  readonly hasPendingApprovals: boolean;
  readonly hasPendingUserInput: boolean;
  readonly pendingSubagentRequest?: PendingSubagentRequest | null;
  readonly interactionMode: OrchestrationV2ThreadShell["interactionMode"];
  readonly hasActionableProposedPlan: boolean;
  readonly latestRun: ThreadRunSummary | null;
  readonly runtime: ThreadRuntimeSummary | null;
  readonly lastVisitedAt?: string | null | undefined;
  readonly pendingBackgroundTasks?:
    | OrchestrationV2ThreadShell["pendingBackgroundTasks"]
    | undefined;
}

export function shellAttentionInput(
  thread: OrchestrationV2ThreadShell,
  pendingSubagentRequest: PendingSubagentRequest | null = null,
): ThreadAttentionInput {
  return {
    latestRun: shellLatestRun(thread),
    runtime: shellRuntime(thread),
    hasPendingApprovals:
      thread.pendingRuntimeRequest !== null &&
      thread.pendingRuntimeRequest.kind !== "user_input" &&
      thread.pendingRuntimeRequest.kind !== "auth_refresh",
    hasPendingUserInput: thread.pendingRuntimeRequest?.kind === "user_input",
    pendingSubagentRequest,
    interactionMode: thread.interactionMode,
    hasActionableProposedPlan: thread.hasActionableProposedPlan,
    pendingBackgroundTasks: thread.pendingBackgroundTasks,
    lastVisitedAt: thread.lastVisitedAt == null ? thread.lastVisitedAt : iso(thread.lastVisitedAt),
  };
}

function isLatestRunSettled(
  latestRun: Pick<ThreadRunSummary, "runId" | "startedAt" | "completedAt" | "status"> | null,
  runtime: Pick<ThreadRuntimeSummary, "status" | "activeRunId"> | null,
): boolean {
  if (latestRun === null) return false;
  if (
    latestRun.status === "preparing" ||
    latestRun.status === "queued" ||
    latestRun.status === "starting" ||
    latestRun.status === "running" ||
    latestRun.status === "waiting"
  )
    return false;
  return runtime?.activeRunId !== latestRun.runId;
}

export function hasUnseenCompletion(thread: ThreadAttentionInput): boolean {
  if (!thread.latestRun?.completedAt) return false;
  const completedAt = Date.parse(thread.latestRun.completedAt);
  if (Number.isNaN(completedAt)) return false;
  if (!thread.lastVisitedAt) return false;

  const lastVisitedAt = Date.parse(thread.lastVisitedAt);
  if (Number.isNaN(lastVisitedAt)) return true;
  return completedAt > lastVisitedAt;
}

export function resolveThreadAttentionStatus(
  thread: Pick<
    ThreadAttentionInput,
    "hasPendingApprovals" | "hasPendingUserInput" | "pendingSubagentRequest" | "runtime"
  >,
): ThreadAttentionStatus {
  if (thread.hasPendingApprovals || thread.pendingSubagentRequest === "approval") {
    return "approval";
  }
  if (thread.hasPendingUserInput || thread.pendingSubagentRequest === "user_input") {
    return "input";
  }
  if (
    thread.runtime !== null &&
    ["preparing", "queued", "starting", "running", "waiting"].includes(thread.runtime.status)
  ) {
    return "working";
  }
  if (thread.runtime?.status === "idle") {
    return "waiting";
  }
  if (thread.runtime?.status === "failed") {
    return thread.runtime.lastErrorClass === "usage_limit" ? "limited" : "failed";
  }
  return "ready";
}

export function resolveThreadAttentionPill(input: {
  thread: ThreadAttentionInput;
}): ThreadAttentionPill | null {
  const { thread } = input;

  if (thread.hasPendingApprovals || thread.pendingSubagentRequest === "approval") {
    return "Pending Approval";
  }

  if (thread.hasPendingUserInput || thread.pendingSubagentRequest === "user_input") {
    return "Awaiting Input";
  }

  if (thread.runtime?.status === "running" || thread.runtime?.status === "waiting") {
    return "Working";
  }

  if (
    thread.runtime?.status === "preparing" ||
    thread.runtime?.status === "starting" ||
    thread.runtime?.status === "queued"
  ) {
    return "Connecting";
  }

  if (backgroundWorkHoldsCompletion(thread.pendingBackgroundTasks ?? [])) {
    return "Waiting";
  }

  const hasPlanReadyPrompt =
    !thread.hasPendingUserInput &&
    thread.interactionMode === "plan" &&
    isLatestRunSettled(thread.latestRun, thread.runtime) &&
    thread.hasActionableProposedPlan;
  if (hasPlanReadyPrompt) {
    return "Plan Ready";
  }

  if (hasUnseenCompletion(thread)) {
    return "Completed";
  }

  return null;
}

// An async question outlives the subagent's run, but once that run ends the
// parent is no longer waiting on the answer, so it stops counting as input.
// The activity run stays set when a follow-up is queued behind a blocked run;
// older servers omit it, so fall back to the latest run's status.
const isChildRunLive = (
  thread: Pick<OrchestrationV2ThreadShell, "activityRunStatus" | "status">,
) =>
  thread.activityRunStatus !== undefined
    ? thread.activityRunStatus !== null
    : thread.status === "preparing" ||
      thread.status === "starting" ||
      thread.status === "running" ||
      thread.status === "waiting";

/** Provider-native
    subagents ask on their parent, so they are skipped. */
export function subagentRequest(thread: OrchestrationV2ThreadShell): PendingSubagentRequest | null {
  const kind = thread.pendingRuntimeRequest?.kind;
  if (
    kind === undefined ||
    kind === "auth_refresh" ||
    thread.lineage.relationshipToParent !== "subagent" ||
    isProviderNativeSubagentThread(thread) ||
    !isChildRunLive(thread)
  ) {
    return null;
  }
  return kind === "user_input" ? "user_input" : "approval";
}

export function pendingSubagentRequestsByThread(
  threads: ReadonlyArray<OrchestrationV2ThreadShell>,
): ReadonlyMap<ThreadId, PendingSubagentRequest> {
  const requests = new Map<ThreadId, PendingSubagentRequest>();
  let byId: ReadonlyMap<ThreadId, OrchestrationV2ThreadShell> | undefined;
  for (const child of threads) {
    const request = subagentRequest(child);
    if (request === null) continue;
    byId ??= new Map(threads.map((thread) => [thread.id, thread] as const));
    const seen = new Set<ThreadId>([child.id]);
    let current: OrchestrationV2ThreadShell | undefined = child;
    while (current?.lineage.relationshipToParent === "subagent") {
      const parentId: ThreadId | null = current.lineage.parentThreadId;
      if (parentId === null || seen.has(parentId)) break;
      seen.add(parentId);
      if (requests.get(parentId) !== "approval") requests.set(parentId, request);
      current = byId.get(parentId);
    }
  }
  return requests.size === 0 ? EMPTY_SUBAGENT_REQUESTS : requests;
}

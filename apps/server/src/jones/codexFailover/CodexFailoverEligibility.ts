import type {
  MessageId,
  ModelSelection,
  OrchestrationV2AppThread,
  OrchestrationV2ProviderThread,
  OrchestrationV2Run,
  OrchestrationV2RunAttempt,
  ProviderInstanceId,
  ProviderThreadId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import { modelSelectionsEqual } from "@t3tools/shared/model";
import { latestExecutedRun } from "@t3tools/shared/orchestrationV2ThreadError";
import * as DateTime from "effect/DateTime";

import type { CodexFailoverEvidenceInput } from "./CodexFailoverEvidence.ts";
import type { CodexFailoverFailure } from "../provider/codex/CodexFailoverFailure.ts";

export type CodexFailoverEvidence = Pick<
  CodexFailoverEvidenceInput,
  | "providerTurnId"
  | "threadId"
  | "runId"
  | "attemptId"
  | "providerThreadId"
  | "providerInstanceId"
  | "occurredAt"
> & { readonly disposition: CodexFailoverFailure };

export interface CodexFailoverProjection {
  readonly thread: Pick<
    OrchestrationV2AppThread,
    | "id"
    | "modelSelection"
    | "activeProviderThreadId"
    | "archivedAt"
    | "deletedAt"
    | "settledAt"
    | "settledOverride"
    | "snoozedUntil"
  >;
  readonly runs: ReadonlyArray<OrchestrationV2Run>;
  readonly attempts: ReadonlyArray<
    Pick<
      OrchestrationV2RunAttempt,
      "id" | "runId" | "providerInstanceId" | "providerThreadId" | "providerSettlement"
    >
  >;
  readonly providerThreads: ReadonlyArray<
    Pick<
      OrchestrationV2ProviderThread,
      "id" | "appThreadId" | "ownerNodeId" | "driver" | "providerInstanceId" | "nativeThreadRef"
    >
  >;
}

export interface CodexFailoverFacts {
  readonly driver: string | undefined;
  readonly stopped: boolean;
  readonly pendingRequest: boolean;
  readonly worktreePresent: boolean;
  readonly sourceQuiet: boolean;
  readonly localFailure?: boolean;
  readonly now: number;
  readonly startup?: boolean;
}

export type CodexFailoverSkipReason =
  | "stale"
  | "not_failed"
  | "local_failure"
  | "not_latest"
  | "not_codex"
  | "selection_moved"
  | "provider_thread_changed"
  | "no_native_ref"
  | "stopped"
  | "pending_request"
  | "inactive_thread"
  | "snoozed"
  | "worktree_missing";

export type CodexFailoverEligibility =
  | { readonly type: "eligible" }
  | { readonly type: "defer"; readonly reason: "source_busy" }
  | { readonly type: "skip"; readonly reason: CodexFailoverSkipReason };

function activeThread(thread: CodexFailoverProjection["thread"]): boolean {
  return (
    thread.archivedAt === null &&
    thread.deletedAt === null &&
    thread.settledAt === null &&
    thread.settledOverride !== "settled"
  );
}

function snoozed(thread: CodexFailoverProjection["thread"], now: number): boolean {
  return thread.snoozedUntil != null && DateTime.toEpochMillis(thread.snoozedUntil) > now;
}

export function codexFailoverEligibility(input: {
  readonly projection: CodexFailoverProjection;
  readonly evidence: CodexFailoverEvidence;
  readonly facts: CodexFailoverFacts;
}): CodexFailoverEligibility {
  const { projection, evidence, facts } = input;
  const skip = (reason: CodexFailoverSkipReason): CodexFailoverEligibility => ({
    type: "skip",
    reason,
  });
  const occurredAt = Date.parse(evidence.occurredAt);
  if (facts.startup && (!Number.isFinite(occurredAt) || facts.now - occurredAt > 86_400_000))
    return skip("stale");
  if (facts.localFailure) return skip("local_failure");
  const origin = projection.runs.find((run) => run.id === evidence.runId);
  const attempt = projection.attempts.find((row) => row.id === evidence.attemptId);
  if (
    projection.thread.id !== evidence.threadId ||
    origin?.threadId !== evidence.threadId ||
    origin.status !== "failed" ||
    origin.activeAttemptId !== evidence.attemptId ||
    attempt?.runId !== origin.id ||
    attempt.providerSettlement?.status !== "failed" ||
    attempt.providerSettlement.runAttemptId !== evidence.attemptId ||
    attempt.providerSettlement.providerTurnId !== evidence.providerTurnId
  )
    return skip("not_failed");
  if (latestExecutedRun(projection.runs)?.id !== origin.id) return skip("not_latest");
  if (facts.driver !== "codex") return skip("not_codex");
  if (
    origin.providerInstanceId !== evidence.providerInstanceId ||
    attempt.providerInstanceId !== evidence.providerInstanceId ||
    projection.thread.modelSelection.instanceId !== evidence.providerInstanceId
  )
    return skip("selection_moved");
  if (
    origin.providerThreadId !== evidence.providerThreadId ||
    attempt.providerThreadId !== evidence.providerThreadId ||
    projection.thread.activeProviderThreadId !== evidence.providerThreadId
  )
    return skip("provider_thread_changed");
  const providerThread = projection.providerThreads.find(
    (row) => row.id === evidence.providerThreadId,
  );
  if (
    providerThread?.appThreadId !== evidence.threadId ||
    providerThread.ownerNodeId !== null ||
    providerThread.providerInstanceId !== evidence.providerInstanceId ||
    providerThread.driver !== "codex" ||
    providerThread.nativeThreadRef?.driver !== "codex" ||
    !providerThread.nativeThreadRef.nativeId?.trim()
  )
    return skip("no_native_ref");
  if (facts.stopped) return skip("stopped");
  if (facts.pendingRequest) return skip("pending_request");
  if (!activeThread(projection.thread)) return skip("inactive_thread");
  if (snoozed(projection.thread, facts.now)) return skip("snoozed");
  if (!facts.worktreePresent) return skip("worktree_missing");
  if (!facts.sourceQuiet) return { type: "defer", reason: "source_busy" };
  return { type: "eligible" };
}

export function codexFailoverPostSwitchEligible(input: {
  readonly projection: CodexFailoverProjection;
  readonly step: {
    readonly threadId: ThreadId;
    readonly originRunId: RunId;
    readonly status: "switched" | "continued" | "abandoned";
    readonly savedSelection: ModelSelection;
  };
  readonly selectionRevision: number;
  readonly switchSequence: number | null;
  readonly facts: CodexFailoverFacts & { readonly blockingRun: boolean };
  readonly targetAdmitted: boolean;
}): boolean {
  const { projection, step, facts } = input;
  // Recovery observes the saved target. Reusing the pre-switch source check
  // would reject every switch that committed before a crash.
  return (
    step.status === "switched" &&
    projection.thread.id === step.threadId &&
    modelSelectionsEqual(projection.thread.modelSelection, step.savedSelection) &&
    input.switchSequence !== null &&
    input.selectionRevision === input.switchSequence &&
    latestExecutedRun(projection.runs)?.id === step.originRunId &&
    !facts.stopped &&
    !facts.blockingRun &&
    !facts.pendingRequest &&
    activeThread(projection.thread) &&
    !snoozed(projection.thread, facts.now) &&
    facts.worktreePresent &&
    facts.sourceQuiet &&
    input.targetAdmitted
  );
}

export interface CodexFailoverChainStep {
  readonly threadId: ThreadId;
  readonly chainRootRunId: RunId;
  readonly originRunId: RunId;
  readonly continueMessageId: MessageId;
  readonly sourceInstanceId: ProviderInstanceId;
}

export interface CodexFailoverUsageReset {
  readonly threadId: ThreadId;
  readonly sourceInstanceId: ProviderInstanceId;
  readonly sourceFailure: CodexFailoverFailure;
  readonly sourceResetAt: string | null;
}

export function codexFailoverChain(input: {
  readonly threadId: ThreadId;
  readonly origin: Pick<OrchestrationV2Run, "id" | "userMessageId" | "providerInstanceId">;
  readonly steps: ReadonlyArray<CodexFailoverChainStep>;
  readonly usageResets: ReadonlyArray<CodexFailoverUsageReset>;
  readonly now: number;
}): { readonly chainRootRunId: RunId; readonly visited: ReadonlySet<ProviderInstanceId> } {
  const predecessor = input.steps.find(
    (step) =>
      step.threadId === input.threadId && step.continueMessageId === input.origin.userMessageId,
  );
  const chainRootRunId = predecessor?.chainRootRunId ?? input.origin.id;
  const visited = new Set<ProviderInstanceId>([input.origin.providerInstanceId]);
  for (const step of input.steps) {
    if (step.threadId === input.threadId && step.chainRootRunId === chainRootRunId)
      visited.add(step.sourceInstanceId);
  }
  for (const reset of input.usageResets) {
    if (
      reset.threadId === input.threadId &&
      reset.sourceFailure === "usage_limit" &&
      reset.sourceResetAt !== null &&
      Date.parse(reset.sourceResetAt) > input.now
    )
      visited.add(reset.sourceInstanceId);
  }
  return { chainRootRunId, visited };
}

export function codexFailoverFollowerSelection(input: {
  readonly run: Pick<OrchestrationV2Run, "id" | "threadId" | "providerThreadId" | "status">;
  readonly marker: { readonly threadId: ThreadId; readonly runId: RunId } | null;
  readonly threadId: ThreadId;
  readonly activeProviderThreadId: ProviderThreadId | null;
  readonly sourceInstanceId: ProviderInstanceId;
  readonly targetInstanceId: ProviderInstanceId;
  readonly effectiveSelection: ModelSelection;
  readonly targetAdmissible: (selection: ModelSelection) => boolean;
}): ModelSelection | null {
  const { run, marker } = input;
  if (
    marker === null ||
    marker.runId !== run.id ||
    marker.threadId !== input.threadId ||
    run.threadId !== input.threadId ||
    run.status !== "queued" ||
    input.activeProviderThreadId === null ||
    run.providerThreadId !== input.activeProviderThreadId ||
    input.effectiveSelection.instanceId !== input.sourceInstanceId
  )
    return null;
  const selection = { ...input.effectiveSelection, instanceId: input.targetInstanceId };
  return input.targetAdmissible(selection) ? selection : null;
}

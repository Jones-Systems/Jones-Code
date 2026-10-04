import {
  type CommandId,
  type MessageId,
  type ThreadId,
  NativeCommandReceiptObservationV2,
  type NativeCommandObservationV2,
  type NativeCreationObservationV2,
  NATIVE_CREATION_OBSERVATION_V2_MAX_FACTS,
  NATIVE_CREATION_OBSERVATION_V2_MAX_STAGES,
  type OrchestrationCommandObservation,
  type OrchestrationDispatchBlockerV2,
  type OrchestrationDispatchTargetV2,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { EventSinkV2, type NativeCommandFactsV2, type NativeCommandAuthorityReadV2 } from "./EventSink.ts";
import { ProviderSessionManagerV2 } from "./ProviderSessionManager.ts";
import type { ProviderRuntimeObservation } from "./ProviderAdapter.ts";
import { ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION } from "./ProjectionStore.ts";
import type { NativeCreationBoundedHistoryV2 } from "../persistence/Services/NativeCreationRepository.ts";
import { nativeCreationCanonicalJson } from "./NativeCreationPreparation.ts";

export class CommandObservationUnsupportedError extends Schema.TaggedError<CommandObservationUnsupportedError>()(
  "CommandObservationUnsupportedError",
  { reason: Schema.Literal("observation_unsupported") },
) {}

const contributorTables = [
  "threads", "runs", "run_attempts", "nodes", "provider_threads", "provider_turns",
  "runtime_requests", "messages", "plans", "turn_items", "checkpoint_scopes", "checkpoints",
  "context_handoffs", "context_transfers", "subagents", "provider_sessions", "session_bindings",
  "effects", "unknown_effect_holds", "launch_workflows", "runtime_evidence", "restart_continuations", "legacy_continuation",
  "project", "projection_schema", "source_runtime",
] as const;
const active = (status: string) => status === "pending" || status === "running" || status === "waiting";
const completeContributorRows = (facts: NativeCommandFactsV2) =>
  contributorTables.every((name) => Array.isArray(facts.commitSnapshot.records[name])) &&
  ["claims", "attempts", "effect_facts", "normalized_commands", "reserved_commands", "reserved_command_identities"]
    .every((name) => Array.isArray(facts.commitSnapshot.authorityRecords[name]));

const sameReceipt = Schema.toEquivalence(NativeCommandReceiptObservationV2);
const receiptMetadata = (receipt: NativeCommandReceiptObservationV2 | null): NativeCommandReceiptObservationV2 | null =>
  receipt === null ? null : {
    commandId: receipt.commandId, threadId: receipt.threadId, commandType: receipt.commandType,
    acceptedAt: receipt.acceptedAt, resultSequence: receipt.resultSequence, status: receipt.status, error: receipt.error,
  };

export function nativeCreationObservationFromHistoryV2(
  input: { readonly threadId: ThreadId; readonly commandId: CommandId; readonly messageId: MessageId },
  facts: NativeCommandFactsV2,
  history: NativeCreationBoundedHistoryV2 | null,
): NativeCreationObservationV2 | undefined {
  if (history === null || history.threadId !== input.threadId || history.messageId !== input.messageId ||
    history.threadId !== facts.threadId) return undefined;
  const expectedIds = {
    "thread.create": `${history.originalCommandId}:native:v2:create`,
    "message.dispatch": `${history.originalCommandId}:native:v2:message`,
    "prepared-run.release": history.originalCommandId,
  };
  const validStage = (stage: NativeCreationBoundedHistoryV2["stageCommands"][number]) =>
    stage.claimId === history.claimId && stage.threadId === history.threadId &&
    stage.commandId === expectedIds[stage.commandType];
  if (input.commandId !== history.originalCommandId &&
    !history.stageCommands.some((stage) => validStage(stage) && stage.commandId === input.commandId))
    return undefined;
  const overflow = history.overflow || history.stageCommands.length > NATIVE_CREATION_OBSERVATION_V2_MAX_STAGES ||
    history.effectsV1.length > NATIVE_CREATION_OBSERVATION_V2_MAX_FACTS ||
    history.effectsV2.length > NATIVE_CREATION_OBSERVATION_V2_MAX_FACTS ||
    history.unresolvedEffects.length > NATIVE_CREATION_OBSERVATION_V2_MAX_FACTS;
  const stages = history.stageCommands;
  let inconsistent = facts.commandId !== input.commandId || stages.some((stage) => !validStage(stage) ||
    stages.filter((other) => other.commandType === stage.commandType || other.commandId === stage.commandId).length !== 1);
  for (const stage of stages) {
    if (stage.receipt !== null && (stage.receipt.commandId !== stage.commandId ||
      stage.receipt.threadId !== stage.threadId || stage.receipt.commandType !== stage.commandType ||
      stage.receipt.resultSequence > facts.snapshotSequence)) inconsistent = true;
    if (stage.event !== null && (stage.receipt?.status !== "accepted" ||
      stage.receipt.resultSequence !== stage.event.sequence)) inconsistent = true;
    if (stage.receipt?.status === "accepted" && stage.event === null) inconsistent = true;
  }
  for (const effect of history.effectsV2) {
    const stage = stages.find((candidate) => candidate.commandId === effect.commandId);
    if (effect.commandType !== "prepared-run.release" || stage === undefined || !validStage(stage) || effect.threadId !== stage.threadId ||
      effect.commandType !== stage.commandType || effect.commandDigest !== stage.commandDigest) inconsistent = true;
    if (effect.phase === "completed") {
      const starts = history.effectsV2.filter((start) => start.phase === "started" && start.effectId === effect.effectId &&
        start.commandId === effect.commandId && start.threadId === effect.threadId &&
        start.commandType === effect.commandType && start.commandDigest === effect.commandDigest && start.ordinal < effect.ordinal);
      if (starts.length !== 1 || stage?.receipt?.status !== "accepted" || effect.sequence > facts.snapshotSequence)
        inconsistent = true;
    }
  }
  const finalStage = stages.find((stage) => stage.commandType === "prepared-run.release");
  if (finalStage !== undefined && history.normalizedCommandDigest !== null &&
    history.normalizedCommandDigest !== finalStage.commandDigest) inconsistent = true;
  if ((history.finalReceipt === null) !== (finalStage?.receipt == null) ||
    (history.finalReceipt !== null && finalStage?.receipt != null && !sameReceipt(history.finalReceipt, finalStage.receipt)))
    inconsistent = true;
  const requestedStage = stages.find((stage) => stage.commandId === input.commandId);
  if (requestedStage === undefined && (facts.receipt !== null || facts.identity !== null)) inconsistent = true;
  if (requestedStage !== undefined && ((facts.receipt === null) !== (requestedStage.receipt === null) ||
    (facts.receipt !== null && requestedStage.receipt !== null && !sameReceipt(facts.receipt, requestedStage.receipt))))
    inconsistent = true;
  if (requestedStage?.receipt !== null && requestedStage?.receipt !== undefined &&
    (facts.identity === null || facts.identity.kind !== "native_creation_stage" || facts.identity.version !== 2 ||
      facts.identity.commandId !== requestedStage.commandId || facts.identity.commandType !== requestedStage.commandType ||
      facts.identity.aggregateKind !== "thread" || facts.identity.aggregateId !== history.threadId ||
      facts.identity.normalizedCommandDigest !== requestedStage.commandDigest || facts.identity.bindingDigest !== history.bindingDigest))
    inconsistent = true;
  const requestedEvent = requestedStage?.event;
  if (requestedStage !== undefined && requestedEvent != null &&
    (facts.eventMetadataOverflow || facts.eventMetadata.filter((event) =>
      event.eventId === requestedEvent.eventId && event.sequence === requestedEvent.sequence &&
      event.commandId === requestedStage.commandId && event.aggregateKind === "thread" &&
      event.aggregateId === history.threadId && event.applicationEventVersion === 2).length !== 1 ||
      facts.events.filter((stored) => stored.commandId === requestedStage.commandId && stored.sequence === requestedEvent.sequence &&
        stored.event.id === requestedEvent.eventId && stored.event.threadId === history.threadId).length !== 1))
    inconsistent = true;
  const createStage = stages.find((stage) => stage.commandType === "thread.create");
  const birth = createStage?.receipt?.status === "accepted" && createStage.event !== null ? createStage.event : null;
  if (birth !== null && (facts.creationProvenance !== "native_created" ||
    facts.incarnation?.eventId !== birth.eventId || facts.incarnation?.sequence !== birth.sequence)) inconsistent = true;
  // Historical V1 command facts cannot attest the separately attributed V2 command chain.
  if (history.effectsV1.some((effect) => effect.kind === "native_command")) inconsistent = true;
  const allEffects = [...history.effectsV1, ...history.effectsV2];
  const completionOutputFields = new Set(["timestamp", "ordinal", "phase", "result", "eventId", "sequence", "exitCode", "terminalId", "ownership"]);
  if (allEffects.some((effect) => {
    if (effect.phase !== "completed") return false;
    const starts = allEffects.filter((start) => start.phase === "started" && start.effectId === effect.effectId &&
      start.kind === effect.kind && start.ordinal < effect.ordinal);
    const start = starts[0];
    return starts.length !== 1 || allEffects.filter((end) => end.effectId === effect.effectId && end.phase === "completed").length !== 1 ||
      (start !== undefined && Object.entries(start).some(([key, value]) => !completionOutputFields.has(key) &&
        nativeCreationCanonicalJson(value) !== nativeCreationCanonicalJson(Reflect.get(effect, key)))) ||
      (start?.kind === "setup" && effect.kind === "setup" && start.terminalId !== null && start.terminalId !== effect.terminalId);
  })) inconsistent = true;
  const unresolved = history.unresolvedEffects.length > 0 || allEffects.some((effect) => effect.phase === "started" &&
    !allEffects.some((end) => end.phase === "completed" && end.effectId === effect.effectId &&
      end.kind === effect.kind && (!("result" in end) || end.result !== "unknown")));
  const unknownResult = history.effectsV1.some((effect) => effect.phase === "completed" &&
    "result" in effect && effect.result === "unknown");
  const failed = history.effectsV1.some((effect) => effect.phase === "completed" &&
    "result" in effect && effect.result === "failed");
  const cleanup = history.effectsV1.some((effect) => effect.kind === "cleanup");
  const lifecycleComplete = ["normalization", "tracker_registration", "bootstrap_detachment"].every((action) =>
    history.effectsV1.some((effect) => effect.kind === "lifecycle" && effect.phase === "completed" &&
      effect.threadId === history.threadId && effect.action === action && effect.result === "succeeded"));
  const checkoutComplete = history.effectsV1.some((effect) => effect.kind === "worktree" &&
    effect.phase === "completed" && effect.result === "succeeded");
  const setupComplete = !history.binding.runSetupScript || history.effectsV1.some((effect) =>
    effect.kind === "setup" && effect.phase === "completed" && effect.result === "succeeded");
  // Create/message receipts attest acceptance; the final provider start has separate execution facts.
  const commandChainComplete = Object.keys(expectedIds).every((type) => stages.some((stage) =>
    stage.commandType === type && stage.receipt?.status === "accepted" && stage.event !== null));
  const executionComplete = history.effectsV2.some((effect) => effect.commandType === "prepared-run.release" && effect.phase === "completed");
  const outcome = overflow || inconsistent || unresolved || unknownResult || history.normalizedCommandDigest === null
    ? "unknown" : cleanup || failed || stages.some((stage) => stage.receipt?.status === "rejected") ? "incomplete"
      : birth !== null && commandChainComplete && lifecycleComplete && checkoutComplete && setupComplete &&
        history.finalReceipt?.status === "accepted" && executionComplete ? "complete" : "in_progress";
  // Pick the public envelope explicitly; the internal carrier also owns query-association IDs.
  return {
    version: 2, schema: "t3.native-creation-observation/v2",
    preparationId: history.preparationId, operationId: history.operationId,
    preparationSha256: history.preparationSha256, bindingDigest: history.bindingDigest,
    promptDigest: history.promptDigest, commandDigest: history.commandDigest,
    normalizedCommandDigest: history.normalizedCommandDigest, claimId: history.claimId,
    claimedBootId: history.claimedBootId, claimedAt: history.claimedAt, actorSessionId: history.actorSessionId,
    grantId: history.grantId, grantRevision: history.grantRevision, binding: history.binding,
    incarnation: birth, effectsV1: history.effectsV1.slice(0, NATIVE_CREATION_OBSERVATION_V2_MAX_FACTS),
    effectsV2: history.effectsV2.slice(0, NATIVE_CREATION_OBSERVATION_V2_MAX_FACTS),
    unresolvedEffects: history.unresolvedEffects.slice(0, NATIVE_CREATION_OBSERVATION_V2_MAX_FACTS),
    stageCommands: stages.slice(0, NATIVE_CREATION_OBSERVATION_V2_MAX_STAGES).map((stage) => ({
      claimId: stage.claimId, commandId: stage.commandId, threadId: stage.threadId, commandType: stage.commandType,
      commandDigest: stage.commandDigest, event: stage.event === null ? null : { eventId: stage.event.eventId, sequence: stage.event.sequence },
      receipt: receiptMetadata(stage.receipt),
    })),
    finalReceipt: receiptMetadata(history.finalReceipt), outcome, overflow,
  };
}

export function dispatchTargetFromNativeFactsV2(
  facts: NativeCommandFactsV2,
  runtime?: ProviderRuntimeObservation,
): { readonly target: OrchestrationDispatchTargetV2 | null; readonly runtimeReason?: string } {
  const projection = facts.projection;
  if (projection === null) return { target: null };
  const { thread } = projection;
  const blockers = new Set<OrchestrationDispatchBlockerV2>();
  const runs = [...projection.runs].sort((a, b) => b.ordinal - a.ordinal);
  const activeRuns = runs.filter((run) => ["preparing", "starting", "running", "waiting"].includes(run.status));
  const running = activeRuns.length === 1 ? activeRuns[0] : undefined;
  const providerThread = projection.providerThreads.find((candidate) => candidate.id === thread.activeProviderThreadId);
  const session = projection.providerSessions.find((candidate) => candidate.id === providerThread?.providerSessionId);
  const evidenceRows = facts.commitSnapshot.records.runtime_evidence ?? [];
  const evidence = evidenceRows.length === 1 ? evidenceRows[0] : undefined;
  const registrationMatches = providerThread !== undefined && session !== undefined && evidence !== undefined &&
    evidence.thread_id === facts.threadId && evidence.provider_thread_id === providerThread.id &&
    evidence.provider_session_id === session.id && evidence.provider_instance_id === providerThread.providerInstanceId &&
    evidence.provider_instance_id === session.providerInstanceId && evidence.provider_instance_id === thread.modelSelection.instanceId &&
    evidence.driver === providerThread.driver && evidence.driver === session.driver &&
    evidence.native_thread_id === (providerThread.nativeThreadRef?.nativeId ?? null) &&
    typeof evidence.evidence_revision === "number" && evidence.evidence_revision > 0;
  const runtimeGeneration = registrationMatches && typeof evidence?.runtime_generation === "string"
    ? evidence.runtime_generation : undefined;
  const runtimeMatches = runtime !== undefined && runtime.status !== "unknown" &&
    providerThread !== undefined && session !== undefined && evidence !== undefined &&
    runtime.binding.threadId === facts.threadId && runtime.binding.providerThreadId === providerThread.id &&
    runtime.binding.providerSessionId === session.id && runtime.binding.instanceId === thread.modelSelection.instanceId &&
    runtime.binding.instanceId === providerThread.providerInstanceId && runtime.binding.instanceId === session.providerInstanceId &&
    runtime.binding.runtimeGeneration === runtimeGeneration &&
    (runtime.binding.nativeThreadId ?? null) === (providerThread.nativeThreadRef?.nativeId ?? null) &&
    evidence.thread_id === facts.threadId && evidence.provider_thread_id === providerThread.id &&
    evidence.provider_session_id === session.id && evidence.provider_instance_id === runtime.binding.instanceId &&
    evidence.driver === providerThread.driver && evidence.driver === session.driver &&
    evidence.native_thread_id === (runtime.binding.nativeThreadId ?? null) &&
    typeof evidence.evidence_revision === "number" && evidence.evidence_revision > 0 &&
    typeof evidence.registered_at === "string" && Number.isFinite(Date.parse(runtime.observedAt)) &&
    Number.isFinite(Date.parse(evidence.registered_at)) && Date.parse(runtime.observedAt) >= Date.parse(evidence.registered_at);
  if (thread.archivedAt !== null) blockers.add("archived");
  if (thread.deletedAt !== null) blockers.add("deleted");
  if (thread.settledOverride === "settled") blockers.add("settled");
  if (runs.some((run) => run.status === "queued")) blockers.add("queued_run");
  if (runs.some((run) => run.queueHeld === true)) blockers.add("held_run");
  if (activeRuns.length > 0) blockers.add("active_run");
  if (projection.attempts.some((attempt) => active(attempt.status))) blockers.add("active_attempt");
  if (projection.providerTurns.some((turn) => active(turn.status))) blockers.add("provider_turn");
  if (projection.nodes.some((node) => active(node.status))) blockers.add("execution_node");
  if (projection.subagents.some((agent) => active(agent.status))) blockers.add("subagent_work");
  if (projection.providerThreads.some((candidate) => (candidate.pendingBackgroundTasks?.length ?? 0) > 0))
    blockers.add("background_work");
  if (projection.subagents.some((agent) => agent.completionDelivery !== undefined &&
      ["pending", "claimed", "acknowledged"].includes(agent.completionDelivery.state)))
    blockers.add("completion_delivery");
  if (runs.some((run) => run.delegatedCompletion?.delivery !== null && run.delegatedCompletion?.delivery !== undefined))
    blockers.add("wake_delivery");
  for (const request of projection.runtimeRequests) {
    if (request.status !== "pending") continue;
    if (request.kind === "user_input") blockers.add("pending_user_input");
    else if (request.kind === "dynamic_tool_call") blockers.add("pending_tool");
    else if (request.kind === "auth_refresh") blockers.add("pending_auth_refresh");
    else blockers.add("pending_approval");
  }
  if (projection.plans.some((plan) => plan.kind === "proposed_plan" && ["draft", "active"].includes(plan.status)))
    blockers.add("actionable_plan");
  const records = facts.commitSnapshot.records;
  if ((records.effects ?? []).some((effect) => effect.status === "pending" || effect.status === "running"))
    blockers.add("pending_native_effect");
  if ((records.unknown_effect_holds?.length ?? 0) > 0) blockers.add("unknown_resume");
  if ((records.restart_continuations ?? []).some((marker) => marker.status === "dormant" || marker.status === "released"))
    blockers.add("unknown_resume");
  if ((records.launch_workflows ?? []).some((workflow) => workflow.status !== "completed" && workflow.status !== "failed"))
    blockers.add("unresolved_start");
  if (runtimeMatches && (runtime?.status === "working" || runtime?.status === "busy")) blockers.add("provider_activity");
  if (runtimeMatches && runtime?.status === "monitoring") blockers.add("background_work");

  // Only an unstarted native birth can prove absence without a resident runtime.
  const neverAttached = completeContributorRows(facts) && facts.creationProvenance === "native_created" &&
    facts.incarnation !== null && facts.creationHistory.length === 0 && facts.nativeCreationHistory === null &&
    thread.activeProviderThreadId === null && records.threads?.length === 1 &&
    records.threads[0]?.thread_id === facts.threadId &&
    facts.commitSnapshot.creationProvenance === facts.creationProvenance &&
    facts.commitSnapshot.incarnation?.eventId === facts.incarnation.eventId &&
    facts.commitSnapshot.incarnation.sequence === facts.incarnation.sequence &&
    projection.runs.length === 0 && projection.attempts.length === 0 && projection.nodes.length === 0 &&
    projection.providerThreads.length === 0 && projection.providerSessions.length === 0 &&
    projection.providerTurns.length === 0 && projection.runtimeRequests.length === 0 && projection.subagents.length === 0 &&
    records.project?.length === 1 && records.project[0]?.project_id === thread.projectId &&
    records.project[0]?.deleted_at === null && records.projection_schema?.length === 1 &&
    records.projection_schema[0]?.schema_version === ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION &&
    ["runs", "run_attempts", "nodes", "provider_threads", "provider_turns", "runtime_requests", "subagents",
      "provider_sessions", "session_bindings", "effects", "unknown_effect_holds", "launch_workflows",
      "runtime_evidence", "restart_continuations", "legacy_continuation", "source_runtime", "context_handoffs", "context_transfers"]
      .every((name) => records[name]?.length === 0) &&
    ["claims", "attempts", "effect_facts", "normalized_commands", "reserved_commands", "reserved_command_identities"]
      .every((name) => facts.commitSnapshot.authorityRecords[name]?.length === 0);
  // Current provider probes do not attest a complete resumed-descendant or pending-wake roster.
  const complete = neverAttached && thread.id === facts.threadId &&
    facts.commitSnapshot.threadId === facts.threadId && facts.commitSnapshot.commandId === facts.commandId &&
    facts.targetEventSequence <= facts.snapshotSequence && activeRuns.length <= 1;
  const runtimeReason = complete ? undefined : runtime?.status === "unknown"
    ? runtime.reason : runtime !== undefined && !runtimeMatches
      ? "native_activity_binding_mismatch" : "native_activity_coverage_incomplete";
  if (!complete) blockers.add("unknown_evidence");
  return {
    ...(runtimeReason === undefined ? {} : { runtimeReason }),
    target: {
      incarnation: facts.incarnation,
      modelSelection: thread.modelSelection,
      activeRunId: running?.id ?? null,
      latestRunId: runs[0]?.id ?? null,
      activeRunAttemptId: running?.activeAttemptId ?? null,
      activeProviderThreadId: thread.activeProviderThreadId,
      providerSessionId: providerThread?.providerSessionId ?? null,
      providerSessionStatus: session?.status ?? null,
      ...(runtimeGeneration === undefined ? {} : { runtimeGeneration }),
      snapshotSequence: facts.snapshotSequence,
      targetEventSequence: facts.targetEventSequence,
      complete,
      requireIdle: true,
      idle: complete && blockers.size === 0,
      blockers: [...blockers],
    },
  };
}

export function commandObservationFromNativeFactsV2(
  input: { readonly threadId: ThreadId; readonly commandId: CommandId; readonly messageId: MessageId },
  facts: NativeCommandFactsV2,
  target: OrchestrationDispatchTargetV2 | null,
): NativeCommandObservationV2 {
  const { receipt, identity, projection } = facts;
  const metadata = facts.eventMetadata;
  const completeEvents = !facts.eventMetadataOverflow && metadata.length <= 256 &&
    metadata.length === facts.events.length &&
    metadata.every((event, index) => event.applicationEventVersion === 2 &&
      event.eventId === facts.events[index]?.event.id && event.sequence === facts.events[index]?.sequence &&
      event.type === facts.events[index]?.event.type &&
      event.sequence <= facts.snapshotSequence &&
      (index === 0 || event.sequence === metadata[index - 1]!.sequence + 1));
  const wrongDomain = facts.threadId !== input.threadId || facts.commandId !== input.commandId ||
    (receipt !== null && (receipt.commandId !== input.commandId || receipt.threadId !== input.threadId)) ||
    (identity !== null && (identity.commandId !== input.commandId || identity.aggregateId !== input.threadId ||
      identity.aggregateKind !== "thread" || (receipt !== null && identity.commandType !== receipt.commandType))) ||
    metadata.some((event) => event.commandId !== input.commandId || event.aggregateKind !== "thread" || event.aggregateId !== input.threadId) ||
    facts.events.some((stored) => stored.commandId !== input.commandId || stored.event.threadId !== input.threadId);
  const messages = facts.events.flatMap((stored) => stored.event.type === "message.updated" && stored.event.payload.role === "user"
    ? [{ sequence: stored.sequence, message: stored.event.payload }] : []);
  const eventMessages = messages.filter((event) => event.message.id === input.messageId && event.message.threadId === input.threadId);
  const eventRunIds = eventMessages.map((event) => event.message.runId).filter((id) => id !== null);
  const projectedMessages = projection?.messages.filter((message) => message.id === input.messageId && message.role === "user") ?? [];
  const projectedRuns = projection?.runs.filter((run) => run.userMessageId === input.messageId || eventRunIds.includes(run.id)) ?? [];
  let correlation: NativeCommandObservationV2["correlation"] = "missing";
  let run: NativeCommandObservationV2["run"] = null;
  let correlatedMessageId: MessageId | null = null;
  const finalSequence = facts.events.at(-1)?.sequence;
  const wrongMessage = receipt?.commandType === "message.dispatch" &&
    messages.some((event) => event.message.id !== input.messageId || event.message.threadId !== input.threadId);
  const wrongProjection = projectedMessages.some((message) => message.threadId !== input.threadId) ||
    projectedRuns.some((candidate) => candidate.threadId !== input.threadId) ||
    (projectedRuns.length === 1 && (eventRunIds.some((id) => id !== projectedRuns[0]?.id) ||
      projectedMessages.some((message) => message.runId !== projectedRuns[0]?.id)));
  if (wrongDomain || wrongMessage || wrongProjection) correlation = "mismatched";
  else if (eventMessages.length > 1 || projectedMessages.length > 1 || projectedRuns.length > 1) correlation = "ambiguous";
  else if (receipt?.status === "accepted" && completeEvents && finalSequence === receipt.resultSequence &&
    eventMessages.length === 1) {
    correlatedMessageId = input.messageId;
    const candidate = projectedRuns[0];
    if (receipt.resultSequence > facts.snapshotSequence || projectedMessages.length !== 1 || candidate === undefined)
      correlation = "pending";
    else {
      const attempts = projection?.attempts.filter((attempt) => attempt.runId === candidate.id) ?? [];
      const attempt = candidate.activeAttemptId === null
        ? [...attempts].sort((a, b) => b.attemptOrdinal - a.attemptOrdinal)[0]
        : attempts.find((entry) => entry.id === candidate.activeAttemptId);
      if (candidate.activeAttemptId !== null && attempt === undefined) correlation = "pending";
      else if (attempt !== undefined && attempts.filter((entry) => entry.id === attempt.id).length !== 1)
        correlation = "ambiguous";
      else if (attempt !== undefined && candidate.providerThreadId !== null && attempt.providerThreadId !== candidate.providerThreadId)
        correlation = "mismatched";
      else {
        correlation = "exact";
        run = {
          runId: candidate.id,
          runAttemptId: attempt?.id ?? null,
          providerThreadId: attempt?.providerThreadId ?? candidate.providerThreadId,
          providerTurnId: attempt?.providerTurnId ?? null,
          status: candidate.status,
        };
      }
    }
  } else if (receipt?.status === "accepted" && (!completeEvents || metadata.length === 0))
    correlation = "pending";
  const identityVerification: NativeCommandObservationV2["identityVerification"] = wrongDomain
    ? "mismatched" : identity === null ? receipt === null ? "missing" : "unbound"
    : receipt === null || !completeEvents || finalSequence !== receipt.resultSequence ? "unknown" : "verified";
  const creation = nativeCreationObservationFromHistoryV2(input, facts, facts.nativeCreationHistory);
  return {
    version: 2,
    ...(creation === undefined ? {} : { creation }),
    ...input,
    commandStatus: receipt?.status ?? "not_found",
    identity,
    identityVerification,
    correlation,
    receipt,
    snapshot: {
      snapshotSequence: facts.snapshotSequence,
      targetEventSequence: facts.targetEventSequence,
      complete: completeEvents && !wrongDomain,
    },
    correlatedMessageId,
    run,
    target,
  };
}

export const makeCommandObservationQuery = Effect.fn("makeCommandObservationQuery")(function* () {
  const sink = yield* EventSinkV2;
  const manager = yield* ProviderSessionManagerV2;
  const getTarget = Effect.fn("CommandObservation.getTarget")(function* (
    threadId: ThreadId, commandId: CommandId, authority?: NativeCommandAuthorityReadV2,
  ) {
    const facts = yield* sink.readNativeCommandFacts({ threadId, commandId, ...(authority === undefined ? {} : { authority }) });
    const projection = facts.projection;
    const providerThread = projection?.providerThreads.find((entry) => entry.id === projection.thread.activeProviderThreadId);
    const session = projection?.providerSessions.find((entry) => entry.id === providerThread?.providerSessionId);
    const rows = facts.commitSnapshot.records.runtime_evidence ?? [];
    const row = rows.length === 1 ? rows[0] : undefined;
    let runtime: ProviderRuntimeObservation | undefined;
    if (providerThread !== undefined && session !== undefined && row !== undefined &&
      row.thread_id === threadId && row.provider_thread_id === providerThread.id &&
      row.provider_session_id === session.id && row.provider_instance_id === providerThread.providerInstanceId &&
      row.driver === providerThread.driver && row.driver === session.driver &&
      row.native_thread_id === (providerThread.nativeThreadRef?.nativeId ?? null) &&
      typeof row.runtime_generation === "string" && typeof row.evidence_revision === "number") {
      runtime = yield* manager.observeThreadRuntime({
        threadId, providerThreadId: providerThread.id, providerSessionId: session.id,
        instanceId: providerThread.providerInstanceId, runtimeGeneration: row.runtime_generation,
        ...(providerThread.nativeThreadRef?.nativeId == null ? {} : { nativeThreadId: providerThread.nativeThreadRef.nativeId }),
      });
    }
    return { facts, ...dispatchTargetFromNativeFactsV2(facts, runtime) };
  });
  const observe = Effect.fn("CommandObservation.observe")(function* (
    input: { readonly threadId: ThreadId; readonly commandId: CommandId; readonly messageId: MessageId },
  ) {
    const current = yield* getTarget(input.threadId, input.commandId);
    return commandObservationFromNativeFactsV2(input, current.facts, current.target);
  });
  const observeLegacy = Effect.fn("CommandObservation.observeLegacy")(function* (
    input: { readonly threadId: ThreadId; readonly commandId: CommandId; readonly messageId: MessageId },
  ) {
    const facts = yield* sink.readNativeCommandFacts(input);
    if (facts.creationProvenance !== "legacy_import" || facts.identity !== null || facts.events.length > 0 ||
      facts.eventMetadata.length > 0 || facts.eventMetadataOverflow || facts.creationHistory.length > 0 ||
      facts.nativeCreationHistory !== null || facts.threadId !== input.threadId || facts.commandId !== input.commandId ||
      (facts.receipt !== null && (facts.receipt.threadId !== input.threadId ||
        facts.receipt.commandId !== input.commandId || facts.receipt.commandType !== "thread.turn.start" ||
        facts.receipt.status !== "rejected")))
      return yield* new CommandObservationUnsupportedError({ reason: "observation_unsupported" });
    return {
      ...input, snapshotSequence: facts.snapshotSequence, commandStatus: facts.receipt?.status ?? "not_found",
      acceptedSequence: null, correlation: "missing", turn: null, target: null,
    } satisfies OrchestrationCommandObservation;
  });
  return { getTarget, observe, observeLegacy };
});

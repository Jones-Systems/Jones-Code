import type { OrchestrationV2DomainEvent } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/sql/SqlClient";
import {
  LegacyDeletionProvenance,
  LegacyFailureDeletionProvenance,
  LegacyPreparationFailureDecision,
  RecordedLifecycleEvent,
  type RecordedRun,
} from "../../orchestration-v2/RecordedTypes.ts";
import {
  canonicalLegacyPayload,
  legacyBootstrapCreateCommandId,
  legacyNeverInvokedWorkspaceFailure,
  legacyPreparationGeneration,
  sameLegacyBootstrapPolicy,
} from "../../orchestration-v2/LegacyBootstrap.ts";
import * as M from "./RecordedMetadata.ts";
import type * as Storage from "./Storage.ts";
import { LegacyStorageError, type RunMetadata } from "./StorageSchema.ts";

// Ported from receiving ProjectionStore (cee2cdd): validators at lines 826-1090, the run merge
// at 1092-1135 and the SQL apply correlation at about 2533-2590.
export type RunEvent = Extract<
  RecordedLifecycleEvent,
  { readonly type: "run.created" | "run.updated" }
>;

const decodeLegacyDeletionProvenance = Schema.decodeSync(LegacyDeletionProvenance);
const decodeLegacyPreparationFailureDecision = Schema.decodeSync(LegacyPreparationFailureDecision);
const decodeLegacyFailureDeletionProvenance = Schema.decodeSync(LegacyFailureDeletionProvenance);
const decodeLifecycleEvent = Schema.decodeUnknownEffect(RecordedLifecycleEvent);
const toEquivalenceLegacyDeletionProvenance = Schema.toEquivalence(LegacyDeletionProvenance);
const toEquivalenceLegacyFailureDeletionProvenance = Schema.toEquivalence(
  LegacyFailureDeletionProvenance,
);

function isPreparationJournalEvent(run: RecordedRun, eventId: string): boolean {
  const preparation = run.legacyPreparation;
  return (
    preparation !== undefined &&
    (eventId ===
      `${preparation.policy.createCommandId}:preparation:${preparation.generation}:initialize:event` ||
      eventId ===
        `${preparation.policy.createCommandId}:preparation:${preparation.generation}:setup-policy:event` ||
      preparation.steps.some(
        (step) => step.intentEventId === eventId || step.outcomeEventId === eventId,
      ))
  );
}

/** Returns whether the incoming D provenance is authoritative; throws when malformed or replaced. */
function validateDeletionProvenance(
  current: RecordedRun | undefined,
  next: RecordedRun,
  event: RunEvent,
): boolean {
  const incoming = next.legacyReleaseDecision?.deletion;
  const accepted = current?.legacyReleaseDecision?.deletion;
  const fail = () => {
    throw new Error("Malformed or replaced legacy D provenance.");
  };
  if (incoming === undefined) {
    if (accepted !== undefined && next.legacyReleaseDecision !== undefined) {
      const { deletion: _old, ...oldDecision } = current!.legacyReleaseDecision!;
      if (
        canonicalLegacyPayload(oldDecision) !== canonicalLegacyPayload(next.legacyReleaseDecision)
      )
        return fail();
    }
    return false;
  }
  if (
    current !== undefined &&
    (event.type !== "run.updated" ||
      event.threadId !== current.threadId ||
      event.runId !== current.id ||
      next.threadId !== current.threadId ||
      next.id !== current.id)
  )
    return fail();
  if (accepted !== undefined) {
    if (!toEquivalenceLegacyDeletionProvenance(accepted, incoming)) return fail();
    const { deletion: _old, ...oldDecision } = current!.legacyReleaseDecision!;
    const { deletion: _next, ...nextDecision } = next.legacyReleaseDecision!;
    if (canonicalLegacyPayload(oldDecision) !== canonicalLegacyPayload(nextDecision)) return fail();
    return true;
  }
  const decision = current?.legacyReleaseDecision;
  const preparation = current?.legacyPreparation;
  if (
    current === undefined ||
    decision === undefined ||
    preparation === undefined ||
    event.type !== "run.updated"
  )
    return fail();
  const proof = decodeLegacyDeletionProvenance(incoming);
  const { deletion: _deletion, ...original } = next.legacyReleaseDecision!;
  const control = proof.control;
  if (
    event.threadId !== current.threadId ||
    event.runId !== current.id ||
    next.id !== current.id ||
    next.threadId !== current.threadId ||
    event.id !== proof.evidenceEventId ||
    proof.evidenceEventId !== `${proof.commandId}:event` ||
    proof.commandId !== `${proof.policy.createCommandId}:guard-rejection-delete` ||
    proof.policy.createCommandId !==
      legacyBootstrapCreateCommandId(proof.threadId, proof.policy.releaseCommandId) ||
    proof.threadId !== current.threadId ||
    proof.runId !== current.id ||
    proof.policy.runId !== current.id ||
    !proof.policy.ownsNewThread ||
    current.status !== "failed" ||
    current.startedAt !== null ||
    next.status !== "failed" ||
    next.startedAt !== null ||
    canonicalLegacyPayload(original) !== canonicalLegacyPayload(decision) ||
    canonicalLegacyPayload(next.legacyPreparation) !== canonicalLegacyPayload(preparation) ||
    next.legacyBootstrap === undefined ||
    current.legacyBootstrap === undefined ||
    !sameLegacyBootstrapPolicy(proof.policy, decision.policy) ||
    !sameLegacyBootstrapPolicy(proof.policy, preparation.policy) ||
    !sameLegacyBootstrapPolicy(proof.policy, current.legacyBootstrap) ||
    !sameLegacyBootstrapPolicy(proof.policy, next.legacyBootstrap) ||
    !sameLegacyBootstrapPolicy(proof.policy, control.policy) ||
    control.threadId !== proof.threadId ||
    control.runId !== proof.runId ||
    proof.projectWorkspaceRoot !== preparation.projectWorkspaceRoot ||
    proof.preparationGeneration !== preparation.generation ||
    control.preparationGeneration !== preparation.generation ||
    preparation.generation !==
      legacyPreparationGeneration({
        runId: current.id,
        birthEventId: decision.birthEventId,
        birthSequence: decision.birthSequence,
      }) ||
    (
      [
        "claimEventId",
        "claimSequence",
        "claimReceiptSequence",
        "birthEventId",
        "birthSequence",
        "birthReceiptSequence",
      ] as const
    ).some(
      (field) =>
        proof[field] !== decision[field] ||
        proof[field] !== preparation[field] ||
        proof[field] !== control[field],
    ) ||
    (proof.type === "no_control"
      ? !["no_script", "opted_out"].includes(preparation.setup.status) ||
        proof.workspacePath !== proof.control.workspacePath ||
        proof.projectWorkspaceRoot !== proof.control.projectWorkspaceRoot ||
        preparation.steps.some(
          (step) => step.effect.kind.startsWith("setup.") || step.effect.kind === "terminal.close",
        )
      : preparation.setup.status !== "resolved" ||
        proof.workspacePath !== preparation.setup.definition.cwd ||
        proof.control.terminalId !== preparation.setup.definition.terminalId ||
        proof.control.generation !== preparation.setup.definition.generation)
  )
    return fail();
  return true;
}

/** Returns whether the incoming workspace-failure decision is authoritative; throws when malformed or replaced. */
export function validateFailureDecision(
  current: RecordedRun | undefined,
  next: RecordedRun,
  event: RunEvent,
): boolean {
  const incoming = next.legacyPreparationFailureDecision;
  const accepted = current?.legacyPreparationFailureDecision;
  const fail = () => {
    throw new Error("Malformed or replaced legacy workspace-failure provenance.");
  };
  if (incoming === undefined) return false;
  const decision = decodeLegacyPreparationFailureDecision(incoming);
  const { deletion: _incomingDeletion, ...original } = decision;
  const { deletion: _acceptedDeletion, ...previous } = accepted ?? decision;
  if (
    accepted !== undefined &&
    canonicalLegacyPayload(original) !== canonicalLegacyPayload(previous)
  )
    return fail();
  if (accepted !== undefined) {
    if (
      current === undefined ||
      event.type !== "run.updated" ||
      next.id !== current.id ||
      next.threadId !== current.threadId ||
      event.threadId !== current.threadId ||
      event.runId !== current.id
    )
      return fail();
    if (decision.deletion === undefined) return false;
    if (accepted.deletion !== undefined) {
      if (!toEquivalenceLegacyFailureDeletionProvenance(accepted.deletion, decision.deletion))
        return fail();
      return true;
    }
  }
  const candidate = current === undefined ? undefined : legacyNeverInvokedWorkspaceFailure(current);
  if (
    current === undefined ||
    candidate === undefined ||
    event.type !== "run.updated" ||
    next.id !== current.id ||
    next.threadId !== current.threadId ||
    event.threadId !== current.threadId ||
    event.runId !== current.id ||
    next.status !== "failed" ||
    next.startedAt !== null ||
    canonicalLegacyPayload(next.legacyPreparation) !==
      canonicalLegacyPayload(current.legacyPreparation) ||
    canonicalLegacyPayload(next.legacyBootstrap) !==
      canonicalLegacyPayload(current.legacyBootstrap) ||
    !sameLegacyBootstrapPolicy(decision.policy, candidate.policy) ||
    decision.failureCommandId !== `${candidate.policy.createCommandId}:fail` ||
    decision.preparationGeneration !== candidate.preparation.generation ||
    decision.projectWorkspaceRoot !== candidate.preparation.projectWorkspaceRoot ||
    decision.workspacePath !== candidate.preparation.projectWorkspaceRoot ||
    decision.failedEffectId !== candidate.step.effectId ||
    decision.failedInputHash !== candidate.step.inputHash ||
    decision.outcomeCommandId !== candidate.step.outcomeCommandId ||
    decision.outcomeEventId !== candidate.step.outcomeEventId ||
    decision.outcomeEventSequence !== decision.outcomeReceiptSequence ||
    decision.outcomeEventSequence <= decision.birthReceiptSequence ||
    (
      [
        "claimEventId",
        "claimSequence",
        "claimReceiptSequence",
        "birthEventId",
        "birthSequence",
        "birthReceiptSequence",
      ] as const
    ).some((field) => decision[field] !== candidate.preparation[field])
  )
    return fail();
  if (accepted === undefined) {
    if (
      current.status !== "preparing" ||
      event.id !== decision.evidenceEventId ||
      decision.deletion !== undefined
    )
      return fail();
    return true;
  }
  if (decision.deletion === undefined) return false;
  if (accepted.deletion !== undefined) {
    if (!toEquivalenceLegacyFailureDeletionProvenance(accepted.deletion, decision.deletion))
      return fail();
    return true;
  }
  const deletion = decodeLegacyFailureDeletionProvenance(decision.deletion);
  const proof = deletion.provenance;
  if (
    current.status !== "failed" ||
    proof.type !== "no_control" ||
    event.id !== proof.evidenceEventId ||
    proof.evidenceEventId !== `${proof.commandId}:event` ||
    proof.commandId !== `${decision.policy.createCommandId}:failure-delete` ||
    proof.threadId !== current.threadId ||
    proof.runId !== current.id ||
    !sameLegacyBootstrapPolicy(proof.policy, decision.policy) ||
    !sameLegacyBootstrapPolicy(proof.control.policy, decision.policy) ||
    proof.control.threadId !== current.threadId ||
    proof.control.runId !== current.id ||
    proof.preparationGeneration !== decision.preparationGeneration ||
    proof.control.preparationGeneration !== decision.preparationGeneration ||
    proof.workspacePath !== decision.workspacePath ||
    proof.projectWorkspaceRoot !== decision.projectWorkspaceRoot ||
    proof.control.workspacePath !== decision.workspacePath ||
    proof.control.projectWorkspaceRoot !== decision.projectWorkspaceRoot ||
    deletion.failureCommandId !== decision.failureCommandId ||
    deletion.failureEvidenceEventId !== decision.evidenceEventId ||
    deletion.failureEventSequence !== deletion.failureReceiptSequence ||
    deletion.failureEventSequence <= decision.outcomeEventSequence ||
    (
      [
        "claimEventId",
        "claimSequence",
        "claimReceiptSequence",
        "birthEventId",
        "birthSequence",
        "birthReceiptSequence",
      ] as const
    ).some((field) => proof[field] !== decision[field] || proof.control[field] !== decision[field])
  )
    return fail();
  return true;
}

/**
 * Receiving's private-field merge: a stale run snapshot cannot erase or replace recorded values.
 * Throws when incoming D or workspace-failure provenance is malformed or replaced.
 */
export function retainRunMetadata(
  current: RecordedRun | undefined,
  next: RecordedRun,
  event: RunEvent,
): RunMetadata {
  const authoritativeDeletion = validateDeletionProvenance(current, next, event);
  const authoritativeFailure = validateFailureDecision(current, next, event);
  if (current === undefined) return M.runMetadata(next);
  return M.runMetadata({
    ...next,
    ...(current.legacyBootstrap === undefined ? {} : { legacyBootstrap: current.legacyBootstrap }),
    ...(next.workspaceRunSetupScript === undefined && current.workspaceRunSetupScript !== undefined
      ? { workspaceRunSetupScript: current.workspaceRunSetupScript }
      : {}),
    ...(next.legacyPreparationFailureKnown === undefined &&
    current.legacyPreparationFailureKnown !== undefined
      ? { legacyPreparationFailureKnown: current.legacyPreparationFailureKnown }
      : {}),
    ...(current.legacyPreparation !== undefined && !isPreparationJournalEvent(next, event.id)
      ? { legacyPreparation: current.legacyPreparation }
      : {}),
    ...(current.legacyPreparationFailureDecision !== undefined && !authoritativeFailure
      ? { legacyPreparationFailureDecision: current.legacyPreparationFailureDecision }
      : {}),
    ...(current.legacyReleaseDecision !== undefined &&
    !authoritativeDeletion &&
    (current.legacyReleaseDecision.deletion !== undefined ||
      next.legacyReleaseDecision?.evidenceEventId !== event.id)
      ? { legacyReleaseDecision: current.legacyReleaseDecision }
      : {}),
  });
}

const malformed = (cause: unknown) =>
  new LegacyStorageError({ reason: "malformed_legacy_provenance", cause });

/**
 * SQL projection hook. Public events return before any read. An event carrying a private decision
 * is checked against the retained run and the actual ledger row of the same transaction.
 */
export const assertRunEvent = Effect.fn("LegacyIntegrity.assertRunEvent")(function* (
  storage: Storage.LegacyStorage,
  sql: SqlClient.SqlClient,
  input: Extract<OrchestrationV2DomainEvent, { readonly type: "run.created" | "run.updated" }>,
) {
  if (
    !("legacyReleaseDecision" in input.payload) &&
    !("legacyPreparationFailureDecision" in input.payload)
  )
    return;
  const event = yield* decodeLifecycleEvent(input).pipe(Effect.mapError(malformed));
  if (event.type !== "run.created" && event.type !== "run.updated") return;
  const next = event.payload;
  if (
    next.legacyReleaseDecision === undefined &&
    next.legacyPreparationFailureDecision === undefined
  )
    return;
  const current = (yield* storage.readRun(next.id))?.run;
  yield* Effect.try({
    try: () => {
      validateDeletionProvenance(current, next, event);
      validateFailureDecision(current, next, event);
    },
    catch: malformed,
  });
  const recordedCommand = Effect.fn("LegacyIntegrity.recordedCommand")(function* (
    expectedCommandId: string,
    streamId: string,
    cause: string,
  ) {
    const recorded = yield* sql<{
      command_id: string | null;
      stream_id: string;
      event_type: string;
    }>`
    SELECT command_id, stream_id, event_type FROM orchestration_events WHERE event_id = ${event.id}`;
    if (
      recorded.length !== 1 ||
      recorded[0]?.command_id !== expectedCommandId ||
      recorded[0].stream_id !== streamId ||
      recorded[0].event_type !== "run.updated"
    )
      return yield* malformed(cause);
  });
  const deletion = next.legacyReleaseDecision?.deletion;
  if (deletion !== undefined && current?.legacyReleaseDecision?.deletion === undefined)
    yield* recordedCommand(
      deletion.commandId,
      deletion.threadId,
      "D evidence lacks exact durable command correlation.",
    );
  const failure = next.legacyPreparationFailureDecision;
  if (
    failure !== undefined &&
    (current?.legacyPreparationFailureDecision === undefined ||
      (failure.deletion !== undefined &&
        current.legacyPreparationFailureDecision.deletion === undefined))
  )
    yield* recordedCommand(
      failure.deletion?.provenance.commandId ?? failure.failureCommandId,
      event.threadId,
      "Workspace-failure evidence lacks exact durable command correlation.",
    );
});

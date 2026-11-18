import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import type { CommandReceiptV2 } from "../../orchestration-v2/CommandReceiptStore.ts";
import type {
  RecordedLifecycleEvent,
  RecordedStoredLifecycleEvent,
} from "../../orchestration-v2/RecordedTypes.ts";
import {
  legacyBootstrapBirth,
  sameLegacyBootstrapPolicy,
  transitionLegacyPreparation,
  legacyPreparationReleaseBlocker,
} from "../../orchestration-v2/LegacyBootstrap.ts";
import type { LegacyCommitContext } from "./Decision.ts";
import * as Storage from "./Storage.ts";
import * as M from "./RecordedMetadata.ts";
import { LegacyStorageError } from "./StorageSchema.ts";

type Store = ReturnType<typeof Storage.make>;
const refused = (reason: string) => Effect.fail(new LegacyStorageError({ reason }));
export const validate = Effect.fn("LegacyTransaction.validate")(function* (
  storage: Store,
  context: LegacyCommitContext,
  events: readonly RecordedLifecycleEvent[],
  disposition: "accepted" | "rejected" = "accepted",
) {
  const { mutation, binding } = context;
  const thread = yield* storage.readThread(binding.threadId);
  const runId =
    mutation.kind === "claim" || mutation.kind === "birth"
      ? undefined
      : mutation.kind === "release"
        ? mutation.policy.runId
        : mutation.kind === "delete"
          ? mutation.command.runId
          : mutation.runId;
  const run = runId === undefined ? undefined : yield* storage.readRun(runId);
  if (
    (thread?.revision ?? null) !== context.expectedThreadRevision ||
    (run?.revision ?? null) !== context.expectedRunRevision ||
    M.canonical(thread === undefined ? {} : M.threadMetadata(thread.thread)) !==
      context.expectedThreadMetadata ||
    M.canonical(run === undefined ? {} : M.runMetadata(run.run)) !== context.expectedRunMetadata ||
    M.canonical(thread?.thread ?? null) !== context.expectedThread ||
    M.canonical(run?.run ?? null) !== context.expectedRun
  )
    return yield* refused("stale_legacy_metadata");
  if (
    thread !== undefined &&
    (thread.thread.projectId !== binding.projectId ||
      DateTime.formatIso(thread.thread.createdAt) !== binding.threadCreatedAt)
  )
    return yield* refused("legacy_incarnation_changed");
  const existing = yield* storage.findCommand(binding.commandId);
  if (existing !== undefined && M.canonical(existing.binding) !== M.canonical(binding))
    return yield* refused("legacy_command_collision");
  const policy =
    mutation.kind === "claim" || mutation.kind === "birth" || mutation.kind === "release"
      ? mutation.policy
      : run?.run.legacyBootstrap;
  if (
    policy === undefined ||
    policy.threadId !== binding.threadId ||
    policy.projectId !== binding.projectId
  )
    return yield* refused("legacy_policy_unavailable");
  if (mutation.kind === "claim") {
    if (
      binding.commandId !== policy.createCommandId ||
      policy.runId !== undefined ||
      thread?.thread.deletedAt != null ||
      (thread?.thread.legacyBootstrapClaim !== undefined &&
        !sameLegacyBootstrapPolicy(thread.thread.legacyBootstrapClaim, policy))
    )
      return yield* refused("legacy_claim_conflict");
    return;
  }
  const claim = yield* storage.readLifecycleByCommandId(policy.createCommandId);
  const claimReceipt = yield* storage.readLegacyReceipt(policy.createCommandId);
  if (
    claimReceipt?.domain !== "lifecycle" ||
    claimReceipt.receipt.status !== "accepted" ||
    claimReceipt.receipt.commandType !==
      (policy.ownsNewThread ? "thread.create" : "thread.metadata.update") ||
    claimReceipt.receipt.threadId !== policy.threadId ||
    !claim.some((stored) => stored.sequence === claimReceipt.receipt.resultSequence)
  )
    return yield* refused("claim_receipt_unavailable");
  if (mutation.kind === "birth") {
    if (
      binding.commandId !== policy.birthCommandId ||
      thread?.thread.legacyBootstrapClaim === undefined ||
      !sameLegacyBootstrapPolicy(thread.thread.legacyBootstrapClaim, policy)
    )
      return yield* refused("legacy_birth_conflict");
    return;
  }
  const birth = yield* storage.readLifecycleByCommandId(policy.birthCommandId);
  const birthReceipt = yield* storage.readLegacyReceipt(policy.birthCommandId);
  const proof = legacyBootstrapBirth({ policy, claimEvents: claim, birthEvents: birth });
  if (
    proof.type !== "valid" ||
    birthReceipt?.domain !== "lifecycle" ||
    birthReceipt.receipt.status !== "accepted" ||
    birthReceipt.receipt.commandType !== "message.dispatch" ||
    birthReceipt.receipt.threadId !== policy.threadId ||
    !birth.some((stored) => stored.sequence === birthReceipt.receipt.resultSequence)
  )
    return yield* refused("birth_receipt_unavailable");
  const preparation =
    mutation.kind === "progress" && mutation.update.type === "initialize"
      ? mutation.update.preparation
      : run?.run.legacyPreparation;
  if (
    preparation === undefined ||
    !sameLegacyBootstrapPolicy(preparation.policy, policy) ||
    preparation.claimEventId !== proof.claimEventId ||
    preparation.claimSequence !== proof.claimSequence ||
    preparation.claimReceiptSequence !== claimReceipt.receipt.resultSequence ||
    preparation.birthEventId !== proof.birthEventId ||
    preparation.birthSequence !== proof.sequence ||
    preparation.birthReceiptSequence !== birthReceipt.receipt.resultSequence
  )
    return yield* refused("preparation_anchor_changed");
  const planned = events.findLast(
    (event) =>
      (event.type === "run.updated" || event.type === "run.created") && event.payload.id === runId,
  );
  if (run === undefined || planned?.type !== "run.updated")
    return yield* refused("legacy_planned_run_unavailable");
  const plannedMetadata = M.runMetadata(planned.payload);
  const currentMetadata = M.runMetadata(run.run);
  if (mutation.kind === "progress") {
    const transition = transitionLegacyPreparation({
      current: run.run.legacyPreparation,
      update: mutation.update,
      commandId: binding.commandId,
    });
    if (transition.type === "rejected") return yield* refused("preparation_transition_refused");
    if (
      M.canonical(plannedMetadata) !==
      M.canonical({ ...currentMetadata, legacyPreparation: transition.preparation })
    )
      return yield* refused("stale_preparation_plan");
  } else {
    const {
      legacyReleaseDecision: plannedRelease,
      legacyPreparationFailureDecision: plannedFailure,
      legacyPreparationFailureKnown: _plannedKnown,
      ...plannedBase
    } = plannedMetadata;
    const {
      legacyReleaseDecision: currentRelease,
      legacyPreparationFailureDecision: currentFailure,
      legacyPreparationFailureKnown: _currentKnown,
      ...currentBase
    } = currentMetadata;
    if (M.canonical(plannedBase) !== M.canonical(currentBase))
      return yield* refused("stale_legacy_plan");
    if (
      mutation.kind === "release" &&
      disposition === "accepted" &&
      M.canonical(plannedMetadata) !== M.canonical(currentMetadata)
    )
      return yield* refused("stale_release_plan");
    if (mutation.kind === "delete") {
      const removeDeletion = (decision: typeof plannedRelease | typeof plannedFailure) =>
        decision === undefined
          ? undefined
          : (({ deletion: _deletion, ...original }) => original)(decision);
      if (
        M.canonical(removeDeletion(plannedRelease)) !==
          M.canonical(removeDeletion(currentRelease)) ||
        M.canonical(removeDeletion(plannedFailure)) !== M.canonical(removeDeletion(currentFailure))
      )
        return yield* refused("stale_delete_plan");
    }
  }
  if (
    mutation.kind === "release" &&
    (binding.commandId !== policy.releaseCommandId ||
      run === undefined ||
      legacyPreparationReleaseBlocker({ run: run.run }) !== undefined)
  )
    return yield* refused("release_not_ready");
});

// Called after actual public append, projection apply and receipt insertion, inside their transaction.
export const record = Effect.fn("LegacyTransaction.record")(function* (
  storage: Store,
  context: LegacyCommitContext,
  receipt: CommandReceiptV2,
  events: readonly RecordedStoredLifecycleEvent[],
) {
  const writer = yield* storage.forTransaction;
  if (context.mutation.kind !== "delete")
    yield* writer.recordLifecycle(context.binding, receipt, events);
  for (const stored of events) {
    const event = stored.event;
    const metadata = M.eventMetadata(stored);
    if (metadata === undefined) continue;
    yield* writer.recordEvent(stored);
    if (
      metadata.kind === "thread" &&
      (event.type === "thread.created" ||
        event.type === "thread.metadata-updated" ||
        event.type === "thread.deleted")
    ) {
      const policy = metadata.metadata.legacyBootstrapClaim;
      if (policy === undefined) continue;
      const claims =
        context.mutation.kind === "claim"
          ? events
          : yield* storage.readLifecycleByCommandId(policy.createCommandId);
      const claim = claims.find(
        (entry) =>
          entry.event.type ===
          (policy.ownsNewThread ? "thread.created" : "thread.metadata-updated"),
      );
      if (claim === undefined) return yield* refused("claim_event_unavailable");
      const existing = yield* storage.readThread(event.threadId);
      yield* writer.recordThread({
        identity: {
          threadId: event.threadId,
          projectId: event.payload.projectId,
          createdAt: DateTime.formatIso(event.payload.createdAt),
          claimEventId: claim.event.id,
          claimSequence: claim.sequence,
          claimCommandId: policy.createCommandId,
        },
        metadata: metadata.metadata,
        expectedRevision: existing?.revision ?? null,
      });
    }
    if (metadata.kind === "run" && (event.type === "run.created" || event.type === "run.updated")) {
      const policy = metadata.metadata.legacyBootstrap;
      if (policy === undefined) return yield* refused("run_policy_required");
      const births =
        context.mutation.kind === "birth"
          ? events
          : yield* storage.readLifecycleByCommandId(policy.birthCommandId);
      const birth = births.find(
        (entry) =>
          entry.event.type === "run.created" && entry.event.payload.id === event.payload.id,
      );
      if (birth === undefined || birth.commandId === null)
        return yield* refused("birth_event_unavailable");
      const existing = yield* storage.readRun(event.payload.id);
      yield* writer.recordRun({
        identity: {
          runId: event.payload.id,
          threadId: event.threadId,
          userMessageId: event.payload.userMessageId,
          requestedAt: DateTime.formatIso(event.payload.requestedAt),
          birthEventId: birth.event.id,
          birthSequence: birth.sequence,
          birthCommandId: birth.commandId,
        },
        metadata: metadata.metadata,
        metadataEvent: stored,
        expectedRevision: existing?.revision ?? null,
      });
    }
  }
});

import type {
  CommandId,
  EnvironmentId,
  ThreadId,
  OrchestrationV2ImportedHistoryReviewResult,
  OrchestrationV2ImportedHistoryStartReceipt,
  OrchestrationV2ObserveImportedHistoryStartInput,
  OrchestrationV2ReviewImportedHistoryStartInput,
  OrchestrationV2StartWithImportedHistoryCommand,
} from "@t3tools/contracts";
import {
  resolveImportedContinuationReceipt,
  resolveImportedContinuationReview,
} from "@t3tools/client-runtime/state/thread-continuation";
import type { ImportedContinuationPointer } from "../../state/use-composer-drafts";

export type ImportedContinuationReview = ReturnType<typeof resolveImportedContinuationReview>;
export type ImportedContinuationReceipt = ReturnType<typeof resolveImportedContinuationReceipt>;
export type ImportedContinuationStartInput = Omit<
  OrchestrationV2StartWithImportedHistoryCommand,
  "type"
>;

export interface MobileImportedContinuationPorts {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly savePointer: (pointer: ImportedContinuationPointer) => Promise<void>;
  readonly review: (
    input: OrchestrationV2ReviewImportedHistoryStartInput,
  ) => Promise<OrchestrationV2ImportedHistoryReviewResult>;
  readonly deliver: (
    input: ImportedContinuationStartInput,
  ) => Promise<OrchestrationV2ImportedHistoryStartReceipt>;
  readonly observe: (
    input: OrchestrationV2ObserveImportedHistoryStartInput,
  ) => Promise<OrchestrationV2ImportedHistoryStartReceipt>;
  readonly isCurrent: () => boolean;
}

export interface MobileImportedContinuationState {
  readonly reviewing: boolean;
  readonly submitting: boolean;
  readonly review: ImportedContinuationReview | null;
  readonly receipt: ImportedContinuationReceipt | null;
  readonly command: ImportedContinuationStartInput | null;
  readonly pointer: ImportedContinuationPointer | null;
  readonly notice: string | null;
  readonly nativeEffectsUnknown: boolean;
  readonly saveFailedBeforeDelivery: boolean;
}

export function importedContinuationTarget(
  delivery: OrchestrationV2ReviewImportedHistoryStartInput["delivery"],
): OrchestrationV2ImportedHistoryReviewResult["target"] {
  return delivery.type === "queued_run"
    ? { type: "queued_run", runId: delivery.runId, messageId: delivery.messageId }
    : { type: "message", messageId: delivery.messageId };
}

export function createMobileImportedContinuationDelivery(
  getPorts: () => MobileImportedContinuationPorts,
) {
  const initialPorts = getPorts();
  const scope = { environmentId: initialPorts.environmentId, threadId: initialPorts.threadId };
  const sameScope = (ports: MobileImportedContinuationPorts) =>
    ports.environmentId === scope.environmentId && ports.threadId === scope.threadId;
  let reviewed: OrchestrationV2ReviewImportedHistoryStartInput | null = null;
  let state: MobileImportedContinuationState = {
    reviewing: false,
    submitting: false,
    review: null,
    receipt: null,
    command: null,
    pointer: null,
    notice: null,
    nativeEffectsUnknown: false,
    saveFailedBeforeDelivery: false,
  };
  let reviewRevision = 0;
  const listeners = new Set<() => void>();
  const update = (next: Partial<MobileImportedContinuationState>) => {
    state = { ...state, ...next };
    for (const listener of listeners) listener();
  };
  const pending = () => state.pointer !== null;
  const receive = (
    receipt: OrchestrationV2ImportedHistoryStartReceipt,
    pointer: ImportedContinuationPointer,
    current: boolean,
  ) => {
    if (!current) {
      update({ notice: "The connection changed. Check the existing request before continuing." });
      return;
    }
    if (receipt.target === null) {
      update({
        receipt: {
          status: "unknown",
          intentAccepted: false,
          reason: "The existing request has no target receipt yet. Keep checking the same request.",
        },
        notice: null,
      });
      return;
    }
    const basis = state.command?.reviewedBasis ?? receipt.reviewedBasis;
    const resolved: ImportedContinuationReceipt =
      basis === null
        ? {
            status: "unknown",
            intentAccepted: false,
            reason: "The existing request has no reviewed receipt yet.",
          }
        : resolveImportedContinuationReceipt(receipt, {
            threadId: pointer.threadId,
            commandId: pointer.commandId,
            target: pointer.target,
            reviewedBasis: basis,
          });
    update({
      receipt: resolved,
      ...(resolved.status === "started" || resolved.status === "rejected" ? { review: null } : {}),
      notice: null,
    });
  };
  return {
    getSnapshot: () => state,
    subscribe: (listener: () => void) => {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    restorePointer: (pointer: ImportedContinuationPointer) => {
      const ports = getPorts();
      if (
        !sameScope(ports) ||
        state.submitting ||
        state.pointer !== null ||
        pointer.environmentId !== ports.environmentId ||
        pointer.threadId !== ports.threadId
      )
        return;
      reviewed = null;
      update({
        pointer,
        command: null,
        review: null,
        receipt: null,
        notice: "Check the existing imported history request before sending another message.",
      });
    },
    retirePointer: (expected: ImportedContinuationPointer) => {
      if (
        state.pointer !== expected ||
        (state.receipt?.status !== "started" && state.receipt?.status !== "rejected")
      )
        return;
      update({ pointer: null, command: null, review: null });
    },
    invalidateReview: () => {
      if (state.submitting || pending()) return;
      reviewRevision += 1;
      reviewed = null;
      update({
        review: state.nativeEffectsUnknown ? state.review : null,
        receipt: null,
        command: null,
        notice: state.nativeEffectsUnknown ? state.notice : null,
      });
    },
    review: async (input: OrchestrationV2ReviewImportedHistoryStartInput) => {
      if (state.reviewing || state.submitting || pending()) return;
      const ports = getPorts();
      if (!sameScope(ports) || input.threadId !== scope.threadId || !ports.isCurrent()) return;
      const revision = ++reviewRevision;
      reviewed = null;
      update({ reviewing: true, review: null, receipt: null, command: null, notice: null });
      try {
        const result = await ports.review(input);
        if (!sameScope(ports) || !ports.isCurrent() || revision !== reviewRevision) return;
        reviewed = input;
        const correlated =
          result.threadId === input.threadId &&
          JSON.stringify(result.target) ===
            JSON.stringify(importedContinuationTarget(input.delivery));
        update({
          review: resolveImportedContinuationReview(result, {
            threadId: input.threadId,
            target: importedContinuationTarget(input.delivery),
          }),
          nativeEffectsUnknown: correlated
            ? result.nativeEffects.type === "unknown"
            : state.nativeEffectsUnknown,
        });
      } catch {
        update({
          notice: "Imported history review is unavailable. Your message is still pending.",
        });
      } finally {
        update({ reviewing: false });
      }
    },
    start: async (commandId: CommandId) => {
      const retrySave =
        state.saveFailedBeforeDelivery && state.command !== null && state.pointer !== null;
      if (
        state.submitting ||
        state.reviewing ||
        state.nativeEffectsUnknown ||
        (pending() && !retrySave) ||
        (retrySave && commandId !== state.command?.commandId) ||
        (!retrySave && (reviewed === null || state.review?.status !== "available"))
      )
        return;
      const ports = getPorts();
      if (!sameScope(ports) || !ports.isCurrent()) return;
      const command: ImportedContinuationStartInput = retrySave
        ? state.command!
        : {
            commandId,
            threadId: reviewed!.threadId,
            reviewedBasis: (
              state.review as Extract<ImportedContinuationReview, { status: "available" }>
            ).reviewedBasis,
            delivery: reviewed!.delivery,
          };
      const pointer: ImportedContinuationPointer = retrySave
        ? state.pointer!
        : {
            environmentId: ports.environmentId,
            threadId: command.threadId,
            commandId,
            target: importedContinuationTarget(command.delivery),
          };
      update({
        command,
        pointer,
        submitting: true,
        receipt: null,
        notice: null,
        saveFailedBeforeDelivery: false,
      });
      try {
        await ports.savePointer(pointer);
      } catch {
        update({
          submitting: false,
          saveFailedBeforeDelivery: true,
          notice:
            "The request was not sent because its receipt pointer could not be saved. Save and start the same request when storage is available. Your draft is unchanged.",
        });
        return;
      }
      if (!sameScope(ports) || !ports.isCurrent()) {
        update({
          submitting: false,
          notice:
            "The connection changed before delivery. Check the saved request before continuing.",
        });
        return;
      }
      try {
        receive(await ports.deliver(command), pointer, sameScope(ports) && ports.isCurrent());
      } catch {
        update({
          notice: "Delivery is unconfirmed. Check the existing request; do not send it again.",
        });
      } finally {
        update({ submitting: false });
      }
    },
    observe: async () => {
      if (state.submitting || state.pointer === null) return;
      const pointer = state.pointer;
      const ports = getPorts();
      if (!sameScope(ports) || !ports.isCurrent() || state.saveFailedBeforeDelivery) return;
      update({ submitting: true });
      try {
        receive(
          await ports.observe({ threadId: pointer.threadId, commandId: pointer.commandId }),
          pointer,
          sameScope(ports) && ports.isCurrent(),
        );
      } catch {
        update({ notice: "Could not confirm the existing request. Your message remains pending." });
      } finally {
        update({ submitting: false });
      }
    },
  };
}

export interface MobileImportedContinuationPresentation {
  readonly notice: string | null;
  readonly canStart: boolean;
  readonly canObserve: boolean;
  readonly busy: boolean;
  readonly blocksOrdinarySend: boolean;
  readonly isSaveRetry: boolean;
}

export function presentMobileImportedContinuation(
  state: MobileImportedContinuationState,
): MobileImportedContinuationPresentation {
  const receipt = state.receipt;
  const unresolved = state.pointer !== null;
  const review = state.review;
  const retrySave = state.saveFailedBeforeDelivery && state.command !== null;
  const notice =
    state.notice ??
    (receipt?.status === "started"
      ? "A new agent conversation has started using imported history."
      : receipt?.status === "pending"
        ? "The request is accepted. The new agent conversation has not started yet."
        : receipt?.status === "unknown"
          ? receipt.reason
          : receipt?.status === "rejected"
            ? receipt.reason
            : review?.status === "available"
              ? "Start a new agent conversation using the imported history. You can leave this message pending."
              : review?.status === "held" || review?.status === "unknown"
                ? review.reason
                : null);
  return {
    notice,
    canStart:
      (review?.status === "available" || retrySave) &&
      (!unresolved || retrySave) &&
      !state.nativeEffectsUnknown &&
      !state.reviewing &&
      !state.submitting,
    canObserve:
      unresolved &&
      !retrySave &&
      receipt?.status !== "started" &&
      receipt?.status !== "rejected" &&
      !state.submitting,
    isSaveRetry: retrySave,
    busy: state.reviewing || state.submitting,
    blocksOrdinarySend:
      state.nativeEffectsUnknown ||
      state.reviewing ||
      state.submitting ||
      (unresolved && receipt?.status !== "started") ||
      review?.status === "available" ||
      review?.status === "held" ||
      review?.status === "unknown",
  };
}

export function canUseOrdinaryImportedContinuationDelivery(
  state: MobileImportedContinuationState,
  persistedPointer: ImportedContinuationPointer | undefined,
): boolean {
  return (
    persistedPointer === undefined && !presentMobileImportedContinuation(state).blocksOrdinarySend
  );
}

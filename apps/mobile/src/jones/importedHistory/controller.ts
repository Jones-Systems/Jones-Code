import {
  type CommandId,
  type EnvironmentId,
  type ThreadId,
  type ImportedHistoryDelivery,
  type ImportedHistoryReview,
  type ImportedHistoryStart,
  type ImportedHistoryOutcome,
} from "@t3tools/contracts";
import {
  createImportedHistoryChoiceController,
  resolveImportedHistoryReview,
  type ImportedHistoryCorrelation,
  type ImportedHistoryCorrelationStorage,
  type ImportedHistoryChoiceState,
} from "@t3tools/client-runtime/jones/imported-history/continuation";

export interface MobileImportedHistoryPorts {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly storage: ImportedHistoryCorrelationStorage;
  readonly isCurrent: () => boolean;
  readonly allocateCommandId: () => CommandId;
  readonly review: (delivery: ImportedHistoryDelivery) => Promise<ImportedHistoryReview | null>;
  readonly identity: (
    command: ImportedHistoryStart,
  ) => Promise<Pick<ImportedHistoryCorrelation, "commandDigest" | "deliveryDigest">>;
  readonly persistReadback: (correlation: ImportedHistoryCorrelation) => Promise<void>;
  readonly start: (command: ImportedHistoryStart) => Promise<ImportedHistoryOutcome | null>;
  readonly observe: (
    command: Pick<ImportedHistoryStart, "threadId" | "commandId">,
  ) => Promise<ImportedHistoryOutcome | null>;
}
export type MobileImportedHistorySnapshot = {
  readonly busy: boolean;
  readonly canStart: boolean;
  readonly pending: boolean;
  readonly notice: string | null;
  readonly outcome: ImportedHistoryChoiceState | null;
};
export function createMobileImportedHistoryChoice(ports: MobileImportedHistoryPorts) {
  let state: MobileImportedHistorySnapshot = {
    busy: false,
    canStart: false,
    pending: false,
    notice: null,
    outcome: null,
  };
  let prepared: {
    delivery: ImportedHistoryDelivery;
    reviewedBasis: string;
    draftIdentity: string;
    unchanged: () => boolean;
  } | null = null;
  let deliveryHeldReason: string | null = null;
  let hydrated = false;
  const listeners = new Set<() => void>();
  const update = (patch: Partial<MobileImportedHistorySnapshot>) => {
    state = { ...state, ...patch };
    for (const listener of listeners) listener();
  };
  const shared = createImportedHistoryChoiceController(ports.storage, {
    start: async (command) => {
      const pointer = ports.storage.read();
      if (pointer === null || pointer.command.commandId !== command.commandId)
        throw new Error("Saved imported history command changed.");
      try {
        await ports.persistReadback(pointer);
      } catch (error) {
        deliveryHeldReason = "Could not save the choice. No start was submitted.";
        throw error;
      }
      if (prepared === null || !ports.isCurrent() || !prepared.unchanged()) {
        deliveryHeldReason =
          "The draft or target changed during persistence. No start was submitted; observe the saved command.";
        throw new Error("Imported history delivery target changed.");
      }
      return ports.start(command);
    },
    observe: ports.observe,
  });
  const receive = (outcome: ImportedHistoryChoiceState) => {
    update({
      outcome,
      pending: ports.storage.read() !== null,
      canStart: false,
      notice: deliveryHeldReason ?? outcome.reason,
    });
  };
  return {
    snapshot: () => state,
    subscribe(listener: () => void) {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    async hydrate() {
      if (state.busy || hydrated) return;
      update({ busy: true });
      try {
        await ports.storage.withLock(async () => {
          const saved = ports.storage.read();
          hydrated = true;
          update({
            pending: saved !== null,
            notice:
              saved === null
                ? null
                : "A saved choice awaits observation; execution is unconfirmed.",
          });
        });
      } catch {
        update({
          pending: true,
          notice: "Saved choice is unavailable. Resolve persistence before continuing.",
        });
      } finally {
        update({ busy: false });
      }
    },
    async review(
      delivery: ImportedHistoryDelivery,
      draftIdentity: string,
      unchanged: () => boolean,
    ) {
      if (state.busy) return;
      update({ busy: true, canStart: false, outcome: null });
      prepared = null;
      try {
        await ports.storage.withLock(async () => {
          const saved = ports.storage.read();
          hydrated = true;
          if (saved !== null) {
            update({
              pending: true,
              notice: "Observe the saved choice before another submission.",
            });
            return;
          }
          if (!ports.isCurrent() || !unchanged()) {
            update({ notice: "The draft or target changed. Review it again." });
            return;
          }
          const review = resolveImportedHistoryReview(await ports.review(delivery));
          if (!ports.isCurrent() || !unchanged()) {
            update({ notice: "The draft or target changed. Review it again." });
            return;
          }
          if (review.status === "available")
            prepared = { delivery, reviewedBasis: review.reviewedBasis, draftIdentity, unchanged };
          update({
            pending: false,
            canStart: prepared !== null,
            notice: review.reason ?? "Use the reviewed imported history for a new conversation?",
          });
        });
      } catch {
        update({ notice: "Imported history review is unavailable." });
      } finally {
        update({ busy: false });
      }
    },
    async start() {
      if (state.busy || !hydrated) return;
      update({ busy: true });
      deliveryHeldReason = null;
      try {
        if (ports.storage.read() !== null) {
          receive(await shared.observe());
          return;
        }
        const current = prepared;
        if (current === null || !ports.isCurrent() || !current.unchanged()) {
          prepared = null;
          update({ canStart: false, notice: "The draft or target changed. Review it again." });
          return;
        }
        const command: ImportedHistoryStart = {
          type: "thread.imported-history.start",
          commandId:
            current.delivery.type === "message"
              ? current.delivery.command.commandId
              : ports.allocateCommandId(),
          threadId: ports.threadId,
          reviewedBasis: current.reviewedBasis,
          delivery: current.delivery,
        };
        const identity = await ports.identity(command);
        receive(
          await shared.start(
            {
              environmentId: ports.environmentId,
              command,
              draftIdentity: current.draftIdentity,
              ...identity,
            },
            () => ports.isCurrent() && current.unchanged(),
          ),
        );
      } catch {
        update({
          canStart: false,
          notice:
            "The reviewed choice is unavailable. Observe any saved command before continuing.",
        });
      } finally {
        update({ busy: false });
      }
    },
    async observe() {
      if (state.busy || !hydrated) return;
      update({ busy: true });
      deliveryHeldReason = null;
      try {
        receive(await shared.observe());
      } catch {
        update({ pending: true, notice: "The saved choice could not be observed." });
      } finally {
        update({ busy: false });
      }
    },
  };
}
export type MobileImportedHistoryChoice = ReturnType<typeof createMobileImportedHistoryChoice>;

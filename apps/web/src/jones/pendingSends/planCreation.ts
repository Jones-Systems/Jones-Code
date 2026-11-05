import { AsyncResult } from "effect/reactivity";
import * as Cause from "effect/Cause";
import type { CreateThreadInput, PreparedThreadTurn } from "@t3tools/client-runtime/operations";
import type { AtomCommandResult } from "@t3tools/client-runtime/state/runtime";
import { squashAtomCommandFailure } from "@t3tools/client-runtime/state/runtime";
import {
  planCreateDelivery,
  planCreationSlotKey,
  type PlanCreationOperation,
  type PlanCreationSlot,
} from "@t3tools/client-runtime/jones/planCreationOperation";
import type { CommandId, EnvironmentId } from "@t3tools/contracts";
import { sendPendingTurn } from "./send";
import { pendingSends, type createPendingSendStore } from "./store";
import { planCreationStorage, type createPlanCreationStorage } from "./planCreationStorage";

interface Callbacks {
  readonly accepted: () => void;
  readonly messageRejected: () => void;
}
interface PlanAction {
  readonly slot: PlanCreationSlot;
  readonly allocate: () => Promise<PlanCreationOperation>;
  readonly create: (input: CreateThreadInput) => Promise<AtomCommandResult<unknown, unknown>>;
  readonly dispatch: (prepared: PreparedThreadTurn) => Promise<AtomCommandResult<unknown, unknown>>;
  readonly admitted: (operation: PlanCreationOperation) => Callbacks;
}
interface PlanCreationResult {
  readonly status: "unknown" | "held" | "accepted" | "create-rejected" | "message-rejected";
  readonly operation?: PlanCreationOperation;
}

export function createPlanCreationController(
  journal: ReturnType<typeof createPlanCreationStorage>,
  store: ReturnType<typeof createPendingSendStore> = pendingSends,
) {
  const running = new Map<string, Promise<PlanCreationResult>>();
  const generations = new Map<EnvironmentId, number>();
  const bindings = new Map<CommandId, {
    readonly environmentId: EnvironmentId;
    readonly callbacks: Callbacks;
    accepted: boolean;
    rejected: boolean;
    settlement: Promise<unknown>;
  }>();
  const execute = async (action: PlanAction): Promise<PlanCreationResult> => {
    let operation: PlanCreationOperation | undefined;
    let entryKey: string | undefined;
    const generation = generations.get(action.slot.environmentId);
    const ensureCurrent = () => {
      if (generations.get(action.slot.environmentId) !== generation)
        throw new Error("This environment's saved plan actions were removed.");
    };
    try {
      // Admission reads the saved identity before the lazy allocator reads a picker/catalog.
      operation = await journal.admit(action.slot, action.allocate);
      ensureCurrent();
      const captured = operation;
      const request = operation.prepared;
      if (request.method !== "orchestration.dispatchCommand" || request.payload.type !== "message.dispatch")
        throw new Error("Saved plan message does not match its action.");
      const message = request.payload;
      if (operation.phase === "accepted") return { status: "accepted", operation };
      entryKey = store.admit(operation.environmentId, operation.create.threadId, {
        id: message.messageId,
        role: "user",
        text: message.text,
        createdAt: operation.create.createdAt,
        updatedAt: operation.create.createdAt,
        runId: null,
        streaming: false,
      });
      store.retainDurably(entryKey);
      let binding = bindings.get(operation.create.commandId);
      if (!binding) {
        binding = { environmentId: operation.environmentId, callbacks: action.admitted(operation), accepted: false, rejected: false, settlement: Promise.resolve() };
        bindings.set(operation.create.commandId, binding);
        const observer = binding;
        store.whenAccepted(operation.environmentId, operation.create.threadId, message.messageId, () => {
          observer.settlement = observer.settlement.catch(() => undefined).then(async () => {
            ensureCurrent();
            await journal.advance(captured, "accepted");
            ensureCurrent();
            if (!observer.accepted) {
              observer.accepted = true;
              observer.callbacks.accepted();
            }
          });
          // The durable record stays pending if persistence fails; explicit recovery replays it.
          void observer.settlement.catch(() => undefined);
        });
        store.whenRejected(operation.environmentId, operation.create.threadId, message.messageId, () => {
          observer.settlement = observer.settlement.catch(() => undefined).then(async () => {
            ensureCurrent();
            const current = await journal.read(captured);
            if (!current || current.phase === "create-rejected" || current.phase === "create-pending") return;
            const next = await journal.advance(captured, "message-rejected");
            if (next.phase !== "accepted" && !observer.rejected) {
              observer.rejected = true;
              observer.callbacks.messageRejected();
            }
          });
          void observer.settlement.catch(() => undefined);
        });
      }
      if (operation.phase === "message-rejected") {
        store.confirmThreadCreation(operation.environmentId, operation.create.threadId, message.messageId);
        store.finish(entryKey, "rejected");
        await binding.settlement;
        return { status: "message-rejected", operation };
      }
      if (operation.phase === "create-pending") {
        store.awaitingCreation(entryKey, () => run(action), true);
        const result = await action.create(operation.create);
        ensureCurrent();
        const disposition = result._tag === "Success" ? "accepted" :
          planCreateDelivery(squashAtomCommandFailure(result), operation);
        if (disposition === "unknown") {
          store.awaitingCreation(entryKey, () => run(action), false);
          return { status: "unknown", operation };
        }
        if (disposition === "rejected") {
          operation = await journal.advance(operation, "create-rejected");
          store.finish(entryKey, "rejected");
          await binding.settlement;
          store.dismiss(entryKey);
          bindings.delete(operation.create.commandId);
          return { status: "create-rejected", operation };
        }
        // Creation has its own receipt. Commit the message stage before issuing that command.
        operation = await journal.advance(operation, "message-pending");
        ensureCurrent();
      }
      store.confirmThreadCreation(operation.environmentId, operation.create.threadId, message.messageId);
      const delivery = store.get(operation.environmentId, operation.create.threadId, message.messageId)?.delivery;
      if (delivery === "failed") {
        await binding.settlement.catch(() => undefined);
        operation = await journal.advance(operation, "message-rejected");
        if (!binding.rejected) {
          binding.rejected = true;
          binding.callbacks.messageRejected();
        }
      } else if (delivery === "accepted") {
        await binding.settlement.catch(() => undefined);
        operation = await journal.advance(operation, "accepted");
        if (!binding.accepted) {
          binding.accepted = true;
          binding.callbacks.accepted();
        }
      } else {
        ensureCurrent();
        await sendPendingTurn({
          environmentId: operation.environmentId,
          input: {
            commandId: message.commandId,
            threadId: message.threadId,
            createdAt: operation.create.createdAt,
            message: { messageId: message.messageId, role: "user", text: message.text, attachments: [] },
            runtimeMode: operation.create.runtimeMode,
            interactionMode: operation.create.interactionMode,
            dispatchMode: "start",
          },
          prepare: async () => AsyncResult.success(request),
          dispatch: async (prepared) => {
            try {
              return await action.dispatch(prepared);
            } catch (error) {
              return AsyncResult.failure(Cause.fail(error));
            }
          },
          store,
        });
        await binding.settlement;
        operation = await journal.read(operation) ?? operation;
      }
      return { status: operation.phase === "accepted" || operation.phase === "message-rejected" ? operation.phase : "unknown", operation };
    } catch {
      if (operation?.phase === "create-pending" && entryKey && generations.get(action.slot.environmentId) === generation)
        store.awaitingCreation(entryKey, () => run(action), false);
      return { status: "held", ...(operation ? { operation } : {}) };
    }
  };
  const run = (action: PlanAction): Promise<PlanCreationResult> => {
    const slot = planCreationSlotKey(action.slot);
    const existing = running.get(slot);
    if (existing) return existing;
    const work = execute(action).finally(() => running.delete(slot));
    running.set(slot, work);
    return work;
  };
  return {
    run,
    async clearEnvironment(environmentId: EnvironmentId) {
      generations.set(environmentId, (generations.get(environmentId) ?? 0) + 1);
      await journal.clearEnvironment(environmentId);
      for (const [id, binding] of bindings)
        if (binding.environmentId === environmentId) bindings.delete(id);
      for (const entry of store.getSnapshot())
        if (entry.environmentId === environmentId && entry.durable) store.dismiss(entry.key);
    },
  };
}

export const planCreation = createPlanCreationController(planCreationStorage);

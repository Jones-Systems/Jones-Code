import { planPinnedReorder } from "@t3tools/client-runtime/state/thread-sort";
import type { SidebarDropTarget, SidebarSection } from "../Sidebar.logic";
import { ThreadMovementError } from "../workstreams/nativeWorkstreamActions";

export interface SelectedShelfThread {
  readonly key: string;
  readonly section: SidebarSection;
  readonly pinned: boolean;
  readonly settled: boolean;
  readonly supportsPinning: boolean;
  readonly supportsSettlement: boolean;
  readonly supportsSnooze: boolean;
}

export type SelectedShelfStep = {
  readonly key: string;
  readonly operation:
    | "pin"
    | "unpin"
    | "unsettle"
    | "unsnooze"
    | "settle"
    | "order-active"
    | "order-pinned";
  readonly orderKey?: string;
};

export function planSelectedShelfDrop(input: {
  readonly initiator: string;
  readonly threads: readonly SelectedShelfThread[];
  readonly target: SidebarDropTarget;
  readonly currentOrder: readonly string[];
  readonly keysById: ReadonlyMap<string, string | null | undefined>;
  readonly reorderableKeys: ReadonlySet<string>;
}): readonly SelectedShelfStep[] {
  const { threads, target } = input;
  const selected = new Set(threads.map((thread) => thread.key));
  const steps: SelectedShelfStep[] = [];
  for (const thread of threads) {
    if (target.section === "settled") {
      if (thread.settled) continue;
      if (!thread.supportsSettlement)
        throw new Error(`Settlement is unavailable for ${thread.key}. No threads were moved.`);
      steps.push({ key: thread.key, operation: "settle" });
      continue;
    }
    if (target.section === "pinned") {
      if (!thread.supportsPinning || (thread.settled && !thread.supportsSettlement))
        throw new Error(`Pinning is unavailable for ${thread.key}. No threads were moved.`);
      if (!thread.pinned || thread.settled || thread.section === "snoozed")
        steps.push({ key: thread.key, operation: "pin" });
    } else {
      if (thread.pinned) {
        if (!thread.supportsPinning)
          throw new Error(`Unpinning is unavailable for ${thread.key}. No threads were moved.`);
        steps.push({ key: thread.key, operation: "unpin" });
      }
      if (thread.settled) {
        if (!thread.supportsSettlement)
          throw new Error(`Restoration is unavailable for ${thread.key}. No threads were moved.`);
        steps.push({ key: thread.key, operation: "unsettle" });
      }
      if (thread.section === "snoozed") {
        if (!thread.supportsSnooze)
          throw new Error(`Waking is unavailable for ${thread.key}. No threads were moved.`);
        steps.push({ key: thread.key, operation: "unsnooze" });
      }
    }
  }
  if (target.section === "settled") return steps;
  const singleOrder = target.section === "pinned" ? target.pinnedOrder : target.activeOrder;
  const initiatorIndex = singleOrder.indexOf(input.initiator);
  if (initiatorIndex === -1)
    throw new Error("The drop position is unavailable. No threads were moved.");
  const insertionIndex = singleOrder
    .slice(0, initiatorIndex)
    .filter((key) => !selected.has(key)).length;
  const order = singleOrder.filter((key) => !selected.has(key));
  order.splice(insertionIndex, 0, ...threads.map((thread) => thread.key));
  if (
    steps.length === 0 &&
    order.length === input.currentOrder.length &&
    order.every((key, index) => key === input.currentOrder[index])
  )
    return [];
  const keys = new Map(input.keysById);
  for (const key of selected) keys.set(key, null);
  const assignments = planPinnedReorder({
    orderedIds: order,
    keysById: keys,
    movedId: input.initiator,
  });
  if (assignments.some(({ id }) => !input.reorderableKeys.has(id)))
    throw new Error(
      "An environment does not support selected thread ordering. No threads were moved.",
    );
  const assigned = new Map(assignments.map(({ id, orderKey }) => [id, orderKey]));
  return [
    ...steps.map((step) => {
      if (step.operation !== "pin") return step;
      const orderKey = assigned.get(step.key);
      if (orderKey === undefined)
        throw new Error("The selected pin position is unavailable. No threads were moved.");
      return { ...step, orderKey };
    }),
    ...assignments
      .filter(({ id }) => !steps.some((step) => step.key === id && step.operation === "pin"))
      .map(({ id, orderKey }) => ({
        key: id,
        operation:
          target.section === "active" ? ("order-active" as const) : ("order-pinned" as const),
        orderKey,
      })),
  ];
}

export async function runSelectedThreadSteps(input: {
  readonly selectedKeys: readonly string[];
  readonly steps: readonly SelectedShelfStep[];
  readonly run: (step: SelectedShelfStep) => Promise<void>;
}): Promise<readonly string[]> {
  const remaining = new Map(
    input.selectedKeys.map((key) => [key, input.steps.filter((step) => step.key === key).length]),
  );
  for (const step of input.steps) {
    try {
      await input.run(step);
      if (remaining.has(step.key)) remaining.set(step.key, remaining.get(step.key)! - 1);
    } catch (cause) {
      throw new ThreadMovementError(
        `${step.operation}: ${cause instanceof Error ? cause.message : "effect unknown"}`,
        input.selectedKeys.filter((key) => remaining.get(key) === 0),
        step.key,
        input.selectedKeys.filter((key) => remaining.get(key)! > 0 && key !== step.key),
        null,
      );
    }
  }
  return input.selectedKeys;
}

export async function runSelectedShelfSteps(
  input: Parameters<typeof runSelectedThreadSteps>[0] & {
    readonly removeMembership: () => Promise<void>;
  },
): Promise<readonly string[]> {
  try {
    await input.removeMembership();
  } catch (cause) {
    // Committed membership removal alone does not complete native shelf placement.
    if (cause instanceof ThreadMovementError)
      throw new ThreadMovementError(
        cause.message,
        [],
        cause.stoppedKey,
        input.selectedKeys.filter((key) => key !== cause.stoppedKey),
        cause.commandId,
      );
    throw cause;
  }
  return runSelectedThreadSteps(input);
}

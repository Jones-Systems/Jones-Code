import {
  isAtomCommandInterrupted,
  squashAtomCommandFailure,
} from "@t3tools/client-runtime/state/runtime";
import type { ContextMenuItem, ScopedThreadRef } from "@t3tools/contracts";

import { withSettlementRestoreMenuItem } from "./menu";
import {
  readEnvironmentSupportsSettlementRestore,
  restoreCurrentSettlement,
} from "./settlementRestore";

/**
 * Undo settle for a one-thread menu. The chat header and the sidebar build that
 * menu separately, so both add the item through here.
 */
export function withThreadSettlementRestore<T extends string>(
  items: ReadonlyArray<ContextMenuItem<T>>,
  input: {
    readonly threadRef: ScopedThreadRef;
    readonly isSettled: boolean;
    readonly canOperate: boolean;
  },
) {
  return withSettlementRestoreMenuItem(items, {
    available:
      input.isSettled && readEnvironmentSupportsSettlementRestore(input.threadRef.environmentId),
    canOperate: input.canOperate,
  });
}

/** Runs the menu's Undo settle; a refusal or failure, but not an interruption, is reported. */
export async function undoSettleFromMenu(
  threadRef: ScopedThreadRef,
  reportFailure: (error: unknown) => void,
): Promise<void> {
  const result = await restoreCurrentSettlement(threadRef);
  if (result._tag === "Failure" && !isAtomCommandInterrupted(result)) {
    reportFailure(squashAtomCommandFailure(result));
  }
}

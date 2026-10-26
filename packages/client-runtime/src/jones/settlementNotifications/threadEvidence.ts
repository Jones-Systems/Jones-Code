import type { OrchestrationV2ThreadFamily, ThreadId } from "@t3tools/contracts";
import * as Option from "effect/Option";

import type { EnvironmentShellState } from "../../state/shell.ts";
import { threadFamilyRowsMissingFromShell } from "../../state/threadFamilies.ts";

/** A successful family read bound to the authoritative shell generation that requested it. */
export interface SettlementThreadFamilyEvidence {
  readonly threadId: ThreadId;
  readonly generation: number;
  readonly family: OrchestrationV2ThreadFamily;
}

export function settlementThreadEvidence(
  state: EnvironmentShellState,
  families: ReadonlyArray<SettlementThreadFamilyEvidence> = [],
) {
  const snapshot = Option.getOrNull(state.snapshot);
  const shellThreads = snapshot === null ? [] : [...snapshot.threads, ...snapshot.archivedThreads];
  const currentFamilies = families.filter(
    (read) => state.generation > 0 && read.generation === state.generation,
  );
  const threads = [
    ...shellThreads,
    ...threadFamilyRowsMissingFromShell({
      shellThreads,
      families: currentFamilies.map((read) => read.family),
      removedAt: state.removedAt,
    }),
  ];
  const gone = new Set(state.removedAt.keys());
  for (const read of currentFamilies) {
    if (!threads.some((thread) => thread.id === read.threadId)) gone.add(read.threadId);
  }
  for (const thread of threads) {
    if (thread.deletedAt !== null) gone.add(thread.id);
    else if ((state.removedAt.get(thread.id) ?? -1) <= (snapshot?.snapshotSequence ?? -1)) {
      // A newer present row can restore a thread after a removal.
      gone.delete(thread.id);
    }
  }
  return { threads, gone };
}

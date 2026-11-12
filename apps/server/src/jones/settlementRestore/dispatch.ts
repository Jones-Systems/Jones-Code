import type {
  OrchestrationV2AppThread,
  OrchestrationV2ThreadShell,
  SettlementRestoreCommand,
  SettlementRestoreRejectionCode,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import { planSettlementRestore } from "./plan.ts";
import type { SettlementRestoreStore } from "./store.ts";

interface ThreadReads<E> {
  readonly getThreadShell: (
    threadId: ThreadId,
  ) => Effect.Effect<OrchestrationV2ThreadShell | null, E>;
  readonly getThread: (threadId: ThreadId) => Effect.Effect<OrchestrationV2AppThread, E>;
}

/** The orchestrator's decision for a restore command after a receipt miss, under the thread lock. */
export const decideSettlementRestore = Effect.fn(
  "SettlementRestoreDispatch.decideSettlementRestore",
)(function* <E>(
  store: SettlementRestoreStore,
  threads: ThreadReads<E>,
  command: SettlementRestoreCommand,
) {
  const shell = yield* threads.getThreadShell(command.threadId);
  const thread =
    shell === null || shell.deletedAt !== null ? null : yield* threads.getThread(command.threadId);
  return planSettlementRestore({
    command,
    stored: yield* store.read(command.threadId),
    sameRestoreCommand: yield* store.findRestoreCommand(command.commandId),
    thread,
    now: yield* DateTime.now,
  });
});

/**
 * Generic receipt replay only checks the thread. A restore receipt must also
 * belong to a restore of the same settle while its record still names it.
 */
export const checkSettlementRestoreReplay = Effect.fn(
  "SettlementRestoreDispatch.checkSettlementRestoreReplay",
)(function* (
  store: SettlementRestoreStore,
  command: SettlementRestoreCommand,
  receiptCommandType: string,
) {
  const reused: SettlementRestoreRejectionCode = "command_id_reused";
  if (receiptCommandType !== command.type) return reused;
  const same = yield* store.findRestoreCommand(command.commandId);
  return same !== null &&
    (same.threadId !== command.threadId || same.record.settleCommandId !== command.settleCommandId)
    ? reused
    : null;
});

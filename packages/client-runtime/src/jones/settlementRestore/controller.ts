import type {
  CommandId,
  EnvironmentId,
  SettlementRestoreCommand,
  SettlementRestoreInspectQuery,
  SettlementRestoreInspection,
  ThreadId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";

import type { AtomCommandResult } from "../../state/runtime.ts";
import {
  inspectionRejection,
  isPreviouslyRejectedCommand,
  readSettlementRestoreRejection,
  type SettlementRestoreOutcome,
} from "./model.ts";

export interface SettlementRestoreTarget {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
}

export interface SettlementRestorePorts {
  readonly dispatch: (
    environmentId: EnvironmentId,
    command: Omit<SettlementRestoreCommand, "type">,
  ) => Promise<AtomCommandResult<unknown, unknown>>;
  readonly inspect: (
    environmentId: EnvironmentId,
    query: SettlementRestoreInspectQuery,
  ) => Promise<AtomCommandResult<SettlementRestoreInspection, unknown>>;
  readonly newCommandId: () => CommandId;
}

/**
 * Undoes settles through the server's native restore, bound to the exact
 * settle the caller observed. A restore whose effect is unknown keeps its
 * command ID, so a later attempt for the same settle replays rather than
 * issuing a second, independent restore.
 */
export function createSettlementRestoreController(ports: SettlementRestorePorts) {
  const pending = new Map<string, CommandId>();
  const inFlight = new Map<string, Promise<SettlementRestoreOutcome>>();
  const keyOf = (target: SettlementRestoreTarget, settleCommandId: CommandId) =>
    JSON.stringify([target.environmentId, target.threadId, settleCommandId]);

  const reconcile = async (
    target: SettlementRestoreTarget,
    settleCommandId: CommandId,
    restoreCommandId: CommandId,
  ): Promise<SettlementRestoreOutcome> => {
    const inspected = await ports.inspect(target.environmentId, {
      threadId: target.threadId,
      settleCommandId,
    });
    if (inspected._tag === "Failure") return { type: "unknown", restoreCommandId };
    const inspection = inspected.value;
    if (
      inspection.record?.settleCommandId === settleCommandId &&
      inspection.record.restore?.commandId === restoreCommandId
    )
      return { type: "restored", restoreCommandId };
    const code = inspectionRejection(inspection);
    return code === null ? { type: "unknown", restoreCommandId } : { type: "rejected", code };
  };

  const attempt = async (
    target: SettlementRestoreTarget,
    settleCommandId: CommandId,
    key: string,
  ): Promise<SettlementRestoreOutcome> => {
    const restoreCommandId = pending.get(key) ?? ports.newCommandId();
    pending.set(key, restoreCommandId);
    const result = await ports.dispatch(target.environmentId, {
      commandId: restoreCommandId,
      threadId: target.threadId,
      settleCommandId,
    });
    let outcome: SettlementRestoreOutcome;
    if (result._tag === "Success") {
      outcome = { type: "restored", restoreCommandId };
    } else {
      const failure = Cause.squash(result.cause);
      const code = readSettlementRestoreRejection(failure);
      if (code !== null) outcome = { type: "rejected", code };
      else if (
        typeof failure === "object" &&
        failure !== null &&
        "_tag" in failure &&
        failure._tag === "SettlementRestoreUnsupportedError"
      )
        outcome = { type: "unsupported" };
      else {
        outcome = await reconcile(target, settleCommandId, restoreCommandId);
        // That ID can never succeed now; the next attempt needs a fresh one.
        if (outcome.type === "unknown" && isPreviouslyRejectedCommand(failure)) pending.delete(key);
      }
    }
    if (outcome.type !== "unknown") pending.delete(key);
    return outcome;
  };

  /** Undo the settle identified by `settleCommandId`, and only that settle. */
  const restore = (
    target: SettlementRestoreTarget,
    settleCommandId: CommandId,
  ): Promise<SettlementRestoreOutcome> => {
    const key = keyOf(target, settleCommandId);
    const running = inFlight.get(key);
    if (running !== undefined) return running;
    const started = attempt(target, settleCommandId, key).finally(() => inFlight.delete(key));
    inFlight.set(key, started);
    return started;
  };

  /** Undo the thread's current settle, as the server's record names it. */
  const restoreCurrent = async (
    target: SettlementRestoreTarget,
  ): Promise<SettlementRestoreOutcome> => {
    const inspected = await ports.inspect(target.environmentId, { threadId: target.threadId });
    if (inspected._tag === "Failure") return { type: "unavailable" };
    const { record, status } = inspected.value;
    if (status === "restorable" && record !== null) return restore(target, record.settleCommandId);
    if (status === "restored" && record?.restore != null) {
      // An earlier attempt from this client whose response was lost.
      const key = keyOf(target, record.settleCommandId);
      if (pending.get(key) === record.restore.commandId) {
        pending.delete(key);
        return { type: "restored", restoreCommandId: record.restore.commandId };
      }
    }
    return { type: "rejected", code: inspectionRejection(inspected.value) ?? "superseded" };
  };

  return { restore, restoreCurrent };
}

export type SettlementRestoreController = ReturnType<typeof createSettlementRestoreController>;

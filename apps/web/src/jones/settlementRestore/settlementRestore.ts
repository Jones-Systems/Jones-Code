import { scopedThreadKey } from "@t3tools/client-runtime/environment";
import {
  createSettlementRestoreController,
  dispatchSettlementRestore,
  inspectEnvironmentSettlementRestore,
  settlementRestoreNotApplied,
  supportsSettlementRestore,
  type SettlementRestoreOutcome,
  type SettlementRestorePorts,
} from "@t3tools/client-runtime/jones/settlement-restore";
import {
  type AtomCommandResult,
  createEnvironmentCommand,
  runAtomCommand,
} from "@t3tools/client-runtime/state/runtime";
import {
  AuthOrchestrationOperateScope,
  CommandId,
  EnvironmentAuthorizationError,
  type EnvironmentId,
  type ScopedThreadRef,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import { AsyncResult } from "effect/reactivity";

import * as ThreadUndo from "../../hooks/threadUndo";
import { randomUUID } from "../../lib/utils";
import { appAtomRegistry } from "../../rpc/atomRegistry";
import { environmentServerConfigsAtom } from "../../state/server";
import { readEnvironmentScope } from "../../state/session";
import { ORCHESTRATION_OPERATE_DENIED_MESSAGE } from "../../state/use-orchestration-command";

export function readEnvironmentSupportsSettlementRestore(environmentId: EnvironmentId): boolean {
  return supportsSettlementRestore(
    appAtomRegistry.get(environmentServerConfigsAtom).get(environmentId)?.environment.capabilities,
  );
}

/** A settle command ID for the notice to bind its Undo to. */
export const newSettleCommandId = () => CommandId.make(randomUUID());

// Defer the platform runtime import until a restore actually runs.
const makeCommands = (
  runtime: (typeof import("../../connection/runtime"))["connectionAtomRuntime"],
) => ({
  dispatch: createEnvironmentCommand(runtime, {
    label: "web-settlement-restore:restore",
    execute: (command: Parameters<SettlementRestorePorts["dispatch"]>[1]) =>
      dispatchSettlementRestore(command),
  }),
  inspect: createEnvironmentCommand(runtime, {
    label: "web-settlement-restore:inspect",
    execute: (query: Parameters<SettlementRestorePorts["inspect"]>[1], _registry, environmentId) =>
      inspectEnvironmentSettlementRestore(environmentId, query),
  }),
});
let commands: Promise<ReturnType<typeof makeCommands>> | undefined;
const loadCommands = () =>
  (commands ??= import("../../connection/runtime").then(({ connectionAtomRuntime }) =>
    makeCommands(connectionAtomRuntime),
  ));
const quiet = { reportFailure: false, reportDefect: false };

/** The environment calls behind restore; tests replace these, not the decisions above them. */
export const settlementRestoreTransport: Omit<SettlementRestorePorts, "newCommandId"> = {
  dispatch: async (environmentId, input) =>
    runAtomCommand(
      appAtomRegistry,
      (await loadCommands()).dispatch,
      { environmentId, input },
      quiet,
    ),
  inspect: async (environmentId, input) =>
    runAtomCommand(
      appAtomRegistry,
      (await loadCommands()).inspect,
      { environmentId, input },
      quiet,
    ),
};

// One controller per page, so the menu and the notice share pending restore IDs.
const controller = createSettlementRestoreController({
  newCommandId: () => CommandId.make(randomUUID()),
  dispatch: (environmentId, input) => settlementRestoreTransport.dispatch(environmentId, input),
  inspect: (environmentId, input) => settlementRestoreTransport.inspect(environmentId, input),
});

function toResult(outcome: SettlementRestoreOutcome): AtomCommandResult<void, unknown> {
  return outcome.type === "restored"
    ? AsyncResult.success(undefined)
    : AsyncResult.failure(Cause.fail(settlementRestoreNotApplied(outcome)));
}

function operateFailure(target: ScopedThreadRef): AtomCommandResult<void, unknown> | null {
  return readEnvironmentScope(target.environmentId, AuthOrchestrationOperateScope)
    ? null
    : AsyncResult.failure<void, unknown>(
        Cause.fail(
          new EnvironmentAuthorizationError({
            requiredScope: AuthOrchestrationOperateScope,
            message: ORCHESTRATION_OPERATE_DENIED_MESSAGE,
          }),
        ),
      );
}

/**
 * Undo for the settle notice: restores exactly the settle this client sent.
 * A settle of an already-settled thread changes nothing on the server, so
 * there is nothing for its Undo to reverse.
 */
export async function restoreObservedSettlement(
  target: ScopedThreadRef,
  settleCommandId: CommandId,
  options: { readonly settleWasNoop: boolean },
): Promise<AtomCommandResult<void, unknown>> {
  const denied = operateFailure(target);
  if (denied) return denied;
  const outcome = await controller.restore(target, settleCommandId);
  if (
    options.settleWasNoop &&
    outcome.type === "rejected" &&
    (outcome.code === "none" || outcome.code === "stale_settle_command")
  )
    return AsyncResult.success(undefined);
  return toResult(outcome);
}

/** The thread menu's Undo settle, which works after the notice is gone or the page reloaded. */
export async function restoreCurrentSettlement(
  target: ScopedThreadRef,
): Promise<AtomCommandResult<void, unknown>> {
  const denied = operateFailure(target);
  if (denied) return denied;
  // A still-visible notice for this thread would name the same settle; this undo replaces it.
  ThreadUndo.invalidate("settle", scopedThreadKey(target));
  return toResult(await controller.restoreCurrent(target));
}

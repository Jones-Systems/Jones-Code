import {
  dispatchSettlementRestore,
  inspectEnvironmentSettlementRestore,
  supportsSettlementRestore,
} from "@t3tools/client-runtime/jones/settlement-restore";
import { createEnvironmentCommand, runAtomCommand } from "@t3tools/client-runtime/state/runtime";
import * as ThreadUndo from "@t3tools/client-runtime/state/thread-undo";
import {
  AuthOrchestrationOperateScope,
  CommandId,
  type EnvironmentId,
  type ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Dialog from "../../components/ui/Dialog";
import { scopedThreadKey } from "../../lib/scopedEntities";
import { uuidv4 } from "../../lib/uuid";
import { appAtomRegistry } from "../../state/atom-registry";
import { environmentServerConfigsAtom } from "../../state/server";
import { readEnvironmentScope } from "../../state/session";
import { createMobileSettlementRestoreAction } from "./action";

const capabilities = (environmentId: EnvironmentId) =>
  appAtomRegistry.get(environmentServerConfigsAtom).get(environmentId)?.environment.capabilities;
const canOperate = (environmentId: EnvironmentId) =>
  readEnvironmentScope(environmentId, AuthOrchestrationOperateScope);
const guard = (environmentId: EnvironmentId) =>
  Effect.suspend(() =>
    canOperate(environmentId) && supportsSettlementRestore(capabilities(environmentId))
      ? Effect.void
      : Effect.fail(new Error("Settlement restore is no longer available on this connection.")),
  );

const makeCommands = (
  runtime: (typeof import("../../connection/runtime"))["connectionAtomRuntime"],
) => ({
  inspect: createEnvironmentCommand(runtime, {
    label: "mobile:settlement-restore:inspect",
    execute: (
      query: Parameters<typeof inspectEnvironmentSettlementRestore>[1],
      _registry,
      environmentId,
    ) =>
      guard(environmentId).pipe(
        Effect.andThen(inspectEnvironmentSettlementRestore(environmentId, query)),
      ),
  }),
  dispatch: createEnvironmentCommand(runtime, {
    label: "mobile:settlement-restore:dispatch",
    execute: (command: Parameters<typeof dispatchSettlementRestore>[0], _registry, environmentId) =>
      guard(environmentId).pipe(Effect.andThen(dispatchSettlementRestore(command))),
  }),
});
let commands: Promise<ReturnType<typeof makeCommands>> | undefined;
const getCommands = () =>
  (commands ??= import("../../connection/runtime").then(({ connectionAtomRuntime }) =>
    makeCommands(connectionAtomRuntime),
  ));
const options = { reportFailure: false, reportDefect: false };

// App lifetime, rather than screen lifetime, preserves unknown command IDs across navigation.
const restore = createMobileSettlementRestoreAction({
  capabilities,
  canOperate,
  newCommandId: () => CommandId.make(uuidv4()),
  invalidateTransientUndo: ({ environmentId, threadId }) =>
    ThreadUndo.invalidate("settle", scopedThreadKey(environmentId, threadId)),
  alert: Dialog.alert,
  inspect: async (environmentId, input) =>
    runAtomCommand(
      appAtomRegistry,
      (await getCommands()).inspect,
      { environmentId, input },
      options,
    ),
  dispatch: async (environmentId, input) =>
    runAtomCommand(
      appAtomRegistry,
      (await getCommands()).dispatch,
      { environmentId, input },
      options,
    ),
});

export function undoSettleThread(thread: {
  readonly environmentId: EnvironmentId;
  readonly id: ThreadId;
}) {
  return restore({ environmentId: thread.environmentId, threadId: thread.id });
}

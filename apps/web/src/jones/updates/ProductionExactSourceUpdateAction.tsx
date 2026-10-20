import { useAtomValue } from "@effect/atom-react";
import {
  AuthOrchestrationOperateScope,
  sessionGrantsScope,
  type AuthSessionState,
} from "@t3tools/contracts";
import type { AsyncResult } from "effect/reactivity";
import { requestConfirmDialog } from "../../confirmDialog";
import { appAtomRegistry } from "../../rpc/atomRegistry";
import { environmentSession } from "../../state/session";
import { useAtomCommand } from "../../state/use-atom-command";
import {
  ExactSourceUpdateAction,
  type ExactSourceHost,
  type ExactSourceTarget,
} from "./ExactSourceUpdateAction";
import { jonesUpdates } from "./jonesUpdates";

function canOperate(session: AsyncResult.AsyncResult<AuthSessionState, unknown>) {
  return (
    session._tag === "Success" &&
    session.value.authenticated &&
    sessionGrantsScope(session.value, AuthOrchestrationOperateScope)
  );
}

/** Binds the exact-source action to the host's real observation, session and update command. */
export function ProductionExactSourceUpdateAction(target: ExactSourceTarget) {
  const { environmentId } = target;
  const sessionAtom = environmentSession.sessionStateAtom(environmentId);
  const observation = useAtomValue(jonesUpdates.observation(environmentId));
  const operate = canOperate(useAtomValue(sessionAtom));
  const run = useAtomCommand(jonesUpdates.action, { reportFailure: false });
  const host: ExactSourceHost = {
    snapshot: { observation, canOperate: operate },
    readLatest: () => ({
      observation: appAtomRegistry.get(jonesUpdates.observation(environmentId)),
      canOperate: canOperate(appAtomRegistry.get(sessionAtom)),
    }),
    run: (input) => run({ environmentId, input }),
    // Without a dialog host the restart is not confirmed, so it is not sent.
    confirm: async (message) => (await requestConfirmDialog(message)) ?? false,
  };
  return <ExactSourceUpdateAction {...target} host={host} />;
}

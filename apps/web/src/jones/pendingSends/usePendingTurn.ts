import type { StartThreadTurnInput } from "@t3tools/client-runtime/operations";
import type { EnvironmentId } from "@t3tools/contracts";
import { threadEnvironment } from "../../state/threads";
import { useOrchestrationCommand } from "../../state/use-orchestration-command";
import { sendPendingTurn } from "./send";

export function usePendingTurn() {
  const prepare = useOrchestrationCommand(threadEnvironment.prepareTurn, { reportFailure: false });
  const dispatch = useOrchestrationCommand(threadEnvironment.dispatchPreparedTurn, {
    reportFailure: false,
  });
  return ({
    environmentId,
    input,
  }: {
    environmentId: EnvironmentId;
    input: StartThreadTurnInput;
  }) =>
    sendPendingTurn({
      environmentId,
      input,
      prepare: () => prepare({ environmentId, input }),
      dispatch: (prepared) => dispatch({ environmentId, input: prepared }),
    });
}

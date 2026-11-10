import type {
  ModelSelection,
  OrchestrationV2Command,
  OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import { modelSelectionsEqual } from "@t3tools/shared/model";

type MessageDispatch = Extract<OrchestrationV2Command, { type: "message.dispatch" }>;

/** Identify ordinary follow-ups that can wait for the active provider to become steerable.
 * The caller must check the durable Stop barrier before changing delivery mode.
 */
export function transientSteerTarget(
  projection: OrchestrationV2ThreadProjection,
  command: MessageDispatch,
  dispatchMode: MessageDispatch["dispatchMode"],
  modelSelection: ModelSelection,
  separateTurnCommand: boolean,
) {
  if (
    command.deliveryIntent !== "steer" ||
    command.createdBy !== "user" ||
    (command.creationSource !== "web" && command.creationSource !== "mobile") ||
    dispatchMode.type !== "steer_active" ||
    separateTurnCommand
  )
    return undefined;
  const run = projection.runs.find((candidate) => candidate.id === dispatchMode.targetRunId);
  // Keep explicit configuration transitions on their existing validation path.
  if (run === undefined || !modelSelectionsEqual(run.modelSelection, modelSelection))
    return undefined;
  return run.status === "preparing" ||
    run.status === "starting" ||
    run.status === "waiting" ||
    (run.status === "running" &&
      !projection.providerTurns.some(
        (turn) => turn.runAttemptId === run.activeAttemptId && turn.status === "running",
      ))
    ? run
    : undefined;
}

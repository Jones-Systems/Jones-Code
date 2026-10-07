import {
  ModelSelection,
  NonNegativeInt,
  OrchestrationDispatchTarget,
  type OrchestrationV2Command,
  type OrchestrationV2ServerCommand,
  type ThreadTurnDispatchGuard,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { makeCommandObservationQuery } from "./CommandObservation.ts";

export class DispatchGuardRejected extends Schema.TaggedError<DispatchGuardRejected>()(
  "DispatchGuardRejected",
  {
    reason: Schema.String,
    observed: Schema.optional(
      Schema.Struct({
        snapshotSequence: NonNegativeInt,
        lastEventSequence: NonNegativeInt,
        target: Schema.NullOr(OrchestrationDispatchTarget),
      }),
    ),
  },
) {}
const sameModelSelection = Schema.toEquivalence(ModelSelection);
export const makeDispatchGuard = Effect.fn("makeDispatchGuard")(function* () {
  const query = yield* makeCommandObservationQuery();
  return Effect.fn("DispatchGuard.validate")(function* (
    command:
      | Exclude<OrchestrationV2Command, { type: "prepared-run.release" }>
      | (Extract<OrchestrationV2ServerCommand, { type: "prepared-run.release" }> & {
          readonly dispatchGuard?: ThreadTurnDispatchGuard;
          readonly modelSelection?: ModelSelection;
        }),
  ) {
    if (
      (command.type !== "message.dispatch" && command.type !== "prepared-run.release") ||
      command.dispatchGuard === undefined
    )
      return;
    const rejectMode = (reason: string) => new DispatchGuardRejected({ reason });
    if (command.type === "message.dispatch" && command.dispatchMode.type !== "start_immediately")
      return yield* rejectMode("guarded dispatch requires an immediate idle start");
    const guard = command.dispatchGuard;
    const current = yield* query.getTarget(command.threadId);
    const target = current.target;
    const reject = (reason: string) => new DispatchGuardRejected({ reason, observed: current });
    if (target === null) return yield* reject("target is missing");
    if (
      guard.observedSnapshotSequence > current.snapshotSequence ||
      current.lastEventSequence > guard.observedSnapshotSequence
    )
      return yield* reject("target changed after observation");
    if (
      !sameModelSelection(target.modelSelection, guard.expectedModelSelection) ||
      (command.modelSelection !== undefined &&
        command.modelSelection.instanceId !== guard.expectedModelSelection.instanceId) ||
      target.sessionStatus !== guard.expectedSessionStatus ||
      target.activeTurnId !== guard.expectedActiveTurnId ||
      target.latestTurnId !== guard.expectedLatestTurnId
    )
      return yield* reject("target binding changed");
    if (!target.idle) return yield* reject(target.blockers.join(", "));
  });
});

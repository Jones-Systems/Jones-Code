import { ModelSelection, type OrchestrationCommand } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { OrchestrationCommandInvariantError } from "./Errors.ts";
import { makeCommandObservationQuery } from "./CommandObservation.ts";

const sameModelSelection = Schema.toEquivalence(ModelSelection);

export const makeDispatchGuard = Effect.fn("makeDispatchGuard")(function* () {
  const query = yield* makeCommandObservationQuery();
  return Effect.fn("DispatchGuard.validate")(function* (command: OrchestrationCommand) {
    if (command.type !== "thread.turn.start" || command.dispatchGuard === undefined) return;
    const reject = (reason: string) =>
      new OrchestrationCommandInvariantError({
        commandType: command.type,
        detail: `dispatch_guard_rejected: ${reason}`,
      });
    if (command.bootstrap !== undefined) return yield* reject("bootstrap is unsupported");
    const guard = command.dispatchGuard;
    const current = yield* query.getTarget(command.threadId);
    const target = current.target;
    if (target === null) return yield* reject("target is missing");
    if (
      guard.observedSnapshotSequence > current.snapshotSequence ||
      current.lastEventSequence > guard.observedSnapshotSequence
    ) {
      return yield* reject("target changed after observation");
    }
    if (
      !sameModelSelection(target.modelSelection, guard.expectedModelSelection) ||
      (command.modelSelection !== undefined &&
        command.modelSelection.instanceId !== guard.expectedModelSelection.instanceId) ||
      target.sessionStatus !== guard.expectedSessionStatus ||
      target.activeTurnId !== guard.expectedActiveTurnId ||
      target.latestTurnId !== guard.expectedLatestTurnId
    ) {
      return yield* reject("target binding changed");
    }
    if (!target.idle) return yield* reject(target.blockers.join(", "));
  });
});

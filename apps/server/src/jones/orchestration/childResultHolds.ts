import type { NodeId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import type { RecordedRun } from "../../orchestration-v2/RecordedTypes.ts";

export interface ChildResultHoldPorts<DutyError, FailoverError> {
  /** Result dependencies this thread still owes its waiters. */
  readonly duties: (threadId: ThreadId) => Effect.Effect<{ readonly outgoing: number }, DutyError>;
  /** A failed Codex run whose failover has not decided, so a retry may still answer. */
  readonly holdsChildResult: (
    threadId: ThreadId,
    run: RecordedRun | null | undefined,
    taskId?: NodeId,
  ) => Effect.Effect<boolean, FailoverError>;
}

/**
 * The Jones reasons a delegated child's result must not publish yet. Upstream decides
 * when a result is delivered and wakes the parent; the Orchestrator asks here at each
 * point where it decides a child has a result, so those functions stay near upstream.
 */
export const make = <DutyError, FailoverError>(
  ports: ChildResultHoldPorts<DutyError, FailoverError>,
) => ({
  owesDependencies: (childThreadId: ThreadId) =>
    ports.duties(childThreadId).pipe(Effect.map(({ outgoing }) => outgoing > 0)),
  holdsFailover: ports.holdsChildResult,
});

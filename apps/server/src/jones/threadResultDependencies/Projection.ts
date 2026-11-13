import type {
  OrchestrationV2PendingBackgroundTask,
  ThreadResultDependency,
} from "@t3tools/contracts";

import type { WakeHoldState } from "./WakeObligations.ts";

const reasons = {
  admission_held: "Awaiting receiving owner approval",
  admission_dispatching: "Receiving environment is admitting the request",
  queued: "Request queued",
  queued_held: "Receiver queue is held",
  receiver_working: "Awaiting explicit answer",
  receiver_needs_owner: "Receiver needs owner input",
  receiver_idle_unanswered: "Receiver ended its turn without answering",
  receiver_stopped: "Receiver stopped without answering",
  unreachable: "Environment unreachable; observation continues",
  delivery_unknown: "Delivery unknown; checking",
};
export function roster(
  dependencies: ReadonlyArray<ThreadResultDependency & { readonly wakeHold?: WakeHoldState }>,
): ReadonlyArray<OrchestrationV2PendingBackgroundTask> {
  return dependencies
    .filter((dependency) => ["sending", "awaiting", "resolved"].includes(dependency.state))
    .map((dependency) => ({
      taskId: `jones-dependency:${dependency.dependencyId}`,
      kind: "monitor" as const,
      awaited: true,
      description:
        `${dependency.state === "resolved" ? (dependency.wakeHold === "held" || dependency.wakeHold === "armed" ? "Result held: delivery refused before provider start; retries after the next accepted start, or release" : "Result ready; delivering") : reasons[dependency.observation]}: ${dependency.targetLabel}`.slice(
          0,
          160,
        ),
    }));
}

export function byThread(
  dependencies: ReadonlyArray<
    ThreadResultDependency & {
      readonly waiterThreadId: import("@t3tools/contracts").ThreadId;
      readonly wakeHold?: WakeHoldState;
    }
  >,
) {
  const grouped = new Map<
    import("@t3tools/contracts").ThreadId,
    (ThreadResultDependency & { readonly wakeHold?: WakeHoldState })[]
  >();
  for (const dependency of dependencies) {
    const group = grouped.get(dependency.waiterThreadId) ?? [];
    group.push(dependency);
    grouped.set(dependency.waiterThreadId, group);
  }
  return new Map([...grouped].map(([id, entries]) => [id, roster(entries)]));
}

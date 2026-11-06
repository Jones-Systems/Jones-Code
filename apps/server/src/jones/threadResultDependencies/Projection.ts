import type {
  OrchestrationV2PendingBackgroundTask,
  ThreadResultDependency,
} from "@t3tools/contracts";

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
  dependencies: ReadonlyArray<ThreadResultDependency>,
): ReadonlyArray<OrchestrationV2PendingBackgroundTask> {
  return dependencies
    .filter((dependency) => ["sending", "awaiting", "resolved"].includes(dependency.state))
    .map((dependency) => ({
      taskId: `jones-dependency:${dependency.dependencyId}`,
      kind: "monitor" as const,
      awaited: true,
      description:
        `${dependency.state === "resolved" ? "Result ready; delivering" : reasons[dependency.observation]}: ${dependency.targetLabel}`.slice(
          0,
          160,
        ),
    }));
}

export function byThread(
  dependencies: ReadonlyArray<
    ThreadResultDependency & { readonly waiterThreadId: import("@t3tools/contracts").ThreadId }
  >,
) {
  const grouped = new Map<import("@t3tools/contracts").ThreadId, ThreadResultDependency[]>();
  for (const dependency of dependencies) {
    const group = grouped.get(dependency.waiterThreadId) ?? [];
    group.push(dependency);
    grouped.set(dependency.waiterThreadId, group);
  }
  return new Map([...grouped].map(([id, entries]) => [id, roster(entries)]));
}

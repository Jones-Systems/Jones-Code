import type { ThreadRegistryComposedThread, ThreadRegistryWorkstreams } from "@t3tools/contracts";
import { destinationAssignment, assignmentSummary } from "./promptPresentation";
import { workstreamFilterKey } from "./promptSearch";
import type { PromptItem } from "./promptItems";

export interface PromptWorkstreamLine {
  readonly key: string;
  readonly filterKey: string;
  readonly id: string;
  readonly name: string | null;
  readonly color: string | null;
  readonly provenance: "from_thread" | "suggested" | "requested_by_queue" | "queue_id";
  readonly summary: string | null;
  readonly threadId: string | null;
  readonly assignedAutomatically: boolean;
}

interface WorkstreamLabel {
  readonly name: string;
  readonly color: string | null;
}

interface PromptWorkstreamSources {
  readonly environmentId: string;
  readonly queueBound: boolean;
  readonly names: ReadonlyMap<string, WorkstreamLabel>;
  readonly labels?: ThreadRegistryWorkstreams | null;
  readonly registryThread?: ThreadRegistryComposedThread | null;
  readonly threadAssignments?: ReadonlyMap<string, { readonly workstreamId: string | null }>;
  readonly autoAssignedThreadKeys?: ReadonlySet<string> | undefined;
}

export function promptWorkstreams(item: PromptItem, sources: PromptWorkstreamSources) {
  const lines = new Map<string, PromptWorkstreamLine>();
  const add = (
    provenance: PromptWorkstreamLine["provenance"],
    id: string,
    label: WorkstreamLabel | undefined,
    threadId: string | null = null,
    summary: string | null = null,
    evidenceKey: string | null = null,
  ) => {
    const threadKey = JSON.stringify([sources.environmentId, threadId]);
    const filterKey = workstreamFilterKey(sources.environmentId, id);
    const key = JSON.stringify([provenance, filterKey, threadId, evidenceKey]);
    lines.set(key, {
      key,
      filterKey,
      id,
      name: label?.name ?? null,
      color: label?.color ?? null,
      provenance,
      summary,
      threadId,
      assignedAutomatically:
        provenance === "from_thread" &&
        threadId !== null &&
        (sources.autoAssignedThreadKeys?.has(threadKey) ?? false),
    });
  };
  const queueName = (id: string) => (sources.queueBound ? sources.names.get(id) : undefined);
  const refs = new Set(item.entry?.workstream_refs ?? []);
  for (const association of item.entry?.associations ?? []) {
    if (association.state === "active") refs.add(association.workstream_ref);
    else refs.delete(association.workstream_ref);
  }
  for (const ref of refs) {
    if (ref.startsWith("native:")) {
      if (
        !sources.registryThread?.personal_memberships?.some(
          (member) => member.workstream_ref === ref,
        )
      )
        add("from_thread", ref, undefined);
      continue;
    }
    if (!ref.startsWith("inferred:")) continue;
    const name = sources.labels?.workstreams.find((label) => label.label_id === ref)?.name;
    add("suggested", ref, name ? { name, color: null } : undefined);
  }
  for (const member of sources.registryThread?.personal_memberships ?? []) {
    if (member.environment_id !== sources.environmentId) continue;
    const threadKey = JSON.stringify([sources.environmentId, member.thread_id]);
    const current = sources.threadAssignments?.get(threadKey);
    if (current && current.workstreamId !== member.workstream_id) continue;
    add(
      "from_thread",
      member.workstream_id,
      sources.names.get(member.workstream_id) ?? { name: member.name, color: null },
      member.thread_id,
    );
  }
  for (const member of sources.registryThread?.native_memberships ?? []) {
    add("from_thread", member.workstream_ref, undefined);
  }
  for (const destination of item.destinations) {
    const threadId = destination.threadId;
    if (
      threadId &&
      (destination.targetEnvironmentRef === null ||
        destination.targetEnvironmentRef === sources.environmentId)
    ) {
      const threadKey = JSON.stringify([sources.environmentId, threadId]);
      const id = sources.threadAssignments?.get(threadKey)?.workstreamId;
      if (id) add("from_thread", id, sources.names.get(id), threadId);
    }
    if (!destination.queueItem) continue;
    const id = destination.queueItem.workstream_id;
    add("queue_id", id, queueName(id));
    const assignment = destinationAssignment(destination);
    if (assignment) {
      add(
        "requested_by_queue",
        assignment.workstream_id,
        queueName(assignment.workstream_id),
        null,
        assignmentSummary(assignment, (id) => queueName(id)?.name ?? id),
        destination.key,
      );
    }
  }
  return [...lines.values()];
}

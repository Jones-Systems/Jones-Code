import {
  personalThreadKey,
  type PersonalThreadRef,
  type PersonalWorkstreamsState,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import { ThreadId, type EnvironmentId } from "@t3tools/contracts";
import type { PersonalWorkstreamAssignment } from "@t3tools/contracts/jones/workstreamsPersonal";
import type { PromptDestination } from "./promptItems";
import { workstreamFilterKey } from "./promptSearch";
import type { PromptWorkstreamLine } from "./promptWorkstreams";
import { displayText } from "./PromptRow";

export interface SentReceiptSource {
  readonly environmentId: EnvironmentId;
  readonly hostId: string;
  readonly expiresAt: number;
}

export type SentThreadWorkstream =
  | { readonly kind: "unknown" }
  | {
      readonly kind: "unassigned";
      readonly thread: PersonalThreadRef;
      readonly revision: number;
    }
  | { readonly kind: "assigned"; readonly line: PromptWorkstreamLine };

export function sentWorkstreamLines(memberships: readonly SentThreadWorkstream[]) {
  const lines = new Map<string, PromptWorkstreamLine>();
  for (const membership of memberships) {
    if (membership.kind === "assigned") lines.set(membership.line.key, membership.line);
  }
  return [...lines.values()];
}

export function sentWorkstreamGroup(memberships: readonly SentThreadWorkstream[]) {
  if (memberships.some((membership) => membership.kind === "unassigned"))
    return { key: "unassigned", label: "Unassigned" };
  const lines = sentWorkstreamLines(memberships);
  const first = lines[0];
  if (!first || memberships.some((membership) => membership.kind === "unknown"))
    return { key: "unknown", label: "Assignment unavailable", unrecognized: true };
  if (lines.some((line) => line.filterKey !== first.filterKey))
    return { key: "multiple", label: "Multiple Workstreams" };
  return { key: first.filterKey, label: displayText(first.name, first.id) };
}

export function sentThreadWorkstream(
  destination: PromptDestination,
  source: SentReceiptSource | null,
  state: PersonalWorkstreamsState,
  live: ReadonlyMap<ThreadId, PersonalWorkstreamAssignment> | null,
  now: number,
): SentThreadWorkstream {
  const receipt = destination.handoff;
  if (
    !source ||
    source.expiresAt <= now ||
    !live ||
    !receipt ||
    destination.targetHostId !== source.hostId ||
    destination.targetEnvironmentRef !== source.environmentId ||
    receipt.target.threadId !== destination.threadId
  )
    return { kind: "unknown" };

  const { environmentId } = source;
  const environment = state.environments.find((entry) => entry.environmentId === environmentId);
  if (
    !environment?.confirmed ||
    (environment.status !== "ready" && environment.status !== "read_only")
  )
    return { kind: "unknown" };

  const thread = { environmentId, id: ThreadId.make(receipt.target.threadId) };
  const key = personalThreadKey(thread);
  const observed = live.get(thread.id);
  const projected = state.pendingThreadKeys.has(key)
    ? undefined
    : state.assignmentsByThreadKey.get(key);
  // A newer canonical RPC result can precede the next subscription snapshot. A projected
  // null can also mask an assignment whose definition is missing, so only the stream proves null.
  const newer = projected && projected.revision > (observed?.revision ?? 0);
  const assignment = newer ? projected : observed;
  if (newer && assignment?.workstreamId === null) return { kind: "unknown" };
  if (!assignment?.workstreamId)
    return { kind: "unassigned", thread, revision: assignment?.revision ?? 0 };

  const id = assignment.workstreamId;
  const definition = environment.workstreams?.find((workstream) => workstream.id === id);
  const filterKey = workstreamFilterKey(environmentId, id);
  return {
    kind: "assigned",
    line: {
      key: JSON.stringify([filterKey, thread.id]),
      filterKey,
      id,
      name: definition?.name ?? null,
      color: definition?.color ?? null,
      provenance: "from_thread",
      summary: null,
      threadId: thread.id,
      assignedAutomatically: state.autoAssignedThreadKeys?.has(key) ?? false,
    },
  };
}

export function sentAssignmentOptions(
  state: PersonalWorkstreamsState,
  environmentId: EnvironmentId,
) {
  const environment = state.environments.find((entry) => entry.environmentId === environmentId);
  const local = new Map(environment?.workstreams?.map((workstream) => [workstream.id, workstream]));
  // Use the native order and choose only definitions available on the receiving environment.
  return state.workstreams.flatMap((merged) => {
    const option = local.get(merged.id);
    return option &&
      option.lifecycle !== "completed" &&
      option.lifecycle !== "abandoned" &&
      merged.lifecycle !== "completed" &&
      merged.lifecycle !== "abandoned"
      ? [option]
      : [];
  });
}

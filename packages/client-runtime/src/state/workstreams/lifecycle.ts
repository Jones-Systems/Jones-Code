import type { Workstream, WorkstreamLifecycle } from "@t3tools/contracts";
import { WorkstreamAuditEvent, WorkstreamLifecycleDeclaration } from "@t3tools/contracts";

type LifecycleDeclaration = typeof WorkstreamLifecycleDeclaration.Type;
type AuditEvent = typeof WorkstreamAuditEvent.Type;

export type WorkstreamCompletionAuthority =
  | {
      readonly state: "not-complete";
      readonly lifecycle: Exclude<WorkstreamLifecycle, "completed">;
    }
  | {
      readonly state: "owner-declared";
      readonly lifecycle: "completed";
      readonly declaration: LifecycleDeclaration;
    }
  | {
      readonly state: "unverified";
      readonly lifecycle: "completed";
      readonly reason: "incomplete-history" | "missing-declaration" | "conflicting-declaration";
    };

function compareLifecycleDeclarations(
  left: LifecycleDeclaration,
  right: LifecycleDeclaration,
): number {
  return (
    left.revision - right.revision ||
    left.registry_version - right.registry_version ||
    left.lifecycle_declaration_id.localeCompare(right.lifecycle_declaration_id)
  );
}

/**
 * Confirms completion only from the owner registry's explicit lifecycle declaration.
 * Turn termination, membership disposition, delivery state, and native settlement
 * deliberately are not inputs.
 */
export function resolveWorkstreamCompletionAuthority(
  workstream: Pick<Workstream, "workstream_id" | "lifecycle">,
  history: {
    readonly coverage: "complete";
    readonly context: { readonly registry_version: number };
    readonly items: readonly Pick<
      AuditEvent,
      | "actor"
      | "changed"
      | "command_id"
      | "lifecycle_declaration"
      | "occurred_at"
      | "operation"
      | "registry_version"
      | "workstream_versions"
    >[];
    readonly next_cursor: null;
  },
): WorkstreamCompletionAuthority {
  if (workstream.lifecycle !== "completed") {
    return { state: "not-complete", lifecycle: workstream.lifecycle };
  }
  if (history.coverage !== "complete" || history.next_cursor !== null) {
    return { state: "unverified", lifecycle: "completed", reason: "incomplete-history" };
  }

  const declarations = history.items
    .flatMap((event) =>
      event.lifecycle_declaration ? [{ declaration: event.lifecycle_declaration, event }] : [],
    )
    .filter(({ declaration }) => declaration.workstream_id === workstream.workstream_id)
    .sort((left, right) => compareLifecycleDeclarations(left.declaration, right.declaration));
  const latestRevision = declarations.at(-1)?.declaration.revision;
  if (latestRevision === undefined) {
    return { state: "unverified", lifecycle: "completed", reason: "missing-declaration" };
  }
  const latest = declarations.filter(({ declaration }) => declaration.revision === latestRevision);
  const candidate = latest[0];
  if (
    latest.length !== 1 ||
    !candidate ||
    candidate.declaration.new_lifecycle !== "completed" ||
    candidate.event.changed !== true ||
    !(["create_workstream", "update_workstream"] as const).includes(
      candidate.event.operation as "create_workstream" | "update_workstream",
    ) ||
    candidate.event.command_id !== candidate.declaration.command_id ||
    candidate.event.actor.principal_id !== candidate.declaration.actor.principal_id ||
    candidate.event.occurred_at !== candidate.declaration.recorded_at ||
    candidate.event.registry_version !== candidate.declaration.registry_version ||
    candidate.declaration.registry_version > history.context.registry_version ||
    !candidate.event.workstream_versions.some(
      ({ workstream_id }) => workstream_id === workstream.workstream_id,
    )
  ) {
    return { state: "unverified", lifecycle: "completed", reason: "conflicting-declaration" };
  }
  return {
    state: "owner-declared",
    lifecycle: "completed",
    declaration: candidate.declaration,
  };
}

import type {
  MembershipEpisode,
  NativeReference,
  T3ThreadPlacement,
  Workstream,
  WorkstreamCommand,
} from "@t3tools/contracts";
import type { WorkstreamSetPrimaryRequest } from "@t3tools/contracts/jones/workstreamOperations";

export type PrimaryMembershipAction = Extract<
  WorkstreamCommand["action"],
  { operation: "attach_primary" | "reattach_primary" | "move_primary" | "remove_membership" }
>;

export interface SetPrimaryPlanInput {
  readonly request: WorkstreamSetPrimaryRequest;
  readonly registry: { readonly registry_id: string; readonly owner_id: string };
  readonly reference: Pick<
    NativeReference,
    "native_reference_id" | "owner_id" | "identity" | "registration"
  >;
  readonly placements: readonly T3ThreadPlacement[];
  readonly workstreams: readonly Pick<
    Workstream,
    "workstream_id" | "owner_id" | "name" | "version"
  >[];
  /** Complete, caller-qualified destination scan; required when planning an attach. */
  readonly destinationMemberships?: readonly MembershipEpisode[];
}

export type SetPrimaryPlan =
  | { readonly kind: "already_satisfied" }
  | { readonly kind: "conflict"; readonly reason: "precondition_failed" }
  | { readonly kind: "action"; readonly action: PrimaryMembershipAction };

/**
 * Consumes decoded, qualified observations from one coherent registry snapshot.
 * The caller owns authorization, freshness, trust, scan completeness and recovery.
 * These local decisions prove neither remote effects nor a final-result release fence.
 */
export function planSetPrimary(input: SetPrimaryPlanInput): SetPrimaryPlan {
  const { request, registry, reference } = input;
  if (
    request.target.registry_id !== registry.registry_id ||
    request.target.owner_id !== registry.owner_id ||
    reference.owner_id !== registry.owner_id
  )
    throw new Error("Inconsistent registry or owner.");

  const identity = reference.identity;
  const evidence = reference.registration.evidence;
  if (
    identity.provider !== "t3" ||
    identity.resource_kind !== "thread" ||
    identity.id_kind !== "internal" ||
    identity.source_instance_id !== request.thread.environment_id ||
    identity.native_id !== request.thread.thread_id ||
    reference.registration.state !== "attested" ||
    evidence === null ||
    evidence.provider !== identity.provider ||
    evidence.source_instance_id !== identity.source_instance_id ||
    evidence.native_id !== identity.native_id
  )
    throw new Error("Inconsistent thread reference.");

  const primaries = input.placements.filter((placement) => placement.kind === "primary");
  if (
    primaries.length > 1 ||
    input.placements.some(
      (placement) =>
        placement.source_instance_id !== request.thread.environment_id ||
        placement.native_thread_id !== request.thread.thread_id ||
        placement.native_reference_id !== reference.native_reference_id ||
        placement.attestation_version !== reference.registration.attestation_version ||
        placement.evidence_sha256 !== evidence.evidence_sha256 ||
        placement.authority_namespace !== evidence.authority_namespace ||
        placement.store_generation !== evidence.store_generation,
    )
  )
    throw new Error("Inconsistent thread placements.");

  const groups = new Map(input.workstreams.map((group) => [group.workstream_id, group]));
  if (
    groups.size !== input.workstreams.length ||
    input.workstreams.some(
      (group) =>
        group.owner_id !== registry.owner_id ||
        !Number.isSafeInteger(group.version) ||
        group.version < 1,
    )
  )
    throw new Error("Inconsistent workstream data.");
  const current = primaries[0];
  const source = current ? groups.get(current.workstream_id) : undefined;
  const destination =
    request.target.kind === "workstream" ? groups.get(request.target.workstream_id) : undefined;
  if (current && !source) throw new Error("Current workstream is unavailable.");
  if (request.target.kind === "workstream" && !destination)
    throw new Error("Destination workstream is unavailable.");

  if ((current?.workstream_id ?? null) === (destination?.workstream_id ?? null))
    return { kind: "already_satisfied" };
  if (
    (request.expected.kind === "unassigned" && current !== undefined) ||
    (request.expected.kind === "primary" &&
      current?.workstream_id !== request.expected.workstream_id)
  )
    return { kind: "conflict", reason: "precondition_failed" };

  if (source && current) {
    return {
      kind: "action",
      action: destination
        ? {
            operation: "move_primary",
            source_workstream_id: source.workstream_id,
            expected_source_version: source.version,
            source_membership_id: current.membership_id,
            destination_workstream_id: destination.workstream_id,
            expected_destination_version: destination.version,
          }
        : {
            operation: "remove_membership",
            workstream_id: source.workstream_id,
            expected_version: source.version,
            membership_id: current.membership_id,
          },
    };
  }
  if (!destination) throw new Error("Destination workstream is unavailable.");
  if (input.destinationMemberships === undefined)
    throw new Error("Destination membership scan is required.");
  const matching = input.destinationMemberships.filter(
    (episode) =>
      episode.kind === "primary" &&
      episode.workstream_id === destination.workstream_id &&
      episode.native_reference_id === reference.native_reference_id,
  );
  if (matching.some((episode) => episode.closed === null))
    throw new Error("Destination membership contradicts unassigned placement.");
  return {
    kind: "action",
    action: {
      operation: matching.length > 0 ? "reattach_primary" : "attach_primary",
      workstream_id: destination.workstream_id,
      expected_version: destination.version,
      native_reference_id: reference.native_reference_id,
    },
  };
}

import type {
  MembershipEpisode,
  NativeReference,
  T3WorkstreamListResult,
  WorkstreamCommand,
} from "@t3tools/contracts";
import {
  currentT3Placement,
  type LiveT3Placements,
} from "@t3tools/client-runtime/state/workstreams";
import {
  attestedNativeThreadKey,
  nativeWorkstreamThreadKey,
  type WorkstreamThreadLike,
} from "./nativeThreadGrouping";

export const canEditWorkstreams = (data: T3WorkstreamListResult | null): boolean =>
  data !== null &&
  data.source === "live" &&
  !data.stale &&
  data.nextCursor === null &&
  data.binding.permissions.includes("workstreams:write");

export { workstreamTint } from "@t3tools/client-runtime/state/workstreams";

export function planNativeMembership(input: {
  readonly data: T3WorkstreamListResult;
  readonly placements: LiveT3Placements;
  readonly references: readonly NativeReference[];
  readonly destinationMemberships?: readonly MembershipEpisode[];
  readonly thread: WorkstreamThreadLike;
  readonly destination: string | null;
  readonly now: number;
}): WorkstreamCommand["action"] | null {
  if (!canEditWorkstreams(input.data))
    throw new Error("Refresh Workstreams with write access before changing membership.");
  const context = input.placements.context;
  const binding = input.data.binding;
  if (
    context.owner_id !== binding.ownerId ||
    context.principal_id !== binding.principalId ||
    context.authorization_revision !== binding.authorizationRevision ||
    context.server_generation !== binding.serverGeneration ||
    context.registry_version !== binding.registryVersion ||
    input.placements.readiness !== "ready"
  )
    throw new Error("Thread placement binding changed. Refresh before changing membership.");
  const key = nativeWorkstreamThreadKey(input.thread.environmentId, input.thread.id);
  const trust = new Map(
    input.placements.trustedEnvironments.map((entry) => [entry.environmentId, entry]),
  );
  const references = input.references.filter(
    (reference) =>
      reference.owner_id === input.data.binding.ownerId &&
      attestedNativeThreadKey(reference, input.now, trust) === key,
  );
  if (references.length !== 1)
    throw new Error(
      "This thread needs one current, verified reference in the owner registry before it can be assigned.",
    );
  const reference = references[0]!;
  const placements = input.placements.items.filter(
    (entry) =>
      entry.source_instance_id === input.thread.environmentId &&
      entry.native_thread_id === input.thread.id &&
      entry.kind === "primary",
  );
  if (placements.some((entry) => !currentT3Placement(entry, input.now)) || placements.length > 1)
    throw new Error(
      "Thread placement is stale or conflicting. Refresh before changing membership.",
    );
  const current = placements[0];
  if (
    current &&
    (current.native_reference_id !== reference.native_reference_id ||
      current.attestation_version !== reference.registration.attestation_version ||
      current.evidence_sha256 !== reference.registration.evidence?.evidence_sha256 ||
      current.authority_namespace !== reference.registration.evidence?.authority_namespace ||
      current.store_generation !== reference.registration.evidence?.store_generation)
  )
    throw new Error("Thread reference changed. Refresh before changing membership.");
  if ((current?.workstream_id ?? null) === input.destination) return null;
  const source = input.data.items.find((item) => item.workstreamId === current?.workstream_id);
  const destination = input.data.items.find((item) => item.workstreamId === input.destination);
  if (current && !source)
    throw new Error("Current Workstream is unavailable. Refresh before moving the thread.");
  if (input.destination !== null && !destination)
    throw new Error("Destination Workstream is unavailable.");
  if (source && current) {
    if (destination)
      return {
        operation: "move_primary",
        source_workstream_id: source.workstreamId,
        expected_source_version: source.version,
        source_membership_id: current.membership_id,
        destination_workstream_id: destination.workstreamId,
        expected_destination_version: destination.version,
      };
    return {
      operation: "remove_membership",
      workstream_id: source.workstreamId,
      expected_version: source.version,
      membership_id: current.membership_id,
    };
  }
  if (!destination) return null;
  const reattaching = input.destinationMemberships?.some(
    (episode) =>
      episode.closed !== null &&
      episode.kind === "primary" &&
      episode.workstream_id === destination.workstreamId &&
      episode.native_reference_id === reference.native_reference_id,
  );
  return {
    operation: reattaching ? "reattach_primary" : "attach_primary",
    workstream_id: destination.workstreamId,
    expected_version: destination.version,
    native_reference_id: reference.native_reference_id,
  };
}

export function moveNativeThreadOrder(
  order: readonly string[],
  movedId: string,
  neighborId: string,
  after: boolean,
): readonly string[] {
  if (movedId === neighborId || !order.includes(movedId) || !order.includes(neighborId))
    return order;
  const next = order.filter((key) => key !== movedId);
  next.splice(next.indexOf(neighborId) + (after ? 1 : 0), 0, movedId);
  return next;
}

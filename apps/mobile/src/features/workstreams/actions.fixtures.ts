import type {
  MembershipEpisode,
  NativeReference,
  T3ThreadPlacement,
  T3WorkstreamListResult,
} from "@t3tools/contracts";
import {
  buildWorkstreamAssignmentIndex,
  type LiveT3Placements,
  type WorkstreamAssignmentIndex,
} from "@t3tools/client-runtime/state/workstreams";

export const now = Date.parse("2026-09-30T12:00:00Z");
export const thread = {
  environmentId: "env:a",
  id: "thread",
  projectId: "repo-one",
  pinnedAt: null,
  settledOverride: null,
};
export const metadata = (id: string, sortOrder: number) => ({
  workstreamId: id,
  name: id,
  sortOrder,
  version: 3,
  lifecycle: "active" as const,
  progress: { state: "unknown" as const },
  delivery: "none-observed" as const,
  freshness: "current" as const,
  updatedAt: "2026-09-30T11:00:00Z",
});
export const data: T3WorkstreamListResult = {
  binding: {
    registryId: "registry",
    ownerId: "owner",
    principalId: "principal",
    authorizationRevision: 1,
    serverGeneration: 7,
    registryVersion: 11,
    permissions: ["workstreams:read", "workstreams:write"],
    contractVersion: "workstreams/1.0.0",
    contractManifest: "6d8da23d51c1bba024dddc4b8d4dd9a594d2f474affd73e4cc7de508b797f557",
  },
  items: [metadata("alpha", 0), metadata("beta", 1)],
  source: "live",
  stale: false,
  nextCursor: null,
};
export const reference: NativeReference = {
  native_reference_id: "reference",
  owner_id: "owner",
  identity: {
    provider: "t3",
    source_instance_id: "env:a",
    native_id: "thread",
    resource_kind: "thread",
    id_kind: "internal",
    account_provenance: { kind: "not_account_scoped" },
  },
  pr_locator: null,
  registration: {
    state: "attested",
    attestation_version: 2,
    attested_at: "2026-09-30T11:00:00Z",
    expires_at: "2026-10-01T12:00:00Z",
    evidence: {
      provider: "t3",
      source_instance_id: "env:a",
      native_id: "thread",
      authority_namespace: "authority",
      store_generation: 1,
      evidence_sha256: "a".repeat(64),
    },
  },
  created_at: "2026-09-30T11:00:00Z",
  created_by: { principal_id: "owner" },
  created_registry_version: 1,
};
export const primary: T3ThreadPlacement = {
  native_reference_id: "reference",
  membership_id: "membership",
  workstream_id: "alpha",
  kind: "primary",
  source_instance_id: "env:a",
  native_thread_id: "thread",
  attestation_version: 2,
  attested_at: "2026-09-30T11:00:00Z",
  expires_at: "2026-10-01T12:00:00Z",
  evidence_sha256: "a".repeat(64),
  authority_namespace: "authority",
  store_generation: 1,
  source_binding_version: 1,
};
export const placements: LiveT3Placements = {
  context: {
    owner_id: "owner",
    principal_id: "principal",
    authorization_revision: 1,
    server_generation: 7,
    registry_version: 11,
  },
  items: [
    primary,
    { ...primary, kind: "secondary", membership_id: "secondary", workstream_id: "beta" },
  ],
  trustedEnvironments: [
    { environmentId: "env:a", authorityNamespace: "authority", storeGeneration: 1 },
  ],
  readiness: "ready",
};
export const input = {
  data,
  placements,
  references: [reference],
  thread,
  destination: "beta",
  now,
};

const opened = {
  at: "2026-09-30T11:00:00Z",
  actor: { principal_id: "owner" },
  command_id: "fixture-command",
  registry_version: 1,
};
/** A stored reference for any environment and thread, attested once and then expired. */
export const referenceFor = (environmentId: string, threadId: string): NativeReference => ({
  ...reference,
  native_reference_id: `reference:${environmentId}:${threadId}`,
  identity: { ...reference.identity, source_instance_id: environmentId, native_id: threadId },
  registration: {
    ...reference.registration,
    evidence: {
      ...reference.registration.evidence!,
      source_instance_id: environmentId,
      native_id: threadId,
    },
  },
});
export const membershipFor = (
  membershipId: string,
  workstreamId: string,
  referenceId: string,
  kind: "primary" | "secondary" = "primary",
): MembershipEpisode => ({
  membership_id: membershipId,
  workstream_id: workstreamId,
  native_reference_id: referenceId,
  kind,
  opened,
  closed: null,
});
/** The fixture thread's stored primary in alpha, matching the `primary` placement. */
export const assignmentIndexFor = (
  assignments: readonly {
    readonly environmentId: string;
    readonly threadId: string;
    readonly workstreamId: string;
    readonly kind?: "primary" | "secondary";
    readonly membershipId?: string;
  }[] = [{ environmentId: "env:a", threadId: "thread", workstreamId: "alpha" }],
  complete = true,
): WorkstreamAssignmentIndex => {
  const references = new Map<string, NativeReference>();
  const memberships = assignments.map((entry, position) => {
    const stored =
      entry.environmentId === "env:a" && entry.threadId === "thread"
        ? reference
        : referenceFor(entry.environmentId, entry.threadId);
    references.set(stored.native_reference_id, stored);
    return membershipFor(
      entry.membershipId ??
        (stored === reference && position === 0 ? "membership" : `membership:${position}`),
      entry.workstreamId,
      stored.native_reference_id,
      entry.kind,
    );
  });
  return buildWorkstreamAssignmentIndex({
    ownerId: data.binding.ownerId,
    registryVersion: data.binding.registryVersion,
    references: [...references.values()],
    memberships,
    complete,
  });
};

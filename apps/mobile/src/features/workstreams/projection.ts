import type { PreparedConnection } from "@t3tools/client-runtime/connection";
import {
  buildConnectedEnvironmentTrust,
  groupNativeThreadsByWorkstream,
  orderWorkstreamMetadata,
  nativeWorkstreamThreadKey,
  workstreamPaletteIndex,
  type ConnectedEnvironmentTrustInput,
  type LiveT3Placements,
  type NativeWorkstreamThreadAssurance,
  type WorkstreamAssignmentIndex,
  type WorkstreamThreadLike,
} from "@t3tools/client-runtime/state/workstreams";
import {
  T3_PLACEMENT_MAX_IDENTITIES,
  T3_PLACEMENT_MAX_REQUEST_BYTES,
  type WorkstreamAppearancePage,
  type T3PlacementIdentity,
  type T3WorkstreamListResult,
  type T3WorkstreamMetadata,
} from "@t3tools/contracts";
const COLORS = ["#0284c7", "#7c3aed", "#059669", "#d97706", "#e11d48"];
export interface MobileWorkstreamSnapshot {
  readonly appearance?: WorkstreamAppearancePage | null;
  readonly prepared: PreparedConnection;
  readonly generation: number;
  readonly data: T3WorkstreamListResult;
  /** Current proof: an optional "verified" signal and the gate for membership changes. */
  readonly placements: LiveT3Placements | null;
  readonly identityKeys: ReadonlySet<string>;
  /**
   * Stored assignments read with `data`. Null when only the list could be read; grouping then
   * falls back to current proof and can never confirm a thread as Unassigned.
   */
  readonly index: WorkstreamAssignmentIndex | null;
  /** The last good load after a failed refresh or a lost connection; read-only, never proof. */
  readonly stale: boolean;
}
export function sameMobileWorkstreamAuthority(
  left: MobileWorkstreamSnapshot,
  right: MobileWorkstreamSnapshot,
): boolean {
  return (
    left.prepared === right.prepared &&
    left.generation === right.generation &&
    left.data === right.data &&
    left.placements === right.placements &&
    left.index === right.index &&
    left.identityKeys === right.identityKeys
  );
}

export interface MobileWorkstreamGroup {
  readonly key: string;
  readonly name: string;
  readonly color: string;
  readonly borderColor?: string;
  readonly workstream: T3WorkstreamMetadata;
  readonly threadKeys: ReadonlySet<string>;
  readonly snapshot: MobileWorkstreamSnapshot;
}
const namespace = (data: T3WorkstreamListResult) =>
  JSON.stringify([
    data.binding.registryId,
    data.binding.ownerId,
    data.binding.contractVersion,
    data.binding.contractManifest,
  ]);

/**
 * `liveEnvironments` are the environments' current-session configs. Their descriptors only add
 * display trust, so a thread can reach "verified" without current proof; disagreeing descriptors
 * mark that environment's threads as conflicts. Membership changes keep using placement trust.
 */
export function projectMobileWorkstreams(
  snapshots: readonly MobileWorkstreamSnapshot[],
  threads: readonly WorkstreamThreadLike[],
  now: number,
  liveEnvironments: readonly ConnectedEnvironmentTrustInput[] = [],
) {
  const groups = new Map<string, MobileWorkstreamGroup>();
  const conflicts = new Set<string>();
  const secondaryLabelsByKey = new Map<string, readonly string[]>();
  const assuranceByKey = new Map<string, NativeWorkstreamThreadAssurance>();
  const confirmedUnassignedKeys = new Set<string>();
  const conflictingThreadKeys = new Set<string>();
  const nowIso = new Date(now).toISOString();
  for (const snapshot of snapshots) {
    const binding = snapshot.data.binding;
    const proof = currentProof(snapshot);
    const index = snapshot.index;
    if (
      index !== null
        ? index.ownerId !== binding.ownerId || index.registryVersion !== binding.registryVersion
        : proof === null ||
          snapshot.data.source !== "live" ||
          snapshot.data.stale ||
          snapshot.data.nextCursor !== null
    )
      continue;
    const trust = buildConnectedEnvironmentTrust({
      environments: liveEnvironments,
      placementTrustedEnvironments: proof?.trustedEnvironments,
    });
    const ownThreads = threads.filter(
      (thread) => thread.environmentId === snapshot.prepared.environmentId,
    );
    for (const thread of ownThreads) {
      if (!trust.conflicts.has(thread.environmentId)) continue;
      const key = nativeWorkstreamThreadKey(thread.environmentId, thread.id);
      assuranceByKey.set(key, "conflict");
      conflictingThreadKeys.add(key);
    }
    const live =
      !snapshot.stale &&
      snapshot.data.source === "live" &&
      !snapshot.data.stale &&
      snapshot.data.nextCursor === null;
    const grouped = groupNativeThreadsByWorkstream({
      workstreams: orderWorkstreamMetadata(snapshot.data.items),
      threads: ownThreads.filter((thread) => !trust.conflicts.has(thread.environmentId)),
      trustedNow: nowIso,
      trustedEnvironments: trust.trusted,
      ...(proof ? { placements: proof.items } : {}),
      // Stored assignments group without any proof clock; `now` only decides "verified".
      ...(index !== null
        ? {
            assignmentIndex: live ? index : { ...index, complete: false },
            verificationNow: nowIso,
          }
        : {}),
    });
    for (const [key, assurance] of grouped.assuranceByKey ?? []) {
      assuranceByKey.set(key, assurance);
      if (assurance === "unassigned") confirmedUnassignedKeys.add(key);
    }
    for (const key of grouped.conflictingKeys) conflictingThreadKeys.add(key);
    for (const workstream of snapshot.data.items) {
      const group = grouped.groups.find(
        (entry) => entry.workstream.workstreamId === workstream.workstreamId,
      ) ?? { workstream, threads: [] };
      const key = JSON.stringify([namespace(snapshot.data), group.workstream.workstreamId]);
      if (conflicts.has(key)) continue;
      const prior = groups.get(key);
      if (
        prior &&
        (prior.workstream.version !== group.workstream.version ||
          JSON.stringify(prior.workstream) !== JSON.stringify(group.workstream))
      ) {
        groups.delete(key);
        conflicts.add(key);
        continue;
      }
      const savedAppearance = snapshot.appearance?.items.find(
        (item) => item.workstream_id === group.workstream.workstreamId,
      );
      const priorAppearance = prior?.snapshot.appearance?.items.find(
        (item) => item.workstream_id === group.workstream.workstreamId,
      );
      const appearanceSnapshot =
        priorAppearance && priorAppearance.version > (savedAppearance?.version ?? -1)
          ? prior!.snapshot
          : snapshot;
      const appearance = appearanceSnapshot.appearance?.items.find(
        (item) => item.workstream_id === group.workstream.workstreamId,
      );
      groups.set(key, {
        key,
        name: group.workstream.name,
        color: COLORS[workstreamPaletteIndex(group.workstream.workstreamId)]!,
        ...(appearance?.border_color ? { borderColor: appearance.border_color } : {}),
        workstream: group.workstream,
        snapshot: appearanceSnapshot,
        threadKeys: new Set([
          ...(prior?.threadKeys ?? []),
          ...group.threads.map((thread) =>
            nativeWorkstreamThreadKey(thread.environmentId, thread.id),
          ),
        ]),
      });
    }
    for (const [key, labels] of grouped.secondaryWorkstreamLabelsByKey)
      secondaryLabelsByKey.set(key, labels);
  }
  return {
    groups: [...groups.values()].sort(
      (a, b) => a.workstream.sortOrder - b.workstream.sortOrder || a.key.localeCompare(b.key),
    ),
    secondaryLabelsByKey,
    assuranceByKey,
    /** Visible threads that a complete, live index shows without an open primary. */
    confirmedUnassignedKeys,
    /** Threads with conflicting stored assignments; membership changes stay blocked. */
    conflictingThreadKeys,
    stale: snapshots.some((snapshot) => snapshot.stale),
  };
}

/** Placements bound to the snapshot's list, or null; never a reason to hide stored groups. */
function currentProof(snapshot: MobileWorkstreamSnapshot): LiveT3Placements | null {
  const placements = snapshot.placements;
  const binding = snapshot.data.binding;
  if (!placements || placements.readiness !== "ready") return null;
  const context = placements.context;
  return context.owner_id === binding.ownerId &&
    context.principal_id === binding.principalId &&
    context.authorization_revision === binding.authorizationRevision &&
    context.server_generation === binding.serverGeneration &&
    context.registry_version === binding.registryVersion
    ? placements
    : null;
}

export function mobilePlacementInventory(
  threads: readonly WorkstreamThreadLike[],
): readonly T3PlacementIdentity[] {
  const unique = new Map(
    threads.map((thread) => [
      nativeWorkstreamThreadKey(thread.environmentId, thread.id),
      { source_instance_id: thread.environmentId, native_thread_id: thread.id },
    ]),
  );
  const result: T3PlacementIdentity[] = [];
  const encoder = new TextEncoder();
  let bytes = encoder.encode(JSON.stringify({ identities: [] })).length;
  for (const [, identity] of [...unique].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))) {
    const next = encoder.encode(JSON.stringify(identity)).length + (result.length ? 1 : 0);
    if (
      result.length === T3_PLACEMENT_MAX_IDENTITIES ||
      bytes + next > T3_PLACEMENT_MAX_REQUEST_BYTES
    )
      break;
    bytes += next;
    result.push(identity);
  }
  return result;
}

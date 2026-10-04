import type { PreparedConnection } from "@t3tools/client-runtime/connection";
import {
  groupNativeThreadsByWorkstream,
  orderWorkstreamMetadata,
  nativeWorkstreamThreadKey,
  workstreamPaletteIndex,
  type LiveT3Placements,
  type WorkstreamThreadLike,
} from "@t3tools/client-runtime/state/workstreams";
import {
  T3_PLACEMENT_MAX_IDENTITIES,
  T3_PLACEMENT_MAX_REQUEST_BYTES,
  type T3PlacementIdentity,
  type T3WorkstreamListResult,
  type T3WorkstreamMetadata,
} from "@t3tools/contracts";
const COLORS = ["#0284c7", "#7c3aed", "#059669", "#d97706", "#e11d48"];
export interface MobileWorkstreamSnapshot {
  readonly prepared: PreparedConnection;
  readonly generation: number;
  readonly data: T3WorkstreamListResult;
  readonly placements: LiveT3Placements | null;
  readonly identityKeys: ReadonlySet<string>;
}
export interface MobileWorkstreamGroup {
  readonly key: string;
  readonly name: string;
  readonly color: string;
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

export function projectMobileWorkstreams(
  snapshots: readonly MobileWorkstreamSnapshot[],
  threads: readonly WorkstreamThreadLike[],
  now: number,
) {
  const groups = new Map<string, MobileWorkstreamGroup>();
  const conflicts = new Set<string>();
  const secondaryLabelsByKey = new Map<string, readonly string[]>();
  for (const snapshot of snapshots) {
    if (
      !snapshot.placements ||
      snapshot.placements.readiness !== "ready" ||
      snapshot.data.source !== "live" ||
      snapshot.data.stale ||
      snapshot.data.nextCursor !== null
    )
      continue;
    const context = snapshot.placements.context;
    const binding = snapshot.data.binding;
    if (
      context.owner_id !== binding.ownerId ||
      context.principal_id !== binding.principalId ||
      context.authorization_revision !== binding.authorizationRevision ||
      context.server_generation !== binding.serverGeneration ||
      context.registry_version !== binding.registryVersion
    )
      continue;
    const ownThreads = threads.filter(
      (thread) => thread.environmentId === snapshot.prepared.environmentId,
    );
    const grouped = groupNativeThreadsByWorkstream({
      workstreams: orderWorkstreamMetadata(snapshot.data.items),
      placements: snapshot.placements.items,
      threads: ownThreads,
      trustedNow: new Date(now).toISOString(),
      trustedEnvironments: new Map(
        snapshot.placements.trustedEnvironments.map((trust) => [trust.environmentId, trust]),
      ),
    });
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
      groups.set(key, {
        key,
        name: group.workstream.name,
        color: COLORS[workstreamPaletteIndex(group.workstream.workstreamId)]!,
        workstream: group.workstream,
        snapshot: prior?.snapshot ?? snapshot,
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
  };
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

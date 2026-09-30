import {
  T3PlacementLoadRequest,
  T3PlacementResult,
  WORKSTREAM_MAX_RESPONSE_BYTES,
  t3PlacementIdentityKey,
  t3PlacementInventoryJson,
  type T3PlacementIdentity,
  type T3ThreadPlacement,
  type T3WorkstreamListResult,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";

export interface LiveT3Placements {
  readonly context: T3PlacementResult["page"]["context"];
  readonly items: readonly T3ThreadPlacement[];
  readonly trustedEnvironments: T3PlacementResult["trustedEnvironments"];
  readonly readiness: T3PlacementResult["readiness"];
}

export const currentT3Placement = (item: T3ThreadPlacement, now: number): boolean =>
  Number.isFinite(now) &&
  Number.isFinite(Date.parse(item.attested_at)) &&
  Date.parse(item.attested_at) <= now &&
  Date.parse(item.expires_at) > now &&
  Date.parse(item.expires_at) > Date.parse(item.attested_at);

export type WorkstreamSha256 = (bytes: Uint8Array) => Promise<Uint8Array>;

const browserSha256: WorkstreamSha256 = async (bytes) =>
  new Uint8Array(await crypto.subtle.digest("SHA-256", Uint8Array.from(bytes)));

export async function loadLiveT3Placements(
  metadata: T3WorkstreamListResult,
  identities: readonly T3PlacementIdentity[],
  load: () => Promise<T3PlacementResult>,
  now: () => number = Date.now,
  sha256: WorkstreamSha256 = browserSha256,
): Promise<LiveT3Placements> {
  if (metadata.source !== "live" || metadata.stale || metadata.nextCursor !== null)
    throw new Error("Placement metadata is not current.");
  Schema.decodeUnknownSync(T3PlacementLoadRequest)({ identities }, { onExcessProperty: "error" });
  const digest = await sha256(new TextEncoder().encode(t3PlacementInventoryJson(identities)));
  if (digest.length !== 32) throw new Error("Invalid SHA-256 digest length.");
  const inventoryDigest = Array.from(digest, (value) => value.toString(16).padStart(2, "0")).join(
    "",
  );
  const result = Schema.decodeUnknownSync(T3PlacementResult)(await load(), {
    onExcessProperty: "error",
  });
  if (new TextEncoder().encode(JSON.stringify(result)).length > WORKSTREAM_MAX_RESPONSE_BYTES)
    throw new Error("Placement response workload exceeded.");
  const { page } = result;
  const { binding } = metadata;
  if (
    page.inventory_sha256 !== inventoryDigest ||
    page.context.owner_id !== binding.ownerId ||
    page.context.principal_id !== binding.principalId ||
    page.context.authorization_revision !== binding.authorizationRevision ||
    page.context.server_generation !== binding.serverGeneration ||
    page.context.registry_version !== binding.registryVersion
  )
    throw new Error("Placement revision changed.");
  if (
    new Set(result.trustedEnvironments.map((value) => value.environmentId)).size !==
      result.trustedEnvironments.length ||
    (result.readiness === "trust-provider-required" && result.trustedEnvironments.length !== 0)
  )
    throw new Error("Invalid placement trust snapshot.");
  const requested = new Set(identities.map(t3PlacementIdentityKey));
  const memberships = new Set<string>();
  const referenceRouting = new Map<string, string>();
  let previous: readonly [string, string] | undefined;
  for (const item of page.items) {
    if (
      !currentT3Placement(item, now()) ||
      memberships.has(item.membership_id) ||
      !requested.has(
        t3PlacementIdentityKey({
          source_instance_id: item.source_instance_id,
          native_thread_id: item.native_thread_id,
        }),
      )
    )
      throw new Error("Stale, repeated, or unrelated placement.");
    const tuple = [item.native_reference_id, item.membership_id] as const;
    if (
      previous &&
      (tuple[0] < previous[0] || (tuple[0] === previous[0] && tuple[1] <= previous[1]))
    )
      throw new Error("Placement order changed.");
    previous = tuple;
    const {
      membership_id: _membership,
      workstream_id: _workstream,
      kind: _kind,
      ...routing
    } = item;
    const serialized = JSON.stringify(routing);
    const priorRouting = referenceRouting.get(item.native_reference_id);
    if (priorRouting !== undefined && priorRouting !== serialized)
      throw new Error("Placement reference changed.");
    referenceRouting.set(item.native_reference_id, serialized);
    memberships.add(item.membership_id);
  }
  if (page.items.some((item) => !currentT3Placement(item, now())))
    throw new Error("Placement expired while loading.");
  return {
    context: page.context,
    items: page.items,
    trustedEnvironments: result.trustedEnvironments,
    readiness: result.readiness,
  };
}

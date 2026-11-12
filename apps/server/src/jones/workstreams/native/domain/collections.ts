import { requireDomain, WorkstreamDomainError } from "./errors.ts";
import { requireAttested } from "./decider.ts";
import type { WorkstreamDomainState, NativeReference, RegistrySourceBinding } from "./state.ts";
import type { RegistryCollections } from "./state.ts";

export type RegistryCollectionPosition = readonly (number | string)[];

const id = (value: unknown): value is string =>
  typeof value === "string" && value.length >= 1 && value.length <= 512;
const integer = (value: unknown): value is number =>
  Number.isSafeInteger(value) && (value as number) >= 0;

export function validCollectionPosition(
  collection: keyof RegistryCollections,
  position: unknown,
): position is RegistryCollectionPosition | null {
  if (position === null) return true;
  if (!Array.isArray(position)) return false;
  switch (collection) {
    case "workstreams":
      return position.length === 2 && integer(position[0]) && id(position[1]);
    case "references":
      return position.length === 1 && id(position[0]);
    case "memberships":
    case "edges":
    case "history":
      return position.length === 2 && integer(position[0]) && id(position[1]);
    case "declarations":
      return (
        position.length === 3 && integer(position[0]) && id(position[1]) && integer(position[2])
      );
  }
}

export function collectionPosition<K extends keyof RegistryCollections>(
  collection: K,
  value: RegistryCollections[K],
): RegistryCollectionPosition {
  switch (collection) {
    case "workstreams": {
      const item = value as RegistryCollections["workstreams"];
      return [item.sort_order, item.workstream_id];
    }
    case "references":
      return [(value as RegistryCollections["references"]).native_reference_id];
    case "memberships": {
      const item = value as RegistryCollections["memberships"];
      return [item.opened.registry_version, item.membership_id];
    }
    case "declarations": {
      const item = value as RegistryCollections["declarations"];
      return [item.registry_version, item.declaration_id, item.revision];
    }
    case "edges": {
      const item = value as RegistryCollections["edges"];
      return [item.opened.registry_version, item.edge_id];
    }
    case "history": {
      const item = value as RegistryCollections["history"];
      return [item.registry_version, item.event_id];
    }
  }
}

export function compareCollectionPositions(
  left: RegistryCollectionPosition,
  right: RegistryCollectionPosition,
): number {
  for (let index = 0; index < left.length; index += 1) {
    const a = left[index]!;
    const b = right[index];
    if (typeof a !== typeof b || b === undefined) throw new Error("invalid_collection_position");
    if (typeof a === "string" && typeof b === "string") {
      const order = Buffer.compare(Buffer.from(a, "utf8"), Buffer.from(b, "utf8"));
      if (order !== 0) return Math.sign(order);
      continue;
    }
    if (a < b) return -1;
    if (a > b) return 1;
  }
  return left.length - right.length;
}

/** event_sequence is store-owned and may contain gaps after an import. */
export interface SequencedAuditEvent {
  readonly event_sequence: number;
  readonly event: RegistryCollections["history"];
}

export function pageCollection<K extends keyof RegistryCollections>(
  collection: K,
  values: readonly RegistryCollections[K][],
  after: RegistryCollectionPosition | null,
  limit: number,
): readonly RegistryCollections[K][] {
  requireDomain(
    validCollectionPosition(collection, after) &&
      Number.isSafeInteger(limit) &&
      limit >= 1 &&
      limit <= 101,
    "invalid_request",
  );
  return values
    .filter(
      (item) =>
        after === null ||
        compareCollectionPositions(collectionPosition(collection, item), after) > 0,
    )
    .toSorted((a, b) =>
      compareCollectionPositions(
        collectionPosition(collection, a),
        collectionPosition(collection, b),
      ),
    )
    .slice(0, limit);
}

export function historyPage(
  events: readonly SequencedAuditEvent[],
  workstreamId: string | null,
  highWater: number,
  after: RegistryCollectionPosition | null,
  limit: number,
): readonly RegistryCollections["history"][] {
  requireDomain(Number.isSafeInteger(highWater) && highWater >= 0, "invalid_request");
  return pageCollection(
    "history",
    events
      .filter(
        (row) =>
          row.event_sequence <= highWater &&
          (workstreamId === null ||
            row.event.workstream_versions.some((v) => v.workstream_id === workstreamId)),
      )
      .map((row) => row.event),
    after,
    limit,
  );
}

export function lifecycleCounts(
  state: Pick<WorkstreamDomainState, "workstreams" | "lifecycle_declarations">,
): { readonly total: number; readonly active: number; readonly unknown_lifecycle: number } {
  let active = 0,
    unknown_lifecycle = 0;
  for (const id of state.workstreams.keys()) {
    const latest = state.lifecycle_declarations
      .get(id)
      ?.reduce<RegistryCollections["history"]["lifecycle_declaration"]>(
        (previous, next) => (!previous || next.revision > previous.revision ? next : previous),
        null,
      );
    if (latest?.new_lifecycle === "active") active++;
    else if (
      !latest ||
      !["planned", "paused", "completed", "deferred", "abandoned"].includes(latest.new_lifecycle)
    )
      unknown_lifecycle++;
  }
  return { total: state.workstreams.size, active, unknown_lifecycle };
}

export function referenceForRead(
  reference: NativeReference | null,
  ownerId: string,
  principalId: string,
  binding: RegistrySourceBinding | undefined,
  now: string,
): NativeReference | null {
  if (!reference || reference.owner_id !== ownerId) return null;
  if (reference.registration.state !== "attested")
    return reference.created_by.principal_id === principalId ? reference : null;
  try {
    requireAttested(reference, now, binding);
    return reference;
  } catch (error) {
    if (!(error instanceof WorkstreamDomainError) || error.code !== "registration_unverified")
      throw error;
    const expired =
      reference.registration.expires_at !== null &&
      Date.parse(reference.registration.expires_at) <= Date.parse(now);
    return {
      ...reference,
      registration: {
        ...reference.registration,
        state: expired ? "expired" : "re-attestation-required",
      },
    };
  }
}

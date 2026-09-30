import type {
  T3WorkstreamBinding,
  T3WorkstreamListResult,
  T3WorkstreamMetadata,
  WorkstreamReadContext,
} from "@t3tools/contracts";

export function compareWorkstreamId(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}

export function orderWorkstreamMetadata(
  items: readonly T3WorkstreamMetadata[],
): readonly T3WorkstreamMetadata[] {
  return [...items].sort(
    (left, right) =>
      left.sortOrder - right.sortOrder ||
      compareWorkstreamId(left.workstreamId, right.workstreamId),
  );
}

export function planWorkstreamOwnerOrder(
  items: readonly T3WorkstreamMetadata[],
  sourceWorkstreamId: string,
  targetIndex: number,
): readonly { readonly item: T3WorkstreamMetadata; readonly sortOrder: number }[] {
  const ordered = [...orderWorkstreamMetadata(items)];
  const sourceIndex = ordered.findIndex((item) => item.workstreamId === sourceWorkstreamId);
  if (sourceIndex < 0 || !Number.isSafeInteger(targetIndex)) return [];
  const [source] = ordered.splice(sourceIndex, 1);
  if (!source) return [];
  ordered.splice(Math.max(0, Math.min(targetIndex, ordered.length)), 0, source);
  return ordered.map((item, sortOrder) => ({ item, sortOrder }));
}

export interface WorkstreamDtoPage<Item> {
  readonly context: WorkstreamReadContext;
  readonly items: readonly Item[];
  readonly next_cursor: string | null;
}

export function appendWorkstreamDtoPage<Item>(
  current: WorkstreamDtoPage<Item>,
  next: WorkstreamDtoPage<Item>,
): WorkstreamDtoPage<Item> {
  if (
    current.context.owner_id !== next.context.owner_id ||
    current.context.server_generation !== next.context.server_generation ||
    current.context.registry_version !== next.context.registry_version
  ) {
    throw new Error("Workstream page binding changed during pagination.");
  }
  return {
    context: current.context,
    items: [...current.items, ...next.items],
    next_cursor: next.next_cursor,
  };
}

export function appendWorkstreamListResult(
  current: T3WorkstreamListResult,
  next: T3WorkstreamListResult,
): T3WorkstreamListResult {
  if (workstreamBindingKey(current.binding) !== workstreamBindingKey(next.binding)) {
    throw new Error("Workstream list binding changed during pagination.");
  }
  return {
    ...current,
    items: [...current.items, ...next.items],
    nextCursor: next.nextCursor,
    source: current.source === "live" && next.source === "live" ? "live" : "cache",
    stale: current.stale || next.stale,
  };
}

type WorkstreamBindingIdentity = Pick<
  T3WorkstreamBinding,
  | "registryId"
  | "ownerId"
  | "principalId"
  | "authorizationRevision"
  | "serverGeneration"
  | "registryVersion"
  | "contractVersion"
  | "contractManifest"
>;

export const workstreamBindingKey = (binding: WorkstreamBindingIdentity): string =>
  [
    binding.registryId,
    binding.ownerId,
    binding.principalId,
    binding.authorizationRevision,
    binding.serverGeneration,
    binding.registryVersion,
    binding.contractVersion,
    binding.contractManifest,
  ].join("\u0000");

/** In-memory cache for the server-minimized Workstream list DTO only. */
export class LiveWorkstreamMetadataCache {
  #entry: { readonly key: string; readonly value: T3WorkstreamListResult } | null = null;

  read(binding: T3WorkstreamBinding): T3WorkstreamListResult | null {
    return this.#entry?.key === workstreamBindingKey(binding) ? this.#entry.value : null;
  }

  write(value: T3WorkstreamListResult): void {
    this.#entry = {
      key: workstreamBindingKey(value.binding),
      value: { ...value, items: [...value.items] },
    };
  }

  purgeAuthorization(): void {
    this.#entry = null;
  }
}

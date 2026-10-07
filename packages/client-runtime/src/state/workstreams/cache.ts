import type { WorkstreamReadContext, WorkstreamRecord, WorkstreamState } from "./model.ts";

function cacheKey(context: WorkstreamReadContext): string {
  return [
    context.registryId,
    context.ownerScopeId,
    context.principalId,
    context.authorizationRevision,
    context.serverGeneration,
    context.registryVersion,
    context.contractVersion,
    context.contractManifest,
  ].join("\u0000");
}

export type CachedWorkstreamMetadata = Pick<
  WorkstreamRecord,
  | "workstreamId"
  | "name"
  | "lifecycle"
  | "progress"
  | "delivery"
  | "freshness"
  | "sortOrder"
  | "version"
  | "updatedAt"
>;

export interface WorkstreamMetadataSnapshot {
  readonly context: WorkstreamReadContext;
  readonly workstreams: Readonly<Record<string, CachedWorkstreamMetadata>>;
}

function minimize(state: WorkstreamState): WorkstreamMetadataSnapshot {
  return {
    context: state.context,
    workstreams: Object.fromEntries(
      Object.values(state.workstreams).map((item) => [
        item.workstreamId,
        {
          workstreamId: item.workstreamId,
          name: item.name,
          lifecycle: item.lifecycle,
          progress: item.progress,
          delivery: item.delivery,
          freshness: item.freshness,
          sortOrder: item.sortOrder,
          version: item.version,
          updatedAt: item.updatedAt,
        },
      ]),
    ),
  };
}

/** In-memory, metadata-only cache. Authorization loss makes every read unavailable. */
export class WorkstreamMetadataCache {
  readonly #entries = new Map<string, WorkstreamMetadataSnapshot>();
  readonly maxEntries: number;

  constructor(maxEntries = 4) {
    if (!Number.isSafeInteger(maxEntries) || maxEntries < 1) {
      throw new RangeError("maxEntries must be a positive safe integer");
    }
    this.maxEntries = maxEntries;
  }

  read(context: WorkstreamReadContext, authorized: boolean): WorkstreamMetadataSnapshot | null {
    if (!authorized) return null;
    const key = cacheKey(context);
    const value = this.#entries.get(key) ?? null;
    if (value) {
      this.#entries.delete(key);
      this.#entries.set(key, value);
    }
    return value;
  }

  write(state: WorkstreamState, authorized = true): void {
    if (!authorized) {
      this.purge();
      return;
    }
    const key = cacheKey(state.context);
    this.#entries.delete(key);
    this.#entries.set(key, minimize(state));
    while (this.#entries.size > this.maxEntries) {
      const oldest = this.#entries.keys().next().value;
      if (oldest === undefined) break;
      this.#entries.delete(oldest);
    }
  }

  purge(): void {
    this.#entries.clear();
  }

  transitionBinding(
    previous: WorkstreamReadContext,
    next: WorkstreamReadContext,
  ): WorkstreamMetadataSnapshot | null {
    this.#entries.delete(cacheKey(previous));
    return this.read(next, true);
  }
}

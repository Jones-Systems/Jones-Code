import type { EnvironmentId } from "@t3tools/contracts";
import type {
  ThreadMonitorLifetime,
  ThreadMonitorRef,
  ThreadMonitorViewChunk,
} from "@t3tools/contracts/jones/desktopThreadMonitor";

export const sameMonitorLifetime = (
  left: ThreadMonitorLifetime,
  right: ThreadMonitorLifetime,
): boolean =>
  left.publisherGeneration === right.publisherGeneration &&
  left.environmentId === right.environmentId &&
  left.environmentEpoch === right.environmentEpoch;
export const monitorLifetimeKey = (ref: ThreadMonitorLifetime): string =>
  JSON.stringify([ref.publisherGeneration, ref.environmentId, ref.environmentEpoch]);
export const monitorDraftKey = (ref: ThreadMonitorRef, requestId: string | null = null): string =>
  JSON.stringify([
    ref.publisherGeneration,
    ref.environmentId,
    ref.environmentEpoch,
    ref.threadId,
    requestId,
  ]);

// Registration belongs to the main renderer's authoritative registry, never incoming peer data.
export class MonitorLifetimeFence {
  readonly publisherGeneration: string;
  private readonly epochs = new Map<EnvironmentId, number>();
  private readonly registered = new Set<EnvironmentId>();
  constructor(publisherGeneration: string) {
    this.publisherGeneration = publisherGeneration;
  }
  register(environmentId: EnvironmentId): ThreadMonitorLifetime {
    if (!this.registered.has(environmentId)) {
      this.epochs.set(environmentId, (this.epochs.get(environmentId) ?? -1) + 1);
      this.registered.add(environmentId);
    }
    return {
      publisherGeneration: this.publisherGeneration,
      environmentId,
      environmentEpoch: this.epochs.get(environmentId)!,
    };
  }
  retire(environmentId: EnvironmentId): ThreadMonitorLifetime | null {
    if (!this.registered.delete(environmentId)) return null;
    return {
      publisherGeneration: this.publisherGeneration,
      environmentId,
      environmentEpoch: this.epochs.get(environmentId)!,
    };
  }
  accepts(ref: ThreadMonitorLifetime): boolean {
    return (
      ref.publisherGeneration === this.publisherGeneration &&
      this.registered.has(ref.environmentId) &&
      this.epochs.get(ref.environmentId) === ref.environmentEpoch
    );
  }
}
export interface MonitorChunkState {
  readonly publisherGeneration: string;
  readonly snapshotId: string;
  readonly revision: number;
  readonly nextChunkIndex: number;
  readonly chunkCount: number;
  readonly chunks: ReadonlyArray<ThreadMonitorViewChunk>;
  readonly complete: boolean;
}
export function acceptMonitorChunk(
  state: MonitorChunkState | null,
  chunk: ThreadMonitorViewChunk,
  acceptsLifetime: (ref: ThreadMonitorLifetime) => boolean,
): MonitorChunkState | null {
  if (
    chunk.chunkIndex >= chunk.chunkCount ||
    chunk.complete !== (chunk.chunkIndex === chunk.chunkCount - 1) ||
    [...chunk.environments, ...chunk.upserts, ...chunk.removed].some(
      (ref) => ref.publisherGeneration !== chunk.publisherGeneration || !acceptsLifetime(ref),
    )
  )
    return null;
  if (
    state !== null &&
    state.publisherGeneration === chunk.publisherGeneration &&
    chunk.revision < state.revision
  )
    return null;
  if (chunk.chunkIndex === 0) {
    if (
      state !== null &&
      state.publisherGeneration === chunk.publisherGeneration &&
      chunk.revision === state.revision
    )
      return null;
    return {
      publisherGeneration: chunk.publisherGeneration,
      snapshotId: chunk.snapshotId,
      revision: chunk.revision,
      nextChunkIndex: 1,
      chunkCount: chunk.chunkCount,
      chunks: [chunk],
      complete: chunk.complete,
    };
  }
  if (
    state === null ||
    state.complete ||
    state.publisherGeneration !== chunk.publisherGeneration ||
    state.snapshotId !== chunk.snapshotId ||
    state.revision !== chunk.revision ||
    state.nextChunkIndex !== chunk.chunkIndex ||
    state.chunkCount !== chunk.chunkCount
  )
    return null;
  return {
    ...state,
    nextChunkIndex: chunk.chunkIndex + 1,
    chunks: [...state.chunks, chunk],
    complete: chunk.complete,
  };
}

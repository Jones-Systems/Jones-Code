/** Receiving PDF zoom is a percentage of fitted width, never PdfViewer's absolute scale. */
export interface WorkspacePdfPosition {
  readonly page: number;
  readonly offset: number;
  readonly left: number;
  readonly zoomPercent: number;
}
export const initialWorkspacePdfPosition: WorkspacePdfPosition = {
  page: 0,
  offset: 0,
  left: 0,
  zoomPercent: 100,
};
interface WorkspacePdfLoad {
  readonly src: string;
  readonly generation: number;
  readonly retryAttempt: number;
}

/** One resource owner survives unavailable grants, content revisions and explicit retries. */
export function createWorkspacePdfSession(resourceId: string) {
  let active = true;
  let lifetime = 0;
  let revision: string | null | undefined;
  let generation = 0;
  let current: WorkspacePdfLoad | undefined;
  let positionOwner: WorkspacePdfLoad | undefined;
  let position = initialWorkspacePdfPosition;
  const selectRevision = (next: string | null) => {
    if (revision === next) return;
    revision = next;
    generation += 1;
    current = undefined;
  };
  return {
    resourceId,
    selectRevision,
    load(src: string, retryAttempt = 0): WorkspacePdfLoad {
      return (current ??= { src, generation, retryAttempt });
    },
    readPosition: () => position,
    remember(load: WorkspacePdfLoad, next: WorkspacePdfPosition) {
      if (!active || (load !== current && load !== positionOwner)) return;
      // The old viewer can synchronously capture its last scroll before the new one initializes.
      if (load === current) positionOwner = load;
      position = { ...next };
    },
    beginRetry(load: WorkspacePdfLoad) {
      const capturedLifetime = lifetime;
      return (src: string): boolean => {
        if (!active || current !== load || capturedLifetime !== lifetime) return false;
        current = { src, generation: ++generation, retryAttempt: load.retryAttempt + 1 };
        return true;
      };
    },
    activate() {
      if (!active) {
        active = true;
        lifetime += 1;
      }
    },
    dispose() {
      active = false;
      lifetime += 1;
    },
  };
}
export type WorkspacePdfSession = ReturnType<typeof createWorkspacePdfSession>;

export function clampWorkspacePdfPosition(
  view: WorkspacePdfPosition,
  pageHeights: readonly number[],
  maximumLeft: number,
): WorkspacePdfPosition {
  const page = Math.max(0, Math.min(pageHeights.length - 1, Math.trunc(view.page)));
  return {
    page,
    offset: Math.max(0, Math.min(view.offset, Math.max(0, (pageHeights[page] ?? 0) - 1))),
    left: Math.max(0, Math.min(view.left, maximumLeft)),
    zoomPercent: Math.max(60, Math.min(800, view.zoomPercent)),
  };
}

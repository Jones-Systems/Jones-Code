export type CitationPoint = { x: number; y: number };
export type CitationRect = { left: number; right: number; top: number; bottom: number };

const GAP = 8;
const MARGIN = 8;
const POINTER_CLEARANCE = 4;

export function resolveCitationPlacement({
  rects,
  pointer,
  toolbar,
  viewport,
}: {
  rects: readonly CitationRect[];
  pointer: CitationPoint | null;
  toolbar: { width: number; height: number };
  viewport: { width: number; height: number };
}): CitationPoint | null {
  const visible = rects
    .map((rect) => ({
      left: Math.max(0, rect.left),
      right: Math.min(viewport.width, rect.right),
      top: Math.max(0, rect.top),
      bottom: Math.min(viewport.height, rect.bottom),
    }))
    .filter((rect) => rect.right > rect.left && rect.bottom > rect.top);
  const last = visible.at(-1);
  if (!last) return null;
  const target = pointer ?? { x: last.right, y: last.bottom };
  const maxX = viewport.width - toolbar.width - MARGIN;
  const maxY = viewport.height - toolbar.height - MARGIN;
  if (maxX < MARGIN || maxY < MARGIN) return null;
  const clamp = (value: number, max: number) => Math.max(MARGIN, Math.min(value, max));
  type Candidate = { position: CitationPoint; distance: number };
  const candidates: (Candidate | undefined)[] = Array.from({ length: 4 });
  let top = Infinity;
  let bottom = -Infinity;

  const consider = (side: number, position: CitationPoint, anchor: CitationPoint) => {
    if (position.x < MARGIN || position.x > maxX || position.y < MARGIN || position.y > maxY) {
      return;
    }
    if (
      pointer &&
      pointer.x >= position.x - POINTER_CLEARANCE &&
      pointer.x <= position.x + toolbar.width + POINTER_CLEARANCE &&
      pointer.y >= position.y - POINTER_CLEARANCE &&
      pointer.y <= position.y + toolbar.height + POINTER_CLEARANCE
    ) {
      return;
    }
    const distance = (anchor.x - target.x) ** 2 + (anchor.y - target.y) ** 2;
    if (!candidates[side] || distance < candidates[side].distance) {
      candidates[side] = { position, distance };
    }
  };

  for (const rect of visible) {
    top = Math.min(top, rect.top);
    bottom = Math.max(bottom, rect.bottom);
  }
  // Only the outer horizontal edges are exposed across a multiline selection.
  // Keep four candidates so collision checks remain linear in the rect count.
  for (const rect of visible) {
    const x = Math.max(rect.left, Math.min(target.x, rect.right));
    const y = Math.max(rect.top, Math.min(target.y, rect.bottom));
    consider(
      0,
      { x: rect.left - toolbar.width - GAP, y: clamp(y - toolbar.height / 2, maxY) },
      { x: rect.left, y },
    );
    consider(
      1,
      { x: rect.right + GAP, y: clamp(y - toolbar.height / 2, maxY) },
      { x: rect.right, y },
    );
    if (rect.top === top) {
      consider(
        2,
        { x: clamp(x - toolbar.width / 2, maxX), y: rect.top - toolbar.height - GAP },
        { x, y: rect.top },
      );
    }
    if (rect.bottom === bottom) {
      consider(
        3,
        { x: clamp(x - toolbar.width / 2, maxX), y: rect.bottom + GAP },
        { x, y: rect.bottom },
      );
    }
  }

  let best: Candidate | undefined;
  for (const candidate of candidates) {
    if (!candidate) continue;
    const { x, y } = candidate.position;
    if (
      visible.some(
        (rect) =>
          x < rect.right &&
          x + toolbar.width > rect.left &&
          y < rect.bottom &&
          y + toolbar.height > rect.top,
      )
    ) {
      continue;
    }
    if (
      !best ||
      candidate.distance < best.distance ||
      (!pointer && candidate === candidates[3] && candidate.distance === best.distance)
    ) {
      best = candidate;
    }
  }
  return best?.position ?? null;
}

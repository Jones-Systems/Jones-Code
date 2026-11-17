type Point = { x: number; y: number };

/** Keep hit testing current as rows move beneath a stationary drag pointer. */
export function createSidebarDragAutoScroll(source: HTMLElement, onScroll: (point: Point) => void) {
  const viewport = source.closest<HTMLElement>('[data-slot="scroll-area-viewport"]');
  const view = source.ownerDocument.defaultView!;
  let point: Point | null = null;
  let frame: number | null = null;
  let previousTime: number | null = null;

  const velocity = () => {
    if (!viewport || !point) return 0;
    const bounds = viewport.getBoundingClientRect();
    if (point.x < bounds.left || point.x > bounds.right) return 0;
    const edge = Math.min(80, bounds.height / 4);
    if (edge <= 0) return 0;
    if (point.y < bounds.top + edge && viewport.scrollTop > 0)
      return -600 * Math.min(1, (bounds.top + edge - point.y) / edge);
    if (
      point.y > bounds.bottom - edge &&
      viewport.scrollTop < viewport.scrollHeight - viewport.clientHeight
    )
      return 600 * Math.min(1, (point.y - bounds.bottom + edge) / edge);
    return 0;
  };
  const stop = () => {
    if (frame !== null) view.cancelAnimationFrame(frame);
    frame = null;
    previousTime = null;
  };
  const tick = (time: number) => {
    frame = null;
    const speed = velocity();
    if (!viewport || !point || speed === 0) {
      previousTime = null;
      return;
    }
    const elapsed = previousTime === null ? 16 : Math.min(32, time - previousTime);
    previousTime = time;
    const before = viewport.scrollTop;
    viewport.scrollTop += (speed * elapsed) / 1000;
    if (viewport.scrollTop !== before) onScroll(point);
    frame = view.requestAnimationFrame(tick);
  };
  return {
    move(next: Point) {
      point = next;
      if (velocity() === 0) stop();
      else if (frame === null) frame = view.requestAnimationFrame(tick);
    },
    dispose: stop,
  };
}

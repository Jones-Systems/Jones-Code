// @effect-diagnostics globalDate:off globalTimers:off - Browser and native view lifecycles own this imperative timer outside an Effect runtime.
const THREAD_READ_DWELL_MS = 3_000;
// During an active turn the thread's updatedAt advances several times per
// second, and every server-side visit is a full command dispatch plus a
// broadcast to all shell subscribers.
const THREAD_VISIT_THROTTLE_MS = 10_000;

type ViewingThread = {
  threadKey: string;
  updatedAt: string;
  lastVisitedAt: string | null | undefined;
  completedAt?: string | null;
  visible: boolean;
  onVisit: (updatedAt: string) => void;
};

const activeViews = new Map<string, Set<() => void>>();

export function suppressThreadReadDwell(threadKey: string): void {
  for (const suppress of activeViews.get(threadKey) ?? []) suppress();
}

export function createThreadReadDwell() {
  let current: ViewingThread | null = null;
  let visibleSince: number | null = null;
  let lastDispatchedWatermark: string | null = null;
  let lastDispatchedAt: number | null = null;
  let suppressed = false;
  let timer: ReturnType<typeof setTimeout> | null = null;

  const clearTimer = () => {
    if (timer !== null) clearTimeout(timer);
    timer = null;
  };
  const suppress = () => {
    suppressed = true;
    clearTimer();
  };
  const unregister = () => {
    if (current === null) return;
    const views = activeViews.get(current.threadKey);
    views?.delete(suppress);
    if (views?.size === 0) activeViews.delete(current.threadKey);
  };
  const schedule = () => {
    clearTimer();
    if (current === null || !current.visible || visibleSince === null || suppressed) return;
    const updatedAt = Date.parse(current.updatedAt);
    const visitedAt = current.lastVisitedAt ? Date.parse(current.lastVisitedAt) : NaN;
    if (
      !Number.isFinite(updatedAt) ||
      visitedAt >= updatedAt ||
      lastDispatchedWatermark === current.updatedAt
    )
      return;
    const completedAt = current.completedAt ? Date.parse(current.completedAt) : NaN;
    const unseenCompletion =
      Number.isFinite(completedAt) && (!Number.isFinite(visitedAt) || completedAt > visitedAt);
    const throttleUntil =
      lastDispatchedAt !== null && !unseenCompletion
        ? lastDispatchedAt + THREAD_VISIT_THROTTLE_MS
        : 0;
    const dueAt = Math.max(visibleSince + THREAD_READ_DWELL_MS, throttleUntil);
    const remaining = dueAt - Date.now();
    if (remaining > 0) {
      timer = setTimeout(schedule, remaining);
      return;
    }
    lastDispatchedWatermark = current.updatedAt;
    lastDispatchedAt = Date.now();
    current.onVisit(current.updatedAt);
  };

  return {
    update(next: ViewingThread | null) {
      if (current?.threadKey !== next?.threadKey) {
        unregister();
        visibleSince = null;
        lastDispatchedWatermark = null;
        lastDispatchedAt = null;
        suppressed = false;
        if (next !== null) {
          const views = activeViews.get(next.threadKey) ?? new Set();
          views.add(suppress);
          activeViews.set(next.threadKey, views);
        }
      }
      current = next;
      if (!next?.visible) visibleSince = null;
      else visibleSince ??= Date.now();
      schedule();
    },
    setVisible(visible: boolean) {
      if (current === null) return;
      current = { ...current, visible };
      if (!visible) visibleSince = null;
      else visibleSince ??= Date.now();
      schedule();
    },
    dispose() {
      unregister();
      clearTimer();
      current = null;
      visibleSince = null;
    },
  };
}

// @effect-diagnostics globalDate:off globalTimers:off - Browser and native view lifecycles own this imperative timer outside an Effect runtime.
const THREAD_READ_DWELL_MS = 3_000;
// During an active turn the thread's updatedAt advances several times per
// second, and every server-side visit is a full command dispatch plus a
// broadcast to all shell subscribers.
const THREAD_VISIT_THROTTLE_MS = 10_000;

/**
 * The server's answer to one dispatched visit. Before retrying a rejected visit, the
 * controller calls its `restore`, so metadata the lost dispatch carried (an open) rides on
 * the retry and on nothing else.
 */
export type ThreadVisitOutcome =
  | { readonly accepted: true }
  | { readonly accepted: false; readonly restore?: () => void };

type ViewingThread = {
  threadKey: string;
  updatedAt: string;
  lastVisitedAt: string | null | undefined;
  completedAt?: string | null;
  visible: boolean;
  /** Counts the environment's connection sessions; a rejected visit retries once it changes. */
  sessionGeneration?: number;
  /** A returned answer lets a rejected visit retry; a local-only visit returns nothing. */
  onVisit: (updatedAt: string) => PromiseLike<ThreadVisitOutcome> | undefined;
};

type VisitDispatch = {
  readonly watermark: string;
  readonly generation: number | undefined;
};

type RejectedVisit = {
  readonly dispatch: VisitDispatch;
  readonly restore: (() => void) | undefined;
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
  // Only the newest dispatch of this viewing session may report a rejection.
  let latestDispatch: VisitDispatch | null = null;
  let rejected: RejectedVisit | null = null;
  // The newest visit the server has shown during this viewing session. A rejected visit it
  // already covers was accepted with a lost answer, so a later mark-unread rewind stays unread.
  let serverVisitedAtMs = Number.NEGATIVE_INFINITY;
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
  const observe = (lastVisitedAt: string | null | undefined) => {
    const visitedAtMs = lastVisitedAt ? Date.parse(lastVisitedAt) : NaN;
    if (visitedAtMs > serverVisitedAtMs) serverVisitedAtMs = visitedAtMs;
  };
  const serverShows = (watermark: string) => serverVisitedAtMs >= Date.parse(watermark);
  const settle = (dispatch: VisitDispatch, outcome: ThreadVisitOutcome) => {
    // Navigation, disposal or a newer dispatch makes the answer stale.
    if (dispatch !== latestDispatch || outcome.accepted) return;
    // Explicit unread holds for the rest of the viewing session, so nothing is retried.
    if (suppressed || serverShows(dispatch.watermark)) return;
    rejected = { dispatch, restore: outcome.restore };
    schedule();
  };
  const schedule = () => {
    clearTimer();
    if (current === null || !current.visible || visibleSince === null || suppressed) return;
    if (rejected !== null && serverShows(rejected.dispatch.watermark)) rejected = null;
    const retry = rejected;
    // A rejected visit waits for a new session, even while streaming moves the watermark.
    if (retry !== null && current.sessionGeneration === retry.dispatch.generation) return;
    const updatedAt = Date.parse(current.updatedAt);
    const visitedAt = current.lastVisitedAt ? Date.parse(current.lastVisitedAt) : NaN;
    if (
      !Number.isFinite(updatedAt) ||
      visitedAt >= updatedAt ||
      (retry === null && lastDispatchedWatermark === current.updatedAt)
    )
      return;
    const completedAt = current.completedAt ? Date.parse(current.completedAt) : NaN;
    const unseenCompletion =
      Number.isFinite(completedAt) && (!Number.isFinite(visitedAt) || completedAt > visitedAt);
    // A retry always waits out the throttle, so a flapping connection cannot repeat visits.
    const throttleUntil =
      lastDispatchedAt !== null && (retry !== null || !unseenCompletion)
        ? lastDispatchedAt + THREAD_VISIT_THROTTLE_MS
        : 0;
    const dueAt = Math.max(visibleSince + THREAD_READ_DWELL_MS, throttleUntil);
    const remaining = dueAt - Date.now();
    if (remaining > 0) {
      timer = setTimeout(schedule, remaining);
      return;
    }
    const dispatch: VisitDispatch = {
      watermark: current.updatedAt,
      generation: current.sessionGeneration,
    };
    latestDispatch = dispatch;
    rejected = null;
    lastDispatchedWatermark = current.updatedAt;
    lastDispatchedAt = Date.now();
    retry?.restore?.();
    void current.onVisit(current.updatedAt)?.then(
      (outcome) => settle(dispatch, outcome),
      () => settle(dispatch, { accepted: false }),
    );
  };

  return {
    update(next: ViewingThread | null) {
      if (current?.threadKey !== next?.threadKey) {
        unregister();
        visibleSince = null;
        lastDispatchedWatermark = null;
        lastDispatchedAt = null;
        suppressed = false;
        latestDispatch = null;
        rejected = null;
        serverVisitedAtMs = Number.NEGATIVE_INFINITY;
        if (next !== null) {
          const views = activeViews.get(next.threadKey) ?? new Set();
          views.add(suppress);
          activeViews.set(next.threadKey, views);
        }
      }
      current = next;
      if (next !== null) observe(next.lastVisitedAt);
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
      latestDispatch = null;
      rejected = null;
    },
  };
}

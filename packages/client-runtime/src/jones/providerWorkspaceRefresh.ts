// @effect-diagnostics globalDate:off globalTimers:off - Composer lifecycles own this timer outside an Effect runtime.
import { PROVIDER_WORKSPACE_SNAPSHOT_TTL_MS } from "@t3tools/contracts";

const RETRY_COOLDOWN_MS = 10_000;
const MAX_FAILURES = 3;
const MAX_WINDOWS = 64;

type RefreshInput = {
  readonly key: string;
  readonly hasSnapshot: boolean;
  readonly isCurrent: boolean;
  readonly retryPending: boolean;
  readonly refresh: () => Promise<boolean>;
  readonly onRetry: () => void;
};

type RefreshWindow = {
  expiresAt: number;
  failures: number;
  retryAt: number | null;
  succeeded: boolean;
  request: object | null;
};

export function providerWorkspaceRefreshKey(
  environmentId: string,
  instanceId: string,
  cwd: string,
): string {
  return JSON.stringify([environmentId, instanceId, cwd]);
}

/** Per-composer policy shared by web/desktop and mobile. */
export function createProviderWorkspaceRefresh() {
  const windows = new Map<string, RefreshWindow>();
  let current: RefreshInput | null = null;
  let timer: ReturnType<typeof setTimeout> | null = null;

  const clearTimer = () => {
    if (timer !== null) clearTimeout(timer);
    timer = null;
  };
  // A failure just before expiry still receives its full ten-second cooldown.
  const retainedUntil = (window: RefreshWindow) => Math.max(window.expiresAt, window.retryAt ?? 0);
  const prune = (now: number) => {
    for (const [key, window] of windows) {
      if (now >= retainedUntil(window)) windows.delete(key);
    }
  };
  const scheduleRetry = () => {
    clearTimer();
    if (!current?.retryPending || current.isCurrent) return;
    const now = Date.now();
    prune(now);
    const window = windows.get(current.key);
    let retryAt: number;
    if (window) {
      if (window.request || window.succeeded || window.failures >= MAX_FAILURES) return;
      if (window.retryAt === null) return;
      retryAt = window.retryAt;
    } else {
      // A different request may settle after expiry but before the overflow
      // timer runs. Preserve its wakeup when pruning has already freed a slot.
      retryAt =
        windows.size < MAX_WINDOWS ? now : Math.min(...Array.from(windows.values(), retainedUntil));
    }
    timer = setTimeout(
      () => {
        timer = null;
        current?.onRetry();
      },
      Math.max(0, retryAt - now),
    );
  };

  return {
    update(next: RefreshInput | null) {
      clearTimer();
      const now = Date.now();
      prune(now);
      // Only an observed loss within the same selection invalidates that key.
      // Switching from a complete A to an incomplete B must preserve A's budget.
      if (next && current?.key === next.key && current.hasSnapshot && !next.hasSnapshot) {
        windows.delete(next.key);
      }
      current = next;
      if (!next || next.isCurrent) return;
      let window = windows.get(next.key);
      if (!window) {
        // Do not evict live budgets to admit unbounded selection churn. At
        // capacity, a new key waits for a slot to expire; existing keys retain
        // their cap. No unbounded waiting-key list or fairness queue is kept.
        if (windows.size >= MAX_WINDOWS) {
          scheduleRetry();
          return;
        }
        window = {
          expiresAt: now + PROVIDER_WORKSPACE_SNAPSHOT_TTL_MS,
          failures: 0,
          retryAt: null,
          succeeded: false,
          request: null,
        };
        windows.set(next.key, window);
      }
      if (window.request || window.succeeded || window.failures >= MAX_FAILURES) return;
      if (window.retryAt !== null && now < window.retryAt) {
        scheduleRetry();
        return;
      }

      const request = {};
      const requestedAt = now;
      const requestedWindow = window;
      requestedWindow.request = request;
      const settle = (succeeded: boolean) => {
        const settledAt = Date.now();
        prune(settledAt);
        // Expiry, snapshot loss, or a newer request makes this result obsolete.
        if (windows.get(next.key) !== requestedWindow || requestedWindow.request !== request)
          return;
        requestedWindow.request = null;
        if (succeeded) {
          requestedWindow.failures = 0;
          requestedWindow.retryAt = null;
          requestedWindow.succeeded = true;
          // Retain the successful request's TTL guard against client/server
          // clock skew, while clearing only this key's failure budget.
          requestedWindow.expiresAt = requestedAt + PROVIDER_WORKSPACE_SNAPSHOT_TTL_MS;
        } else {
          requestedWindow.failures += 1;
          requestedWindow.retryAt = settledAt + RETRY_COOLDOWN_MS;
        }
        scheduleRetry();
      };
      try {
        void next.refresh().then(settle, () => settle(false));
      } catch {
        settle(false);
      }
    },
    // Effect cleanup pauses wakeups without forgetting budgets or invalidating
    // requests. React's effect replay can resume the same composer safely.
    pause() {
      current = null;
      clearTimer();
    },
  };
}

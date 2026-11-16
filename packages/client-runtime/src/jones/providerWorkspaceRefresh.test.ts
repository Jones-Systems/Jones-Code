// @effect-diagnostics globalTimers:off - Vitest's virtual clock exercises the composer-owned scheduler.
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import {
  createProviderWorkspaceRefresh,
  providerWorkspaceRefreshKey,
} from "./providerWorkspaceRefresh.js";

type RefreshInput = NonNullable<
  Parameters<ReturnType<typeof createProviderWorkspaceRefresh>["update"]>[0]
>;
const A = providerWorkspaceRefreshKey("environment", "provider", "/a");
const B = providerWorkspaceRefreshKey("environment", "provider", "/b");
const controllers: Array<ReturnType<typeof createProviderWorkspaceRefresh>> = [];
const advance = (ms: number) => vi.advanceTimersByTimeAsync(ms);

function harness() {
  const controller = createProviderWorkspaceRefresh();
  controllers.push(controller);
  let selected: RefreshInput | null = null;
  return {
    controller,
    select(
      key: string,
      refresh: () => Promise<boolean>,
      snapshot: Partial<Pick<RefreshInput, "hasSnapshot" | "isCurrent" | "retryPending">> = {},
    ) {
      selected = {
        key,
        hasSnapshot: false,
        isCurrent: false,
        retryPending: true,
        ...snapshot,
        refresh,
        onRetry: () => controller.update(selected),
      };
      controller.update(selected);
    },
  };
}

function deferred() {
  let resolve!: (value: boolean) => void;
  const promise = new Promise<boolean>((complete) => {
    resolve = complete;
  });
  return { promise, resolve };
}

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(0);
});
afterEach(() => {
  for (const controller of controllers) controller.pause();
  controllers.length = 0;
  vi.useRealTimers();
});

describe("provider workspace refresh windows", () => {
  it.each([true, false])(
    "holds A after three failures across B result %s and A reselection",
    async (bResult) => {
      const { select } = harness();
      const a = vi.fn(async () => false);
      const b = vi.fn(async () => bResult);
      select(A, a);
      await advance(0);
      await advance(9_999);
      expect(a).toHaveBeenCalledTimes(1);
      await advance(1);
      expect(a).toHaveBeenCalledTimes(2);
      await advance(10_000);
      expect(a).toHaveBeenCalledTimes(3);
      await advance(1_000);
      select(B, b);
      await advance(0);
      await advance(1_000);
      select(A, a);
      await advance(30_000);
      expect(a).toHaveBeenCalledTimes(3);
      expect(b).toHaveBeenCalledTimes(1);
    },
  );

  it("renews all three failures at the first request's five-minute boundary", async () => {
    const { select } = harness();
    const refresh = vi.fn(async () => false);
    select(A, refresh);
    await advance(0);
    await advance(20_000);
    expect(refresh).toHaveBeenCalledTimes(3);
    await advance(279_999);
    select(A, refresh);
    expect(refresh).toHaveBeenCalledTimes(3);
    await advance(1);
    select(A, refresh);
    await advance(0);
    expect(refresh).toHaveBeenCalledTimes(4);
    await advance(20_000);
    expect(refresh).toHaveBeenCalledTimes(6);
    await advance(20_000);
    select(A, refresh);
    expect(refresh).toHaveBeenCalledTimes(6);
  });

  it("settles a successful retry and retains that request's TTL against clock skew", async () => {
    const { select } = harness();
    const refresh = vi.fn(async () => false);
    select(A, refresh);
    await advance(0);
    await advance(10_000);
    refresh.mockResolvedValue(true);
    await advance(10_000);
    expect(refresh).toHaveBeenCalledTimes(3);
    // Keep presenting an out-of-date snapshot, as an ahead-of-server clock can.
    await advance(299_999);
    select(A, refresh);
    expect(refresh).toHaveBeenCalledTimes(3);
    await advance(1);
    refresh.mockResolvedValue(false);
    select(A, refresh);
    await advance(0);
    await advance(20_000);
    expect(refresh).toHaveBeenCalledTimes(6);
  });

  it("preserves the full cooldown when a failure lands just before the TTL boundary", async () => {
    const { select } = harness();
    const first = deferred();
    const refresh = vi.fn(async () => false).mockImplementationOnce(() => first.promise);
    select(A, refresh);
    await advance(299_000);
    first.resolve(false);
    await advance(0);
    await advance(1_000);
    select(A, refresh);
    expect(refresh).toHaveBeenCalledTimes(1);
    await advance(8_999);
    expect(refresh).toHaveBeenCalledTimes(1);
    await advance(1);
    expect(refresh).toHaveBeenCalledTimes(2);
    await advance(20_000);
    expect(refresh).toHaveBeenCalledTimes(4);
  });

  it.each([true, false])(
    "ignores an old window's late %s result after the new window reaches its cap",
    async (oldResult) => {
      const { select } = harness();
      const old = deferred();
      const oldRefresh = vi.fn(() => old.promise);
      const refresh = vi.fn(async () => false);
      select(A, oldRefresh);
      select(A, oldRefresh);
      expect(oldRefresh).toHaveBeenCalledTimes(1);
      await advance(300_000);
      select(A, refresh);
      await advance(0);
      await advance(20_000);
      expect(refresh).toHaveBeenCalledTimes(3);
      old.resolve(oldResult);
      await advance(0);
      select(A, refresh);
      await advance(20_000);
      expect(refresh).toHaveBeenCalledTimes(3);
    },
  );

  it("does not let an obsolete failure postpone a newer cooldown", async () => {
    const { select } = harness();
    const old = deferred();
    const refresh = vi.fn(async () => false);
    select(A, () => old.promise);
    await advance(300_000);
    select(A, refresh);
    await advance(0);
    await advance(5_000);
    old.resolve(false);
    await advance(0);
    await advance(4_999);
    expect(refresh).toHaveBeenCalledTimes(1);
    await advance(1);
    expect(refresh).toHaveBeenCalledTimes(2);
  });

  it("keeps overlapping keys' results and retry deadlines independent", async () => {
    const { select } = harness();
    const oldA = deferred();
    const a = vi.fn(() => oldA.promise);
    const b = vi.fn(async () => false);
    select(A, a);
    select(B, b);
    await advance(0);
    await advance(5_000);
    oldA.resolve(true);
    await advance(0);
    await advance(5_000);
    expect(b).toHaveBeenCalledTimes(2);
    await advance(10_000);
    expect(b).toHaveBeenCalledTimes(3);
    select(A, a);
    await advance(0);
    select(B, b);
    expect(a).toHaveBeenCalledTimes(1);
    expect(b).toHaveBeenCalledTimes(3);
  });

  it("only invalidates a complete-to-missing observation within the same selection", async () => {
    const { select } = harness();
    const a = vi.fn(async () => false);
    const b = vi.fn(async () => false);
    select(A, a);
    await advance(0);
    await advance(20_000);
    select(A, a, { hasSnapshot: true, isCurrent: true });
    select(B, b);
    await advance(0);
    select(A, a);
    expect(a).toHaveBeenCalledTimes(3);
    select(A, a, { hasSnapshot: true, isCurrent: true });
    select(A, a);
    expect(a).toHaveBeenCalledTimes(4);
    await advance(0);
    await advance(9_999);
    expect(a).toHaveBeenCalledTimes(4);
    await advance(1);
    expect(a).toHaveBeenCalledTimes(5);
  });

  it("ignores a pre-invalidation success while the replacement request is in flight", async () => {
    const { select } = harness();
    const old = deferred();
    const replacement = deferred();
    const refresh = vi.fn(() => replacement.promise);
    select(A, () => old.promise, { hasSnapshot: true });
    select(A, refresh);
    old.resolve(true);
    await advance(0);
    select(A, refresh);
    expect(refresh).toHaveBeenCalledTimes(1);
    replacement.resolve(false);
    await advance(0);
    await advance(10_000);
    expect(refresh).toHaveBeenCalledTimes(2);
  });

  it("bounds bookkeeping by deferring new keys instead of evicting live budgets", async () => {
    const { select } = harness();
    const a = vi.fn(async () => false);
    select(A, a);
    await advance(0);
    await advance(20_000);
    const admitted = vi.fn(async () => false);
    for (let index = 1; index < 64; index += 1) {
      select(providerWorkspaceRefreshKey("environment", "provider", `/cwd-${index}`), admitted);
      await advance(0);
    }
    expect(admitted).toHaveBeenCalledTimes(63);
    const overflow = vi.fn(async () => false);
    for (let index = 64; index < 128; index += 1) {
      select(providerWorkspaceRefreshKey("environment", "provider", `/cwd-${index}`), overflow);
    }
    expect(overflow).not.toHaveBeenCalled();
    select(A, a);
    expect(a).toHaveBeenCalledTimes(3);
    select(B, overflow);
    await advance(279_999);
    expect(overflow).not.toHaveBeenCalled();
    await advance(1);
    // The oldest record expires, so the currently selected overflow key wakes.
    expect(overflow).toHaveBeenCalledTimes(1);
    await advance(20_000);
    select(A, a);
    expect(a).toHaveBeenCalledTimes(4);
  });

  it("wakes an overflow selection when another key settles after capacity expires", async () => {
    const { select } = harness();
    select(A, async () => false);
    await advance(0);
    await advance(1_000);
    const late = deferred();
    select(B, () => late.promise);
    for (let index = 0; index < 62; index += 1) {
      select(
        providerWorkspaceRefreshKey("environment", "provider", "/held-" + index),
        async () => false,
      );
      await advance(0);
    }
    const overflow = vi.fn(async () => false);
    select(providerWorkspaceRefreshKey("environment", "provider", "/overflow"), overflow);
    expect(overflow).not.toHaveBeenCalled();
    // Model a suspended event loop: wall time advances before the admission
    // callback is serviced, and another still-valid request settles first.
    vi.setSystemTime(300_000);
    late.resolve(false);
    await Promise.resolve();
    await advance(0);
    expect(overflow).toHaveBeenCalledTimes(1);
    await advance(9_999);
    expect(overflow).toHaveBeenCalledTimes(1);
    await advance(1);
    expect(overflow).toHaveBeenCalledTimes(2);
  });

  it("pauses wakeups on cleanup and resumes the retained cooldown on effect replay", async () => {
    const { select, controller } = harness();
    const refresh = vi.fn(async () => false);
    select(A, refresh);
    await advance(0);
    await advance(1_000);
    controller.pause();
    await advance(5_000);
    expect(refresh).toHaveBeenCalledTimes(1);
    select(A, refresh);
    await advance(3_999);
    expect(refresh).toHaveBeenCalledTimes(1);
    await advance(1);
    expect(refresh).toHaveBeenCalledTimes(2);
    controller.update(null);
    await advance(20_000);
    expect(refresh).toHaveBeenCalledTimes(2);
  });

  it("counts thrown and rejected refreshes, with no automatic retry when commands are not pending", async () => {
    const { select } = harness();
    const refresh = vi
      .fn<() => Promise<boolean>>()
      .mockImplementationOnce(() => {
        throw new Error("dispatch failed");
      })
      .mockRejectedValue(new Error("refresh rejected"));
    const check = () => select(A, refresh, { retryPending: false });
    check();
    await advance(9_999);
    check();
    expect(refresh).toHaveBeenCalledTimes(1);
    await advance(1);
    check();
    await advance(0);
    await advance(10_000);
    check();
    await advance(0);
    await advance(10_000);
    check();
    expect(refresh).toHaveBeenCalledTimes(3);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("isolates all three scope components without delimiter collisions", async () => {
    const { select } = harness();
    const refresh = vi.fn(async () => true);
    const scopes = [
      ["a:b", "c", "/cwd"],
      ["a", "b:c", "/cwd"],
      ["other", "c", "/cwd"],
      ["a:b", "other", "/cwd"],
      ["a:b", "c", "/other"],
    ] as const;
    for (const [environment, instance, cwd] of scopes) {
      select(providerWorkspaceRefreshKey(environment, instance, cwd), refresh);
      await advance(0);
    }
    expect(refresh).toHaveBeenCalledTimes(5);
    select(providerWorkspaceRefreshKey(...scopes[0]), refresh);
    expect(refresh).toHaveBeenCalledTimes(5);
  });
});

import { describe, expect, it } from "vite-plus/test";

import { shouldRefreshOnArrival } from "~/hooks/useLiveRefresh";

describe("shouldRefreshOnArrival", () => {
  it.each([10_000, 59_000, 60_000])("uses the PR minimum interval at %i ms", (now) => {
    expect(
      shouldRefreshOnArrival({
        visible: true,
        now,
        lastRefreshedAt: 0,
        minimumIntervalMs: 60_000,
      }),
    ).toBe(now >= 60_000);
  });

  it("keeps other callers eligible after ten seconds", () => {
    expect(shouldRefreshOnArrival({ visible: true, now: 10_000, lastRefreshedAt: 0 })).toBe(true);
  });

  it("keeps a hidden PR quiet after its minimum interval", () => {
    expect(
      shouldRefreshOnArrival({
        visible: false,
        now: 60_000,
        lastRefreshedAt: 0,
        minimumIntervalMs: 60_000,
      }),
    ).toBe(false);
  });
});

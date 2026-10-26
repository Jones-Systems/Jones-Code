import { describe, expect, it } from "vite-plus/test";
import { monitorThrottleLease, setMonitorAwareBackgroundThrottling } from "./throttle.ts";

describe("visible monitor publisher throttle lease", () => {
  it("preserves first-reveal and preview policy changes without throttling a visible publisher", () => {
    let actual = false;
    const target = {
      getBackgroundThrottling: () => actual,
      setBackgroundThrottling: (enabled: boolean) => {
        actual = enabled;
      },
    };
    const lease = monitorThrottleLease(target);
    lease.visible(true);
    lease.normal(true);
    expect(actual).toBe(false);
    setMonitorAwareBackgroundThrottling(target, true);
    expect(actual).toBe(false);
    lease.visible(false);
    expect(actual).toBe(true);
    setMonitorAwareBackgroundThrottling(target, false);
    lease.visible(true);
    lease.visible(false);
    expect(actual).toBe(false);
    lease.dispose();
    setMonitorAwareBackgroundThrottling(target, true);
    expect(actual).toBe(true);
  });
});

import { describe, expect, it } from "vite-plus/test";
import type { ServerProviderUsageLimits } from "@t3tools/contracts";
import { providerShortcutLabelColor } from "./providerShortcutLabelColor";

function limits(...usedPercent: number[]): ServerProviderUsageLimits {
  return {
    checkedAt: "2026-10-20T15:00:00.000Z",
    windows: usedPercent.map((used, index) => ({
      id: `quota-${index}`,
      kind: "other",
      label: `Quota ${index}`,
      usedPercent: used,
    })),
  };
}

describe("provider shortcut capacity colors", () => {
  it.each([
    [100, "#72df9c"],
    [50, "#72df9c"],
    [49.6, "#f2d46c"],
    [25, "#f2d46c"],
    [24.6, "#ffa05e"],
    [10, "#ffa05e"],
    [9.6, "#ff747d"],
    [5, "#ff747d"],
    [0, "#ff747d"],
  ])("shows %s percent remaining as %s without rounding", (remaining, color) => {
    expect(providerShortcutLabelColor(limits(100 - remaining))).toBe(color);
  });

  it("uses the tightest quota rather than averaging or taking the first window", () => {
    expect(providerShortcutLabelColor(limits(5, 96, 40))).toBe("#ff747d");
    expect(providerShortcutLabelColor(limits(96, 40, 5))).toBe("#ff747d");
  });

  it("keeps unknown and unavailable usage neutral", () => {
    expect(providerShortcutLabelColor(undefined)).toBe("white");
    expect(providerShortcutLabelColor(limits())).toBe("white");
    for (const reason of ["unsupported", "probeFailed"] as const) {
      expect(providerShortcutLabelColor({ ...limits(0), unavailable: { reason } })).toBe("white");
    }
  });
});

import { describe, expect, it } from "vite-plus/test";
import { resolveJonesSourceCurrency } from "./sourceCurrency";

const installedSource = "a".repeat(40);
const targetSource = "b".repeat(40);

describe("Jones source currency", () => {
  it("recognizes the same source independently of platform artifact versions", () => {
    expect(resolveJonesSourceCurrency({ installedSource, targetSource: installedSource })).toBe("current");
  });

  it("does not guess order from different or missing source identities", () => {
    expect(resolveJonesSourceCurrency({ installedSource, targetSource })).toBe("unknown");
    expect(resolveJonesSourceCurrency({ targetSource })).toBe("unknown");
    expect(resolveJonesSourceCurrency({ installedSource: "preview.3801", targetSource })).toBe("unknown");
  });

  it.each([
    ["ahead", "behind"],
    ["behind", "ahead"],
    ["diverged", "diverged"],
    ["identical", "unknown"],
  ] as const)("uses a bound %s ancestry observation as %s", (relation, expected) => {
    expect(resolveJonesSourceCurrency({
      installedSource, targetSource,
      comparison: { base: installedSource, head: targetSource, relation },
    })).toBe(expected);
  });

  it("ignores ancestry evidence belonging to a different selection", () => {
    expect(resolveJonesSourceCurrency({
      installedSource, targetSource,
      comparison: { base: installedSource, head: "c".repeat(40), relation: "ahead" },
    })).toBe("unknown");
  });
});

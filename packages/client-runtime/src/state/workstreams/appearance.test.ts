import { describe, expect, it } from "vite-plus/test";
import {
  WORKSTREAM_COLOR_PRESETS,
  normalizeWorkstreamColor,
  workstreamAppearanceBorder,
} from "./appearance.ts";

describe("Workstream border color selection", () => {
  it("allows RGB extremes and normalizes custom hex without accepting CSS injection", () => {
    expect(normalizeWorkstreamColor(" #aBcDeF ")).toBe("#ABCDEF");
    expect(normalizeWorkstreamColor("#000000")).toBe("#000000");
    expect(normalizeWorkstreamColor("#FFFFFF")).toBe("#FFFFFF");
    for (const invalid of ["red", "#ABC", "#FFFFFFFF", "var(--background)", "url(example)"])
      expect(normalizeWorkstreamColor(invalid)).toBeNull();
  });
  it("offers the rainbow and neutrals without changing backgrounds or text", () => {
    for (const name of [
      "Red",
      "Orange",
      "Yellow",
      "Green",
      "Cyan",
      "Blue",
      "Indigo",
      "Violet",
      "Pink",
      "Brown",
      "Slate",
    ])
      expect(WORKSTREAM_COLOR_PRESETS.some(([label]) => label === name)).toBe(true);
    expect(workstreamAppearanceBorder("#123ABC")).toEqual({ borderLeftColor: "#123ABC" });
    expect(workstreamAppearanceBorder(null)).toBeUndefined();
    expect(workstreamAppearanceBorder("invalid")).toBeUndefined();
  });
});

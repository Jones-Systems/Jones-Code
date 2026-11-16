import { describe, expect, it } from "vite-plus/test";
import { personalWorkstreamRailColor } from "./personalRail";

describe("personal Workstream rails", () => {
  it("keeps identity colors through reorder, rename and repeated selection", () => {
    const items = ["work-a", "work-b", "work-c", "work-d"].map((id) => ({
      id,
      name: id,
      color: null,
    }));
    const original = new Map(items.map((item) => [item.id, personalWorkstreamRailColor(item)]));
    expect(new Set(original.values()).size).toBe(4);
    for (const item of items.toReversed()) {
      const renamed = { ...item, name: "Renamed" };
      expect(personalWorkstreamRailColor(renamed)).toBe(original.get(item.id));
      expect(personalWorkstreamRailColor(item)).toBe(original.get(item.id));
    }
  });

  it("preserves an explicit hex selection and restores the identity color when cleared", () => {
    const item = { id: "work-a", color: null };
    const automatic = personalWorkstreamRailColor(item);
    expect(personalWorkstreamRailColor({ ...item, color: "#AABBCC" })).toBe(
      "color-mix(in srgb, #AABBCC 28%, var(--sidebar))",
    );
    expect(personalWorkstreamRailColor({ id: "work-b", color: "#AABBCC" })).toBe(
      personalWorkstreamRailColor({ ...item, color: "#AABBCC" }),
    );
    expect(personalWorkstreamRailColor(item)).toBe(automatic);
  });
});

import { describe, expect, it } from "vite-plus/test";
import { personalWorkstreamGroupColors } from "./personalRail";

describe("personal Workstream group colors", () => {
  it("keeps identity colors through reorder, rename and repeated selection", () => {
    const items = ["work-a", "work-b", "work-c", "work-d"].map((id) => ({
      id,
      name: id,
      color: null,
    }));
    const original = new Map(items.map((item) => [item.id, personalWorkstreamGroupColors(item)]));
    expect(new Set([...original.values()].map(({ rail }) => rail)).size).toBe(4);
    expect(new Set([...original.values()].map(({ tint }) => tint)).size).toBe(4);
    for (const item of items.toReversed()) {
      const renamed = { ...item, name: "Renamed" };
      expect(personalWorkstreamGroupColors(renamed)).toEqual(original.get(item.id));
      expect(personalWorkstreamGroupColors(item)).toEqual(original.get(item.id));
    }
  });

  it("preserves an explicit hex selection and restores the identity color when cleared", () => {
    const item = { id: "work-a", color: null };
    const automatic = personalWorkstreamGroupColors(item);
    const custom = personalWorkstreamGroupColors({ ...item, color: "#AABBCC" });
    expect(custom).toEqual({
      rail: "color-mix(in srgb, #AABBCC 70%, var(--sidebar))",
      tint: "color-mix(in srgb, #AABBCC 8%, transparent)",
    });
    expect(personalWorkstreamGroupColors({ id: "work-b", color: "#AABBCC" })).toEqual(custom);
    expect(personalWorkstreamGroupColors(item)).toEqual(automatic);
  });
});

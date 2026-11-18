import { describe, expect, it } from "vite-plus/test";
import {
  PERSONAL_WORKSTREAM_PALETTE,
  personalWorkstreamAutomaticColors,
  personalWorkstreamGroupColors,
} from "./personalRail";

describe("personal Workstream group colors", () => {
  it("keeps identity colors through reorder, rename and repeated selection", () => {
    const items = PERSONAL_WORKSTREAM_PALETTE.map((_, index) => ({
      id: `work-${index}`,
      name: `Workstream ${index}`,
      color: null,
    }));
    const original = personalWorkstreamAutomaticColors(items);
    expect(new Set(original.values()).size).toBe(PERSONAL_WORKSTREAM_PALETTE.length);
    const changed = items
      .toReversed()
      .map((item) => ({ ...item, name: "Renamed", color: "#ABCDEF" }));
    expect(personalWorkstreamAutomaticColors(changed)).toEqual(original);
  });

  it("continues assigning palette colors when the number of groups exceeds the palette", () => {
    const items = Array.from({ length: PERSONAL_WORKSTREAM_PALETTE.length + 7 }, (_, index) => ({
      id: String(index),
    }));
    const assigned = personalWorkstreamAutomaticColors(items);
    expect(assigned.size).toBe(items.length);
    expect(new Set(assigned.values())).toEqual(
      new Set(PERSONAL_WORKSTREAM_PALETTE.map(({ color }) => color)),
    );
  });

  it("preserves an explicit hex selection and restores the identity color when cleared", () => {
    const item = { id: "work-a", color: null };
    const assigned = personalWorkstreamAutomaticColors([item]).get(item.id);
    const automatic = personalWorkstreamGroupColors(item, assigned);
    const custom = personalWorkstreamGroupColors({ ...item, color: "#AABBCC" }, assigned);
    expect(custom).toEqual({
      color: "#AABBCC",
      automaticColor: automatic.color,
      rail: "color-mix(in srgb, #AABBCC 70%, var(--sidebar))",
    });
    expect(personalWorkstreamGroupColors(item).color).toBe(custom.automaticColor);
  });
});

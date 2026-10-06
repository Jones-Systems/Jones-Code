import { describe, expect, it } from "vite-plus/test";

import { WORKSTREAM_TINT_PALETTE, workstreamPaletteIndex, workstreamTint } from "./colors.ts";

describe("stable Workstream colors", () => {
  it("preserves the web palette assignment for persisted Workstream identities", () => {
    expect(workstreamPaletteIndex("workstream-a")).toBe(2);
    expect(workstreamPaletteIndex("workstream-b")).toBe(3);
    expect(workstreamPaletteIndex("owner:delivery")).toBe(0);
    expect(workstreamTint("workstream-a")).toBe(
      "border-emerald-200 bg-emerald-50/70 dark:border-emerald-800/60 dark:bg-emerald-950/25",
    );
    expect(workstreamTint("workstream-b")).toBe(
      "border-amber-200 bg-amber-50/70 dark:border-amber-800/60 dark:bg-amber-950/25",
    );
  });

  it("keeps identity colors when names and owner order change", () => {
    const before = [
      { id: "workstream-a", name: "First", order: 0 },
      { id: "workstream-b", name: "Second", order: 1 },
    ];
    const after = [
      { id: "workstream-b", name: "Renamed second", order: 0 },
      { id: "workstream-a", name: "Renamed first", order: 1 },
    ];
    const assignments = (items: typeof before) =>
      Object.fromEntries(items.map(({ id }) => [id, workstreamTint(id)]));
    expect(assignments(after)).toEqual(assignments(before));
    for (const { id } of after) {
      expect(WORKSTREAM_TINT_PALETTE[workstreamPaletteIndex(id)]).toBe(workstreamTint(id));
    }
  });
});

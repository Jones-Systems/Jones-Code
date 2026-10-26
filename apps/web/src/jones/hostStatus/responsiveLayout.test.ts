import { describe, expect, it } from "vite-plus/test";
import { fitHostStatus, HOST_DISPLAY_STAGES, hostDisplayValues } from "./responsiveLayout";

const measurements = ["VPS", "Test", "Mini", "Laptop"].map(() => ({
  name: (name: string, inline: boolean) => name.length * 7 + (inline ? 7 : 0),
  cpu: (unit: boolean) => (unit ? 21 : 14),
  ram: (unit: boolean) => (unit ? 56 : 21),
  dot: 4,
}));

describe("responsive host readings", () => {
  it("removes only units from readings and keeps lowercase mb/s at full width", () => {
    expect(hostDisplayValues("26%", "0.0 mb/s", HOST_DISPLAY_STAGES[0]!, 3)).toEqual({
      cpu: "26%",
      ram: "0.0 mb/s",
    });
    expect(hostDisplayValues("26%", "0.0 mb/s", HOST_DISPLAY_STAGES.at(-1)!, 3)).toEqual({
      cpu: "26",
      ram: "0.0",
    });
    expect(hostDisplayValues("N/A", "N/A mb/s", HOST_DISPLAY_STAGES.at(-1)!, 3)).toEqual({
      cpu: "N/A",
      ram: "N/A",
    });
    expect(hostDisplayValues("26%", "42", HOST_DISPLAY_STAGES[0]!, 0).ram).toBe("42");
  });
  it("uses only the accepted names and changes at most one host name at each step", () => {
    const names = HOST_DISPLAY_STAGES.map((stage) => stage.names);
    for (const [index, allowed] of [
      [0, ["VPS", "V", ""]],
      [1, ["Test", "T", ""]],
      [2, ["Mini", "M", ""]],
      [3, ["Laptop", "Lap", "L", ""]],
    ] as const) {
      expect([...new Set(names.map((name) => name[index]))]).toEqual([...allowed]);
    }
    for (let i = 1; i < names.length; i++)
      expect(names[i]!.filter((name, j) => name !== names[i - 1]![j]).length).toBeLessThanOrEqual(
        1,
      );
  });
  it("passes through diagonal pairs before vertical pairs and removes names after stacking", () => {
    for (let i = 0; i < 4; i++) {
      const stages = HOST_DISPLAY_STAGES.map((stage) => stage.layouts[i]);
      expect(stages.indexOf("diagonal")).toBeLessThan(stages.indexOf("vertical"));
      expect(HOST_DISPLAY_STAGES.find((stage) => stage.names[i] === "")?.layouts[i]).toBe(
        "vertical",
      );
    }
  });
  it("fits the full set, reduces progressively, and restores all detail without stretching spacing", () => {
    const wide = fitHostStatus(1000, measurements)!;
    expect(wide).toEqual({ stage: 0, padding: 8, gap: 8, scale: 1 });
    let previous = 0;
    const seen = new Set<number>();
    for (let width = 650; width >= 100; width--) {
      const fit = fitHostStatus(width, measurements);
      if (!fit) continue;
      expect(fit.stage).toBeGreaterThanOrEqual(previous);
      expect(fit.padding).toBeGreaterThanOrEqual(3);
      expect(fit.padding).toBeLessThanOrEqual(8);
      expect(fit.gap).toBeLessThanOrEqual(8);
      expect(fit.scale).toBeGreaterThanOrEqual(10 / 12);
      seen.add(fit.stage);
      previous = fit.stage;
    }
    expect(seen.size).toBeGreaterThan(15);
    expect(fitHostStatus(30, measurements)).toBeNull();
    expect(fitHostStatus(1000, measurements)).toEqual(wide);
  });
});

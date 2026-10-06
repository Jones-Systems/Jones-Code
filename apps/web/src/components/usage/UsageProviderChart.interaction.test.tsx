// @vitest-environment jsdom
import { enumerateHourStarts } from "@t3tools/shared/usageFormat";
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

import { UsageProviderChart } from "./UsageProviderChart";

describe("single-bucket usage chart", () => {
  let renderer: Root;
  let container: HTMLDivElement;

  beforeEach(() => {
    vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
    container = document.createElement("div");
    document.body.append(container);
    renderer = createRoot(container);
  });

  afterEach(async () => {
    await act(() => renderer.unmount());
    container.remove();
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });

  it.each([
    { minutes: 60, metric: "tokens" as const, values: ["100", "50", "150"] },
    { minutes: 30, metric: "cost" as const, values: ["$4.00", "$2.00", "$6.00"] },
  ])(
    "plots and reads the actual bucket for a $minutes-minute $metric range",
    async ({ minutes, metric, values }) => {
      const hourStart = "2026-09-18T12:00:00.000Z";
      const hours = enumerateHourStarts(
        hourStart,
        new Date(Date.parse(hourStart) + minutes * 60 * 1000).toISOString(),
      );
      expect(hours).toEqual([hourStart]);
      await act(() =>
        renderer.render(
          <UsageProviderChart
            providers={["codex", "claude"]}
            days={[]}
            daily={[]}
            hours={hours}
            hourly={[
              {
                day: "2026-09-18",
                hourStart,
                costUsd: 6,
                totalTokens: 150,
                byProvider: new Map([
                  ["codex", { costUsd: 4, totalTokens: 100 }],
                  ["claude", { costUsd: 2, totalTokens: 50 }],
                ]),
              },
            ]}
            metric={metric}
            referenceTime={undefined}
            resolution="hour"
            timeZone="UTC"
          />,
        ),
      );

      const svg = container.querySelector("svg")!;
      const [, , width, height] = svg.getAttribute("viewBox")!.split(" ").map(Number);
      const lines = [...svg.querySelectorAll('path[fill="none"]')].map((path) => {
        const points = [
          ...path.getAttribute("d")!.matchAll(/(-?\d+(?:\.\d+)?),(-?\d+(?:\.\d+)?)/g),
        ].map(([, x, y]) => ({ x: Number(x), y: Number(y) }));
        expect(points.length, "a nonzero bucket must draw a visible line").toBeGreaterThan(1);
        expect(points.at(-1)!.x - points[0]!.x).toBe(width);
        for (const point of points) expect(point.y).toBe(points[0]!.y);
        expect(points[0]!.y).toBeLessThan(height!);
        return points;
      });
      expect(lines).toHaveLength(2);
      expect((height! - lines[0]![0]!.y) / (height! - lines[1]![0]!.y)).toBeCloseTo(2);
      for (const area of svg.querySelectorAll('path:not([fill="none"])')) {
        expect(area.getAttribute("d")).toMatch(/Z$/);
      }

      const plot = svg.parentElement!;
      vi.spyOn(plot, "getBoundingClientRect").mockReturnValue(new DOMRect(0, 0, 960, 260));
      await act(() =>
        plot.dispatchEvent(
          new MouseEvent("mousemove", { bubbles: true, clientX: 480, clientY: 60 }),
        ),
      );
      expect(plot.textContent).toContain("Codex");
      expect(plot.textContent).toContain("Claude Code");
      expect(plot.textContent).toContain("Total");
      for (const value of values) expect(plot.textContent).toContain(value);
    },
  );
});

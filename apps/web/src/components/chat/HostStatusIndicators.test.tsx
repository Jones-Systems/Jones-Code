// @vitest-environment jsdom
import type { HostStatusSnapshot } from "@t3tools/contracts";
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";

import { observeHostStatus } from "../../hostStatus";
import { TooltipProvider } from "../ui/tooltip";
import { HostStatusIndicators } from "./HostStatusIndicators";

vi.mock("../../hostStatus", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hostStatus")>()),
  observeHostStatus: vi.fn(),
}));

let container: HTMLDivElement;
let root: Root;
let receive: (snapshot: HostStatusSnapshot | null) => void;
let availableWidth: number;
let bubbleWidths: number[];
let resize: () => void;
let observerDisconnected: boolean;

beforeEach(async () => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  availableWidth = 1000;
  bubbleWidths = [100, 110, 120, 130];
  observerDisconnected = false;
  vi.stubGlobal(
    "ResizeObserver",
    class {
      constructor(callback: () => void) {
        resize = callback;
      }
      observe() {}
      disconnect() {
        observerDisconnected = true;
      }
    },
  );
  const originalRect = HTMLElement.prototype.getBoundingClientRect;
  vi.spyOn(HTMLElement.prototype, "getBoundingClientRect").mockImplementation(function (
    this: HTMLElement,
  ) {
    if (this.getAttribute("aria-label") === "Host status") {
      return new DOMRect(0, 0, availableWidth, 32);
    }
    if (this.parentElement?.hasAttribute("data-host-status-measurement")) {
      const index = Array.from(this.parentElement.children).indexOf(this);
      const left = bubbleWidths.slice(0, index).reduce((sum, width) => sum + width + 8, 0);
      return new DOMRect(left, 0, bubbleWidths[index] ?? 0, 32);
    }
    return originalRect.call(this);
  });
  vi.mocked(observeHostStatus).mockImplementation((_visibility, listener) => {
    receive = listener;
    return () => {};
  });
  container = document.createElement("div");
  document.body.appendChild(container);
  root = createRoot(container);
  await act(async () => {
    root.render(
      <TooltipProvider>
        <HostStatusIndicators />
      </TooltipProvider>,
    );
  });
});

afterEach(async () => {
  await act(async () => root.unmount());
  expect(observerDisconnected).toBe(true);
  container.remove();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
  vi.clearAllMocks();
});

it("keeps one bubble per host while live samples update text, CPU health, and accessible detail", async () => {
  const bubbles = () => Array.from(container.querySelectorAll('[role="img"]'));
  expect(bubbles().map((bubble) => bubble.textContent)).toEqual([
    "VPS · CPU — · RAM —",
    "Test · CPU — · RAM —",
    "Mini · CPU — · RAM —",
    "Home · CPU — · RAM —",
  ]);
  expect(bubbles().every((bubble) => bubble.className.includes("bg-muted"))).toBe(true);
  await act(async () =>
    receive({
      hosts: [
        {
          id: "vps",
          status: "available",
          cpuUsagePercent: 23,
          logicalCpuCount: 16,
          occupiedMemoryBytes: 12 * 1024 ** 3,
          totalMemoryBytes: 16 * 1024 ** 3,
          sampledAt: new Date().toISOString(),
        },
      ],
    }),
  );
  expect(bubbles()).toHaveLength(4);
  expect(bubbles()[0]?.textContent).toBe("VPS · CPU 23% · RAM 12/16 GiB");
  expect(bubbles()[0]?.className).toContain("bg-emerald-500/10");
  expect(bubbles()[0]?.getAttribute("aria-label")).toContain("CPU 23%");
  expect(bubbles()[0]?.getAttribute("aria-label")).toContain("Occupied RAM 12/16 GiB");
  expect(bubbles()[0]?.getAttribute("aria-label")).toContain("includes reclaimable cache");
  expect(bubbles()[0]?.getAttribute("aria-label")).toContain("not memory pressure");
  expect(bubbles()[1]?.textContent).toBe("Test · CPU — · RAM —");
  await act(async () => receive(null));
  expect(bubbles()[0]?.textContent).toBe("VPS · CPU — · RAM —");
  await act(async () =>
    receive({ hosts: [{ id: "vps", status: "unavailable", reason: "stale" }] }),
  );
  expect(bubbles()[0]?.textContent).toBe("VPS · CPU — · RAM —");
  expect(bubbles()[0]?.className).toContain("bg-muted");
  expect(bubbles()[0]?.getAttribute("aria-label")).toContain("stale");
});

it("removes complete bubbles from the right as available header space shrinks and restores them", async () => {
  const names = () =>
    Array.from(container.querySelectorAll('[role="img"]')).map(
      (bubble) => bubble.textContent?.split(" · ")[0],
    );
  expect(names()).toEqual(["VPS", "Test", "Mini", "Home"]);
  for (const [width, expected] of [
    [353, ["VPS", "Test", "Mini"]],
    [225, ["VPS", "Test"]],
    [217, ["VPS"]],
    [99, []],
    [100, ["VPS"]],
    [218, ["VPS", "Test"]],
    [346, ["VPS", "Test", "Mini"]],
    [484, ["VPS", "Test", "Mini", "Home"]],
  ] as const) {
    await act(async () => {
      availableWidth = width;
      resize();
    });
    expect(names()).toEqual(expected);
    expect(container.querySelectorAll('[tabindex="0"]')).toHaveLength(expected.length);
  }
  expect(
    container.querySelector("[data-host-status-measurement]")?.querySelector("[tabindex]"),
  ).toBeNull();
});

it("recalculates fitting bubbles when live metric text changes width without a viewport resize", async () => {
  const names = () =>
    Array.from(container.querySelectorAll('[role="img"]')).map(
      (bubble) => bubble.textContent?.split(" · ")[0],
    );
  await act(async () => {
    availableWidth = 346;
    resize();
  });
  expect(names()).toEqual(["VPS", "Test", "Mini"]);
  await act(async () => {
    receive({
      hosts: [
        {
          id: "vps",
          status: "available",
          cpuUsagePercent: 100,
          logicalCpuCount: 16,
          occupiedMemoryBytes: 128 * 1024 ** 3,
          totalMemoryBytes: 256 * 1024 ** 3,
          sampledAt: new Date().toISOString(),
        },
      ],
    });
  });
  expect(container.querySelector("[data-host-status-measurement]")?.textContent).toContain(
    "VPS · CPU 100% · RAM 128/256 GiB",
  );
  await act(async () => {
    bubbleWidths = [180, 110, 120, 130];
    resize();
  });
  expect(names()).toEqual(["VPS", "Test"]);
  await act(async () => {
    receive(null);
  });
  await act(async () => {
    bubbleWidths = [100, 110, 120, 130];
    resize();
  });
  expect(names()).toEqual(["VPS", "Test", "Mini"]);
});

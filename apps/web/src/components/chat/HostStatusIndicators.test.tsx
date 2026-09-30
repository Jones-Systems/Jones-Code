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

beforeEach(async () => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
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
  container.remove();
  vi.unstubAllGlobals();
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

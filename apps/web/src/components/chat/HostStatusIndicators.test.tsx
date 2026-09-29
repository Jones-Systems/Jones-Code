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

it("keeps one bubble per host while live samples update text, worst health, and accessible detail", async () => {
  const bubbles = () => Array.from(container.querySelectorAll('[role="img"]'));
  expect(bubbles().map((bubble) => bubble.textContent)).toEqual([
    "VPS · — · — GiB",
    "Test · — · — GiB",
    "Mini · — · — GiB",
    "Home · — · — GiB",
  ]);
  expect(bubbles().every((bubble) => bubble.className.includes("bg-muted"))).toBe(true);
  await act(async () =>
    receive({
      hosts: [
        {
          id: "vps",
          status: "available",
          load1: 17,
          logicalCpuCount: 16,
          availableMemoryBytes: 44 * 1024 ** 3,
          totalMemoryBytes: 64 * 1024 ** 3,
          sampledAt: "2026-09-29T12:00:00.000Z",
        },
      ],
    }),
  );
  expect(bubbles()).toHaveLength(4);
  expect(bubbles()[0]?.textContent).toBe("VPS · 17 · 44 GiB");
  expect(bubbles()[0]?.className).toContain("bg-red-500/15");
  expect(bubbles()[0]?.getAttribute("aria-label")).toContain("16 logical CPUs");
  expect(bubbles()[0]?.getAttribute("aria-label")).toContain("Available RAM 44.0 GiB");
  await act(async () =>
    receive({ hosts: [{ id: "vps", status: "unavailable", reason: "stale" }] }),
  );
  expect(bubbles()[0]?.textContent).toBe("VPS · — · — GiB");
  expect(bubbles()[0]?.className).toContain("bg-muted");
  expect(bubbles()[0]?.getAttribute("aria-label")).toContain("stale");
});

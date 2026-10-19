import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";
import "~/index.css";
import { CompactSidebarHeader } from "./CompactSidebarHeader";
import { ThreadStatusFilterPill } from "../threadStatusFilter/ThreadStatusFilterPill";

let root: Root;
let container: HTMLDivElement;
beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  container = document.createElement("div");
  container.style.width = "500px";
  document.body.append(container);
  root = createRoot(container);
});
afterEach(async () => {
  await act(async () => root.unmount());
  container.remove();
  vi.unstubAllGlobals();
});

function Fixture({ large = false }: { large?: boolean }) {
  return (
    <CompactSidebarHeader>
      <a
        data-sidebar-brand
        href="#threads"
        style={{ marginLeft: 52 }}
        className="inline-flex h-7 shrink-0 items-center gap-1 text-sm"
      >
        <span>J3</span>
        <span data-sidebar-brand-code>Code</span>
      </a>
      <ThreadStatusFilterPill
        counts={
          large
            ? { working: 999999, input: 888888, done: 777777 }
            : { working: 1234, input: 56, done: 789 }
        }
        filter="all"
        onCycle={() => {}}
      />
      <button type="button" className="h-6 shrink-0 rounded-full px-2 text-xs">
        Work
      </button>
      <span className="shrink-0 text-xs">Preview</span>
    </CompactSidebarHeader>
  );
}
const frame = () => new Promise<void>((resolve) => requestAnimationFrame(() => resolve()));

it("compresses at actual sidebar widths, keeps Work in the row, and restores the brand on expansion", async () => {
  await act(async () => root.render(<Fixture />));
  const row = container.firstElementChild as HTMLElement;
  const densities = new Set<string>();
  for (let width = 400; width >= 160; width -= 2) {
    container.style.width = `${width}px`;
    await act(async () => {
      await frame();
      await frame();
    });
    densities.add(row.dataset.density!);
    const work = Array.from(container.querySelectorAll("button")).find(
      (button) => button.textContent === "Work",
    )!;
    const bounds = row.getBoundingClientRect();
    expect(work.getBoundingClientRect().right).toBeLessThanOrEqual(bounds.right);
    expect(work.getBoundingClientRect().bottom).toBeLessThanOrEqual(bounds.bottom);
    expect(work.getBoundingClientRect().height).toBeGreaterThanOrEqual(24);
    if (row.dataset.density === "minimal") break;
  }
  expect([...densities]).toEqual(["full", "code-hidden", "third", "half", "quarter", "minimal"]);
  container.style.width = "500px";
  await expect.poll(() => row.dataset.density).toBe("full");
  expect(getComputedStyle(container.querySelector("[data-sidebar-brand-code]")!).display).not.toBe(
    "none",
  );
});

it("refits changed large counts and an environment label without clipping controls", async () => {
  container.style.width = "320px";
  await act(async () => root.render(<Fixture />));
  await act(async () => root.render(<Fixture large />));
  const row = container.firstElementChild as HTMLElement;
  await expect.poll(() => row.scrollWidth <= row.clientWidth).toBe(true);
  expect(container.textContent).toContain("999999");
  expect(container.textContent).toContain("Preview");
  expect(container.querySelector("button")!.getAttribute("aria-label")).toContain("999999 working");
});

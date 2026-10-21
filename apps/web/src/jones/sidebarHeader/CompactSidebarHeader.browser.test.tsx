import { act, render as renderTest, type Screen } from "../../../test/render";

import { expect, it, vi } from "vite-plus/test";
import "~/index.css";
import { CompactSidebarHeader } from "./CompactSidebarHeader";
import { ThreadStatusFilterPill } from "../threadStatusFilter/ThreadStatusFilterPill";

let resizeRevision = 0;
let root: Screen;
let container: HTMLDivElement;
async function setupTest(): Promise<AsyncDisposable> {
  let initialized = false;
  let ownedContainer: HTMLDivElement | undefined;
  async function cleanup() {
    const screen = initialized ? root : undefined;
    try {
  await act(async () => screen?.unmount());
    } finally {
      try {
        await screen?.[Symbol.asyncDispose]();
      } finally {
        ownedContainer?.remove();
        vi.unstubAllGlobals();
      }
    }
  }
  try {
  resizeRevision = 0;
  const NativeResizeObserver = globalThis.ResizeObserver;
  vi.stubGlobal("ResizeObserver", class extends NativeResizeObserver {
    constructor(callback: ResizeObserverCallback) {
      super((entries, observer) => {
        callback(entries, observer);
        resizeRevision += 1;
      });
    }
  });
  ownedContainer = container = document.createElement("div");
  container.style.width = "500px";
  document.body.append(container);
  root = await renderTest(null, { container: container, router: false });
  initialized = true;

    return { [Symbol.asyncDispose]: cleanup };
  } catch (error) {
    await cleanup();
    throw error;
  }
}


function Fixture({ large = false, label = "Preview" }: { large?: boolean; label?: string }) {
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
      <span className="shrink-0 text-xs">{label}</span>
    </CompactSidebarHeader>
  );
}
const waitForResize = (previous: number) =>
  vi.waitFor(() => expect(resizeRevision).toBeGreaterThan(previous), { interval: 1 });

it("scales smoothly at actual sidebar widths, keeps Work in the row, and restores the brand on expansion", async () => {
    await using _fixture = await setupTest();
  await act(async () => await root.rerender(<Fixture />));
  const row = container.firstElementChild as HTMLElement;
  const densities = new Set<string>();
  const factors: number[] = [];
  const brand = container.querySelector("[data-sidebar-brand]")!;
  const code = container.querySelector("[data-sidebar-brand-code]")!;
  for (let width = 400; width >= 160; width -= 2) {
    const previousResize = resizeRevision;
    container.style.width = `${width}px`;
    await waitForResize(previousResize);
    densities.add(row.dataset.density!);
    const factor = Number(row.style.getPropertyValue("--sidebar-header-spacing"));
    expect(factor).toBeGreaterThanOrEqual(0.25);
    expect(factor).toBeLessThanOrEqual(1);
    if (row.dataset.density === "continuous") {
      factors.push(factor);
      expect(getComputedStyle(brand).display).not.toBe("none");
      expect(getComputedStyle(code).display).toBe("none");
      expect(row.scrollWidth).toBeLessThanOrEqual(row.clientWidth);
    } else if (row.dataset.density === "minimal") {
      expect(factor).toBe(0.25);
      expect(getComputedStyle(brand).display).toBe("none");
    } else {
      expect(factor).toBe(1);
    }
    const work = Array.from(container.querySelectorAll("button")).find(
      (button) => button.textContent === "Work",
    )!;
    const bounds = row.getBoundingClientRect();
    expect(work.getBoundingClientRect().right).toBeLessThanOrEqual(bounds.right);
    expect(work.getBoundingClientRect().bottom).toBeLessThanOrEqual(bounds.bottom);
    expect(work.getBoundingClientRect().height).toBeGreaterThanOrEqual(24);
    if (row.dataset.density === "minimal") break;
  }
  expect([...densities]).toEqual(["full", "code-hidden", "continuous", "minimal"]);
  expect(factors.length).toBeGreaterThan(10);
  expect(factors[0]).toBeGreaterThan(0.9);
  expect(factors.at(-1)).toBeLessThan(0.3);
  const step = factors[0]! - factors[1]!;
  for (let index = 1; index < factors.length; index++) {
    expect(factors[index]).toBeLessThan(factors[index - 1]!);
    if (factors[index]! > 0.25) expect(factors[index - 1]! - factors[index]!).toBeCloseTo(step, 4);
  }
  container.style.width = "500px";
  await expect.poll(() => row.dataset.density).toBe("full");
  expect(getComputedStyle(container.querySelector("[data-sidebar-brand-code]")!).display).not.toBe(
    "none",
  );
});

it("refits changed large counts and an environment label without clipping controls", async () => {
    await using _fixture = await setupTest();
  container.style.width = "320px";
  await act(async () => await root.rerender(<Fixture />));
  const previousResize = resizeRevision;
  await act(async () => await root.rerender(<Fixture large label="Preview EU" />));
  const row = container.firstElementChild as HTMLElement;
  await expect.poll(() => row.scrollWidth <= row.clientWidth).toBe(true);
  expect(container.textContent).toContain("999999");
  expect(container.textContent).toContain("Preview EU");
  expect(container.querySelector("button")!.getAttribute("aria-label")).toContain("999999 working");
  const factor = row.style.getPropertyValue("--sidebar-header-spacing");
  const density = row.dataset.density;
  await waitForResize(previousResize);
  expect(row.style.getPropertyValue("--sidebar-header-spacing")).toBe(factor);
  expect(row.dataset.density).toBe(density);
  await act(async () => await root.rerender(<Fixture />));
  await expect.poll(() => row.dataset.density).not.toBe("minimal");
});

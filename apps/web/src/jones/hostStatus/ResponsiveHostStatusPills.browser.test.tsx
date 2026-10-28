import "../../index.css";
import { expect, it, vi } from "vite-plus/test";
import { userEvent } from "vite-plus/test/browser";
import { act, render } from "../../../test/render";
import { hostStatusMetrics } from "../../hostStatus";
import { TooltipProvider } from "../../components/ui/tooltip";
import { ResponsiveHostStatusPills, type HostStatusDisplay } from "./ResponsiveHostStatusPills";

const hosts: HostStatusDisplay[] = ["VPS", "Test", "Mini", "Laptop"].map((name, index) => ({
  id: ["vps", "test", "mini", "home"][index]!,
  name,
  label: `${name}: CPU 26%; original complete readings`,
  metrics: {
    cpu: index === 1 ? "0%" : "26%",
    ram: index > 1 ? "0.0 mb/s" : "42",
    health: "healthy",
    cpuHealth: "healthy",
    ramHealth: "healthy",
    tooltipLines: [
      `CPU ${index === 1 ? "0%" : "26%"} of total cores`,
      "RAM 42 GiB available of 64 GiB",
      ...(index > 1 ? ["Swap I/O 0.0 MiB/s"] : []),
      "Storage unavailable",
    ],
  },
}));

it("keeps the original wide formatting and left alignment without spreading spare space", async () => {
  const container = document.createElement("div");
  container.style.width = "1000px";
  await using _screen = await render(
    <TooltipProvider>
      <ResponsiveHostStatusPills hosts={hosts} />
    </TooltipProvider>,
    { container, router: false },
  );
  const group = container.querySelector<HTMLElement>('[aria-label="Host status"]')!;
  const pills = () => [...container.querySelectorAll<HTMLElement>('[role="img"]')];
  await expect.poll(() => pills().length).toBe(4);
  expect(pills().map((p) => p.textContent)).toEqual([
    "VPS · 26% · 42",
    "Test · 0% · 42",
    "Mini · 26% · 0.0 mb/s",
    "Laptop · 26% · 0.0 mb/s",
  ]);
  expect(pills()[0]!.getBoundingClientRect().left).toBe(group.getBoundingClientRect().left);
  const width =
    pills().at(-1)!.getBoundingClientRect().right - pills()[0]!.getBoundingClientRect().left;
  container.style.width = "1200px";
  await expect.poll(() => group.clientWidth).toBe(1200);
  expect(
    pills().at(-1)!.getBoundingClientRect().right - pills()[0]!.getBoundingClientRect().left,
  ).toBe(width);
  expect(pills().some((p) => p.textContent?.includes("GiB"))).toBe(false);
});

it("preserves all numbers through individual name reductions, diagonal and vertical layouts, then restores", async () => {
  const container = document.createElement("div");
  container.style.width = "1000px";
  await using _screen = await render(
    <TooltipProvider>
      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
        <span data-title style={{ flex: "none" }}>
          Thread title stays
        </span>
        <ResponsiveHostStatusPills hosts={hosts} />
      </div>
    </TooltipProvider>,
    { container, router: false },
  );
  const group = container.querySelector<HTMLElement>('[aria-label="Host status"]')!;
  const title = container.querySelector<HTMLElement>("[data-title]")!;
  const initialTitleWidth = title.getBoundingClientRect().width;
  const seen = new Set<string>();
  let mixed = false;
  const names = () =>
    [...container.querySelectorAll('[role="img"] [data-host-name]')].map((n) =>
      n.textContent?.replace(/ · $/, ""),
    );
  for (const width of [
    700, 620, 580, 550, 520, 490, 460, 440, 420, 400, 380, 360, 340, 320, 300, 280, 260, 240, 220,
  ]) {
    container.style.width = `${width}px`;
    await expect
      .poll(() => Math.abs(group.getBoundingClientRect().width - (width - initialTitleWidth - 8)))
      .toBeLessThan(1);
    await expect
      .poll(() => {
        const last = container.querySelectorAll<HTMLElement>('[role="img"]');
        return (
          last.length === 0 ||
          last[last.length - 1]!.getBoundingClientRect().right <=
            group.getBoundingClientRect().right + 0.5
        );
      })
      .toBe(true);
    expect(title.getBoundingClientRect().width).toBe(initialTitleWidth);
    const pills = [...container.querySelectorAll<HTMLElement>('[role="img"]')];
    if (!pills.length) continue;
    expect(pills.length).toBe(4);
    expect(pills[0]!.getBoundingClientRect().left).toBeGreaterThanOrEqual(
      title.getBoundingClientRect().right + 7.9,
    );
    expect(pills.at(-1)!.getBoundingClientRect().right).toBeLessThanOrEqual(
      group.getBoundingClientRect().right + 0.5,
    );
    for (const [index, pill] of pills.entries()) {
      seen.add(pill.dataset.pair!);
      const values = [...pill.querySelectorAll<HTMLElement>("[data-host-value]")];
      expect(values.map((n) => n.textContent?.replace(/%| mb\/s/g, ""))).toEqual([
        index === 1 ? "0" : "26",
        index > 1 ? "0.0" : "42",
      ]);
      for (const value of values) {
        const v = value.getBoundingClientRect(),
          p = pill.getBoundingClientRect();
        expect(v.left).toBeGreaterThanOrEqual(p.left);
        expect(v.right).toBeLessThanOrEqual(p.right + 0.1);
      }
      expect(pill.getAttribute("aria-label")).toBe(hosts[index]!.label);
    }
    const labels = names();
    expect(labels.some((n) => n === "Min" || n === "Tst")).toBe(false);
    if (labels.includes("Lap") && labels.includes("Mini")) mixed = true;
  }
  expect(mixed).toBe(true);
  expect([...seen].sort()).toEqual(["diagonal", "inline", "vertical"]);
  container.style.width = "1000px";
  await expect
    .poll(() => container.querySelector('[role="img"]')?.textContent)
    .toBe("VPS · 26% · 42");
  expect(names()).toEqual(["VPS", "Test", "Mini", "Laptop"]);
});

it.each(["vps", "home"] as const)(
  "shows metric bullet lines on hover and keyboard focus for %s",
  async (id) => {
    using _clock = { [Symbol.dispose]: () => vi.useRealTimers() };
    vi.setSystemTime(new Date("2027-01-15T08:00:00.000Z"));
    const metrics = hostStatusMetrics({
      id,
      status: "available",
      cpuUsagePercent: 18,
      logicalCpuCount: 8,
      occupiedMemoryBytes: 18 * 1024 ** 3,
      availableMemoryBytes: 46 * 1024 ** 3,
      totalMemoryBytes: 64 * 1024 ** 3,
      swapIoBytesPerSecond: 0.3 * 1024 ** 2,
      availableStorageBytes: 220 * 1024 ** 3,
      totalStorageBytes: 500 * 1024 ** 3,
      storageMountPoint: "/",
      sampledAt: "2027-01-15T08:00:00.000Z",
    });
    const name = id === "home" ? "Laptop" : "VPS";
    const label = `${name}: ${metrics.detail}`;
    const container = document.createElement("div");
    container.style.width = "1000px";
    await using _screen = await render(
      <TooltipProvider>
        <ResponsiveHostStatusPills
          hosts={hosts.map((host) => (host.id === id ? { id, name, label, metrics } : host))}
        />
      </TooltipProvider>,
      { container, router: false },
    );
    await expect.poll(() => container.querySelector(`[data-host-id="${id}"]`)).not.toBeNull();
    const pill = container.querySelector<HTMLElement>(`[data-host-id="${id}"]`)!;
    const popup = () => document.querySelector<HTMLElement>('[data-slot="tooltip-popup"]');
    const expected = [
      "CPU 18% of total cores",
      "RAM 46 GiB available of 64 GiB",
      ...(id === "home" ? ["Swap I/O 0.3 MiB/s"] : []),
      "Storage 220 GiB free of 500 GiB",
    ];
    await userEvent.hover(pill);
    await expect
      .poll(() => [...(popup()?.querySelectorAll("ul > li") ?? [])].map((li) => li.textContent))
      .toEqual(expected);
    expect(getComputedStyle(popup()!.querySelector("ul")!).listStyleType).toBe("disc");
    expect(popup()!.textContent).not.toContain(name);
    expect(pill.getAttribute("aria-label")).toBe(label);
    await userEvent.unhover(pill);
    await expect.poll(() => popup()).toBeNull();
    await act(async () => pill.focus());
    await expect
      .poll(() => [...(popup()?.querySelectorAll("li") ?? [])].map((li) => li.textContent))
      .toEqual(expected);
    await userEvent.keyboard("{Escape}");
    await expect.poll(() => popup()).toBeNull();
  },
);

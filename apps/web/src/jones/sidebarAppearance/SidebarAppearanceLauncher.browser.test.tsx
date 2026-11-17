import { expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";
import { act, render } from "../../../test/render";
import { SidebarContent, SidebarGroup } from "../../components/ui/sidebar";
import { SidebarAppearanceLauncher } from "./SidebarAppearanceLauncher";
import { SidebarAppearanceProvider, useSidebarAppearanceHost } from "./useSidebarAppearanceHost";
import { SIDEBAR_APPEARANCE_STORAGE_KEY } from "./model";
import "~/index.css";

function Fixture({ narrow = false }: { narrow?: boolean }) {
  const host = useSidebarAppearanceHost();
  return (
    <SidebarAppearanceProvider host={host}>
      <aside
        data-slot="sidebar"
        style={narrow ? { width: 240, position: "fixed", right: 0, top: 0 } : { width: 280 }}
      >
        <SidebarAppearanceLauncher />
        <input aria-label="Rename thread" />
        <button type="button" onClick={() => host.update({ cardScale: 0.87 })}>
          Sidebar action
        </button>
        <output data-testid="presentation">{JSON.stringify(host.presented)}</output>
      </aside>
    </SidebarAppearanceProvider>
  );
}
function appearanceStorage(): Disposable {
  const previous = localStorage.getItem(SIDEBAR_APPEARANCE_STORAGE_KEY);
  localStorage.removeItem(SIDEBAR_APPEARANCE_STORAGE_KEY);
  return {
    [Symbol.dispose]() {
      vi.restoreAllMocks();
      if (previous === null) localStorage.removeItem(SIDEBAR_APPEARANCE_STORAGE_KEY);
      else localStorage.setItem(SIDEBAR_APPEARANCE_STORAGE_KEY, previous);
    },
  };
}

it("keeps the sidebar usable, compares without saving, and restores focus on Escape", async () => {
  using _storage = appearanceStorage();
  await using screen = await render(<Fixture />);
  await screen.getByRole("button", { name: "Sidebar appearance", exact: true }).click();
  await expect.element(page.getByRole("dialog", { name: "Sidebar appearance" })).toBeVisible();
  const panel = document.querySelector<HTMLElement>('[role="dialog"]')!;
  expect(panel.getAttribute("aria-modal")).toBe("false");
  await page.getByRole("button", { name: "Compare with Original" }).click();
  expect(localStorage.getItem(SIDEBAR_APPEARANCE_STORAGE_KEY)).toBeNull();
  expect(screen.container.querySelector('[data-testid="presentation"]')!.textContent).toContain(
    '"cardScale":1',
  );
  await screen.getByRole("textbox", { name: "Rename thread" }).fill("Still editable");
  await expect.element(page.getByRole("dialog")).toBeVisible();
  await screen.getByRole("button", { name: "Sidebar action" }).click();
  expect(screen.container.querySelector('[data-testid="presentation"]')!.textContent).toContain(
    '"cardScale":0.87',
  );
  await expect.element(page.getByRole("button", { name: "Compare with Original" })).toBeVisible();
  await page.getByRole("button", { name: "Compare with Original" }).click();
  await act(() => panel.focus());
  await userEvent.keyboard("{Escape}");
  expect(document.querySelector('[role="dialog"]')).toBeNull();
  expect(document.activeElement?.getAttribute("aria-label")).toBe("Sidebar appearance");
  expect(screen.container.querySelector('[data-testid="presentation"]')!.textContent).toContain(
    '"cardScale":0.87',
  );
});

it("preserves boost in presets, removes the key on reset, and exposes denied writes", async () => {
  using _storage = appearanceStorage();
  localStorage.setItem(
    SIDEBAR_APPEARANCE_STORAGE_KEY,
    JSON.stringify({ threadTitleBrightnessBoost: 65 }),
  );
  await using screen = await render(<Fixture />);
  await screen.getByRole("button", { name: "Sidebar appearance", exact: true }).click();
  await page.getByRole("radio", { name: "Original ○", exact: true }).click();
  expect(
    JSON.parse(localStorage.getItem(SIDEBAR_APPEARANCE_STORAGE_KEY)!).threadTitleBrightnessBoost,
  ).toBe(65);
  await page.getByRole("button", { name: "Reset all" }).click();
  expect(localStorage.getItem(SIDEBAR_APPEARANCE_STORAGE_KEY)).toBeNull();
  expect(screen.container.querySelector('[data-testid="presentation"]')!.textContent).toContain(
    '"threadTitleBrightnessBoost":50',
  );
  vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => {
    throw new DOMException("denied", "SecurityError");
  });
  await page.getByRole("radio", { name: "Original ○", exact: true }).click();
  await expect
    .element(page.getByText("Couldn't save; showing the saved appearance.", { exact: true }))
    .toHaveTextContent("Couldn't save; showing the saved appearance.");
  expect(screen.container.querySelector('[data-testid="presentation"]')!.textContent).toContain(
    '"cardScale":0.93',
  );
});

it("shows the selected VPS symbol at the same size as a thread card", async () => {
  using _storage = appearanceStorage();
  await using screen = await render(<Fixture />);
  await screen.getByRole("button", { name: "Sidebar appearance", exact: true }).click();
  const sample = document.querySelector('[aria-label="Example VPS thread card"]')!;
  const icon = () => sample.querySelector("svg")!;
  const filledHalves = () => icon().querySelectorAll('path[fill="currentColor"][mask]').length;
  expect(icon().getBoundingClientRect().width).toBeCloseTo(12 * 0.93, 1);
  expect(icon().classList.contains("lucide-brain")).toBe(true);
  await page.getByRole("radio", { name: "Filled", exact: true }).click();
  expect(filledHalves()).toBe(2);
  await page.getByRole("radio", { name: "Left", exact: true }).click();
  expect(filledHalves()).toBe(1);
  await page
    .getByRole("group", { name: "VPS symbol", exact: true })
    .getByRole("radio", { name: /Outline .*selected default/ })
    .click();
  expect(icon().classList.contains("lucide-brain")).toBe(true);
});

it("falls back in flow when there is no room for a 280px panel", async () => {
  using _storage = appearanceStorage();
  await using screen = await render(<Fixture narrow />);
  await screen.getByRole("button", { name: "Sidebar appearance", exact: true }).click();
  const panel = screen.container.querySelector<HTMLElement>('[role="dialog"]')!;
  expect(panel).not.toBeNull();
  expect(getComputedStyle(panel).position).toBe("relative");
  expect(panel.getBoundingClientRect().width).toBeLessThanOrEqual(240);
});

it("reserves the real SidebarContent thread viewport in a short narrow window", async () => {
  using _storage = appearanceStorage();
  function SidebarFixture() {
    const host = useSidebarAppearanceHost();
    return (
      <SidebarAppearanceProvider host={host}>
        <aside
          data-slot="sidebar"
          data-sidebar="sidebar"
          style={{
            position: "fixed",
            right: 0,
            top: 0,
            width: 240,
            height: 400,
            display: "flex",
            flexDirection: "column",
          }}
        >
          <div style={{ height: 52, flexShrink: 0 }}>Chrome</div>
          <SidebarContent
            fixedHeader={
              <SidebarGroup>
                <div style={{ height: 32 }}>Search</div>
                <SidebarAppearanceLauncher />
              </SidebarGroup>
            }
          >
            <button type="button">Thread remains clickable</button>
            <div style={{ height: 900 }}>More thread cards</div>
          </SidebarContent>
          <div style={{ height: 84, flexShrink: 0 }}>Footer</div>
        </aside>
      </SidebarAppearanceProvider>
    );
  }
  await using screen = await render(<SidebarFixture />);
  const viewport = screen.container.querySelector<HTMLElement>(
    '[data-slot="scroll-area-viewport"]',
  )!;
  const before = viewport.getBoundingClientRect().height;
  await screen.getByRole("button", { name: "Sidebar appearance", exact: true }).click();
  const panel = screen.container.querySelector<HTMLElement>('[role="dialog"]')!;
  expect(getComputedStyle(panel).position).toBe("relative");
  expect(panel.getBoundingClientRect().height).toBeLessThanOrEqual(before * 0.4 + 1);
  expect(viewport.getBoundingClientRect().height).toBeGreaterThanOrEqual(before * 0.6 - 1);
  expect(viewport.getBoundingClientRect().height).toBeGreaterThan(100);
  await screen.getByRole("button", { name: "Thread remains clickable" }).click();
  await expect.element(page.getByRole("dialog", { name: "Sidebar appearance" })).toBeVisible();
});

it("invalidates Compare on cross-tab changes without reviving an earlier comparison", async () => {
  using _storage = appearanceStorage();
  await using screen = await render(<Fixture />);
  await screen.getByRole("button", { name: "Sidebar appearance", exact: true }).click();
  await page.getByRole("button", { name: "Compare with Original" }).click();
  await expect
    .poll(
      () =>
        JSON.parse(screen.container.querySelector('[data-testid="presentation"]')!.textContent!)
          .cardScale,
    )
    .toBe(1);
  await act(() => {
    localStorage.setItem(SIDEBAR_APPEARANCE_STORAGE_KEY, JSON.stringify({ cardScale: 0.87 }));
    window.dispatchEvent(new StorageEvent("storage", { key: SIDEBAR_APPEARANCE_STORAGE_KEY }));
  });
  await expect
    .poll(
      () =>
        JSON.parse(screen.container.querySelector('[data-testid="presentation"]')!.textContent!)
          .cardScale,
    )
    .toBe(0.87);
  await expect.element(page.getByRole("button", { name: "Compare with Original" })).toBeVisible();
  await act(() => {
    localStorage.removeItem(SIDEBAR_APPEARANCE_STORAGE_KEY);
    window.dispatchEvent(new StorageEvent("storage", { key: SIDEBAR_APPEARANCE_STORAGE_KEY }));
  });
  await expect
    .poll(
      () =>
        JSON.parse(screen.container.querySelector('[data-testid="presentation"]')!.textContent!)
          .cardScale,
    )
    .toBe(0.93);
  await expect.element(page.getByRole("button", { name: "Compare with Original" })).toBeVisible();
  expect(localStorage.getItem(SIDEBAR_APPEARANCE_STORAGE_KEY)).toBeNull();
});

import { expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";
import { act, render } from "../../../test/render";
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
    JSON.stringify({ threadTitleBrightnessBoost: 50 }),
  );
  await using screen = await render(<Fixture />);
  await screen.getByRole("button", { name: "Sidebar appearance", exact: true }).click();
  await page.getByRole("radio", { name: "Original ○", exact: true }).click();
  expect(
    JSON.parse(localStorage.getItem(SIDEBAR_APPEARANCE_STORAGE_KEY)!).threadTitleBrightnessBoost,
  ).toBe(50);
  await page.getByRole("button", { name: "Reset all" }).click();
  expect(localStorage.getItem(SIDEBAR_APPEARANCE_STORAGE_KEY)).toBeNull();
  expect(screen.container.querySelector('[data-testid="presentation"]')!.textContent).toContain(
    '"threadTitleBrightnessBoost":0',
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

it("falls back in flow when there is no room for a 280px panel", async () => {
  using _storage = appearanceStorage();
  await using screen = await render(<Fixture narrow />);
  await screen.getByRole("button", { name: "Sidebar appearance", exact: true }).click();
  const panel = screen.container.querySelector<HTMLElement>('[role="dialog"]')!;
  expect(panel).not.toBeNull();
  expect(getComputedStyle(panel).position).toBe("relative");
  expect(panel.getBoundingClientRect().width).toBeLessThanOrEqual(240);
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

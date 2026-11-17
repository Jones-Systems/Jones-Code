import { expect, it } from "vite-plus/test";
import { page } from "vite-plus/test/browser";
import { act, render } from "../../../test/render";
import { ThreadTitleBrightnessSettings } from "./ThreadTitleBrightnessSettings";
import { SidebarAppearanceControls } from "./SidebarAppearanceControls";
import { SidebarAppearanceProvider, useSidebarAppearanceHost } from "./useSidebarAppearanceHost";
import { SIDEBAR_APPEARANCE_STORAGE_KEY } from "./model";
import "~/index.css";

function Fixture() {
  const host = useSidebarAppearanceHost();
  return (
    <>
      <ThreadTitleBrightnessSettings />
      <SidebarAppearanceProvider host={host}>
        <div data-testid="panel">
          <SidebarAppearanceControls />
          <output data-testid="presentation">{JSON.stringify(host.presented)}</output>
        </div>
      </SidebarAppearanceProvider>
    </>
  );
}
function appearanceStorage(): Disposable {
  const previous = localStorage.getItem(SIDEBAR_APPEARANCE_STORAGE_KEY);
  localStorage.removeItem(SIDEBAR_APPEARANCE_STORAGE_KEY);
  return {
    [Symbol.dispose]() {
      if (previous === null) localStorage.removeItem(SIDEBAR_APPEARANCE_STORAGE_KEY);
      else localStorage.setItem(SIDEBAR_APPEARANCE_STORAGE_KEY, previous);
    },
  };
}

it("shares the native slider's persisted value across settings and the live panel", async () => {
  using _storage = appearanceStorage();
  await using screen = await render(<Fixture />);
  const sliders = screen.container.querySelectorAll<HTMLInputElement>('input[type="range"]');
  expect(sliders).toHaveLength(2);
  for (const slider of sliders) {
    expect([slider.min, slider.max, slider.step]).toEqual(["0", "100", "5"]);
    expect(slider.getAttribute("aria-label")).toBe("Sidebar thread title brightness");
  }
  await screen.getByRole("button", { name: "Compare with Original" }).click();
  await expect
    .poll(
      () =>
        JSON.parse(screen.container.querySelector('[data-testid="presentation"]')!.textContent!)
          .cardScale,
    )
    .toBe(1);
  expect(localStorage.getItem(SIDEBAR_APPEARANCE_STORAGE_KEY)).toBeNull();
  await act(() => {
    const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")!.set!;
    setter.call(sliders[0], "50");
    sliders[0]!.dispatchEvent(new Event("input", { bubbles: true }));
    sliders[0]!.dispatchEvent(new Event("change", { bubbles: true }));
  });
  expect(sliders[1]!.value).toBe("50");
  await expect
    .poll(
      () =>
        JSON.parse(screen.container.querySelector('[data-testid="presentation"]')!.textContent!)
          .cardScale,
    )
    .toBe(0.93);
  await expect.element(page.getByRole("button", { name: "Compare with Original" })).toBeVisible();
  expect(
    JSON.parse(localStorage.getItem(SIDEBAR_APPEARANCE_STORAGE_KEY)!).threadTitleBrightnessBoost,
  ).toBe(50);
  const previews = screen.container.querySelectorAll(
    '[aria-label="Thread title previews"] [data-jones-thread-title]',
  );
  expect(previews).toHaveLength(2);
  expect(previews[0]!.textContent).toBe(previews[1]!.textContent);
  expect(getComputedStyle(previews[0]!).fontWeight).toBe(getComputedStyle(previews[1]!).fontWeight);
  expect(getComputedStyle(previews[0]!).color).not.toBe(getComputedStyle(previews[1]!).color);
});

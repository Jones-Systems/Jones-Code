import { expect, it } from "vite-plus/test";
import { userEvent } from "vite-plus/test/browser";
import { render } from "../../../test/render";
import { ThreadBoardAppearanceSettings } from "./ThreadBoardAppearanceSettings";
import { THREAD_BOARD_APPEARANCE_KEY } from "./threadBoardAppearance";
import { useSidebarAppearanceHost } from "./useSidebarAppearanceHost";
import "~/index.css";

function Fixture() {
  const sidebar = useSidebarAppearanceHost();
  return (
    <>
      <div {...sidebar.scopeProps} data-testid="live-sidebar" />
      <ThreadBoardAppearanceSettings />
    </>
  );
}

it("previews separately saved board controls, rejects out-of-range values, and resets after reload", async () => {
  const previous = localStorage.getItem(THREAD_BOARD_APPEARANCE_KEY);
  localStorage.removeItem(THREAD_BOARD_APPEARANCE_KEY);
  try {
    await using initial = await render(<Fixture />);
    const size = initial.getByRole("spinbutton", { name: "Failed and total count size" });
    await size.fill("99");
    await userEvent.keyboard("{Tab}");
    await expect.element(size).toHaveValue(10);
    expect(localStorage.getItem(THREAD_BOARD_APPEARANCE_KEY)).toBeNull();

    await size.fill("14");
    await userEvent.keyboard("{Enter}");
    await initial.getByRole("button", { name: "Keep filter spacing fixed" }).click();
    await initial
      .getByRole("textbox", { name: "Unassigned background", exact: true })
      .fill("#263e61");
    await userEvent.keyboard("{Tab}");
    const tint = initial.getByRole("slider", { name: "Workstream tint opacity" });
    await tint.click();
    await userEvent.keyboard("{Home}{ArrowRight}{ArrowRight}");

    const scope = initial.getByTestId("live-sidebar").element() as HTMLElement;
    expect(scope.style.getPropertyValue("--jones-board-count-size")).toBe("14px");
    expect(scope.style.getPropertyValue("--jones-board-unassigned-background")).toBe("#263e61");
    expect(scope.style.getPropertyValue("--jones-board-tint-opacity")).toBe("2%");
    expect(scope.dataset.jonesBoardFilterSpacing).toBe("fixed");
    const preview = initial.getByRole("group", { name: "Thread board preview" }).element();
    expect(getComputedStyle(preview.querySelector("[data-personal-group-count]")!).fontSize).toBe(
      "14px",
    );

    await initial.unmount();
    await using reopened = await render(<Fixture />);
    await expect
      .element(reopened.getByRole("spinbutton", { name: "Failed and total count size" }))
      .toHaveValue(14);
    await expect
      .element(reopened.getByRole("button", { name: "Keep filter spacing fixed" }))
      .toHaveAttribute("aria-pressed", "true");
    await reopened.getByRole("button", { name: "Reset board settings" }).click();
    expect(localStorage.getItem(THREAD_BOARD_APPEARANCE_KEY)).toBeNull();
    await expect
      .element(reopened.getByRole("spinbutton", { name: "Failed and total count size" }))
      .toHaveValue(10);
  } finally {
    if (previous === null) localStorage.removeItem(THREAD_BOARD_APPEARANCE_KEY);
    else localStorage.setItem(THREAD_BOARD_APPEARANCE_KEY, previous);
  }
});

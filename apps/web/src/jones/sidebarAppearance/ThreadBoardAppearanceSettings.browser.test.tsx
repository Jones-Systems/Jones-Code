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

    const setNumber = async (name: RegExp, value: string) => {
      await initial.getByRole("spinbutton", { name }).fill(value);
      await userEvent.keyboard("{Enter}");
    };
    await setNumber(/Filter pill offset left/, "-4");
    await setNumber(/Filter pill offset up/, "3");
    await setNumber(/Count offset left/, "2");
    await setNumber(/Count offset up/, "-3");
    await setNumber(/Workstream top padding/, "8");
    await setNumber(/Workstream bottom padding/, "2");

    expect(getComputedStyle(preview.querySelector("[data-personal-status-pill]")!).translate).toBe(
      "-4px 3px",
    );
    expect(getComputedStyle(preview.querySelector("[data-personal-group-count]")!).translate).toBe(
      "2px -3px",
    );
    const header = getComputedStyle(preview.querySelector("[data-personal-group-header]")!);
    expect([header.paddingTop, header.paddingBottom]).toEqual(["8px", "2px"]);

    const rail = initial.getByRole("button", { name: /Collapse CI .* from the color bar/ });
    await expect.element(rail).not.toBeInTheDocument();
    await initial.getByRole("button", { name: "Click color bar to collapse or expand" }).click();
    await rail.click();
    await expect.element(initial.getByText("Assess CI performance")).not.toBeInTheDocument();
    await initial.getByRole("button", { name: /Expand CI .* from the color bar/ }).click();
    await expect.element(initial.getByText("Assess CI performance")).toBeVisible();

    await initial.unmount();
    await using reopened = await render(<Fixture />);
    await expect
      .element(reopened.getByRole("spinbutton", { name: "Failed and total count size" }))
      .toHaveValue(14);
    await expect
      .element(reopened.getByRole("button", { name: "Keep filter spacing fixed" }))
      .toHaveAttribute("aria-pressed", "true");
    await expect
      .element(reopened.getByRole("spinbutton", { name: /Filter pill offset left/ }))
      .toHaveValue(-4);
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

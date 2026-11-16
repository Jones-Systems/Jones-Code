import "~/index.css";
import { useState } from "react";
import { describe, expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";
import { act, render } from "../../../test/render";
import { PersonalSidebarGroupHeader } from "./PersonalSidebarGroupHeader";
import type { PersonalStatusFilter } from "./personalStatusFilter";

const summary = { total: 14, running: 3, attention: 2, done: 8, failed: 1 };

function Header({
  width = 360,
  name = "Alpha",
  onRename = () => {},
}: {
  width?: number;
  name?: string;
  onRename?: () => void;
}) {
  const [collapsed, setCollapsed] = useState(false);
  const [filter, setFilter] = useState<PersonalStatusFilter | null>(null);
  return (
    <div style={{ width }}>
      <PersonalSidebarGroupHeader
        name={name}
        collapsed={collapsed}
        onToggle={() => {
          setCollapsed((value) => !value);
          return () => setCollapsed(collapsed);
        }}
        summary={summary}
        statusFilter={filter}
        onStatusFilterChange={setFilter}
        onRenameRequested={onRename}
        actions={
          <button type="button" aria-label={`Options for ${name}`}>
            …
          </button>
        }
      />
      <output aria-label="Local status">{filter ?? "inherit"}</output>
      {!collapsed ? <p>Member threads</p> : null}
    </div>
  );
}

function counter(container: ParentNode, name: string) {
  return container.querySelector<HTMLButtonElement>(`button[aria-label^="${name}:"]`)!;
}
async function clickAt(target: Element, at: number) {
  await act(() => {
    const event = new MouseEvent("click", { bubbles: true, detail: 1 });
    Object.defineProperty(event, "timeStamp", { value: at });
    target.dispatchEvent(event);
  });
}

describe("personal Workstream header controls", () => {
  it.each([
    ["Working", "working", 3],
    ["Needs input", "input", 2],
    ["Done", "done", 8],
  ] as const)(
    "selects, clears and excludes %s without collapsing or renaming",
    async (name, status, count) => {
      const rename = vi.fn();
      await using screen = await render(<Header onRename={rename} />);
      const target = counter(screen.container, name);
      expect(screen.container.querySelector("button button")).toBeNull();
      expect(target.getAttribute("aria-label")).toContain(
        `${name}: ${count}, using global status filter`,
      );
      await clickAt(target, 1000);
      expect(target.getAttribute("aria-pressed")).toBe("true");
      expect(target.getAttribute("aria-label")).toContain(`${count}, selected`);
      expect(getComputedStyle(target).backgroundColor).not.toBe("rgba(0, 0, 0, 0)");
      await expect
        .element(page.getByRole("status", { name: "Local status" }))
        .toHaveTextContent(status);
      await clickAt(target, 1500);
      expect(target.getAttribute("aria-pressed")).toBe("false");
      await expect
        .element(page.getByRole("status", { name: "Local status" }))
        .toHaveTextContent("inherit");
      await clickAt(target, 2000);
      await clickAt(target, 2250);
      expect(target.getAttribute("aria-pressed")).toBe("true");
      expect(target.getAttribute("aria-label")).toContain(`${count}, excluded`);
      expect(getComputedStyle(target).textDecorationLine).toContain("line-through");
      await expect
        .element(page.getByRole("status", { name: "Local status" }))
        .toHaveTextContent(`hide-${status}`);
      await clickAt(target, 2300);
      await expect
        .element(page.getByRole("status", { name: "Local status" }))
        .toHaveTextContent(`hide-${status}`);
      await clickAt(target, 2551);
      expect(target.getAttribute("aria-label")).toContain(`${count}, selected`);
      await act(() => target.focus());
      await userEvent.keyboard("{Enter}");
      await expect
        .element(page.getByRole("status", { name: "Local status" }))
        .toHaveTextContent("inherit");
      expect(target.getAttribute("aria-pressed")).toBe("false");
      await expect.element(page.getByText("Member threads", { exact: true })).toBeVisible();
      expect(rename).not.toHaveBeenCalled();
      const transfer = new DataTransfer();
      await act(() => {
        const event = new DragEvent("dragstart", {
          bubbles: true,
          cancelable: true,
          dataTransfer: transfer,
        });
        target.dispatchEvent(event);
        expect(event.defaultPrevented).toBe(true);
      });
      await userEvent.click(page.getByRole("button", { name: "Collapse Alpha", exact: true }));
      await expect
        .element(page.getByText("Member threads", { exact: true }))
        .not.toBeInTheDocument();
    },
  );

  it("keeps independent clicks on different counters independent within 250 ms", async () => {
    await using screen = await render(<Header />);
    await clickAt(counter(screen.container, "Working"), 1000);
    await clickAt(counter(screen.container, "Done"), 1100);
    await expect
      .element(page.getByRole("status", { name: "Local status" }))
      .toHaveTextContent("done");
    expect(counter(screen.container, "Working").getAttribute("aria-pressed")).toBe("false");
    expect(counter(screen.container, "Done").getAttribute("aria-label")).toContain("selected");
  });

  it("keeps counters compact and gives their spare spacing to a long title", async () => {
    await using screen = await render(<Header width={420} />);
    const counters = () =>
      counter(screen.container, "Working").closest<HTMLElement>(
        "[data-workstream-header-control]",
      )!;
    const roomyWidth = counters().getBoundingClientRect().width;
    expect(roomyWidth).toBeLessThanOrEqual(56);
    expect(getComputedStyle(counter(screen.container, "Working")).fontSize).toBe("12px");

    for (const width of [200, 260, 360]) {
      await screen.rerender(
        <Header width={width} name="A long Workstream title that needs the available space" />,
      );
      const header = screen.container.querySelector<HTMLElement>("[data-personal-group-header]")!;
      const group = counters();
      const title = header.querySelector(".line-clamp-2")!;
      expect(group.getBoundingClientRect().width).toBeLessThan(roomyWidth - 2);
      expect(group.scrollWidth).toBeLessThanOrEqual(group.clientWidth + 1);
      expect(header.scrollWidth).toBeLessThanOrEqual(header.clientWidth);
      expect(title.getBoundingClientRect().right).toBeLessThanOrEqual(
        group.getBoundingClientRect().left,
      );
      for (const name of ["Working", "Needs input", "Done"]) {
        const button = counter(header, name);
        expect(button.getBoundingClientRect().width).toBeGreaterThan(8);
        expect(button.getBoundingClientRect().right).toBeLessThanOrEqual(
          group.getBoundingClientRect().right,
        );
      }
    }
    await screen.rerender(<Header width={420} />);
    expect(counters().getBoundingClientRect().width).toBeCloseTo(roomyWidth, 1);
  });

  it.each([260, 360])(
    "places failed metadata below the action at the full right edge at %s px",
    async (width) => {
      await using screen = await render(
        <Header width={width} name="A long Workstream title that wraps onto two lines" />,
      );
      const header = screen.container.querySelector<HTMLElement>("[data-personal-group-header]")!;
      const options = header.querySelector<HTMLButtonElement>('[aria-label^="Options for"]')!;
      const failed = [...header.querySelectorAll("span")].find(
        (element) => element.textContent === "1 failed",
      )!;
      const title = header.querySelector(".line-clamp-2")!;
      const working = counter(header, "Working");
      const style = getComputedStyle(header);
      const rect = header.getBoundingClientRect();
      const failedRect = failed.getBoundingClientRect();
      expect(
        Math.abs(failedRect.right - (rect.right - parseFloat(style.paddingRight))),
      ).toBeLessThan(1);
      expect(Math.abs(failedRect.right - options.getBoundingClientRect().right)).toBeLessThan(1);
      expect(failedRect.top).toBeGreaterThanOrEqual(options.getBoundingClientRect().bottom);
      expect(title.getBoundingClientRect().right).toBeLessThanOrEqual(
        working.getBoundingClientRect().left,
      );
      expect(header.scrollWidth).toBeLessThanOrEqual(header.clientWidth);
      await expect.element(page.getByText("14 threads", { exact: true })).toBeVisible();
    },
  );
});

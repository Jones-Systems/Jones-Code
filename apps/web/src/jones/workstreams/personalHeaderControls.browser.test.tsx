import "~/index.css";
import "../sidebarAppearance/sidebarAppearance.workstreams.css";
import { useState, type ReactNode } from "react";
import { describe, expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";
import { act, render } from "../../../test/render";
import { PersonalStatusPill } from "./PersonalStatusPill";
import { PersonalSidebarGroupHeader } from "./PersonalSidebarGroupHeader";
import type { PersonalStatusFilter } from "./personalStatusFilter";
import type { PersonalThreadSummary } from "./personalPresentation";
import { ThreadStatusFilterPill } from "../threadStatusFilter/ThreadStatusFilterPill";

const summary = { total: 14, running: 3, attention: 2, done: 8, failed: 1 };

function Header({
  width = 360,
  name = "Alpha",
  onRename = () => {},
  counts = summary,
  nameEditor,
  nameIcon,
  fontFamily,
}: {
  width?: number;
  name?: string;
  onRename?: () => void;
  counts?: PersonalThreadSummary;
  nameEditor?: ReactNode;
  nameIcon?: ReactNode;
  fontFamily?: string | undefined;
}) {
  const [collapsed, setCollapsed] = useState(false);
  const [filter, setFilter] = useState<PersonalStatusFilter | null>(null);
  return (
    <div style={{ width, fontFamily }}>
      <PersonalSidebarGroupHeader
        name={name}
        collapsed={collapsed}
        onToggle={() => {
          setCollapsed((value) => !value);
          return () => setCollapsed(collapsed);
        }}
        summary={counts}
        nameEditor={nameEditor}
        nameIcon={nameIcon}
        statusFilter={filter}
        onStatusFilterChange={setFilter}
        onRenameRequested={onRename}
      />
      <output aria-label="Local status">{filter ?? "inherit"}</output>
      {!collapsed ? <p>Member threads</p> : null}
    </div>
  );
}

function counter(container: ParentNode, name: string) {
  return container.querySelector<HTMLButtonElement>(`button[aria-label^="${name}:"]`)!;
}
function pill(container: ParentNode) {
  return container.querySelector<HTMLElement>("[data-personal-status-pill]")!;
}
function visibleTitleLines(container: ParentNode) {
  const title = container.querySelector<HTMLElement>(".line-clamp-2")!;
  const text = title.firstChild!;
  const range = document.createRange();
  const bottom = title.getBoundingClientRect().bottom;
  const lines = new Map<number, string>();
  for (let index = 0; index < text.textContent!.length; index++) {
    range.setStart(text, index);
    range.setEnd(text, index + 1);
    const rect = range.getBoundingClientRect();
    if (rect.top >= bottom || rect.width === 0) continue;
    lines.set(rect.top, (lines.get(rect.top) ?? "") + text.textContent![index]);
  }
  return [...lines.values()].map((line) => line.trim());
}
function minimumLayout(container: ParentNode) {
  const group = pill(container);
  const spacing = group.style.getPropertyValue("--personal-status-spacing");
  group.style.setProperty("--personal-status-spacing", "0.25");
  try {
    return {
      width: group.getBoundingClientRect().width,
      titleWidth: container.querySelector(".line-clamp-2")!.getBoundingClientRect().width,
      lines: visibleTitleLines(container),
    };
  } finally {
    group.style.setProperty("--personal-status-spacing", spacing);
  }
}
async function clickAt(target: Element, at: number) {
  await act(() => {
    const event = new MouseEvent("click", { bubbles: true, detail: 1 });
    Object.defineProperty(event, "timeStamp", { value: at });
    target.dispatchEvent(event);
  });
}

describe("personal Workstream header controls", () => {
  it("uses pointer cursors for actions and a default cursor in pill gaps and read-only counters", async () => {
    await using screen = await render(
      <div style={{ cursor: "grab" }}>
        <Header />
        <div data-read-only>
          <PersonalStatusPill summary={summary} />
        </div>
      </div>,
    );
    const header = screen.container.querySelector("[data-personal-group-header]")!;
    for (const element of [
      ...header.querySelectorAll("button"),
      header.querySelector(".line-clamp-2")!,
    ])
      expect(getComputedStyle(element).cursor).toBe("pointer");
    const group = pill(header);
    expect(getComputedStyle(group).cursor).toBe("default");
    const separator = [...group.querySelectorAll("span")].find(
      (element) => element.textContent === "·",
    )!;
    const rect = separator.getBoundingClientRect();
    const hit = document.elementFromPoint(rect.left + rect.width / 2, rect.top + rect.height / 2)!;
    expect(getComputedStyle(hit).cursor).toBe("default");
    for (const element of screen.container.querySelectorAll("[data-read-only] span"))
      expect(getComputedStyle(element).cursor).toBe("default");
  });

  it.each([true, false])(
    "shows grab only on a draggable title (draggable=%s)",
    async (draggable) => {
      await using screen = await render(
        <div data-workstream-header draggable={draggable}>
          <Header />
        </div>,
      );
      const title = screen.container.querySelector<HTMLElement>(".line-clamp-2")!;
      const rect = title.getBoundingClientRect();
      const hit = document.elementFromPoint(rect.left + 2, rect.top + rect.height / 2)!;
      expect(hit).toBe(title);
      expect(getComputedStyle(hit).cursor).toBe(draggable ? "grab" : "pointer");
      for (const button of screen.container.querySelectorAll("button"))
        expect(getComputedStyle(button).cursor).toBe("pointer");
    },
  );

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

  it("has no disclosure chevron, keeps the title flush left and toggles through the labelled overlay", async () => {
    await using screen = await render(<Header />);
    const header = screen.container.querySelector<HTMLElement>("[data-personal-group-header]")!;
    expect(header.querySelector("svg")).toBeNull();
    const headerRect = header.getBoundingClientRect();
    const title = header.querySelector<HTMLElement>(".line-clamp-2")!;
    const paddingLeft = parseFloat(getComputedStyle(header).paddingLeft);
    expect(title.getBoundingClientRect().left - headerRect.left).toBeCloseTo(paddingLeft, 1);
    const count = [...header.querySelectorAll("span")].find(
      (element) => element.textContent === "14 threads",
    )!;
    expect(count.getBoundingClientRect().left).toBeCloseTo(title.getBoundingClientRect().left, 1);
    const toggle = page.getByRole("button", { name: "Collapse Alpha", exact: true });
    await expect.element(toggle).toHaveAttribute("aria-expanded", "true");
    await userEvent.click(page.getByText("Alpha", { exact: true }));
    await expect
      .element(page.getByRole("button", { name: "Expand Alpha", exact: true }))
      .toHaveAttribute("aria-expanded", "false");
    await expect.element(page.getByText("Member threads", { exact: true })).not.toBeInTheDocument();
  });

  it("matches the real top-bar pill's standard geometry when the title has room", async () => {
    await using screen = await render(
      <>
        <Header width={420} />
        <div data-standard-pill>
          <ThreadStatusFilterPill
            counts={{ working: 3, input: 2, done: 8 }}
            filter="all"
            onCycle={() => {}}
          />
        </div>
      </>,
    );
    const group = pill(screen.container);
    const standard = screen.container.querySelector<HTMLElement>("[data-standard-pill] button")!;
    expect(group.getBoundingClientRect().width).toBeCloseTo(
      standard.getBoundingClientRect().width,
      1,
    );
    expect(group.getBoundingClientRect().height).toBe(24);
    expect(getComputedStyle(group).gap).toBe(getComputedStyle(standard).gap);
    expect(getComputedStyle(group).fontSize).toBe("12px");
    expect(getComputedStyle(group).fontWeight).toBe(getComputedStyle(standard).fontWeight);
    expect(getComputedStyle(group).backgroundColor).toBe(
      getComputedStyle(standard).backgroundColor,
    );
    expect(getComputedStyle(counter(screen.container, "Working")).paddingLeft).toBe("6px");
    expect(group.textContent).toBe("3·2·8");
    const minimum = minimumLayout(screen.container);
    expect(minimum.width).toBeGreaterThanOrEqual(47);
    expect(minimum.width).toBeLessThan(54);
  });

  it.each([undefined, "Arial", "monospace", "serif"])(
    "compresses for the title while preserving the minimum layout's two visible lines with %s",
    async (fontFamily) => {
      await using screen = await render(<Header width={420} fontFamily={fontFamily} />);
      const roomyWidth = pill(screen.container).getBoundingClientRect().width;
      for (const width of [200, 260, 360, 213.3, 287.7]) {
        await screen.rerender(
          <Header width={width} name={"W".repeat(100)} fontFamily={fontFamily} />,
        );
        const header = screen.container.querySelector<HTMLElement>("[data-personal-group-header]")!;
        const group = pill(header);
        const title = header.querySelector(".line-clamp-2")!;
        const minimum = minimumLayout(header);
        // A width-only rerender refits through ResizeObserver before the next paint.
        await expect.poll(() => visibleTitleLines(header)).toEqual(minimum.lines);
        expect(minimum.lines).toHaveLength(2);
        expect(group.getBoundingClientRect().width).toBeLessThan(roomyWidth - 2);
        expect(group.getBoundingClientRect().width).toBeGreaterThanOrEqual(minimum.width);
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
      await screen.rerender(<Header width={420} fontFamily={fontFamily} />);
      expect(pill(screen.container).getBoundingClientRect().width).toBeCloseTo(roomyWidth, 1);
    },
  );

  it("refits a narrow header when switching from fixed to adaptive spacing", async () => {
    const ui = (spacing: "fixed" | "adaptive") => (
      <div data-jones-board-filter-spacing={spacing}>
        <Header width={213} name={"W".repeat(100)} />
      </div>
    );
    await using screen = await render(ui("fixed"));
    const group = pill(screen.container);
    const fixedWidth = group.getBoundingClientRect().width;
    expect(group.style.getPropertyValue("--personal-status-spacing")).toBe("");
    await screen.rerender(ui("adaptive"));
    await expect.poll(() => group.getBoundingClientRect().width).toBeLessThan(fixedWidth - 2);
    await screen.rerender(ui("fixed"));
    expect(group.getBoundingClientRect().width).toBeCloseTo(fixedWidth);
  });

  it("returns unused line space to the counters when further title wrapping cannot improve", async () => {
    await using screen = await render(<Header width={420} />);
    const standardWidth = pill(screen.container).getBoundingClientRect().width;
    await screen.rerender(<Header width={260} name={"Workstream ".repeat(12)} />);
    const title = screen.container.querySelector<HTMLElement>(".line-clamp-2")!;
    const word = document.createRange();
    word.setStart(title.firstChild!, 0);
    word.setEnd(title.firstChild!, "Workstream".length);
    const wordWidth = word.getBoundingClientRect().width;
    const minimum = minimumLayout(screen.container);
    const width = 260 + wordWidth * 1.6 - minimum.titleWidth;
    await screen.rerender(<Header width={width} name={"Workstream ".repeat(12)} />);
    // The title is flush left, so at 260 px two words share a line and the counters stay
    // compressed. A width-only rerender refits through ResizeObserver before the next paint.
    await expect
      .poll(() => pill(screen.container).getBoundingClientRect().width)
      .toBeCloseTo(standardWidth, 1);
    expect(visibleTitleLines(screen.container)).toEqual(minimumLayout(screen.container).lines);
    expect(visibleTitleLines(screen.container)).toEqual(["Workstream", "Workstream"]);
  });

  it("refits live resize, count and font changes without hiding large counts or the name editor", async () => {
    await using screen = await render(<Header width={420} name={"W".repeat(100)} />);
    const host = screen.container.firstElementChild as HTMLElement;
    const group = pill(screen.container);
    const initial = group.style.getPropertyValue("--personal-status-spacing");
    await act(() => {
      host.style.width = "213px";
    });
    await expect
      .poll(() => group.style.getPropertyValue("--personal-status-spacing"))
      .not.toBe(initial);
    expect(visibleTitleLines(screen.container)).toEqual(minimumLayout(screen.container).lines);
    await act(() => {
      host.style.fontFamily = "monospace";
    });
    await expect.poll(() => getComputedStyle(group).fontFamily).toContain("monospace");
    await expect
      .poll(() => visibleTitleLines(screen.container))
      .toEqual(minimumLayout(screen.container).lines);
    await screen.rerender(
      <Header
        width={260}
        name={"W".repeat(100)}
        counts={{ total: 123456, running: 1234, attention: 5678, done: 9012, failed: 1 }}
      />,
    );
    expect(group.textContent).toBe("1234·5678·9012");
    for (const name of ["Working", "Needs input", "Done"]) {
      const target = counter(screen.container, name);
      expect(target.scrollWidth).toBeLessThanOrEqual(target.clientWidth);
      expect(target.getBoundingClientRect().right).toBeLessThanOrEqual(
        group.getBoundingClientRect().right,
      );
    }
    expect(
      screen.container.querySelector<HTMLElement>("[data-personal-group-header]")!.scrollWidth,
    ).toBeLessThanOrEqual(260);
    await screen.rerender(
      <Header
        width={260}
        nameEditor={
          <input aria-label="Workstream name" defaultValue="Alpha" className="w-full min-w-0" />
        }
      />,
    );
    await userEvent.fill(page.getByRole("textbox", { name: "Workstream name" }), "Renamed");
    await expect
      .element(page.getByRole("textbox", { name: "Workstream name" }))
      .toHaveValue("Renamed");
    await screen.rerender(<Header width={420} />);
    expect(group.style.getPropertyValue("--personal-status-spacing")).toBe("1");
    expect(screen.container.querySelectorAll(".line-clamp-2")).toHaveLength(1);
  });

  it.each([260, 360])(
    "places failed metadata below the filters at the full right edge at %s px",
    async (width) => {
      await using screen = await render(
        <Header width={width} name="A long Workstream title that wraps onto two lines" />,
      );
      const header = screen.container.querySelector<HTMLElement>("[data-personal-group-header]")!;
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
      expect(failedRect.top).toBeGreaterThanOrEqual(pill(header).getBoundingClientRect().bottom);
      expect(title.getBoundingClientRect().right).toBeLessThanOrEqual(
        working.getBoundingClientRect().left,
      );
      expect(header.scrollWidth).toBeLessThanOrEqual(header.clientWidth);
      await expect.element(page.getByText("14 threads", { exact: true })).toBeVisible();
    },
  );
});

it("keeps the roster total visible when a status filter hides every member", async () => {
  await using _screen = await render(
    <PersonalSidebarGroupHeader
      name="JC Sync"
      collapsed={false}
      onToggle={() => {}}
      summary={{ total: 3, running: 0, attention: 0, done: 1, failed: 0 }}
      visibleCount={0}
    />,
  );
  await expect.element(page.getByText("3 threads", { exact: true })).toBeVisible();
  expect(document.body.textContent).not.toContain("0 of 3");
});

it("aligns the icon to the first title line and wraps the next line under it", async () => {
  await using screen = await render(
    <Header width={260} name="Workstream title wrapping underneath its icon" nameIcon={<span />} />,
  );
  const title = screen.container.querySelector<HTMLElement>(".line-clamp-2")!;
  const icon = title.querySelector<HTMLElement>('[aria-hidden="true"]')!;
  const text = title.lastChild!;
  const range = document.createRange();
  range.selectNodeContents(text);
  const [first, second] = Array.from(range.getClientRects());
  const iconBox = icon.getBoundingClientRect();
  expect(iconBox.width).toBeCloseTo(parseFloat(getComputedStyle(title).fontSize), 1);
  expect(first!.left).toBeGreaterThan(iconBox.right);
  expect(iconBox.top).toBeLessThan(first!.bottom);
  expect(iconBox.bottom).toBeGreaterThan(first!.top);
  expect(second!.left).toBeCloseTo(iconBox.left, 1);
  expect(second!.top).toBeGreaterThan(first!.top);
});

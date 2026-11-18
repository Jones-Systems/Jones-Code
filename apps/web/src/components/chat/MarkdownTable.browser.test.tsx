import { expect, it, vi } from "vite-plus/test";
import { act, render } from "../../../test/render";
import "../../index.css";
import { MarkdownTable } from "./MarkdownTable";

vi.mock("../../hooks/useSettings", async (original) => {
  const actual = await original<typeof import("../../hooks/useSettings")>();
  return {
    ...actual,
    getClientSettings: () => ({ ...actual.getClientSettings(), wordWrap: false }),
  };
});

it("keeps the labeled expand control and bounded scroll surface after markdown extraction", async () => {
  await using screen = await render(
    <div className="chat-markdown" style={{ width: 420 }}>
      <MarkdownTable>
        <tbody>
          <tr>
            <td>{"Wide table content ".repeat(80)}</td>
          </tr>
        </tbody>
      </MarkdownTable>
    </div>,
  );
  const table = screen.container.querySelector<HTMLElement>(".chat-markdown-table-container")!;
  const scroll = table.querySelector<HTMLElement>("[data-markdown-table-scroll]")!;
  expect(scroll).not.toBeNull();
  expect(getComputedStyle(scroll).maxHeight).not.toBe("none");
  const expand = table.querySelector<HTMLButtonElement>('[aria-label="Expand table"]')!;
  expect(expand).not.toBeNull();
  expect(expand.getAttribute("aria-pressed")).toBe("false");
  await act(() => expand.click());
  expect(table.dataset.expanded).toBe("true");
  const collapse = table.querySelector<HTMLButtonElement>('[aria-label="Collapse table"]')!;
  expect(collapse.getAttribute("aria-pressed")).toBe("true");
  await act(() => collapse.click());
  expect(table.dataset.expanded).toBe("false");
});

it("offers expansion when individually fitting cells overflow the scroll viewport together", async () => {
  await using screen = await render(
    <div className="chat-markdown" style={{ width: 360 }}>
      <MarkdownTable>
        <tbody>
          <tr>
            <td>Dependency installation</td>
            <td>Observed physical allocation</td>
            <td>Rebuildable project artifacts</td>
          </tr>
        </tbody>
      </MarkdownTable>
    </div>,
  );
  const viewport = screen.container.querySelector<HTMLElement>(
    '[data-slot="scroll-area-viewport"]',
  )!;
  expect(viewport.scrollWidth).toBeGreaterThan(viewport.clientWidth);
  for (const cell of viewport.querySelectorAll("td")) {
    expect(cell.scrollWidth).toBe(cell.clientWidth);
  }
  expect(screen.container.querySelector('button[aria-label="Expand table"]')).not.toBeNull();

  await act(() => {
    viewport.closest<HTMLElement>(".chat-markdown")!.style.width = "700px";
  });
  await vi.waitFor(() => {
    expect(screen.container.querySelector('button[aria-label="Expand table"]')).toBeNull();
  });
});

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

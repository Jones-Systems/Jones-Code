import { act, useState } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";
import { userEvent } from "vite-plus/test/browser";
import "~/index.css";
import { ThreadStatusFilterPill } from "../threadStatusFilter/ThreadStatusFilterPill";
import { nextThreadStatusFilter, type ThreadStatusFilter } from "../threadStatusFilter/model";

let root: Root;
let container: HTMLDivElement;
beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
});
afterEach(async () => {
  await act(async () => root.unmount());
  container.remove();
  vi.unstubAllGlobals();
});

it("cycles all statuses with only the selected number highlighted and no underline", async () => {
  function Fixture() {
    const [filter, setFilter] = useState<ThreadStatusFilter>("all");
    return (
      <ThreadStatusFilterPill
        counts={{ working: 1234, input: 56, done: 789 }}
        filter={filter}
        onCycle={() => setFilter(nextThreadStatusFilter(filter))}
      />
    );
  }
  await act(async () => root.render(<Fixture />));
  const button = container.querySelector("button")!;
  const numbers = () =>
    Array.from(button.querySelectorAll("span")).filter(
      (node) =>
        ["1234", "56", "789"].includes(node.textContent ?? "") && node.children.length === 0,
    );
  for (const selected of [-1, 0, 1, 2, -1]) {
    expect(button.getAttribute("aria-label")).toContain(
      `Showing ${["working", "needs input", "done"][selected] ?? "all threads"}.`,
    );
    numbers().forEach((number, index) => {
      expect(getComputedStyle(number).textDecorationLine.includes("underline")).toBe(false);
      expect(getComputedStyle(number).backgroundColor !== "rgba(0, 0, 0, 0)").toBe(
        index === selected,
      );
    });
    if (selected !== -1 || button.getAttribute("aria-label")?.includes("Showing all threads"))
      await act(async () => {
        await userEvent.click(button);
      });
  }
});

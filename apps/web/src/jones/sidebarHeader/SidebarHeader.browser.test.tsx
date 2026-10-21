import { act, render as renderTest, type Screen } from "../../../test/render";
import { useState } from "react";
import { expect, it, vi } from "vite-plus/test";
import { userEvent } from "vite-plus/test/browser";
import "~/index.css";
import { ThreadStatusFilterPill } from "../threadStatusFilter/ThreadStatusFilterPill";
import { nextThreadStatusFilter, type ThreadStatusFilter } from "../threadStatusFilter/model";

let root: Screen;
let container: HTMLDivElement;
async function setupTest(): Promise<AsyncDisposable> {
  let initialized = false;
  let ownedContainer: HTMLDivElement | undefined;
  async function cleanup() {
    const screen = initialized ? root : undefined;
    try {
      await act(async () => screen?.unmount());
    } finally {
      try {
        await screen?.[Symbol.asyncDispose]();
      } finally {
        ownedContainer?.remove();
        vi.unstubAllGlobals();
      }
    }
  }
  try {
    ownedContainer = container = document.createElement("div");
    document.body.append(container);
    root = await renderTest(null, { container: container, router: false });
    initialized = true;

    return { [Symbol.asyncDispose]: cleanup };
  } catch (error) {
    await cleanup();
    throw error;
  }
}

it("cycles all statuses with only the selected number highlighted and no underline", async () => {
  await using _fixture = await setupTest();
  function Fixture() {
    const [filter, setFilter] = useState<ThreadStatusFilter>("all");
    return (
      <ThreadStatusFilterPill
        counts={{ working: 1234, input: 56, done: 789 }}
        filter={filter}
        onCycle={() => setFilter(nextThreadStatusFilter(filter))}
        onHideWorking={() => setFilter("hide-working")}
      />
    );
  }
  await act(async () => await root.rerender(<Fixture />));
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
    if (selected !== -1 || button.getAttribute("aria-label")?.includes("Showing all threads")) {
      button.focus();
      await userEvent.keyboard("{Enter}");
    }
  }
});

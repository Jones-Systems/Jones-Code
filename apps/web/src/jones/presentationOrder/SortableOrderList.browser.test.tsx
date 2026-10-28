import { useState } from "react";
import { act, render } from "../../../test/render";
import { userEvent } from "vite-plus/test/browser";
import { expect, it, vi } from "vite-plus/test";
import "../../index.css";
import { SortableOrderList } from "./SortableOrderList";

function Editor() {
  const [keys, setKeys] = useState(["Alpha", "Beta", "Gamma"]);
  return (
    <SortableOrderList
      label="Project order"
      rows={keys.map((key) => ({ key, label: key }))}
      onReorder={setKeys}
    />
  );
}
const order = (container: HTMLElement) =>
  [...container.querySelectorAll("li")].map((node) =>
    node.querySelector('[aria-label^="Reorder"]')!.getAttribute("aria-label"),
  );
const button = (container: HTMLElement, label: string) =>
  container.querySelector<HTMLButtonElement>(`[aria-label="${label}"]`)!;
it("moves visible rows with arrow buttons and disables moves beyond the ends", async () => {
  const container = document.createElement("div");
  container.style.width = "320px";
  await using screen = await render(<Editor />, { container });
  expect(button(screen.container, "Move Alpha up").disabled).toBe(true);
  expect(button(screen.container, "Move Gamma down").disabled).toBe(true);
  await act(async () => button(screen.container, "Move Beta up").click());
  expect(order(screen.container)).toEqual(["Reorder Beta", "Reorder Alpha", "Reorder Gamma"]);
  await act(async () => button(screen.container, "Move Beta down").click());
  expect(order(screen.container)).toEqual(["Reorder Alpha", "Reorder Beta", "Reorder Gamma"]);
});
it("reorders with keyboard drag and preserves order when drag is cancelled", async () => {
  const container = document.createElement("div");
  container.style.width = "320px";
  await using screen = await render(<Editor />, { container });
  button(screen.container, "Reorder Alpha").focus();
  await userEvent.keyboard(" ");
  await userEvent.keyboard("{ArrowDown}");
  await vi.waitFor(() =>
    expect(document.querySelector('[role="status"]')?.textContent).toContain(
      "was moved over droppable area Beta.",
    ),
  );
  await userEvent.keyboard(" ");
  expect(order(screen.container)).toEqual(["Reorder Beta", "Reorder Alpha", "Reorder Gamma"]);
  button(screen.container, "Reorder Alpha").focus();
  await userEvent.keyboard(" ");
  await userEvent.keyboard("{ArrowDown}");
  await vi.waitFor(() =>
    expect(document.querySelector('[role="status"]')?.textContent).toContain(
      "was moved over droppable area Gamma.",
    ),
  );
  await userEvent.keyboard("{Escape}");
  expect(order(screen.container)).toEqual(["Reorder Beta", "Reorder Alpha", "Reorder Gamma"]);
});

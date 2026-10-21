import { act, useState } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";
import "../../index.css";
import { SortableOrderList } from "./SortableOrderList";

let root: Root;
let container: HTMLDivElement;
beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  container = document.createElement("div");
  container.style.width = "320px";
  document.body.append(container);
  root = createRoot(container);
});
afterEach(async () => {
  await act(async () => root.unmount());
  container.remove();
  vi.unstubAllGlobals();
});
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
async function render() {
  await act(async () => root.render(<Editor />));
}
const order = () =>
  [...container.querySelectorAll("li")].map((node) =>
    node.querySelector('[aria-label^="Reorder"]')!.getAttribute("aria-label"),
  );
const button = (label: string) =>
  container.querySelector<HTMLButtonElement>(`[aria-label="${label}"]`)!;
async function key(target: EventTarget, code: string) {
  await act(async () => {
    target.dispatchEvent(
      new KeyboardEvent("keydown", {
        key: code === "Space" ? " " : code,
        code,
        bubbles: true,
        cancelable: true,
      }),
    );
    await new Promise<void>((resolve) => requestAnimationFrame(() => resolve()));
  });
}
it("moves visible rows with arrow buttons and disables moves beyond the ends", async () => {
  await render();
  expect(button("Move Alpha up").disabled).toBe(true);
  expect(button("Move Gamma down").disabled).toBe(true);
  await act(async () => button("Move Beta up").click());
  expect(order()).toEqual(["Reorder Beta", "Reorder Alpha", "Reorder Gamma"]);
  await act(async () => button("Move Beta down").click());
  expect(order()).toEqual(["Reorder Alpha", "Reorder Beta", "Reorder Gamma"]);
});
it("reorders with keyboard drag and preserves order when drag is cancelled", async () => {
  await render();
  button("Reorder Alpha").focus();
  await key(button("Reorder Alpha"), "Space");
  await key(document, "ArrowDown");
  await key(document, "Space");
  expect(order()).toEqual(["Reorder Beta", "Reorder Alpha", "Reorder Gamma"]);
  button("Reorder Alpha").focus();
  await key(button("Reorder Alpha"), "Space");
  await key(document, "ArrowDown");
  await key(document, "Escape");
  expect(order()).toEqual(["Reorder Beta", "Reorder Alpha", "Reorder Gamma"]);
});

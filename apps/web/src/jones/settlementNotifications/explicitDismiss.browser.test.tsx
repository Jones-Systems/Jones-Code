import "~/index.css";
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";

vi.mock(import("~/hooks/useThreadRouteTarget"), () => ({
  useThreadRouteTarget: () => null,
}));

import { ToastProvider, toastManager } from "~/components/ui/toast";

let root: Root;
let container: HTMLDivElement;

beforeEach(async () => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
  await act(async () => root.render(<ToastProvider />));
});

afterEach(async () => {
  await act(async () => toastManager.close());
  await act(async () => root.unmount());
  container.remove();
  vi.unstubAllGlobals();
});

function closeButton() {
  return document.querySelector<HTMLButtonElement>('[aria-label="Dismiss notification"]')!;
}

async function addToast(explicitDismissOnly = false) {
  const onClose = vi.fn();
  const navigate = vi.fn();
  await act(async () => {
    toastManager.add({
      title: <button onClick={navigate}>Settled thread</button>,
      description: "Settlement details",
      timeout: 0,
      data: { explicitDismissOnly, onClose },
    });
  });
  await expect.poll(() => closeButton()).not.toBeNull();
  return { onClose, navigate };
}

it("keeps default Escape dismissal", async () => {
  await addToast();
  closeButton().focus();
  await userEvent.keyboard("{Escape}");
  await expect.poll(() => closeButton()).toBeNull();
});

it("keeps an explicit-only toast through navigation and Escape until X closes it once", async () => {
  const { onClose, navigate } = await addToast(true);
  const title = document.querySelector<HTMLButtonElement>('[data-slot="toast-title"] button')!;
  await userEvent.click(title);
  expect(navigate).toHaveBeenCalledTimes(1);
  closeButton().focus();
  await userEvent.keyboard("{Escape}");
  expect(closeButton()).not.toBeNull();
  expect(onClose).not.toHaveBeenCalled();
  await userEvent.click(closeButton());
  await expect.poll(() => closeButton()).toBeNull();
  expect(onClose).toHaveBeenCalledTimes(1);
});

async function swipeRight() {
  const description = document.querySelector<HTMLElement>('[data-slot="toast-description"]')!;
  const toast = description.closest<HTMLElement>("[data-position]")!;
  // Synthetic pointers have no browser capture; keep the real bubbling handlers.
  const capture = vi.spyOn(toast, "setPointerCapture").mockImplementation(() => {});
  const pointer = (type: string, clientX: number, movementX = 0) =>
    description.dispatchEvent(
      new PointerEvent(type, {
        bubbles: true,
        cancelable: true,
        pointerId: 1,
        pointerType: "touch",
        button: 0,
        buttons: type === "pointerup" ? 0 : 1,
        clientX,
        clientY: 20,
        movementX,
      }),
    );
  await act(async () => {
    pointer("pointerdown", 20);
  });
  await act(async () => {
    pointer("pointermove", 25, 5);
  });
  await act(async () => {
    pointer("pointermove", 100, 75);
  });
  await act(async () => {
    pointer("pointermove", 260, 160);
  });
  await act(async () => {
    pointer("pointerup", 260);
  });
  capture.mockRestore();
}

it("keeps default swipe dismissal", async () => {
  await addToast();
  await swipeRight();
  await expect.poll(() => closeButton()).toBeNull();
});

it("ignores swipe dismissal for an explicit-only toast", async () => {
  const { onClose } = await addToast(true);
  await swipeRight();
  expect(closeButton()).not.toBeNull();
  expect(onClose).not.toHaveBeenCalled();
  await userEvent.click(closeButton());
  await expect.poll(() => closeButton()).toBeNull();
  expect(onClose).toHaveBeenCalledTimes(1);
});

it("keeps ordinary expandable toasts at their original width", async () => {
  await act(async () => {
    toastManager.add({
      title: "Ordinary notice",
      timeout: 0,
      data: { expandableContent: <p>{"Detail ".repeat(100)}</p> },
    });
  });
  const toast = closeButton().closest<HTMLElement>("[data-position]")!;
  const width = toast.getBoundingClientRect().width;
  expect(width).toBeGreaterThan(0);
  await userEvent.click(page.getByRole("button", { name: "Show details" }));
  expect(document.querySelector('[data-slot="toast-expandable-panel"]')).not.toBeNull();
  expect(toast.getBoundingClientRect().width).toBe(width);
});

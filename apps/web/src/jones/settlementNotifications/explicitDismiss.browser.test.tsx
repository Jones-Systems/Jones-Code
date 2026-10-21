import { act, render as renderTest, type Screen } from "../../../test/render";
import "~/index.css";

import { expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";

vi.mock(import("~/hooks/useThreadRouteTarget"), () => ({
  useThreadRouteTarget: () => null,
}));

import { ToastProvider, toastManager } from "~/components/ui/toast";

let root: Screen;
let container: HTMLDivElement;

async function setupTest(): Promise<AsyncDisposable> {
  let initialized = false;
  let ownedContainer: HTMLDivElement | undefined;
  async function cleanup() {
    const screen = initialized ? root : undefined;
    try {
  await act(async () => toastManager.close());
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
  await act(async () => await root.rerender(<ToastProvider />));

    return { [Symbol.asyncDispose]: cleanup };
  } catch (error) {
    await cleanup();
    throw error;
  }
}



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
    await using _fixture = await setupTest();
  await addToast();
  closeButton().focus();
  await userEvent.keyboard("{Escape}");
  await expect.poll(() => closeButton()).toBeNull();
});

it("keeps an explicit-only toast through navigation and Escape until X closes it once", async () => {
    await using _fixture = await setupTest();
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
    await using _fixture = await setupTest();
  await addToast();
  await swipeRight();
  await expect.poll(() => closeButton()).toBeNull();
});

it("ignores swipe dismissal for an explicit-only toast", async () => {
    await using _fixture = await setupTest();
  const { onClose } = await addToast(true);
  await swipeRight();
  expect(closeButton()).not.toBeNull();
  expect(onClose).not.toHaveBeenCalled();
  await userEvent.click(closeButton());
  await expect.poll(() => closeButton()).toBeNull();
  expect(onClose).toHaveBeenCalledTimes(1);
});

it("keeps ordinary expandable toasts at their original width", async () => {
    await using _fixture = await setupTest();
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

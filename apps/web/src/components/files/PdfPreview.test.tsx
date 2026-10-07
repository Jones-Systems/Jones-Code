// @vitest-environment jsdom
import { act, useState } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

const engine = vi.hoisted(() => ({
  viewers: [] as {
    scale: number;
    document: unknown;
    cleanup: ReturnType<typeof vi.fn>;
    signal: AbortSignal;
    bus: { dispatch: (name: string, data: object) => void };
  }[],
  tasks: [] as {
    destroy: ReturnType<typeof vi.fn>;
    resolve: (document: object) => void;
    reject: (error: Error) => void;
    onPassword: (() => void) | undefined;
  }[],
  failed: false,
  fitScale: 0.5,
  loads: vi.fn(),
  scales: vi.fn(),
  finds: vi.fn(),
}));
vi.mock("pdfjs-dist", () => ({
  GlobalWorkerOptions: { workerSrc: "" },
  getDocument: (options: object) => {
    engine.loads(options);
    let resolve!: (document: object) => void;
    let reject!: (error: Error) => void;
    const promise = new Promise<object>((yes, no) => {
      resolve = yes;
      reject = no;
    });
    const task = {
      promise,
      resolve,
      reject,
      destroy: vi.fn().mockResolvedValue(undefined),
      onPassword: undefined as (() => void) | undefined,
    };
    engine.tasks.push(task);
    if (engine.failed) reject(new Error("invalid PDF"));
    else resolve({ fixture: "PDF" });
    return task;
  },
}));
vi.mock("pdfjs-dist/web/pdf_viewer.mjs", () => ({
  LinkTarget: { BLANK: 2 },
  EventBus: class {
    listeners = new Map<string, Set<(data: object) => void>>();
    on(name: string, callback: (data: object) => void) {
      if (!this.listeners.has(name)) this.listeners.set(name, new Set());
      this.listeners.get(name)!.add(callback);
    }
    off(name: string, callback: (data: object) => void) {
      this.listeners.get(name)?.delete(callback);
    }
    dispatch(name: string, data: object) {
      if (name === "find") engine.finds(data);
      for (const callback of this.listeners.get(name) ?? []) callback(data);
    }
  },
  PDFLinkService: class {
    setViewer() {}
    setDocument() {}
  },
  PDFFindController: vi.fn(),
  PDFViewer: class {
    scale = 0;
    document: unknown = null;
    cleanup = vi.fn();
    bus: { dispatch: (name: string, data: object) => void };
    signal: AbortSignal;
    constructor(options: {
      eventBus: { dispatch: (name: string, data: object) => void };
      abortSignal: AbortSignal;
    }) {
      this.bus = options.eventBus;
      this.signal = options.abortSignal;
      engine.viewers.push(this);
    }
    get currentScale() {
      return this.scale;
    }
    set currentScale(value: number) {
      this.scale = value;
    }
    set currentScaleValue(value: string) {
      if (value === "page-width") this.scale = engine.fitScale;
    }
    setDocument(document: unknown) {
      this.document = document;
      if (document) {
        this.bus.dispatch("pagesinit", {});
        this.bus.dispatch("pagerendered", {});
      }
    }
    updateScale(options: { scaleFactor: number; origin?: [number, number] }) {
      engine.scales(options);
      this.scale = Math.round(this.scale * options.scaleFactor * 100) / 100;
    }
  },
}));
import PdfPreview from "./PdfPreview";

describe("PDF zoom and lifetime", () => {
  let container: HTMLDivElement;
  let root: Root;
  let notifyResize: () => void;
  const region = () => container.querySelector<HTMLElement>('[role="region"]')!;
  const input = () => container.querySelector<HTMLInputElement>('[aria-label="Zoom percentage"]')!;
  const viewer = () => engine.viewers.at(-1)!;
  const dispatch = async (target: Element, event: Event) =>
    act(() => {
      target.dispatchEvent(event);
    });
  const key = (target: Element, key: string) =>
    dispatch(target, new KeyboardEvent("keydown", { key, bubbles: true, cancelable: true }));
  const click = (target: Element) =>
    dispatch(target, new MouseEvent("click", { bubbles: true, clientX: 100, clientY: 80 }));
  const render = async (src = "/fixture.pdf") =>
    act(() => root.render(<PdfPreview src={src} title="Fixture" />));
  const edit = async (value: string) => {
    await act(() => {
      Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")!.set!.call(
        input(),
        value,
      );
      input().dispatchEvent(new Event("input", { bubbles: true }));
    });
    await key(input(), "Enter");
  };
  beforeEach(() => {
    engine.viewers.length = 0;
    engine.tasks.length = 0;
    engine.failed = false;
    engine.fitScale = 0.5;
    engine.scales.mockClear();
    engine.loads.mockClear();
    engine.finds.mockClear();
    vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
    vi.stubGlobal(
      "ResizeObserver",
      class {
        constructor(callback: () => void) {
          notifyResize = callback;
        }
        observe() {}
        disconnect() {}
      },
    );
    container = document.createElement("div");
    document.body.append(container);
    root = createRoot(container);
  });
  afterEach(async () => {
    await act(() => root.unmount());
    container.remove();
    vi.unstubAllGlobals();
  });

  it("zooms from fitted width through the click cycle and keyboard steps", async () => {
    await render();
    expect(viewer().scale).toBe(0.5);
    for (const percent of [110, 120, 130, 140, 150, 100]) {
      await click(region());
      expect(input().value).toBe(String(percent));
      expect(viewer().scale).toBeCloseTo((0.5 * percent) / 100);
      expect(region().style.cursor).toBe(percent >= 150 ? "zoom-out" : "zoom-in");
    }
    await key(region(), "Enter");
    await key(region(), " ");
    expect(input().value).toBe("120");
    await key(region(), "-");
    expect(input().value).toBe("110");
    await key(region(), "0");
    expect(viewer().scale).toBeCloseTo(0.5);
  });

  it("commits percentages, rejects invalid values, clamps wheel and button steps, and resets", async () => {
    await render();
    await edit("245");
    expect(viewer().scale).toBeCloseTo(1.23);
    await edit("60");
    for (const value of ["", "bad", "59", "801"]) {
      await edit(value);
      expect(input().value).toBe("60");
    }
    expect(container.querySelector<HTMLButtonElement>('[aria-label="Zoom out"]')!.disabled).toBe(
      true,
    );
    await click(container.querySelector('[aria-label="Zoom in"]')!);
    expect(input().value).toBe("70");
    const wheel = new WheelEvent("wheel", {
      deltaY: 10000,
      bubbles: true,
      cancelable: true,
      clientX: 100,
      clientY: 80,
    });
    await dispatch(region(), wheel);
    expect(wheel.defaultPrevented).toBe(true);
    expect(input().value).toBe("60");
    expect(engine.scales.mock.lastCall![0].origin).toEqual([100, 80]);
    await dispatch(
      region(),
      new WheelEvent("wheel", { deltaY: -10000, bubbles: true, cancelable: true }),
    );
    expect(input().value).toBe("800");
    expect(viewer().scale).toBeCloseTo(4);
    await click(container.querySelector('[aria-label="Reset zoom to 100%"]')!);
    expect(input().value).toBe("100");
    expect(viewer().scale).toBeCloseTo(0.5);
    const scrolling = new WheelEvent("wheel", {
      shiftKey: true,
      deltaY: 200,
      bubbles: true,
      cancelable: true,
    });
    await dispatch(region(), scrolling);
    expect(scrolling.defaultPrevented).toBe(true);
    expect(region().scrollTop).toBe(200);
    expect(input().value).toBe("100");
  });

  it("enforces the fitted sixty-percent floor despite engine rounding", async () => {
    engine.fitScale = 0.556;
    await render();
    await edit("60");
    expect(viewer().scale).toBe(0.556 * 0.6);
    await key(region(), "0");
    expect(viewer().scale).toBe(0.556);
  });

  it("retains zoom when scrollbars narrow the viewport and refits when the panel narrows", async () => {
    await render();
    const sizing = region().parentElement!;
    Object.defineProperty(sizing, "clientWidth", { configurable: true, value: 400 });
    await act(() => notifyResize());
    await edit("150");
    Object.defineProperty(region(), "clientWidth", { configurable: true, value: 380 });
    await act(() => notifyResize());
    expect(input().value).toBe("150");
    expect(viewer().scale).toBeCloseTo(0.75);
    Object.defineProperty(sizing, "clientWidth", { configurable: true, value: 300 });
    await act(() => notifyResize());
    expect(input().value).toBe("100");
  });

  it("preserves link/form actions and suppresses zoom after dragging text selection", async () => {
    await render();
    const link = document.createElement("a");
    link.href = "#page2";
    const field = document.createElement("input");
    region().append(link, field);
    await click(link);
    await click(field);
    await key(field, "Enter");
    expect(input().value).toBe("100");
    await dispatch(
      region(),
      new MouseEvent("pointerdown", { bubbles: true, clientX: 20, clientY: 20 }),
    );
    await dispatch(
      region(),
      new MouseEvent("pointermove", { bubbles: true, clientX: 40, clientY: 20 }),
    );
    await click(region());
    expect(input().value).toBe("100");
    await click(region());
    expect(input().value).toBe("110");
  });

  it("destroys the previous task and cancels viewer observers on source change and unmount", async () => {
    await render();
    await edit("150");
    const oldViewer = viewer();
    const oldTask = engine.tasks[0]!;
    await render("/next.pdf");
    expect(oldTask.destroy).toHaveBeenCalledOnce();
    expect(oldViewer.signal.aborted).toBe(true);
    expect(oldViewer.document).toBeNull();
    expect(oldViewer.cleanup).toHaveBeenCalledOnce();
    expect(input().value).toBe("100");
    expect(viewer().scale).toBe(0.5);
    const currentViewer = viewer();
    const currentTask = engine.tasks.at(-1)!;
    await act(() => root.unmount());
    expect(currentTask.destroy).toHaveBeenCalledOnce();
    expect(currentViewer.signal.aborted).toBe(true);
    root = createRoot(container);
  });

  it("shows a truthful failure and retries a failed load", async () => {
    engine.failed = true;
    await render();
    expect(container.querySelector('[role="alert"]')!.textContent).toContain("Unable to preview");
    expect(input().disabled).toBe(true);
    engine.failed = false;
    await click(
      Array.from(container.querySelectorAll("button")).find(
        (button) => button.textContent === "Retry",
      )!,
    );
    expect(container.querySelector('[role="alert"]')).toBeNull();
    expect(input().disabled).toBe(false);
    await act(() => viewer().bus.dispatch("pagerendered", { error: new Error("render failed") }));
    expect(container.querySelector('[role="alert"]')).not.toBeNull();
  });

  it("renews authorization before the owning panel remounts a failed PDF", async () => {
    engine.failed = true;
    const renew = vi.fn().mockResolvedValue("/renewed.pdf");
    function AuthorizedPreview() {
      const [src, setSrc] = useState("/expired.pdf");
      return (
        <PdfPreview
          key={src}
          src={src}
          title="Fixture"
          onRetry={async () => setSrc(await renew())}
        />
      );
    }
    await act(() => root.render(<AuthorizedPreview />));
    const oldTask = engine.tasks[0]!;
    engine.failed = false;
    await click(
      Array.from(container.querySelectorAll("button")).find(
        (button) => button.textContent === "Retry",
      )!,
    );
    expect(renew).toHaveBeenCalledOnce();
    expect(engine.loads.mock.calls.map(([options]) => options.url)).toEqual([
      "/expired.pdf",
      "/renewed.pdf",
    ]);
    expect(oldTask.destroy).toHaveBeenCalledOnce();
    expect(container.querySelector('[role="alert"]')).toBeNull();
    expect(input().value).toBe("100");
  });

  it("keeps the failure and external-open link when reauthorization fails without reloading", async () => {
    engine.failed = true;
    let reject!: (error: Error) => void;
    const onRetry = vi.fn(
      () =>
        new Promise<void>((_, no) => {
          reject = no;
        }),
    );
    await act(() =>
      root.render(<PdfPreview src="/expired.pdf" title="Fixture" onRetry={onRetry} />),
    );
    const retry = Array.from(container.querySelectorAll("button")).find(
      (button) => button.textContent === "Retry",
    )!;
    await click(retry);
    expect(retry.disabled).toBe(true);
    expect(engine.loads).toHaveBeenCalledOnce();
    await act(() => reject(new Error("Reconnect to the environment and try again.")));
    expect(retry.disabled).toBe(false);
    expect(container.querySelector('[role="alert"]')).not.toBeNull();
    expect(container.querySelector("a")!.getAttribute("href")).toBe("/expired.pdf");
    expect(engine.loads).toHaveBeenCalledOnce();
  });

  it("offers external open for a password-protected PDF", async () => {
    await render();
    await act(() => engine.tasks[0]!.onPassword!());
    expect(input().disabled).toBe(true);
    expect(container.querySelector('[role="alert"]')).not.toBeNull();
    const link = container.querySelector("a")!;
    expect(link.getAttribute("href")).toBe("/fixture.pdf");
    expect(link.target).toBe("_blank");
    expect(link.rel).toBe("noopener noreferrer");
  });

  it("routes PDF find and repeat searches through the engine", async () => {
    await render();
    await dispatch(
      region(),
      new KeyboardEvent("keydown", { key: "f", ctrlKey: true, bubbles: true, cancelable: true }),
    );
    const search = container.querySelector<HTMLInputElement>('[aria-label="Find in PDF"]')!;
    await act(() => {
      Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")!.set!.call(
        search,
        "needle",
      );
      search.dispatchEvent(new Event("input", { bubbles: true }));
    });
    expect(engine.finds.mock.lastCall![0]).toMatchObject({ query: "needle", highlightAll: true });
    await key(search, "Enter");
    expect(engine.finds.mock.lastCall![0]).toMatchObject({ type: "again" });
    await key(search, "Escape");
    expect(container.querySelector('[role="search"]')).toBeNull();
  });
});
